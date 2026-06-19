// License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl).

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// logRecord is a single merged, labelled log record fanned in from one pod
// container. It is emitted to the client as one newline-delimited JSON object
// per line (NDJSON) — see the WIRE FORMAT note below.
type logRecord struct {
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Line      string `json:"line"`
	// Ts is the time the sidecar read the line (RFC3339Nano). The kubelet log
	// stream does not reliably carry per-line timestamps without Timestamps:true
	// (which would prepend them to Line), so this is the sidecar's receive time.
	Ts string `json:"ts"`
}

// WIRE FORMAT (documented for the future react-logviewer SPA):
//
//	Content-Type: application/x-ndjson
//
// The /stream response is a single long-lived HTTP response. The body is
// newline-delimited JSON: exactly one JSON object per line, each of the shape
//
//	{"pod":"...","container":"...","line":"...","ts":"2026-01-02T15:04:05.999Z"}
//
// One "heartbeat" object — {"heartbeat":true,"ts":"..."} — is emitted
// periodically so idle connections (and intermediary proxies) stay alive and
// the client can detect liveness.
//
// NDJSON (rather than SSE) is chosen because the SPA opens the stream with
// fetch() + a ReadableStream reader so the token can travel in an
// Authorization: Bearer header (EventSource/SSE cannot set arbitrary headers).
// See odoo_herd_portal/docs/SIDECAR.md §5.

const (
	// tailLines is the "recent context" window opened per container so the
	// viewer shows recent history on connect, not the whole log. See §6 ("recent
	// logs only") — this sidecar tails live pod logs, it is not a log store.
	tailLines int64 = 200

	// heartbeatInterval is how often a heartbeat record is written to keep the
	// connection (and proxies) alive when no log lines are flowing.
	heartbeatInterval = 20 * time.Second

	// podStreamRetryBackoff is the pause before re-attaching a container log
	// stream that errored while the pod is still present. A transient error on
	// one pod must never kill the whole response.
	podStreamRetryBackoff = 2 * time.Second
)

// multiplexPodLogs implements the Stern-style fan-in described in
// docs/SIDECAR.md §4: it watches pods in scope.Ns matching label selector
// scope.Sel via a SharedInformer, attaches a Follow:true log stream per running
// container as pods appear, detaches it when the pod is deleted, fans every
// line into a shared channel, and drains that channel to the HTTP response,
// flushing per line.
//
// CRITICAL: scoping uses ONLY scope.Ns and scope.Sel (both from the verified
// token). No request parameter ever reaches here.
//
// All goroutines, the informer, and every per-pod log stream are torn down when
// ctx is cancelled (client disconnect) — no leaks.
func multiplexPodLogs(ctx context.Context, scope *Scope, w http.ResponseWriter, flusher http.Flusher) error {
	clientset, err := newKubeClient()
	if err != nil {
		return fmt.Errorf("kube client: %w", err)
	}
	return multiplexWithClient(ctx, clientset, scope, w, flusher)
}

// multiplexWithClient is the testable core: it takes any kubernetes.Interface
// (real in-cluster client in production, fake clientset in unit tests) and runs
// the discovery + fan-in loop. Note: the fake clientset can serve pod List/Watch
// (so pod discovery is unit-testable) but does NOT implement the pods/log
// subresource streaming, so the Follow log multiplex itself is only exercisable
// against a real cluster.
func multiplexWithClient(ctx context.Context, clientset kubernetes.Interface, scope *Scope, w http.ResponseWriter, flusher http.Flusher) error {
	// Root context for everything we spawn; cancelled on client disconnect or
	// when this function returns, tearing down the informer and all streams.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lines := make(chan logRecord, 256)

	mgr := &streamManager{
		clientset: clientset,
		ns:        scope.Ns,
		out:       lines,
		active:    make(map[string]context.CancelFunc),
	}

	// --- Pod informer scoped to ns + label selector ----------------------
	// The selector is applied server-side; the informer only ever sees pods in
	// scope.Ns matching scope.Sel. This is the authority-derived scope. Its
	// lifetime is bound to ctx (cancelled on teardown), so we don't retain it.
	if _, err := mgr.startInformer(ctx, scope.Sel); err != nil {
		return fmt.Errorf("start pod informer: %w", err)
	}

	// --- Fan-in writer: drain merged lines to the response ----------------
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	enc := json.NewEncoder(w)
	for {
		select {
		case <-ctx.Done():
			mgr.stopAll()
			return ctx.Err()
		case <-heartbeat.C:
			if err := writeHeartbeat(enc, flusher); err != nil {
				mgr.stopAll()
				return err
			}
		case rec := <-lines:
			rec.Ts = time.Now().UTC().Format(time.RFC3339Nano)
			if err := enc.Encode(rec); err != nil {
				// Client write failure: stop everything.
				mgr.stopAll()
				return err
			}
			flusher.Flush()
		}
	}
}

func writeHeartbeat(enc *json.Encoder, flusher http.Flusher) error {
	hb := map[string]any{"heartbeat": true, "ts": time.Now().UTC().Format(time.RFC3339Nano)}
	if err := enc.Encode(hb); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// streamManager tracks the set of currently-tailed pods and attaches/detaches
// per-pod log streams as the informer reports pod add/delete events (the Stern
// model: the tailed set tracks the live selector through rollouts and cron/Job
// pods).
type streamManager struct {
	clientset kubernetes.Interface
	ns        string
	out       chan<- logRecord

	mu     sync.Mutex
	active map[string]context.CancelFunc // pod name -> cancel for its streams
}

// startInformer builds and runs a pod SharedInformer scoped to mgr.ns and the
// given label selector, wiring Add/Delete handlers to attach/detach streams.
// The informer stops when ctx is cancelled.
func (m *streamManager) startInformer(ctx context.Context, labelSelector string) (cache.SharedIndexInformer, error) {
	lw := cache.NewFilteredListWatchFromClient(
		m.clientset.CoreV1().RESTClient(),
		"pods",
		m.ns,
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = labelSelector
			opts.FieldSelector = fields.Everything().String()
		},
	)
	informer := cache.NewSharedIndexInformer(
		lw,
		&corev1.Pod{},
		0, // no resync; we react to add/delete events
		cache.Indexers{},
	)
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok {
				m.attach(ctx, pod)
			}
		},
		UpdateFunc: func(_, newObj any) {
			// A pod that transitions into Running after add (e.g. a freshly
			// scheduled rollout/cron/Job pod) gets its streams attached here.
			if pod, ok := newObj.(*corev1.Pod); ok {
				m.attach(ctx, pod)
			}
		},
		DeleteFunc: func(obj any) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				// Tombstone on delete.
				if tomb, isTomb := obj.(cache.DeletedFinalStateUnknown); isTomb {
					pod, ok = tomb.Obj.(*corev1.Pod)
				}
			}
			if ok && pod != nil {
				m.detach(pod.Name)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	go informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return nil, fmt.Errorf("informer cache failed to sync")
	}
	return informer, nil
}

// attach starts Follow log streams for every container of a Running pod, once.
// It is idempotent per pod: a pod already being tailed is left alone.
func (m *streamManager) attach(ctx context.Context, pod *corev1.Pod) {
	if pod.Status.Phase != corev1.PodRunning {
		return
	}
	m.mu.Lock()
	if _, exists := m.active[pod.Name]; exists {
		m.mu.Unlock()
		return
	}
	podCtx, cancel := context.WithCancel(ctx)
	m.active[pod.Name] = cancel
	containers := pod.Spec.Containers
	m.mu.Unlock()

	for _, c := range containers {
		go m.tailContainer(podCtx, pod.Name, c.Name)
	}
}

// detach cancels the streams for a deleted pod and forgets it.
func (m *streamManager) detach(podName string) {
	m.mu.Lock()
	cancel, ok := m.active[podName]
	delete(m.active, podName)
	m.mu.Unlock()
	if ok {
		cancel()
	}
}

// stopAll cancels every active pod's streams. Called on teardown.
func (m *streamManager) stopAll() {
	m.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(m.active))
	for name, c := range m.active {
		cancels = append(cancels, c)
		delete(m.active, name)
	}
	m.mu.Unlock()
	for _, c := range cancels {
		c()
	}
}

// tailContainer opens a Follow:true log stream for one container and pushes
// each line onto the fan-in channel until ctx is cancelled. A transient stream
// error (pod still present) is retried with a small backoff so one flaky pod
// never kills the whole response; the loop exits cleanly on ctx cancellation
// (pod deleted or client disconnected).
func (m *streamManager) tailContainer(ctx context.Context, pod, container string) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := m.streamOnce(ctx, pod, container)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("log stream %s/%s errored, retrying: %v", pod, container, err)
		}
		// Whether the stream ended (pod still around but kubelet closed it) or
		// errored, back off briefly then re-attach until ctx is cancelled.
		select {
		case <-ctx.Done():
			return
		case <-time.After(podStreamRetryBackoff):
		}
	}
}

// streamOnce opens a single Follow log stream and pumps its lines until the
// stream ends, ctx is cancelled, or an error occurs.
func (m *streamManager) streamOnce(ctx context.Context, pod, container string) error {
	tail := tailLines
	req := m.clientset.CoreV1().Pods(m.ns).GetLogs(pod, &corev1.PodLogOptions{
		Container: container,
		Follow:    true,
		TailLines: &tail,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	// Allow long log lines (default 64K cap is easy to exceed with tracebacks).
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		case m.out <- logRecord{Pod: pod, Container: container, Line: scanner.Text()}:
		}
	}
	return scanner.Err()
}

// newKubeClient builds a Kubernetes clientset. In production it uses the
// in-cluster config (the sidecar's own least-privilege ServiceAccount). For
// local development off-cluster it falls back to KUBECONFIG (or ~/.kube/config)
// so the sidecar is runnable against a cluster via `go run .`. It NEVER reads
// the operator kubeconfig in production — that fallback only triggers when
// there is no in-cluster ServiceAccount.
func newKubeClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		// Off-cluster (local dev): fall back to kubeconfig.
		cfg, err = outOfClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("no in-cluster config and no usable kubeconfig: %w", err)
		}
	}
	return kubernetes.NewForConfig(cfg)
}

// discoverPods lists the pods in ns matching the label selector via the given
// clientset. It encapsulates the scope-driven server-side filtering used to
// seed the informer, and exists primarily so pod discovery is unit-testable
// against the fake clientset (which serves List but not pods/log streaming).
func discoverPods(ctx context.Context, clientset kubernetes.Interface, ns, labelSelector string) ([]corev1.Pod, error) {
	list, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func outOfClusterConfig() (*rest.Config, error) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		kubeconfig = filepath.Join(home, ".kube", "config")
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}
