package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// logLine is a single merged, labelled log record fanned in from a pod.
type logLine struct {
	pod       string
	container string
	line      string
}

// multiplexPodLogs implements the Stern-style fan-in described in
// docs/SIDECAR.md §4. It is SCAFFOLDED: the informer/attach-detach lifecycle is
// TODO-stubbed. The wiring (in-cluster client, scope-driven List, Follow log
// streams, fan-in channel, per-line flush) is laid out so a human reviewer can
// complete it against a real cluster.
//
// CRITICAL: scoping uses ONLY scope.Ns and scope.Sel (both from the verified
// token). No request parameter ever reaches here.
func multiplexPodLogs(ctx context.Context, scope *Scope, w http.ResponseWriter, flusher http.Flusher) error {
	clientset, err := inClusterClient()
	if err != nil {
		return fmt.Errorf("kube client: %w", err)
	}

	lines := make(chan logLine, 256)
	var wg sync.WaitGroup

	// --- Initial pod list, scoped by ns + label selector ------------------
	pods, err := clientset.CoreV1().Pods(scope.Ns).List(ctx, metav1.ListOptions{
		LabelSelector: scope.Sel,
	})
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		for _, c := range pod.Spec.Containers {
			wg.Add(1)
			go func(podName, containerName string) {
				defer wg.Done()
				tailContainer(ctx, clientset, scope.Ns, podName, containerName, lines)
			}(pod.Name, c.Name)
		}
	}

	// TODO(human review + cluster): start a pod informer/watch scoped to
	// scope.Ns + scope.Sel and attach/detach tailContainer goroutines as pods
	// are added/deleted (rollouts, cron pods, Job pods). On pod delete, cancel
	// that pod's context. See docs/SIDECAR.md §4 steps 2 and 5.

	// --- Fan-in writer: drain merged lines to the response ----------------
	go func() {
		wg.Wait()
		close(lines)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ln, ok := <-lines:
			if !ok {
				return nil
			}
			fmt.Fprintf(w, "[%s/%s] %s\n", ln.pod, ln.container, ln.line)
			flusher.Flush()
		}
	}
}

// tailContainer opens a Follow:true log stream for one container and pushes
// each line onto the fan-in channel until ctx is cancelled or the stream ends.
func tailContainer(ctx context.Context, cs *kubernetes.Clientset, ns, pod, container string, out chan<- logLine) {
	// TODO(human review + cluster): tune TailLines / SinceSeconds for the
	// "recent only" window (docs/SIDECAR.md §6).
	tail := int64(200)
	req := cs.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{
		Container: container,
		Follow:    true,
		TailLines: &tail,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		// A pod can vanish mid-attach; that's expected, not fatal.
		return
	}
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		case out <- logLine{pod: pod, container: container, line: scanner.Text()}:
		}
	}
}

// inClusterClient builds a Kubernetes clientset from the in-cluster config
// (the sidecar's own least-privilege ServiceAccount). It never reads the
// operator kubeconfig.
func inClusterClient() (*kubernetes.Clientset, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}
