// License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl).

package main

import (
	"context"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// NOTE: the fake clientset serves Pods().List/Watch with label-selector
// filtering, so pod DISCOVERY (which pods a token's ns+sel resolves to) is
// fully unit-testable here. The fake clientset does NOT implement the pods/log
// subresource, so the Follow:true log multiplex itself (streamOnce /
// tailContainer fan-in) is only exercisable against a real cluster.

func pod(name, ns string, labels map[string]string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

func TestDiscoverPodsSelectorScoping(t *testing.T) {
	const ns = "nsclient-alpha"
	const sel = "bemade.org/instance=alpha-prod"

	match := map[string]string{"bemade.org/instance": "alpha-prod"}

	cs := fake.NewSimpleClientset(
		// Matching pods in the target namespace: web, cron, transient Job pod.
		pod("alpha-prod-web-abc", ns, match, corev1.PodRunning),
		pod("alpha-prod-cron-xyz", ns, match, corev1.PodRunning),
		pod("alpha-prod-job-123", ns, match, corev1.PodSucceeded),
		// Different instance in the SAME namespace: must be excluded (namespaces
		// are per-client and may hold multiple instances; selector isolates).
		pod("beta-prod-web-def", ns, map[string]string{"bemade.org/instance": "beta-prod"}, corev1.PodRunning),
		// No matching label at all: excluded.
		pod("unlabelled", ns, map[string]string{"app": "other"}, corev1.PodRunning),
		// Matching label but in ANOTHER namespace: excluded (ns scoping).
		pod("alpha-prod-web-other-ns", "nsclient-beta", match, corev1.PodRunning),
	)

	got, err := discoverPods(context.Background(), cs, ns, sel)
	if err != nil {
		t.Fatalf("discoverPods: %v", err)
	}

	var names []string
	for _, p := range got {
		names = append(names, p.Name)
	}
	sort.Strings(names)

	want := []string{"alpha-prod-cron-xyz", "alpha-prod-job-123", "alpha-prod-web-abc"}
	if len(names) != len(want) {
		t.Fatalf("matched pods = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("matched pods = %v, want %v", names, want)
		}
	}
}

func TestDiscoverPodsEmptyWhenNoMatch(t *testing.T) {
	cs := fake.NewSimpleClientset(
		pod("other", "nsclient-alpha", map[string]string{"app": "x"}, corev1.PodRunning),
	)
	got, err := discoverPods(context.Background(), cs, "nsclient-alpha", "bemade.org/instance=nope")
	if err != nil {
		t.Fatalf("discoverPods: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected no pods, got %d", len(got))
	}
}

// TestStreamManagerAttachIdempotent checks the Stern attach/detach bookkeeping:
// attaching the same running pod twice tracks it once; detach forgets it. (Log
// streaming itself is not exercised — fake clientset has no pods/log.)
func TestStreamManagerAttachIdempotent(t *testing.T) {
	cs := fake.NewSimpleClientset()
	out := make(chan logRecord, 1)
	m := &streamManager{
		clientset: cs,
		ns:        "nsclient-alpha",
		out:       out,
		active:    make(map[string]context.CancelFunc),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := pod("alpha-prod-web-abc", "nsclient-alpha",
		map[string]string{"bemade.org/instance": "alpha-prod"}, corev1.PodRunning)

	m.attach(ctx, p)
	m.attach(ctx, p) // idempotent
	m.mu.Lock()
	n := len(m.active)
	m.mu.Unlock()
	if n != 1 {
		t.Fatalf("active pods = %d, want 1 (attach must be idempotent)", n)
	}

	// A non-running pod is not tracked.
	pending := pod("alpha-prod-job-999", "nsclient-alpha",
		map[string]string{"bemade.org/instance": "alpha-prod"}, corev1.PodPending)
	m.attach(ctx, pending)
	m.mu.Lock()
	n = len(m.active)
	m.mu.Unlock()
	if n != 1 {
		t.Fatalf("active pods = %d, want 1 (pending pod must not be tracked)", n)
	}

	m.detach("alpha-prod-web-abc")
	m.mu.Lock()
	n = len(m.active)
	m.mu.Unlock()
	if n != 0 {
		t.Fatalf("active pods = %d, want 0 after detach", n)
	}
}
