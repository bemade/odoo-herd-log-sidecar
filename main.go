// Command odoo-herd-log-sidecar is an in-cluster log-stream sidecar for the
// Odoo Herd portal's Feature C live log viewer.
//
// STATUS: functional, but the live Follow log-multiplex is NOT yet verified
// against a real cluster (the fake clientset used in tests has no pods/log
// subresource). Token verification (token.go), the /stream auth gate, and pod
// discovery scoping are implemented and unit-tested. See docs/SIDECAR.md in the
// odoo_herd_portal module for the full design + token contract.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"
)

const tokenSecretEnv = "LOG_TOKEN_SECRET"

func main() {
	secret := os.Getenv(tokenSecretEnv)
	if secret == "" {
		log.Fatalf("%s must be set (mounted from the k8s Secret shared with Odoo)", tokenSecretEnv)
	}

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	srv := &server{secret: secret, stream: streamLogs}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", srv.handleHealthz)
	mux.HandleFunc("/stream", srv.handleStream)

	httpSrv := &http.Server{
		Addr:    addr,
		Handler: mux,
		// No write timeout: /stream is a long-lived streaming response.
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("odoo-herd-log-sidecar listening on %s", addr)
	if err := httpSrv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// streamFunc is the pod-log streaming entrypoint. It is a field on server so
// tests can substitute it (the real one needs a cluster). The default is
// streamLogs.
type streamFunc func(ctx context.Context, scope *Scope, w http.ResponseWriter, flusher http.Flusher) error

type server struct {
	secret string
	stream streamFunc
}

func (s *server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleStream verifies the Bearer scope token, then streams merged pod logs
// for the namespace + label selector encoded in the VERIFIED token payload.
// Scope is NEVER taken from request parameters.
func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	if token == "" {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}

	scope, err := VerifyToken(token, s.secret, time.Now())
	if err != nil {
		// Do not leak which check failed beyond a generic 401.
		http.Error(w, "invalid or expired token", http.StatusUnauthorized)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Wire format: newline-delimited JSON (NDJSON), one record per line. See the
	// WIRE FORMAT note in kube.go. Chosen over SSE because the SPA reads the
	// stream with fetch()+ReadableStream so the token rides in the Authorization
	// header (EventSource cannot set headers).
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Stream until the client disconnects (or, per review decision, until the
	// token would expire — see docs/SIDECAR.md §4/§8).
	ctx := r.Context()

	// s.stream ONLY uses scope.Ns and scope.Sel for scoping.
	if err := s.stream(ctx, scope, w, flusher); err != nil && ctx.Err() == nil {
		log.Printf("stream for iid=%d ns=%s sel=%q ended: %v",
			scope.Iid, scope.Ns, scope.Sel, err)
	}
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header. Returns "" when absent or malformed. The token is intentionally kept
// out of the URL so it never lands in access logs or browser history.
func bearerToken(r *http.Request) string {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
		return ""
	}
	return h[len(prefix):]
}

// streamLogs discovers the pods matching scope.Sel in scope.Ns and multiplexes
// their (Follow:true) log streams into w, flushing per line. See kube.go.
func streamLogs(ctx context.Context, scope *Scope, w http.ResponseWriter, flusher http.Flusher) error {
	return multiplexPodLogs(ctx, scope, w, flusher)
}
