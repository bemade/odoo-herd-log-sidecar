// License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl).

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newTestServer builds a server whose stream func records whether it was
// reached and with which scope, so auth tests can assert that the kube path is
// only entered for a valid token — without needing a real cluster.
func newTestServer(secret string) (*server, *int32, *Scope) {
	var reached int32
	var gotScope Scope
	srv := &server{
		secret: secret,
		stream: func(_ context.Context, scope *Scope, _ http.ResponseWriter, _ http.Flusher) error {
			atomic.StoreInt32(&reached, 1)
			gotScope = *scope
			return nil
		},
	}
	return srv, &reached, &gotScope
}

func doStream(srv *server, authHeader string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	rec := httptest.NewRecorder()
	srv.handleStream(rec, req)
	return rec
}

func TestStreamRejectsMissingToken(t *testing.T) {
	srv, reached, _ := newTestServer(testSecret)
	rec := doStream(srv, "")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("missing token: status = %d, want 401", rec.Code)
	}
	if atomic.LoadInt32(reached) != 0 {
		t.Error("missing token must not reach the kube path")
	}
}

func TestStreamRejectsMalformedHeader(t *testing.T) {
	srv, reached, _ := newTestServer(testSecret)
	for _, h := range []string{"Bearer", "Bearer ", "Token abc", "abc"} {
		rec := doStream(srv, h)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("header %q: status = %d, want 401", h, rec.Code)
		}
	}
	if atomic.LoadInt32(reached) != 0 {
		t.Error("malformed header must not reach the kube path")
	}
}

func TestStreamRejectsBadSignature(t *testing.T) {
	srv, reached, _ := newTestServer(testSecret)
	now := time.Now()
	// Mint with the WRONG secret -> signature won't verify against testSecret.
	tok := mintToken(t, "the-wrong-secret", validScope(now))
	rec := doStream(srv, "Bearer "+tok)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("bad signature: status = %d, want 401", rec.Code)
	}
	if atomic.LoadInt32(reached) != 0 {
		t.Error("bad-signature token must not reach the kube path")
	}
}

func TestStreamRejectsExpiredToken(t *testing.T) {
	srv, reached, _ := newTestServer(testSecret)
	now := time.Now()
	// Mint a token that already expired (exp in the past).
	expired := validScope(now)
	expired.Iat = now.Add(-300 * time.Second).Unix()
	expired.Exp = now.Add(-180 * time.Second).Unix()
	tok := mintToken(t, testSecret, expired)
	rec := doStream(srv, "Bearer "+tok)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expired token: status = %d, want 401", rec.Code)
	}
	if atomic.LoadInt32(reached) != 0 {
		t.Error("expired token must not reach the kube path")
	}
}

func TestStreamValidTokenReachesKubePath(t *testing.T) {
	srv, reached, gotScope := newTestServer(testSecret)
	now := time.Now()
	tok := mintToken(t, testSecret, validScope(now))
	rec := doStream(srv, "Bearer "+tok)

	if rec.Code != http.StatusOK {
		t.Errorf("valid token: status = %d, want 200", rec.Code)
	}
	if atomic.LoadInt32(reached) != 1 {
		t.Fatal("valid token must reach the kube path")
	}
	// Scope must come from the verified payload, not from any request param.
	if gotScope.Ns != "nsclient-alpha" || gotScope.Sel != "bemade.org/instance=alpha-prod" {
		t.Errorf("scope = %+v, want ns=nsclient-alpha sel=bemade.org/instance=alpha-prod", *gotScope)
	}
	// NDJSON content type must be advertised.
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/x-ndjson") {
		t.Errorf("Content-Type = %q, want application/x-ndjson...", ct)
	}
}

// TestStreamScopeNotFromRequestParams asserts a request query/param cannot
// influence scope: the only authority is the token payload. Even if a caller
// appends ?ns=evil&sel=evil, the handler ignores it.
func TestStreamScopeNotFromRequestParams(t *testing.T) {
	srv, _, gotScope := newTestServer(testSecret)
	now := time.Now()
	tok := mintToken(t, testSecret, validScope(now))

	q := url.Values{}
	q.Set("ns", "victim-namespace")
	q.Set("sel", "bemade.org/instance=victim")
	req := httptest.NewRequest(http.MethodGet, "/stream?"+q.Encode(), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	srv.handleStream(rec, req)

	if gotScope.Ns != "nsclient-alpha" {
		t.Errorf("scope.Ns = %q, want nsclient-alpha (request param must be ignored)", gotScope.Ns)
	}
}
