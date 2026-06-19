// License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl).

package main

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

// mintToken wraps MintToken (the shared mint helper in token.go, also used by
// cmd/mint-token) for the tests.
func mintToken(t *testing.T, secret string, s Scope) string {
	t.Helper()
	return MintToken(secret, s)
}

const testSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" // 64 hex chars

func validScope(now time.Time) Scope {
	return Scope{
		Exp: now.Add(120 * time.Second).Unix(),
		Iat: now.Unix(),
		Iid: 42,
		Ns:  "nsclient-alpha",
		Sel: "bemade.org/instance=alpha-prod",
	}
}

func TestVerifyAcceptsValidToken(t *testing.T) {
	now := time.Now()
	tok := mintToken(t, testSecret, validScope(now))

	scope, err := VerifyToken(tok, testSecret, now)
	if err != nil {
		t.Fatalf("expected valid token to verify, got: %v", err)
	}
	if scope.Ns != "nsclient-alpha" {
		t.Errorf("ns = %q, want nsclient-alpha", scope.Ns)
	}
	if scope.Sel != "bemade.org/instance=alpha-prod" {
		t.Errorf("sel = %q, want bemade.org/instance=alpha-prod", scope.Sel)
	}
	if scope.Iid != 42 {
		t.Errorf("iid = %d, want 42", scope.Iid)
	}
}

func TestVerifyRejectsTamperedPayload(t *testing.T) {
	now := time.Now()
	tok := mintToken(t, testSecret, validScope(now))

	// Forge a different payload but keep the original signature (mirrors the
	// Odoo test_tampered_payload_fails_verification).
	forged := Scope{
		Exp: now.Add(120 * time.Second).Unix(),
		Iat: now.Unix(),
		Iid: 999999,
		Ns:  "victim-namespace",
		Sel: "bemade.org/instance=victim",
	}
	raw, _ := json.Marshal(forged)
	forgedB64 := base64.RawURLEncoding.EncodeToString(raw)
	// Original signature segment.
	origSig := tok[len(tok)-(len(tok)-indexByte(tok, '.')-1):]
	forgedToken := forgedB64 + "." + origSig

	if _, err := VerifyToken(forgedToken, testSecret, now); err == nil {
		t.Fatal("expected tampered token to be rejected")
	}
}

func TestVerifyRejectsWrongSecret(t *testing.T) {
	now := time.Now()
	tok := mintToken(t, testSecret, validScope(now))

	if _, err := VerifyToken(tok, "not-the-real-secret", now); err == nil {
		t.Fatal("expected wrong-secret verification to fail")
	}
}

func TestVerifyRejectsExpiredToken(t *testing.T) {
	now := time.Now()
	tok := mintToken(t, testSecret, validScope(now))

	// Verifies now ...
	if _, err := VerifyToken(tok, testSecret, now); err != nil {
		t.Fatalf("token should verify at mint time: %v", err)
	}
	// ... but not 200s later (past its 120s window).
	later := now.Add(200 * time.Second)
	if _, err := VerifyToken(tok, testSecret, later); err == nil {
		t.Fatal("expected expired token to be rejected")
	}
}

func TestVerifyRejectsMalformed(t *testing.T) {
	now := time.Now()
	for _, bad := range []string{"", "no-dot", ".", "a.", ".b", "a.b.c"} {
		if _, err := VerifyToken(bad, testSecret, now); err == nil {
			t.Errorf("expected %q to be rejected as malformed", bad)
		}
	}
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
