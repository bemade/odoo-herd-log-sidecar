// License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl).

package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Scope is the verified token payload. ONLY Ns and Sel are trusted for
// scoping pod discovery; Iid is informational/audit only.
type Scope struct {
	Exp int64  `json:"exp"`
	Iat int64  `json:"iat"`
	Iid int64  `json:"iid"`
	Ns  string `json:"ns"`
	Sel string `json:"sel"`
}

var (
	errMalformed = errors.New("malformed token")
	errBadSig    = errors.New("bad signature")
	errExpired   = errors.New("expired")
)

// b64urlDecode decodes an unpadded URL-safe base64 string, re-padding to a
// multiple of 4 first. Mirrors the Odoo side's `_b64url_decode`:
//
//	pad = "=" * (-len(segment) % 4)
func b64urlDecode(segment string) ([]byte, error) {
	// (-len % 4) in Go: compute the non-negative padding length.
	if pad := (4 - len(segment)%4) % 4; pad != 0 {
		segment += strings.Repeat("=", pad)
	}
	return base64.URLEncoding.DecodeString(segment)
}

// MintToken constructs a sidecar scope token EXACTLY as the Odoo side does
// (K8sOdooInstance._mint_log_token):
//
//	payload_b64 = b64url( json.dumps(payload, separators=(",",":"), sort_keys=True) )
//	sig         = b64url( HMAC_SHA256(secret_utf8, payload_b64_ascii) )
//	token       = payload_b64 "." sig
//
// Go's encoding/json marshals struct fields in declaration order, and the Scope
// fields are declared in sorted key order (exp, iat, iid, ns, sel) with compact
// separators by default — reproducing Python's sort_keys=True compact output
// byte-for-byte. This is the single source of truth shared by the unit tests
// and the cmd/mint-token dev CLI.
func MintToken(secret string, s Scope) string {
	raw, _ := json.Marshal(s)
	payloadB64 := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payloadB64))
	sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payloadB64 + "." + sigB64
}

// VerifyToken validates a sidecar scope token against the shared secret and
// returns the decoded payload. It implements, byte-for-byte, the contract minted
// by K8sOdooInstance._mint_log_token and verified by the reference _verify_token
// in tests/test_log_viewer.py:
//
//	token = b64url(payload_json) "." b64url(HMAC_SHA256(secret, b64url_payload_string))
//
// The HMAC is computed over the ASCII bytes of the b64url payload STRING (the
// first token segment), NOT over the raw payload JSON. `secret` is the UTF-8
// bytes of the 64-hex-char shared string.
//
// Verification: split on the first ".", recompute the HMAC, constant-time
// compare, b64url-decode + JSON-parse the payload, reject if exp < now. The
// caller MUST trust only Ns and Sel from the result for scoping.
func VerifyToken(token, secret string, now time.Time) (*Scope, error) {
	// Exactly one separator: split on the first "." and ensure no extra ".".
	dot := strings.IndexByte(token, '.')
	if dot < 0 {
		return nil, errMalformed
	}
	payloadB64 := token[:dot]
	sigB64 := token[dot+1:]
	if payloadB64 == "" || sigB64 == "" || strings.ContainsRune(sigB64, '.') {
		return nil, errMalformed
	}

	// Recompute HMAC over the ASCII bytes of the b64url payload string.
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payloadB64))
	expected := mac.Sum(nil)

	got, err := b64urlDecode(sigB64)
	if err != nil {
		return nil, errMalformed
	}
	// Constant-time compare.
	if !hmac.Equal(expected, got) {
		return nil, errBadSig
	}

	payloadJSON, err := b64urlDecode(payloadB64)
	if err != nil {
		return nil, errMalformed
	}
	var scope Scope
	if err := json.Unmarshal(payloadJSON, &scope); err != nil {
		return nil, fmt.Errorf("%w: %v", errMalformed, err)
	}

	if scope.Exp < now.Unix() {
		return nil, errExpired
	}
	if scope.Ns == "" || scope.Sel == "" {
		return nil, fmt.Errorf("%w: missing ns/sel", errMalformed)
	}
	return &scope, nil
}
