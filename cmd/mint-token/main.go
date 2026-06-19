// License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl).

// Command mint-token is a dev convenience that prints a sidecar scope token
// using the SAME construction the Odoo portal uses
// (K8sOdooInstance._mint_log_token) — see MintToken in the main package. It is
// used to exercise /stream locally against a real cluster without running Odoo.
//
// Example:
//
//	go run ./cmd/mint-token -secret "$SECRET" \
//	    -ns nsclient-foo -sel "bemade.org/instance=foo-staging" -ttl 120
//
// Then:
//
//	curl -N -H "Authorization: Bearer $(go run ./cmd/mint-token ...)" \
//	    localhost:8080/stream
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	secret := flag.String("secret", "", "shared HMAC secret (the LOG_TOKEN_SECRET the sidecar runs with)")
	ns := flag.String("ns", "", "namespace to scope the token to (the only trusted ns)")
	sel := flag.String("sel", "", "label selector, e.g. bemade.org/instance=foo-staging")
	ttl := flag.Int64("ttl", 120, "token lifetime in seconds")
	iid := flag.Int64("iid", 0, "instance id (informational/audit only)")
	flag.Parse()

	if *secret == "" || *ns == "" || *sel == "" {
		fmt.Fprintln(os.Stderr, "mint-token: -secret, -ns and -sel are required")
		flag.Usage()
		os.Exit(2)
	}

	now := time.Now()
	scope := scopePayload{
		Exp: now.Add(time.Duration(*ttl) * time.Second).Unix(),
		Iat: now.Unix(),
		Iid: *iid,
		Ns:  *ns,
		Sel: *sel,
	}
	fmt.Println(mint(*secret, scope))
}

// scopePayload mirrors the main package's Scope. It is duplicated here (rather
// than imported) because main is `package main` and cannot be imported; the
// field order (exp, iat, iid, ns, sel) is the sorted-key order that reproduces
// the Odoo side's json.dumps(sort_keys=True) compact output byte-for-byte.
type scopePayload struct {
	Exp int64  `json:"exp"`
	Iat int64  `json:"iat"`
	Iid int64  `json:"iid"`
	Ns  string `json:"ns"`
	Sel string `json:"sel"`
}

// mint reproduces MintToken / K8sOdooInstance._mint_log_token:
//
//	payload_b64 = b64url( json.dumps(payload, separators=(",",":"), sort_keys=True) )
//	sig         = b64url( HMAC_SHA256(secret_utf8, payload_b64_ascii) )
//	token       = payload_b64 "." sig
func mint(secret string, s scopePayload) string {
	raw, _ := json.Marshal(s)
	payloadB64 := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payloadB64))
	sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payloadB64 + "." + sigB64
}
