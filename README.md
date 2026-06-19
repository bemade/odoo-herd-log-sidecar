# odoo-herd-log-sidecar

In-cluster log-stream sidecar for the Odoo Herd portal's **Feature C** live log
viewer. It accepts a short-lived, HMAC-signed scope token minted by Odoo,
verifies it, and streams the merged logs of the pods that the token authorises.

Licensed under **LGPL-3.0-or-later** (see `LICENSE`). Author: Bemade Inc.

## STATUS — read this first

**Functional, but NOT yet verified against a live cluster. Do not deploy until
the live-streaming path is validated on-cluster.**

- It needs **cluster validation** of the live `Follow` multiplex and a
  **deploy** decision before production use.
- **Real and tested:**
  - the token verifier (`token.go`) — implements the Odoo mint contract
    byte-for-byte, covered by `token_test.go`;
  - the `/stream` auth gate (`main.go`) — missing/malformed/bad-sig/expired
    tokens are rejected with 401 **before any kube call**; a valid token reaches
    the kube path with scope taken **only** from the verified payload
    (`main_test.go`);
  - pod **discovery** scoping — ns + label-selector filtering returns exactly
    the matching pods and excludes other instances / other namespaces, tested
    with `k8s.io/client-go/kubernetes/fake` (`kube_test.go`);
  - the Stern attach/detach bookkeeping (idempotent attach, non-running pods
    skipped, detach forgets) (`kube_test.go`).
- **Implemented but only verifiable on a real cluster:** the live `Follow:true`
  log multiplex itself — per-container streams, the fan-in channel, per-line
  flush, the informer add/update/delete attach-detach lifecycle, and per-pod
  retry/backoff. The fake clientset does **not** implement the `pods/log`
  subresource, so this path cannot be exercised headless.
- The **react-logviewer SPA** front-end (served from `logs.bemade.org`) is
  **described but not built**.

The authoritative design + token contract is in the Odoo module at
`odoo_herd_portal/docs/SIDECAR.md`.

## How it works (summary)

1. Browser hits Odoo `/my/instances/<id>/logs`; Odoo checks ownership and mints
   a ~120 s token whose payload carries the namespace (`ns`) and label selector
   (`sel`, `bemade.org/instance=<name>`).
2. The Odoo page hands the token to the iframe SPA via `postMessage` (never in a
   URL).
3. The SPA opens `fetch("/stream", {headers:{Authorization:"Bearer <token>"}})`.
4. The sidecar verifies the token (HMAC-SHA256, constant-time, expiry), then
   derives scope **only** from the verified payload, watches matching pods via a
   SharedInformer, tails their logs (`Follow:true`, `TailLines:200`),
   multiplexes them, and streams merged lines back — attaching new pods
   (rollouts, `-cron`, transient Job pods) and detaching deleted ones (Stern
   pattern). Client disconnect (request-context cancel) tears down the informer
   and every per-pod stream.

## Wire format

`/stream` responds with `Content-Type: application/x-ndjson` — a single
long-lived HTTP response whose body is **newline-delimited JSON**: one JSON
object per line.

Log line:

```json
{"pod":"alpha-prod-web-abc","container":"odoo","line":"...","ts":"2026-01-02T15:04:05.123456789Z"}
```

Heartbeat (emitted every ~20 s to keep idle connections / proxies alive):

```json
{"heartbeat":true,"ts":"2026-01-02T15:04:25Z"}
```

`ts` is the sidecar's receive time (RFC3339Nano). NDJSON is chosen over SSE
because the SPA opens the stream with `fetch()` + a `ReadableStream` reader so
the token can ride in an `Authorization: Bearer` header (EventSource/SSE cannot
set arbitrary headers). See `odoo_herd_portal/docs/SIDECAR.md` §5.

## Configuration

| Env var            | Required | Description                                                                 |
| ------------------ | -------- | --------------------------------------------------------------------------- |
| `LOG_TOKEN_SECRET` | yes      | 64-hex shared HMAC secret; MUST equal Odoo's `odoo_herd_portal.log_token_secret`. Mounted from the k8s `Secret` (`deploy/deployment.yaml`). |
| `LISTEN_ADDR`      | no       | Listen address (default `:8080`).                                           |

## Build & test

```sh
go test ./...        # token verifier + handler-auth + pod-discovery tests
go build ./...
go vet ./...
docker build -t odoo-herd-log-sidecar:dev .
```

## Run locally against a cluster (KUBECONFIG fallback)

The sidecar uses `rest.InClusterConfig()` in production and falls back to
`KUBECONFIG` (or `~/.kube/config`) when run off-cluster, so you can point it at a
real cluster from your laptop. You need the **same** 64-hex secret Odoo holds in
`ir.config_parameter` under `odoo_herd_portal.log_token_secret`, and a token
minted by Odoo for an instance you can reach.

```sh
# 1. Run the sidecar (uses ~/.kube/config; current kube context = target cluster)
LOG_TOKEN_SECRET=<64-hex secret> go run .
#   …or with an explicit kubeconfig / listen addr:
# KUBECONFIG=/path/to/kubeconfig LISTEN_ADDR=:8080 LOG_TOKEN_SECRET=<secret> go run .

# 2. Mint a token from Odoo (portal page does this via _mint_log_token), then:
curl -N -H "Authorization: Bearer <token>" http://localhost:8080/stream
#   -N disables curl buffering so you see lines as they stream (NDJSON).
```

Off-cluster, your kubeconfig identity must have `get,list,watch` on pods and
`get` on `pods/log` in the target namespace (in production the sidecar's own
least-privilege ServiceAccount provides exactly this — see `deploy/rbac.yaml`).

## Deploy (after review + cluster validation)

The container image is published to `ghcr.io/bemade/odoo-herd-log-sidecar` by the
release-please workflow (multi-arch, push-by-digest). The production Kubernetes
manifests live in the cluster GitOps repo under **`kube-gitops/log-sidecar/`** —
that is the source of truth applied to the cluster.

The `deploy/` directory in this repo holds reference copies of those manifests
(namespace `odoo-herd`):

- `rbac.yaml` — least-privilege `ServiceAccount` + `ClusterRole`
  (`get,list,watch` pods, `get` pods/log) + `ClusterRoleBinding`. No exec, no
  secrets, no kubeconfig.
- `deployment.yaml` — `Deployment` (image `ghcr.io/bemade/odoo-herd-log-sidecar:latest`)
  + the shared-secret `Secret` (`log-sidecar-token-secret`, **placeholder value
  only** — the real 64-hex secret is populated out of band via
  sealed-secret / external-secrets, never committed here).
- `service.yaml` — `Service`.
- `ingress.yaml` — Traefik + cert-manager TLS, host `logs.bemade.org`.

## Layout

```
.
├── main.go          # HTTP server: /healthz, /stream (token verify + stream)
├── main_test.go     # /stream auth gate: reject before kube; scope from token only
├── token.go         # HMAC-SHA256 scope-token verifier (REAL, contract-exact)
├── token_test.go    # mints tokens as Odoo does; valid/tampered/expired cases
├── kube.go          # pod discovery + Follow log multiplex (Stern pattern)
├── kube_test.go     # pod-discovery scoping + attach/detach (fake clientset)
├── go.mod / go.sum
├── Dockerfile       # multi-stage, distroless/nonroot static build
├── LICENSE          # LGPL-3.0
├── release-please-config.json / .release-please-manifest.json
├── .github/workflows/
│   ├── build.yml          # CI: gofmt + build + vet + test + golangci-lint
│   └── release-please.yml # release-please → multi-arch image push-by-digest to GHCR
└── deploy/
    ├── rbac.yaml
    ├── deployment.yaml
    ├── service.yaml
    └── ingress.yaml
```
