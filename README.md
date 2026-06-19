# odoo-herd-log-sidecar

In-cluster log-stream sidecar for the Odoo Herd portal's **Feature C** live log
viewer. It accepts a short-lived, HMAC-signed scope token minted by Odoo,
verifies it, and streams the merged logs of the pods that the token authorises.

## STATUS — read this first

**This is a SCAFFOLD / DESIGN deliverable. It has NOT been verified against a
cluster. Do not deploy as-is.**

- It needs **human review**, a **real git repo/remote** (none configured), and
  **cluster validation** before it is anything more than a design artifact.
- **Real and tested:** the token verifier (`token.go`) — it implements the Odoo
  mint contract byte-for-byte and is covered by `token_test.go` (`go test ./...`
  passes).
- **Stubbed:** the Kubernetes pod discovery + log multiplex (`kube.go`). The
  in-cluster client, scope-driven pod `List`, `Follow:true` log streams, the
  fan-in channel and per-line flush are wired, but the **pod informer /
  attach-detach lifecycle is `TODO`** and nothing has been run against a live
  cluster.
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
   derives scope **only** from the verified payload, lists matching pods, tails
   their logs (`Follow:true`), multiplexes them, and streams merged lines back.

## Configuration

| Env var            | Required | Description                                                                 |
| ------------------ | -------- | --------------------------------------------------------------------------- |
| `LOG_TOKEN_SECRET` | yes      | 64-hex shared HMAC secret; MUST equal Odoo's `odoo_herd_portal.log_token_secret`. Mounted from the k8s `Secret` (`deploy/deployment.yaml`). |
| `LISTEN_ADDR`      | no       | Listen address (default `:8080`).                                           |

## Build & test

```sh
go test ./...        # token verifier tests
go build ./...
docker build -t odoo-herd-log-sidecar:dev .
```

## Deploy (after review + cluster validation)

Manifests in `deploy/` (namespace `odoo-herd`):

- `rbac.yaml` — least-privilege `ServiceAccount` + `ClusterRole`
  (`get,list,watch` pods, `get` pods/log) + `ClusterRoleBinding`. No exec, no
  secrets, no kubeconfig.
- `deployment.yaml` — `Deployment` + the shared-secret `Secret`
  (`log-sidecar-token-secret`, placeholder value — populate out of band).
- `service.yaml` — `Service`.
- `ingress.yaml` — Traefik + cert-manager TLS, host `logs.bemade.org`.

## Layout

```
.
├── main.go          # HTTP server: /healthz, /stream (token verify + stream)
├── token.go         # HMAC-SHA256 scope-token verifier (REAL, contract-exact)
├── token_test.go    # mints tokens as Odoo does; valid/tampered/expired cases
├── kube.go          # pod discovery + log multiplex (Stern pattern, STUBBED)
├── go.mod / go.sum
├── Dockerfile       # multi-stage, distroless/nonroot static build
└── deploy/
    ├── rbac.yaml
    ├── deployment.yaml
    ├── service.yaml
    └── ingress.yaml
```
