# --- build stage ---
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
# Static assets embedded via //go:embed web (the viewer SPA) — must be in the
# build context or the embed fails the build.
COPY web ./web
# Static build so it runs in a distroless/scratch base.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/log-sidecar .

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/log-sidecar /usr/local/bin/log-sidecar
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/log-sidecar"]
