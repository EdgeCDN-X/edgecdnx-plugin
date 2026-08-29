# Environment and baseline

## Pinned toolchain

- Go 1.25.0, matching `go.mod`
- CoreDNS 1.14.2, matching `Makefile`
- kind 0.33.0
- Kubernetes node image `kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`
- Helm 3.21.4
- Windows amd64 local host
- Upstream commits listed in `UPSTREAM.md`

The local Go archive came from the official Go download site and was verified against SHA-256:

```text
go1.25.0.windows-amd64.zip
89efb4f9b30812eee083cc1770fdd2913c14d301064f6454851428f9707d190b
```

The portable toolchain lives under `.tools/` and is intentionally ignored by Git.

## Reproduce the upstream baseline

PowerShell:

```powershell
New-Item -ItemType Directory -Force .tmp/gomodcache,.tmp/gocache,.tmp/temp | Out-Null
$env:GOMODCACHE = (Resolve-Path .tmp/gomodcache).Path
$env:GOCACHE = (Resolve-Path .tmp/gocache).Path
$env:TEMP = (Resolve-Path .tmp/temp).Path
$env:TMP = $env:TEMP
& ./.tools/go/bin/go.exe test ./...
& ./.tools/go/bin/go.exe build ./...
```

Observed at the pinned upstream commit:

```text
go test ./...  exit 0; no upstream test files
go build ./... exit 0
```

## Verified local infrastructure

- Docker Engine 29.0.1 is running through Docker Desktop.
- Portable kind and Helm binaries live under `.tools/`; published checksums were verified.
- The `edgeroute` kind cluster has one control-plane and two workers, all `Ready` on Kubernetes 1.36.1.
- Namespaces `edge-system`, `edge-data`, `monitoring`, and `loadtest` exist.
- The five pinned EdgeCDN-X CRDs are installed from `edgecdnx-controller/config/crd`.
- Host `make` is not required: the upstream Makefile is executed inside the pinned `golang:1.25.0-bookworm` build container with `patch` installed.

The baseline image is `edgeroute-coredns:upstream-1dd32f2` with local image ID:

```text
sha256:6aeb4f5d8f8d49c9b8dc9a804956be9f0155c525afc561bacb3322ef4f9ec917
```

Observed runtime evidence:

```text
CoreDNS-1.14.2-edgecdnx-upstream-1dd32f2
linux/amd64, go1.25.0
plugin list contains: edgecdnx
```

The image has been loaded into all three kind nodes. This is a local reproducibility baseline, not evidence of production or cross-region deployment.
