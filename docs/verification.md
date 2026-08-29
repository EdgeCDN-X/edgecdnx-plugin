# Verification record

Last updated: 2026-08-29 (Australia/Sydney).

## Passed locally

- `go test ./...`
- `go vet ./...`
- Linux/amd64 `go test -race ./...` in `golang:1.25.0-bookworm`
- `FuzzBuildRoutingKeyStable`, 10 seconds, 696,984 executions
- `FuzzWeightedRendezvousNeverSelectsInvalidCandidate`, 10 seconds, 3,910,262 executions
- PowerShell parser checks for the experiment runner, HLS verifier, e2e runner, and demo script
- GNU Make dry-run expansion for all documented targets
- Offline parsing of every Kubernetes YAML document under `config/`, `deploy/`, and `experiments/k6/`; the MediaMTX and Helm values application configs are explicitly excluded
- Final CoreDNS and Quality Controller `linux/amd64` image builds after excluding experiment evidence from the Docker context
- Day 6 three-policy smoke matrix: 36/36 runs accepted by the strict processor
- Repeated Day 6 processing: identical SHA-256 for `runs.csv`, `summary.csv`, `report.md`, and `policy-comparison.png`
- Post-experiment cluster check: CoreDNS 2/2 Ready, Quality Controller 1/1 Ready, all three edge Deployments Ready, Corefile restored to `routingmode adaptive`
- Final server-side deployment with `--force-conflicts`: the committed Corefile and `:dev` image fields successfully took ownership back from the experiment runner
- `scripts/e2e-smoke.ps1`: PASS with three `MISS -> HIT` cache checks, unique Job UID/Pod, 0 HLS session failures, non-empty Prometheus telemetry, fault recovery, and adaptive-mode restoration
- `scripts/demo.ps1`: PASS without hand-edited manifests; temporary evidence and all three NodeQuality capture stages were written under `.tmp/`

## Release gate completed

- The final e2e/HLS/CI fixes are committed in `3ea8f0a`; the generated CoreDNS binary records the full commit SHA, `linux/amd64`, and `CGO_ENABLED=0`.
- The e2e and demo workflows passed after their final script fixes, against the same production plugin and controller sources contained in `3ea8f0a`.
- GitHub Actions run `33235078112` passed both jobs: patched CoreDNS plus runtime-image builds, and formatting, vet, unit, race, fuzz-smoke, and offline manifest checks.
- The release tag is `edgeroute-v0.1.0`. The inherited upstream `v0.1.0` tag already points to `8d74b93` and is deliberately not overwritten.

Final validation found and corrected three automation defects rather than accepting partial success:

1. The Day 6 runner had temporarily patched Corefile/image fields, causing a later server-side apply conflict. `make deploy` now deliberately takes ownership of repository-declared fields with `--force-conflicts`; the final apply and rollouts passed.
2. The e2e recovery assertion applied `-notmatch` to a PowerShell array of Corefile lines, so non-matching lines produced a false failure even though adaptive mode was restored. It now joins the complete Corefile before matching.
3. The HLS verifier selected the oldest live-playlist segment and could race MediaMTX window eviction. It now selects the latest complete segment, matching the k6 client; all three caches then passed `MISS -> HIT` and the complete demo passed.

The first uploaded CI run also failed because `kubectl apply --dry-run=client` still attempted OpenAPI discovery without a cluster. The corrected workflow uses a repository Go test and Kubernetes' YAML decoder for truly offline manifest parsing. The corrected remote run passed as GitHub Actions run `33235078112`.
