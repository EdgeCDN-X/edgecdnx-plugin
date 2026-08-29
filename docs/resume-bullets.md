# Evidence-backed resume bullets

Project title:

```text
EdgeRoute — QoE-Aware Traffic Steering for Distributed Edge Caches
```

Technology:

```text
Go, CoreDNS, Kubernetes, Prometheus, NGINX, MediaMTX, Toxiproxy, k6, Docker, Helm
```

Bullets supported by the committed repository and recorded results:

```text
• Extended EdgeCDN-X's Go/CoreDNS plugin with a Kubernetes NodeQuality
  controller that converts Prometheus latency, error, cache, origin and
  capacity telemetry into bounded per-node routing weights outside the DNS
  request path.

• Implemented EWMA smoothing, consecutive/error-rate outlier ejection,
  per-location ejection limits, last-known-good stale handling, weighted
  rendezvous selection and staged 10%/25%/50%/100% cold-cache recovery while
  preserving upstream geographic fallback.

• Built a reproducible HLS fault lab from MediaMTX, three NGINX caches,
  Toxiproxy and k6; in a 36-run smoke matrix, reduced HLS session failures
  from 56.18% to 0% during connection reset and from 55.52% to 0% during cache
  Pod outage, with three repetitions per policy/scenario.

• Added unit, race, fuzz, distribution and end-to-end checks plus strict
  Git/image/Job/Pod/run identity validation; measured 512-candidate weighted
  selection at 35.51 us/op with 0 request-path allocations on an Intel
  i5-12400F Windows/amd64 lab host.
```

Traceability:

| Claim | Evidence |
|---|---|
| 36 runs and three repetitions | `experiments/results/processed/report.md`, `runs.csv` |
| 56.18% → 0% disconnect | generated report, raw `*/disconnect-*/k6-summary.json` |
| 55.52% → 0% Pod outage | generated report, raw `*/pod-down-*/k6-summary.json` |
| 512 candidates, 35.51 us/op, 0 alloc | `docs/algorithm.md`, `internal/routing/rendezvous_benchmark_test.go` |
| unit/race/fuzz/e2e | test sources, CI workflow, and Day 7 verification record |

Do not rewrite the bullets as “built a global CDN”, “production SLO”, “100 VU performance improvement”, “AI scheduling”, or “improved cache efficiency”. The experiment is a one-workstation smoke profile; the static-rendezvous control shows that active-health/fallback and hash-family effects share credit for the observed fault improvements.
