# Day 6 processed results

Generated from immutable files under `../raw/` by `experiments/process_results.py`.
All 36 runs passed the evidence-file, execution-identity, run-ID, and Prometheus telemetry checks; no run was excluded.
Profile: `smoke`. Commit: `8794f2bc354abdc6c54521a37f9d099ac375e371`. CoreDNS image ID: `sha256:67702937d3c2610d224b5af5a8dc44d366c3ec817e589f58b8ac9bb073c013fb`.
Host CPU: 12th Gen Intel(R) Core(TM) i5-12400F.
Percentages and latency values are measured results for this recorded profile, not production SLO claims.

| Scenario | Variant | Runs | Playlist success | Segment success | Session failures | Segment P50 | Segment P95 | Segment P99 | Stalls |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| latency | baseline | 3 | 100.00% | 100.00% | 0.00% | 1.30 ms | 502.06 ms | 503.78 ms | 0.0 |
| latency | static-rendezvous | 3 | 100.00% | 100.00% | 0.00% | 1.27 ms | 5.55 ms | 378.06 ms | 0.0 |
| latency | adaptive | 3 | 100.00% | 99.82% | 0.18% | 1.29 ms | 171.12 ms | 879.72 ms | 1.7 |
| disconnect | baseline | 3 | 51.44% | 100.00% | 56.18% | 1.40 ms | 5.05 ms | 49.44 ms | 0.0 |
| disconnect | static-rendezvous | 3 | 100.00% | 100.00% | 0.00% | 1.30 ms | 6.31 ms | 391.72 ms | 0.0 |
| disconnect | adaptive | 3 | 100.00% | 100.00% | 0.00% | 1.28 ms | 4.92 ms | 249.78 ms | 0.0 |
| pod-down | baseline | 3 | 60.81% | 100.00% | 55.52% | 1.77 ms | 7.73 ms | 12.25 ms | 0.0 |
| pod-down | static-rendezvous | 3 | 100.00% | 100.00% | 0.00% | 1.31 ms | 5.56 ms | 502.02 ms | 0.0 |
| pod-down | adaptive | 3 | 100.00% | 100.00% | 0.00% | 1.37 ms | 5.59 ms | 502.30 ms | 0.0 |
| cold-cache | baseline | 3 | 98.96% | 100.00% | 2.05% | 1.28 ms | 4.56 ms | 135.47 ms | 0.0 |
| cold-cache | static-rendezvous | 3 | 100.00% | 100.00% | 0.00% | 1.32 ms | 5.83 ms | 415.43 ms | 0.0 |
| cold-cache | adaptive | 3 | 100.00% | 100.00% | 0.00% | 1.27 ms | 170.40 ms | 400.82 ms | 0.0 |

## Policy effect versus deterministic baseline

Negative values are improvements for both failure-rate delta and latency change.

| Scenario | Variant | Session failure delta | Segment P95 change |
|---|---|---:|---:|
| latency | static-rendezvous | +0.00 pp | -98.89% |
| latency | adaptive | +0.18 pp | -65.92% |
| disconnect | static-rendezvous | -56.18 pp | +24.95% |
| disconnect | adaptive | -56.18 pp | -2.60% |
| pod-down | static-rendezvous | -55.52 pp | -28.07% |
| pod-down | adaptive | -55.52 pp | -27.64% |
| cold-cache | static-rendezvous | -2.05 pp | +27.82% |
| cold-cache | adaptive | -2.05 pp | +3635.93% |

![Three-policy comparison](policy-comparison.png)

## Interpretation limits

- The `smoke` profile validates the pipeline only (2-5 VUs for 18 seconds); it is not a performance result.
- The static-rendezvous control isolates the hash-family change: it keeps active-health filtering and fallback, assigns equal weights, and does not consume NodeQuality.
- Three repetitions expose run-to-run spread but are insufficient for production SLO or global-scale claims.
- The kind cluster uses logical Sydney/Singapore regions on one workstation, not real cross-region links.
