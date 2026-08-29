# Failure analysis

## Evidence boundary

The recorded Day 6 dataset is a local smoke test: 2–5 k6 VUs for 18 seconds, three repetitions per policy/scenario, on an Intel i5-12400F. It validates fault handling, experiment identity, and telemetry flow; it is not evidence of production throughput, global routing, or an SLO.

The final matrix contains 36/36 accepted runs. The independent audit found 36 unique run IDs, Job UIDs, and Pods; zero ID mismatches; zero non-zero k6 exits; zero missing P99 values; and zero empty Prometheus responses (6–12 series per run). All runs use commit `8794f2bc354abdc6c54521a37f9d099ac375e371` and CoreDNS image ID `sha256:67702937d3c2610d224b5af5a8dc44d366c3ec817e589f58b8ac9bb073c013fb`.

## Observed fault behavior

| Fault | Deterministic | Static rendezvous | Adaptive | Interpretation |
|---|---:|---:|---:|---|
| 150 ms + 20 ms jitter | 0.00% session failures | 0.00% | 0.18% | All paths served traffic, but adaptive had high run-to-run latency spread and was not the best policy in this smoke sample. |
| connection reset | 56.18% | 0.00% | 0.00% | Active-health filtering/fallback removed session failures for both rendezvous modes; this does not isolate the quality controller as the sole cause. |
| cache Pod down | 55.52% | 0.00% | 0.00% | Both rendezvous controls preserved sessions after the selected node disappeared. |
| cold-cache Pod replacement | 2.05% | 0.00% | 0.00% | Sessions recovered, but adaptive segment P95 was 170.40 ms versus 4.56 ms baseline; safe recovery behavior is not the same as lower latency. |

The three-policy control is essential. `static-rendezvous` keeps active health and fallback but ignores NodeQuality, so improvements shared by static and adaptive cannot honestly be credited only to telemetry-driven weighting.

## Component failure behavior

### Prometheus unavailable or samples stale

The provider returns an error/empty-sample outcome to the controller; it never appears in `ServeDNS`. The controller retains bounded last-known-good state, then marks the object `Stale` and later `HardStale` according to lab timers. Positive stale weights remain eligible; `Disabled` and `Ejected` do not fail open. After the final matrix, two idle nodes became `Stale` because the experiment had stopped generating node samples while all Deployments remained Ready—expected stale-data behavior, not a controller crash.

Risk: a last-known-good weight can lag a sudden failure. Active-health filtering and geographic fallback are independent safeguards, but the system has no proof that every stale node is reachable.

### Quality Controller unavailable

CoreDNS continues using the last published snapshot and does not wait on the controller. Cooldown, ejection count, state timestamp, and recovery step live in NodeQuality status; the Day 4 restart experiment showed that an Ejected node remained Ejected after controller restart.

Risk: no fresh score or recovery transition occurs while the controller is down. Lease leader election protects against double writers, but this lab has one active replica and one Kubernetes cluster.

### Node failure or connection reset

Active-health/alert filters and NodeQuality candidate state run before selection. If a node becomes unavailable, Weighted Rendezvous remaps only keys that selected the removed node; surviving-node assignments remain stable. The recorded disconnect and Pod-down tests had 0% session failures under static and adaptive modes, versus 56.18% and 55.52% for deterministic.

Risk: DNS TTL means already-resolved clients may continue to use an old address. The test uses a 1-second k6 DNS TTL in smoke mode to expose routing changes quickly; the deployed lab answer TTL is 30 seconds.

### Multiple failures in one Location

The controller enforces a 50% maximum ejection per Location and checks fallback availability. In the controlled Day 4 reset-peer experiment, one Sydney node was Ejected and the second was protected by the maximum-ejection limit.

Risk: safety against total ejection deliberately trades correctness for capacity: a degraded node can remain eligible. A real deployment would need capacity-aware regional admission and better active probing.

### Cold-cache recovery

After cooldown and healthy samples, a node moves to `Recovering` and receives bounded 10%/25%/50%/100% weight steps. This limits the control-plane rate increase while NGINX repopulates playlist/segment objects.

Risk: the smoke data did not demonstrate lower cold-cache latency or origin bandwidth. Adaptive cold-cache P95 was materially worse than baseline in this sample, so no “improved cache efficiency” claim is made.

### Kubernetes API or informer interruption

DNS goroutines keep reading the last immutable snapshot. Initial readiness remains false until required informers sync. NodeQuality is optional at startup: if its CRD is missing, the plugin logs a warning and uses `defaultweight`; upstream Service/Location/PrefixList/Zone objects are still required for normal EdgeCDN-X routing.

Risk: the local control plane is not highly available, and status replication across clusters is not implemented.

## Experiment-integrity incidents and fixes

Two early Day 6 batches were rejected rather than used:

1. The runner selected `.items[0]` from a shared Pod label, so an old Pod could be copied into a new run directory. Fifteen of 24 detail files had the wrong run ID. The runner now selects by the current Job controller UID, requires exactly one Pod, verifies its `RUN_ID`, records Job UID/Pod/exit code, and the processor rejects any identity mismatch.
2. The Prometheus capture queried `nginx_http...` while the deployed log exporter exposes `nginxlog_http...`. The first corrected batch still had empty telemetry and was rejected. The query and strict processor now require successful, non-empty response/cache series.

Rejected local batches are quarantined and are not part of the published evidence. This is why “containers started” or “k6 exited” alone is not treated as experiment success.

## Unresolved design problems

1. A DNS control plane cannot see player buffer, bitrate switching, HLS path/video popularity, or a pinned session; it cannot optimize true per-stream QoE by itself.
2. Static lab thresholds are not calibrated for heterogeneous networks, workloads, or capacity. Automated safe parameter tuning is not implemented.
3. The single-cluster NodeQuality status model has no cross-region consensus, conflict resolution, or disaster-recovery design.
4. The 20→100 VU full profile and real multi-region network have not been recorded, so high-concurrency and global-scale claims are unsupported.
5. Recovery weight limits DNS selection only; it does not directly cap origin fetch bandwidth or coordinate cache prefetch.
