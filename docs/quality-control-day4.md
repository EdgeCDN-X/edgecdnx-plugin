# Day 4 quality control evidence

This document records the reproducible evidence collected for the Day 4 EWMA,
outlier ejection, state machine, and progressive recovery milestone. All values
below are lab results from the three-node `kind` topology; they are not
production SLOs or global-CDN claims.

## Reused and implemented boundaries

- Reused: controller-runtime, Prometheus HTTP API, the official NGINX exporter,
  `prometheus-nginxlog-exporter`, Toxiproxy, Kubernetes CRDs, and EdgeCDN-X
  `Location.fallbackLocations`.
- Implemented here: sample validation, EWMA composition, failure-type handling,
  per-Location ejection safety, persistent cooldown/backoff, stale-data rules,
  the seven-state machine, bounded multiplicative weights, and recovery ramps.

The Prometheus expressions are supplied through `--query-*` flags. The state
engine contains no exporter metric names. All other lab defaults and their
flags are listed in the root README.

## Automated verification

Executed against the final source on 2026-08-28:

```text
go test ./...                 PASS
go vet ./...                  PASS
go test -cover ./internal/quality
                              PASS, 69.3% statement coverage
go test -race ./...           PASS in golang:1.25.0-bookworm
kubectl apply --server-side --dry-run=server -f config/crd/
                              PASS
```

The tests cover first/stable/spike/recovery EWMA samples, alpha boundaries,
NaN/Inf/negative rejection, fourth-versus-fifth failure, low-volume error
windows, maximum ejection percentage, cooldown backoff and cap, restart
persistence, stale/hard-stale safety, interrupted recovery, every recovery
weight boundary, and Prometheus normal/empty/partial/timeout/invalid responses.
Status writes use bounded `RetryOnConflict`; status-only watch events are
filtered so the 5-second requeue is the single refresh clock.

## Live fault evidence

The HLS data plane passed playlist, segment, and MISS-to-HIT checks before and
after fault injection. A Toxiproxy downstream `reset_peer` toxic was applied to
one edge at a time; unique query strings forced origin access so cached objects
could not hide failures.

Observed first Sydney fault:

```text
edge-syd-a  Ejected  effectiveWeight=0
errorEWMA=0.0984375  ejectionCount=1
reason="outlier threshold reached"
```

The Controller Deployment was restarted during the cooldown. The status was
identical before and after restart:

```text
1|2026-08-27T14:02:29Z|Ejected
CONTROLLER_RESTART_PERSISTENCE=PASS
```

The final deployment also enables controller-runtime leader election. Two
successive rollouts moved the `quality-controller.adaptive.edgecdnx.io` Lease
between Pod identities (final `leaseTransitions=3`). Each replacement started workers
only after `Successfully acquired lease`; logs contained no RBAC, conflict, or
controller errors after the minimal Lease and Event permissions were applied.

After repeating the failure with an existing 502 metric series, the first node
was Ejected and the second-node ejection was blocked by the 50% Location limit:

```text
edge-syd-a  Ejected   effectiveWeight=0  ejectionCount=3
edge-syd-b  Degraded  effectiveWeight=2
reason="ejection blocked by maximum percentage"
```

The controller read the existing Sydney Location and logged
`fallbackAvailable=true`. Its bounded-cardinality metric recorded exactly one
event for the transition:

```text
quality_controller_ejection_overflow_total{location="sydney",node="edge-syd-b"} 1
```

Both toxics were deleted and the Toxiproxy API returned `"toxics":[]` for each
proxy.

## Live recovery evidence

Healthy traffic was kept above the 50-request minimum. While latency EWMA was
still outside the lab threshold, every unhealthy window reset `stateSince` and
the node remained at recovery step 0. Once continuously healthy, the observed
timeline was:

```text
approximately  0s  Recovering  step=0  10% state factor
approximately 35s  Recovering  step=1  25% state factor
approximately 68s  Recovering  step=2  50% state factor
approximately120s  Healthy     step=3  100% state factor
```

`TestRecoveryPublishesEveryWeightStep` verifies that each boundary bypasses
steady-state minimum-delta suppression and publishes a strictly increasing
effective weight. The final deployed image then converged all three real
NodeQuality objects to Healthy:

```text
edge-sin-a  Healthy  effectiveWeight=10  sampleCount=86
edge-syd-a  Healthy  effectiveWeight=10  sampleCount=87
edge-syd-b  Healthy  effectiveWeight=10  sampleCount=334
```

Absolute weights are lower than the state-factor percentages because the
documented formula also multiplies latency, reliability, and headroom factors.
Only relative behavior in this local lab is evidence.

## Prometheus series caveat

Prometheus `increase()` returns zero when a newly created label series has only
one scrape sample. The first `status="502"` series therefore required a second
scrape/increment before it became a valid rate signal. The test was repeated
across an existing series; no special-case or fabricated metric was added to
the controller.
