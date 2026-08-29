# Five-minute demonstration

Use the automated script so the fault and recovery do not depend on hand-edited manifests:

```powershell
pwsh -NoProfile -File scripts/demo.ps1
```

Prerequisites are the deployed Quick Start environment and local image `edgeroute-coredns:dev`. The script always uses the runner’s `finally` recovery path and stores its one-run evidence under ignored `.tmp/` instead of changing the published 36-run dataset.

## 0:00–0:45 — architecture

Show `docs/architecture.md`:

- EdgeCDN-X supplies Service/Location/prefix/Geo and fallback routing.
- EdgeRoute adds the NodeQuality Controller, immutable snapshot, three policy modes, and safety state machine.
- MediaMTX, three NGINX caches, Prometheus, Toxiproxy, and k6 are pinned mature modules.

## 0:45–1:30 — normal path

The script prints NodeQuality objects and runs `scripts/verify-hls.ps1`. Point out the real MediaMTX `/live/demo` stream and `MISS -> HIT` transition at all three caches. A node can be `Stale` when idle; explain that stale telemetry is an explicit state and does not mean its Pod is down.

## 1:30–2:15 — inject latency

The e2e runner adds 150 ms latency plus 20 ms jitter only to `edge-syd-a-origin`, verifies the toxic, and runs k6 through the EdgeRoute DNS Service. No manifest is edited by hand.

## 2:15–3:15 — observe control and data evidence

The runner captures NodeQuality before injection, after injection, and at run completion. It also captures CoreDNS/controller metrics, k6 summary/detail, Prometheus response/cache series, Toxiproxy state, Job UID, Pod, commit, and image ID.

Explain that short smoke timing may capture `Healthy`, `Degraded`, `Stale`, or `Recovering`; do not promise an Ejection in every 18-second run because the configured sample/cooldown gates are intentionally stricter.

## 3:15–4:15 — recover

The runner removes the toxic, verifies recovery, deletes the k6 Job, restores the NodeQuality baseline and `adaptive` Corefile mode, and waits for both CoreDNS and controller rollouts. Recovery weights use 10%/25%/50%/100% steps in the longer controlled state-machine experiment.

## 4:15–5:00 — recorded result and limits

Show `experiments/results/processed/policy-comparison.png` and `report.md`:

- 36/36 accepted smoke runs with strict execution identity;
- deterministic disconnect and Pod-down failures of 56.18% and 55.52%, versus 0% for both rendezvous modes;
- adaptive is not universally faster and had an adverse cold-cache P95 result;
- one-machine kind, 2–5 VUs, three repetitions, no real player QoE, no production/global claim.
