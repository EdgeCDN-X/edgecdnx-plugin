# Reproducible Day 6 experiments

This directory implements the manual's k6 HLS and fault-injection stage. It reuses k6 for load generation, Toxiproxy for network faults, Kubernetes Deployments for Pod lifecycle, Prometheus for telemetry, and Matplotlib for result processing. Project code only supplies the CDN request flow, orchestration, metadata capture, and comparison logic.

## Compared variants

- `baseline`: upstream EdgeCDN-X deterministic modulo hash with active-health filtering and geographic fallback.
- `static-rendezvous`: equal-weight Weighted Rendezvous with the same active-health filtering and fallback, while the NodeQuality Controller is disabled. This isolates hash-family effects from quality-aware control.
- `adaptive`: EWMA/outlier state, NodeQuality effective weights, Weighted Rendezvous, ejection, and recovery ramp.

All three variants use the same compiled image. The explicit `routingmode` Corefile setting and controller replica count are the only control changes, which avoids mixing binary or dependency changes into the comparison. The runner creates immutable per-variant tags and records their image IDs.

## Profiles

The default `smoke` profile is intended to validate the complete pipeline on a developer machine: 2 to 5 VUs over 18 seconds. It must not be presented as a performance result.

The `full` profile follows the manual: one-minute warm-up, three minutes at 20 VUs, two-minute ramp to 100 VUs, three minutes at 100 VUs, and two-minute recovery. Hardware and all parameters are recorded for every run.

## Run

Prerequisites are the Day 5 kind testbed, `edgeroute-coredns:dev`, Docker, kubectl, and PowerShell 7. The runner discovers the current `edgeroute-coredns` Service ClusterIP; no cluster-specific DNS address is stored in the Job template.

```powershell
./experiments/run-day6.ps1 -Profile smoke -Repetitions 3
```

The runner always attempts to restore Toxiproxy, `edge-syd-a`, the Quality Controller, and adaptive routing in its `finally` block. Raw outputs are written under `experiments/results/raw/<run_id>/` without post-processing edits.

Process results with a pinned mature plotting dependency:

```powershell
python -m pip install -r experiments/requirements.txt
python experiments/process_results.py
```

This produces `runs.csv`, aggregated `summary.csv`, a Markdown table, and `policy-comparison.png` under `experiments/results/processed/`. Re-running the processor replaces only derived artifacts; it never edits raw run data.

By default, the processor requires the complete 3 variants x 4 scenarios x 3 repetitions matrix. It also requires one shared profile, Git commit, CoreDNS image ID, and host; all nine evidence files; a successful k6 execution identity; non-empty Prometheus response/cache telemetry; and matching directory, metadata, and k6 detail run IDs. `--allow-incomplete` is only for runner development and must not be used for published evidence.

## Scenario contract

Every directory under `scenarios/` contains `setup.ps1`, `inject.ps1`, `recover.ps1`, `verify.ps1`, and `scenario.md` as required by the implementation manual. The implemented faults are 150 ms + 20 ms jitter, connection reset, Pod outage, and empty-cache Pod replacement.
