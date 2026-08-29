#!/usr/bin/env python3
"""Build reproducible Day 6 CSV, Markdown, and comparison plots from raw k6 runs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
from collections import defaultdict
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(__file__).resolve().parents[1] / ".tmp" / "matplotlib"))

import matplotlib.pyplot as plt

EXPECTED_VARIANTS = ("baseline", "static-rendezvous", "adaptive")
EXPECTED_SCENARIOS = ("latency", "disconnect", "pod-down", "cold-cache")
REQUIRED_METRICS = (
    "hls_playlist_requests",
    "hls_playlist_failures",
    "hls_segment_requests",
    "hls_segment_failures",
    "hls_session_failures",
    "hls_segment_duration",
    "iterations",
)
REQUIRED_FILES = (
    "experiment-metadata.json",
    "k6-summary.json",
    "k6-metrics.jsonl",
    "k6.log",
    "nodequality-events.jsonl",
    "prometheus-range-query.json",
    "toxiproxy-config.json",
    "controller-metrics.txt",
    "coredns-metrics.txt",
)


def metric(summary: dict, name: str, value: str, default: float | None = None) -> float:
    try:
        payload = summary["metrics"][name]
        if "values" in payload:
            return float(payload["values"][value])
        if value == "rate" and "value" in payload:
            return float(payload["value"])
        return float(payload[value])
    except (KeyError, TypeError, ValueError) as error:
        if default is not None:
            return default
        raise ValueError(f"Missing or invalid k6 metric {name}.{value}") from error


def first_detail_run_id(path: Path) -> str:
    if not path.exists():
        raise ValueError(f"Missing k6 detail file: {path}")
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if '"run_id"' not in line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"Invalid JSON in {path}:{line_number}") from error
            run_id = record.get("data", {}).get("tags", {}).get("run_id")
            if run_id:
                return str(run_id)
    raise ValueError(f"No run_id tag found in {path}")


def load_runs(raw_root: Path) -> list[dict]:
    runs: list[dict] = []
    for metadata_path in sorted(raw_root.glob("*/experiment-metadata.json")):
        run_dir = metadata_path.parent
        missing_files = [name for name in REQUIRED_FILES if not (run_dir / name).is_file()]
        if missing_files:
            raise ValueError(f"{run_dir.name} is missing evidence files: {', '.join(missing_files)}")
        summary_path = run_dir / "k6-summary.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        run_id = str(metadata.get("run_id", ""))
        if not run_id or run_id != run_dir.name:
            raise ValueError(f"Metadata run_id '{run_id}' does not match directory '{run_dir.name}'")
        detail_run_id = first_detail_run_id(run_dir / "k6-metrics.jsonl")
        if detail_run_id != run_id:
            raise ValueError(f"k6 detail run_id '{detail_run_id}' does not match metadata '{run_id}'")
        if int(metadata.get("k6_exit_code", -1)) != 0 or not metadata.get("job_uid") or not metadata.get("k6_pod"):
            raise ValueError(f"{run_id} has invalid k6 execution identity or exit status")
        prometheus = json.loads((run_dir / "prometheus-range-query.json").read_text(encoding="utf-8"))
        prometheus_results = prometheus.get("data", {}).get("result", [])
        if prometheus.get("status") != "success" or not prometheus_results:
            raise ValueError(f"{run_id} has no Prometheus response/cache telemetry")
        missing_metrics = [name for name in REQUIRED_METRICS if name not in summary.get("metrics", {})]
        if missing_metrics:
            raise ValueError(f"{run_id} is missing k6 metrics: {', '.join(missing_metrics)}")
        playlist_requests = metric(summary, "hls_playlist_requests", "count")
        playlist_failures = metric(summary, "hls_playlist_failures", "count")
        segment_requests = metric(summary, "hls_segment_requests", "count")
        segment_failures = metric(summary, "hls_segment_failures", "count")
        runs.append(
            {
                "run_id": run_id,
                "variant": metadata["variant"],
                "scenario": metadata["scenario"],
                "repetition": metadata["repetition"],
                "profile": metadata["profile"],
                "git_commit": metadata["git_commit"],
                "coredns_image_id": metadata["container_tags"]["coredns_id"],
                "host_hardware": metadata["host_hardware"],
                "playlist_success_rate": 1.0 - playlist_failures / playlist_requests if playlist_requests else 0.0,
                "segment_success_rate": 1.0 - segment_failures / segment_requests if segment_requests else 0.0,
                "session_failure_rate": metric(summary, "hls_session_failures", "rate"),
                "segment_p50_ms": metric(summary, "hls_segment_duration", "med"),
                "segment_p95_ms": metric(summary, "hls_segment_duration", "p(95)"),
                "segment_p99_ms": metric(summary, "hls_segment_duration", "p(99)"),
                "stall_events": metric(summary, "hls_stall_events", "count", default=0.0),
                "iterations": metric(summary, "iterations", "count"),
            }
        )
    return runs


def validate_matrix(runs: list[dict], expected_repetitions: int, allow_incomplete: bool) -> dict[str, str]:
    if not runs:
        raise ValueError("No complete raw runs were found.")
    keys = [(run["variant"], run["scenario"], int(run["repetition"])) for run in runs]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate variant/scenario/repetition keys were found.")
    if not allow_incomplete:
        expected = {
            (variant, scenario, repetition)
            for variant in EXPECTED_VARIANTS
            for scenario in EXPECTED_SCENARIOS
            for repetition in range(1, expected_repetitions + 1)
        }
        actual = set(keys)
        if actual != expected:
            missing = sorted(expected - actual)
            extra = sorted(actual - expected)
            raise ValueError(f"Incomplete experiment matrix; missing={missing}, extra={extra}")
    profiles = {str(run["profile"]) for run in runs}
    commits = {str(run["git_commit"]) for run in runs}
    image_ids = {str(run["coredns_image_id"]) for run in runs}
    hardware = {str(run["host_hardware"]) for run in runs}
    if len(profiles) != 1 or len(commits) != 1 or len(image_ids) != 1 or len(hardware) != 1:
        raise ValueError(
            "Runs must share one profile, commit, image ID, and host; "
            f"profiles={profiles}, commits={commits}, images={image_ids}, hardware={hardware}"
        )
    return {
        "profile": next(iter(profiles)),
        "commit": next(iter(commits)),
        "image_id": next(iter(image_ids)),
        "hardware": next(iter(hardware)),
    }


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = (len(ordered) - 1) * fraction
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def aggregate(runs: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for run in runs:
        groups[(run["scenario"], run["variant"])].append(run)
    rows: list[dict] = []
    for (scenario, variant), items in sorted(groups.items()):
        row: dict[str, object] = {"scenario": scenario, "variant": variant, "runs": len(items)}
        for field in (
            "playlist_success_rate",
            "segment_success_rate",
            "session_failure_rate",
            "segment_p50_ms",
            "segment_p95_ms",
            "segment_p99_ms",
            "stall_events",
        ):
            values = [float(item[field]) for item in items]
            row[f"{field}_mean"] = statistics.fmean(values)
            row[f"{field}_median"] = statistics.median(values)
            row[f"{field}_p95"] = percentile(values, 0.95)
            row[f"{field}_min"] = min(values)
            row[f"{field}_max"] = max(values)
            row[f"{field}_stdev"] = statistics.stdev(values) if len(values) > 1 else 0.0
        rows.append(row)
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise SystemExit("No complete raw runs were found.")
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def plot_summary(path: Path, rows: list[dict]) -> None:
    scenarios = sorted({str(row["scenario"]) for row in rows})
    variants = [variant for variant in EXPECTED_VARIANTS if any(row["variant"] == variant for row in rows)]
    lookup = {(row["scenario"], row["variant"]): row for row in rows}
    x = list(range(len(scenarios)))
    width = 0.24 if len(variants) == 3 else (0.36 if len(variants) == 2 else 0.6)
    colors = {"baseline": "#6b7280", "static-rendezvous": "#2563eb", "adaptive": "#0f766e"}
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), constrained_layout=True)
    for index, variant in enumerate(variants):
        offset = (index - (len(variants) - 1) / 2) * width
        failure = [100 * float(lookup.get((scenario, variant), {}).get("session_failure_rate_mean", math.nan)) for scenario in scenarios]
        failure_err = [100 * float(lookup.get((scenario, variant), {}).get("session_failure_rate_stdev", 0)) for scenario in scenarios]
        latency = [float(lookup.get((scenario, variant), {}).get("segment_p95_ms_mean", math.nan)) for scenario in scenarios]
        latency_err = [float(lookup.get((scenario, variant), {}).get("segment_p95_ms_stdev", 0)) for scenario in scenarios]
        axes[0].bar([value + offset for value in x], failure, width, yerr=failure_err, label=variant, color=colors[variant], capsize=3)
        axes[1].bar([value + offset for value in x], latency, width, yerr=latency_err, label=variant, color=colors[variant], capsize=3)
    axes[0].set_title("HLS session failure rate")
    axes[0].set_ylabel("Failure rate (%)")
    axes[1].set_title("HLS segment P95 latency")
    axes[1].set_ylabel("Milliseconds")
    for axis in axes:
        axis.set_xticks(x, scenarios, rotation=20, ha="right")
        axis.grid(axis="y", alpha=0.25)
        axis.legend(frameon=False)
    repetitions = sorted({int(row["runs"]) for row in rows})
    sample_note = f"n={repetitions[0]} per variant/scenario" if len(repetitions) == 1 else "uneven repetition counts"
    fig.suptitle(
        f"EdgeRoute Day 6: deterministic, static rendezvous, and adaptive routing\nmean ± sample standard deviation; {sample_note}",
        fontsize=12,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=180)
    plt.close(fig)


def write_report(path: Path, rows: list[dict], evidence: dict[str, str], run_count: int) -> None:
    lookup = {(str(row["scenario"]), str(row["variant"])): row for row in rows}
    profile_limit = (
        "- The `smoke` profile validates the pipeline only (2-5 VUs for 18 seconds); it is not a performance result."
        if evidence["profile"] == "smoke"
        else "- The `full` profile is a controlled single-workstation load test; it is not a production-scale benchmark."
    )
    lines = [
        "# Day 6 processed results",
        "",
        "Generated from immutable files under `../raw/` by `experiments/process_results.py`.",
        f"All {run_count} runs passed the evidence-file, execution-identity, run-ID, and Prometheus telemetry checks; no run was excluded.",
        f"Profile: `{evidence['profile']}`. Commit: `{evidence['commit']}`. CoreDNS image ID: `{evidence['image_id']}`.",
        f"Host CPU: {evidence['hardware']}.",
        "Percentages and latency values are measured results for this recorded profile, not production SLO claims.",
        "",
        "| Scenario | Variant | Runs | Playlist success | Segment success | Session failures | Segment P50 | Segment P95 | Segment P99 | Stalls |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for scenario in EXPECTED_SCENARIOS:
        for variant in EXPECTED_VARIANTS:
            row = lookup.get((scenario, variant))
            if not row:
                continue
            lines.append(
                f"| {scenario} | {variant} | {row['runs']} | "
                f"{100 * float(row['playlist_success_rate_mean']):.2f}% | "
                f"{100 * float(row['segment_success_rate_mean']):.2f}% | "
                f"{100 * float(row['session_failure_rate_mean']):.2f}% | "
                f"{float(row['segment_p50_ms_mean']):.2f} ms | "
                f"{float(row['segment_p95_ms_mean']):.2f} ms | "
                f"{float(row['segment_p99_ms_mean']):.2f} ms | "
                f"{float(row['stall_events_mean']):.1f} |"
            )
    lines.extend(
        [
            "",
            "## Policy effect versus deterministic baseline",
            "",
            "Negative values are improvements for both failure-rate delta and latency change.",
            "",
            "| Scenario | Variant | Session failure delta | Segment P95 change |",
            "|---|---|---:|---:|",
        ]
    )
    for scenario in EXPECTED_SCENARIOS:
        baseline = lookup.get((scenario, "baseline"))
        for variant in ("static-rendezvous", "adaptive"):
            treatment = lookup.get((scenario, variant))
            if not baseline or not treatment:
                continue
            failure_delta = 100 * (
                float(treatment["session_failure_rate_mean"]) - float(baseline["session_failure_rate_mean"])
            )
            baseline_latency = float(baseline["segment_p95_ms_mean"])
            latency_change = 100 * (float(treatment["segment_p95_ms_mean"]) / baseline_latency - 1)
            lines.append(f"| {scenario} | {variant} | {failure_delta:+.2f} pp | {latency_change:+.2f}% |")
    lines.extend(
        [
            "",
            "![Three-policy comparison](policy-comparison.png)",
            "",
            "## Interpretation limits",
            "",
            profile_limit,
            "- The static-rendezvous control isolates the hash-family change: it keeps active-health filtering and fallback, assigns equal weights, and does not consume NodeQuality.",
            "- Three repetitions expose run-to-run spread but are insufficient for production SLO or global-scale claims.",
            "- The kind cluster uses logical Sydney/Singapore regions on one workstation, not real cross-region links.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, default=Path(__file__).parent / "results" / "raw")
    parser.add_argument("--processed", type=Path, default=Path(__file__).parent / "results" / "processed")
    parser.add_argument("--expected-repetitions", type=int, default=3)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    runs = load_runs(args.raw)
    evidence = validate_matrix(runs, args.expected_repetitions, args.allow_incomplete)
    rows = aggregate(runs)
    write_csv(args.processed / "runs.csv", runs)
    write_csv(args.processed / "summary.csv", rows)
    plot_summary(args.processed / "policy-comparison.png", rows)
    write_report(args.processed / "report.md", rows, evidence, len(runs))
    print(f"Processed {len(runs)} runs into {args.processed}")


if __name__ == "__main__":
    main()
