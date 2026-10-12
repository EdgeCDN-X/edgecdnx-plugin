#!/usr/bin/env python3
"""Measure how EdgeCDN-X plugin query latency changes as zones and records grow.

For each scale step (default: 100, 1k, 2k, 5k, 10k zones):
  1. Generate zones with --min-records..--max-records records each (random A/AAAA/CNAME mix)
     and apply them to the cluster. Steps build on each other: only the new zones are
     applied, which is possible because generation with a fixed seed is deterministic.
  2. Pick a random --sample share (default 30%) of all records and write a dnsperf query file.
  3. Wait until the plugin answers every sampled query with NOERROR.
  4. Run dnsperf and append one row of results to the CSV.

Example:
  ./test-simple.py --server 127.0.0.1 --port 5553 --output results.csv
  ./test-simple.py --steps 100,1000 --duration 20 --qps 0      # unthrottled
"""

import argparse
import csv
import ipaddress
import os
import random
import re
import subprocess
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import generate_zones as gz  # noqa: E402

CSV_FIELDS = [
    "timestamp", "zones", "records", "a_records", "aaaa_records", "cname_records",
    "sampled_queries", "apply_seconds", "sync_seconds", "duration_s", "qps_limit", "clients",
    "queries_sent", "queries_completed", "queries_lost", "noerror", "nxdomain", "servfail",
    "achieved_qps", "avg_latency_ms", "min_latency_ms", "max_latency_ms", "stddev_latency_ms",
    "p50_latency_ms", "p90_latency_ms", "p95_latency_ms", "p99_latency_ms", "p999_latency_ms",
    "coredns_rss_mb",
]


def parse_args(routing_policy="Simple"):
    description = __doc__ if routing_policy == "Simple" else (
        f"Benchmark {routing_policy} A/AAAA endpoints as zones, records and endpoint-specific "
        "locations grow. Deletes profiler-labelled resources "
        "before and after each run unless --no-cleanup-before or --keep is supplied."
    )
    p = argparse.ArgumentParser(description=description, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--steps", default="100,1000,2000,5000,10000",
                   help="Comma separated zone counts, ascending (default: 100,1000,2000,5000,10000)")
    p.add_argument("--min-records", type=int, default=10, help="Min records per zone (default: 10)")
    p.add_argument("--max-records", type=int, default=20, help="Max records per zone (default: 20)")
    p.add_argument("--sample", type=float, default=0.30, help="Share of records to query (default: 0.30)")
    p.add_argument("-s", "--server", default="127.0.0.1", help="DNS server (default: 127.0.0.1)")
    p.add_argument("-p", "--port", type=int, default=5553, help="DNS port (default: 5553)")
    p.add_argument("--duration", type=int, default=30, help="dnsperf run time per step in seconds (default: 30)")
    p.add_argument("--qps", type=int, default=0,
                   help="dnsperf QPS limit; keep it constant so latencies are comparable, 0 = unlimited (default: 0)")
    p.add_argument("--clients", type=int, default=10, help="dnsperf clients (default: 10)")
    p.add_argument("--sync-timeout", type=int, default=600,
                   help="Max seconds to wait for the plugin to serve new records (default: 600)")
    p.add_argument("-n", "--namespace", default="edgecdnx", help="Namespace (default: edgecdnx)")
    p.add_argument("--context", help="kubectl context")
    p.add_argument("--apply-workers", type=int, default=8, help="Parallel kubectl apply processes (default: 8)")
    p.add_argument("--chunk-size", type=int, default=500, help="Manifests per kubectl apply (default: 500)")
    p.add_argument("--seed", type=int, default=1, help="Random seed (default: 1)")
    suite = {"Simple": None, "Weighted": "wrr", "Geolocation": "geo"}[routing_policy]
    default_output = f"{suite}-results.csv" if suite else "profile-results.csv"
    default_workdir = f"profile-work/{suite}" if suite else "profile-work"
    p.add_argument("-o", "--output", default=default_output, help=f"CSV file, appended to (default: {default_output})")
    p.add_argument("--workdir", default=default_workdir, help=f"Query files and dnsperf logs (default: {default_workdir})")
    p.add_argument("--coredns-pid", type=int, help="CoreDNS PID for RSS tracking (default: auto-detect listener on --port)")
    p.add_argument("--no-cleanup-before", action="store_true", help="Do not delete previously generated resources first")
    p.add_argument("--keep", action="store_true", help="Keep generated resources after the run")
    if routing_policy in ("Weighted", "Geolocation"):
        gz.add_wrr_options(p)
    if routing_policy == "Geolocation":
        gz.add_geo_options(p)
        p.add_argument("--ecs-us-subnet", default="24.0.0.0/24",
                       help="IPv4 block for random US ECS clients (default: 24.0.0.0/24)")
        p.add_argument("--ecs-eu-subnet", default="80.156.0.0/24",
                       help="IPv4 block for random EU ECS clients (default: 80.156.0.0/24)")
    args = p.parse_args()

    args.steps = [int(x) for x in args.steps.split(",") if x.strip()]
    if not args.steps or args.steps != sorted(args.steps) or args.steps[0] < 1:
        p.error("--steps must be ascending positive integers")
    if not 0 < args.sample <= 1:
        p.error("--sample must be in (0, 1]")
    if args.min_records < 1 or args.max_records < args.min_records:
        p.error("require 1 <= --min-records <= --max-records")
    if args.apply_workers < 1 or args.chunk_size < 1 or args.clients < 1:
        p.error("--apply-workers, --chunk-size and --clients must be >= 1")
    if args.duration < 1 or args.sync_timeout < 1 or args.qps < 0:
        p.error("--duration and --sync-timeout must be >= 1, and --qps must be >= 0")
    args.routing_policy = routing_policy
    if routing_policy in ("Weighted", "Geolocation"):
        gz.validate_wrr_options(p, args)
    if routing_policy == "Geolocation":
        gz.validate_geo_options(p, args)
        for option in ("ecs_us_subnet", "ecs_eu_subnet"):
            try:
                subnet = ipaddress.IPv4Network(getattr(args, option), strict=True)
            except ValueError as error:
                p.error(f"--{option.replace('_', '-')} must be an IPv4 network: {error}")
            if not subnet.is_global:
                p.error(f"--{option.replace('_', '-')} must be a public IPv4 network")
            setattr(args, option, subnet)
    return args


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", file=sys.stderr, flush=True)


def kubectl_cmd(args):
    return ["kubectl"] + (["--context", args.context] if args.context else [])


def gen_args(args, zones):
    return SimpleNamespace(
        zones=zones, min_records=args.min_records, max_records=args.max_records, total=True,
        name_width=len(str(args.steps[-1])), max_targets=3, namespace=args.namespace,
        zone_suffix="profiler.test", zone_prefix="zone", ttl=60,
        routing_policy=args.routing_policy,
        location_weights=getattr(args, "location_weights", []),
        locations_per_endpoint=getattr(args, "locations_per_endpoint", 0),
        nodes_per_location=getattr(args, "nodes_per_location", 1),
        routing_instance=getattr(args, "routing_instance", "edgecdnx"),
        node_groups_per_location=getattr(args, "node_groups_per_location", 1),
        geo_attribute=getattr(args, "geo_attribute", "geoip/continent/code"),
        geo_values=getattr(args, "geo_values", ["EU", "NA", "AS"]),
    )


def cleanup(args):
    log("Deleting previously generated resources ...")
    kinds = "dnsendpoints,zones,locations" if args.routing_policy in ("Weighted", "Geolocation") else "dnsendpoints,zones"
    for kind in kinds.split(","):
        subprocess.run(
            kubectl_cmd(args) + ["delete", kind, "-n", args.namespace, "-l", f"{gz.LABEL_KEY}=true",
                                 "--wait=false", "--ignore-not-found"],
            stdout=subprocess.DEVNULL, check=True,
        )
    # Wait until the API server reports nothing left, so the next step starts clean.
    while True:
        out = subprocess.run(
            kubectl_cmd(args) + ["get", kinds, "-n", args.namespace,
                                 "-l", f"{gz.LABEL_KEY}=true", "-o", "name"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        if not out:
            return
        log(f"  {len(out.splitlines())} resources still terminating ...")
        time.sleep(5)


def apply_manifests(args, docs):
    chunks = ["".join(docs[i:i + args.chunk_size]) for i in range(0, len(docs), args.chunk_size)]

    def run(chunk):
        r = subprocess.run(
            kubectl_cmd(args) + ["apply", "--server-side", "--force-conflicts", "-f", "-", "-o", "name"],
            input=chunk, capture_output=True, text=True,
        )
        if r.returncode != 0:
            raise RuntimeError(f"kubectl apply failed: {r.stderr.strip()[:2000]}")

    with ThreadPoolExecutor(max_workers=args.apply_workers) as pool:
        for i, _ in enumerate(pool.map(run, chunks), 1):
            if i % 20 == 0 or i == len(chunks):
                log(f"  applied {i}/{len(chunks)} chunks")


def dnsperf_cmd(args, query_file, extra):
    return ["dnsperf", "-s", args.server, "-p", str(args.port), "-d", query_file,
            "-O", "suppress=timeout,unexpected"] + (
                ["-B"] if getattr(args, "routing_policy", "Simple") == "Geolocation" else []
            ) + extra


def ecs_query(name, record_type, address):
    """Encode a DNS query with EDNS0 Client Subnet (IPv4 /32, scope /0)."""
    labels = name.rstrip(".").encode("idna").split(b".")
    if any(not label or len(label) > 63 for label in labels):
        raise ValueError(f"Invalid DNS name: {name}")
    qname = b"".join(bytes([len(label)]) + label for label in labels) + b"\0"
    if len(qname) > 255:
        raise ValueError(f"DNS name exceeds 255 wire bytes: {name}")
    qtype = {"A": 1, "AAAA": 28}[record_type]
    question = qname + struct.pack("!HH", qtype, 1)
    subnet = struct.pack("!HBB", 1, 32, 0) + ipaddress.IPv4Address(address).packed
    option = struct.pack("!HH", 8, len(subnet)) + subnet
    opt = b"\0" + struct.pack("!HHIH", 41, 1232, 0, len(option)) + option
    packet = struct.pack("!HHHHHH", 0, 0x0100, 1, 0, 0, 1) + question + opt
    return struct.pack("!H", len(packet)) + packet


def write_geo_queries(args, sample, query_file, rng):
    """Write per-query ECS packets and an auditable text companion."""
    subnets = (("US", args.ecs_us_subnet), ("EU", args.ecs_eu_subnet))
    count = 0
    with open(query_file, "wb") as binary, open(query_file + ".csv", "w", newline="") as companion:
        writer = csv.writer(companion)
        writer.writerow(["name", "record_type", "region", "ecs_address", "ecs_prefix_length"])
        for name, record_type in sample:
            for region, subnet in subnets:
                low, high = (1, subnet.num_addresses - 2) if subnet.num_addresses > 2 else (0, subnet.num_addresses - 1)
                address = subnet.network_address + rng.randint(low, high)
                binary.write(ecs_query(name, record_type, address))
                writer.writerow([name, record_type, region, str(address), 32])
                count += 1
    return count


def wait_for_sync(args, query_file, expected):
    """Run single passes over the query file until every query returns NOERROR."""
    deadline = time.monotonic() + args.sync_timeout
    while True:
        result = subprocess.run(dnsperf_cmd(args, query_file, ["-n", "1", "-t", "2"]),
                                capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"dnsperf sync check failed: {result.stderr.strip()[:2000]}")
        out = result.stdout
        noerror = int(m.group(1)) if (m := re.search(r"NOERROR (\d+)", out)) else 0
        if noerror >= expected:
            return
        if time.monotonic() > deadline:
            raise RuntimeError(f"Plugin not in sync after {args.sync_timeout}s ({noerror}/{expected} NOERROR)")
        log(f"  waiting for plugin sync: {noerror}/{expected} NOERROR")
        time.sleep(3)


def percentile(buckets, total, q):
    """Return the upper bound (s) of the histogram bucket containing quantile q."""
    target = q * total
    acc = 0
    for upper, count in buckets:
        acc += count
        if acc >= target:
            return upper
    return buckets[-1][0] if buckets else 0.0


def parse_dnsperf(out):
    def num(pattern, cast=float, default=0):
        m = re.search(pattern, out)
        return cast(m.group(1)) if m else default

    res = {
        "queries_sent": num(r"Queries sent:\s+(\d+)", int),
        "queries_completed": num(r"Queries completed:\s+(\d+)", int),
        "queries_lost": num(r"Queries lost:\s+(\d+)", int),
        "noerror": num(r"NOERROR (\d+)", int),
        "nxdomain": num(r"NXDOMAIN (\d+)", int),
        "servfail": num(r"SERVFAIL (\d+)", int),
        "achieved_qps": round(num(r"Queries per second:\s+([\d.]+)"), 2),
    }
    m = re.search(r"Average Latency \(s\):\s+([\d.]+) \(min ([\d.]+), max ([\d.]+)\)", out)
    if m:
        res["avg_latency_ms"], res["min_latency_ms"], res["max_latency_ms"] = (
            round(float(x) * 1000, 4) for x in m.groups())
    res["stddev_latency_ms"] = round(num(r"Latency StdDev \(s\):\s+([\d.]+)") * 1000, 4)

    buckets = [(float(hi), int(c)) for hi, c in
               re.findall(r"^\s+[\d.]+ - ([\d.]+):\s+(\d+)$", out, re.MULTILINE)]
    total = sum(c for _, c in buckets)
    for name, q in (("p50", .5), ("p90", .9), ("p95", .95), ("p99", .99), ("p999", .999)):
        res[f"{name}_latency_ms"] = round(percentile(buckets, total, q) * 1000, 4) if total else ""
    return res


def find_coredns_pid(args):
    if args.coredns_pid:
        return args.coredns_pid
    r = subprocess.run(["lsof", "-nP", f"-iUDP:{args.port}", "-t"], capture_output=True, text=True)
    pids = r.stdout.split()
    return int(pids[0]) if pids else None


def rss_mb(pid):
    if not pid:
        return ""
    r = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
    return round(int(r.stdout.strip()) / 1024, 1) if r.stdout.strip() else ""


def main(routing_policy="Simple"):
    args = parse_args(routing_policy)
    os.makedirs(args.workdir, exist_ok=True)

    for tool in ("kubectl", "dnsperf"):
        if subprocess.run(["which", tool], capture_output=True).returncode != 0:
            sys.exit(f"{tool} not found in PATH")

    pid = find_coredns_pid(args)
    log(f"CoreDNS PID: {pid or 'not found (RSS will not be recorded)'}")

    if not args.no_cleanup_before:
        cleanup(args)

    write_header = not os.path.exists(args.output) or os.path.getsize(args.output) == 0
    sample_rng = random.Random(args.seed + 1)
    ecs_rng = random.Random(args.seed + 2)
    all_records = []
    totals = {"zones": 0, "locations": 0, **{t: 0 for t in gz.RECORD_TYPES}}
    fields = CSV_FIELDS
    if routing_policy in ("Weighted", "Geolocation"):
        fields = CSV_FIELDS + ["locations", "locations_per_endpoint", "location_weights", "nodes_per_location"]
    if routing_policy == "Geolocation":
        fields = fields + ["node_groups_per_location", "geo_attribute", "geo_values",
                           "ecs_us_subnet", "ecs_eu_subnet"]
    applied = 0

    try:
        if not write_header:
            with open(args.output, newline="") as existing:
                if next(csv.reader(existing), []) != fields:
                    raise RuntimeError(f"CSV header does not match {routing_policy} benchmark: {args.output}")
        with open(args.output, "a", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            if write_header:
                writer.writeheader()
                fh.flush()

            for zones in args.steps:
                log(f"=== Step: {zones} zones ===")
                stats = {"zones": 0, "locations": 0, **{t: 0 for t in gz.RECORD_TYPES}}
                docs = list(gz.generate(gen_args(args, zones), random.Random(args.seed), stats,
                                        all_records, start=applied + 1))
                for k in totals:
                    totals[k] += stats[k]
                log(f"Applying {len(docs)} new manifests ({stats['zones']} zones)")
                t0 = time.monotonic()
                if routing_policy in ("Weighted", "Geolocation"):
                    location_docs = [doc for doc in docs if "\nkind: Location\n" in doc]
                    apply_manifests(args, location_docs)
                    docs = [doc for doc in docs if "\nkind: Location\n" not in doc]
                apply_manifests(args, docs)
                apply_s = round(time.monotonic() - t0, 2)
                applied = zones

                sample = sample_rng.sample(all_records, max(1, round(len(all_records) * args.sample)))
                if routing_policy == "Geolocation":
                    query_file = os.path.join(args.workdir, f"queries-{zones}.bin")
                    sampled_queries = write_geo_queries(args, sample, query_file, ecs_rng)
                    log(f"ECS /32 clients from US {args.ecs_us_subnet} and EU {args.ecs_eu_subnet}")
                else:
                    query_file = os.path.join(args.workdir, f"queries-{zones}.txt")
                    with open(query_file, "w") as qf:
                        qf.writelines(f"{name} {rtype}\n" for name, rtype in sample)
                    sampled_queries = len(sample)

                log(f"Waiting for plugin to serve {sampled_queries} sampled queries")
                t0 = time.monotonic()
                wait_for_sync(args, query_file, sampled_queries)
                sync_s = round(time.monotonic() - t0, 2)

                extra = ["-l", str(args.duration), "-c", str(args.clients), "-O", "latency-histogram"]
                if args.qps > 0:
                    extra += ["-Q", str(args.qps)]
                log(f"Running dnsperf for {args.duration}s")
                result = subprocess.run(dnsperf_cmd(args, query_file, extra),
                                        capture_output=True, text=True)
                if result.returncode != 0:
                    raise RuntimeError(f"dnsperf failed: {result.stderr.strip()[:2000]}")
                out = result.stdout
                with open(os.path.join(args.workdir, f"dnsperf-{zones}.log"), "w") as lf:
                    lf.write(out)
                measurements = parse_dnsperf(out)
                if measurements["queries_sent"] == 0 or "avg_latency_ms" not in measurements:
                    raise RuntimeError(f"dnsperf produced no usable measurements; see {args.workdir}")

                row = {
                    "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "zones": totals["zones"],
                    "records": len(all_records),
                    "a_records": totals["A"], "aaaa_records": totals["AAAA"], "cname_records": totals["CNAME"],
                    "sampled_queries": sampled_queries,
                    "apply_seconds": apply_s, "sync_seconds": sync_s,
                    "duration_s": args.duration, "qps_limit": args.qps or "unlimited", "clients": args.clients,
                    "coredns_rss_mb": rss_mb(pid),
                    **measurements,
                }
                if routing_policy in ("Weighted", "Geolocation"):
                    row.update(locations=totals["locations"],
                               locations_per_endpoint=args.locations_per_endpoint,
                               location_weights=",".join(map(str, args.location_weights)),
                               nodes_per_location=args.nodes_per_location)
                if routing_policy == "Geolocation":
                    row.update(node_groups_per_location=args.node_groups_per_location,
                               geo_attribute=args.geo_attribute,
                               geo_values=",".join(args.geo_values),
                               ecs_us_subnet=str(args.ecs_us_subnet),
                               ecs_eu_subnet=str(args.ecs_eu_subnet))
                writer.writerow(row)
                fh.flush()
                log(f"zones={row['zones']} records={row['records']} locations={totals['locations']} "
                    f"qps={row['achieved_qps']} "
                    f"avg={row.get('avg_latency_ms')}ms p99={row['p99_latency_ms']}ms rss={row['coredns_rss_mb']}MB")
    finally:
        if not args.keep:
            cleanup(args)

    log(f"Results written to {args.output}")


if __name__ == "__main__":
    main()
