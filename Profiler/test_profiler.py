"""Offline regression tests for generation and benchmark orchestration."""

import csv
import io
import random
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import generate_zones as gz
import profile_runner as runner


def options(policy="Weighted", zones=3):
    return SimpleNamespace(
        zones=zones, min_records=3, max_records=5, total=True, name_width=4,
        max_targets=3, namespace="edgecdnx", zone_suffix="profiler.test",
        zone_prefix="zone", ttl=60, routing_policy=policy, location_weights=[1, 2, 3],
        nodes_per_location=2, routing_instance="edgecdnx", locations_per_endpoint=3,
    )


def generate(args, start=1):
    stats = {"zones": 0, **{t: 0 for t in gz.RECORD_TYPES}}
    records = []
    docs = list(gz.generate(args, random.Random(1), stats, records, start=start))
    return docs, stats, records


class GeneratorTests(unittest.TestCase):
    def test_weighted_locations_are_strictly_endpoint_specific(self):
        docs, stats, records = generate(options())
        locations = [doc for doc in docs if "kind: Location\n" in doc]
        endpoints = [doc for doc in docs if "kind: DNSEndpoint\n" in doc]
        self.assertEqual(stats["locations"], len(endpoints) * 3)
        self.assertEqual(stats["zones"], 3)
        self.assertEqual(stats["CNAME"], 0)
        self.assertEqual(len(endpoints), len(records))
        self.assertTrue(all(record_type in ("A", "AAAA") for _, record_type in records))
        names = [re.search(r"^  name: (.+)$", doc, re.MULTILINE).group(1) for doc in locations]
        self.assertEqual(len(names), len(set(names)))
        for index, doc in enumerate(locations):
            weight = [1, 2, 3][index % 3]
            self.assertIn(f"  weight: {weight}\n", doc)
            self.assertIn('    - name: default\n      flavor: ""\n      nodes:\n', doc)
            self.assertEqual(doc.count("ipv4:"), 2)
            self.assertEqual(doc.count("ipv6:"), 2)
            self.assertIn(f'{gz.LABEL_KEY}: "true"\n', doc)
        for doc in endpoints:
            name = re.search(r"^  name: (.+)$", doc, re.MULTILINE).group(1)
            zone = re.search(r"profiler.edgecdnx.com/zone: (.+)", doc).group(1)
            self.assertIn("routingPolicy: Weighted\n", doc)
            self.assertIn(f"      zone: {zone}\n      dnsendpoint: {name}\n", doc)
            matching = [loc for loc in locations
                        if f"    zone: {zone}\n    dnsendpoint: {name}\n" in loc]
            self.assertEqual(len(matching), 3)
            self.assertNotIn("  targets:", doc)

    def test_incremental_generation_matches_full_run(self):
        for policy in ("Simple", "Weighted"):
            with self.subTest(policy=policy):
                full, _, full_records = generate(options(policy, zones=4))
                first, _, first_records = generate(options(policy, zones=2))
                later, stats, later_records = generate(options(policy, zones=4), start=3)
                self.assertEqual(first + later, full)
                self.assertEqual(first_records + later_records, full_records)
                self.assertEqual(stats.get("locations", 0), len(later_records) * 3 if policy == "Weighted" else 0)

    def test_location_count_and_cycled_weights(self):
        for count in (1, 5):
            args = options(zones=1)
            args.locations_per_endpoint = count
            docs, stats, records = generate(args)
            locations = [doc for doc in docs if "kind: Location\n" in doc]
            self.assertEqual(stats["locations"], len(records) * count)
            weights = [int(re.search(r"^  weight: (\d+)$", doc, re.MULTILINE).group(1))
                       for doc in locations]
            self.assertEqual(weights, [1 + i % 3 for i in range(count)] * len(records))

    def test_invalid_location_counts_rejected(self):
        for command in ("generate_zones.py", "test-wrr.py"):
            for count in ("0", "-1"):
                argv = [command, "--locations-per-endpoint", count]
                if command == "generate_zones.py":
                    argv += ["--zones", "1", "--routing-policy", "Weighted"]
                with self.subTest(command=command, count=count), patch("sys.argv", argv), \
                        patch("sys.stderr", new_callable=io.StringIO):
                    with self.assertRaises(SystemExit) as error:
                        if command == "generate_zones.py":
                            gz.parse_args()
                        else:
                            runner.parse_args("Weighted")
                    self.assertEqual(error.exception.code, 2)

    def test_simple_defaults_preserve_legacy_generation(self):
        args = options("Simple")
        del args.routing_policy
        legacy, stats, records = generate(args)
        explicit, _, _ = generate(options("Simple"))
        self.assertEqual(legacy, explicit)
        self.assertNotIn("locations", stats)
        self.assertTrue(any(record_type == "CNAME" for _, record_type in records))
        self.assertTrue(all("routingPolicy: Simple" in doc
                            for doc in legacy if "kind: DNSEndpoint" in doc))

    def test_weighted_per_type_counts(self):
        args = options()
        args.total = False
        args.min_records = args.max_records = 2
        _, stats, _ = generate(args)
        self.assertEqual((stats["A"], stats["AAAA"], stats["CNAME"]), (6, 6, 0))

    def test_invalid_weights_rejected(self):
        for value in ("", "x", "0,0", "-1,2", "2147483648"):
            with self.subTest(value=value), patch("sys.argv", [
                "generate_zones.py", "--zones", "1", "--location-weights", value,
            ]), patch("sys.stderr", new_callable=io.StringIO):
                with self.assertRaises(SystemExit) as error:
                    gz.parse_args()
                self.assertEqual(error.exception.code, 2)

    def test_delete_includes_generated_locations(self):
        with patch("sys.argv", ["generate_zones.py", "--delete"]), \
                patch.object(gz, "kubectl", return_value=0) as kubectl:
            with self.assertRaises(SystemExit) as error:
                gz.main()
        self.assertEqual(error.exception.code, 0)
        self.assertEqual(kubectl.call_args.args[1:3], ("delete", "dnsendpoints,zones,locations"))


class RunnerTests(unittest.TestCase):
    def test_wrr_defaults(self):
        with patch("sys.argv", ["test-wrr.py"]):
            args = runner.parse_args("Weighted")
        self.assertEqual(args.location_weights, [1, 2, 3])
        self.assertEqual(args.locations_per_endpoint, 3)
        self.assertEqual(args.output, "wrr-results.csv")
        self.assertEqual(args.workdir, "profile-work/wrr")
        self.assertEqual(runner.gen_args(args, 2).routing_policy, "Weighted")

    def test_sync_failure_is_reported(self):
        args = SimpleNamespace(server="127.0.0.1", port=5553, sync_timeout=1)
        with patch.object(runner.subprocess, "run", return_value=SimpleNamespace(
            returncode=1, stdout="", stderr="bad option",
        )):
            with self.assertRaisesRegex(RuntimeError, "bad option"):
                runner.wait_for_sync(args, "queries.txt", 1)

    def test_simple_defaults_unchanged(self):
        with patch("sys.argv", ["test-simple.py"]):
            args = runner.parse_args()
        self.assertEqual(args.output, "profile-results.csv")
        self.assertEqual(args.workdir, "profile-work")
        self.assertEqual(args.routing_policy, "Simple")

    def test_cleanup_includes_locations_for_wrr_only(self):
        for policy, expected in (
            ("Simple", "dnsendpoints,zones"),
            ("Weighted", "dnsendpoints,zones,locations"),
        ):
            args = SimpleNamespace(routing_policy=policy, namespace="test", context=None)
            with patch.object(runner.subprocess, "run", return_value=SimpleNamespace(
                stdout="", returncode=0,
            )) as run:
                runner.cleanup(args)
            commands = [call.args[0] for call in run.call_args_list]
            self.assertEqual(commands[-1][2], expected)
            self.assertEqual([cmd[2] for cmd in commands[:-1]], expected.split(","))

    def test_apply_failure_still_cleans_up(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("sys.argv", [
                "test-wrr.py", "--steps", "1", "--workdir", directory,
                "--output", str(Path(directory) / "results.csv"),
            ]), patch.object(runner, "cleanup") as cleanup, \
                    patch.object(runner, "find_coredns_pid", return_value=None), \
                    patch.object(runner.subprocess, "run", return_value=SimpleNamespace(returncode=0)), \
                    patch.object(runner, "apply_manifests", side_effect=RuntimeError("apply failed")):
                with self.assertRaisesRegex(RuntimeError, "apply failed"):
                    runner.main("Weighted")
            self.assertEqual(cleanup.call_count, 2)

    def test_weighted_run_applies_new_locations_each_step_and_writes_csv(self):
        output = (
            "Queries sent: 100\nQueries completed: 100\nQueries lost: 0\n"
            "NOERROR 100\nQueries per second: 1000.00\n"
            "Average Latency (s): 0.001 (min 0.0005, max 0.002)\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "results.csv"
            argv = [
                "test-wrr.py", "--steps", "1,2", "--min-records", "2", "--max-records", "2",
                "--locations-per-endpoint", "5",
                "--output", str(csv_path), "--workdir", directory,
            ]
            with patch("sys.argv", argv), patch.object(runner, "cleanup") as cleanup, \
                    patch.object(runner, "find_coredns_pid", return_value=None), \
                    patch.object(runner, "apply_manifests") as apply, \
                    patch.object(runner, "wait_for_sync") as sync, \
                    patch.object(runner.subprocess, "run", return_value=SimpleNamespace(
                        returncode=0, stdout=output, stderr="",
                    )):
                runner.main("Weighted")
            self.assertEqual(cleanup.call_count, 2)
            self.assertEqual(sync.call_count, 2)
            batches = [call.args[1] for call in apply.call_args_list]
            self.assertEqual(len(batches), 4)
            for batch in batches[::2]:
                self.assertEqual(len(batch), 10)
                self.assertTrue(all("kind: Location\n" in doc for doc in batch))
            self.assertFalse(any("kind: Location\n" in doc for batch in batches[1::2] for doc in batch))
            first_names = {re.search(r"^  name: (.+)$", doc, re.MULTILINE).group(1) for doc in batches[0]}
            later_names = {re.search(r"^  name: (.+)$", doc, re.MULTILINE).group(1) for doc in batches[2]}
            self.assertTrue(first_names.isdisjoint(later_names))
            with csv_path.open() as file:
                rows = list(csv.DictReader(file))
            self.assertEqual([row["records"] for row in rows], ["2", "4"])
            self.assertEqual([row["locations"] for row in rows], ["10", "20"])
            self.assertEqual([row["locations_per_endpoint"] for row in rows], ["5", "5"])
            self.assertEqual(rows[0]["location_weights"], "1,2,3")
            self.assertEqual(rows[0]["cname_records"], "0")


if __name__ == "__main__":
    unittest.main()
