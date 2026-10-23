"""Offline regression tests for generation and benchmark orchestration."""

import csv
import ipaddress
import io
import random
import re
import shutil
import socket
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor

import generate_zones as gz
import profile_runner as runner


def options(policy="Weighted", zones=3):
    return SimpleNamespace(
        zones=zones, min_records=3, max_records=5, total=True, name_width=4,
        max_targets=3, namespace="edgecdnx", zone_suffix="profiler.test",
        zone_prefix="zone", ttl=60, routing_policy=policy, location_weights=[1, 2, 3],
        nodes_per_location=2, routing_instance="edgecdnx", locations_per_endpoint=3,
        node_groups_per_location=4, geo_attribute="geoip/continent/code",
        geo_values=["EU", "NA", "AS"],
    )


def generate(args, start=1):
    stats = {"zones": 0, **{t: 0 for t in gz.RECORD_TYPES}}
    records = []
    docs = list(gz.generate(args, random.Random(1), stats, records, start=start))
    return docs, stats, records


def read_dns_packets(path):
    data = path.read_bytes()
    packets = []
    while data:
        size, = struct.unpack("!H", data[:2])
        packet, data = data[2:2 + size], data[2 + size:]
        if len(packet) != size:
            raise AssertionError("Truncated DNS packet")
        packets.append(packet)
    return packets


class ECSTests(unittest.TestCase):
    def test_binary_queries_have_correct_questions_and_ecs_wire_format(self):
        sample = [("a.example.test.", "A"), ("aaaa.example.test", "AAAA")]
        with patch("sys.argv", ["test-geo.py"]):
            args = runner.parse_args("Geolocation")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.bin"
            self.assertEqual(runner.write_geo_queries(args, sample, str(path), random.Random(3)), 4)
            packets = read_dns_packets(path)
            with Path(str(path) + ".csv").open() as companion:
                rows = list(csv.DictReader(companion))
            self.assertEqual(len(packets), 4)
            self.assertEqual([row["region"] for row in rows], ["US", "EU", "US", "EU"])
            addresses = []
            for index, (packet, row) in enumerate(zip(packets, rows)):
                self.assertEqual(struct.unpack("!HHHHHH", packet[:12]), (0, 0x0100, 1, 0, 0, 1))
                offset = 12
                labels = []
                while packet[offset]:
                    size = packet[offset]
                    labels.append(packet[offset + 1:offset + 1 + size].decode("ascii"))
                    offset += size + 1
                self.assertEqual(".".join(labels), sample[index // 2][0].rstrip("."))
                offset += 1
                self.assertEqual(struct.unpack("!HH", packet[offset:offset + 4]), (1 if index < 2 else 28, 1))
                offset += 4
                self.assertEqual(packet[offset], 0)
                offset += 1
                self.assertEqual(struct.unpack("!HHIH", packet[offset:offset + 10]), (41, 1232, 0, 12))
                offset += 10
                self.assertEqual(struct.unpack("!HHHBB", packet[offset:offset + 8]), (8, 8, 1, 32, 0))
                address = ipaddress.IPv4Address(packet[offset + 8:offset + 12])
                self.assertEqual(len(packet), offset + 12)
                self.assertEqual(str(address), row["ecs_address"])
                self.assertEqual(row["ecs_prefix_length"], "32")
                self.assertIn(address, args.ecs_us_subnet if row["region"] == "US" else args.ecs_eu_subnet)
                addresses.append(address)
            self.assertGreater(len(set(addresses)), 2)
            original = path.read_bytes()
            runner.write_geo_queries(args, sample, str(path), random.Random(3))
            self.assertEqual(original, path.read_bytes())

    def test_custom_single_address_subnets(self):
        with patch("sys.argv", ["test-geo.py", "--ecs-us-subnet", "24.0.0.1/32",
                                "--ecs-eu-subnet", "80.156.0.1/32"]):
            args = runner.parse_args("Geolocation")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.bin"
            runner.write_geo_queries(args, [("example.test", "A")], str(path), random.Random(3))
            self.assertEqual([str(ipaddress.IPv4Address(packet[-4:])) for packet in read_dns_packets(path)],
                             ["24.0.0.1", "80.156.0.1"])

    def test_invalid_subnets_rejected(self):
        for option in ("--ecs-us-subnet", "--ecs-eu-subnet"):
            for value in ("invalid", "::/0", "24.0.0.1/24", "127.0.0.0/8", "192.0.2.0/24"):
                with self.subTest(option=option, value=value), \
                        patch("sys.argv", ["test-geo.py", option, value]), \
                        patch("sys.stderr", new_callable=io.StringIO):
                    with self.assertRaises(SystemExit) as error:
                        runner.parse_args("Geolocation")
                    self.assertEqual(error.exception.code, 2)

    def test_binary_mode_used_for_sync_and_load_only_in_geo_suite(self):
        for policy in ("Simple", "Weighted", "Geolocation"):
            args = SimpleNamespace(server="127.0.0.1", port=5553, routing_policy=policy)
            for extra in (["-n", "1"], ["-l", "30", "-c", "10"]):
                command = runner.dnsperf_cmd(args, "queries", extra)
                self.assertEqual("-B" in command, policy == "Geolocation")
                self.assertNotIn("-E", command)
                self.assertNotIn("-e", command)
                self.assertEqual(command[-len(extra):], extra)

    def test_invalid_dns_names_rejected(self):
        for name in ("a..test", "x" * 64 + ".test", ".".join(["x" * 63] * 4)):
            with self.subTest(name=name), self.assertRaises(ValueError):
                runner.ecs_query(name, "A", "24.0.0.1")

    @unittest.skipUnless(shutil.which("dnsperf"), "optional local wire test requires dnsperf")
    def test_dnsperf_sends_per_query_ecs_over_udp(self):
        with patch("sys.argv", ["test-geo.py"]):
            args = runner.parse_args("Geolocation")
        with tempfile.TemporaryDirectory() as directory, socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server:
            server.bind(("127.0.0.1", 0))
            server.settimeout(5)
            args.server, args.port = server.getsockname()
            path = Path(directory) / "queries.bin"
            count = runner.write_geo_queries(args, [("a.test", "A"), ("aaaa.test", "AAAA")],
                                             str(path), random.Random(3))
            expected = read_dns_packets(path)

            def receive():
                received = []
                for _ in range(count):
                    packet, peer = server.recvfrom(4096)
                    received.append(packet)
                    # Echo question and OPT in a valid NOERROR response.
                    server.sendto(packet[:2] + struct.pack("!H", 0x8180) + packet[4:], peer)
                return received

            with ThreadPoolExecutor(max_workers=1) as pool:
                received = pool.submit(receive)
                result = subprocess.run(runner.dnsperf_cmd(args, str(path), ["-n", "1", "-c", "1", "-t", "2"]),
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual([packet[2:] for packet in received.result()], [packet[2:] for packet in expected])
            self.assertEqual(runner.parse_dnsperf(result.stdout)["noerror"], count)


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
        for policy in ("Simple", "Weighted", "Geolocation"):
            with self.subTest(policy=policy):
                full, _, full_records = generate(options(policy, zones=4))
                first, _, first_records = generate(options(policy, zones=2))
                later, stats, later_records = generate(options(policy, zones=4), start=3)
                self.assertEqual(first + later, full)
                self.assertEqual(first_records + later_records, full_records)
                self.assertEqual(stats.get("locations", 0), len(later_records) * 3 if policy != "Simple" else 0)

    def test_geo_locations_and_node_groups_are_endpoint_specific(self):
        docs, stats, records = generate(options("Geolocation"))
        locations = [doc for doc in docs if "kind: Location\n" in doc]
        endpoints = [doc for doc in docs if "kind: DNSEndpoint\n" in doc]
        self.assertEqual(stats["locations"], len(records) * 3)
        self.assertEqual(stats["CNAME"], 0)
        self.assertTrue(all(record_type in ("A", "AAAA") for _, record_type in records))
        for index, doc in enumerate(locations):
            self.assertIn(f"    weight: {[1, 2, 3][index % 3]}\n", doc)
            self.assertIn('      "geoip/continent/code":\n        weight: 100\n', doc)
            self.assertIn(f'          - value: "{["EU", "NA", "AS"][index % 3]}"\n', doc)
            self.assertIn(f"            weight: {index % 3 + 1}\n", doc)
            self.assertIn("      default:\n        weight: 1\n", doc)
            self.assertEqual(doc.count("ipv4:"), 8)
            self.assertEqual(doc.count("ipv6:"), 8)
            self.assertEqual(len(re.findall(r"^    - name: group-", doc, re.MULTILINE)), 4)
            self.assertEqual(doc.count('profiler.edgecdnx.com/node-group: "selected"'), 1)
            self.assertEqual(doc.count('profiler.edgecdnx.com/node-group: "excluded"'), 3)
            self.assertNotIn("profiler.edgecdnx.com/node-group", doc.split("spec:\n")[0])
            self.assertIn('    - name: group-4\n      flavor: ""\n      labels:\n'
                          '        profiler.edgecdnx.com/node-group: "selected"\n', doc)
        for doc in endpoints:
            name = re.search(r"^  name: (.+)$", doc, re.MULTILINE).group(1)
            zone = re.search(r"profiler.edgecdnx.com/zone: (.+)", doc).group(1)
            self.assertIn("routingPolicy: Geolocation\n", doc)
            self.assertIn(f"      zone: {zone}\n      dnsendpoint: {name}\n", doc)
            self.assertIn('      profiler.edgecdnx.com/node-group: "selected"\n', doc)
            self.assertNotIn("  targets:", doc)
            matching = [loc for loc in locations if f"    zone: {zone}\n    dnsendpoint: {name}\n" in loc]
            self.assertEqual(len(matching), 3)

    def test_geo_custom_group_and_value_options(self):
        args = options("Geolocation", zones=1)
        args.node_groups_per_location = 1
        args.locations_per_endpoint = 5
        args.geo_attribute = 'custom/quoted"label'
        args.geo_values = ['quoted"value']
        docs, stats, records = generate(args)
        locations = [doc for doc in docs if "kind: Location\n" in doc]
        self.assertEqual(stats["locations"], len(records) * 5)
        for doc in locations:
            self.assertEqual(doc.count("ipv4:"), 2)
            self.assertNotIn('"excluded"', doc)
            self.assertIn('"custom/quoted\\"label":', doc)
            self.assertIn('value: "quoted\\"value"', doc)

    def test_invalid_geo_options_rejected(self):
        for option, value in (
            ("--node-groups-per-location", "0"), ("--node-groups-per-location", "-1"),
            ("--location-weights", "1001"), ("--geo-attribute", "invalid"),
            ("--geo-attribute", "/missing"), ("--geo-values", ""), ("--geo-values", "EU,"),
        ):
            for command in ("generate_zones.py", "test-geo.py"):
                argv = [command, option, value]
                if command == "generate_zones.py":
                    argv += ["--zones", "1", "--routing-policy", "Geolocation"]
                with self.subTest(command=command, option=option, value=value), \
                        patch("sys.argv", argv), patch("sys.stderr", new_callable=io.StringIO):
                    with self.assertRaises(SystemExit) as error:
                        if command == "generate_zones.py":
                            gz.parse_args()
                        else:
                            runner.parse_args("Geolocation")
                    self.assertEqual(error.exception.code, 2)

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

    def test_every_generated_cr_spec_has_project(self):
        for policy in ("Simple", "Weighted", "Geolocation"):
            with self.subTest(policy=policy):
                args = options(policy, zones=1)
                args.namespace = "load-test"
                args.project = "edge-project"
                docs, _, _ = generate(args)
                resources = [doc for doc in docs if re.search(r"^kind: (Zone|DNSEndpoint|Location)$", doc, re.MULTILINE)]
                self.assertTrue(any("kind: Zone\n" in doc for doc in resources))
                self.assertTrue(any("kind: DNSEndpoint\n" in doc for doc in resources))
                if policy != "Simple":
                    self.assertTrue(any("kind: Location\n" in doc for doc in resources))
                for doc in resources:
                    self.assertIn("spec:\n  project:\n    name: edge-project\n", doc)

    def test_generator_project_defaults_to_namespace_and_can_be_overridden(self):
        for argv, expected in (
            (["generate_zones.py", "--zones", "1", "--namespace", "load-test"], "load-test"),
            (["generate_zones.py", "--zones", "1", "--namespace", "load-test", "--project", "edge-project"], "edge-project"),
        ):
            with self.subTest(argv=argv), patch("sys.argv", argv):
                args = gz.parse_args()
            self.assertEqual(args.project, expected)

    def test_weighted_per_type_counts(self):
        for policy in ("Weighted", "Geolocation"):
            with self.subTest(policy=policy):
                args = options(policy)
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
    def test_runner_project_defaults_to_namespace_and_can_be_overridden(self):
        for extra, expected in (([], "load-test"), (["--project", "edge-project"], "edge-project")):
            argv = ["test-simple.py", "--namespace", "load-test", *extra]
            with self.subTest(argv=argv), patch("sys.argv", argv):
                args = runner.parse_args()
            self.assertEqual(args.project, expected)
            self.assertEqual(runner.gen_args(args, 1).project, expected)

    def test_kubectl_command_pins_kubeconfig_and_context(self):
        with patch("sys.argv", [
            "test-simple.py", "--kubeconfig", "/tmp/upstream.kubeconfig", "--context", "kind-edgecdnx",
        ]):
            parsed_runner_args = runner.parse_args()
        self.assertEqual(parsed_runner_args.kubeconfig, "/tmp/upstream.kubeconfig")
        self.assertEqual(parsed_runner_args.context, "kind-edgecdnx")

        with patch("sys.argv", [
            "generate_zones.py", "--zones", "1", "--kubeconfig", "/tmp/local.kubeconfig",
            "--context", "kind-edgecdnx-downstream",
        ]):
            parsed_generator_args = gz.parse_args()
        self.assertEqual(parsed_generator_args.kubeconfig, "/tmp/local.kubeconfig")
        self.assertEqual(parsed_generator_args.context, "kind-edgecdnx-downstream")

        args = SimpleNamespace(kubeconfig="/tmp/upstream.kubeconfig", context="kind-edgecdnx")
        self.assertEqual(
            runner.kubectl_cmd(args),
            ["kubectl", "--kubeconfig", "/tmp/upstream.kubeconfig", "--context", "kind-edgecdnx"],
        )

        generated_args = SimpleNamespace(kubeconfig="/tmp/local.kubeconfig", context="kind-edgecdnx-downstream")
        with patch.object(gz.subprocess, "run", return_value=SimpleNamespace(returncode=0)) as run:
            self.assertEqual(gz.kubectl(generated_args, "get", "dnsendpoints"), 0)
        self.assertEqual(
            run.call_args.args[0],
            ["kubectl", "--kubeconfig", "/tmp/local.kubeconfig", "--context", "kind-edgecdnx-downstream",
             "get", "dnsendpoints"],
        )

    def test_geo_defaults(self):
        with patch("sys.argv", ["test-geo.py"]):
            args = runner.parse_args("Geolocation")
        self.assertEqual(args.output, "geo-results.csv")
        self.assertEqual(args.workdir, "profile-work/geo")
        self.assertEqual(args.node_groups_per_location, 4)
        self.assertEqual(args.geo_values, ["EU", "NA", "AS"])
        self.assertEqual(str(args.ecs_us_subnet), "24.0.0.0/24")
        self.assertEqual(str(args.ecs_eu_subnet), "80.156.0.0/24")
        generated = runner.gen_args(args, 2)
        self.assertEqual(generated.routing_policy, "Geolocation")
        self.assertEqual(generated.geo_attribute, "geoip/continent/code")
        self.assertEqual(generated.geo_values, args.geo_values)
        self.assertEqual(generated.node_groups_per_location, 4)

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

    def test_cleanup_includes_locations_for_dynamic_policies(self):
        for policy, expected in (
            ("Simple", "dnsendpoints,zones"),
            ("Weighted", "dnsendpoints,zones,locations"),
            ("Geolocation", "dnsendpoints,zones,locations"),
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
        for policy in ("Weighted", "Geolocation"):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as directory:
                with patch("sys.argv", [
                    "profiler", "--steps", "1", "--workdir", directory,
                    "--output", str(Path(directory) / "results.csv"),
                ]), patch.object(runner, "cleanup") as cleanup, \
                        patch.object(runner, "find_coredns_pid", return_value=None), \
                        patch.object(runner.subprocess, "run", return_value=SimpleNamespace(returncode=0)), \
                        patch.object(runner, "apply_manifests", side_effect=RuntimeError("apply failed")):
                    with self.assertRaisesRegex(RuntimeError, "apply failed"):
                        runner.main(policy)
                self.assertEqual(cleanup.call_count, 2)

    def test_weighted_run_applies_new_locations_each_step_and_writes_csv(self):
        self.assert_dynamic_run("Weighted")

    def test_geo_run_applies_new_locations_each_step_and_writes_csv(self):
        self.assert_dynamic_run("Geolocation")

    def assert_dynamic_run(self, policy):
        output = (
            "Queries sent: 100\nQueries completed: 100\nQueries lost: 0\n"
            "NOERROR 100\nQueries per second: 1000.00\n"
            "Average Latency (s): 0.001 (min 0.0005, max 0.002)\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "results.csv"
            argv = [
                "profiler", "--steps", "1,2", "--min-records", "2", "--max-records", "2",
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
                runner.main(policy)
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
            if policy == "Geolocation":
                self.assertEqual(rows[0]["node_groups_per_location"], "4")
                self.assertEqual(rows[0]["geo_attribute"], "geoip/continent/code")
                self.assertEqual(rows[0]["geo_values"], "EU,NA,AS")
                self.assertEqual(rows[0]["ecs_us_subnet"], "24.0.0.0/24")
                self.assertEqual(rows[0]["ecs_eu_subnet"], "80.156.0.0/24")
                self.assertTrue(all(row["sampled_queries"] == "2" for row in rows))
                for call in sync.call_args_list:
                    self.assertTrue(call.args[1].endswith(".bin"))
                    self.assertEqual(call.args[2], 2)
                packets = read_dns_packets(Path(directory) / "queries-1.bin")
                self.assertEqual(len(packets), 2)
                self.assertTrue(all("  geoLookup:\n" in doc for batch in batches[::2] for doc in batch))
                self.assertTrue(all("routingPolicy: Geolocation\n" in doc
                                    for batch in batches[1::2] for doc in batch if "kind: DNSEndpoint" in doc))


if __name__ == "__main__":
    unittest.main()
