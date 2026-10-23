import importlib.util
import io
import json
from pathlib import Path
import shlex
import socket
import sys
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("preflight", ROOT / "hw1/preflight.py")
preflight = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = preflight
spec.loader.exec_module(preflight)


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.nodes = json.loads((ROOT / "inventory.example.json").read_text())["nodes"]
        self.lookup = {}
        self.reverse = {}
        for n in self.nodes:
            host, ip = n["ssh_host"], n["private_ip"]
            self.lookup[host] = self.lookup[host + ".internal"] = [ip]
            self.reverse[ip] = (host + ".internal", [], [ip])

    def getaddrinfo(self, name, port, family):
        if name not in self.lookup:
            raise socket.gaierror("Unknown name")
        return [(family, socket.SOCK_STREAM, 6, "", (ip, 0)) for ip in self.lookup[name]]

    def run_probe(self, reverse_lookup=None):
        with patch.object(preflight.socket, "getaddrinfo", side_effect=self.getaddrinfo), \
                patch.object(preflight.socket, "getfqdn", side_effect=lambda h: h + ".internal"), \
                patch.object(preflight.socket, "gethostbyaddr", side_effect=reverse_lookup or self.reverse.__getitem__):
            return preflight.probe(self.nodes)

    def test_all_forward_canonical_and_reverse_records_pass(self):
        rows = self.run_probe()
        self.assertEqual(len(rows), 4)
        self.assertEqual({row["ip"] for row in rows}, {n["private_ip"] for n in self.nodes})

    def test_loopback_duplicate_or_wrong_address_fails(self):
        host = self.nodes[2]["ssh_host"]
        for addresses in (["127.0.1.1"], [self.nodes[2]["private_ip"], "127.0.1.1"], ["10.10.0.99"]):
            self.lookup[host] = addresses
            with self.subTest(addresses=addresses), self.assertRaisesRegex(RuntimeError, "expected only"):
                self.run_probe()

    def test_unresolvable_canonical_name_fails(self):
        del self.lookup[self.nodes[3]["ssh_host"] + ".internal"]
        with self.assertRaisesRegex(RuntimeError, "Unknown name"):
            self.run_probe()

    def test_reverse_name_must_resolve_back_to_same_address(self):
        self.reverse[self.nodes[1]["private_ip"]] = (self.nodes[2]["ssh_host"], [], [])
        with self.assertRaisesRegex(RuntimeError, "Reverse name"):
            self.run_probe()

    def test_missing_reverse_mapping_fails(self):
        def missing(ip):
            raise socket.herror("No reverse mapping")
        with self.assertRaisesRegex(RuntimeError, "No reverse mapping"):
            self.run_probe(reverse_lookup=missing)

    def test_preview_does_not_connect(self):
        with patch.object(preflight, "remote") as remote, patch("sys.stdout", new_callable=io.StringIO):
            code = preflight.main(["--inventory", str(ROOT / "inventory.example.json")])
        self.assertEqual(code, 0)
        remote.assert_not_called()

    def test_remote_payload_is_standalone_and_excludes_inventory_credentials(self):
        argv = shlex.split(preflight.command_for(self.nodes))
        self.assertEqual(argv[:2], ["python3", "-c"])
        self.assertEqual(json.loads(argv[3]), [
            {"ssh_host": n["ssh_host"], "private_ip": n["private_ip"]} for n in self.nodes])
        namespace = {"__name__": "test_remote_probe"}
        with patch.object(preflight.socket, "getaddrinfo", side_effect=self.getaddrinfo), \
                patch.object(preflight.socket, "getfqdn", side_effect=lambda h: h + ".internal"), \
                patch.object(preflight.socket, "gethostbyaddr", side_effect=self.reverse.__getitem__), \
                patch("sys.argv", ["-c", argv[3]]), patch("sys.stdout", new_callable=io.StringIO) as out:
            exec(compile(argv[2], "<remote-preflight>", "exec"), namespace)
        self.assertEqual(len(json.loads(out.getvalue())), 4)
