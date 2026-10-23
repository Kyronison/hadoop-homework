import contextlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hw2 import open_interfaces


class InterfaceTests(unittest.TestCase):
    def setUp(self):
        self.inventory = ROOT / "inventory.example.json"
        self.config = json.loads(self.inventory.read_text(encoding="utf-8-sig"))

    def test_all_services_use_inventory_and_loopback_ports(self):
        addresses = ["10.20.0.21", "10.20.0.22", "10.20.0.23", "10.20.0.24"]
        for node, address in zip(self.config["nodes"], addresses):
            node["private_ip"] = address
        command = open_interfaces.build_command(
            self.config, "student@edge.example.org", Path("local key"), 2222, Path("verified hosts")
        )
        forwards = [command[i + 1] for i, arg in enumerate(command) if arg == "-L"]
        expected_targets = [
            (addresses[0], 9870), (addresses[1], 9868),
            (addresses[1], 9864), (addresses[2], 9864), (addresses[3], 9864),
            (addresses[0], 8088), (addresses[1], 8042), (addresses[2], 8042),
            (addresses[3], 8042), (addresses[0], 19888), (addresses[0], 9099),
        ]
        self.assertEqual(forwards, [
            f"127.0.0.1:{19000 + i}:{host}:{port}"
            for i, (host, port) in enumerate(expected_targets)
        ])
        self.assertEqual(command[-1], "student@edge.example.org")
        self.assertEqual(command[command.index("-p") + 1], "2222")
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertIn("ExitOnForwardFailure=yes", command)
        self.assertIn("ServerAliveInterval=30", command)
        self.assertIn("UserKnownHostsFile=verified hosts", command)
        self.assertNotIn(self.config["ssh_key"], command)

    def test_default_preview_does_not_connect_or_require_local_key(self):
        output = io.StringIO()
        with patch.object(open_interfaces.subprocess, "run") as run, contextlib.redirect_stdout(output):
            result = open_interfaces.main([
                "--inventory", str(self.inventory), "--gateway", "student@edge.example.org",
                "--identity", str(ROOT / "missing-preview-key"),
            ])
        self.assertEqual(result, 0)
        run.assert_not_called()
        self.assertIn("Preview only", output.getvalue())
        self.assertIn("http://127.0.0.1:19010/proxy/application_ID/", output.getvalue())

    def test_invalid_gateway_and_port_are_rejected(self):
        for gateway in ["edge.example.org", "student@@edge", "-o@edge", "student@-oProxyCommand=bad",
                        "student@edge;whoami", "student@edge name", "student@999.1.1.1"]:
            with self.subTest(gateway=gateway), self.assertRaises(ValueError):
                open_interfaces.build_command(self.config, gateway, "key")
        for port in (0, 65536):
            with self.subTest(port=port), self.assertRaises(ValueError):
                open_interfaces.build_command(self.config, "student@edge", "key", port)


if __name__ == "__main__":
    unittest.main()
