import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from common import cluster
from hw3 import config, deploy


ROOT = Path(__file__).resolve().parents[1]


class HiveConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads((ROOT / "inventory.example.json").read_text(encoding="utf-8"))
        local_temp = patch.object(tempfile, "tempdir", str(ROOT))
        local_temp.start()
        self.addCleanup(local_temp.stop)

    def test_remote_metastore_clients_never_receive_jdbc_password(self):
        master = config.properties(self.c, "master")
        self.assertEqual(master["javax.jdo.option.ConnectionDriverName"], "org.postgresql.Driver")
        self.assertEqual(master["javax.jdo.option.ConnectionURL"], "jdbc:postgresql://127.0.0.1:5432/hw3_hive")
        self.assertEqual(master["javax.jdo.option.ConnectionPassword"], config.PASSWORD_PLACEHOLDER)
        self.assertNotIn("hive.metastore.uris", master)
        self.assertFalse(master["metastore.try.direct.sql.ddl"])
        self.assertEqual(master["hive.metastore.server.max.threads"], 30)
        for name in ("worker1", "worker2"):
            with self.subTest(name=name):
                properties = config.properties(self.c, name)
                self.assertEqual(properties["hive.metastore.uris"], "thrift://10.10.0.10:9083")
                self.assertFalse(any(key.startswith("javax.jdo") for key in properties))
                self.assertNotIn("derby", str(properties).lower())
        hs2 = config.properties(self.c, "worker2")
        self.assertFalse(hs2["hive.server2.enable.doAs"])
        self.assertEqual(hs2["hive.server2.thrift.bind.host"], "10.10.0.12")
        self.assertEqual(hs2["hive.server2.webui.port"], 0)
        self.assertEqual(hs2["hive.execution.engine"], "mr")

    def test_hive_render_preserves_hadoop_configuration(self):
        original = copy.deepcopy(self.c)
        expected = {n["name"]: cluster.properties(self.c, n) for n in self.c["nodes"]}
        with tempfile.TemporaryDirectory() as td:
            rendered = config.render(self.c, Path(td))
            self.assertEqual({p.name for p in rendered.iterdir()}, {"master", "worker1", "worker2"})
            for n in config.selected_nodes(self.c):
                files = {p.name for p in (rendered / n["name"]).iterdir()}
                self.assertEqual(files, {"hive-site.xml", "node.env", "node.sh"})
                ET.parse(rendered / n["name"] / "hive-site.xml")
                self.assertNotIn(b"\r\n", (rendered / n["name"] / "node.env").read_bytes())
        self.assertEqual(self.c, original)
        self.assertEqual({n["name"]: cluster.properties(self.c, n) for n in self.c["nodes"]}, expected)

    def test_rejects_paths_versions_roles_ports_and_secrets(self):
        invalid = [
            ("install_root", "/"), ("install_root", "/home/team/../teachbase-hive"),
            ("install_root", "/home/team/teachbase-hadoop/teachbase-hive"),
            ("version", "4.0.0"), ("server_node", "master"),
            ("metastore_port", 5432), ("server_port", 10020),
            ("database", "hive; DROP DATABASE postgres"),
            ("database_user", "a'b"), ("hive_table", "sales-name"),
            ("database_password", "private"), ("client_secret", "private"),
            ("server_heap_mb", 4096), ("warehouse", "/"),
        ]
        for key, value in invalid:
            with self.subTest(key=key, value=value):
                c = copy.deepcopy(self.c)
                c["hive"] = {key: value}
                with self.assertRaises(ValueError):
                    config.validate(c)
        self.c["ssh_user"] = "root"
        with self.assertRaises(ValueError):
            config.validate(self.c)

    def test_hive_settings_use_inventory_addresses(self):
        self.c["nodes"][0]["private_ip"] = "10.20.0.11"
        self.c["nodes"][2]["private_ip"] = "10.20.0.12"
        h = config.settings(self.c)
        self.assertEqual(h["metastore_ip"], "10.20.0.11")
        self.assertEqual(h["server_ip"], "10.20.0.12")
        self.assertEqual(h["hive_database"], "hw3_lab")
        self.assertEqual(h["hive_table"], "sales")

    def test_preview_cannot_connect_to_remote_hosts(self):
        with tempfile.TemporaryDirectory() as td:
            args = ["deploy.py", "--inventory", str(ROOT / "inventory.example.json"),
                    "--output", td, "--action", "deploy"]
            with patch.object(sys, "argv", args), \
                    patch.object(cluster, "remote", side_effect=AssertionError("Preview used SSH")), \
                    patch.object(deploy, "deploy", side_effect=AssertionError("Preview ran deployment")):
                deploy.main()
            self.assertTrue((Path(td) / "master/hive-site.xml").is_file())

    def test_bad_cached_archive_is_rejected_without_network(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "downloads").mkdir()
            (root / "downloads" / config.ARCHIVE).write_bytes(b"corrupt archive")
            with patch.object(deploy.subprocess, "run", side_effect=AssertionError("Unexpected network")):
                with self.assertRaisesRegex(RuntimeError, "SHA-256"):
                    deploy.ensure_archive({"install_root": str(root)})

    def test_failed_download_is_cleaned_without_changing_existing_files(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "downloads").mkdir()
            preserved = root / "downloads/keep.txt"
            preserved.write_text("keep")
            def fail(*args, **kwargs):
                (root / "downloads" / (config.ARCHIVE + ".part")).write_bytes(b"partial")
                raise subprocess.CalledProcessError(7, "curl")
            with patch.object(deploy.subprocess, "run", side_effect=fail):
                with self.assertRaises(subprocess.CalledProcessError):
                    deploy.ensure_archive({"install_root": str(root)})
            self.assertEqual(preserved.read_text(), "keep")
            self.assertFalse((root / "downloads" / (config.ARCHIVE + ".part")).exists())

    def test_staging_path_from_remote_is_validated_before_cleanup(self):
        with patch.object(config, "validate", return_value=config.settings(self.c)), \
                patch.object(deploy, "require_edge_platform"), \
                patch.object(cluster, "remote", return_value=subprocess.CompletedProcess([], 0, "/home/team\n")) as remote:
            with self.assertRaisesRegex(RuntimeError, "Unexpected Hive staging"):
                deploy.deploy(self.c)
        self.assertEqual(remote.call_count, 1)


if __name__ == "__main__":
    unittest.main()
