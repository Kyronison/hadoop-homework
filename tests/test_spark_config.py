import copy
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from hw4 import config, deploy

ROOT = Path(__file__).resolve().parents[1]


class SparkConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads((ROOT / "inventory.example.json").read_text(encoding="utf-8"))
        local_temp = patch.object(tempfile, "tempdir", str(ROOT))
        local_temp.start()
        self.addCleanup(local_temp.stop)

    def test_configuration_uses_yarn_without_embedded_hive(self):
        s = config.validate(self.c)
        self.assertEqual(s["input_path"], "/user/hive/warehouse/hw3/hw3_lab.db/sales")
        self.assertEqual(s["hdfs_root"], "/user/team/hw4")
        values = config.defaults(self.c)
        self.assertEqual(values["spark.master"], "yarn")
        self.assertEqual(values["spark.submit.deployMode"], "client")
        self.assertEqual(values["spark.sql.catalogImplementation"], "in-memory")
        self.assertEqual(values["spark.yarn.populateHadoopClasspath"], "false")
        self.assertEqual(values["spark.driver.bindAddress"], "10.10.0.11")
        self.assertEqual(values["spark.yarn.am.clientModeTreatDisconnectAsFailed"], "true")
        self.assertEqual(values["spark.yarn.am.clientModeExitOnError"], "true")
        self.assertFalse(any("metastore" in k or "javax.jdo" in k for k in values))

    def test_rejects_unsafe_and_overlapping_installation_paths(self):
        paths = ["/", "/tmp/teachbase-spark", "/home/team/../teachbase-spark",
                 "/home/team//teachbase-spark", "/home/team/teachbase-spark/",
                 "/home/team/teachbase-hadoop/teachbase-spark",
                 "/home/team/teachbase-hive/teachbase-spark"]
        for root in paths:
            with self.subTest(root=root):
                c = copy.deepcopy(self.c)
                c["spark"] = {"install_root": root}
                with self.assertRaises(ValueError):
                    config.validate(c)
        c = copy.deepcopy(self.c)
        c["install_root"] = "/home/team/teachbase-spark/teachbase-hadoop"
        with self.assertRaises(ValueError):
            config.validate(c)

    def test_rejects_unknown_overrides_versions_and_nonprivate_driver(self):
        for value in ({"version": "4.0.0"}, {"master": "local[*]"}, {"password": "secret"}):
            with self.subTest(value=value):
                c = copy.deepcopy(self.c)
                c["spark"] = value
                with self.assertRaises(ValueError):
                    config.validate(c)
        self.c["nodes"][1]["private_ip"] = "8.8.8.8"
        with self.assertRaises(ValueError):
            config.validate(self.c)

    def test_preview_never_launches_commands_or_modifies_inventory(self):
        original = copy.deepcopy(self.c)
        with tempfile.TemporaryDirectory() as td:
            args = ["deploy.py", "--inventory", str(ROOT / "inventory.example.json"), "--output", td]
            with patch.object(sys, "argv", args), \
                    patch.object(deploy.subprocess, "run", side_effect=AssertionError("Preview ran a command")), \
                    patch.object(deploy, "deploy", side_effect=AssertionError("Preview deployed Spark")):
                deploy.main()
            self.assertEqual({p.name for p in Path(td).iterdir()}, {"course-env.sh", "spark-defaults.conf"})
            self.assertNotIn(b"\r\n", (Path(td) / "course-env.sh").read_bytes())
        self.assertEqual(self.c, original)

    def test_wrong_cached_checksum_never_downloads_or_overwrites(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "downloads").mkdir()
            archive = root / "downloads" / config.ARCHIVE
            archive.write_bytes(b"corrupt")
            with patch.object(deploy.subprocess, "run", side_effect=AssertionError("Unexpected network")):
                with self.assertRaisesRegex(RuntimeError, "SHA-512"):
                    deploy.ensure_archive({"install_root": str(root)})
            self.assertEqual(archive.read_bytes(), b"corrupt")

    def test_failed_download_removes_only_its_temporary_file(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache = root / "downloads"
            cache.mkdir()
            (cache / "keep.txt").write_text("keep")
            def fail(argv, **kwargs):
                Path(argv[-1]).write_bytes(b"partial")
                raise subprocess.CalledProcessError(7, argv)
            with patch.object(deploy.subprocess, "run", side_effect=fail):
                with self.assertRaises(subprocess.CalledProcessError):
                    deploy.ensure_archive({"install_root": str(root)})
            self.assertEqual({p.name for p in cache.iterdir()}, {"keep.txt"})

    def archive_members(self, names_and_types):
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as package:
            for name, kind in names_and_types:
                member = tarfile.TarInfo(name)
                member.type = kind
                if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                    member.linkname = "/tmp/outside"
                package.addfile(member)
        stream.seek(0)
        return tarfile.open(fileobj=stream, mode="r")

    def test_archive_traversal_links_devices_and_duplicate_names_are_rejected(self):
        invalid = [("/tmp/escape", tarfile.REGTYPE),
                   (config.HOME_NAME + "/../../escape", tarfile.REGTYPE),
                   ("other-root/bin/spark-submit", tarfile.REGTYPE),
                   (config.HOME_NAME + "/link", tarfile.SYMTYPE),
                   (config.HOME_NAME + "/hardlink", tarfile.LNKTYPE),
                   (config.HOME_NAME + "/device", tarfile.CHRTYPE),
                   (config.HOME_NAME + "/a\\b", tarfile.REGTYPE)]
        for entry in invalid:
            with self.subTest(entry=entry), self.archive_members([entry]) as package:
                with self.assertRaisesRegex(RuntimeError, "Unsafe"):
                    deploy.validated_members(package)
        entry = (config.HOME_NAME + "/file", tarfile.REGTYPE)
        with self.archive_members([entry, entry]) as package:
            with self.assertRaisesRegex(RuntimeError, "Duplicate"):
                deploy.validated_members(package)

    def test_unowned_nonempty_root_is_preserved(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            existing = root / "preserve.txt"
            existing.write_text("not owned by this homework")
            with patch.object(deploy.os, "getuid", return_value=root.stat().st_uid, create=True):
                with self.assertRaisesRegex(RuntimeError, "ownership marker"):
                    deploy.prepare_root({"install_root": str(root)})
            self.assertEqual(existing.read_text(), "not owned by this homework")
            self.assertEqual({p.name for p in root.iterdir()}, {"preserve.txt"})

    def test_malformed_archive_cannot_replace_existing_installation(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            home = root / config.HOME_NAME
            home.mkdir()
            (home / "keep.txt").write_text("keep")
            with self.assertRaisesRegex(RuntimeError, "archive marker"):
                deploy.install_archive({"install_root": str(root), "home": str(home)}, root / "missing.tgz")
            self.assertEqual((home / "keep.txt").read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
