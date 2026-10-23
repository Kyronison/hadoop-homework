import copy
import importlib.util
import io
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, file)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


cluster = load("cluster", ROOT / "common/cluster.py")
health = load("health", ROOT / "common/check.py")


class SolutionTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads((ROOT / "inventory.example.json").read_text())

    def test_render_four_nodes_and_replication(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            out = cluster.render(self.c, Path(td))
            for node in self.c["nodes"]:
                d = out / node["name"]
                for f in d.glob("*.xml"):
                    ET.parse(f)
                    self.assertNotIn(b"\r\n", f.read_bytes())
                props = health.read_xml(d / "hdfs-site.xml")
                self.assertEqual(props["dfs.replication"], "3")
                self.assertEqual(props["dfs.datanode.hostname"], node["private_ip"])
                self.assertEqual(len((d / "workers").read_text().split()), 3)
                self.assertEqual(health.read_xml(d / "mapred-site.xml")["mapreduce.framework.name"], "yarn")
                self.assertTrue((d / "node.sh").is_file())

    def test_render_without_public_https(self):
        self.c["web"] = {"enabled": False}
        self.c["yarn_memory_mb"] = 1536
        cluster.validate(self.c)
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            out = cluster.render(self.c, Path(td))
            self.assertFalse((out / "nginx.conf").exists())
            self.assertTrue((out / "master/core-site.xml").is_file())

    def test_disabled_web_rejects_stale_nginx_config(self):
        self.c["web"] = {"enabled": False}
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            out = Path(td)
            stale = out / "nginx.conf"
            original = b"# Previous public endpoint configuration\n"
            stale.write_bytes(original)
            with self.assertRaisesRegex(ValueError, "Web disabled but old nginx.conf exists"):
                cluster.render(self.c, out)
            self.assertEqual(stale.read_bytes(), original)

    def test_reject_duplicate_worker_ip(self):
        self.c["nodes"][2]["private_ip"] = self.c["nodes"][1]["private_ip"]
        with self.assertRaises(ValueError): cluster.validate(self.c)

    def test_reject_dangerous_root_and_path(self):
        for key, value in [("ssh_user", "root"), ("install_root", "/"), ("install_root", "/home/u/../teachbase-hadoop")]:
            c = copy.deepcopy(self.c)
            c[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError): cluster.validate(c)

    def test_reject_shell_injection_in_host(self):
        self.c["nodes"][0]["ssh_host"] = 'host;touch /tmp/injected'
        with self.assertRaises(ValueError): cluster.validate(self.c)

    def test_live_rejects_documentation_addresses(self):
        self.c["nodes"][0]["ssh_host"] = "192.0.2.10"
        with self.assertRaisesRegex(ValueError, "example SSH"):
            cluster.validate(self.c, live=True)

    def test_live_rejects_local_node_on_wrong_host(self):
        for node in self.c["nodes"]:
            node["ssh_host"] = "course-" + node["name"]
        self.c["nodes"][1]["local"] = True
        with patch.object(cluster.socket, "gethostname", return_value="operator-laptop"), \
                patch.object(Path, "is_file", return_value=True):
            with self.assertRaisesRegex(ValueError, "Local node does not match this host"):
                cluster.validate(self.c, live=True)

    def test_matching_local_node_runs_bash_without_ssh(self):
        for node in self.c["nodes"]:
            node["ssh_host"] = "course-" + node["name"]
        local = self.c["nodes"][1]
        local["local"] = True
        command = "printf '%s\\n' 'probe;literal'"
        with patch.object(cluster.socket, "gethostname", return_value=local["ssh_host"]), \
                patch.object(Path, "is_file", return_value=True), \
                patch.object(cluster, "ssh_options", side_effect=AssertionError("Local execution must not use SSH")), \
                patch.object(cluster.subprocess, "run") as run:
            cluster.validate(self.c, live=True)
            result = cluster.remote(self.c, local, command, capture=True)
        run.assert_called_once_with(["bash", "-lc", command], check=True, text=True, capture_output=True)
        self.assertIs(result, run.return_value)

    def test_no_remote_without_execute(self):
        for action in ("hdfs", "yarn"):
            with self.subTest(action=action):
                result = subprocess.run([sys.executable, str(ROOT / "common/cluster.py"), action,
                                         "--inventory", str(ROOT / "inventory.example.json")], capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("--execute", result.stderr)

    def test_nginx_all_endpoints_allowlisted(self):
        self.c["web"]["enabled"] = True
        self.c["web"]["allowed_cidrs"] = ["192.0.2.100/32"]
        conf = cluster.nginx(self.c)
        self.assertEqual(conf.count("server_name "), 11)
        self.assertEqual(conf.count("allow 192.0.2.100/32;"), 11)
        for port in range(18443, 18454):
            self.assertIn(f"listen {port} ssl;", conf)
        self.assertNotIn("listen 9000", conf)
        for bad in (["0.0.0.0/0"], ["::/0"]):
            self.c["web"]["allowed_cidrs"] = bad
            with self.subTest(bad=bad), self.assertRaises(ValueError): cluster.nginx(self.c)

    def test_health_rejects_dead_node(self):
        payload = {"beans": [{"LiveNodes": "{}", "DeadNodes": '{"bad": {}}'}]}
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            out = cluster.render(self.c, Path(td)) / "master"
            argv = ["check.py", str(out / "core-site.xml"), str(out / "hdfs-site.xml"), str(out / "workers"), "hdfs"]
            with patch.object(sys, "argv", argv), patch.object(health, "fetch", return_value=payload), patch.object(health, "retry", side_effect=lambda fn: fn()):
                with self.assertRaisesRegex(AssertionError, "Live=0, dead=1"):
                    health.main()

    def test_health_rejects_failed_volume(self):
        live = {ip: {"xferaddr": ip + ":9866", "adminState": "In Service", "volfails": int(i == 0)}
                for i, ip in enumerate(n["private_ip"] for n in self.c["nodes"][1:])}
        payload = {"beans": [{"LiveNodes": json.dumps(live), "DeadNodes": "{}"}]}
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            out = cluster.render(self.c, Path(td)) / "master"
            argv = ["check.py", str(out / "core-site.xml"), str(out / "hdfs-site.xml"), str(out / "workers"), "hdfs"]
            with patch.object(sys, "argv", argv), patch.object(health, "fetch", return_value=payload), patch.object(health, "retry", side_effect=lambda fn: fn()):
                with self.assertRaisesRegex(AssertionError, "Failed data volumes"):
                    health.main()

    def run_yarn_health_with_hostnames(self, overrides=None):
        workers = [n["private_ip"] for n in self.c["nodes"][1:]]
        master = self.c["nodes"][0]["private_ip"]
        names = ["cluster-edge", "cluster-worker-2", "cluster-worker-3"]
        addresses = dict(zip(names, workers))
        addresses.update(overrides or {})
        live = {ip: {"xferaddr": ip + ":9866", "adminState": "In Service", "volfails": 0}
                for ip in workers}
        payloads = {
            f"http://{master}:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo":
                {"beans": [{"LiveNodes": json.dumps(live), "DeadNodes": "{}"}]},
            f"http://{master}:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState":
                {"beans": [{"NumStaleDataNodes": 0, "NumDecommissioningDataNodes": 0}]},
            f"http://{workers[0]}:9868/jmx?qry=Hadoop:service=SecondaryNameNode,name=SecondaryNameNodeInfo":
                {"beans": [{"name": "SecondaryNameNodeInfo", "LastCheckpointTime": 1792718505313}]},
            f"http://{master}:8088/ws/v1/cluster/nodes":
                {"nodes": {"node": [{"nodeHostName": name, "state": "RUNNING"} for name in names]}},
            f"http://{master}:8088/ws/v1/cluster/metrics":
                {"clusterMetrics": {"unhealthyNodes": 0, "lostNodes": 0}},
            f"http://{master}:19888/ws/v1/history/info": {"historyInfo": {}},
        }
        payloads.update({f"http://{ip}:8042/ws/v1/node/info": {"nodeInfo": {}} for ip in workers})
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            out = cluster.render(self.c, Path(td)) / "master"
            argv = ["check.py", str(out / "core-site.xml"), str(out / "hdfs-site.xml"), str(out / "workers"), "yarn"]
            report = io.StringIO()
            with patch.object(sys, "argv", argv), \
                    patch.object(sys, "stdout", report), \
                    patch.object(health, "fetch", side_effect=payloads.__getitem__), \
                    patch.object(health, "retry", side_effect=lambda fn: fn()), \
                    patch.object(health.socket, "gethostbyname", side_effect=addresses.__getitem__):
                health.main()
            return report.getvalue()

    def test_yarn_health_accepts_canonical_node_names(self):
        report = self.run_yarn_health_with_hostnames()
        self.assertIn('"YARN": "healthy"', report)
        self.assertIn('"nodeHostName": "cluster-edge"', report)

    def test_yarn_health_rejects_name_resolving_to_unexpected_ip(self):
        with self.assertRaisesRegex(AssertionError, "Unexpected NodeManagers"):
            self.run_yarn_health_with_hostnames({"cluster-worker-3": "10.10.0.99"})

    def run_daemon_scenario(self, initial="stopped", start_succeeds=True):
        """Run the real node entry point against a fake Hadoop CLI, without Java."""
        bash = Path("C:/Program Files/Git/bin/bash.exe")
        if not bash.is_file():
            found = shutil.which("bash")
            if not found: self.skipTest("Bash unavailable")
            bash = Path(found)
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            base = Path(td) / "teachbase-hadoop"
            (base / "data/name/current").mkdir(parents=True)
            (base / "data/name/current/VERSION").touch()
            (base / ".course-owned").touch()
            bin_dir = base / "fake-hadoop/bin"
            bin_dir.mkdir(parents=True)
            fake = bin_dir / "hdfs"
            fake.write_text("""#!/usr/bin/env bash
set -eu
[[ "$1" == --daemon ]]
printf '%s\\n' "$2" >> "$FAKE_CALLS"
case "$2" in
    status)
        case "$(cat "$FAKE_STATE")" in
            running) printf '%s is running as process %s.\\n' "$3" "$FAKE_LIVE_PID" ;;
            stale) printf '%s is running as process 2147483647.\\n' "$3" ;;
            *) printf '%s is stopped.\\n' "$3" ;;
        esac
        # Reproduce Hadoop 3.4.2: stopped status also returns success.
        exit 0
        ;;
    start)
        if [[ "$FAKE_START_SUCCEEDS" == 1 ]]; then printf 'running' > "$FAKE_STATE"; fi
        exit 0
        ;;
    *) exit 99 ;;
esac
""", encoding="utf-8", newline="\n")
            fake.chmod(0o755)
            (base / "fake-state").write_text(initial)
            runner = Path(td) / "node.sh"
            shutil.copyfile(ROOT / "common/node.sh", runner)
            if sys.platform == "win32":
                posix = subprocess.check_output([str(bash), "-c", 'cygpath -u "$1"', "_", str(base)], text=True).strip()
            else:
                posix = str(base)
            (Path(td) / "node.env").write_text(
                f"BASE={shlex.quote(posix)}\nVERSION=3.4.2\nNODE=master\n",
                encoding="utf-8", newline="\n")
            environment = {
                "HADOOP_HOME": posix + "/fake-hadoop",
                "FAKE_STATE": posix + "/fake-state",
                "FAKE_CALLS": posix + "/fake-calls",
                "FAKE_START_SUCCEEDS": "1" if start_succeeds else "0",
            }
            (base / "course-env.sh").write_text(
                "".join(f"export {key}={shlex.quote(value)}\n" for key, value in environment.items())
                + "export FAKE_LIVE_PID=$$\n",
                encoding="utf-8", newline="\n")
            result = subprocess.run([str(bash), str(runner), "start-hdfs"], text=True, capture_output=True)
            if "ordinary user" in result.stderr:
                self.skipTest("Non-root execution required")
            calls = base / "fake-calls"
            return result, calls.read_text().splitlines() if calls.exists() else []

    def test_stopped_status_zero_still_starts_daemon(self):
        result, calls = self.run_daemon_scenario()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["status", "start", "status"])
        self.assertIn("namenode is stopped.", result.stdout)
        self.assertIn("namenode is running as process", result.stdout)

    def test_daemon_still_stopped_after_start_fails(self):
        result, calls = self.run_daemon_scenario(start_succeeds=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, ["status", "start", "status"])
        self.assertIn("Failed to start namenode", result.stderr)

    def test_running_live_pid_skips_daemon_start(self):
        result, calls = self.run_daemon_scenario(initial="running")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["status"])

    def test_running_status_with_stale_pid_does_not_skip_start(self):
        result, calls = self.run_daemon_scenario(initial="stale")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["status", "start", "status"])

    def test_format_refuses_nonempty_metadata(self):
        bash = Path("C:/Program Files/Git/bin/bash.exe")
        if not bash.is_file():
            found = shutil.which("bash")
            if not found: self.skipTest("Bash unavailable")
            bash = Path(found)
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            base = Path(td) / "teachbase-hadoop"
            (base / "data/name").mkdir(parents=True)
            (base / "data/name/preserve-me").write_text("existing metadata")
            (base / ".course-owned").touch()
            (base / "course-env.sh").write_text("export HADOOP_HOME=/must-not-run\n")
            runner = Path(td) / "node.sh"
            shutil.copyfile(ROOT / "common/node.sh", runner)
            if sys.platform == "win32":
                posix = subprocess.check_output([str(bash), "-c", 'cygpath -u "$1"', "_", str(base)], text=True).strip()
            else:
                posix = str(base)
            (Path(td) / "node.env").write_text(f"BASE='{posix}'\nVERSION=3.4.2\nNODE=master\n")
            result = subprocess.run([str(bash), str(runner), "initialize-empty-namenode"], text=True, capture_output=True)
            if "ordinary user" in result.stderr:
                self.skipTest("Non-root execution required")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("refusing formatting", result.stderr)
            self.assertEqual((base / "data/name/preserve-me").read_text(), "existing metadata")


if __name__ == "__main__":
    unittest.main()
