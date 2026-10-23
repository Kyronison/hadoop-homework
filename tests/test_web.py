import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("web", ROOT / "hw2/web.py")
web = importlib.util.module_from_spec(spec)
spec.loader.exec_module(web)
with patch.dict(sys.modules, {"web": web}):
    deploy_spec = importlib.util.spec_from_file_location("deploy_web", ROOT / "hw2/deploy_web.py")
    deploy = importlib.util.module_from_spec(deploy_spec)
    deploy_spec.loader.exec_module(deploy)


def inventory():
    c = json.loads((ROOT / "inventory.example.json").read_text())
    c["web"] = {"enabled": True, "public_host": "203.0.113.8", "port_base": 18443,
                "certificate": "/etc/ssl/course/fullchain.pem", "certificate_key": "/etc/ssl/course/key.pem",
                "auth_file": "/etc/nginx/teachbase-htpasswd", "allowed_cidrs": []}
    for n in c["nodes"]:
        n["aliases"] = [n["ssh_host"] + ".internal"]
    return c


class WebTests(unittest.TestCase):
    def test_disabled_by_default(self):
        for setting in ({}, {"enabled": False}):
            c = inventory()
            c["web"] = setting
            self.assertEqual(web.nginx(c), "")
            self.assertEqual(web.public_endpoints(c), [])

    def test_all_services_and_aliases_map_to_distinct_tls_ports(self):
        c = inventory()
        conf = web.nginx(c)
        self.assertEqual(web.public_endpoints(c), [(label, "203.0.113.8", 18443 + i) for i, label in enumerate(web.LABELS)])
        for i, (_, host, port) in enumerate(web.public_endpoints(c)):
            self.assertIn(f"listen {port} ssl;", conf)
            node = c["nodes"][web.NODE_INDEX[i]]
            self.assertIn(f"proxy_pass http://{node['private_ip']}:{web.PORTS[i]};", conf)
            for alias in [node["private_ip"], node["ssh_host"], *node["aliases"]]:
                self.assertIn(f"sub_filter '{alias}:{web.PORTS[i]}' '{host}:{port}';", conf)
                self.assertIn(f"proxy_redirect http://{alias}:{web.PORTS[i]}/ https://{host}:{port}/;", conf)
        self.assertEqual(conf.count("auth_basic_user_file "), 11)
        self.assertEqual(conf.count('proxy_set_header Authorization "";'), 11)
        self.assertEqual(conf.count("location / { return 404; }"), 11)
        self.assertEqual(conf.count('if ($request_method !~ ^(GET|HEAD)$) { return 405; }'), 11)
        self.assertIn("upgrade-insecure-requests", conf)

    def test_sensitive_paths_never_published(self):
        for route in ("/webhdfs/v1/file", "/logLevel", "/conf", "/fsck", "/imagetransfer",
                      "/proxy/application_1_1/logLevel", "/jobhistory/conf/job_1_1", "/stacks", "/prof"):
            self.assertIsNotNone(re.search(web.SENSITIVE, route, re.I), route)
        for label in web.LABELS:
            self.assertFalse(any(re.search(pattern, "/admin/unknown") for pattern in web._routes(label)))
        self.assertFalse(any(re.search(pattern, "/ws/v1/cluster/delegation-token") for pattern in web._routes("rm")))

    def test_head_uses_get_upstream_without_forwarding_request_body(self):
        conf = web.nginx(inventory())
        self.assertEqual(conf.count("proxy_method GET;"), 11)
        self.assertEqual(conf.count("proxy_pass_request_body off;"), 11)
        self.assertEqual(conf.count('proxy_set_header Content-Length "";'), 11)
        # Method normalization must not let a write-method request reach Hadoop.
        self.assertEqual(conf.count('if ($request_method !~ ^(GET|HEAD)$) { return 405; }'), 11)

    def test_expected_ui_routes_and_proxy_boundaries(self):
        examples = {"nn": "/dfshealth.html", "snn": "/status.html", "dn1": "/datanode.html",
                    "rm": "/cluster/nodes", "nm1": "/node/containerlogs/container_1/team",
                    "history": "/jobhistory/job/job_1_1", "proxy": "/proxy/application_1_1/mapreduce/job/job_1_1"}
        for label, route in examples.items():
            self.assertTrue(any(re.search(pattern, route) for pattern in web._routes(label)), route)
        for route in ("/proxy/http://evil.example/", "/proxy/application_1_1/logLevel",
                      "/proxy/application_1_1/arbitrary-action", "/proxy/application_1_1;bad/"):
            self.assertIsNone(re.search(web.PROXY_UI, route), route)

    def test_only_observed_hdfs_root_scripts_are_allowed(self):
        scripts = {"nn": "/dfshealth.js", "snn": "/snn.js",
                   "dn1": "/dn.js", "dn2": "/dn.js", "dn3": "/dn.js"}
        for label in web.LABELS:
            patterns = web._routes(label)
            for path in ("/dfshealth.js", "/snn.js", "/dn.js", "/arbitrary.js"):
                with self.subTest(label=label, path=path):
                    allowed = any(re.search(pattern, path) for pattern in patterns)
                    self.assertEqual(allowed, path == scripts.get(label))

    def test_reject_invalid_config_and_injection(self):
        bad = [("public_host", "x; include /tmp/bad;"), ("public_host", "https://example.org"),
               ("public_host", "256.3.4.5"), ("public_host", "127.0.0.1"), ("public_host", "-bad.example"),
               ("port_base", True), ("port_base", 65526), ("port_base", 80), ("port_base", 9870),
               ("certificate", "/tmp/a;return 200;"), ("certificate_key", "/etc/../tmp/key"),
               ("auth_file", "relative"), ("allowed_cidrs", ["0.0.0.0/0"]), ("allowed_cidrs", ["192.0.2.7/24"]),
               ("enabled", "true")]
        for key, value in bad:
            c = inventory()
            c["web"][key] = value
            with self.subTest(key=key, value=value), self.assertRaises((ValueError, KeyError)):
                web.nginx(c)
        c = inventory()
        c["nodes"][1]["aliases"] = ["node';return 200;"]
        with self.assertRaises(ValueError):
            web.nginx(c)

    def test_optional_allowlist_keeps_auth_required(self):
        c = inventory()
        c["web"]["allowed_cidrs"] = ["192.0.2.9/32"]
        conf = web.nginx(c)
        self.assertEqual(conf.count("allow 192.0.2.9/32;"), 11)
        self.assertEqual(conf.count("deny all;"), 11)
        self.assertNotIn("satisfy any", conf)

    def test_deploy_preview_does_not_require_linux_or_credentials(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            path = Path(td) / "inventory.json"
            path.write_text(json.dumps(inventory()))
            result = subprocess.run([sys.executable, str(ROOT / "hw2/deploy_web.py"), "--inventory", str(path)], text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Preview only", result.stdout)
            self.assertIn(web.MARKER, result.stdout)

    def test_failed_nginx_validation_restores_previous_config(self):
        c = inventory()
        previous = web.MARKER + "\n# previous known-good configuration\n"
        actions = []
        checks = 0

        def run(*args, capture=False):
            nonlocal checks
            actions.append(args)
            output = ""
            if args[:2] == ("nginx", "-t"):
                checks += 1
                if checks == 2:
                    raise subprocess.CalledProcessError(1, args)
            elif args[0] == "cat":
                output = previous
            elif args[0] == "mktemp":
                output = "/etc/nginx/conf.d/.teachbase-hadoop.ABC12345\n"
            elif "subjectAltName" in args:
                output = "X509v3 Subject Alternative Name: IP Address:203.0.113.8\n"
            elif "-pubkey" in args or "-pubout" in args:
                output = "MATCHING PUBLIC KEY\n"
            if args[0] == "install" and "previous.conf" in args[-2]:
                self.assertEqual(Path(args[-2]).read_text(), previous)
            return SimpleNamespace(stdout=output)

        tempdir = tempfile.TemporaryDirectory
        with patch.object(deploy, "os", SimpleNamespace(name="posix")), \
                patch.object(deploy.socket, "gethostname", return_value=c["nodes"][1]["ssh_host"]), \
                patch.object(deploy.shutil, "which", side_effect=lambda name: "/usr/bin/" + name), \
                patch.object(deploy, "sudo", side_effect=run), \
                patch.object(deploy.subprocess, "run", return_value=SimpleNamespace(returncode=0)), \
                patch.object(deploy.tempfile, "TemporaryDirectory", side_effect=lambda **kw: tempdir(dir=ROOT, **kw)):
            with self.assertRaises(subprocess.CalledProcessError):
                deploy.install(c)
        self.assertEqual(checks, 3)
        self.assertTrue(any(a[0] == "install" and "previous.conf" in a[-2] for a in actions))
        self.assertEqual(actions.count(("systemctl", "reload", "nginx")), 1)

    def test_refuses_to_replace_unrelated_nginx_config(self):
        c = inventory()

        def run(*args, capture=False):
            if args[0] == "cat":
                return SimpleNamespace(stdout="# someone else's config\n")
            if "subjectAltName" in args:
                return SimpleNamespace(stdout="X509v3 Subject Alternative Name: IP Address:203.0.113.8\n")
            return SimpleNamespace(stdout="MATCHING PUBLIC KEY\n")

        with patch.object(deploy, "os", SimpleNamespace(name="posix")), \
                patch.object(deploy.socket, "gethostname", return_value=c["nodes"][1]["ssh_host"]), \
                patch.object(deploy.shutil, "which", return_value="/usr/bin/tool"), \
                patch.object(deploy, "sudo", side_effect=run) as sudo, \
                patch.object(deploy.subprocess, "run", return_value=SimpleNamespace(returncode=0)):
            with self.assertRaisesRegex(ValueError, "unrelated"):
                deploy.install(c)
        self.assertFalse(any(call.args[0] in ("install", "mv", "rm") for call in sudo.call_args_list))


if __name__ == "__main__":
    unittest.main()
