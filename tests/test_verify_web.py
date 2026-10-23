import base64
import importlib.util
import json
from pathlib import Path
import ssl
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("verify_web", ROOT / "hw2/verify_web.py")
verify_web = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify_web)


class VerifyWebTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / "inventory.example.json").read_text())
        self.config["web"]["enabled"] = True
        self.context = ssl.create_default_context()
        self.auth = "Basic " + base64.b64encode(b"reviewer:secret-value").decode()
        self.calls = []

    def response(self, host, port, method, path, authorization, context, connect_host):
        self.calls.append((host, port, method, path, authorization))
        if authorization != self.auth:
            return 401, None, b"Authentication required"
        if method == "POST":
            return 405, None, b""
        if path == "/conf" or path.startswith("/webhdfs/"):
            return 403, None, b""
        if path.startswith("/jmx"):
            if "SecondaryNameNode" in path:
                body = {"beans": [{"LastCheckpointTime": 1792718505313}]}
            elif "DataNode" in path:
                body = {"beans": [{"name": "Hadoop:service=DataNode,name=DataNodeInfo"}]}
            else:
                body = {"beans": [{"LiveNodes": '{"a":{},"b":{},"c":{}}', "DeadNodes": "{}"}]}
        elif path == "/ws/v1/cluster/nodes":
            body = {"nodes": {"node": [{"state": "RUNNING"}] * 3}}
        elif path == "/ws/v1/node/info":
            body = {"nodeInfo": {"nodeHealthy": True}}
        elif path == "/ws/v1/history/info":
            body = {"historyInfo": {"startedOn": 1}}
        else:
            return 200, None, b"<html>NameNode Secondary DataNode Applications Node History MapReduce Job</html>"
        return 200, None, json.dumps(body).encode()

    def run_check(self, **kwargs):
        return verify_web.verify(self.config, "reviewer", "secret-value", self.context,
                                 requester=kwargs.pop("requester", self.response), **kwargs)

    def test_complete_run_checks_auth_methods_and_health_without_secrets_in_report(self):
        report = self.run_check(application_id="application_1792718504081_0001")
        self.assertEqual(report["result"], "pass")
        self.assertEqual(report["scope"], "external")
        self.assertEqual(len({r["service"] for r in report["checks"]}), 11)
        self.assertNotIn("secret-value", json.dumps(report))
        self.assertNotIn(self.auth, json.dumps(report))
        self.assertEqual({r["check"] for r in report["checks"] if r["service"] == "nn"},
                         {"unauthenticated", "wrong_password", "page", "head", "post_blocked", "configuration_blocked", "webhdfs_blocked", "service_data"})

    def test_missing_application_is_partial_never_pass(self):
        report = self.run_check()
        self.assertEqual(report["result"], "partial")
        self.assertEqual(report["checks"][-1]["result"], "skipped")

    def test_internal_override_is_not_external_evidence(self):
        report = self.run_check(connect_host="127.0.0.1")
        self.assertEqual(report["scope"], "internal")

    def test_generic_200_error_page_cannot_pass_service_health(self):
        def bad(*args):
            result = self.response(*args)
            if args[3].startswith("/ws/v1/cluster/nodes"):
                return 200, None, b"<html>Something went wrong</html>"
            return result
        self.assertEqual(self.run_check(requester=bad)["result"], "fail")

    def test_redirect_never_followed_or_given_credentials(self):
        def redirect(*args):
            if args[3].startswith("/proxy/") and args[2] in ("GET", "HEAD") and args[4] == self.auth:
                return 302, "http://untrusted.example/steal", b""
            return self.response(*args)
        report = self.run_check(application_id="application_1_0001", requester=redirect)
        self.assertEqual(report["result"], "fail")
        self.assertFalse(report["redirects_followed"])
        self.assertTrue(all(c[0] == self.config["web"]["public_host"] for c in self.calls))

    def test_completed_proxy_follows_only_matching_published_history_job_once(self):
        host = self.config["web"]["public_host"]
        port = self.config["web"]["port_base"] + 9
        job_path = "/jobhistory/job/job_1_0001"
        target_requests = []

        def completed(*args):
            if args[3].startswith("/proxy/") and args[2] in ("GET", "HEAD") and args[4] == self.auth:
                return 302, f"https://{host}:{port}{job_path}", b""
            if args[3] == job_path:
                target_requests.append(args)
                return 200, None, b"<html><title>MapReduce Job job_1_0001</title>Job History</html>"
            return self.response(*args)

        report = self.run_check(application_id="application_1_0001", requester=completed)
        self.assertEqual(report["result"], "pass")
        self.assertTrue(report["redirects_followed"])
        self.assertEqual(len(target_requests), 1)
        self.assertEqual(target_requests[0][:5], (host, port, "GET", job_path, self.auth))
        target = next(row for row in report["checks"] if row["check"] == "proxy_history_target")
        self.assertEqual(target["result"], "pass")
        self.assertEqual(target["status"], 200)

    def test_proxy_redirect_rejects_other_origins_protocols_paths_and_jobs(self):
        host = self.config["web"]["public_host"]
        port = self.config["web"]["port_base"] + 9
        prefix = f"https://{host}:{port}"
        for location in (f"http://{host}:{port}/jobhistory/job/job_1_0001",
                         f"https://evil.example:{port}/jobhistory/job/job_1_0001",
                         f"https://{host}:{port + 1}/jobhistory/job/job_1_0001",
                         f"https://user:pass@{host}:{port}/jobhistory/job/job_1_0001",
                         prefix + "/conf", prefix + "/jobhistory/job/job_1_0002",
                         prefix + "/jobhistory/job/job_1_0001?next=evil",
                         prefix + "/jobhistory/job/job_1_0001#fragment",
                         prefix + "/jobhistory/job/%6aob_1_0001",
                         "/jobhistory/job/job_1_0001"):
            with self.subTest(location=location), self.assertRaises(ValueError):
                verify_web.history_redirect_path(location, host, port, "application_1_0001")
        self.assertEqual(verify_web.history_redirect_path(prefix + "/jobhistory/job/job_1_0001/",
                                                         host, port, "application_1_0001"),
                         "/jobhistory/job/job_1_0001/")

    def test_history_redirect_cannot_chain_or_pass_with_placeholder_body(self):
        host = self.config["web"]["public_host"]
        port = self.config["web"]["port_base"] + 9
        for target_response in ((302, "https://evil.example/steal", b""),
                                (200, None, b"<html>Job History</html>")):
            target_requests = []

            def bad_target(*args):
                if args[3].startswith("/proxy/") and args[2] in ("GET", "HEAD") and args[4] == self.auth:
                    return 302, f"https://{host}:{port}/jobhistory/job/job_1_0001", b""
                if args[3] == "/jobhistory/job/job_1_0001":
                    target_requests.append(args)
                    return target_response
                return self.response(*args)

            with self.subTest(target_response=target_response):
                report = self.run_check(application_id="application_1_0001", requester=bad_target)
                self.assertEqual(report["result"], "fail")
                self.assertEqual(len(target_requests), 1)
                self.assertTrue(all(c[0] == host for c in self.calls))

    def test_unverified_tls_context_is_rejected_before_requests(self):
        self.context.check_hostname = False
        with self.assertRaisesRegex(ValueError, "verification"):
            self.run_check()
        self.assertEqual(self.calls, [])

    def test_tls_failure_is_not_retried_and_exception_text_is_not_logged(self):
        requester = MagicMock(side_effect=ssl.SSLError("secret-value must never be recorded"))
        report = self.run_check(application_id="application_1_0001", requester=requester)
        self.assertEqual(report["result"], "fail")
        self.assertEqual(requester.call_count, 11)
        self.assertNotIn("secret-value", json.dumps(report))

    def test_request_override_preserves_https_hostname_and_does_not_follow(self):
        connection = MagicMock()
        connection.getresponse.return_value.status = 302
        connection.getresponse.return_value.getheader.return_value = "http://elsewhere/"
        connection.getresponse.return_value.read.return_value = b""
        with patch.object(verify_web.http.client, "HTTPSConnection", return_value=connection) as create, \
                patch.object(verify_web.socket, "create_connection") as connect:
            verify_web.request("public.example", 18443, "GET", "/cluster", self.auth, self.context, "127.0.0.1")
            create.assert_called_once_with("public.example", 18443, timeout=20, context=self.context)
            connection._create_connection(("public.example", 18443), 20)
            connect.assert_called_once_with(("127.0.0.1", 18443), 20, None)
        connection.request.assert_called_once()
        connection.close.assert_called_once()
