#!/usr/bin/env python3
"""Check published Hadoop HTTPS interfaces without submitting or changing jobs."""
import argparse
import base64
from datetime import datetime, timezone
import http.client
import json
from pathlib import Path
import re
import secrets
import socket
import ssl
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hw2.web import public_endpoints

PAGES = {"nn": ("/dfshealth.html", ("namenode",)),
         "snn": ("/status.html", ("secondary", "namenode")),
         "rm": ("/cluster", ("applications", "resource manager")),
         "history": ("/jobhistory", ("history",))}


def now():
    return datetime.now(timezone.utc).isoformat()


def request(host, port, method, path, authorization, context, connect_host=None):
    connection = http.client.HTTPSConnection(host, port, timeout=20, context=context)
    if connect_host:
        # Keep the original TLS SNI, certificate hostname check and Host header.
        # Only the TCP destination is overridden for an explicitly internal test.
        connection._create_connection = lambda address, timeout, source_address=None: socket.create_connection(
            (connect_host, address[1]), timeout, source_address)
    headers = {"Accept": "text/html, application/json", "User-Agent": "hadoop-homework-check/1"}
    if authorization is not None:
        headers["Authorization"] = authorization
    try:
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        body = response.read(8 * 1024 * 1024 + 1)
        if len(body) > 8 * 1024 * 1024:
            raise ValueError("Response exceeds 8 MiB verification limit")
        return response.status, response.getheader("Location"), body
    finally:
        connection.close()


def data_check(label, body):
    data = json.loads(body)
    if label == "nn":
        info = data["beans"][0]
        return len(json.loads(info["LiveNodes"])) == 3 and not json.loads(info["DeadNodes"])
    if label == "snn":
        return data["beans"][0]["LastCheckpointTime"] > 0
    if label.startswith("dn"):
        return "DataNodeInfo" in data["beans"][0]["name"]
    if label == "rm":
        rows = data["nodes"]["node"]
        return len(rows) == 3 and all(row["state"] == "RUNNING" for row in rows)
    if label.startswith("nm"):
        return data["nodeInfo"]["nodeHealthy"] is True
    if label == "history":
        return data["historyInfo"]["startedOn"] > 0
    return False


def data_path(label):
    if label in ("nn", "snn") or label.startswith("dn"):
        role = "NameNode" if label == "nn" else "SecondaryNameNode" if label == "snn" else "DataNode"
        return f"/jmx?qry=Hadoop:service={role},name={role}Info"
    if label == "rm":
        return "/ws/v1/cluster/nodes"
    if label.startswith("nm"):
        return "/ws/v1/node/info"
    return "/ws/v1/history/info"


def history_redirect_path(location, host, port, application_id):
    """Accept only the matching job on this cluster's published JobHistory endpoint."""
    if not location or not application_id:
        raise ValueError("Missing proxy redirect or application ID")
    parsed = urlsplit(location)
    expected_job = "job_" + application_id.removeprefix("application_")
    if (parsed.scheme != "https" or parsed.hostname != host.lower() or parsed.port != port
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment
            or not re.fullmatch(r"/jobhistory/job/job_[0-9]+_[0-9]+/?", parsed.path)
            or parsed.path.rstrip("/").rsplit("/", 1)[1] != expected_job):
        raise ValueError("Proxy redirect is not the expected published JobHistory job")
    return parsed.path


def verify(config, username, password, context, application_id=None, connect_host=None, requester=request):
    endpoints = public_endpoints(config)
    if not endpoints:
        raise ValueError("Enable and configure web publication in inventory first")
    if not username or ":" in username or not password:
        raise ValueError("A nonempty username without ':' and a nonempty password are required")
    if application_id is not None and not re.fullmatch(r"application_[0-9]+_[0-9]+", application_id):
        raise ValueError("Invalid application ID")
    if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED:
        raise ValueError("Certificate and hostname verification must remain enabled")
    auth = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
    wrong = "Basic " + base64.b64encode(f"{username}:{secrets.token_urlsafe(32)}".encode()).decode()
    report = {"started_at": now(), "scope": "internal" if connect_host else "external",
              "tls_verification": True, "redirects_followed": False,
              "redirect_policy": "Only the matching job on the configured HTTPS JobHistory endpoint; one hop",
              "checks": []}
    if connect_host:
        report["connect_host"] = connect_host
    _, history_host, history_port = next(endpoint for endpoint in endpoints if endpoint[0] == "history")
    history_targets = {}

    def check(label, host, port, name, method, path, authorization, expected, validator=None):
        row = {"service": label, "check": name, "method": method,
               "url": f"https://{host}:{port}{path}", "at": now(), "expected_status": list(expected)}
        try:
            status, location, body = requester(host, port, method, path, authorization, context, connect_host)
            row.update(status=status, bytes=len(body), result="pass" if status in expected else "fail")
            if location:
                # Credentials embedded by an upstream in a Location value are not logged.
                parsed = urlsplit(location)
                row["redirect_location"] = location if not parsed.username and not parsed.password else "[userinfo omitted]"
            if label == "proxy" and name in ("page", "head") and status in (301, 302, 303, 307, 308):
                target_path = history_redirect_path(location, history_host, history_port, application_id)
                if target_path not in history_targets:
                    expected_job = "job_" + application_id.removeprefix("application_")

                    def job_page(body):
                        html = body.decode("utf-8", "replace").lower()
                        return ("<html" in html and expected_job in html
                                and any(token in html for token in ("history", "mapreduce", "job overview")))

                    # This is a new authenticated request to the known cluster history endpoint,
                    # never generic redirect following. Any further redirect fails this check.
                    report["redirects_followed"] = True
                    history_targets[target_path] = check("proxy", history_host, history_port,
                                                         "proxy_history_target", "GET", target_path,
                                                         auth, (200,), job_page)
                target = history_targets[target_path]
                row["result"] = target["result"]
                row["verified_history_target"] = target["url"] if target["result"] == "pass" else None
                row["expected_status"] = [200, status]
                row["reason"] = ("Redirect target GET and job content verified; HEAD reuses this content check"
                                 if target["result"] == "pass" else "Published JobHistory redirect target failed verification")
            elif status in expected and validator is not None:
                if not validator(body):
                    row.update(result="fail", reason="Service content or health did not match")
        except Exception as exc:
            # Exception strings and response bodies can contain sensitive data.
            row.update(result="fail", error_type=type(exc).__name__)
        report["checks"].append(row)
        return row

    for label, host, port in endpoints:
        if label == "proxy":
            if not application_id:
                report["checks"].append({"service": label, "check": "application_proxy", "result": "skipped",
                                         "reason": "Supply --application-id for a running or completed application"})
                continue
            path, tokens = f"/proxy/{application_id}/", ("mapreduce", "application", "job")
        elif label.startswith("dn"):
            path, tokens = "/datanode.html", ("datanode",)
        elif label.startswith("nm"):
            path, tokens = "/node", ("node",)
        else:
            path, tokens = PAGES[label]
        first = check(label, host, port, "unauthenticated", "GET", path, None, (401,))
        if first.get("error_type"):
            # A TLS/network failure prevents every remaining request to this endpoint.
            report["checks"].append({"service": label, "check": "remaining_checks", "result": "skipped",
                                     "reason": "Endpoint request failed; see unauthenticated check"})
            continue
        check(label, host, port, "wrong_password", "GET", path, wrong, (401,))
        check(label, host, port, "page", "GET", path, auth, (200,),
              lambda body, tokens=tokens: any(token in body.decode("utf-8", "replace").lower() for token in tokens))
        check(label, host, port, "head", "HEAD", path, auth, (200,))
        # POST to a UI page with no request body does not submit a YARN application.
        check(label, host, port, "post_blocked", "POST", path, auth, (405,))
        check(label, host, port, "configuration_blocked", "GET", "/conf", auth, (403, 404))
        # Read-only WebHDFS operation: even a defective proxy cannot create a token or change data.
        check(label, host, port, "webhdfs_blocked", "GET", "/webhdfs/v1/?op=GETFILESTATUS", auth, (403, 404))
        if label != "proxy":
            check(label, host, port, "service_data", "GET", data_path(label), auth, (200,),
                  lambda body, label=label: data_check(label, body))
    results = {row["result"] for row in report["checks"]}
    report["result"] = "fail" if "fail" in results else "partial" if results & {"partial", "skipped"} else "pass"
    report["finished_at"] = now()
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password-file", type=Path, required=True, help="Local plain-text password file; never committed")
    parser.add_argument("--application-id")
    parser.add_argument("--ca-file", type=Path, help="Explicitly trusted private/test CA; hostname verification stays enabled")
    parser.add_argument("--connect-host", help="Override TCP destination for an INTERNAL diagnostic only; TLS hostname stays unchanged")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        password = args.password_file.read_text(encoding="utf-8-sig").rstrip("\r\n")
        if not password or "\r" in password or "\n" in password:
            raise ValueError("Password file must contain exactly one nonempty line")
        config = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
        context = ssl.create_default_context(cafile=str(args.ca_file) if args.ca_file else None)
        report = verify(config, args.username, password, context, args.application_id, args.connect_host)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"{report['scope']}: {report['result']}; report: {args.output}")
        return 0 if report["result"] == "pass" else 1
    except (ValueError, OSError, KeyError) as exc:
        parser.exit(1, f"Verification could not run: {type(exc).__name__}\n")


if __name__ == "__main__":
    raise SystemExit(main())
