#!/usr/bin/env python3
"""Verify two concurrent remote Hive JDBC sessions against the example dataset."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from hw3 import config

EXPECTED_ROWS = [["2026-10-01", "3", "400.00"], ["2026-10-02", "3", "500.00"]]
TIMEOUT_SECONDS = 600


def now():
    return datetime.now(timezone.utc).isoformat()


def jdbc_url(settings):
    return f"jdbc:hive2://{settings['server_ip']}:{settings['server_port']}/{settings['hive_database']}"


def build_script(settings, user, source, build_dir, raw_report):
    """Arguments are shell-quoted; only the project-owned environment file is sourced."""
    q = shlex.quote
    environment = str(Path(settings["install_root"]) / "course-env.sh")
    return "\n".join([
        "set -euo pipefail",
        f"source {q(environment)}",
        'test -x "$JAVA_HOME/bin/javac"',
        'test -x "$JAVA_HOME/bin/java"',
        f'test -f "$HIVE_HOME/jdbc/hive-jdbc-{config.VERSION}-standalone.jar"',
        f'"$JAVA_HOME/bin/javac" -encoding UTF-8 -d {q(str(build_dir))} {q(str(source))}',
        f'jdbc_cp={q(str(build_dir))}:"$HIVE_HOME/jdbc/hive-jdbc-{config.VERSION}-standalone.jar:$HIVE_HOME/lib/*"',
        'if [[ -n "${HADOOP_HOME:-}" ]]; then',
        '  jdbc_cp="$jdbc_cp:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*"',
        'fi',
        f'exec "$JAVA_HOME/bin/java" -Xmx{settings["client_heap_mb"]}m -cp "$jdbc_cp" ConcurrentClients '
        f'{q(jdbc_url(settings))} {q(user)} {q(str(raw_report))} {q(settings["hive_table"])}',
    ]) + "\n"


def observed_checks(report, expected_url):
    """Recompute success instead of trusting the Java report's top-level flag."""
    checks = []

    def check(name, passed):
        checks.append({"name": name, "ok": bool(passed)})

    target = urlsplit(expected_url.removeprefix("jdbc:"))
    check("remote_jdbc_endpoint", target.scheme == "hive2" and bool(target.hostname)
          and target.port is not None and bool(target.path.strip("/")) and report.get("jdbc_url") == expected_url)
    check("two_simultaneous_connections", report.get("max_simultaneous_sessions") == 2)
    check("expected_fixture", report.get("expected_rows") == EXPECTED_ROWS)
    clients = report.get("clients", [])
    check("two_client_results", isinstance(clients, list) and len(clients) == 2)
    if not isinstance(clients, list) or len(clients) != 2:
        return checks
    check("client_results_are_objects", all(isinstance(item, dict) for item in clients))
    if not all(isinstance(item, dict) for item in clients):
        return checks
    check("distinct_client_labels", {item.get("client") for item in clients} == {"client_1", "client_2"})
    for index, client in enumerate(clients, 1):
        check(f"client_{index}_expected_rows", client.get("ok") is True and client.get("rows") == EXPECTED_ROWS
              and not client.get("error"))
    try:
        times = [{key: datetime.fromisoformat(client[key].replace("Z", "+00:00")) for key in
                  ("opened_at", "both_connections_ready_at", "query_started_at", "query_finished_at", "closed_at")}
                 for client in clients]
        check("both_open_before_queries", max(item["opened_at"] for item in times)
              <= min(item["query_started_at"] for item in times))
        check("both_held_until_queries_finish", max(item["query_finished_at"] for item in times)
              <= min(item["closed_at"] for item in times))
        check("overlapping_connection_intervals", max(item["opened_at"] for item in times)
              < min(item["closed_at"] for item in times))
        check("ordered_timestamps", all(item["opened_at"] <= item["both_connections_ready_at"]
              <= item["query_started_at"] <= item["query_finished_at"] <= item["closed_at"] for item in times))
    except (KeyError, TypeError, ValueError):
        check("complete_connection_timestamps", False)
    return checks


def execute_script(script):
    process = subprocess.Popen(["bash", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, encoding="utf-8", errors="replace", start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=TIMEOUT_SECONDS)
        return process.returncode, stdout, stderr, False
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            stdout, stderr = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate()
        return process.returncode, stdout, stderr, True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "inventory.json")
    parser.add_argument("--execute", action="store_true", help="Run live JDBC queries from edge")
    parser.add_argument("--output", type=Path, default=ROOT / "checks" / "hive-runtime.json")
    args = parser.parse_args(argv)
    c = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
    settings = config.validate(c, live=args.execute)
    url = jdbc_url(settings)
    if not args.execute:
        print("Preview only; no connections or queries were performed.")
        print(f"Source environment: {settings['install_root']}/course-env.sh")
        print(f"Endpoint: {url}")
        print(f"Compile {Path(__file__).with_name('ConcurrentClients.java').name} in a temporary directory.")
        print(f"Open two JDBC connections; run aggregate queries on {settings['hive_database']}.{settings['hive_table']}.")
        print("Expect both sessions open together; partition row counts 3 + 3 and totals 400.00 + 500.00.")
        print(f"Observed report: {args.output}")
        return 0
    if os.name != "posix":
        raise ValueError("Execute this verifier on the Linux edge node")
    report = {"started_at": now(), "scope": "private_cluster_jdbc", "jdbc_url": url,
              "fixture_rows": 6, "fixture_total": "900.00", "ok": False}
    with tempfile.TemporaryDirectory(prefix="hw3-jdbc-check-") as temporary:
        build_dir = Path(temporary)
        raw_path = build_dir / "jdbc-results.json"
        script = build_script(settings, c["ssh_user"], Path(__file__).with_name("ConcurrentClients.java"), build_dir, raw_path)
        returncode, stdout, stderr, timed_out = execute_script(script)
        report.update({"process_returncode": returncode, "timed_out": timed_out,
                       "stdout_tail": stdout[-6000:], "stderr_tail": stderr[-12000:]})
        # These are observed log IDs only; existence alone does not prove application success.
        report["observed_yarn_application_ids"] = sorted(set(re.findall(r"application_[0-9]+_[0-9]+", stdout + stderr)))
        if raw_path.is_file():
            try:
                raw = json.loads(raw_path.read_text(encoding="utf-8"))
                if not isinstance(raw, dict):
                    raise ValueError("JDBC report is not an object")
                report["jdbc"] = raw
                report["checks"] = observed_checks(raw, url)
                report["ok"] = returncode == 0 and not timed_out and all(item["ok"] for item in report["checks"])
            except (ValueError, TypeError, KeyError) as error:
                report["error"] = "Invalid JDBC result: " + str(error)
        else:
            report["error"] = "JDBC verification did not produce a result file"
    report["finished_at"] = now()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"{'PASS' if report['ok'] else 'FAIL'}: Hive concurrent clients; observed report: {args.output}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
