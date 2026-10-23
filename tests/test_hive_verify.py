import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hw3 import verify


class HiveVerificationTests(unittest.TestCase):
    def report(self):
        first = {"client": "client_1", "ok": True, "rows": copy.deepcopy(verify.EXPECTED_ROWS),
                 "opened_at": "2026-10-23T10:00:00Z", "both_connections_ready_at": "2026-10-23T10:00:02Z",
                 "query_started_at": "2026-10-23T10:00:03Z", "query_finished_at": "2026-10-23T10:01:00Z",
                 "closed_at": "2026-10-23T10:02:00Z"}
        second = dict(first, client="client_2", opened_at="2026-10-23T10:00:01Z",
                      query_finished_at="2026-10-23T10:01:30Z", closed_at="2026-10-23T10:02:01Z")
        return {"jdbc_url": "jdbc:hive2://10.10.0.12:10000/hw3_lab", "max_simultaneous_sessions": 2,
                "expected_rows": copy.deepcopy(verify.EXPECTED_ROWS), "clients": [first, second], "ok": True}

    def passed(self, report):
        return all(row["ok"] for row in verify.observed_checks(report, "jdbc:hive2://10.10.0.12:10000/hw3_lab"))

    def test_valid_overlapping_sessions(self):
        self.assertTrue(self.passed(self.report()))

    def test_sequential_sessions_fail_even_with_claimed_maximum_two(self):
        report = self.report()
        report["clients"][1]["opened_at"] = "2026-10-23T10:02:10Z"
        self.assertFalse(self.passed(report))

    def test_early_close_and_wrong_totals_fail(self):
        for broken in ("close", "total"):
            report = self.report()
            if broken == "close":
                report["clients"][0]["closed_at"] = "2026-10-23T10:01:01Z"
            else:
                report["clients"][1]["rows"][1][2] = "499.00"
            with self.subTest(broken=broken):
                self.assertFalse(self.passed(report))

    def test_missing_malformed_and_embedded_reports_fail_closed(self):
        for clients in ([], [None, None], [{}, {}]):
            report = self.report()
            report["clients"] = clients
            with self.subTest(clients=clients):
                self.assertFalse(self.passed(report))
        report = self.report()
        report["jdbc_url"] = "jdbc:hive2:///"
        self.assertFalse(self.passed(report))

    def test_preview_does_not_execute_or_create_report(self):
        output = io.StringIO()
        with patch.object(verify, "execute_script") as execute, contextlib.redirect_stdout(output):
            result = verify.main(["--inventory", str(ROOT / "inventory.example.json")])
        self.assertEqual(result, 0)
        execute.assert_not_called()
        self.assertIn("Preview only", output.getvalue())


if __name__ == "__main__":
    unittest.main()
