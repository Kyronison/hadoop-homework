import copy
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from hw5 import config, pipeline

ROOT = Path(__file__).resolve().parents[1]
FS = "hdfs://cluster-master:9000"
SOURCE = [
    (1, 101, Decimal("100.00"), date(2026, 10, 1)),
    (2, 102, Decimal("250.50"), date(2026, 10, 1)),
    (3, 101, Decimal("49.50"), date(2026, 10, 1)),
    (4, 103, Decimal("80.00"), date(2026, 10, 2)),
    (5, 102, Decimal("120.00"), date(2026, 10, 2)),
    (6, 104, Decimal("300.00"), date(2026, 10, 2)),
]
EXPECTED = [
    ["101", "2", "1", "2026-10-01", "2026-10-01", "149.50", "74.75", "100.00", "regular"],
    ["102", "2", "2", "2026-10-01", "2026-10-02", "370.50", "185.25", "250.50", "high"],
    ["103", "1", "1", "2026-10-02", "2026-10-02", "80.00", "80.00", "80.00", "regular"],
    ["104", "1", "1", "2026-10-02", "2026-10-02", "300.00", "300.00", "300.00", "high"],
]


class PrefectConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads((ROOT / "inventory.example.json").read_text(encoding="utf-8"))

    def test_settings_reuse_existing_spark_hdfs_and_hive(self):
        s = config.validate(self.c)
        self.assertEqual(s["install_root"], "/home/team/teachbase-prefect")
        self.assertEqual(s["input_path"], "/user/hive/warehouse/hw3/hw3_lab.db/sales")
        self.assertEqual(s["hdfs_root"], "/user/team/hw5")
        self.assertEqual(s["filesystem"], "hdfs://10.10.0.10:9000")
        self.assertEqual(s["resourcemanager"], "http://10.10.0.10:8088")
        self.assertEqual(s["spark_env"], "/home/team/teachbase-spark/course-env.sh")
        self.assertEqual(s["hive_table"], "hw5_lab.customer_summary")

    def test_environment_loads_spark_and_prefect(self):
        text = config.environment(self.c)
        self.assertIn("source /home/team/teachbase-spark/course-env.sh || return", text)
        self.assertIn("source /home/team/teachbase-prefect/venv/bin/activate || return", text)
        self.assertIn("PREFECT_HOME=/home/team/teachbase-prefect/home", text)
        self.assertIn("python/lib/" + config.PY4J, text)
        self.assertNotIn("PREFECT_API_KEY", text)

    def test_rejects_unsafe_paths_versions_and_unknown_keys(self):
        for value in ({"version": "2.20.0"}, {"api_key": "secret"},
                      {"install_root": "/tmp/teachbase-prefect"},
                      {"install_root": "/home/team/../teachbase-prefect"},
                      {"install_root": "/home/team/teachbase-spark/teachbase-prefect"},
                      {"install_root": "/home/team/prefect"}):
            with self.subTest(value=value):
                c = copy.deepcopy(self.c)
                c["prefect"] = value
                with self.assertRaises(ValueError):
                    config.validate(c)

    def test_requirements_pin_the_tested_version(self):
        requirements = (ROOT / "hw5/requirements.txt").read_text(encoding="utf-8").split()
        constraints = (ROOT / "hw5/constraints.txt").read_text(encoding="utf-8").splitlines()
        self.assertEqual(requirements, ["prefect==" + config.VERSION])
        self.assertIn("prefect==" + config.VERSION, constraints)
        self.assertTrue(all("==" in line for line in constraints if line.strip()))

    def test_render_writes_only_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            out = config.render(self.c, temporary)
            self.assertEqual(sorted(p.name for p in out.iterdir()), ["course-env.sh"])


class PrefectPipelineTests(unittest.TestCase):
    def test_reference_summary_matches_expected_table(self):
        rows = pipeline.reference_summary(SOURCE)
        self.assertEqual(pipeline.normalize_all(EXPECTED), rows)
        self.assertEqual(pipeline.check_totals(SOURCE, rows),
                         {"orders": 6, "total_amount": "900.00", "customers": 4})

    def test_non_positive_amounts_are_filtered(self):
        source = SOURCE + [(7, 105, Decimal("0.00"), date(2026, 10, 2)),
                           (8, 101, Decimal("-5.00"), date(2026, 10, 2))]
        rows = pipeline.reference_summary(source)
        self.assertEqual(pipeline.normalize_all(EXPECTED), rows)
        self.assertEqual(pipeline.check_totals(source, rows)["orders"], 6)

    def test_totals_detect_lost_or_duplicated_rows(self):
        rows = pipeline.reference_summary(SOURCE)
        for broken in (rows[1:], rows + [dict(rows[0])]):
            with self.subTest(rows=len(broken)), self.assertRaises(RuntimeError):
                pipeline.check_totals(SOURCE, broken)

    def test_spark_and_hive_rows_normalize_identically(self):
        spark_row = {"customer_id": 102, "order_count": 2, "active_days": 2,
                     "first_purchase": date(2026, 10, 1), "last_purchase": date(2026, 10, 2),
                     "total_amount": Decimal("370.50"), "average_amount": Decimal("185.25"),
                     "max_amount": Decimal("250.50"), "segment": "high"}
        self.assertEqual(pipeline.normalize(spark_row), pipeline.normalize(EXPECTED[1]))
        with self.assertRaises(RuntimeError):
            pipeline.normalize(EXPECTED[1][:-1])

    def test_run_paths_are_dedicated_and_new(self):
        run = "run-" + "a" * 32
        paths = pipeline.run_paths(FS + "/", "/user/team/hw5", run)
        self.assertEqual(paths["data"], FS + "/user/team/hw5/runs/" + run + "/data")
        self.assertEqual(paths["catalog"], FS + "/user/team/hw5/runs/" + run + "/catalog")
        for fs, root, run_id in ((FS, "/user/team/hw4", run), (FS, "/user/team/hw5", "run-1"),
                                 ("file:///tmp", "/user/team/hw5", run),
                                 ("hdfs://user:secret@host:9000", "/user/team/hw5", run)):
            with self.subTest(fs=fs, root=root, run_id=run_id), self.assertRaises(ValueError):
                pipeline.run_paths(fs, root, run_id)

    def test_hive_ddl_is_external_parquet_with_owner(self):
        sql = pipeline.hive_sql("hw5_lab", "customer_summary", FS, "/user/team/hw5", config.OWNER)
        self.assertIn("CREATE EXTERNAL TABLE IF NOT EXISTS hw5_lab.customer_summary", sql)
        self.assertIn("STORED AS PARQUET", sql)
        self.assertIn("first_purchase DATE", sql)
        self.assertIn("total_amount DECIMAL(20,2)", sql)
        self.assertIn(f"'hw5.owner'='{config.OWNER}'", sql)
        self.assertNotIn("DROP", sql.upper())
        with self.assertRaises(ValueError):
            pipeline.publish_sql("hw5_lab", "customer_summary", "/user/team/hw5/table")

    def describe(self, location, kind="EXTERNAL_TABLE"):
        rows = [["# col_name", "data_type", "comment"]]
        rows += [[name, data_type, ""] for name, data_type in pipeline.COLUMNS]
        rows += [["# Detailed Table Information", "NULL", "NULL"],
                 ["Location:", location, "NULL"], ["Table Type:", kind, "NULL"],
                 ["SerDe Library:", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "NULL"]]
        return rows

    def test_hive_table_checks_owner_schema_and_location(self):
        run = FS + "/user/team/hw5/runs/run-" + "b" * 32 + "/data"
        owner = "hw5.owner\t" + config.OWNER
        self.assertEqual(pipeline.check_hive_table(self.describe(run), owner, config.OWNER, FS,
                                                   "/user/team/hw5", expected_location=run), run)
        pipeline.check_hive_table(self.describe(FS + "/user/team/hw5/table"), config.OWNER,
                                  config.OWNER, FS, "/user/team/hw5")
        cases = [
            (self.describe(run), "someone-else"),
            (self.describe(run, kind="MANAGED_TABLE"), owner),
            (self.describe(FS + "/user/team/hw4/table"), owner),
            (self.describe(run)[:3] + self.describe(run)[4:], owner),
        ]
        for rows, owner_text in cases:
            with self.subTest(owner=owner_text), self.assertRaises(RuntimeError):
                pipeline.check_hive_table(rows, owner_text, config.OWNER, FS, "/user/team/hw5")
        other = FS + "/user/team/hw5/runs/run-" + "c" * 32 + "/data"
        with self.assertRaises(RuntimeError):
            pipeline.check_hive_table(self.describe(other), owner, config.OWNER, FS,
                                      "/user/team/hw5", expected_location=run)


if __name__ == "__main__":
    unittest.main()
