import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from hw3 import config, load_data


class HiveDataTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads((Path(__file__).resolve().parents[1] / "inventory.example.json").read_text())
        self.h = config.validate(self.c)

    def test_fixtures_have_exact_expected_partitions(self):
        actual = [(p["dt"], p["rows"], p["sum"]) for p in load_data.fixtures()]
        self.assertEqual(actual, [("2026-10-01", 3, "400.00"), ("2026-10-02", 3, "500.00")])

    def test_rejects_bad_source_before_writes(self):
        for filename, content in [("sales-2026-02-30.csv", "1,1,1.00\n"),
                                  ("sales-2026-10-01.csv", "1,1,NaN\n"),
                                  ("sales-2026-10-01.csv", '"1",1,1.00\n'),
                                  ("sales-2026-10-01.csv", "1,1,1.00\n1,2,2.00\n"),
                                  ("sales-2026-10-01.csv", "2147483648,1,1.00\n")]:
            with self.subTest(filename=filename, content=content), tempfile.TemporaryDirectory() as temp:
                (Path(temp) / filename).write_text(content)
                with self.assertRaises(ValueError):
                    load_data.fixtures(Path(temp))

    def test_load_is_partition_scoped_and_reads_hdfs(self):
        sql = load_data.load_sql(self.h, load_data.fixtures(), "/user/team/hw3/staging/run-" + "a" * 32)
        self.assertEqual(sql.count("LOAD DATA INPATH"), 2)
        self.assertEqual(sql.count("OVERWRITE INTO TABLE hw3_lab.sales PARTITION"), 2)
        self.assertNotIn("LOCAL", sql)
        with self.assertRaises(ValueError):
            load_data.load_sql(self.h, load_data.fixtures(), "/tmp/'; DROP TABLE sales;--")

    def test_refuses_existing_table_with_wrong_location(self):
        formatted = "sale_id\tint\ncustomer_id\tint\namount\tdecimal(10,2)\n# Partition Information\ndt\tstring\n# Detailed Table Information\nLocation:\thdfs://master:9000/other" + load_data.table_location(self.h) + "\nTable Type:\tEXTERNAL_TABLE"
        with patch.object(load_data, "hive_sql", side_effect=[load_data.OWNER, formatted]):
            with self.assertRaisesRegex(RuntimeError, "outside"):
                load_data.check_table(self.c, self.h)

    def test_refuses_unowned_existing_table(self):
        with patch.object(load_data, "hive_sql", return_value="unrelated"):
            with self.assertRaisesRegex(RuntimeError, "ownership"):
                load_data.check_table(self.c, self.h)

    def test_refuses_same_path_on_different_filesystem(self):
        with self.assertRaisesRegex(RuntimeError, "outside"):
            load_data.check_location([["Location:", "hdfs://other:9000/example"]],
                                     "/example", "hdfs://master:9000")

    def test_actual_beeline_null_cells_and_property_key(self):
        formatted = "sale_id\tint\t\ncustomer_id\tint\t\namount\tdecimal(10,2)\t\n\tNULL\tNULL\n# Partition Information\tNULL\tNULL\n# col_name\tdata_type\tcomment\ndt\tstring\t\n\tNULL\tNULL\n# Detailed Table Information\tNULL\tNULL\nLocation:\thdfs://10.10.0.10:9000" + load_data.table_location(self.h) + "\tNULL\nTable Type:\tEXTERNAL_TABLE\tNULL\nSerDe Library:\torg.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\tNULL\nInputFormat:\torg.apache.hadoop.mapred.TextInputFormat\tNULL\n\tfield.delim\t,"
        with patch.object(load_data, "hive_sql", side_effect=["hw3.owner\t" + load_data.OWNER, formatted]):
            load_data.check_table(self.c, self.h)

    def test_refuses_redirected_partition(self):
        location = load_data.table_location(self.h)
        fs = "hdfs://10.10.0.10:9000"
        formatted = "sale_id\tint\ncustomer_id\tint\namount\tdecimal(10,2)\n# Partition Information\ndt\tstring\n# Detailed Table Information\nLocation:\t" + fs + location + "\nTable Type:\tEXTERNAL_TABLE\nSerDe Library:\torg.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\nInputFormat:\torg.apache.hadoop.mapred.TextInputFormat\n\tfield.delim\t,\n\ttransactional\tfalse"
        redirected = "Location:\t" + fs + "/unrelated-data"
        with patch.object(load_data, "hive_sql", side_effect=[load_data.OWNER, formatted, "dt=2026-10-01", redirected]):
            with self.assertRaisesRegex(RuntimeError, "outside"):
                load_data.check_table(self.c, self.h, load_data.fixtures(), fs)


if __name__ == "__main__":
    unittest.main()
