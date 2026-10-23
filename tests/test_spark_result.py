import unittest

from hw4.run import EXPECTED, storage, verify_hive_rows


class HiveResultTests(unittest.TestCase):
    def test_hive_decimal_display_preserves_values(self):
        rows = [r.copy() for r in EXPECTED]
        rows[2][2] = "120"
        rows[2][3] = "120.0"
        self.assertEqual(verify_hive_rows(rows), EXPECTED)

    def test_reject_wrong_average_even_with_correct_total(self):
        rows = [r.copy() for r in EXPECTED]
        rows[0][3] = "149.50"
        with self.assertRaises(RuntimeError):
            verify_hive_rows(rows)

    def test_reject_duplicate_or_missing_groups(self):
        for rows in [EXPECTED + [EXPECTED[0]], EXPECTED[:-1], [EXPECTED[0]] + EXPECTED[:-1]]:
            with self.assertRaises(RuntimeError):
                verify_hive_rows(rows)

    def test_parquet_storage_rejects_transactional_table(self):
        rows = [["SerDe Library:", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "NULL"],
                ["InputFormat:", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "NULL"]]
        storage(rows)
        with self.assertRaises(RuntimeError):
            storage(rows + [["transactional", "true"]])


if __name__ == "__main__":
    unittest.main()
