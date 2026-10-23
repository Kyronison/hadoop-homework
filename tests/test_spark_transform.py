from copy import deepcopy
from decimal import Decimal
import unittest

from hw4 import transform


class SparkTransformTests(unittest.TestCase):
    def setUp(self):
        self.source = "hdfs://cluster-master:9000" + transform.SOURCE_PATH
        self.output = "hdfs://cluster-master:9000/user/team/hw4/runs/run-" + "a" * 32 + "/data"

    def test_requires_dedicated_hdfs_paths_on_same_filesystem(self):
        transform.validate_paths(self.source, self.output)
        invalid = [self.output.replace("cluster-master", "other"),
                   self.output.replace("hdfs://", "file://"),
                   self.output.replace("/hw4/", "/hw3/"), self.output + "/..",
                   self.output.replace("run-", "run-not-hex-"), self.output + "?action=delete",
                   self.output.replace("hdfs://", "hdfs://user:secret@"),
                   self.output.replace(":9000", ":99999")]
        for output in invalid:
            with self.subTest(output=output), self.assertRaises(ValueError):
                transform.validate_paths(self.source, output)
        with self.assertRaises(ValueError):
            transform.validate_paths(self.source + "/dt=2026-10-01", self.output)

    def test_default_filesystem_root_is_valid_but_not_an_input_or_output(self):
        root = "hdfs://cluster-master:9000/"
        parsed = transform.hdfs_uri(root)
        self.assertEqual((parsed.scheme, parsed.netloc, parsed.path),
                         ("hdfs", "cluster-master:9000", "/"))
        with self.assertRaises(ValueError):
            transform.validate_paths(root, self.output)
        with self.assertRaises(ValueError):
            transform.validate_paths(self.source, root)

    def test_checks_exact_groups_not_only_totals(self):
        swapped = deepcopy(transform.EXPECTED_ROWS)
        swapped[0]["customer_id"] = 999
        self.assertEqual(sum(Decimal(row["total_amount"]) for row in swapped), Decimal("900.00"))
        with self.assertRaises(RuntimeError):
            transform.validate_result(swapped)
        duplicated = deepcopy(transform.EXPECTED_ROWS) + [deepcopy(transform.EXPECTED_ROWS[0])]
        with self.assertRaises(RuntimeError):
            transform.validate_result(duplicated)

    def test_average_and_count_are_verified_independently(self):
        for field, value in [("average_amount", "149.50"), ("order_count", 1)]:
            changed = deepcopy(transform.EXPECTED_ROWS)
            changed[0][field] = value
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                transform.validate_result(changed)

    def test_accepts_unordered_decimal_rows_and_preserves_exact_money(self):
        rows = deepcopy(transform.EXPECTED_ROWS)[::-1]
        for row in rows:
            row["total_amount"] = Decimal(row["total_amount"])
            row["average_amount"] = Decimal(row["average_amount"])
        self.assertEqual(transform.validate_result(rows), transform.EXPECTED_ROWS)


if __name__ == "__main__":
    unittest.main()
