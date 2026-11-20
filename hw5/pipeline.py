#!/usr/bin/env python3
"""Pure helpers of the HW5 flow: paths, reference aggregation, Hive DDL and checks.

The module has no third-party imports, so it is covered by the repository unit tests
on any machine. Spark and Prefect are used only in hw5/flow.py.
"""
from collections import OrderedDict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import re
from urllib.parse import urlsplit

COLUMNS = [
    ("customer_id", "int"),
    ("order_count", "bigint"),
    ("active_days", "bigint"),
    ("first_purchase", "date"),
    ("last_purchase", "date"),
    ("total_amount", "decimal(20,2)"),
    ("average_amount", "decimal(20,2)"),
    ("max_amount", "decimal(10,2)"),
    ("segment", "string"),
]
# Customers whose total spend reaches this threshold are marked as "high".
HIGH_SEGMENT_FROM = Decimal("200.00")
CENT = Decimal("0.01")
RUN_ID = re.compile(r"run-[0-9a-f]{32}")


def money(value):
    return str(Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP))


def hdfs_uri(value):
    parsed = urlsplit(value)
    if (parsed.scheme != "hdfs" or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment
            or not re.fullmatch(r"[A-Za-z0-9.-]+(?::[1-9][0-9]{0,4})?", parsed.netloc)
            or not re.fullmatch(r"/[A-Za-z0-9_./=-]*", parsed.path or "/")
            or ".." in parsed.path.split("/") or "//" in parsed.path):
        raise ValueError("Use a plain, fully qualified HDFS URI")
    parsed.port  # rejects invalid port numbers
    return parsed


def run_paths(filesystem, hdfs_root, run_id):
    """HDFS locations of one flow run. Each run writes into its own new directory."""
    filesystem = filesystem.rstrip("/")
    hdfs_uri(filesystem + "/")
    if not RUN_ID.fullmatch(run_id):
        raise ValueError("Run identifier must look like run-<32 hex digits>")
    if not re.fullmatch(r"/user/[a-z_][a-z0-9_-]*/hw5", hdfs_root):
        raise ValueError("HW5 output root must be /user/<account>/hw5")
    run_dir = f"{filesystem}{hdfs_root}/runs/{run_id}"
    return {"root": filesystem + hdfs_root, "run_dir": run_dir,
            "data": run_dir + "/data", "catalog": run_dir + "/catalog"}


def reference_summary(source_rows):
    """Plain-Python version of the Spark aggregation, used to cross-check the result.

    source_rows: iterable of (sale_id, customer_id, amount, sale_date) with
    Decimal amount and datetime.date sale_date.
    """
    groups = OrderedDict()
    for sale_id, customer_id, amount, sale_date in sorted(source_rows, key=lambda r: (r[1], r[0])):
        if not isinstance(sale_date, date):
            raise TypeError("sale_date must be a date")
        amount = Decimal(str(amount))
        if amount <= 0:
            continue
        groups.setdefault(int(customer_id), []).append((amount, sale_date))
    result = []
    for customer_id, items in groups.items():
        amounts = [amount for amount, _ in items]
        dates = [sale_date for _, sale_date in items]
        total = sum(amounts, Decimal(0))
        result.append({
            "customer_id": customer_id,
            "order_count": len(items),
            "active_days": len(set(dates)),
            "first_purchase": min(dates).isoformat(),
            "last_purchase": max(dates).isoformat(),
            "total_amount": money(total),
            "average_amount": money(total / len(items)),
            "max_amount": money(max(amounts)),
            "segment": "high" if total >= HIGH_SEGMENT_FROM else "regular",
        })
    return result


def normalize(row):
    """Bring a Spark Row dict or a Hive TSV row to one comparable representation."""
    if isinstance(row, (list, tuple)):
        if len(row) != len(COLUMNS):
            raise RuntimeError("Unexpected number of columns in the result")
        row = dict(zip([name for name, _ in COLUMNS], row))
    def day(value):
        return value.isoformat() if isinstance(value, date) else str(value).strip()
    return {
        "customer_id": int(row["customer_id"]),
        "order_count": int(row["order_count"]),
        "active_days": int(row["active_days"]),
        "first_purchase": day(row["first_purchase"]),
        "last_purchase": day(row["last_purchase"]),
        "total_amount": money(row["total_amount"]),
        "average_amount": money(row["average_amount"]),
        "max_amount": money(row["max_amount"]),
        "segment": str(row["segment"]).strip(),
    }


def normalize_all(rows):
    return sorted((normalize(row) for row in rows), key=lambda r: r["customer_id"])


def check_totals(source_rows, summary):
    """Aggregation must neither lose nor duplicate orders and money."""
    positive = [r for r in source_rows if Decimal(str(r[2])) > 0]
    if sum(r["order_count"] for r in summary) != len(positive):
        raise RuntimeError("Order count changed during aggregation")
    total = sum((Decimal(r["total_amount"]) for r in summary), Decimal(0))
    if money(total) != money(sum((Decimal(str(r[2])) for r in positive), Decimal(0))):
        raise RuntimeError("Total amount changed during aggregation")
    customers = [r["customer_id"] for r in summary]
    if len(customers) != len(set(customers)):
        raise RuntimeError("Duplicate customers in the aggregated table")
    return {"orders": len(positive), "total_amount": money(total), "customers": len(customers)}


def column_sql():
    return ",\n  ".join(f"{name} {kind.upper()}" for name, kind in COLUMNS)


def hive_sql(database, table, filesystem, hdfs_root, owner):
    """DDL of the durable Hive table; the location is switched to each new run later."""
    filesystem = filesystem.rstrip("/")
    return (f"CREATE DATABASE IF NOT EXISTS {database}\n"
            f"LOCATION '{filesystem}{hdfs_root}/database';\n"
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (\n  {column_sql()}\n)\n"
            f"STORED AS PARQUET\n"
            f"LOCATION '{filesystem}{hdfs_root}/table'\n"
            f"TBLPROPERTIES ('hw5.owner'='{owner}', 'transactional'='false');\n")


def publish_sql(database, table, location):
    hdfs_uri(location)
    return f"ALTER TABLE {database}.{table} SET LOCATION '{location}';\n"


def select_sql(database, table):
    return (f"SELECT {', '.join(name for name, _ in COLUMNS)}\n"
            f"FROM {database}.{table};\n")


def tsv_rows(text):
    return [[value.strip() for value in line.split("\t")] for line in text.splitlines() if line.strip()]


def described_columns(rows):
    columns = []
    for row in rows:
        if not row or row[0].startswith("#") or row[0] == "":
            if columns:
                break
            continue
        if len(row) > 1:
            columns.append((row[0], row[1]))
    return columns


def check_hive_table(rows, owner_text, owner, filesystem, hdfs_root, expected_location=None):
    """Validate DESCRIBE FORMATTED output before and after switching the location."""
    if owner_text.strip() not in {owner, "hw5.owner\t" + owner}:
        raise RuntimeError("Existing Hive table is not owned by this exercise")
    if described_columns(rows) != COLUMNS:
        raise RuntimeError("Hive table schema differs from the Spark output")
    values = {tuple(v for v in row if v and v != "NULL") for row in rows}
    if ("Table Type:", "EXTERNAL_TABLE") not in values:
        raise RuntimeError("Expected an EXTERNAL Hive table")
    if ("SerDe Library:", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe") not in values:
        raise RuntimeError("Hive table is not stored as Parquet")
    locations = [row[1] for row in rows if len(row) > 1 and row[0] == "Location:"]
    if len(locations) != 1:
        raise RuntimeError("Hive metadata does not contain exactly one table location")
    location = locations[0]
    base = filesystem.rstrip("/") + hdfs_root
    allowed = re.fullmatch(re.escape(base) + r"/(table|runs/run-[0-9a-f]{32}/data)", location)
    if not allowed:
        raise RuntimeError("Hive table points outside the dedicated HW5 directory")
    if expected_location is not None and location != expected_location:
        raise RuntimeError("Hive table does not point to the current flow run")
    return location
