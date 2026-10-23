#!/usr/bin/env python3
"""Load the two synthetic sales partitions through remote HiveServer2."""
import argparse
from datetime import date
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import uuid
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from hw3 import config

OWNER = "hadoop-homework-hw3-v1"
DATA = Path(__file__).resolve().parent / "data"


def fixtures(directory=DATA):
    """Validate every input before issuing any HDFS or SQL writes."""
    result, seen = [], set()
    for path in sorted(Path(directory).glob("sales-*.csv")):
        match = re.fullmatch(r"sales-(\d{4}-\d{2}-\d{2})\.csv", path.name)
        if not match:
            raise ValueError(f"Invalid partition filename: {path.name}")
        partition = date.fromisoformat(match[1]).isoformat()
        count, total = 0, Decimal("0.00")
        with path.open(encoding="utf-8", newline="") as stream:
            # LazySimpleSerDe uses plain comma-separated fields, not RFC CSV quoting.
            for line in stream:
                row = line.rstrip("\r\n").split(",")
                if (len(row) != 3 or not re.fullmatch(r"[1-9]\d*", row[0])
                        or not re.fullmatch(r"[1-9]\d*", row[1])
                        or not re.fullmatch(r"\d{1,8}\.\d{2}", row[2])):
                    raise ValueError(f"Invalid CSV record in {path.name}: line {count + 1}")
                if max(int(row[0]), int(row[1])) > 2147483647:
                    raise ValueError("Identifiers must fit Hive INT")
                if row[0] in seen:
                    raise ValueError(f"Duplicate sale_id: {row[0]}")
                seen.add(row[0])
                count += 1
                total += Decimal(row[2])
        if not count:
            raise ValueError(f"Empty partition: {path.name}")
        result.append({"path": path, "dt": partition, "rows": count, "sum": str(total)})
    if not result:
        raise ValueError("No sales fixtures found")
    return result


def table_location(h):
    return f"{h['warehouse']}/{h['hive_database']}.db/{h['hive_table']}"


def create_sql(h):
    return f"""CREATE DATABASE IF NOT EXISTS {h['hive_database']};
CREATE EXTERNAL TABLE IF NOT EXISTS {h['hive_database']}.{h['hive_table']} (
  sale_id INT, customer_id INT, amount DECIMAL(10,2)
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{table_location(h)}'
TBLPROPERTIES ('hw3.owner'='{OWNER}', 'transactional'='false');
"""


def load_sql(h, parts, staging):
    if not re.fullmatch(r"/user/[a-z_][a-z0-9_-]*/hw3/staging/run-[a-f0-9]{32}", staging):
        raise ValueError("Unexpected HDFS staging path")
    return "".join(
        f"LOAD DATA INPATH '{staging}/{p['path'].name}' OVERWRITE INTO TABLE "
        f"{h['hive_database']}.{h['hive_table']} PARTITION (dt='{p['dt']}');\n"
        for p in parts
    )


def environment_run(h, argv, **kwargs):
    command = "source " + shlex.quote(h["install_root"] + "/course-env.sh") + " && exec " + shlex.join(argv)
    return subprocess.run(["bash", "-lc", command], text=True, **kwargs)


def hive_sql(c, h, sql):
    with tempfile.TemporaryDirectory(prefix="hw3-sql-") as temporary:
        path = Path(temporary) / "query.sql"
        path.write_text(sql, encoding="utf-8")
        result = environment_run(h, ["beeline", "-u",
            f"jdbc:hive2://{h['server_ip']}:{h['server_port']}/default", "-n", c["ssh_user"],
            "--silent=true", "--showHeader=false", "--outputformat=tsv2", "--force=false", "-f", str(path)],
            capture_output=True, timeout=600)
        if result.returncode:
            raise RuntimeError("Hive SQL failed:\n" + result.stderr[-12000:])
        return result.stdout.strip()


def check_location(rows, expected_path, filesystem):
    locations = [r[1] for r in rows if len(r) > 1 and r[0] == "Location:"]
    expected = urlsplit(filesystem)
    if len(locations) != 1:
        raise RuntimeError("Hive metadata does not contain exactly one storage location")
    observed = urlsplit(locations[0])
    if ((observed.scheme, observed.netloc, observed.path) !=
            (expected.scheme, expected.netloc, expected_path)):
        raise RuntimeError("Existing Hive table or partition points outside its dedicated HDFS location")


def check_table(c, h, parts=None, filesystem=None):
    filesystem = filesystem or f"hdfs://{c['nodes'][0]['private_ip']}:9000"
    name = f"{h['hive_database']}.{h['hive_table']}"
    owner = hive_sql(c, h, f"SHOW TBLPROPERTIES {name} ('hw3.owner');\n")
    if owner.strip() not in {OWNER, "hw3.owner\t" + OWNER}:
        raise RuntimeError("Table does not carry the expected exercise ownership property")
    description = hive_sql(c, h, f"DESCRIBE FORMATTED {name};\n")
    rows = [[cell.strip() for cell in line.split("\t")] for line in description.splitlines()]
    columns, partition_columns, section = [], [], "columns"
    for row in rows:
        if row[0] == "# Partition Information":
            section = "partitions"
        elif row[0] == "# Detailed Table Information":
            break
        elif row[0] and not row[0].startswith("#") and len(row) > 1:
            (columns if section == "columns" else partition_columns).append(row[:2])
    if (columns != [["sale_id", "int"], ["customer_id", "int"], ["amount", "decimal(10,2)"]]
            or partition_columns != [["dt", "string"]]):
        raise RuntimeError("Existing Hive table schema differs from the sales fixture")
    check_location(rows, table_location(h), filesystem)
    if not any(len(r) > 1 and r[0] == "Table Type:" and r[1] == "EXTERNAL_TABLE" for r in rows):
        raise RuntimeError("Expected an external, non-transactional sales table")
    check_storage(rows)
    if parts is not None:
        existing = set(hive_sql(c, h, f"SHOW PARTITIONS {name};\n").splitlines())
        for part in parts:
            partition = "dt=" + part["dt"]
            if partition in existing:
                description = hive_sql(c, h, f"DESCRIBE FORMATTED {name} PARTITION (dt='{part['dt']}');\n")
                partition_rows = [[cell.strip() for cell in line.split("\t")] for line in description.splitlines()]
                check_location(partition_rows, table_location(h) + "/" + partition, filesystem)
                check_storage(partition_rows)


def check_storage(rows):
    values = [tuple(cell for cell in row if cell and cell != "NULL") for row in rows]
    required = {("SerDe Library:", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
                ("InputFormat:", "org.apache.hadoop.mapred.TextInputFormat"),
                ("field.delim", ",")}
    # Hive alpha2 can omit transactional=false for external tables; absence is non-ACID.
    transactional = [row[1] for row in values if len(row) >= 2 and row[0] == "transactional"]
    if not required.issubset(set(values)) or any(value.lower() != "false" for value in transactional):
        raise RuntimeError("Existing Hive table storage format differs from the CSV fixture")


def execute(c, h, parts):
    if os.name != "posix" or os.geteuid() == 0:
        raise ValueError("Execute as the ordinary Hadoop account on the Linux edge VM")
    # This alpha release requires the managed database directory to be absent at first creation.
    # Let Metastore create it before preparing the external table's child directory.
    hive_sql(c, h, f"CREATE DATABASE IF NOT EXISTS {h['hive_database']};\n")
    location = table_location(h)
    marker = location + "/.hw3-owner"
    def hdfs(*args, check=True, capture=True):
        return environment_run(h, ["hdfs", "dfs", *args], check=check, capture_output=capture, timeout=120)
    exists = hdfs("-test", "-e", location, check=False)
    if exists.returncode not in (0, 1):
        raise RuntimeError("Cannot inspect HDFS exercise location")
    if exists.returncode == 0:
        owned = hdfs("-cat", marker, check=False)
        if owned.returncode or owned.stdout.strip() != OWNER:
            listing = hdfs("-ls", location).stdout.strip()
            if listing:
                raise RuntimeError("Existing HDFS table data lacks our ownership marker; refusing overwrite")
    hdfs("-mkdir", "-p", location)
    with tempfile.TemporaryDirectory(prefix="hw3-owner-") as temporary:
        owned = Path(temporary) / ".hw3-owner"
        owned.write_text(OWNER + "\n", encoding="utf-8")
        hdfs("-put", "-f", str(owned), marker)
    hive_sql(c, h, create_sql(h))
    filesystem = environment_run(h, ["hdfs", "getconf", "-confKey", "fs.defaultFS"],
                                 check=True, capture_output=True, timeout=60).stdout.strip()
    check_table(c, h, parts, filesystem)
    staging = f"/user/{c['ssh_user']}/hw3/staging/run-{uuid.uuid4().hex}"
    hdfs("-mkdir", "-p", staging)
    for part in parts:
        hdfs("-put", str(part["path"].resolve()), staging + "/" + part["path"].name)
    # INPATH without LOCAL refers to HDFS. LOAD moves files from this unique staging directory.
    hive_sql(c, h, load_sql(h, parts, staging))
    hdfs("-rmdir", staging)  # Only an empty, uniquely allocated directory is removed.
    observed = hive_sql(c, h, f"SHOW PARTITIONS {h['hive_database']}.{h['hive_table']};\n")
    expected = sorted("dt=" + p["dt"] for p in parts)
    if sorted(observed.splitlines()) != expected:
        raise RuntimeError(f"Unexpected Hive partitions: {observed}")
    print(json.dumps({"loaded_partitions": expected, "input_rows": sum(p["rows"] for p in parts),
                      "input_total": str(sum((Decimal(p["sum"]) for p in parts), Decimal(0))),
                      "next_check": "Run hw3/verify.py --execute to verify query results"}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "inventory.json")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    c = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
    h = config.validate(c, live=args.execute)
    parts = fixtures()
    if args.execute:
        execute(c, h, parts)
    else:
        print(create_sql(h))
        print(load_sql(h, parts, f"/user/{c['ssh_user']}/hw3/staging/run-" + "0" * 32))
        print("Preview only. Re-upload source files before each LOAD; --execute runs on edge.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(str(error))
