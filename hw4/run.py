#!/usr/bin/env python3
"""Run Spark on YARN, publish its Parquet partitions in Hive, verify with Beeline."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from hw3 import config as hive_config
from hw3.load_data import hive_sql, check_location, table_location
from hw4 import config

OWNER = "hadoop-homework-hw4-v1"
TABLE = "hw4_lab.daily_sales"
DATES = ["2026-10-01", "2026-10-02"]
EXPECTED = [
    ["101", "2", "149.50", "74.75", "2026-10-01"],
    ["102", "1", "250.50", "250.50", "2026-10-01"],
    ["102", "1", "120.00", "120.00", "2026-10-02"],
    ["103", "1", "80.00", "80.00", "2026-10-02"],
    ["104", "1", "300.00", "300.00", "2026-10-02"],
]


def command(s, argv, **kwargs):
    return subprocess.run(["bash", "-lc", "source " + shlex.quote(s["env_file"])
                           + " && exec " + shlex.join(argv)], text=True, **kwargs)


def hdfs(s, *args, check=True):
    return command(s, ["hdfs", "dfs", *args], check=check, capture_output=True, timeout=120)


def tsv_rows(text):
    return [[v.strip() for v in line.split("\t")] for line in text.splitlines() if line.strip()]


def table_sql(filesystem, root):
    return f"""CREATE DATABASE IF NOT EXISTS hw4_lab LOCATION '{filesystem}{root}/database';
CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE} (
  customer_id INT, order_count BIGINT,
  total_amount DECIMAL(20,2), average_amount DECIMAL(20,2)
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '{filesystem}{root}/table'
TBLPROPERTIES ('hw4.owner'='{OWNER}', 'transactional'='false');
"""


def require_owned_root(s):
    root = s["hdfs_root"]
    exists = hdfs(s, "-test", "-e", root, check=False)
    if exists.returncode not in (0, 1):
        raise RuntimeError("Cannot inspect HW4 HDFS directory")
    if exists.returncode == 0:
        marker = hdfs(s, "-cat", root + "/.hw4-owner", check=False)
        if marker.returncode or marker.stdout.strip() != OWNER:
            raise RuntimeError("HW4 directory exists without its ownership marker")
    else:
        hdfs(s, "-mkdir", "-p", root)
        with tempfile.TemporaryDirectory(prefix="hw4-owner-") as temporary:
            marker = Path(temporary) / ".hw4-owner"
            marker.write_text(OWNER + "\n", encoding="utf-8")
            hdfs(s, "-put", str(marker), root + "/.hw4-owner")


def storage(rows):
    values = {tuple(v for v in row if v and v != "NULL") for row in rows}
    required = {
        ("SerDe Library:", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        ("InputFormat:", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
    }
    if not required.issubset(values):
        raise RuntimeError("Hive table has an unexpected storage format")
    if any(len(row) >= 2 and row[0] == "transactional" and row[1].lower() != "false" for row in values):
        raise RuntimeError("Expected a non-transactional Hive table")


def check_table(c, h, s, filesystem, expected_output=None):
    owner = hive_sql(c, h, f"SHOW TBLPROPERTIES {TABLE} ('hw4.owner');")
    if owner not in {OWNER, "hw4.owner\t" + OWNER}:
        raise RuntimeError("Existing Hive table is not owned by this exercise")
    rows = tsv_rows(hive_sql(c, h, f"DESCRIBE FORMATTED {TABLE};"))
    columns, parts, section = [], [], "columns"
    for row in rows:
        if row[0] == "# Partition Information":
            section = "parts"
        elif row[0] == "# Detailed Table Information":
            break
        elif row[0] and not row[0].startswith("#") and len(row) > 1:
            (columns if section == "columns" else parts).append(row[:2])
    if columns != [["customer_id", "int"], ["order_count", "bigint"],
                   ["total_amount", "decimal(20,2)"], ["average_amount", "decimal(20,2)"]] or parts != [["dt", "string"]]:
        raise RuntimeError("Hive table schema differs from the Spark output")
    if not any(row[:2] == ["Table Type:", "EXTERNAL_TABLE"] for row in rows):
        raise RuntimeError("Expected an EXTERNAL Hive table")
    check_location(rows, s["hdfs_root"] + "/table", filesystem)
    storage(rows)
    existing = hive_sql(c, h, f"SHOW PARTITIONS {TABLE};").splitlines()
    if any(p not in {"dt=" + dt for dt in DATES} for p in existing):
        raise RuntimeError("Unexpected existing Hive partition")
    for partition in existing:
        dt = partition.split("=", 1)[1]
        rows = tsv_rows(hive_sql(c, h, f"DESCRIBE FORMATTED {TABLE} PARTITION (dt='{dt}');"))
        locations = [row[1] for row in rows if len(row) > 1 and row[0] == "Location:"]
        pattern = re.escape(filesystem + s["hdfs_root"]) + r"/runs/run-[0-9a-f]{32}/data/dt=" + re.escape(dt)
        if len(locations) != 1 or not re.fullmatch(pattern, locations[0]):
            raise RuntimeError("Existing Hive partition points outside dedicated HW4 runs")
        if expected_output is not None and locations[0] != expected_output + "/dt=" + dt:
            raise RuntimeError("Hive partition does not point to the current Spark run")
        storage(rows)


def publish_sql(output):
    return "\n".join(
        f"ALTER TABLE {TABLE} ADD IF NOT EXISTS PARTITION (dt='{dt}') LOCATION '{output}/dt={dt}';\n"
        f"ALTER TABLE {TABLE} PARTITION (dt='{dt}') SET LOCATION '{output}/dt={dt}';"
        for dt in DATES)


def verify_hive_rows(rows):
    if len(rows) != len(EXPECTED):
        raise RuntimeError("Hive did not return exactly five aggregate rows")
    normalized = []
    for row in rows:
        if len(row) != 5:
            raise RuntimeError("Hive returned an unexpected number of columns")
        normalized.append([str(int(row[0])), str(int(row[1])),
                           str(Decimal(row[2]).quantize(Decimal("0.01"))),
                           str(Decimal(row[3]).quantize(Decimal("0.01"))), row[4]])
    if normalized != EXPECTED:
        raise RuntimeError("Hive result differs from the expected transformed data")
    return normalized


def yarn_result(c, app):
    if not re.fullmatch(r"application_\d+_\d+", app):
        raise RuntimeError("Spark did not report a YARN application ID")
    url = f"http://{c['nodes'][0]['private_ip']}:8088/ws/v1/cluster/apps/{app}"
    for _ in range(20):
        with urlopen(url, timeout=15) as response:
            result = json.load(response)["app"]
        if result["state"] in {"FINISHED", "FAILED", "KILLED"}:
            break
        time.sleep(1)
    if result["state"] != "FINISHED" or result["finalStatus"] != "SUCCEEDED" or result["applicationType"] != "SPARK":
        raise RuntimeError("ResourceManager did not confirm Spark FINISHED / SUCCEEDED")
    return {key: result.get(key) for key in ["id", "name", "applicationType", "state", "finalStatus", "startedTime", "finishedTime", "allocatedMB", "allocatedVCores"]}


def execute(c, s, output):
    if os.name != "posix" or os.geteuid() == 0:
        raise ValueError("Execute as the ordinary Hadoop account on edge")
    import pwd
    if pwd.getpwuid(os.geteuid()).pw_name != c["ssh_user"]:
        raise ValueError("Run as the Hadoop account specified by ssh_user")
    h = hive_config.validate(c, live=True)
    filesystem = command(s, ["hdfs", "getconf", "-confKey", "fs.defaultFS"], check=True,
                         capture_output=True, timeout=60).stdout.strip().rstrip("/")
    if filesystem != f"hdfs://{c['nodes'][0]['private_ip']}:9000":
        raise RuntimeError("HDFS authority differs from the supplied cluster")
    source = filesystem + table_location(h)
    require_owned_root(s)
    ddl = table_sql(filesystem, s["hdfs_root"])
    database_ddl, table_ddl = ddl.split(";", 1)
    hive_sql(c, h, database_ddl + ";")
    database = tsv_rows(hive_sql(c, h, "DESCRIBE DATABASE EXTENDED hw4_lab;"))
    if not any(filesystem + s["hdfs_root"] + "/database" in row for row in database):
        raise RuntimeError("Existing Hive database has an unexpected storage location")
    hive_sql(c, h, table_ddl)
    check_table(c, h, s, filesystem)
    source_before = hdfs(s, "-cat", table_location(h) + "/dt=*/*.csv").stdout
    run = "run-" + uuid4().hex
    destination = filesystem + s["hdfs_root"] + "/runs/" + run + "/data"
    evidence = Path(s["install_root"]) / "evidence" / run
    evidence.mkdir(parents=True, exist_ok=False)
    spark_report = evidence / "spark.json"
    log = evidence / "spark-submit.log"
    started = datetime.now(timezone.utc).isoformat()
    with log.open("w", encoding="utf-8") as stream:
        result = command(s, ["spark-submit", "--master", "yarn", "--deploy-mode", "client",
                             str(ROOT / "hw4/transform.py"), "--input", source,
                             "--output", destination, "--report", str(spark_report)],
                         stdout=stream, stderr=subprocess.STDOUT, timeout=900)
    if result.returncode:
        raise RuntimeError(f"Spark failed; inspect {log}\n" + log.read_text(encoding="utf-8")[-12000:])
    spark = json.loads(spark_report.read_text(encoding="utf-8"))
    if not spark.get("ok") or spark.get("master") != "yarn":
        raise RuntimeError("Spark report does not confirm the YARN run")
    application = yarn_result(c, spark["application_id"])
    hdfs(s, "-test", "-e", destination + "/_SUCCESS")
    # The previous successful run remains intact. Only our table's partition pointers change.
    hive_sql(c, h, publish_sql(destination))
    check_table(c, h, s, filesystem, expected_output=destination)
    partitions = sorted(hive_sql(c, h, f"SHOW PARTITIONS {TABLE};").splitlines())
    if partitions != ["dt=" + dt for dt in DATES]:
        raise RuntimeError("Hive did not register both expected partitions")
    query = f"SELECT customer_id, order_count, total_amount, average_amount, dt FROM {TABLE} ORDER BY dt, customer_id;"
    rows = verify_hive_rows(tsv_rows(hive_sql(c, h, query)))
    source_after = hdfs(s, "-cat", table_location(h) + "/dt=*/*.csv").stdout
    if source_before != source_after:
        raise RuntimeError("Source data changed during the HW4 run")
    listing = hdfs(s, "-ls", "-R", destination).stdout
    if not all("dt=" + dt in listing for dt in DATES) or ".parquet" not in listing:
        raise RuntimeError("Partitioned Parquet files were not found")
    report = {"ok": True, "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
              "spark": spark, "yarn": application,
              "hive": {"client": "Apache Hive Beeline via HiveServer2", "table": TABLE,
                       "query": query, "partitions": partitions, "rows": rows,
                       "partition_locations_match_current_run": True,
                       "output_rows": len(rows), "original_orders": sum(int(r[1]) for r in rows),
                       "total_amount": str(sum((Decimal(r[2]) for r in rows), Decimal(0)))},
              "source_unchanged": True, "hdfs_output": destination, "parquet_files": listing}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "application_id": application["id"], "hive_table": TABLE,
                      "hive_rows": len(rows), "orders": 6, "total": "900.00", "report": str(output)}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "inventory.json")
    parser.add_argument("--output", type=Path, default=ROOT / "checks/spark-runtime.json")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    c = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
    s = config.validate(c, live=args.execute)
    if args.execute:
        if os.name != "posix":
            raise ValueError("Execute on the Linux edge VM")
        import fcntl
        # One local edge process owns publication at a time. Hive ALTER PARTITION
        # is sequential, so this is not a cross-host transaction or reader lock.
        with (Path(s["install_root"]) / ".run.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("Another HW4 run is active on edge")
            execute(c, s, args.output)
    else:
        filesystem = f"hdfs://{c['nodes'][0]['private_ip']}:9000"
        print(table_sql(filesystem, s["hdfs_root"]))
        print(publish_sql(filesystem + s["hdfs_root"] + "/runs/run-" + "0" * 32 + "/data"))
        print("Preview only. --execute runs Spark on YARN and verifies the table with Hive Beeline.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(str(error))
