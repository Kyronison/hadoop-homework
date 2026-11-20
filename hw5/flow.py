#!/usr/bin/env python3
"""Prefect flow: Spark session on YARN -> HDFS -> transformations -> table.

Run on edge inside the HW5 environment:
    source /home/team/teachbase-prefect/course-env.sh
    python hw5/flow.py --inventory inventory.json
"""
import argparse
from datetime import datetime, timezone
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
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from hw3 import config as hive_config
from hw3.load_data import hive_sql
from hw4.transform import executor_evidence
from hw5 import config, pipeline

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run

# Spark objects live only in this Python process: never cache or serialize them.
LOCAL = {"cache_policy": NO_CACHE, "persist_result": False}


def now():
    return datetime.now(timezone.utc).isoformat()


def shell(argv, timeout=180, check=True):
    """Run a Hadoop CLI command in the already loaded course environment."""
    result = subprocess.run(argv, text=True, capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{shlex.join(argv)} failed:\n{result.stderr[-4000:]}")
    return result


def rest(url):
    with urlopen(url, timeout=15) as response:
        return json.load(response)


@task(name="1. check-cluster", retries=2, retry_delay_seconds=10, **LOCAL)
def check_cluster(s):
    """HDFS is writable, YARN has running NodeManagers, the HW3 data is present."""
    logger = get_run_logger()
    safemode = shell(["hdfs", "dfsadmin", "-safemode", "get"]).stdout.strip()
    if "OFF" not in safemode:
        raise RuntimeError("HDFS is in safe mode: " + safemode)
    filesystem = shell(["hdfs", "getconf", "-confKey", "fs.defaultFS"]).stdout.strip().rstrip("/")
    if filesystem != s["filesystem"]:
        raise RuntimeError(f"HADOOP_CONF_DIR points to {filesystem}, expected {s['filesystem']}")
    info = rest(s["resourcemanager"] + "/ws/v1/cluster/info")["clusterInfo"]
    metrics = rest(s["resourcemanager"] + "/ws/v1/cluster/metrics")["clusterMetrics"]
    if info["state"] != "STARTED" or metrics["activeNodes"] < 1:
        raise RuntimeError("YARN ResourceManager is not started or has no active NodeManagers")
    listing = shell(["hdfs", "dfs", "-ls", "-R", s["input_path"]]).stdout
    files = sorted(line.split()[-1] for line in listing.splitlines()
                   if line.startswith("-") and line.endswith(".csv"))
    if not files:
        raise RuntimeError("No CSV files found in " + s["input_path"] + "; load the HW3 data first")
    logger.info("HDFS %s: safe mode OFF; YARN: %s active NodeManagers; input files: %s",
                filesystem, metrics["activeNodes"], len(files))
    return {"filesystem": filesystem, "safemode": "OFF", "yarn_state": info["state"],
            "hadoop_version": info["hadoopVersion"], "active_nodemanagers": metrics["activeNodes"],
            "available_mb": metrics["availableMB"], "input_files": files}


@task(name="2. start-spark-session-on-yarn", **LOCAL)
def start_spark_session(s, paths):
    """SparkSession with master=yarn: driver on edge, executors in YARN containers."""
    logger = get_run_logger()
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .appName(config.APP_NAME)
             .master("yarn")
             .config("spark.submit.deployMode", "client")
             .config("spark.driver.host", s["edge_ip"])
             .config("spark.driver.bindAddress", s["edge_ip"])
             .config("spark.executor.instances", "2")
             .config("spark.executor.cores", "1")
             .config("spark.executor.memory", "512m")
             .config("spark.sql.catalogImplementation", "in-memory")
             .config("spark.sql.warehouse.dir", paths["catalog"])
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "true")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.parquet.writeLegacyFormat", "true")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    context = spark.sparkContext
    context.setLogLevel("WARN")
    if context.master != "yarn" or not re.fullmatch(r"application_\d+_\d+", context.applicationId):
        spark.stop()
        raise RuntimeError("Spark session is not running under YARN")
    logger.info("Spark %s session started on YARN: %s", spark.version, context.applicationId)
    return spark


@task(name="3. connect-to-hdfs", **LOCAL)
def connect_hdfs(spark, s, paths):
    """Use the same Hadoop FileSystem client as Spark: check the cluster and our paths."""
    logger = get_run_logger()
    context = spark.sparkContext
    hadoop_conf = context._jsc.hadoopConfiguration()
    default_fs = hadoop_conf.get("fs.defaultFS").rstrip("/")
    if default_fs != s["filesystem"]:
        raise RuntimeError("Spark is connected to an unexpected HDFS: " + default_fs)
    jvm = context._jvm
    Path_ = jvm.org.apache.hadoop.fs.Path
    fs = Path_(default_fs + "/").getFileSystem(hadoop_conf)
    status = fs.getStatus()
    source = default_fs + s["input_path"]
    files = fs.globStatus(Path_(source + "/dt=*/*.csv"))
    inputs = sorted((str(f.getPath().toString()), int(f.getLen())) for f in files)
    if not inputs:
        raise RuntimeError("Spark cannot see the input files in HDFS")
    root = Path_(paths["root"])
    marker = Path_(paths["root"] + "/" + config.MARKER)
    if fs.exists(root):
        text = shell(["hdfs", "dfs", "-cat", paths["root"] + "/" + config.MARKER], check=False)
        if text.returncode or text.stdout.strip() != config.OWNER:
            raise RuntimeError("HW5 HDFS directory exists without its ownership marker")
    else:
        fs.mkdirs(root)
        stream = fs.create(marker, False)
        stream.writeBytes(config.OWNER + "\n")
        stream.close()
    if fs.exists(Path_(paths["run_dir"])):
        raise RuntimeError("Run directory already exists; refusing to overwrite it")
    fs.mkdirs(Path_(paths["run_dir"]))
    logger.info("Connected to %s: capacity %.1f GiB, used %.2f MiB; %s input files",
                default_fs, status.getCapacity() / 2**30, status.getUsed() / 2**20, len(inputs))
    return {"default_fs": default_fs, "capacity_bytes": int(status.getCapacity()),
            "used_bytes": int(status.getUsed()), "input_path": source,
            "input_files": [{"path": p, "bytes": n} for p, n in inputs],
            "output_path": paths["data"]}


@task(name="4. read-data-from-hdfs", **LOCAL)
def read_data(spark, source):
    """Read the CSV files loaded to HDFS in HW3; the date comes from the partition folder."""
    logger = get_run_logger()
    from pyspark.sql import functions as F, types as T
    schema = T.StructType([T.StructField(name, T.StringType(), True)
                           for name in ("sale_id", "customer_id", "amount")])
    raw = (spark.read.schema(schema)
           .option("header", "false").option("mode", "FAILFAST")
           .csv(source + "/dt=*/*.csv")
           .withColumn("source_file", F.input_file_name())
           .withColumn("dt", F.regexp_extract("source_file", r"/dt=(\d{4}-\d{2}-\d{2})/[^/]+$", 1)))
    rows = raw.count()
    if not rows:
        raise RuntimeError("Input files are empty")
    logger.info("Read %s raw rows from %s", rows, source)
    raw.printSchema()
    return raw


@task(name="5. transform-data", **LOCAL)
def transform(raw):
    """Validate and cast types, filter, aggregate per customer, derive a segment."""
    logger = get_run_logger()
    from pyspark.sql import functions as F
    valid = (F.col("sale_id").rlike(r"^[1-9][0-9]*$")
             & F.col("customer_id").rlike(r"^[1-9][0-9]*$")
             & F.col("amount").rlike(r"^-?[0-9]{1,8}(\.[0-9]{1,2})?$")
             & F.col("dt").rlike(r"^\d{4}-\d{2}-\d{2}$"))
    bad = raw.filter(~F.coalesce(valid, F.lit(False))).count()
    if bad:
        raise RuntimeError(f"Input contains {bad} malformed rows")
    typed = raw.select(
        F.col("sale_id").cast("int").alias("sale_id"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("amount").cast("decimal(10,2)").alias("amount"),
        F.to_date("dt", "yyyy-MM-dd").alias("sale_date"))
    typed.printSchema()
    summary = (typed.filter(F.col("amount") > 0)
               .groupBy("customer_id")
               .agg(F.count("sale_id").alias("order_count"),
                    F.countDistinct("sale_date").alias("active_days"),
                    F.min("sale_date").alias("first_purchase"),
                    F.max("sale_date").alias("last_purchase"),
                    F.sum("amount").cast("decimal(20,2)").alias("total_amount"),
                    F.avg("amount").cast("decimal(20,2)").alias("average_amount"),
                    F.max("amount").alias("max_amount"))
               .withColumn("segment", F.when(F.col("total_amount") >= F.lit(pipeline.HIGH_SEGMENT_FROM),
                                             F.lit("high")).otherwise(F.lit("regular")))
               .select(*[name for name, _ in pipeline.COLUMNS])
               .orderBy("customer_id"))
    actual = [(f.name, f.dataType.simpleString()) for f in summary.schema.fields]
    if actual != pipeline.COLUMNS:
        raise RuntimeError(f"Unexpected result schema: {actual}")
    source = [(r.sale_id, r.customer_id, r.amount, r.sale_date) for r in typed.collect()]
    rows = pipeline.normalize_all(r.asDict() for r in summary.collect())
    if rows != pipeline.reference_summary(source):
        raise RuntimeError("Spark aggregation differs from the reference calculation")
    totals = pipeline.check_totals(source, rows)
    logger.info("Transformed %s orders into %s customer rows, total %s",
                totals["orders"], totals["customers"], totals["total_amount"])
    summary.show(truncate=False)
    return summary, {"source_rows": len(source), "rows": rows, **totals}


@task(name="6. save-as-table", **LOCAL)
def save_table(spark, summary, s, output, expected):
    """saveAsTable writes Parquet into the run directory and registers it in Spark."""
    logger = get_run_logger()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SPARK_DATABASE}")
    (summary.coalesce(1).write.mode("errorifexists").format("parquet")
     .option("path", output).saveAsTable(s["spark_table"]))
    table = spark.catalog.getTable(s["spark_table"])
    from_table = pipeline.normalize_all(r.asDict() for r in spark.table(s["spark_table"]).collect())
    from_files = pipeline.normalize_all(r.asDict() for r in spark.read.parquet(output).collect())
    if from_table != expected or from_files != expected:
        raise RuntimeError("Saved table differs from the transformation result")
    executors = executor_evidence(spark.sparkContext)
    logger.info("Saved %s rows to %s (%s, %s)", len(from_table), s["spark_table"], table.tableType, output)
    return {"spark_table": s["spark_table"], "spark_table_type": table.tableType,
            "output_path": output, "table_readback_matches": True,
            "parquet_readback_matches": True, "executors": executors}


@task(name="7. stop-spark-session", **LOCAL)
def stop_spark_session(spark):
    context = spark.sparkContext
    application = context.applicationId
    version = spark.version
    spark.stop()
    get_run_logger().info("Spark session %s stopped", application)
    return {"application_id": application, "spark_version": version, "master": "yarn"}


@task(name="8. register-hive-table", retries=1, retry_delay_seconds=15, **LOCAL)
def register_hive_table(c, s, filesystem, output):
    """Durable metadata: an external Hive table that points to the Parquet of this run."""
    logger = get_run_logger()
    h = hive_config.settings(c)
    ddl = pipeline.hive_sql(config.HIVE_DATABASE, config.TABLE, filesystem, s["hdfs_root"], config.OWNER)
    hive_sql(c, h, ddl)
    describe = f"DESCRIBE FORMATTED {s['hive_table']};"
    owner = f"SHOW TBLPROPERTIES {s['hive_table']} ('hw5.owner');"
    pipeline.check_hive_table(pipeline.tsv_rows(hive_sql(c, h, describe)), hive_sql(c, h, owner),
                              config.OWNER, filesystem, s["hdfs_root"])
    hive_sql(c, h, pipeline.publish_sql(config.HIVE_DATABASE, config.TABLE, output))
    location = pipeline.check_hive_table(pipeline.tsv_rows(hive_sql(c, h, describe)),
                                         hive_sql(c, h, owner), config.OWNER, filesystem,
                                         s["hdfs_root"], expected_location=output)
    logger.info("Hive table %s now points to %s", s["hive_table"], location)
    return {"hive_table": s["hive_table"], "location": location, "jdbc_url": s["jdbc_url"]}


@task(name="9. verify-table", **LOCAL)
def verify_table(c, s, expected, application_id):
    """Read the table with an independent client (Beeline) after Spark has stopped."""
    logger = get_run_logger()
    h = hive_config.settings(c)
    rows = pipeline.normalize_all(pipeline.tsv_rows(hive_sql(
        c, h, pipeline.select_sql(config.HIVE_DATABASE, config.TABLE))))
    if rows != expected:
        raise RuntimeError("Beeline result differs from the Spark result")
    url = f"{s['resourcemanager']}/ws/v1/cluster/apps/{application_id}"
    for _ in range(30):
        app = rest(url)["app"]
        if app["state"] in {"FINISHED", "FAILED", "KILLED"}:
            break
        time.sleep(2)
    if (app["applicationType"] != "SPARK" or app["state"] != "FINISHED"
            or app["finalStatus"] != "SUCCEEDED"):
        raise RuntimeError(f"YARN reports {app['state']} / {app['finalStatus']} for {application_id}")
    logger.info("Beeline read %s rows; YARN %s: %s / %s", len(rows), application_id,
                app["state"], app["finalStatus"])
    return {"beeline_rows": rows, "beeline_matches_spark": True,
            "yarn": {key: app.get(key) for key in ("id", "name", "applicationType", "queue", "state",
                                                    "finalStatus", "startedTime", "finishedTime")}}


@task(name="10. write-report", **LOCAL)
def write_report(path, report):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", dir=path.parent,
                                     prefix=path.name + ".", suffix=".tmp", delete=False) as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    os.replace(stream.name, path)
    get_run_logger().info("Report written to %s", path)
    return str(path)


def current_run_id():
    try:
        return "run-" + UUID(str(flow_run.id)).hex
    except (TypeError, ValueError):
        return "run-" + uuid4().hex


@flow(name=config.FLOW_NAME, log_prints=True)
def hw5_pipeline(inventory: str = "inventory.json", report: str = "checks/prefect-runtime.json"):
    started = now()
    c = json.loads(Path(inventory).read_text(encoding="utf-8-sig"))
    s = config.validate(c, live=True)
    run_id = current_run_id()
    paths = pipeline.run_paths(s["filesystem"], s["hdfs_root"], run_id)
    print(f"Flow run {flow_run.id}: results go to {paths['run_dir']}")

    cluster = check_cluster(s)
    spark = start_spark_session(s, paths)
    try:
        hdfs = connect_hdfs(spark, s, paths)
        raw = read_data(spark, hdfs["input_path"])
        summary, transformed = transform(raw)
        saved = save_table(spark, summary, s, hdfs["output_path"], transformed["rows"])
    finally:
        session = stop_spark_session(spark)
    hive = register_hive_table(c, s, hdfs["default_fs"], hdfs["output_path"])
    verified = verify_table(c, s, transformed["rows"], session["application_id"])
    result = {
        "ok": True, "flow": config.FLOW_NAME, "flow_run_id": str(flow_run.id), "run_id": run_id,
        "started_at": started, "finished_at": now(), "prefect_version": config.VERSION,
        "cluster": cluster, "spark": session, "hdfs": hdfs,
        "transform": {k: v for k, v in transformed.items() if k != "rows"},
        "rows": transformed["rows"], "spark_table": saved, "hive": hive, "verification": verified,
    }
    write_report(report, result)
    return {"run_id": run_id, "application_id": session["application_id"],
            "rows": len(transformed["rows"]), "orders": transformed["orders"],
            "total_amount": transformed["total_amount"], "hive_table": hive["hive_table"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--inventory", type=Path, default=ROOT / "inventory.json")
    parser.add_argument("--report", type=Path, default=ROOT / "checks/prefect-runtime.json")
    args = parser.parse_args()
    if os.name != "posix" or os.geteuid() == 0:
        raise SystemExit("Run the flow on the Linux edge VM as the ordinary Hadoop account")
    c = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
    s = config.validate(c, live=True)
    import fcntl
    Path(s["install_root"]).mkdir(parents=True, exist_ok=True)
    with (Path(s["install_root"]) / ".run.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("Another HW5 flow run is active on edge")
        result = hw5_pipeline(inventory=str(args.inventory.resolve()), report=str(args.report.resolve()))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
