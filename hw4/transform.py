#!/usr/bin/env python3
"""Transform the existing HW3 HDFS data with Spark on YARN, without a Hive catalog."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlsplit


SOURCE_PATH = "/user/hive/warehouse/hw3/hw3_lab.db/sales"
TABLE = "hw4_spark.daily_sales"
SCHEMA = [("customer_id", "int"), ("order_count", "bigint"),
          ("total_amount", "decimal(20,2)"), ("average_amount", "decimal(20,2)"),
          ("dt", "string")]
EXPECTED_SOURCE = [
    (1, 101, "100.00", "2026-10-01"), (2, 102, "250.50", "2026-10-01"),
    (3, 101, "49.50", "2026-10-01"), (4, 103, "80.00", "2026-10-02"),
    (5, 102, "120.00", "2026-10-02"), (6, 104, "300.00", "2026-10-02"),
]
EXPECTED_ROWS = [
    {"customer_id": 101, "order_count": 2, "total_amount": "149.50", "average_amount": "74.75", "dt": "2026-10-01"},
    {"customer_id": 102, "order_count": 1, "total_amount": "250.50", "average_amount": "250.50", "dt": "2026-10-01"},
    {"customer_id": 102, "order_count": 1, "total_amount": "120.00", "average_amount": "120.00", "dt": "2026-10-02"},
    {"customer_id": 103, "order_count": 1, "total_amount": "80.00", "average_amount": "80.00", "dt": "2026-10-02"},
    {"customer_id": 104, "order_count": 1, "total_amount": "300.00", "average_amount": "300.00", "dt": "2026-10-02"},
]


def now():
    return datetime.now(timezone.utc).isoformat()


def hdfs_uri(value):
    parsed = urlsplit(value)
    if (parsed.scheme != "hdfs" or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment
            or not re.fullmatch(r"[A-Za-z0-9.-]+(?::[1-9][0-9]{0,4})?", parsed.netloc)
            or not re.fullmatch(r"/[A-Za-z0-9_./=-]*", parsed.path)
            or ".." in parsed.path.split("/") or "//" in parsed.path):
        raise ValueError("Use a plain, fully qualified HDFS URI")
    # Evaluating .port also rejects invalid port numbers.
    parsed.port
    return parsed


def validate_paths(source, output):
    src, dst = hdfs_uri(source), hdfs_uri(output)
    if src.path != SOURCE_PATH:
        raise ValueError("Input must be the existing HW3 sales table directory")
    if (src.scheme, src.netloc) != (dst.scheme, dst.netloc):
        raise ValueError("Input and output must use the same HDFS authority")
    if not re.fullmatch(r"/user/[a-z_][a-z0-9_-]*/hw4/runs/run-[a-f0-9]{32}/data", dst.path):
        raise ValueError("Output must be a new, dedicated HW4 run data directory")
    return src, dst


def money(value):
    return str(Decimal(value).quantize(Decimal("0.01")))


def normalize_rows(rows):
    result = [{"customer_id": int(row["customer_id"]), "order_count": int(row["order_count"]),
               "total_amount": money(row["total_amount"]), "average_amount": money(row["average_amount"]),
               "dt": str(row["dt"])} for row in rows]
    return sorted(result, key=lambda row: (row["dt"], row["customer_id"]))


def validate_result(rows):
    normalized = normalize_rows(rows)
    if normalized != EXPECTED_ROWS:
        raise RuntimeError("Transformed rows differ from the five expected customer/day aggregates")
    return normalized


def schema_fields(frame):
    return [{"name": field.name, "type": field.dataType.simpleString(), "nullable": field.nullable}
            for field in frame.schema.fields]


def check_schema(frame):
    if [(field.name, field.dataType.simpleString()) for field in frame.schema.fields] != SCHEMA:
        raise RuntimeError("Unexpected transformed table schema")


def executor_evidence(context):
    """Read Spark's actual executor summaries; do not infer executors from configuration."""
    try:
        summaries = context._jsc.sc().statusStore().executorList(True)
        result = []
        for index in range(summaries.size()):
            item = summaries.apply(index)
            result.append({"id": str(item.id()), "host_port": str(item.hostPort()),
                           "active": bool(item.isActive()), "total_tasks": int(item.totalTasks())})
        return {"available": True, "items": result}
    except Exception as error:
        return {"available": False, "error": type(error).__name__}


def execute(source, output):
    src, _ = validate_paths(source, output)
    from pyspark.sql import SparkSession, functions as F, types as T

    started = now()
    warehouse = output.rsplit("/", 1)[0] + "/catalog"
    spark = (SparkSession.builder.appName("hw4-spark-sales")
             .config("spark.sql.catalogImplementation", "in-memory")
             .config("spark.sql.warehouse.dir", warehouse)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "true")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
             .config("spark.sql.parquet.writeLegacyFormat", "true")
             .getOrCreate())
    try:
        context = spark.sparkContext
        context.setLogLevel("WARN")
        if context.master != "yarn" or not re.fullmatch(r"application_[0-9]+_[0-9]+", context.applicationId):
            raise RuntimeError("This exercise must run on YARN, not in local/standalone mode")
        if spark.conf.get("spark.sql.catalogImplementation") != "in-memory":
            raise RuntimeError("The Spark session must use its in-memory catalog")
        default_fs = context._jsc.hadoopConfiguration().get("fs.defaultFS")
        default = hdfs_uri(default_fs.rstrip("/") + "/")
        if (default.scheme, default.netloc) != (src.scheme, src.netloc):
            raise RuntimeError("HADOOP_CONF_DIR points to a different HDFS filesystem")
        path_class = context._jvm.org.apache.hadoop.fs.Path
        destination = path_class(output)
        filesystem = destination.getFileSystem(context._jsc.hadoopConfiguration())
        if filesystem.exists(destination):
            raise RuntimeError("Refusing to overwrite an existing output path; allocate a new run")

        raw_schema = T.StructType([T.StructField(name, T.StringType(), True)
                                   for name in ("sale_id_raw", "customer_id_raw", "amount_raw")])
        raw = (spark.read.schema(raw_schema).option("header", "false").option("mode", "FAILFAST")
               .option("maxColumns", "3").option("maxCharsPerColumn", "128")
               .option("quote", "\u0000").option("escape", "\u0000")
               .csv(source + "/dt=*/*.csv")
               .withColumn("source_file", F.input_file_name())
               .withColumn("dt_raw", F.regexp_extract("source_file", r"/dt=(\d{4}-\d{2}-\d{2})/[^/]+\.csv$", 1)))
        valid = (F.col("sale_id_raw").rlike(r"^[1-9][0-9]*$")
                 & F.col("customer_id_raw").rlike(r"^[1-9][0-9]*$")
                 & F.col("amount_raw").rlike(r"^[0-9]{1,8}\.[0-9]{2}$")
                 & F.col("dt_raw").rlike(r"^\d{4}-\d{2}-\d{2}$"))
        if raw.filter(~F.coalesce(valid, F.lit(False))).limit(1).count():
            raise RuntimeError("Input contains a malformed identifier, amount, or partition date")
        typed = (raw.select(F.col("sale_id_raw").cast("int").alias("sale_id"),
                            F.col("customer_id_raw").cast("int").alias("customer_id"),
                            F.col("amount_raw").cast("decimal(10,2)").alias("amount"),
                            F.to_date("dt_raw", "yyyy-MM-dd").alias("sale_date"), "source_file")
                 .withColumn("dt", F.date_format("sale_date", "yyyy-MM-dd")))
        source_rows = typed.select("sale_id", "customer_id", "amount", "dt", "source_file").collect()
        actual_source = sorted((int(row.sale_id), int(row.customer_id), money(row.amount), row.dt)
                               for row in source_rows)
        if actual_source != EXPECTED_SOURCE:
            raise RuntimeError("HDFS source differs from the six preloaded HW3 fixture rows")
        source_files = sorted({row.source_file for row in source_rows})
        if len(source_files) != 2 or any(not name.startswith(source + "/dt=") for name in source_files):
            raise RuntimeError("Input did not come from the two expected HDFS partitions")

        transformed = (typed.filter(F.col("amount") > F.lit(0))
                       .groupBy("dt", "customer_id")
                       .agg(F.count("sale_id").alias("order_count"),
                            F.sum("amount").cast("decimal(20,2)").alias("total_amount"),
                            F.avg("amount").cast("decimal(20,2)").alias("average_amount"))
                       .select("customer_id", "order_count", "total_amount", "average_amount", "dt"))
        check_schema(transformed)
        expected = validate_result(transformed.collect())
        # Metadata in this catalog lasts for this session. The orchestrator registers
        # the same Parquet files as a durable external table through existing HS2.
        spark.sql("CREATE DATABASE hw4_spark")
        (transformed.write.mode("errorifexists").format("parquet").option("path", output)
         .partitionBy("dt").saveAsTable(TABLE))
        table = spark.table(TABLE)
        check_schema(table)
        table_rows = validate_result(table.collect())
        parquet = spark.read.parquet(output)
        check_schema(parquet)
        parquet_rows = validate_result(parquet.collect())
        if table_rows != expected or parquet_rows != expected:
            raise RuntimeError("Saved data differs from the transformation result")
        saved_table = spark.catalog.getTable(TABLE)
        if saved_table.tableType != "EXTERNAL":
            raise RuntimeError("Expected a path-backed external Spark table")
        executors = executor_evidence(context)
        if executors["available"] and not any(item["id"] != "driver" and item["total_tasks"] > 0
                                              for item in executors["items"]):
            raise RuntimeError("Spark did not report any YARN executor completing tasks")
        return {
            "ok": True, "started_at": started, "finished_at": now(), "spark_version": spark.version,
            "master": context.master, "application_id": context.applicationId,
            "catalog_implementation": spark.conf.get("spark.sql.catalogImplementation"),
            "spark_table": TABLE, "spark_table_type": saved_table.tableType,
            "spark_catalog_scope": "session", "hive_registration": "performed by the orchestrator through existing HiveServer2",
            "default_filesystem": default_fs, "input_path": source, "input_files": source_files,
            "output_path": output, "input_rows": len(source_rows),
            "input_total": money(sum(row.amount for row in source_rows)),
            "output_rows": len(expected), "output_total": money(sum(Decimal(row["total_amount"]) for row in expected)),
            "output_order_count": sum(row["order_count"] for row in expected),
            "partitions": sorted({"dt=" + row["dt"] for row in expected}),
            "rows": expected, "schema": schema_fields(table), "parquet_schema": schema_fields(parquet),
            "spark_table_readback_matches": True, "parquet_readback_matches": True, "executors": executors,
        }
    finally:
        spark.stop()


def write_report(path, report):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=path.parent,
                                     prefix=path.name + ".", suffix=".tmp", delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    os.replace(temporary, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, dest="source")
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = execute(args.source, args.output)
    except Exception as error:
        write_report(args.report, {"ok": False, "finished_at": now(), "error": str(error)})
        raise
    write_report(args.report, report)
    print(json.dumps({"ok": True, "application_id": report["application_id"],
                      "output_rows": report["output_rows"], "output_total": report["output_total"]}))


if __name__ == "__main__":
    main()
