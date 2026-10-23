#!/usr/bin/env python3
"""Settings for Spark on the existing YARN cluster, without a Hive catalog."""
import ipaddress
from pathlib import Path, PurePosixPath
import re
import shlex
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from hw3 import config as hive_config

VERSION = "3.5.9"
HOME_NAME = f"spark-{VERSION}-bin-hadoop3"
ARCHIVE = HOME_NAME + ".tgz"
URL = f"https://archive.apache.org/dist/spark/spark-{VERSION}/{ARCHIVE}"
SHA512 = "66097faf48b12f8673dc13256a44d731c27c86a5831cd8ca930c6fd9dd9bdf31d40593095e959461351d55da1a8e3ad5ed95e0b4b51bea87ca3989ab002ea8ce"
OWNER = "hadoop-homework-hw4-v1"
MARKER = ".hw4-owner"


def settings(c):
    s = {"version": VERSION,
         "install_root": f"/home/{c['ssh_user']}/teachbase-spark"}
    s.update(c.get("spark", {}))
    h = hive_config.settings(c)
    s.update({
        "home": s["install_root"] + "/" + HOME_NAME,
        "conf_dir": s["install_root"] + "/conf",
        "env_file": s["install_root"] + "/course-env.sh",
        "hdfs_root": f"/user/{c['ssh_user']}/hw4",
        "input_path": h["warehouse"] + "/" + h["hive_database"] + ".db/" + h["hive_table"],
        "edge_ip": hive_config.node_by_name(c, "worker1")["private_ip"],
        "jdbc_url": f"jdbc:hive2://{h['server_ip']}:{h['server_port']}/default",
        "hive_database": "hw4_lab",
        "hive_table": "daily_sales",
    })
    return s


def validate(c, live=False):
    hive_config.validate(c, live=live)
    extra = set(c.get("spark", {})) - {"version", "install_root"}
    if extra:
        raise ValueError("Unsupported spark inventory settings: " + ", ".join(sorted(extra)))
    s = settings(c)
    if s["version"] != VERSION:
        raise ValueError(f"Spark is pinned to {VERSION}; review compatibility before upgrading")
    root = s["install_root"]
    if not isinstance(root, str) or not re.fullmatch(r"/[A-Za-z0-9_./-]+", root):
        raise ValueError("spark.install_root must be a plain absolute Linux path")
    path = PurePosixPath(root)
    if ".." in path.parts or str(path) != root or len(path.parts) < 4 or path.name != "teachbase-spark":
        raise ValueError("spark.install_root must be a dedicated absolute */teachbase-spark path")
    for other in (c["install_root"], hive_config.settings(c)["install_root"]):
        if root == other or root.startswith(other + "/") or other.startswith(root + "/"):
            raise ValueError("Spark, Hadoop and Hive installation directories must be separate")
    ip = ipaddress.IPv4Address(s["edge_ip"])
    if not ip.is_private or ip.is_loopback or ip.is_unspecified:
        raise ValueError("Spark driver requires the edge VM's private address")
    if live and not hive_config.node_by_name(c, "worker1").get("local"):
        raise ValueError("Run Spark deployment on edge with worker1.local=true")
    return s


def defaults(c):
    s = settings(c)
    return {
        "spark.master": "yarn",
        "spark.submit.deployMode": "client",
        "spark.driver.memory": "512m",
        "spark.driver.host": s["edge_ip"],
        "spark.driver.bindAddress": s["edge_ip"],
        "spark.executor.instances": "2",
        "spark.executor.cores": "1",
        "spark.executor.memory": "512m",
        "spark.executor.memoryOverhead": "384m",
        "spark.yarn.am.memory": "512m",
        "spark.yarn.am.memoryOverhead": "384m",
        "spark.dynamicAllocation.enabled": "false",
        "spark.yarn.populateHadoopClasspath": "false",
        "spark.yarn.am.clientModeTreatDisconnectAsFailed": "true",
        "spark.yarn.am.clientModeExitOnError": "true",
        "spark.ui.enabled": "false",
        "spark.sql.catalogImplementation": "in-memory",
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.parquet.writeLegacyFormat": "true",
        "spark.pyspark.python": "/usr/bin/python3",
        "spark.pyspark.driver.python": "/usr/bin/python3",
    }


def environment(c):
    s = settings(c)
    return ("#!/usr/bin/env bash\n"
            + "source " + shlex.quote(c["install_root"] + "/course-env.sh") + " || return\n"
            + "export SPARK_HOME=" + shlex.quote(s["home"]) + "\n"
            + "export SPARK_CONF_DIR=" + shlex.quote(s["conf_dir"]) + "\n"
            + "export PYSPARK_PYTHON=/usr/bin/python3\n"
            + "export PYSPARK_DRIVER_PYTHON=/usr/bin/python3\n"
            + 'export PATH="$SPARK_HOME/bin:$PATH"\n')


def render(c, out):
    validate(c)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "spark-defaults.conf").write_text(
        "".join(f"{key} {value}\n" for key, value in defaults(c).items()),
        encoding="utf-8", newline="\n")
    (out / "course-env.sh").write_text(environment(c), encoding="utf-8", newline="\n")
    return out
