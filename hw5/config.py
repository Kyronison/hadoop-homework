#!/usr/bin/env python3
"""Settings for the Prefect flow that drives Spark on the existing YARN cluster."""
from pathlib import Path, PurePosixPath
import re
import shlex
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from hw3 import config as hive_config
from hw4 import config as spark_config

VERSION = "3.9.2"
PY4J = "py4j-0.10.9.7-src.zip"
OWNER = "hadoop-homework-hw5-v1"
MARKER = ".hw5-owner"
APP_NAME = "hw5-prefect-sales"
FLOW_NAME = "hw5-spark-yarn-pipeline"
SPARK_DATABASE = "hw5_spark"
HIVE_DATABASE = "hw5_lab"
TABLE = "customer_summary"
SERVER_PORT = 4200


def settings(c):
    s = {"version": VERSION,
         "install_root": f"/home/{c['ssh_user']}/teachbase-prefect"}
    s.update(c.get("prefect", {}))
    spark = spark_config.settings(c)
    h = hive_config.settings(c)
    master = hive_config.node_by_name(c, "master")["private_ip"]
    root = s["install_root"]
    s.update({
        "venv": root + "/venv",
        "prefect_home": root + "/home",
        "env_file": root + "/course-env.sh",
        "evidence": root + "/evidence",
        "spark_env": spark["env_file"],
        "spark_home": spark["home"],
        "spark_install_root": spark["install_root"],
        "hdfs_root": f"/user/{c['ssh_user']}/hw5",
        "input_path": spark["input_path"],
        "filesystem": f"hdfs://{master}:9000",
        "resourcemanager": f"http://{master}:8088",
        "edge_ip": spark["edge_ip"],
        "jdbc_url": f"jdbc:hive2://{h['server_ip']}:{h['server_port']}/default",
        "spark_table": SPARK_DATABASE + "." + TABLE,
        "hive_table": HIVE_DATABASE + "." + TABLE,
        "api_url": f"http://127.0.0.1:{SERVER_PORT}/api",
    })
    return s


def validate(c, live=False):
    spark_config.validate(c, live=live)
    extra = set(c.get("prefect", {})) - {"version", "install_root"}
    if extra:
        raise ValueError("Unsupported prefect inventory settings: " + ", ".join(sorted(extra)))
    s = settings(c)
    if s["version"] != VERSION:
        raise ValueError(f"Prefect is pinned to {VERSION}; review the flow before upgrading")
    root = s["install_root"]
    if not isinstance(root, str) or not re.fullmatch(r"/[A-Za-z0-9_./-]+", root):
        raise ValueError("prefect.install_root must be a plain absolute Linux path")
    path = PurePosixPath(root)
    if ".." in path.parts or str(path) != root or len(path.parts) < 4 or path.name != "teachbase-prefect":
        raise ValueError("prefect.install_root must be a dedicated absolute */teachbase-prefect path")
    others = (c["install_root"], hive_config.settings(c)["install_root"], s["spark_install_root"])
    for other in others:
        if root == other or root.startswith(other + "/") or other.startswith(root + "/"):
            raise ValueError("Prefect, Spark, Hadoop and Hive installation directories must be separate")
    return s


def environment(c):
    """Shell environment: existing Hadoop + Spark, plus the Prefect virtualenv."""
    s = settings(c)
    python = s["spark_home"] + "/python"
    return ("#!/usr/bin/env bash\n"
            + "source " + shlex.quote(s["spark_env"]) + " || return\n"
            + "source " + shlex.quote(s["venv"] + "/bin/activate") + " || return\n"
            + "export PREFECT_HOME=" + shlex.quote(s["prefect_home"]) + "\n"
            + "export PREFECT_SERVER_ANALYTICS_ENABLED=false\n"
            + "export PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120\n"
            + "export PYTHONPATH=" + shlex.quote(python) + ":" + shlex.quote(python + "/lib/" + PY4J)
            + '${PYTHONPATH:+:$PYTHONPATH}\n')


def render(c, out):
    validate(c)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "course-env.sh").write_text(environment(c), encoding="utf-8", newline="\n")
    return out
