#!/usr/bin/env python3
"""Configuration for an isolated Hive service using the existing HDFS/YARN."""
import ipaddress
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from common import cluster

VERSION = "4.0.0-alpha-2"
ARCHIVE = f"apache-hive-{VERSION}-bin.tar.gz"
HOME_NAME = f"apache-hive-{VERSION}-bin"
URL = f"https://archive.apache.org/dist/hive/hive-{VERSION}/{ARCHIVE}"
SHA256 = "f94c773e7ae3328d780138d9259c584e94d8f2e34715776cdfaaa78bcf3f25e1"
PASSWORD_PLACEHOLDER = "__GENERATED_ON_MASTER__"
WAREHOUSE = "/user/hive/warehouse/hw3"


def settings(c):
    defaults = {
        "version": VERSION,
        "install_root": f"/home/{c['ssh_user']}/teachbase-hive",
        "metastore_node": "master",
        "server_node": "worker2",
        "client_node": "worker1",
        "database": "hw3_hive",
        "database_user": "hw3_hive",
        "metastore_port": 9083,
        "server_port": 10000,
        "warehouse": WAREHOUSE,
        "metastore_heap_mb": 384,
        "server_heap_mb": 512,
        "client_heap_mb": 256,
        "hive_database": "hw3_lab",
        "hive_table": "sales",
    }
    defaults.update(c.get("hive", {}))
    defaults["metastore_ip"] = node_by_name(c, defaults["metastore_node"])["private_ip"]
    defaults["server_ip"] = node_by_name(c, defaults["server_node"])["private_ip"]
    return defaults


def node_by_name(c, name):
    for node in c["nodes"]:
        if node["name"] == name:
            return node
    raise ValueError(f"Unknown cluster node: {name}")


def validate(c, live=False):
    cluster.validate(c, live=live)
    h = settings(c)
    if h["version"] != VERSION:
        raise ValueError(f"Hive is pinned to {VERSION}; review compatibility before upgrading")
    base = h["install_root"]
    parts = PurePosixPath(base).parts
    if (not re.fullmatch(r"/[A-Za-z0-9_./-]+", base) or ".." in parts
            or len(parts) < 4 or PurePosixPath(base).name != "teachbase-hive"):
        raise ValueError("hive.install_root must be a dedicated absolute */teachbase-hive path")
    if base == c["install_root"] or base.startswith(c["install_root"] + "/"):
        raise ValueError("Hive and Hadoop installation directories must be separate")
    for key, expected in (("metastore_node", "master"), ("server_node", "worker2"), ("client_node", "worker1")):
        if h[key] != expected:
            raise ValueError(f"This deployment assigns {key} to {expected}")
    for key in ("database", "database_user", "hive_database", "hive_table"):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,39}", h[key]):
            raise ValueError(f"Invalid Hive {key}")
    # This is one dedicated exercise, not a general database provisioning tool.
    # Keep the SQL identifiers aligned with the fixtures, ownership markers,
    # manual instructions, and the exact-result verifier.
    fixed_names = {"database": "hw3_hive", "database_user": "hw3_hive",
                   "hive_database": "hw3_lab", "hive_table": "sales"}
    for key, expected in fixed_names.items():
        if h[key] != expected:
            raise ValueError(f"This dedicated exercise requires {key}={expected}")
    for key in ("metastore_port", "server_port"):
        if not isinstance(h[key], int) or not 1024 <= h[key] <= 65535:
            raise ValueError(f"Invalid {key}")
    reserved = {9000, 9870, 9864, 9866, 9867, 9868, 8088, 8042, 8040, 8030, 8031,
                8032, 8033, 45454, 13562, 19888, 10020, 9099, 5432}
    if h["metastore_port"] in reserved or h["server_port"] in reserved:
        raise ValueError("Hive service port conflicts with Hadoop/PostgreSQL")
    for key in ("metastore_heap_mb", "server_heap_mb", "client_heap_mb"):
        if not isinstance(h[key], int) or not 128 <= h[key] <= 1024:
            raise ValueError(f"{key} must be an integer between 128 and 1024")
    if h["warehouse"] != WAREHOUSE:
        raise ValueError(f"This exercise uses only its dedicated warehouse {WAREHOUSE}")
    for name in (h["metastore_node"], h["server_node"], h["client_node"]):
        ip = ipaddress.IPv4Address(node_by_name(c, name)["private_ip"])
        if not ip.is_private or ip.is_loopback or ip.is_unspecified:
            raise ValueError("Hive services require private, non-loopback cluster addresses")
    # Passwords are created on the master and must never be part of inventory/render.
    if any("password" in key.lower() or "secret" in key.lower() for key in c.get("hive", {})):
        raise ValueError("Do not put Hive database passwords in inventory")
    if live:
        edge = node_by_name(c, h["client_node"])
        if not edge.get("local"):
            raise ValueError("Run Hive deployment on edge with worker1.local=true")
    return h


def properties(c, name):
    h = settings(c)
    master = node_by_name(c, h["metastore_node"])["private_ip"]
    server = node_by_name(c, h["server_node"])["private_ip"]
    props = {
        "hive.execution.engine": "mr",
        "hive.metastore.warehouse.dir": h["warehouse"],
        "hive.metastore.warehouse.external.dir": h["warehouse"] + "/external",
        "hive.exec.scratchdir": "/tmp/hive-hw3",
        "hive.exec.local.scratchdir": h["install_root"] + "/tmp",
        "hive.downloaded.resources.dir": h["install_root"] + "/tmp/resources",
        "hive.metastore.schema.verification": True,
        "datanucleus.schema.autoCreateAll": False,
        "hive.support.concurrency": False,
        "hive.txn.manager": "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager",
        "hive.server2.enable.doAs": False,
        "hive.server2.authentication": "NONE",
    }
    if name == h["metastore_node"]:
        props.update({
            "javax.jdo.option.ConnectionURL": f"jdbc:postgresql://127.0.0.1:5432/{h['database']}",
            "javax.jdo.option.ConnectionDriverName": "org.postgresql.Driver",
            "javax.jdo.option.ConnectionUserName": h["database_user"],
            "javax.jdo.option.ConnectionPassword": PASSWORD_PLACEHOLDER,
            "hive.metastore.thrift.bind.host": master,
            "hive.metastore.port": h["metastore_port"],
            "metastore.try.direct.sql.ddl": False,
            "hive.metastore.server.min.threads": 5,
            "hive.metastore.server.max.threads": 30,
        })
    else:
        props["hive.metastore.uris"] = f"thrift://{master}:{h['metastore_port']}"
    if name == h["server_node"]:
        props.update({
            "hive.server2.thrift.bind.host": server,
            "hive.server2.thrift.port": h["server_port"],
            "hive.server2.transport.mode": "binary",
            "hive.server2.webui.port": 0,
            "hive.server2.async.exec.threads": 4,
            "hive.server2.thrift.min.worker.threads": 2,
            "hive.server2.thrift.max.worker.threads": 10,
            "hive.server2.logging.operation.log.location": h["install_root"] + "/logs/operations",
            "hive.server2.tez.initialize.default.sessions": False,
            # Hadoop 3.4 no longer supplies this Hive alpha2 MapReduce dependency.
            "hive.aux.jars.path": f"file://{h['install_root']}/{HOME_NAME}/lib/commons-collections-3.2.2.jar",
        })
    return props


def selected_nodes(c):
    h = settings(c)
    names = [h["client_node"], h["metastore_node"], h["server_node"]]
    return [node_by_name(c, name) for name in names]


def render(c, out):
    h = validate(c)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    for n in selected_nodes(c):
        name = n["name"]
        d = out / name
        d.mkdir(exist_ok=True)
        role = "metastore" if name == h["metastore_node"] else "server" if name == h["server_node"] else "client"
        heap = h[{"metastore": "metastore_heap_mb", "server": "server_heap_mb", "client": "client_heap_mb"}[role]]
        values = {
            "BASE": h["install_root"], "HADOOP_BASE": c["install_root"], "VERSION": VERSION,
            "ARCHIVE": ARCHIVE, "ARCHIVE_URL": URL, "ARCHIVE_SHA256": SHA256,
            "NODE": name, "ROLE": role, "PRIVATE_IP": n["private_ip"], "RUN_USER": c["ssh_user"],
            "MASTER_IP": node_by_name(c, h["metastore_node"])["private_ip"],
            "SERVER_IP": node_by_name(c, h["server_node"])["private_ip"],
            "METASTORE_PORT": h["metastore_port"], "SERVER_PORT": h["server_port"],
            "DB_NAME": h["database"], "DB_USER": h["database_user"],
            "WAREHOUSE": h["warehouse"], "HIVE_HEAP_MB": heap,
        }
        (d / "node.env").write_text("".join(f"{k}={shlex.quote(str(v))}\n" for k, v in values.items()), encoding="utf-8", newline="\n")
        (d / "hive-site.xml").write_text(cluster.xml(properties(c, name)), encoding="utf-8", newline="\n")
        shutil.copyfile(ROOT / "hw3/node.sh", d / "node.sh")
    return out
