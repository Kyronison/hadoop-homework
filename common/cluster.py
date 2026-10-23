#!/usr/bin/env python3
"""Prepare or deploy an isolated four-node teaching cluster. Python stdlib only."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import ipaddress
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
NAMES = ["master", "worker1", "worker2", "worker3"]


def validate(c, live=False):
    if c["hadoop_version"] != "3.4.2":
        raise ValueError("This solution is pinned to Hadoop 3.4.2; review before upgrading")
    if not re.fullmatch(r"[a-z_][a-z0-9_-]*", c["ssh_user"]) or c["ssh_user"] == "root":
        raise ValueError("Use an ordinary Linux account, not root")
    p = c["install_root"]
    if not re.fullmatch(r"/[A-Za-z0-9_./-]+", p) or ".." in PurePosixPath(p).parts:
        raise ValueError("install_root must be a plain absolute Linux path")
    if len(PurePosixPath(p).parts) < 4 or PurePosixPath(p).name != "teachbase-hadoop":
        raise ValueError("Use a dedicated directory, e.g. /home/ubuntu/teachbase-hadoop")
    if not 1 <= c["ssh_port"] <= 65535:
        raise ValueError("Invalid SSH port")
    if not isinstance(c["yarn_memory_mb"], int) or c["yarn_memory_mb"] < 1536:
        raise ValueError("Reserve at least 1536 MB per worker for YARN containers")
    if not isinstance(c["yarn_vcores"], int) or c["yarn_vcores"] < 2:
        raise ValueError("At least two virtual cores per worker are required")
    if [n["name"] for n in c["nodes"]] != NAMES:
        raise ValueError("Exactly master, worker1, worker2, worker3 in this order are required")
    ips = [str(ipaddress.IPv4Address(n["private_ip"])) for n in c["nodes"]]
    if len(set(ips)) != 4 or any(ipaddress.ip_address(ip).is_loopback for ip in ips):
        raise ValueError("Four distinct non-loopback IPv4 addresses are required")
    for n in c["nodes"]:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", n["ssh_host"]):
            raise ValueError("ssh_host must be a hostname or IPv4 address")
        if live and n["ssh_host"].startswith(("192.0.2.", "198.51.100.", "203.0.113.")):
            raise ValueError("Replace example SSH addresses before any remote operation")
        if n.get("local") and live and socket.gethostname() != n["ssh_host"]:
            raise ValueError("Local node does not match this host; run this inventory on the edge VM")
    if c.get("ssh_jump") and not re.fullmatch(r"[A-Za-z0-9_.@:-]+", c["ssh_jump"]):
        raise ValueError("Invalid SSH jump specification")
    if live and not Path(c["ssh_key"]).expanduser().is_file():
        raise ValueError("SSH key file not found (the key is never copied to servers)")


def xml(props):
    root = ET.Element("configuration")
    for k, v in props.items():
        p = ET.SubElement(root, "property")
        ET.SubElement(p, "name").text = k
        ET.SubElement(p, "value").text = str(v).lower() if isinstance(v, bool) else str(v)
    ET.indent(root)
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode") + "\n"


def properties(c, n):
    master, secondary = [x["private_ip"] for x in c["nodes"][:2]]
    ip, base = n["private_ip"], c["install_root"]
    home = base + "/hadoop-" + c["hadoop_version"]
    core = {"fs.defaultFS": f"hdfs://{master}:9000", "hadoop.tmp.dir": base + "/tmp"}
    hdfs = {
        "dfs.replication": 3,
        "dfs.namenode.name.dir": "file://" + base + "/data/name",
        "dfs.datanode.data.dir": "file://" + base + "/data/data",
        "dfs.namenode.checkpoint.dir": "file://" + base + "/data/checkpoint",
        "dfs.namenode.http-address": f"{master}:9870",
        "dfs.namenode.secondary.http-address": f"{secondary}:9868",
        "dfs.datanode.hostname": ip,
        "dfs.datanode.address": f"{ip}:9866",
        "dfs.datanode.ipc.address": f"{ip}:9867",
        "dfs.datanode.http.address": f"{ip}:9864",
        "dfs.namenode.checkpoint.period": 300,
    }
    yarn = {
        "yarn.resourcemanager.hostname": master,
        "yarn.resourcemanager.bind-host": master,
        "yarn.resourcemanager.webapp.address": f"{master}:8088",
        "yarn.nodemanager.hostname": ip,
        "yarn.nodemanager.bind-host": ip,
        "yarn.nodemanager.address": f"{ip}:45454",
        "yarn.nodemanager.localizer.address": f"{ip}:8040",
        "yarn.nodemanager.webapp.address": f"{ip}:8042",
        "yarn.nodemanager.aux-services": "mapreduce_shuffle",
        "yarn.nodemanager.aux-services.mapreduce_shuffle.class": "org.apache.hadoop.mapred.ShuffleHandler",
        "yarn.nodemanager.env-whitelist": "JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME",
        "yarn.nodemanager.resource.memory-mb": c["yarn_memory_mb"],
        "yarn.nodemanager.resource.cpu-vcores": c["yarn_vcores"],
        "yarn.nodemanager.pmem-check-enabled": True,
        "yarn.nodemanager.vmem-check-enabled": False,
        "yarn.scheduler.minimum-allocation-mb": 512,
        "yarn.scheduler.maximum-allocation-mb": c["yarn_memory_mb"],
        "yarn.scheduler.maximum-allocation-vcores": c["yarn_vcores"],
        "yarn.nodemanager.local-dirs": base + "/data/yarn/local",
        "yarn.nodemanager.log-dirs": base + "/data/yarn/logs",
        "yarn.log-aggregation-enable": True,
        "yarn.nodemanager.remote-app-log-dir": "/app-logs",
        "yarn.log.server.url": f"http://{master}:19888/jobhistory/logs",
        "yarn.web-proxy.address": f"{master}:9099",
    }
    mapred = {
        "mapreduce.framework.name": "yarn",
        "mapreduce.application.classpath": home + "/share/hadoop/mapreduce/*:" + home + "/share/hadoop/mapreduce/lib/*",
        "mapreduce.map.memory.mb": 512,
        "mapreduce.reduce.memory.mb": 512,
        "mapreduce.map.java.opts": "-Xmx384m",
        "mapreduce.reduce.java.opts": "-Xmx384m",
        "yarn.app.mapreduce.am.resource.mb": 512,
        "yarn.app.mapreduce.am.command-opts": "-Xmx384m",
        "mapreduce.jobhistory.address": f"{master}:10020",
        "mapreduce.jobhistory.webapp.address": f"{master}:19888",
        "mapreduce.jobhistory.intermediate-done-dir": "/mr-history/tmp",
        "mapreduce.jobhistory.done-dir": "/mr-history/done",
    }
    return {"core-site.xml": core, "hdfs-site.xml": hdfs, "yarn-site.xml": yarn, "mapred-site.xml": mapred}


def endpoints(c):
    m, w1, w2, w3 = [n["private_ip"] for n in c["nodes"]]
    return [("nn", m, 9870), ("snn", w1, 9868),
            ("dn1", w1, 9864), ("dn2", w2, 9864), ("dn3", w3, 9864),
            ("rm", m, 8088), ("nm1", w1, 8042), ("nm2", w2, 8042),
            ("nm3", w3, 8042), ("history", m, 19888), ("proxy", m, 9099)]


def nginx(c):
    # Import lazily so local HDFS configuration remains usable independently.
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from hw2.web import nginx as render_nginx
    return render_nginx(c)


def render(c, out):
    out.mkdir(parents=True, exist_ok=True)
    for n in c["nodes"]:
        d = out / n["name"]
        d.mkdir(exist_ok=True)
        values = {"BASE": c["install_root"], "VERSION": c["hadoop_version"],
                  "NODE": n["name"], "PRIVATE_IP": n["private_ip"],
                  "MASTER_IP": c["nodes"][0]["private_ip"], "NM_MEMORY_MB": c["yarn_memory_mb"]}
        (d / "node.env").write_text("".join(f"{k}={shlex.quote(str(v))}\n" for k, v in values.items()), encoding="utf-8", newline="\n")
        for name, props in properties(c, n).items():
            (d / name).write_text(xml(props), encoding="utf-8", newline="\n")
        (d / "workers").write_text("\n".join(x["private_ip"] for x in c["nodes"][1:]) + "\n", encoding="utf-8", newline="\n")
        shutil.copyfile(ROOT / "common/node.sh", d / "node.sh")
        shutil.copyfile(ROOT / "common/check.py", d / "check.py")
    if c.get("web", {}).get("enabled", False):
        (out / "nginx.conf").write_text(nginx(c), encoding="utf-8", newline="\n")
    elif (out / "nginx.conf").exists():
        raise ValueError("Web disabled but old nginx.conf exists; choose a fresh output directory")
    return out


def ssh_options(c, scp=False):
    options = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=15",
               "-i", str(Path(c["ssh_key"]).expanduser()), "-P" if scp else "-p", str(c["ssh_port"])]
    if c.get("ssh_jump"):
        options += ["-J", c["ssh_jump"]]
    return options


def remote(c, n, command, capture=False):
    argv = ["bash", "-lc", command] if n.get("local") else ["ssh", *ssh_options(c), f"{c['ssh_user']}@{n['ssh_host']}", command]
    return subprocess.run(argv,
                          check=True, text=True, capture_output=capture)


def deploy(c, stage, initialize):
    # All SSH destinations come exclusively from the explicitly supplied inventory.
    with tempfile.TemporaryDirectory(prefix="hadoop-render-") as tmp:
        out = render(c, Path(tmp))
        staging = {}
        try:
            for n in c["nodes"]:
                path = remote(c, n, "umask 077; mktemp -d /tmp/teachbase-hadoop.XXXXXXXXXX", True).stdout.strip()
                if not re.fullmatch(r"/tmp/teachbase-hadoop\.[A-Za-z0-9]+", path):
                    raise RuntimeError("Unexpected staging directory returned by SSH")
                staging[n["name"]] = path
                files = sorted(str(p) for p in (out / n["name"]).iterdir())
                if n.get("local"):
                    for file in files:
                        shutil.copyfile(file, str(Path(path) / Path(file).name))
                else:
                    subprocess.run(["scp", *ssh_options(c, True), *files,
                                    f"{c['ssh_user']}@{n['ssh_host']}:{path}/"], check=True)
            def run(n, action):
                path = staging[n["name"]]
                remote(c, n, f"bash {shlex.quote(path + '/node.sh')} {shlex.quote(action)}")
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(lambda n: run(n, "install-" + stage), c["nodes"]))
            master = c["nodes"][0]
            if stage == "hdfs" and initialize:
                for n in c["nodes"][1:]:
                    run(n, "assert-empty-datanode")
                run(master, "initialize-empty-namenode")
            for n in c["nodes"]:
                run(n, "start-" + stage)
            run(master, "verify-" + stage)
            print("Deployment and functional check completed; review logs and external web access separately.")
        finally:
            for n in c["nodes"]:
                path = staging.get(n["name"])
                if path is not None:
                    # Only exact directories created and validated above are removed.
                    try:
                        remote(c, n, "rm -rf -- " + shlex.quote(path))
                    except (OSError, subprocess.CalledProcessError) as error:
                        print(f"Could not remove staging directory on {n['name']}: {error}", file=sys.stderr)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=["render", "hdfs", "yarn"])
    p.add_argument("--inventory", type=Path, default=ROOT / "inventory.json")
    p.add_argument("--output", type=Path, default=ROOT / "generated")
    p.add_argument("--execute", action="store_true", help="Allow SSH/file transfer and remote changes")
    p.add_argument("--initialize-empty-namenode", action="store_true")
    a = p.parse_args()
    c = json.loads(a.inventory.read_text(encoding="utf-8-sig"))
    live = a.action in ("hdfs", "yarn")
    validate(c, live=live and a.execute)
    if live and not a.execute:
        p.error("Remote actions require --execute. Use render for a completely local preview.")
    if a.initialize_empty_namenode and a.action != "hdfs":
        p.error("Initialization applies only to HDFS")
    if a.action == "render":
        print(render(c, a.output))
    else:
        deploy(c, a.action, a.initialize_empty_namenode)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as e:
        sys.exit(str(e))
