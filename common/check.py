#!/usr/bin/env python3
"""Check HDFS and YARN health through JMX and REST APIs."""
import json
from pathlib import Path
import socket
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET


def read_xml(path):
    return {p.findtext("name"): p.findtext("value") for p in ET.parse(path).getroot()}


def fetch(url):
    # Cluster private traffic must not go through the operator's HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=10) as r:
        return json.load(r)


def retry(fn, seconds=360):
    deadline = time.monotonic() + seconds
    while True:
        try:
            return fn()
        except (OSError, ValueError, AssertionError, KeyError, IndexError) as e:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Cluster did not become healthy: {e}") from e
            print(f"Waiting for services: {e}", flush=True)
            time.sleep(5)


def checkpoint_time(payload):
    beans = payload.get("beans", [])
    assert beans, "SecondaryNameNode JMX is missing"
    value = beans[0].get("LastCheckpointTime")
    assert isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0, \
        "SecondaryNameNode has not reported a completed checkpoint"
    # An existing checkpoint may legitimately predate a daemon restart.
    return value


def main():
    core, hdfs, workers, stage = sys.argv[1:]
    settings = read_xml(hdfs)
    nodes = Path(workers).read_text().split()
    if len(set(nodes)) != 3:
        raise ValueError("Exactly three workers are required")
    nn = settings["dfs.namenode.http-address"]
    master = read_xml(core)["fs.defaultFS"].split("//", 1)[1].split(":", 1)[0]

    def hdfs_health():
        info = fetch(f"http://{nn}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")["beans"][0]
        live, dead = json.loads(info["LiveNodes"]), json.loads(info["DeadNodes"])
        assert len(live) == 3 and not dead, f"Live={len(live)}, dead={len(dead)}"
        assert {v["xferaddr"].split(":")[0] for v in live.values()} == set(nodes), "Unexpected DataNodes"
        assert all(v.get("adminState") == "In Service" for v in live.values()), "DataNode not in service"
        assert all(v["volfails"] == 0 for v in live.values()), "Failed data volumes"
        state = fetch(f"http://{nn}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")["beans"][0]
        assert state.get("NumStaleDataNodes", 0) == 0, "Stale DataNodes"
        assert state.get("NumDecommissioningDataNodes", 0) == 0, "Decommissioning DataNodes"
        snn = settings["dfs.namenode.secondary.http-address"]
        checkpoint = fetch(f"http://{snn}/jmx?qry=Hadoop:service=SecondaryNameNode,name=SecondaryNameNodeInfo")
        last_checkpoint = checkpoint_time(checkpoint)
        print(json.dumps({"HDFS": "healthy", "live": live, "dead": dead,
                          "last_checkpoint_time_ms": last_checkpoint}, indent=2))

    retry(hdfs_health)
    if stage == "yarn":
        def yarn_health():
            response = fetch(f"http://{master}:8088/ws/v1/cluster/nodes")
            rows = (response.get("nodes") or {}).get("node", [])
            assert len(rows) == 3, f"Expected three NodeManagers, got {len(rows)}"
            # YARN reports canonical hostnames even when configured with IPv4.
            assert {socket.gethostbyname(r["nodeHostName"]) for r in rows} == set(nodes), "Unexpected NodeManagers"
            assert all(r["state"] == "RUNNING" for r in rows), "Not all NodeManagers RUNNING"
            metrics = fetch(f"http://{master}:8088/ws/v1/cluster/metrics")["clusterMetrics"]
            assert metrics["unhealthyNodes"] == 0 and metrics["lostNodes"] == 0, "Unhealthy/lost nodes"
            for ip in nodes:
                fetch(f"http://{ip}:8042/ws/v1/node/info")
            fetch(f"http://{master}:19888/ws/v1/history/info")
            print(json.dumps({"YARN": "healthy", "nodes": rows, "metrics": metrics}, indent=2))
        retry(yarn_health)


if __name__ == "__main__":
    main()
