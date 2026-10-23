#!/usr/bin/env python3
"""Preview locally, or deploy Hive from edge using the existing Hadoop inventory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from common import cluster
from hw3 import config


def file_digest(path):
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def ensure_archive(h):
    cache = Path(h["install_root"]) / "downloads"
    cache.mkdir(parents=True, exist_ok=True)
    archive = cache / config.ARCHIVE
    if archive.exists():
        if file_digest(archive) != config.SHA256:
            raise RuntimeError("Cached Hive archive failed SHA-256 verification")
        return archive
    part = cache / (config.ARCHIVE + ".part")
    try:
        subprocess.run(["curl", "--fail", "--location", "--retry", "3", "--proto", "=https",
                        "--tlsv1.2", config.URL, "-o", str(part)], check=True)
        if file_digest(part) != config.SHA256:
            raise RuntimeError("Downloaded Hive archive failed SHA-256 verification")
        part.replace(archive)
    finally:
        if part.exists():
            part.unlink()
    return archive


def copy_to_node(c, n, files, destination):
    if n.get("local"):
        for file in files:
            shutil.copyfile(file, Path(destination) / Path(file).name)
    else:
        subprocess.run(["scp", *cluster.ssh_options(c, True), *(str(p) for p in files),
                        f"{c['ssh_user']}@{n['ssh_host']}:{destination}/"], check=True)


def existing_action(c, action):
    h = config.settings(c)
    roles = [h["metastore_node"], h["server_node"]]
    if action == "stop":
        roles.reverse()
    for name in roles:
        n = config.node_by_name(c, name)
        runner = h["install_root"] + "/artifacts/node.sh"
        cluster.remote(c, n, f"bash {shlex.quote(runner)} {shlex.quote(action)}")


def require_edge_platform():
    if os.name != "posix":
        raise ValueError("Execute deployment on the Linux edge VM")
    if os.geteuid() == 0:
        raise ValueError("Run Hive deployment as the ordinary Hadoop user")


def deploy(c):
    h = config.validate(c, live=True)
    require_edge_platform()
    # Prepare only our directories before downloading; node.sh guards ownership.
    with tempfile.TemporaryDirectory(prefix="hive-render-") as temporary:
        out = config.render(c, Path(temporary))
        staging = {}
        try:
            for n in config.selected_nodes(c):
                stage = cluster.remote(c, n, "umask 077; mktemp -d /tmp/teachbase-hive.XXXXXXXXXX", True).stdout.strip()
                if not re.fullmatch(r"/tmp/teachbase-hive\.[A-Za-z0-9]{10}", stage):
                    raise RuntimeError("Unexpected Hive staging path returned by SSH")
                staging[n["name"]] = stage
                copy_to_node(c, n, sorted((out / n["name"]).iterdir()), stage)
                cluster.remote(c, n, f"bash {shlex.quote(stage + '/node.sh')} prepare")
            archive = ensure_archive(h)
            for n in config.selected_nodes(c):
                if not n.get("local"):
                    destination = h["install_root"] + "/downloads"
                    probe = (f"test -f {shlex.quote(destination + '/' + config.ARCHIVE)} && "
                             f"sha256sum {shlex.quote(destination + '/' + config.ARCHIVE)}")
                    try:
                        cached = cluster.remote(c, n, probe, True).stdout.split()[0] == config.SHA256
                    except subprocess.CalledProcessError:
                        cached = False
                    if not cached:
                        copy_to_node(c, n, [archive], destination)
                stage = staging[n["name"]]
                cluster.remote(c, n, f"bash {shlex.quote(stage + '/node.sh')} install")
            def run(name, action):
                n = config.node_by_name(c, name)
                runner = h["install_root"] + "/artifacts/node.sh"
                cluster.remote(c, n, f"bash {shlex.quote(runner)} {shlex.quote(action)}")
            run(h["metastore_node"], "configure-postgres")
            run(h["metastore_node"], "initialize-schema")
            run(h["client_node"], "prepare-hdfs")
            run(h["metastore_node"], "start")
            run(h["server_node"], "start")
            print("Hive installation completed. Run hw3/verify.py to check JDBC sessions and loaded partitions.")
        finally:
            for n in config.selected_nodes(c):
                stage = staging.get(n["name"])
                if stage is not None:
                    try:
                        cluster.remote(c, n, "rm -rf -- " + shlex.quote(stage))
                    except (OSError, subprocess.CalledProcessError) as error:
                        print(f"Could not remove Hive staging on {n['name']}: {error}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "inventory.json")
    parser.add_argument("--output", type=Path, default=ROOT / "generated-hive")
    parser.add_argument("--action", choices=("deploy", "start", "stop", "status"), default="deploy")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    c = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
    config.validate(c, live=args.execute)
    if not args.execute:
        print(config.render(c, args.output))
        print(f"Preview only; no network or remote changes. Action {args.action} requires --execute on edge.")
    elif args.action == "deploy":
        deploy(c)
    else:
        existing_action(c, args.action)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error))
