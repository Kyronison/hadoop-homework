#!/usr/bin/env python3
"""Install a verified Spark distribution on edge; YARN distributes its jars."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from hw4 import config


def no_symlinks(path):
    path = Path(path).absolute()
    for part in (path, *path.parents):
        if part.is_symlink():
            raise RuntimeError(f"Refusing symlink in Spark installation path: {part}")
    return path


def prepare_root(s):
    root = no_symlinks(s["install_root"])
    if root.exists() and (not root.is_dir() or root.stat().st_uid != os.getuid()):
        raise RuntimeError("Spark root must be a directory owned by the current user")
    marker = no_symlinks(root / config.MARKER)
    if root.exists() and any(root.iterdir()):
        if not marker.is_file() or marker.read_text(encoding="utf-8").strip() != config.OWNER:
            raise RuntimeError("Existing nonempty Spark root lacks the expected ownership marker")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    marker.write_text(config.OWNER + "\n", encoding="utf-8")
    for name in ("downloads", "conf"):
        directory = no_symlinks(root / name)
        directory.mkdir(exist_ok=True, mode=0o700)
        if directory.stat().st_uid != os.getuid():
            raise RuntimeError("Spark directories must be owned by the current user")
    return root


def file_digest(path):
    digest = hashlib.sha512()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_archive(s):
    cache = no_symlinks(Path(s["install_root"]) / "downloads")
    archive = no_symlinks(cache / config.ARCHIVE)
    if archive.exists():
        if file_digest(archive) != config.SHA512:
            raise RuntimeError("Cached Spark archive failed SHA-512 verification")
        return archive
    descriptor, partial_name = tempfile.mkstemp(prefix=".spark-download-", suffix=".part", dir=cache)
    os.close(descriptor)
    partial = Path(partial_name)
    try:
        subprocess.run(["curl", "--fail", "--location", "--retry", "3", "--proto", "=https",
                        "--proto-redir", "=https", "--tlsv1.2", config.URL, "-o", str(partial)], check=True)
        if file_digest(partial) != config.SHA512:
            raise RuntimeError("Downloaded Spark archive failed SHA-512 verification")
        partial.replace(archive)
    finally:
        if partial.exists():
            partial.unlink()
    return archive


def validated_members(archive):
    members = archive.getmembers()
    if not members:
        raise RuntimeError("Empty Spark archive")
    seen = set()
    for member in members:
        path = PurePosixPath(member.name)
        if (path.is_absolute() or ".." in path.parts or not path.parts
                or path.parts[0] != config.HOME_NAME or "\\" in member.name
                or not (member.isdir() or member.isfile())):
            raise RuntimeError(f"Unsafe Spark archive entry: {member.name}")
        if str(path) in seen:
            raise RuntimeError(f"Duplicate Spark archive entry: {member.name}")
        seen.add(str(path))
    return members


def install_archive(s, archive):
    root = no_symlinks(s["install_root"])
    home = no_symlinks(s["home"])
    digest_marker = home / ".hw4-archive-sha512"
    if home.exists():
        no_symlinks(digest_marker)
        if not digest_marker.is_file() or digest_marker.read_text().strip() != config.SHA512:
            raise RuntimeError("Existing Spark installation has no matching archive marker")
        if not (home / "bin/spark-submit").is_file() or not (home / "jars").is_dir():
            raise RuntimeError("Existing Spark installation is incomplete")
        return
    stage = Path(tempfile.mkdtemp(prefix=".spark-extract-", dir=root))
    try:
        with tarfile.open(archive, "r:gz") as package:
            # The official distribution contains directories and regular files.
            # Reject links/devices and traversal before writing any archive member.
            members = validated_members(package)
            package.extractall(stage, members=members)
        extracted = stage / config.HOME_NAME
        if not (extracted / "bin/spark-submit").is_file() or not (extracted / "jars").is_dir():
            raise RuntimeError("Spark archive is missing its launcher or jars")
        (extracted / ".hw4-archive-sha512").write_text(config.SHA512 + "\n", encoding="ascii")
        extracted.rename(home)
    finally:
        # Delete only this invocation's unique staging directory, inside our root.
        if stage.parent.resolve() != root.resolve() or not stage.name.startswith(".spark-extract-"):
            raise RuntimeError("Unexpected Spark extraction staging directory")
        no_symlinks(stage)
        shutil.rmtree(stage)


def atomic_write(path, content):
    path = no_symlinks(path)
    descriptor, name = tempfile.mkstemp(prefix=".spark-config-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def require_edge_platform(c):
    if os.name != "posix":
        raise ValueError("Execute deployment on the Linux edge VM")
    import pwd
    if os.geteuid() == 0 or pwd.getpwuid(os.geteuid()).pw_name != c["ssh_user"]:
        raise ValueError("Run Spark deployment as the ordinary Hadoop account in inventory")
    environment = Path(c["install_root"]) / "course-env.sh"
    if not environment.is_file():
        raise RuntimeError("The existing Hadoop course-env.sh was not found")
    command = ("source " + shlex.quote(str(environment))
               + ' && test -r "$HADOOP_CONF_DIR/core-site.xml"'
               + ' && test -r "$HADOOP_CONF_DIR/yarn-site.xml"'
               + ' && "$JAVA_HOME/bin/java" -version')
    result = subprocess.run(["bash", "-c", command], text=True, capture_output=True, check=True)
    if not re.search(r'version "11(?:\.|\")', result.stdout + result.stderr):
        raise RuntimeError("This tested Spark deployment requires the existing Java 11 runtime")
    if not Path("/usr/bin/python3").is_file():
        raise RuntimeError("Python 3 is required at /usr/bin/python3 on the cluster nodes")


def deploy(c):
    s = config.validate(c, live=True)
    require_edge_platform(c)
    root = prepare_root(s)
    archive = ensure_archive(s)
    install_archive(s, archive)
    conf = root / "conf"
    unexpected = {p.name for p in conf.iterdir()} - {"spark-defaults.conf"}
    if unexpected:
        raise RuntimeError("Unexpected files in dedicated Spark conf directory: " + ", ".join(sorted(unexpected)))
    atomic_write(conf / "spark-defaults.conf", "".join(
        f"{key} {value}\n" for key, value in config.defaults(c).items()))
    atomic_write(root / "course-env.sh", config.environment(c))
    print(f"Spark {config.VERSION} installed on edge at {s['home']}.")
    print("Hadoop and Hive configuration files were not modified. Run hw4/run.py for the YARN workload.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "inventory.json")
    parser.add_argument("--output", type=Path, default=ROOT / "generated-spark")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    c = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
    config.validate(c, live=args.execute)
    if args.execute:
        deploy(c)
    else:
        print(config.render(c, args.output))
        print(f"Archive: {config.URL}\nSHA-512: {config.SHA512}")
        print("Preview only; no network or cluster changes. Execute on edge with --execute.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, RuntimeError, tarfile.TarError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error))
