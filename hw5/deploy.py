#!/usr/bin/env python3
"""Install Prefect into a dedicated virtualenv on edge next to the existing Spark."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from hw5 import config, pipeline

REQUIREMENTS = ROOT / "hw5/requirements.txt"
CONSTRAINTS = ROOT / "hw5/constraints.txt"


def no_symlinks(path):
    path = Path(path).absolute()
    for part in (path, *path.parents):
        if part.is_symlink():
            raise RuntimeError(f"Refusing symlink in Prefect installation path: {part}")
    return path


def prepare_root(s):
    root = no_symlinks(s["install_root"])
    if root.exists() and (not root.is_dir() or root.stat().st_uid != os.getuid()):
        raise RuntimeError("Prefect root must be a directory owned by the current user")
    marker = root / config.MARKER
    if root.exists() and any(root.iterdir()):
        if not marker.is_file() or marker.read_text(encoding="utf-8").strip() != config.OWNER:
            raise RuntimeError("Existing nonempty Prefect root lacks the expected ownership marker")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    marker.write_text(config.OWNER + "\n", encoding="utf-8")
    for name in ("home", "evidence"):
        (root / name).mkdir(exist_ok=True, mode=0o700)
    return root


def require_edge_platform(c, s):
    if os.name != "posix":
        raise ValueError("Execute deployment on the Linux edge VM")
    import pwd
    if os.geteuid() == 0 or pwd.getpwuid(os.geteuid()).pw_name != c["ssh_user"]:
        raise ValueError("Run the deployment as the ordinary Hadoop account in inventory")
    for path in (s["spark_env"], s["spark_home"] + "/bin/spark-submit",
                 s["spark_home"] + "/python/lib/" + config.PY4J):
        if not Path(path).is_file():
            raise RuntimeError(f"{path} not found: install Spark with hw4/deploy.py first")
    if not Path("/usr/bin/python3").is_file():
        raise RuntimeError("Python 3 is required at /usr/bin/python3")


def atomic_write(path, content):
    path = no_symlinks(path)
    descriptor, name = tempfile.mkstemp(prefix=".prefect-config-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def install(s):
    venv = Path(s["venv"])
    if not (venv / "bin/python").is_file():
        subprocess.run(["/usr/bin/python3", "-m", "venv", str(venv)], check=True)
    python = str(venv / "bin/python")
    subprocess.run([python, "-m", "pip", "install", "--quiet", "--disable-pip-version-check",
                    "-r", str(REQUIREMENTS), "-c", str(CONSTRAINTS)], check=True)


def smoke_test(s):
    """The flow interpreter must see both Prefect and the PySpark of the installed Spark."""
    probe = ("import prefect, pyspark, sys; "
             "print(prefect.__version__, pyspark.__version__, sys.version.split()[0])")
    command = "source " + shlex.quote(s["env_file"]) + " && python -c " + shlex.quote(probe)
    result = subprocess.run(["bash", "-c", command], text=True, capture_output=True, check=True)
    prefect_version, spark_version, python_version = result.stdout.split()
    if prefect_version != config.VERSION or spark_version != "3.5.9":
        raise RuntimeError(f"Unexpected versions: Prefect {prefect_version}, PySpark {spark_version}")
    return prefect_version, spark_version, python_version


def deploy(c):
    s = config.validate(c, live=True)
    require_edge_platform(c, s)
    root = prepare_root(s)
    install(s)
    atomic_write(root / "course-env.sh", config.environment(c))
    versions = smoke_test(s)
    print("Prefect {} with PySpark {} on Python {} installed at {}.".format(*versions, s["venv"]))
    print("Spark, Hadoop and Hive files were not modified. Next: python hw5/flow.py --inventory inventory.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "inventory.json")
    parser.add_argument("--output", type=Path, default=ROOT / "generated-prefect")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    c = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
    s = config.validate(c, live=args.execute)
    if args.execute:
        deploy(c)
    else:
        print(config.render(c, args.output))
        print(pipeline.hive_sql(config.HIVE_DATABASE, config.TABLE, s["filesystem"],
                                s["hdfs_root"], config.OWNER))
        print(pipeline.publish_sql(config.HIVE_DATABASE, config.TABLE,
                                   s["filesystem"] + s["hdfs_root"] + "/runs/run-" + "0" * 32 + "/data"))
        print(f"Prefect {config.VERSION} into {s['venv']}; flow results under {s['hdfs_root']}.")
        print("Preview only; no network or cluster changes. Execute on edge with --execute.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error))
