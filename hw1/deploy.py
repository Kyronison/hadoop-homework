#!/usr/bin/env python3
from pathlib import Path
import subprocess
import sys

if __name__ == "__main__":
    raise SystemExit(subprocess.call([sys.executable, str(Path(__file__).resolve().parents[1] / "common/cluster.py"), "hdfs", *sys.argv[1:]]))
