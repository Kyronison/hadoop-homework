#!/usr/bin/env python3
"""Check forward and reverse name resolution from every VM without changing it."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import inspect
import json
from pathlib import Path
import shlex
import socket
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.cluster import remote, validate


def probe(nodes):
    rows, errors = [], []
    for node in nodes:
        host, expected = node["ssh_host"], node["private_ip"]
        try:
            def addresses(name):
                return {row[4][0] for row in socket.getaddrinfo(name, None, socket.AF_INET)}

            forward = addresses(host)
            if forward != {expected}:
                raise ValueError(f"{host} resolves to {sorted(forward)}, expected only {expected}")
            canonical = socket.getfqdn(host)
            if addresses(canonical) != {expected}:
                raise ValueError(f"Canonical name {canonical} does not resolve only to {expected}")
            reverse_name = socket.gethostbyaddr(expected)[0]
            if addresses(reverse_name) != {expected}:
                raise ValueError(f"Reverse name {reverse_name} does not resolve only to {expected}")
            rows.append({"host": host, "ip": expected, "canonical": canonical,
                         "reverse_name": reverse_name})
        except (OSError, ValueError) as exc:
            errors.append(f"{host}: {exc}")
    if errors:
        raise RuntimeError("Name resolution check failed:\n" + "\n".join(errors))
    return rows


def command_for(nodes):
    source = ("import json, socket, sys\n" + inspect.getsource(probe)
              + "\nprint(json.dumps(probe(json.loads(sys.argv[1])), indent=2))\n")
    targets = [{"ssh_host": n["ssh_host"], "private_ip": n["private_ip"]} for n in nodes]
    return shlex.join(["python3", "-c", source, json.dumps(targets)])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true", help="Run read-only checks over SSH")
    args = parser.parse_args(argv)
    try:
        config = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
        validate(config, live=args.execute)
        if not args.execute:
            print("Preview: check all four inventory names, canonical names and reverse mappings on each VM.")
            print("No connection made. Add --execute to run the read-only checks.")
            return 0
        command = command_for(config["nodes"])

        def run(node):
            try:
                result = remote(config, node, command, capture=True)
            except subprocess.CalledProcessError as exc:
                raise RuntimeError(f"{node['name']}: {exc.stderr or exc}") from exc
            return node["name"], result.stdout

        with ThreadPoolExecutor(max_workers=4) as pool:
            for role, output in pool.map(run, config["nodes"]):
                print(f"{role}: PASS\n{output}", end="")
        return 0
    except (RuntimeError, ValueError, KeyError, OSError) as exc:
        parser.exit(1, str(exc) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
