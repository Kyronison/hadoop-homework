#!/usr/bin/env python3
"""Print or open SSH tunnels to the eleven Hadoop web interfaces."""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from common.cluster import endpoints, validate


def validate_gateway(gateway):
    """Accept one SSH user and a DNS name or IPv4 address, without SSH options."""
    if gateway.count("@") != 1:
        raise ValueError("Gateway must have the form user@hostname or user@IPv4")
    user, host = gateway.split("@")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", user):
        raise ValueError("Invalid gateway username")
    if len(host) > 253 or not host:
        raise ValueError("Invalid gateway hostname")
    if re.fullmatch(r"[0-9.]+", host):
        try:
            ipaddress.IPv4Address(host)
        except ValueError as exc:
            raise ValueError("Invalid gateway IPv4 address") from exc
    elif any(not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
             for label in host.split(".")):
        raise ValueError("Invalid gateway hostname")


def build_command(config, gateway, identity, port=22, known_hosts=None):
    """Build an argv list without connecting to a host or requiring key files."""
    validate(config, live=False)
    validate_gateway(gateway)
    if not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("SSH port must be between 1 and 65535")
    command = [
        "ssh", "-N", "-T", "-p", str(port), "-i", str(Path(identity).expanduser()),
        "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
        "-o", "ExitOnForwardFailure=yes", "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
    ]
    if known_hosts is not None:
        command += ["-o", f"UserKnownHostsFile={Path(known_hosts).expanduser()}"]
    for i, (_, host, target_port) in enumerate(endpoints(config)):
        command += ["-L", f"127.0.0.1:{19000 + i}:{host}:{target_port}"]
    command.append(gateway)
    return command


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "inventory.json")
    parser.add_argument("--gateway", required=True, help="SSH entry point: user@hostname or user@IPv4")
    parser.add_argument("--identity", type=Path, required=True, help="Private SSH key on this computer")
    parser.add_argument("--port", type=int, default=22, help="Gateway SSH port (default: 22)")
    parser.add_argument("--known-hosts", type=Path, help="Optional file of already verified server keys")
    parser.add_argument("--execute", action="store_true", help="Open tunnels and keep SSH in the foreground")
    args = parser.parse_args(argv)
    try:
        config = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
        command = build_command(config, args.gateway, args.identity, args.port, args.known_hosts)
        if args.execute:
            if not args.identity.expanduser().is_file():
                raise ValueError("Local SSH identity file does not exist")
            if args.known_hosts is not None and not args.known_hosts.expanduser().is_file():
                raise ValueError("Known-hosts file does not exist")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.error(str(exc))

    for i, (label, _, _) in enumerate(endpoints(config)):
        path = "/proxy/application_ID/" if label == "proxy" else "/"
        print(f"{label}: http://127.0.0.1:{19000 + i}{path}")
    printable = subprocess.list2cmdline(command) if os.name == "nt" else shlex.join(command)
    print("\n" + printable, flush=True)
    if not args.execute:
        print("Preview only. Add --execute to open the tunnels.")
        return 0
    print("Keep this terminal open. Ctrl+C closes the tunnels.", flush=True)
    try:
        return subprocess.run(command, check=False).returncode
    except KeyboardInterrupt:
        return 130
    except OSError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
