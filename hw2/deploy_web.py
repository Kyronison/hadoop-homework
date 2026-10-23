#!/usr/bin/env python3
"""Install only the managed Nginx frontends on edge; TLS/auth must already exist."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile

import web

TARGET = "/etc/nginx/conf.d/teachbase-hadoop.conf"


def sudo(*args, capture=False):
    return subprocess.run(["sudo", "-n", *args], check=True, text=True,
                          capture_output=capture, stdin=subprocess.DEVNULL)


def install(c):
    config = web.nginx(c)
    if not config:
        raise ValueError("Enable web explicitly before installation")
    edge = c["nodes"][1]
    actual = socket.gethostname()
    names = {edge["ssh_host"], *edge.get("aliases", [])}
    if actual not in names:
        raise ValueError("Run this command on the edge node named in inventory")
    if os.name != "posix":
        raise ValueError("Installation runs on the Linux edge VM")
    for command in ("sudo", "openssl", "nginx"):
        if not shutil.which(command):
            raise ValueError(f"Install {command} before deploying web interfaces")
    w = web.validate_web(c)
    for key in ("certificate", "certificate_key", "auth_file"):
        sudo("test", "-s", w[key])
    sudo("openssl", "x509", "-in", w["certificate"], "-noout", "-checkend", "0", capture=True)
    san = sudo("openssl", "x509", "-in", w["certificate"], "-noout", "-ext", "subjectAltName", capture=True).stdout
    if "Subject Alternative Name" not in san:
        raise ValueError("TLS certificate must include a subjectAltName extension")
    try:
        import ipaddress
        ipaddress.IPv4Address(w["public_host"])
        identity_flag = "-checkip"
    except ValueError:
        identity_flag = "-checkhost"
    sudo("openssl", "x509", "-in", w["certificate"], "-noout", identity_flag, w["public_host"], capture=True)
    cert_pub = sudo("openssl", "x509", "-in", w["certificate"], "-noout", "-pubkey", capture=True).stdout.strip()
    key_pub = sudo("openssl", "pkey", "-in", w["certificate_key"], "-pubout", capture=True).stdout.strip()
    if cert_pub != key_pub:
        raise ValueError("TLS certificate and private key do not match")
    # Existing unrelated configurations are never overwritten or removed.
    exists = subprocess.run(["sudo", "-n", "test", "-e", TARGET], stdin=subprocess.DEVNULL).returncode
    if exists not in (0, 1):
        raise RuntimeError("Cannot inspect the managed config path")
    previous = None
    if exists == 0:
        previous = sudo("cat", TARGET, capture=True).stdout
        if not previous.startswith(web.MARKER + "\n"):
            raise ValueError("Refusing to replace an unrelated Nginx configuration")
    sudo("nginx", "-t", capture=True)
    with tempfile.TemporaryDirectory(prefix="teachbase-web-") as td:
        staged = Path(td) / "nginx.conf"
        staged.write_text(config, encoding="utf-8", newline="\n")
        backup = Path(td) / "previous.conf"
        if previous is not None:
            backup.write_text(previous, encoding="utf-8", newline="\n")
        # mktemp is beside the destination, so rename is atomic on one filesystem.
        sibling = sudo("mktemp", "/etc/nginx/conf.d/.teachbase-hadoop.XXXXXXXX", capture=True).stdout.strip()
        import re
        if not re.fullmatch(r"/etc/nginx/conf.d/\.teachbase-hadoop\.[A-Za-z0-9]{8}", sibling):
            raise RuntimeError("Unexpected staging path from mktemp")
        activated = False
        try:
            sudo("install", "-o", "root", "-g", "root", "-m", "0644", str(staged), sibling)
            sudo("mv", "-T", sibling, TARGET)
            activated = True
            sudo("nginx", "-t", capture=True)
            sudo("systemctl", "reload", "nginx", capture=True)
        except BaseException:
            if activated:
                if previous is None:
                    sudo("rm", "--", TARGET)
                else:
                    sudo("install", "-o", "root", "-g", "root", "-m", "0644", str(backup), sibling)
                    sudo("mv", "-T", sibling, TARGET)
                sudo("nginx", "-t", capture=True)
                sudo("systemctl", "reload", "nginx", capture=True)
            raise
        finally:
            sudo("rm", "-f", "--", sibling)
    print("Managed Nginx configuration installed and reloaded. Verify external HTTPS and browser navigation separately.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=Path(__file__).resolve().parents[1] / "inventory.json")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    c = json.loads(args.inventory.read_text(encoding="utf-8-sig"))
    preview = web.nginx(c)
    if not preview:
        parser.error("web.enabled must be true")
    if not args.execute:
        print(preview)
        print("# Preview only. Run on edge with --execute after provisioning TLS and authentication files.")
        return
    install(c)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        # Never print stdout/stderr from openssl or authentication file contents.
        raise SystemExit(str(error))
