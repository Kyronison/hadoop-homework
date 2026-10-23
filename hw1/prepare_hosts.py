#!/usr/bin/env python3
"""Resolve the provisioned VM hostname on its private interface, not loopback."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shlex
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.cluster import remote, validate

SCRIPT = r'''
import datetime, ipaddress, pathlib, shutil, socket, sys
hostname, private_ip = sys.argv[1:]
assert socket.gethostname() == hostname, "Unexpected VM hostname"
path = pathlib.Path("/etc/hosts")
original = path.read_text()
aliases = {hostname}
for row in original.splitlines():
    fields = row.split("#", 1)[0].split()
    if len(fields) > 1 and hostname in fields[1:]:
        aliases.update(x for x in fields[1:] if x == hostname or x.startswith(hostname + "."))
rows = []
for row in original.splitlines():
    body, marker, comment = row.partition("#")
    fields = body.split()
    if len(fields) > 1:
        remaining = [x for x in fields[1:] if x not in aliases]
        if remaining != fields[1:]:
            if remaining:
                rows.append(fields[0] + "\t" + " ".join(remaining) + ((" #" + comment) if marker else ""))
            elif marker:
                rows.append("#" + comment)
            continue
    rows.append(row)
rows.append(private_ip + "\t" + " ".join([hostname] + sorted(aliases - {hostname})))
updated = "\n".join(rows) + "\n"
if updated != original:
    backup = str(path) + ".teachbase-backup-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    shutil.copy2(path, backup)
    path.write_text(updated)
    print("Updated /etc/hosts; backup:", backup)
addresses = {x[4][0] for x in socket.getaddrinfo(hostname, None, socket.AF_INET)}
assert addresses == {private_ip}, (hostname, addresses)
print(hostname, sorted(addresses), "OK")
'''


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--inventory', type=Path, required=True)
    p.add_argument('--execute', action='store_true')
    a = p.parse_args()
    c = json.loads(a.inventory.read_text(encoding='utf-8-sig'))
    validate(c, live=a.execute)
    if not a.execute:
        p.error('Changing /etc/hosts requires --execute')
    def run(n):
        command = shlex.join(['sudo', '-n', 'python3', '-c', SCRIPT, n['ssh_host'], n['private_ip']])
        return remote(c, n, command, capture=True).stdout
    with ThreadPoolExecutor(max_workers=4) as pool:
        for result in pool.map(run, c['nodes']):
            print(result, end='')


if __name__ == '__main__':
    main()
