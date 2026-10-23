"""Restricted HTTPS frontends for the teaching cluster's web interfaces.

This is a renderer, not a deployment command. GET alone is not a security policy:
administration servlets and WebHDFS are deliberately not published.
"""
import ipaddress
from pathlib import PurePosixPath
import re

MARKER = "# Managed by teachbase-hadoop hw2/deploy_web.py; do not edit."
LABELS = ("nn", "snn", "dn1", "dn2", "dn3", "rm", "nm1", "nm2", "nm3", "history", "proxy")
PORTS = (9870, 9868, 9864, 9864, 9864, 8088, 8042, 8042, 8042, 19888, 9099)
NODE_INDEX = (0, 1, 1, 2, 3, 0, 1, 2, 3, 0, 0)
STATIC = r"^/(?:static|css|js|images|fonts)/.*\.(?:css|js|png|gif|jpg|jpeg|svg|ico|woff|woff2|ttf|map)$"
SENSITIVE = r"(?:^|/)(?:webhdfs|logLevel|conf|stacks|fsck|imagetransfer|prof|prof-output|jmxproxy|reconfiguration|startupProgress)(?:/|$)"
# Application-specific configuration is omitted too: it can contain credentials.
PROXY_UI = r"^/proxy/application_[0-9]+_[0-9]+(?:/|/(?:mapreduce(?:/(?:app|job|jobs|jobcounters|singlejobcounter|tasks|attempts|task|counters|singletaskcounter)(?:/[^?]*)?)?/?|static/[^?]*\.(?:css|js|png|gif|jpg|jpeg|svg|ico|woff|woff2|ttf)))?$"


def host(value):
    if not isinstance(value, str) or len(value) > 253:
        raise ValueError("Invalid public host or node alias")
    try:
        addr = ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError:
        if not re.fullmatch(r"(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*", value):
            raise ValueError("Host must be an IPv4 address or DNS hostname")
        if re.fullmatch(r"[0-9.]+", value):
            raise ValueError("Invalid IPv4 address")
    else:
        if addr.is_unspecified or addr.is_multicast or addr.is_loopback:
            raise ValueError("Public host must be a usable non-loopback address")
    return value


def _path(value):
    if not isinstance(value, str) or not re.fullmatch(r"/[A-Za-z0-9_./-]+", value) or ".." in PurePosixPath(value).parts:
        raise ValueError("TLS and authentication paths must be plain absolute paths")
    return value


def validate_web(c):
    w = c.get("web", {})
    if not isinstance(w, dict):
        raise ValueError("web must be an object")
    if w.get("enabled", False) is not True:
        if w.get("enabled", False) is not False:
            raise ValueError("web.enabled must be boolean")
        return None
    host(w["public_host"])
    base = w.get("port_base", 18443)
    if type(base) is not int or not 1024 <= base <= 65525:
        raise ValueError("port_base must leave eleven unprivileged TCP ports")
    if set(range(base, base + 11)) & set(PORTS):
        raise ValueError("Public ports overlap native Hadoop web ports")
    for key in ("certificate", "certificate_key", "auth_file"):
        _path(w[key])
    if len({w[key] for key in ("certificate", "certificate_key", "auth_file")}) != 3:
        raise ValueError("Certificate, key and authentication files must be distinct")
    if not isinstance(w.get("allowed_cidrs", []), list):
        raise ValueError("allowed_cidrs must be a list")
    for cidr in w.get("allowed_cidrs", []):
        network = ipaddress.ip_network(cidr, strict=True)
        if not network.prefixlen:
            raise ValueError("Use an empty optional IP allowlist instead of /0")
    if len(c["nodes"]) != 4:
        raise ValueError("Four nodes are required")
    for node in c["nodes"]:
        ipaddress.IPv4Address(node["private_ip"])
        host(node["ssh_host"])
        aliases = node.get("aliases", [])
        if not isinstance(aliases, list):
            raise ValueError("Node aliases must be a list")
        for alias in aliases:
            host(alias)
    return w


def public_endpoints(c):
    w = validate_web(c)
    if w is None:
        return []
    return [(label, w["public_host"], w.get("port_base", 18443) + i) for i, label in enumerate(LABELS)]


def _routes(label):
    routes = [r"^/favicon\.ico$", STATIC]
    if label in ("nn", "snn") or label.startswith("dn"):
        page = "dfshealth" if label == "nn" else "status" if label == "snn" else "datanode"
        script = "dfshealth" if label == "nn" else "snn" if label == "snn" else "dn"
        routes += [rf"^/(?:{page}\.html)?$", rf"^/{script}\.js$", r"^/jmx$"]
    elif label == "rm":
        routes += [r"^/cluster(?:/(?:cluster|nodes|apps|app|appattempt|container|scheduler|about|nodelabels)(?:/[^?]*)?)?/?$",
                   r"^/ws/v1/cluster/(?:info|metrics|nodes|apps|appstatistics|scheduler|scheduler-overview)(?:/[^?]*)?$", r"^/jmx$"]
    elif label.startswith("nm"):
        routes += [r"^/node(?:/(?:node|allApplications|allContainers|application|container|containerlogs)(?:/[^?]*)?)?/?$",
                   r"^/ws/v1/node/(?:info|apps|containers|resources)(?:/[^?]*)?$", r"^/jmx$"]
    elif label == "history":
        routes += [r"^/jobhistory(?:/(?:app|jobs|job|jobcounters|singlejobcounter|tasks|attempts|task|counters|singletaskcounter|logs|about)(?:/[^?]*)?)?/?$",
                   r"^/ws/v1/history/(?:info|mapreduce/jobs)(?:/[^?]*)?$", r"^/jmx$"]
    else:
        routes += [PROXY_UI]
    return routes


def nginx(c):
    w = validate_web(c)
    if w is None:
        return ""
    public = public_endpoints(c)
    replacements = []
    for i, (_, public_host, public_port) in enumerate(public):
        node = c["nodes"][NODE_INDEX[i]]
        names = {node["private_ip"], node["ssh_host"], *node.get("aliases", [])}
        destination = f"{public_host}:{public_port}"
        for name in sorted(names):
            source = f"{name}:{PORTS[i]}"
            replacements.append((source, destination))
    out = [MARKER, "# TLS + authentication + restricted routes; this is not Hadoop authorization."]
    for i, (label, public_host, public_port) in enumerate(public):
        ip = c["nodes"][NODE_INDEX[i]]["private_ip"]
        lines = ["server {", f"    listen {public_port} ssl;", f"    server_name {public_host};",
                 f"    ssl_certificate {w['certificate']};", f"    ssl_certificate_key {w['certificate_key']};",
                 "    ssl_protocols TLSv1.2 TLSv1.3;", "    server_tokens off;",
                 f"    auth_basic \"Hadoop course: {label}\";", f"    auth_basic_user_file {w['auth_file']};",
                 "    if ($request_method !~ ^(GET|HEAD)$) { return 405; }",
                 # Some Hadoop webapps reject HEAD. Nginx handles the client's
                 # HEAD response semantics while the upstream sees a plain GET.
                 "    proxy_method GET;", "    proxy_pass_request_body off;",
                 "    proxy_set_header Content-Length \"\";",
                 # Authorization is never given to an upstream, especially an AM.
                 "    proxy_set_header Authorization \"\";", "    proxy_set_header Proxy-Authorization \"\";",
                 f"    proxy_set_header Host {ip}:{PORTS[i]};",
                 "    proxy_set_header X-Forwarded-Proto https;", "    proxy_set_header X-Forwarded-Host $http_host;",
                 "    proxy_set_header X-Forwarded-For $remote_addr;", "    proxy_set_header Accept-Encoding \"\";",
                 "    proxy_connect_timeout 10s;", "    proxy_read_timeout 60s;",
                 "    proxy_hide_header Content-Security-Policy;",
                 "    add_header Content-Security-Policy \"upgrade-insecure-requests; frame-ancestors 'self'\" always;",
                 "    add_header X-Content-Type-Options nosniff always;",
                 "    add_header Referrer-Policy no-referrer always;", "    add_header Cache-Control no-store always;",
                 "    sub_filter_once off;",
                 "    sub_filter_types application/json application/javascript text/javascript text/plain;"]
        if w.get("allowed_cidrs"):
            lines += [f"    allow {cidr};" for cidr in w["allowed_cidrs"]] + ["    deny all;"]
        for source, destination in replacements:
            lines += [f"    proxy_redirect http://{source}/ https://{destination}/;",
                      f"    proxy_redirect //{source}/ https://{destination}/;",
                      f"    sub_filter 'http://{source}' 'https://{destination}';",
                      f"    sub_filter '//{source}' '//{destination}';",
                      f"    sub_filter '{source}' '{destination}';"]
        lines += [f"    location ~* {SENSITIVE} {{ return 403; }}"]
        for pattern in _routes(label):
            lines += [f"    location ~ {pattern} {{", f"        proxy_pass http://{ip}:{PORTS[i]};", "    }"]
        lines += ["    location / { return 404; }", "}"]
        out.append("\n".join(lines))
    return "\n\n".join(out) + "\n"
