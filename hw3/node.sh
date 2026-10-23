#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
SELF="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SELF/node.env"
ACTION="${1:?Specify an action}"
die() { printf '%s\n' "$*" >&2; exit 1; }
[[ "$(id -u)" != 0 && "$(id -un)" == "$RUN_USER" ]] || die 'Run Hive as the ordinary Hadoop account from inventory.'
[[ "$BASE" == /*/teachbase-hive && "$HADOOP_BASE" == /*/teachbase-hadoop ]] || die 'Invalid dedicated installation directory.'
[[ "$VERSION" == 4.0.0-alpha-2 ]] || die 'Unexpected Hive version.'
case "$ROLE" in metastore|server|client) ;; *) die 'Unknown Hive role';; esac

load_hadoop() {
    [[ -f "$HADOOP_BASE/.course-owned" && -f "$HADOOP_BASE/course-env.sh" ]] || die 'Install and verify HDFS/YARN first.'
    source "$HADOOP_BASE/course-env.sh"
    for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml; do
        [[ -s "$HADOOP_CONF_DIR/$f" ]] || die "Missing Hadoop configuration: $f"
    done
    "$JAVA_HOME/bin/java" -version 2>&1 | grep -Eq 'version "11\.' || die 'This exercise is tested with the existing Java 11 installation.'
}

prepare() {
    load_hadoop
    for command in python3 tar sha256sum curl nohup; do command -v "$command" >/dev/null || die "Missing command: $command"; done
    python3 - "$BASE" "$HADOOP_BASE" <<'PY'
from pathlib import Path
import sys
base, hadoop = map(Path, sys.argv[1:])
if base.is_symlink() or base.resolve() == hadoop.resolve() or hadoop.resolve() in base.resolve().parents:
    raise SystemExit('Hive directory must be separate and not a symlink')
if (base / '.course-owned').is_symlink():
    raise SystemExit('Hive ownership marker must not be a symbolic link')
if base.exists() and not (base / '.course-owned').is_file():
    # A downloaded archive alone is an allowed cache, not an existing installation.
    if any(p.name != 'downloads' for p in base.iterdir()):
        raise SystemExit('Nonempty Hive directory without ownership marker')
    downloads = base / 'downloads'
    if downloads.is_symlink() or (downloads.exists() and any(p.name not in {
        'apache-hive-4.0.0-alpha-2-bin.tar.gz',
        'apache-hive-4.0.0-alpha-2-bin.tar.gz.part'} or not p.is_file() or p.is_symlink()
        for p in downloads.iterdir())):
        raise SystemExit('Unexpected files in the unowned Hive cache')
elif base.exists() and (base / '.course-owned').read_text().strip() != 'teachbase-hive-v1':
    raise SystemExit('Unexpected Hive ownership marker')
for name in ('downloads', 'conf', 'run', 'logs', 'tmp', 'artifacts', 'evidence', 'private'):
    if (base / name).is_symlink():
        raise SystemExit('Hive subdirectories must not be symbolic links')
PY
    mkdir -p "$BASE"/{downloads,conf,run,logs,tmp,artifacts,evidence,private}
    chmod 700 "$BASE" "$BASE/private" "$BASE/conf" "$BASE/run"
    printf '%s\n' 'teachbase-hive-v1' > "$BASE/.course-owned"
}

load_env() {
    [[ -f "$BASE/.course-owned" && -f "$BASE/course-env.sh" ]] || die 'Run Hive installation first.'
    source "$BASE/course-env.sh"
}

service_name() {
    if [[ "$ROLE" == metastore ]]; then printf '%s' metastore
    elif [[ "$ROLE" == server ]]; then printf '%s' hiveserver2
    else die 'Client has no daemon'; fi
}

running() {
    local name pid args uid state
    name="$(service_name)"
    [[ -s "$BASE/run/$name.pid" ]] || return 1
    pid="$(cat "$BASE/run/$name.pid")"
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || die 'Invalid owned PID file.'
    kill -0 "$pid" 2>/dev/null || return 1
    uid="$(ps -p "$pid" -o uid= | tr -d ' ')"
    state="$(ps -p "$pid" -o stat= | tr -d ' ')"
    if [[ "$uid" == "$(id -u)" && "$state" == Z* ]]; then return 1; fi
    args="$(ps -p "$pid" -o args=)"
    [[ "$uid" == "$(id -u)" && "$args" == *"$BASE/"* ]] || die 'PID belongs to an unrelated process; refusing to signal or reuse it.'
    return 0
}

copy_config() {
    local source="$1" target="$2"
    [[ ! -L "$target" ]] || die "Refusing to replace a symbolic link: $target"
    if [[ -f "$target" ]] && cmp -s "$source" "$target"; then return; fi
    if [[ -f "$target" ]]; then
        if [[ "$ROLE" != client ]] && running; then
            die "Configuration changed while Hive is running: stop this Hive service first ($target)."
        fi
        cp -p "$target" "$target.backup.$(date -u +%Y%m%dT%H%M%SZ)"
    fi
    cp "$source" "$target"
    chmod 600 "$target"
}

set_database_password() {
    python3 - "$1" "$BASE/private/postgres-password" <<'PY'
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
target, secret = map(Path, sys.argv[1:])
if not secret.is_file() or secret.is_symlink():
    raise SystemExit('Master PostgreSQL password is missing or unsafe')
password = secret.read_text().strip()
if len(password) < 32:
    raise SystemExit('Invalid stored database password')
tree = ET.parse(target)
found = False
for prop in tree.getroot().findall('property'):
    if prop.findtext('name') == 'javax.jdo.option.ConnectionPassword':
        prop.find('value').text = password
        found = True
if not found:
    raise SystemExit('Password property absent from the Metastore configuration')
ET.indent(tree)
tree.write(target, encoding='utf-8', xml_declaration=True)
target.chmod(0o600)
PY
}

install() {
    prepare
    printf '%s  %s\n' "$ARCHIVE_SHA256" "$BASE/downloads/$ARCHIVE" | sha256sum --check --status || die 'Hive archive SHA-256 mismatch.'
    hive_home="$BASE/apache-hive-$VERSION-bin"
    if [[ ! -x "$hive_home/bin/hive" ]]; then
        [[ ! -e "$hive_home" ]] || die 'Incomplete Hive directory already exists; inspect it before retrying.'
        tar -xzf "$BASE/downloads/$ARCHIVE" -C "$BASE" --no-same-owner
    fi
    # Preserve Hadoop configuration; HMS needs one process-local proxy-host setting.
    for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml; do
        if [[ "$ROLE" == metastore && "$f" == core-site.xml ]]; then
            python3 - "$HADOOP_CONF_DIR/$f" "$BASE/conf/$f.new" "$RUN_USER" "$SERVER_IP" <<'PY'
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
source, target, user, host = sys.argv[1:]
tree = ET.parse(source)
key = f'hadoop.proxyuser.{user}.hosts'
for prop in list(tree.getroot().findall('property')):
    if prop.findtext('name') == key:
        tree.getroot().remove(prop)
prop = ET.SubElement(tree.getroot(), 'property')
ET.SubElement(prop, 'name').text = key
ET.SubElement(prop, 'value').text = host
ET.indent(tree)
tree.write(target, encoding='utf-8', xml_declaration=True)
Path(target).chmod(0o600)
PY
            if [[ -L "$BASE/conf/$f" ]]; then
                [[ "$(readlink "$BASE/conf/$f")" == "$HADOOP_CONF_DIR/$f" ]] || die 'Unexpected core-site symlink.'
                running && die 'Stop Metastore before changing its process-local Hadoop configuration.'
                rm -- "$BASE/conf/$f"  # Remove only our symlink, never its Hadoop target.
            fi
            copy_config "$BASE/conf/$f.new" "$BASE/conf/$f"
            continue
        fi
        if [[ -e "$BASE/conf/$f" || -L "$BASE/conf/$f" ]]; then
            [[ -L "$BASE/conf/$f" && "$(readlink "$BASE/conf/$f")" == "$HADOOP_CONF_DIR/$f" ]] || die "Unexpected Hive Hadoop config: $f"
        else
            ln -s "$HADOOP_CONF_DIR/$f" "$BASE/conf/$f"
        fi
    done
    {
        printf 'source %q\n' "$HADOOP_BASE/course-env.sh"
        printf 'export HIVE_HOME=%q\n' "$hive_home"
        printf 'export HIVE_CONF_DIR=%q\n' "$BASE/conf"
        printf 'export HADOOP_HEAPSIZE=%q\n' "$HIVE_HEAP_MB"
        printf 'export HADOOP_HEAPSIZE_MAX=%q\n' "$HIVE_HEAP_MB"
        printf 'export HIVE_LOG_DIR=%q\n' "$BASE/logs"
        printf 'export HIVE_PID_DIR=%q\n' "$BASE/run"
        printf 'export HIVE_METASTORE_PORT=%q\n' "$METASTORE_PORT"
        printf 'export HIVE_SERVER2_THRIFT_PORT=%q\n' "$SERVER_PORT"
        printf 'export PATH="$JAVA_HOME/bin:$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH"\n'
    } > "$BASE/course-env.sh.new"
    copy_config "$BASE/course-env.sh.new" "$BASE/course-env.sh"
    cp "$SELF/hive-site.xml" "$BASE/conf/hive-site.xml.new"
    if [[ "$ROLE" == metastore && -f "$BASE/private/postgres-password" ]]; then
        set_database_password "$BASE/conf/hive-site.xml.new"
    fi
    copy_config "$BASE/conf/hive-site.xml.new" "$BASE/conf/hive-site.xml"
    if [[ "$SELF" != "$BASE/artifacts" ]]; then
        cp "$SELF/node.sh" "$SELF/node.env" "$SELF/hive-site.xml" "$BASE/artifacts/"
    fi
    load_env
    printf 'Installed Hive %s for %s on %s.\n' "$VERSION" "$ROLE" "$NODE"
}

configure_postgres() {
    [[ "$ROLE" == metastore ]] || die 'PostgreSQL belongs only on master.'
    load_env
    if ! dpkg-query -W -f='${Status}' postgresql-16 2>/dev/null | grep -q 'install ok installed'; then
        sudo -n apt-get update
    fi
    sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-16
    sudo -n systemctl start postgresql@16-main
    python3 - "$BASE" "$DB_NAME" "$DB_USER" <<'PY'
from pathlib import Path
import re
import secrets
import subprocess
import sys
base, database, user = sys.argv[1:]
for value in (database, user):
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,39}', value):
        raise SystemExit('Invalid PostgreSQL identifier')
def sql(query, db='postgres'):
    result = subprocess.run(['sudo', '-n', '-u', 'postgres', 'psql', '-X', '-q', '-A', '-t',
                             '-p', '5432', '-v', 'ON_ERROR_STOP=1', '-d', db],
                            input=query, text=True, capture_output=True)
    if result.returncode:
        # SQL can contain the password. Do not echo its text or PostgreSQL DETAIL.
        raise SystemExit('PostgreSQL setup query failed; inspect server logs locally')
    return result.stdout.strip()
if not 160000 <= int(sql('SHOW server_version_num;')) < 170000:
    raise SystemExit('Expected PostgreSQL 16 on loopback port 5432')
listen = {part.strip() for part in sql('SHOW listen_addresses;').split(',')}
if not listen <= {'localhost', '127.0.0.1', '::1'}:
    raise SystemExit('PostgreSQL is not limited to loopback; refusing to change an existing instance')
secret = Path(base) / 'private/postgres-password'
role_exists = sql(f"SELECT 1 FROM pg_roles WHERE rolname='{user}';") == '1'
db_exists = sql(f"SELECT 1 FROM pg_database WHERE datname='{database}';") == '1'
if not secret.exists():
    if role_exists or db_exists:
        raise SystemExit('Existing PostgreSQL role/database without our password file; refusing to claim it')
    with secret.open('x', encoding='utf-8') as stream:
        stream.write(secrets.token_hex(32) + '\n')
    secret.chmod(0o600)
if secret.is_symlink() or secret.stat().st_mode & 0o077:
    raise SystemExit('Database password must be a private regular file')
password = secret.read_text().strip()
if not re.fullmatch(r'[0-9a-f]{64}', password):
    raise SystemExit('Unexpected database password format')
if not role_exists:
    sql(f"CREATE ROLE {user} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}';")
else:
    dangerous = sql(f"SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls FROM pg_roles WHERE rolname='{user}';")
    if dangerous != 'f':
        raise SystemExit('Hive database role has unexpected administrative privileges')
if not db_exists:
    sql(f'CREATE DATABASE {database} OWNER {user};')
elif sql(f"SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='{database}';") != user:
    raise SystemExit('Existing Hive database has an unexpected owner')
print('Dedicated PostgreSQL role/database ready; password remains on master only.')
PY
    # The checksum-pinned Hive archive already contains PostgreSQL JDBC 42.4.1.
    # Do not introduce a second version into Hive's classpath.
    [[ -s "$HIVE_HOME/lib/postgresql-42.4.1.jar" ]] || die 'Bundled PostgreSQL JDBC driver missing.'
    cp "$SELF/hive-site.xml" "$BASE/conf/hive-site.xml.new"
    set_database_password "$BASE/conf/hive-site.xml.new"
    copy_config "$BASE/conf/hive-site.xml.new" "$BASE/conf/hive-site.xml"
}

initialize_schema() {
    [[ "$ROLE" == metastore ]] || die 'Initialize schema only on master.'
    load_env
    [[ -s "$BASE/private/postgres-password" ]] || die 'Configure PostgreSQL first.'
    count="$(sudo -n -u postgres psql -X -A -t -p 5432 -v ON_ERROR_STOP=1 -d "$DB_NAME" -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';")"
    if [[ "$count" == 0 ]]; then
        "$HIVE_HOME/bin/schematool" -dbType postgres -initSchema
    fi
    "$HIVE_HOME/bin/schematool" -dbType postgres -info
    "$HIVE_HOME/bin/schematool" -dbType postgres -validate
    printf '%s\n' 'Metastore schema validated without dropping existing metadata.'
}

prepare_hdfs() {
    [[ "$ROLE" == client ]] || die 'Prepare exercise HDFS paths from edge.'
    load_env
    timeout 180 hdfs dfsadmin -safemode wait
    for path in "$WAREHOUSE" "$WAREHOUSE/external" /tmp/hive-hw3 "/user/$RUN_USER/hw3" "/user/$RUN_USER/hw3/staging"; do
        if hdfs dfs -test -e "$path"; then
            [[ "$(hdfs dfs -stat %u "$path")" == "$RUN_USER" ]] || die "Unexpected owner on exercise HDFS path: $path"
        fi
    done
    hdfs dfs -mkdir -p "$WAREHOUSE" "$WAREHOUSE/external" /tmp/hive-hw3 "/user/$RUN_USER/hw3/staging"
    hdfs dfs -chmod 700 "$WAREHOUSE" "$WAREHOUSE/external" "/user/$RUN_USER/hw3" "/user/$RUN_USER/hw3/staging"
    hdfs dfs -chmod 1777 /tmp/hive-hw3
    for path in "$WAREHOUSE" "$WAREHOUSE/external" "/user/$RUN_USER/hw3/staging"; do
        [[ "$(hdfs dfs -stat %u "$path")" == "$RUN_USER" ]] || die "Unexpected owner on exercise HDFS path: $path"
    done
}

start() {
    load_env
    name="$(service_name)"
    if running; then printf '%s is already running.\n' "$name"; return; fi
    if [[ "$ROLE" == metastore ]]; then
        [[ -s "$BASE/private/postgres-password" ]] || die 'Configure PostgreSQL and initialize the schema first.'
        nohup "$HIVE_HOME/bin/hive" --service metastore -p "$METASTORE_PORT" > "$BASE/logs/metastore.log" 2>&1 < /dev/null &
        port="$METASTORE_PORT"
    else
        nohup "$HIVE_HOME/bin/hive" --service hiveserver2 > "$BASE/logs/hiveserver2.log" 2>&1 < /dev/null &
        port="$SERVER_PORT"
    fi
    pid="$!"
    printf '%s\n' "$pid" > "$BASE/run/$name.pid"
    python3 - "$PRIVATE_IP" "$port" "$pid" "$BASE/logs/$name.log" <<'PY'
import os
import socket
import sys
import time
host, port, pid, log = sys.argv[1:]
for attempt in range(90):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        raise SystemExit(f'Hive process exited; inspect {log}')
    try:
        with socket.create_connection((host, int(port)), timeout=1):
            print(f'Hive listener ready at {host}:{port}; JDBC functional check is still required.')
            break
    except OSError:
        time.sleep(2)
else:
    raise SystemExit(f'Hive listener did not become ready; inspect {log}')
PY
    running || die 'Hive process did not remain running.'
}

stop() {
    load_env
    name="$(service_name)"
    if ! running; then printf '%s is already stopped.\n' "$name"; return; fi
    pid="$(cat "$BASE/run/$name.pid")"
    kill -TERM "$pid"
    for attempt in {1..60}; do
        if ! kill -0 "$pid" 2>/dev/null || [[ "$(ps -p "$pid" -o stat= | tr -d ' ')" == Z* ]]; then
            rm -- "$BASE/run/$name.pid"
            printf '%s stopped.\n' "$name"
            return
        fi
        sleep 1
    done
    die "Hive did not stop after TERM; inspect $BASE/logs. No forced kill was issued."
}

case "$ACTION" in
    prepare) prepare ;;
    install) install ;;
    configure-postgres) configure_postgres ;;
    initialize-schema) initialize_schema ;;
    prepare-hdfs) prepare_hdfs ;;
    start) start ;;
    stop) stop ;;
    status)
        load_env
        name="$(service_name)"
        if running; then printf '%s is running with PID %s.\n' "$name" "$(cat "$BASE/run/$name.pid")"
        else printf '%s is stopped.\n' "$name"; exit 1; fi
        ;;
    *) die "Unsupported Hive action: $ACTION" ;;
esac
