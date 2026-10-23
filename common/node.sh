#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
SELF="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SELF/node.env"
ACTION="${1:?Specify an action}"
die() { printf '%s\n' "$*" >&2; exit 1; }
[[ "$(id -u)" != 0 ]] || die 'Run Hadoop as an ordinary user, not root.'
[[ "$BASE" == /*/teachbase-hadoop ]] || die 'Invalid dedicated installation directory.'
[[ "$VERSION" == 3.4.2 ]] || die 'Unexpected Hadoop version.'
case "$NODE" in master|worker1|worker2|worker3) ;; *) die 'Unknown node role';; esac

load_env() {
    [[ -f "$BASE/.course-owned" && -f "$BASE/course-env.sh" ]] || die 'Run install-hdfs first.'
    source "$BASE/course-env.sh"
}

install_hdfs() {
    for cmd in java javac curl tar sha512sum python3; do command -v "$cmd" >/dev/null || die "Missing: $cmd"; done
    java_home="$(dirname -- "$(dirname -- "$(readlink -f -- "$(command -v javac)")")")"
    "$java_home/bin/java" -version 2>&1 | grep -Eq 'version "11\.' || die 'Java 11 JDK is required.'
    python3 - "$PRIVATE_IP" <<'PY'
import socket, sys
s = socket.socket()
s.bind((sys.argv[1], 0))
s.close()
PY
    if [[ -d "$BASE" && ! -f "$BASE/.course-owned" && -n "$(ls -A -- "$BASE")" ]]; then
        die 'Nonempty installation directory without ownership marker: refusing to overwrite.'
    fi
    mkdir -p -- "$BASE"
    printf '%s\n' 'teachbase-hadoop-v1' > "$BASE/.course-owned"
    mkdir -p -- "$BASE"/{downloads,run,logs,tmp,data/name,data/data,data/checkpoint,data/yarn/local,data/yarn/logs,artifacts,evidence}
    home="$BASE/hadoop-$VERSION"
    if [[ ! -x "$home/bin/hadoop" ]]; then
        archive="hadoop-$VERSION.tar.gz"
        url="https://archive.apache.org/dist/hadoop/common/hadoop-$VERSION/$archive"
        curl --fail --silent --show-error --location --retry 3 --proto '=https' --tlsv1.2 "$url" -o "$BASE/downloads/$archive.part"
        curl --fail --silent --show-error --location --retry 3 --proto '=https' --tlsv1.2 "$url.sha512" -o "$BASE/downloads/$archive.sha512"
        expected="$(python3 - "$BASE/downloads/$archive.sha512" <<'PY'
import re, sys
from pathlib import Path
matches = re.findall(r'(?i)(?<![0-9a-f])[0-9a-f]{128}(?![0-9a-f])', Path(sys.argv[1]).read_text())
if len(matches) != 1:
    raise SystemExit('Expected one SHA-512 digest')
print(matches[0].lower())
PY
)"
        printf '%s  %s\n' "$expected" "$BASE/downloads/$archive.part" | sha512sum --check --status || die 'SHA-512 mismatch.'
        mv -- "$BASE/downloads/$archive.part" "$BASE/downloads/$archive"
        tar -xzf "$BASE/downloads/$archive" -C "$BASE" --no-same-owner
    fi
    {
        printf 'export JAVA_HOME=%q\n' "$java_home"
        printf 'export HADOOP_HOME=%q\n' "$home"
        printf 'export HADOOP_CONF_DIR=%q\n' "$home/etc/hadoop"
        printf 'export HADOOP_LOG_DIR=%q\n' "$BASE/logs"
        printf 'export HADOOP_PID_DIR=%q\n' "$BASE/run"
        printf 'export YARN_LOG_DIR=%q\n' "$BASE/logs"
        printf 'export YARN_PID_DIR=%q\n' "$BASE/run"
        printf 'export HADOOP_HEAPSIZE_MAX=512\n'
        printf 'export HADOOP_COMMON_HOME="$HADOOP_HOME"\nexport HADOOP_HDFS_HOME="$HADOOP_HOME"\n'
        printf 'export HADOOP_YARN_HOME="$HADOOP_HOME"\nexport HADOOP_MAPRED_HOME="$HADOOP_HOME"\n'
        printf 'export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"\n'
    } > "$BASE/course-env.sh.new"
    copy_config "$BASE/course-env.sh.new" "$BASE/course-env.sh"
    load_env
    for f in core-site.xml hdfs-site.xml workers; do copy_config "$SELF/$f" "$HADOOP_CONF_DIR/$f"; done
    printf 'source %q\n' "$BASE/course-env.sh" > "$BASE/hadoop-env.sh.new"
    copy_config "$BASE/hadoop-env.sh.new" "$HADOOP_CONF_DIR/hadoop-env.sh"
    save_artifacts
    "$HADOOP_HOME/bin/hadoop" version
}

copy_config() {
    if [[ -f "$2" ]] && cmp -s -- "$1" "$2"; then return; fi
    if [[ -f "$2" ]]; then
        # Restrict process inspection to this installation, not other users' clusters.
        running="$(ps -u "$(id -u)" -o args= | grep -F -- "$BASE/hadoop-$VERSION/" | grep '[j]ava' || true)"
        if [[ "$ACTION" == install-yarn ]]; then
            running="$(grep -E 'ResourceManager|NodeManager|JobHistoryServer|WebAppProxyServer' <<< "$running" || true)"
        fi
        if [[ -n "$running" ]]; then
            die "Configuration changed while this cluster is running: stop its daemons first ($2)."
        fi
        cp -p -- "$2" "$2.backup.$(date +%s)"
    fi
    cp -- "$1" "$2"
}

save_artifacts() {
    if [[ "$SELF" != "$BASE/artifacts" ]]; then
        cp -- "$SELF/node.sh" "$SELF/node.env" "$SELF/check.py" "$SELF/"*.xml "$SELF/workers" "$BASE/artifacts/"
    fi
}

install_yarn() {
    load_env
    # Leave capacity for DataNode, NodeManager, OS, and SecondaryNameNode on worker1.
    if [[ "$NODE" != master ]]; then
        memory_mb="$(awk '/MemTotal:/ {print int($2/1024)}' /proc/meminfo)"
        (( memory_mb >= NM_MEMORY_MB + 1800 )) || die 'Insufficient RAM for configured YARN capacity plus daemons.'
    fi
    [[ -d "$BASE/data/yarn/local" ]] || die 'Incomplete HDFS installation.'
    # Identical HDFS configuration is required; do not silently switch clusters.
    cmp -s "$SELF/core-site.xml" "$HADOOP_CONF_DIR/core-site.xml" || die 'HDFS master changed in inventory.'
    for f in yarn-site.xml mapred-site.xml; do copy_config "$SELF/$f" "$HADOOP_CONF_DIR/$f"; done
    save_artifacts
}

start_daemon() {
    local tool="$1" name="$2"
    # Hadoop 3.4.2 can return zero even when its status text says stopped.
    # Require the positive status and a live PID before skipping startup.
    daemon_running() {
        local status
        status="$("$HADOOP_HOME/bin/$tool" --daemon status "$name" 2>&1)" || true
        printf '%s\n' "$status"
        [[ "$status" =~ is\ running\ as\ process\ ([0-9]+)\. ]] && kill -0 "${BASH_REMATCH[1]}" 2>/dev/null
    }
    if daemon_running; then return; fi
    "$HADOOP_HOME/bin/$tool" --daemon start "$name"
    sleep 2
    daemon_running || die "Failed to start $name: inspect $BASE/logs"
}

case "$ACTION" in
    install-hdfs) install_hdfs ;;
    install-yarn) install_yarn ;;
    assert-empty-datanode)
        load_env
        [[ -z "$(ls -A -- "$BASE/data/data")" ]] || die 'DataNode already contains data: refusing fresh-cluster initialization.'
        ;;
    initialize-empty-namenode)
        load_env
        [[ "$NODE" == master ]] || die 'Only master may initialize NameNode.'
        if [[ -f "$BASE/data/name/current/VERSION" ]]; then
            printf '%s\n' 'NameNode already initialized; keeping metadata.'
        else
            [[ -z "$(ls -A -- "$BASE/data/name")" ]] || die 'NameNode directory is not empty; refusing formatting.'
            "$HADOOP_HOME/bin/hdfs" namenode -format -nonInteractive
        fi
        ;;
    start-hdfs)
        load_env
        if [[ "$NODE" == master ]]; then
            [[ -f "$BASE/data/name/current/VERSION" ]] || die 'Initialize the empty NameNode explicitly on first deployment.'
            start_daemon hdfs namenode
        else
            start_daemon hdfs datanode
            if [[ "$NODE" == worker1 ]]; then start_daemon hdfs secondarynamenode; fi
        fi
        ;;
    start-yarn)
        load_env
        if [[ "$NODE" == master ]]; then
            timeout 180 "$HADOOP_HOME/bin/hdfs" dfsadmin -safemode wait
            hdfs dfs -mkdir -p /app-logs /mr-history/tmp /mr-history/done "/user/$(id -un)"
            hdfs dfs -chmod 1777 /app-logs /mr-history/tmp
            hdfs dfs -chmod 750 /mr-history/done
            start_daemon yarn resourcemanager
            start_daemon mapred historyserver
            start_daemon yarn proxyserver
        else
            start_daemon yarn nodemanager
        fi
        ;;
    verify-hdfs|verify-yarn)
        load_env
        [[ "$NODE" == master ]] || die 'Run the cluster check on master.'
        kind="${ACTION#verify-}"
        stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
        exec > >(tee "$BASE/evidence/$kind-$stamp.log") 2>&1
        python3 "$SELF/check.py" "$SELF/core-site.xml" "$SELF/hdfs-site.xml" "$SELF/workers" "$kind"
        timeout 180 hdfs dfsadmin -safemode wait
        input="$(mktemp "$BASE/tmp/sample.XXXXXXXX")"
        trap 'rm -f -- "$input"' EXIT
        printf 'hadoop yarn\nhadoop hdfs\n' > "$input"
        dest="/user/$(id -un)/checks/$stamp"
        hdfs dfs -mkdir -p "$dest/input"
        hdfs dfs -put "$input" "$dest/input/sample.txt"
        timeout 180 hdfs dfs -setrep -w 3 "$dest/input/sample.txt"
        hdfs dfs -cat "$dest/input/sample.txt" | cmp - "$input"
        fsck="$(hdfs fsck "$dest/input" -files -blocks -locations)"
        printf '%s\n' "$fsck"
        grep -q 'Status: HEALTHY' <<< "$fsck" || die 'HDFS health check failed.'
        if [[ "$kind" == yarn ]]; then
            timeout 300 hadoop jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-$VERSION.jar" wordcount "$dest/input" "$dest/output"
            actual="$(hdfs dfs -cat "$dest/output/part-r-*")"
            expected=$'hadoop\t2\nhdfs\t1\nyarn\t1'
            [[ "$actual" == "$expected" ]] || die "Unexpected WordCount result: $actual"
            hdfs dfs -test -e "$dest/output/_SUCCESS"
            printf '%s\n' "$actual"
            yarn application -list -appStates FINISHED
        fi
        printf 'PASS: %s; retained test data: %s\n' "$kind" "$dest"
        ;;
    *) die "Unsupported action: $ACTION" ;;
esac
