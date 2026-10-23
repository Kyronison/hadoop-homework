# ДЗ1. Развёртывание HDFS

Кластер состоит из NameNode на master, трёх DataNode на workers и SecondaryNameNode на worker1. Коэффициент репликации — 3.

## Подготовка

Скопировать репозиторий на edge, создать `inventory.json` по [примеру](../inventory.example.json) и заполнить параметры ВМ. Все команды установки ниже выполняются из корня репозитория на edge.

На каждой ВМ установить зависимости:

```bash
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk-headless python3 curl tar coreutils
java -version
javac -version
python3 --version
```

`java` и `javac` должны относиться к Java 11. Hadoop запускается от обычного пользователя, не от root. Для системных пакетов нужен sudo; автоматическое исправление `/etc/hosts` использует `sudo -n`, поэтому требуется возможность вызвать sudo без интерактивного ввода пароля (на учебном стенде — NOPASSWD).

Проверить SSH с edge к master, worker2 и worker3 с ключом из inventory. Ключи серверов должны быть проверены и сохранены в `known_hosts`: автоматизация использует `StrictHostKeyChecking=yes` и не принимает неизвестные ключи автоматически.

Собственное имя каждой ВМ должно разрешаться в её внутренний IP. На учебном стенде короткое имя и FQDN также были записаны на `127.0.1.1`, поэтому Hadoop слушал loopback. Для исправления предусмотрена команда:

```bash
python3 hw1/prepare_hosts.py --inventory inventory.json --execute
```

Она требует, чтобы `ssh_host` совпадал с реальным `hostname` ВМ, переносит её короткое имя и связанные FQDN на `private_ip`, сохраняет резервную копию `/etc/hosts.teachbase-backup-*` и проверяет результат. Если собственного имени нет в `/etc/hosts`, используется имя из inventory. Прочие записи сохраняются.

Эта команда исправляет только собственное имя каждой ВМ. Дополнительно с **каждой** из четырёх машин должны разрешаться короткие и канонические имена всех узлов в соответствующие `private_ip`. Обратное разрешение каждого IP должно давать имя, которое разрешается обратно в тот же IP. Настроить это через DNS либо одинаковые записи всех узлов в `/etc/hosts`; не оставлять имена Hadoop-узлов также на loopback. Затем с edge выполнить проверку:

```bash
python3 hw1/preflight.py --inventory inventory.json --execute
```

Она проверяет прямое, каноническое и обратное разрешение всех четырёх имён на каждой ВМ, не изменяет настройки и не требует sudo. Без `--execute` выводит описание проверки и не подключается к ВМ. Если проверка не прошла, сначала исправить DNS/`/etc/hosts`.

Между четырьмя ВМ должны быть доступны порты HDFS:

| Узел | Порты |
|---|---|
| master | 9000 — RPC, 9870 — NameNode UI |
| worker1 | 9868 — SecondaryNameNode UI |
| каждый worker | 9866 — данные, 9867 — IPC, 9864 — DataNode UI |

Эти порты используются во внутренней сети. Доступ с внешнего компьютера описан в [ДЗ2](../hw2/README.md).

## Конфигурация и первый запуск

Предварительно посмотреть сгенерированные файлы можно без подключения к ВМ:

```bash
python3 common/cluster.py render --inventory inventory.json --output generated
```

Для каждой роли создаются `core-site.xml`, `hdfs-site.xml`, `workers`, `node.env` и скрипты. В `core-site.xml` задаётся `fs.defaultFS=hdfs://<master_ip>:9000`. В `hdfs-site.xml` — адреса сервисов, каталоги данных, репликация 3 и период checkpoint 300 секунд.

На новом пустом стенде:

```bash
python3 hw1/deploy.py --inventory inventory.json --execute --initialize-empty-namenode
```

Скрипт загружает Hadoop 3.4.2 из архива Apache, проверяет SHA-512, устанавливает конфигурацию и окружение, проверяет пустоту каталогов DataNode, форматирует пустой NameNode, запускает сервисы и выполняет проверку HDFS. `JAVA_HOME` определяется через установленный `javac`.

При повторном запуске флаг инициализации не нужен:

```bash
python3 hw1/deploy.py --inventory inventory.json --execute
```

Непустой каталог метаданных не форматируется. Если при работающих сервисах изменена конфигурация, скрипт останавливается с ошибкой: сначала нужно корректно остановить сервисы, затем применить настройки.

## Проверка

На master, если `install_root=/home/team/teachbase-hadoop`:

```bash
source /home/team/teachbase-hadoop/course-env.sh
hdfs dfsadmin -report
bash /home/team/teachbase-hadoop/artifacts/node.sh verify-hdfs
```

Проверка требует ровно три живых DataNode, отсутствие dead/stale/decommissioning-узлов и неисправных томов, а также положительный `LastCheckpointTime` у SecondaryNameNode. Ожидание готовности до 360 секунд учитывает первый checkpoint с периодом 300 секунд. Затем записывает небольшой файл, ждёт три реплики, считывает его обратно и запускает `hdfs fsck`. Успешный результат — `Status: HEALTHY` и `PASS: hdfs`.

На worker1 должны работать DataNode и SecondaryNameNode. Дату последней контрольной точки можно проверить в SecondaryNameNode UI. Логи сохраняются в `<install_root>/logs`, отчёты проверки — в `<install_root>/evidence` на master. Результаты фактического прогона находятся в [отчёте](../VALIDATION.md).

## Установка вручную без deploy.py

Это альтернативный порядок для **нового пустого стенда**, после подготовки Java, Python, сети и имён выше. Если кластер уже установлен автоматизацией, этот раздел пропустить и пользоваться запуском/остановкой ниже. Команды выполняются от обычного пользователя в Bash. Значения IP и `BASE` заменить на параметры своего inventory; `ROLE` и `PRIVATE_IP` отличаются на каждой ВМ.

### Архив и окружение на каждой ВМ

```bash
set -euo pipefail
umask 077
export BASE="$HOME/teachbase-hadoop"
export VERSION=3.4.2
export MASTER_IP=10.10.0.10
export SECONDARY_IP=10.10.0.11
export WORKER2_IP=10.10.0.12
export WORKER3_IP=10.10.0.13
export ROLE=master                 # master, worker1, worker2 или worker3
export PRIVATE_IP=10.10.0.10       # IP именно этой ВМ
test "$(id -u)" -ne 0
if test -e "$BASE" && test -n "$(ls -A -- "$BASE")"; then
    echo "Каталог установки не пуст; ручная установка остановлена." >&2
    exit 1
fi
mkdir -p "$BASE"/{downloads,run,logs,tmp,data/name,data/data,data/checkpoint}
cd "$BASE/downloads"
archive="hadoop-$VERSION.tar.gz"
url="https://archive.apache.org/dist/hadoop/common/hadoop-$VERSION/$archive"
curl --fail --location --retry 3 --proto '=https' --tlsv1.2 "$url" -o "$archive"
curl --fail --location --retry 3 --proto '=https' --tlsv1.2 "$url.sha512" -o "$archive.sha512"
expected="$(python3 -c 'import pathlib,re,sys; m=re.findall(r"(?i)(?<![0-9a-f])[0-9a-f]{128}(?![0-9a-f])", pathlib.Path(sys.argv[1]).read_text()); assert len(m)==1; print(m[0].lower())' "$archive.sha512")"
printf '%s  %s\n' "$expected" "$archive" | sha512sum --check
tar -xzf "$archive" -C "$BASE" --no-same-owner
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
"$JAVA_HOME/bin/java" -version 2>&1 | grep -Eq 'version "11\.'
export HADOOP_HOME="$BASE/hadoop-$VERSION"
cat > "$BASE/course-env.sh" <<EOF
export JAVA_HOME="$JAVA_HOME"
export HADOOP_HOME="$HADOOP_HOME"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_LOG_DIR="$BASE/logs"
export HADOOP_PID_DIR="$BASE/run"
export YARN_LOG_DIR="$BASE/logs"
export YARN_PID_DIR="$BASE/run"
export HADOOP_HEAPSIZE_MAX=512
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export HADOOP_YARN_HOME="$HADOOP_HOME"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:\$PATH"
EOF
source "$BASE/course-env.sh"
printf 'source %q\n' "$BASE/course-env.sh" > "$HADOOP_CONF_DIR/hadoop-env.sh"
hadoop version
```

### Конфигурация HDFS на каждой ВМ

`fs.defaultFS` указывает на RPC NameNode; репликация равна трём. Метаданные NameNode, данные DataNode и checkpoint разделены. IP DataNode задаётся отдельно для каждой машины; на master эти настройки не запускают DataNode.

```bash
cat > "$HADOOP_CONF_DIR/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://$MASTER_IP:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$BASE/tmp</value></property>
</configuration>
EOF
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>3</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$BASE/data/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$BASE/data/data</value></property>
  <property><name>dfs.namenode.checkpoint.dir</name><value>file://$BASE/data/checkpoint</value></property>
  <property><name>dfs.namenode.http-address</name><value>$MASTER_IP:9870</value></property>
  <property><name>dfs.namenode.secondary.http-address</name><value>$SECONDARY_IP:9868</value></property>
  <property><name>dfs.datanode.hostname</name><value>$PRIVATE_IP</value></property>
  <property><name>dfs.datanode.address</name><value>$PRIVATE_IP:9866</value></property>
  <property><name>dfs.datanode.ipc.address</name><value>$PRIVATE_IP:9867</value></property>
  <property><name>dfs.datanode.http.address</name><value>$PRIVATE_IP:9864</value></property>
  <property><name>dfs.namenode.checkpoint.period</name><value>300</value></property>
</configuration>
EOF
printf '%s\n' "$SECONDARY_IP" "$WORKER2_IP" "$WORKER3_IP" > "$HADOOP_CONF_DIR/workers"
```

### Однократное форматирование и запуск

Сначала на **каждом worker** проверить, что каталог данных пуст. На новом стенде следующая команда должна завершиться с кодом 0 и без вывода; при наличии любого файла остановиться и выяснить, кому принадлежат данные:

```bash
test -z "$(find "$BASE/data/data" -mindepth 1 -print -quit)"
```

Только после проверки всех трёх workers на **master** выполнить защиту пустого каталога метаданных и однократное форматирование. Флаг `-force` не применять. При повторном старте кластер никогда не форматируется.

```bash
test "$ROLE" = master
test -d "$BASE/data/name"
if test -n "$(find "$BASE/data/name" -mindepth 1 -print -quit)"; then
    echo "В NameNode уже есть метаданные; форматирование запрещено." >&2
    exit 1
fi
hdfs namenode -format -nonInteractive
hdfs --daemon start namenode
```

На каждом worker выполнить `hdfs --daemon start datanode`, затем дополнительно на worker1 — `hdfs --daemon start secondarynamenode`. На master проверить:

```bash
timeout 360 hdfs dfsadmin -safemode wait
hdfs dfsadmin -report
sample="$(mktemp "$BASE/tmp/manual-check.XXXXXXXX")"
printf 'hadoop yarn\nhadoop hdfs\n' > "$sample"
dest="/user/$(id -un)/manual-check-$(date -u +%Y%m%dT%H%M%SZ)-$$"
hdfs dfs -mkdir -p "$dest"
hdfs dfs -put "$sample" "$dest/sample.txt"
timeout 180 hdfs dfs -setrep -w 3 "$dest/sample.txt"
hdfs dfs -cat "$dest/sample.txt" | cmp - "$sample"
hdfs fsck "$dest" -files -blocks -locations
rm -- "$sample"
curl --fail --noproxy '*' "http://$SECONDARY_IP:9868/jmx?qry=Hadoop:service=SecondaryNameNode,name=SecondaryNameNodeInfo"
```

В отчёте должны быть три Live DataNode, ноль Dead DataNode и failed volumes; `fsck` должен показать `HEALTHY` и три реплики. `cmp` при совпадении ничего не печатает. У SecondaryNameNode `LastCheckpointTime` должен быть положительным; после первого запуска подождать до 300 секунд и повторить последний запрос. Статус и логи доступны в NameNode/SecondaryNameNode UI и `$BASE/logs`. Эта ручная установка не использует `deploy.py`, `node.sh` и генератор конфигураций. После неё продолжить ручной установкой YARN из [ДЗ2](../hw2/README.md): служебные маркеры автоматизации в ручном варианте не создаются.

При запуске учтена особенность Hadoop 3.4.2: `--daemon status` может печатать `is stopped.` и возвращать код 0. Поэтому скрипт проверяет текст о работающем процессе и его PID, а не только код возврата.

## Ручной запуск и остановка

После установки на каждой ВМ загрузить `<install_root>/course-env.sh`. Запускать сервисы в следующем порядке:

```bash
# master
hdfs --daemon start namenode

# каждый worker
hdfs --daemon start datanode

# дополнительно worker1
hdfs --daemon start secondarynamenode
```

Для остановки сначала остановить YARN, если он запущен. Затем на workers выполнить `hdfs --daemon stop datanode`, на worker1 — `hdfs --daemon stop secondarynamenode`, последним на master — `hdfs --daemon stop namenode`. Каталоги данных сохраняются. Автозапуск после перезагрузки ОС не настроен.

Использованные материалы: [Apache Cluster Setup](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/ClusterSetup.html), [обработка статуса демонов в Hadoop 3.4.2](https://github.com/apache/hadoop/blob/rel/release-3.4.2/hadoop-common-project/hadoop-common/src/main/bin/hadoop-functions.sh).
