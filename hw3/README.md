# ДЗ3. Hive с выделенным Metastore и партиционированной таблицей

Решение добавляет Hive к кластеру из [ДЗ1](../hw1/README.md) и [ДЗ2](../hw2/README.md). Клиенты подключаются по JDBC к отдельному HiveServer2. HiveServer2 использует отдельный процесс Metastore, а метаданные хранятся в PostgreSQL. Встроенная база Derby не используется. Скрипт загрузки создаёт таблицу `hw3_lab.sales` и загружает данные в две партиции по дате; отдельная проверка открывает два JDBC-соединения одновременно.

Используется рекомендованный в задании Apache Hive **4.0.0-alpha-2**. Нужен весь репозиторий: скрипты `hw3` читают тот же `inventory.json` и используют SSH-функции из `common`. Результаты выполненных проверок отделены от инструкции в [VALIDATION.md](../VALIDATION.md).

Стек этого стенда — Ubuntu 24.04, Java 11, Hadoop 3.4.2 и PostgreSQL 16. Для alpha2 Apache указывал Hadoop 3.3.1, а [README релиза](https://github.com/apache/hive/blob/rel/release-4.0.0-alpha-2/README.md) — Java 8. Совместимость здесь подтверждается только описанным в отчёте учебным прогоном; особенности доступа Metastore и библиотек MapReduce учтены ниже.

## Размещение сервисов

| Узел из inventory | Что работает | Адрес в примере |
|---|---|---|
| `master` | PostgreSQL 16, Hive Metastore | PostgreSQL: `127.0.0.1:5432`; Metastore: `10.10.0.10:9083` |
| `worker2` | HiveServer2 | `10.10.0.12:10000` |
| `worker1` / edge | Beeline и скрипты загрузки/проверки | Клиент подключается к `worker2` |
| `worker3` | Существующие DataNode и NodeManager | Дополнительных сервисов Hive нет |

Адреса выше — пример из `inventory.example.json`. Для своего стенда нужно заполнить реальные адреса в локальном `inventory.json`. Пароль PostgreSQL в inventory не записывается: он создаётся только на master.

Все процессы Hive запускаются обычным пользователем Hadoop, в примере `team`. Каталог Hive — `/home/team/teachbase-hive`; каталог работающего Hadoop — `/home/team/teachbase-hadoop`. Hive использует конфигурацию этого Hadoop; только Metastore получает собственную копию `core-site.xml` с одним дополнительным разрешённым адресом HiveServer2. Установка не форматирует HDFS и не заменяет исходную конфигурацию Hadoop.

Это учебный стенд в доверенной внутренней сети: `hive.server2.authentication=NONE` и `hive.server2.enable.doAs=false`. JDBC-имя клиента не проверяется паролем, а запросы исполняются от пользователя процесса HiveServer2. Порты 9083 и 10000 слушают только внутренние адреса ВМ, PostgreSQL — только localhost. Веб-интерфейс HiveServer2 выключен. Открывать эти сервисы в интернет не нужно.

## Автоматическая установка и загрузка

Команды выполняются из корня репозитория **на edge**, от того же пользователя, который развернул Hadoop. В inventory у `worker1` должно быть `local: true`, а SSH-ключ должен давать доступ к master и worker2. Python использует стандартную библиотеку; на Ubuntu нужны Python 3, OpenSSH, `curl`, `tar`, `sha256sum`, `sudo`, Java и работающий Hadoop.

Сначала проверить существующий кластер:

```bash
source /home/team/teachbase-hadoop/course-env.sh
java -version
hadoop version
hdfs dfsadmin -safemode get
hdfs dfsadmin -report
yarn node -list
```

Ожидаются выключенный safemode, три живых DataNode и три NodeManager в состоянии `RUNNING`. Если HDFS или YARN не запущен, использовать порядок запуска из ДЗ1/ДЗ2. Команду форматирования NameNode повторно выполнять нельзя.

Без `--execute` команды только готовят локальный предварительный результат:

```bash
python3 hw3/deploy.py --inventory inventory.json
```

Конфигурации появляются в `generated-hive/master`, `generated-hive/worker1` и `generated-hive/worker2`. В шаблоне master вместо пароля находится маркер `__GENERATED_ON_MASTER__`.

Установка, загрузка и проверка:

```bash
python3 hw3/deploy.py --inventory inventory.json --execute
python3 hw3/load_data.py --inventory inventory.json --execute
python3 hw3/verify.py --inventory inventory.json --execute
```

Установка проверяет SHA-256 архива Hive, устанавливает серверные компоненты, создаёт отдельную базу Metastore, инициализирует её схему, готовит каталоги HDFS и запускает Metastore перед HiveServer2. JDBC-драйвер PostgreSQL 42.4.1 уже входит в выбранный дистрибутив Hive. На edge остаётся клиент Hive. Скрипт не переносит пароль PostgreSQL на edge или worker2.

Управление уже установленными сервисами:

```bash
python3 hw3/deploy.py --inventory inventory.json --action status --execute
python3 hw3/deploy.py --inventory inventory.json --action stop --execute
python3 hw3/deploy.py --inventory inventory.json --action start --execute
```

`stop` останавливает сначала HiveServer2, затем Metastore. HDFS, YARN и данные таблиц остаются на месте. Перед повторной установкой с изменёнными настройками нужно остановить Hive. Для повторной загрузки учебного набора достаточно запустить `load_data.py`: он заменяет содержимое двух учебных партиций, поэтому строки не должны удваиваться.

## Ручная установка

Этот вариант описывает те же компоненты без запуска Python-скрипта развёртывания. Команды Bash выполнять обычным пользователем Hadoop. На существующем стенде с уже настроенным Hive сначала остановить его, сохранить конфигурацию и базу Metastore. Ни `DROP DATABASE`, ни форматирование HDFS для установки не нужны.

### 1. Переменные и дистрибутив

На **master, worker2 и edge** установить общие переменные; заменить адреса своими:

```bash
export HADOOP_BASE=/home/team/teachbase-hadoop
export HIVE_BASE=/home/team/teachbase-hive
export HIVE_VERSION=4.0.0-alpha-2
export HIVE_HOME="$HIVE_BASE/apache-hive-$HIVE_VERSION-bin"
export HIVE_CONF_DIR="$HIVE_BASE/conf"
export MASTER_IP=10.10.0.10
export SERVER_IP=10.10.0.12
source "$HADOOP_BASE/course-env.sh"
mkdir -p "$HIVE_BASE"/{downloads,conf,logs,run,tmp/resources,artifacts}
chmod 700 "$HIVE_BASE/conf"

ARCHIVE="apache-hive-$HIVE_VERSION-bin.tar.gz"
curl --fail --location --retry 3 --proto '=https' --tlsv1.2 \
  "https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/$ARCHIVE" \
  -o "$HIVE_BASE/downloads/$ARCHIVE"
printf '%s  %s\n' \
  f94c773e7ae3328d780138d9259c584e94d8f2e34715776cdfaaa78bcf3f25e1 \
  "$HIVE_BASE/downloads/$ARCHIVE" | sha256sum -c -
tar -xzf "$HIVE_BASE/downloads/$ARCHIVE" -C "$HIVE_BASE"
```

При повторном выполнении скачивание и распаковку можно пропустить, если уже установлен именно этот архив и его контрольная сумма подтверждена. Имена `HIVE_HOME` и `HIVE_CONF_DIR` должны оставаться такими же во всех следующих командах.

### 2. Окружение Hive

На этих же трёх ВМ связать конфигурацию Hive с работающим Hadoop:

```bash
for file in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml; do
  test -s "$HADOOP_CONF_DIR/$file" || exit 1
  ln -s "$HADOOP_CONF_DIR/$file" "$HIVE_CONF_DIR/$file"
done
```

На повторном запуске существующие правильные ссылки не создавать заново; собственную копию `core-site.xml` Metastore, описанную ниже, ссылкой не заменять.

Только на **master**, пока Metastore остановлен, заменить ссылку на `core-site.xml` отдельной копией для процесса Metastore. В этой копии сохраняются все свойства исходного Hadoop и добавляется `hadoop.proxyuser.team.hosts` со значением внутреннего адреса HiveServer2. Имя `team` берётся из текущего Linux-пользователя. На edge и worker2 `core-site.xml` остаётся ссылкой на исходный файл Hadoop.

```bash
python3 - "$HADOOP_CONF_DIR/core-site.xml" "$HIVE_CONF_DIR/core-site.xml" \
  "$(id -un)" "$SERVER_IP" <<'PY'
from pathlib import Path
from datetime import datetime, timezone
import os
import shutil
import sys
import xml.etree.ElementTree as ET

source, target = map(Path, sys.argv[1:3])
user, host = sys.argv[3:5]
if target.is_symlink() and os.readlink(target) != str(source):
    raise SystemExit('Unexpected core-site.xml symlink; stop and inspect it')
temporary = target.with_name(target.name + '.new')
if temporary.is_symlink():
    raise SystemExit('Unexpected temporary symlink; stop and inspect it')
tree = ET.parse(source)
key = f'hadoop.proxyuser.{user}.hosts'
for prop in list(tree.getroot().findall('property')):
    if prop.findtext('name') == key:
        tree.getroot().remove(prop)
prop = ET.SubElement(tree.getroot(), 'property')
ET.SubElement(prop, 'name').text = key
ET.SubElement(prop, 'value').text = host
ET.indent(tree)
tree.write(temporary, encoding='utf-8', xml_declaration=True)
temporary.chmod(0o600)
if target.is_symlink():
    target.unlink()  # Only the verified link, never the Hadoop source file.
elif target.exists():
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    shutil.copy2(target, target.with_name(target.name + '.backup.' + stamp))
temporary.replace(target)
PY
```

Это разрешение нужно для обращений HiveServer2 к API уведомлений Metastore с конкретного внутреннего адреса. Проверка доступа к этому API остаётся включённой; разрешения `groups`/`users` и адрес `*` не добавляются. Параметр расположен именно в собственной копии `core-site.xml` Metastore, поскольку проверка Hadoop читает свою конфигурацию; запись только в `hive-site.xml` для этой связки недостаточна. При изменении исходного `core-site.xml` Hadoop обновить копию этим же шагом при остановленном Metastore.

Указать heap в зависимости от узла: на master `384`, на worker2 `512`, на edge `256` MiB. Затем сохранить окружение на каждой ВМ:

```bash
export HIVE_HEAP_MB=384  # master; для worker2 — 512, для edge — 256
cat > "$HIVE_BASE/course-env.sh" <<EOF
source "$HADOOP_BASE/course-env.sh"
export HIVE_HOME="$HIVE_HOME"
export HIVE_CONF_DIR="$HIVE_CONF_DIR"
export HADOOP_HEAPSIZE=$HIVE_HEAP_MB
export HADOOP_HEAPSIZE_MAX=$HIVE_HEAP_MB
export HIVE_LOG_DIR="$HIVE_BASE/logs"
export HIVE_PID_DIR="$HIVE_BASE/run"
export HIVE_METASTORE_PORT=9083
export HIVE_SERVER2_THRIFT_PORT=10000
export PATH="\$JAVA_HOME/bin:\$HIVE_HOME/bin:\$HADOOP_HOME/bin:\$PATH"
EOF
chmod 600 "$HIVE_BASE/course-env.sh"
source "$HIVE_BASE/course-env.sh"
```

### 3. PostgreSQL и отдельная база Metastore

Только на **master**:

```bash
sudo apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-16
sudo systemctl start postgresql@16-main
sudo -u postgres psql -X -p 5432 -d postgres -c 'SHOW server_version;'
sudo -u postgres psql -X -p 5432 -d postgres -c 'SHOW listen_addresses;'
```

Нужен PostgreSQL 16, слушающий `localhost` (допустимы также `127.0.0.1` и `::1`). При другом результате сначала разобраться с существующей установкой: не менять чужую базу и не открывать PostgreSQL во внутреннюю или внешнюю сеть. Стандартная конфигурация Ubuntu использует локальный `peer` для администратора `postgres` и пароль для подключения по TCP.

Для **новой** установки убедиться, что `hw3_hive` ещё не занят:

```bash
sudo -u postgres psql -X -A -t -d postgres -c \
  "SELECT rolname FROM pg_roles WHERE rolname='hw3_hive';"
sudo -u postgres psql -X -A -t -d postgres -c \
  "SELECT datname FROM pg_database WHERE datname='hw3_hive';"
```

Если оба запроса ничего не вернули, создать пароль, роль без административных прав и базу. Пароль не выводится на экран и не передаётся аргументом процесса:

```bash
umask 077
mkdir -p "$HIVE_BASE/private"
chmod 700 "$HIVE_BASE/private"
test ! -e "$HIVE_BASE/private/postgres-password" || exit 1
python3 -c 'import secrets; print(secrets.token_hex(32))' \
  > "$HIVE_BASE/private/postgres-password"
chmod 600 "$HIVE_BASE/private/postgres-password"
DB_PASSWORD="$(cat "$HIVE_BASE/private/postgres-password")"
printf "CREATE ROLE hw3_hive LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '%s';\n" \
  "$DB_PASSWORD" | sudo -u postgres psql -X -v ON_ERROR_STOP=1 -d postgres
unset DB_PASSWORD
sudo -u postgres createdb --owner=hw3_hive hw3_hive
test -s "$HIVE_HOME/lib/postgresql-42.4.1.jar"
```

На повторной установке сохранять прежние роль, базу и файл пароля. Если роль/база уже существуют, но их происхождение или пароль неизвестны, этот шаг не продолжать. Для существующей собственной базы проверить владельца `hw3_hive` и отсутствие у роли прав superuser/createdb/createrole. Используется JDBC-драйвер PostgreSQL из архива Hive: добавлять вторую версию JAR в `lib` не нужно. Пароль PostgreSQL клиентам Beeline не требуется.

### 4. Конфигурация Hive

На **master, worker2 и edge** записать общую часть `hive-site.xml`. Выполнять блок вместе с подходящим продолжением для своего узла и завершающей строкой ниже. Если файл уже существует, предварительно сохранить его копию с правами `600`.

```bash
umask 077
cat > "$HIVE_CONF_DIR/hive-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>hive.execution.engine</name><value>mr</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse/hw3</value></property>
  <property><name>hive.metastore.warehouse.external.dir</name><value>/user/hive/warehouse/hw3/external</value></property>
  <property><name>hive.exec.scratchdir</name><value>/tmp/hive-hw3</value></property>
  <property><name>hive.exec.local.scratchdir</name><value>$HIVE_BASE/tmp</value></property>
  <property><name>hive.downloaded.resources.dir</name><value>$HIVE_BASE/tmp/resources</value></property>
  <property><name>hive.metastore.schema.verification</name><value>true</value></property>
  <property><name>datanucleus.schema.autoCreateAll</name><value>false</value></property>
  <property><name>hive.support.concurrency</name><value>false</value></property>
  <property><name>hive.txn.manager</name><value>org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager</value></property>
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
  <property><name>hive.server2.authentication</name><value>NONE</value></property>
EOF
```

`hive.support.concurrency=false` здесь отключает механизм блокировок/транзакций для учебной non-ACID таблицы. Это не отключает одновременные JDBC-сессии HiveServer2. Параллельные записи в одну партицию в этом примере не выполняются; два клиента проверяются чтением после загрузки.

Только на **master** дописать параметры PostgreSQL и Metastore:

```bash
DB_PASSWORD="$(cat "$HIVE_BASE/private/postgres-password")"
cat >> "$HIVE_CONF_DIR/hive-site.xml" <<EOF
  <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:postgresql://127.0.0.1:5432/hw3_hive</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name><value>org.postgresql.Driver</value></property>
  <property><name>javax.jdo.option.ConnectionUserName</name><value>hw3_hive</value></property>
  <property><name>javax.jdo.option.ConnectionPassword</name><value>$DB_PASSWORD</value></property>
  <property><name>hive.metastore.thrift.bind.host</name><value>$MASTER_IP</value></property>
  <property><name>hive.metastore.port</name><value>9083</value></property>
  <property><name>metastore.try.direct.sql.ddl</name><value>false</value></property>
  <property><name>hive.metastore.server.min.threads</name><value>5</value></property>
  <property><name>hive.metastore.server.max.threads</name><value>30</value></property>
EOF
unset DB_PASSWORD
```

На **worker2 и edge** вместо параметров базы дописать адрес отдельного Metastore:

```bash
cat >> "$HIVE_CONF_DIR/hive-site.xml" <<EOF
  <property><name>hive.metastore.uris</name><value>thrift://$MASTER_IP:9083</value></property>
EOF
```

Дополнительно только на **worker2** дописать настройки HiveServer2:

```bash
cat >> "$HIVE_CONF_DIR/hive-site.xml" <<EOF
  <property><name>hive.server2.thrift.bind.host</name><value>$SERVER_IP</value></property>
  <property><name>hive.server2.thrift.port</name><value>10000</value></property>
  <property><name>hive.server2.transport.mode</name><value>binary</value></property>
  <property><name>hive.server2.webui.port</name><value>0</value></property>
  <property><name>hive.server2.async.exec.threads</name><value>4</value></property>
  <property><name>hive.server2.thrift.min.worker.threads</name><value>2</value></property>
  <property><name>hive.server2.thrift.max.worker.threads</name><value>10</value></property>
  <property><name>hive.server2.logging.operation.log.location</name><value>$HIVE_BASE/logs/operations</value></property>
  <property><name>hive.server2.tez.initialize.default.sessions</name><value>false</value></property>
  <property><name>hive.aux.jars.path</name><value>file://$HIVE_HOME/lib/commons-collections-3.2.2.jar</value></property>
EOF
```

`hive.aux.jars.path` передаёт библиотеку Commons Collections из дистрибутива Hive в его MapReduce-задачи. Она нужна Hive alpha2, а Hadoop 3.4.2 больше не предоставляет её в своём наборе библиотек. Без этой настройки на стенде задача завершалась ошибкой отсутствия `org.apache.commons.collections.CollectionUtils`; глобальные библиотеки Hadoop при этом не меняются.

На каждой из **трёх ВМ** закрыть XML и ограничить доступ к файлу:

```bash
printf '</configuration>\n' >> "$HIVE_CONF_DIR/hive-site.xml"
chmod 600 "$HIVE_CONF_DIR/hive-site.xml"
```

В конфигурации master пароль остаётся только на этой ВМ. Его нельзя копировать в репозиторий, клиентские конфигурации или отчёт проверки.

### 5. Схема Metastore и каталоги HDFS

На **master** проверить, пуста ли новая база:

```bash
source "$HIVE_BASE/course-env.sh"
sudo -u postgres psql -X -A -t -d hw3_hive -c \
  "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';"
```

Только если результат `0`, один раз инициализировать схему:

```bash
"$HIVE_HOME/bin/schematool" -dbType postgres -initSchema
```

Затем, в том числе при повторном запуске, проверить версию и целостность схемы:

```bash
"$HIVE_HOME/bin/schematool" -dbType postgres -info
"$HIVE_HOME/bin/schematool" -dbType postgres -validate
```

Если схема уже есть, `-initSchema` повторно не запускать. Неполную или несовместимую схему нужно исследовать; удалять метаданные для обхода ошибки нельзя.

На **edge** создать каталоги только для этого задания:

```bash
source "$HIVE_BASE/course-env.sh"
timeout 180 hdfs dfsadmin -safemode wait
for path in /user/hive/warehouse/hw3 /user/hive/warehouse/hw3/external \
  /tmp/hive-hw3 "/user/$(id -un)/hw3" "/user/$(id -un)/hw3/staging"; do
  if hdfs dfs -test -e "$path"; then
    test "$(hdfs dfs -stat %u "$path")" = "$(id -un)" || exit 1
  fi
done
hdfs dfs -mkdir -p /user/hive/warehouse/hw3/external \
  /tmp/hive-hw3 "/user/$(id -un)/hw3/staging"
hdfs dfs -chmod 700 /user/hive/warehouse/hw3 \
  /user/hive/warehouse/hw3/external \
  "/user/$(id -un)/hw3" "/user/$(id -un)/hw3/staging"
hdfs dfs -chmod 1777 /tmp/hive-hw3
hdfs dfs -ls -d /user/hive/warehouse/hw3 \
  /user/hive/warehouse/hw3/external "/user/$(id -un)/hw3/staging"
```

Владельцем каталогов должен быть тот же пользователь, от которого работает HiveServer2. Это нужно, поскольку `doAs=false`: файловые операции выполняет сервисный пользователь.

### 6. Запуск и подключение

Сначала на **master** запустить Metastore:

```bash
source "$HIVE_BASE/course-env.sh"
nohup "$HIVE_HOME/bin/hive" --service metastore -p 9083 \
  > "$HIVE_BASE/logs/metastore.log" 2>&1 < /dev/null &
printf '%s\n' "$!" > "$HIVE_BASE/run/metastore.pid"
```

Проверить процесс и внутренний listener:

```bash
ps -p "$(cat "$HIVE_BASE/run/metastore.pid")" -o pid,user,args
ss -ltn | grep ':9083'
tail -n 40 "$HIVE_BASE/logs/metastore.log"
```

После готовности Metastore на **worker2** запустить HiveServer2:

```bash
source "$HIVE_BASE/course-env.sh"
nohup "$HIVE_HOME/bin/hive" --service hiveserver2 \
  > "$HIVE_BASE/logs/hiveserver2.log" 2>&1 < /dev/null &
printf '%s\n' "$!" > "$HIVE_BASE/run/hiveserver2.pid"
ps -p "$(cat "$HIVE_BASE/run/hiveserver2.pid")" -o pid,user,args
ss -ltn | grep ':10000'
```

Появление порта ещё не подтверждает выполнение SQL. На **edge** проверить JDBC:

```bash
source "$HIVE_BASE/course-env.sh"
"$HIVE_HOME/bin/beeline" -u "jdbc:hive2://$SERVER_IP:10000/default" \
  -n "$(id -un)" --showHeader=false --outputformat=tsv2 \
  -e 'SELECT 1; SHOW DATABASES;'
```

Если соединение не установилось, проверить сначала журнал HiveServer2, затем доступность Metastore и PostgreSQL. При `NONE` используется стандартное JDBC-подключение; добавлять `auth=noSasl` к URL не нужно.

Для клиента на своём компьютере можно открыть SSH-туннель через edge. В примере заменить `EDGE_PUBLIC_IP` внешним адресом edge, а внутренний адрес — адресом своего worker2:

```bash
ssh -N -L 127.0.0.1:10000:10.10.0.12:10000 team@EDGE_PUBLIC_IP
```

Тогда JDBC URL на этом компьютере будет `jdbc:hive2://127.0.0.1:10000/default`. Сам SQL-клиент и JDBC-драйвер должны быть установлены на компьютере. Для запуска клиентских команд непосредственно на edge туннель не нужен.

### 7. Данные и партиционированная таблица

В `hw3/data` находятся два маленьких искусственных набора продаж. Это учебные данные, а не выгрузка реальных операций:

| Файл | Партиция `dt` | Строк | Сумма `amount` |
|---|---|---:|---:|
| `sales-2026-10-01.csv` | `2026-10-01` | 3 | 400.00 |
| `sales-2026-10-02.csv` | `2026-10-02` | 3 | 500.00 |
| Всего | 2 партиции | 6 | 900.00 |

Столбцы в CSV: `sale_id`, `customer_id`, `amount`; строка заголовков отсутствует. Значение `dt` задаётся при загрузке в партицию, поэтому четвёртого столбца в CSV нет. Таблица внешняя (`EXTERNAL`), без ACID, формат `TEXTFILE`, разделитель — запятая.

Ручная загрузка ниже предназначена для новой таблицы этого примера или для её повторной загрузки после проверки всех метаданных. `IF NOT EXISTS` не исправляет структуру существующей таблицы. Перед `OVERWRITE` обязательно проверить не только таблицу, но и существующие партиции: у каждой партиции может быть собственный `LOCATION`.

На **edge**, из корня репозитория, сначала создать базу через HiveServer2, затем проверить или создать выделенный каталог таблицы. Для выбранной alpha-версии важен порядок: при первом `CREATE DATABASE` каталог базы должен создать сам Hive; заранее создавать `hw3_lab.db` или его подкаталоги нельзя. Каталоги warehouse, подготовленные выше, этому не мешают. Если каталог таблицы уже существует без правильного маркера, остановиться и изучить его содержимое; добавлять маркер поверх чужих данных нельзя:

```bash
source /home/team/teachbase-hive/course-env.sh
export SERVER_IP=10.10.0.12
"$HIVE_HOME/bin/beeline" -u "jdbc:hive2://$SERVER_IP:10000/default" \
  -n "$(id -un)" --force=false -e 'CREATE DATABASE IF NOT EXISTS hw3_lab;' || exit 1
TABLE_PATH=/user/hive/warehouse/hw3/hw3_lab.db/sales
if hdfs dfs -test -e "$TABLE_PATH"; then
  test "$(hdfs dfs -cat "$TABLE_PATH/.hw3-owner")" = hadoop-homework-hw3-v1 || exit 1
else
  hdfs dfs -mkdir -p "$TABLE_PATH"
  printf 'hadoop-homework-hw3-v1\n' | hdfs dfs -put - "$TABLE_PATH/.hw3-owner"
fi
```

Создать только метаданные таблицы и выполнить SQL через удалённый HiveServer2. Этот блок ещё не загружает данные:

```bash
cat > /home/team/teachbase-hive/artifacts/create-sales.hql <<'EOF'
CREATE DATABASE IF NOT EXISTS hw3_lab;

CREATE EXTERNAL TABLE IF NOT EXISTS hw3_lab.sales (
  sale_id INT,
  customer_id INT,
  amount DECIMAL(10,2)
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/hw3/hw3_lab.db/sales'
TBLPROPERTIES ('transactional'='false', 'hw3.owner'='hadoop-homework-hw3-v1');
EOF

"$HIVE_HOME/bin/beeline" -u "jdbc:hive2://$SERVER_IP:10000/default" \
  -n "$(id -un)" --force=false \
  -f /home/team/teachbase-hive/artifacts/create-sales.hql
```

**До загрузки** получить конфигурацию HDFS и фактические метаданные:

```bash
hdfs getconf -confKey fs.defaultFS
"$HIVE_HOME/bin/beeline" -u "jdbc:hive2://$SERVER_IP:10000/default" \
  -n "$(id -un)" --showHeader=false --outputformat=tsv2 --force=false -e "
SHOW TBLPROPERTIES hw3_lab.sales;
DESCRIBE FORMATTED hw3_lab.sales;
SHOW PARTITIONS hw3_lab.sales;
"
```

Продолжать только при выполнении всех условий:

- `hw3.owner` равен `hadoop-homework-hw3-v1`; `transactional` отсутствует либо равен `false`. Для внешней non-ACID таблицы эта версия Hive может не сохранять явно заданное `false`; значение `true` недопустимо.
- Тип таблицы — `EXTERNAL_TABLE`; обычные столбцы — `sale_id int`, `customer_id int`, `amount decimal(10,2)`; единственный столбец партиционирования — `dt string`.
- `InputFormat` — `org.apache.hadoop.mapred.TextInputFormat`; `SerDe Library` — `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`; `field.delim` — запятая.
- `Location` таблицы — полный URI из значения `fs.defaultFS` и пути `$TABLE_PATH`. Для inventory из примера это `hdfs://10.10.0.10:9000/user/hive/warehouse/hw3/hw3_lab.db/sales`. Совпадение только окончания пути недостаточно: другая файловая система или другой NameNode недопустимы.
- Партиций пока нет либо присутствуют только `dt=2026-10-01` и `dt=2026-10-02`.

Для **каждой партиции, уже существующей по `SHOW PARTITIONS`**, до загрузки выполнить соответствующий запрос. Для отсутствующей партиции этот запрос пропустить:

```sql
DESCRIBE FORMATTED hw3_lab.sales PARTITION (dt='2026-10-01');
DESCRIBE FORMATTED hw3_lab.sales PARTITION (dt='2026-10-02');
```

Запросы можно выполнить в Beeline, открытом командой подключения из предыдущего шага. У существующих партиций проверить тот же текстовый формат и SerDe, а также точное расположение: `fs.defaultFS` + `$TABLE_PATH/dt=2026-10-01` или `fs.defaultFS` + `$TABLE_PATH/dt=2026-10-02`. Если любой параметр отличается, не выполнять `LOAD DATA` и не исправлять путь вслепую: сначала выяснить происхождение таблицы и файлов.

После успешной проверки загрузить исходники в новый staging-каталог и выполнить отдельный файл загрузки:

```bash
STAGING="/user/$(id -un)/hw3/staging/manual-$(date -u +%Y%m%dT%H%M%S)-$$"
hdfs dfs -mkdir -p "$STAGING"
hdfs dfs -chmod 700 "$STAGING"
hdfs dfs -put hw3/data/sales-2026-10-01.csv "$STAGING/"
hdfs dfs -put hw3/data/sales-2026-10-02.csv "$STAGING/"
cat > /home/team/teachbase-hive/artifacts/load-sales.hql <<EOF
LOAD DATA INPATH '$STAGING/sales-2026-10-01.csv'
OVERWRITE INTO TABLE hw3_lab.sales PARTITION (dt='2026-10-01');

LOAD DATA INPATH '$STAGING/sales-2026-10-02.csv'
OVERWRITE INTO TABLE hw3_lab.sales PARTITION (dt='2026-10-02');
EOF

"$HIVE_HOME/bin/beeline" -u "jdbc:hive2://$SERVER_IP:10000/default" \
  -n "$(id -un)" --force=false \
  -f /home/team/teachbase-hive/artifacts/load-sales.hql
```

Здесь намеренно используется `LOAD DATA INPATH` **без `LOCAL`**: исходники лежат в HDFS. `LOCAL` при подключении к HiveServer2 означал бы локальный диск **worker2**, а не диск edge. Указанные HDFS-файлы перемещаются в каталог партиции. Поэтому перед повторным запуском загрузки нужно снова выполнить `hdfs dfs -put` в новый staging-каталог. `OVERWRITE` заменяет содержимое только соответствующей учебной партиции; повторная загрузка не добавляет копии строк. Оба `LOAD DATA` выполняются последовательно; это не общая транзакция двух партиций. При сбое между ними можно повторить загрузку всего учебного набора после тех же проверок.

Маркер `.hw3-owner` в HDFS и свойство `hw3.owner` таблицы обозначают каталог этого примера. Не добавлять их к чужим данным ради обхода проверки. Автоматическая загрузка проверяет эти признаки, схему и расположение перед `OVERWRITE`; после описанной ручной подготовки её можно использовать для повторной загрузки.

### 8. Проверка загрузки

На **edge**:

```bash
"$HIVE_HOME/bin/beeline" -u "jdbc:hive2://$SERVER_IP:10000/default" \
  -n "$(id -un)" --showHeader=false --outputformat=tsv2 --force=false -e "
SHOW PARTITIONS hw3_lab.sales;
SELECT COUNT(*), SUM(amount) FROM hw3_lab.sales;
SELECT dt, COUNT(*), SUM(amount)
FROM hw3_lab.sales GROUP BY dt ORDER BY dt;
SELECT sale_id, customer_id, amount
FROM hw3_lab.sales WHERE dt='2026-10-01' ORDER BY sale_id;
"
```

Ожидаемые значения, с точностью до отображения десятичных нулей:

```text
dt=2026-10-01
dt=2026-10-02

6    900.00

2026-10-01    3    400.00
2026-10-02    3    500.00

1    101    100.00
2    102    250.50
3    101     49.50
```

Проверить также размещение файлов:

```bash
hdfs dfs -ls -R /user/hive/warehouse/hw3/hw3_lab.db/sales
```

Под таблицей должны быть каталоги `dt=2026-10-01` и `dt=2026-10-02`. Выполнить загрузку повторно и снова проверить общее число строк: должно остаться `6`.

### 9. Два одновременных клиента

Открыть **два терминала на edge**. В первом выполнить:

```bash
source /home/team/teachbase-hive/course-env.sh
"$HIVE_HOME/bin/beeline" -u 'jdbc:hive2://10.10.0.12:10000/default' -n client_a
```

Оставить первое соединение открытым. Во втором терминале:

```bash
source /home/team/teachbase-hive/course-env.sh
"$HIVE_HOME/bin/beeline" -u 'jdbc:hive2://10.10.0.12:10000/default' -n client_b
```

Когда оба клиента подключены, выполнить в каждом:

```sql
SELECT dt, COUNT(*), SUM(amount)
FROM hw3_lab.sales GROUP BY dt ORDER BY dt;
```

Оба клиента должны вернуть две строки: `2026-10-01, 3, 400.00` и `2026-10-02, 3, 500.00`. Это две отдельные JDBC-сессии одного серверного Hive. Имена `client_a`/`client_b` служат для различения сессий; при `authentication=NONE` это не отдельные защищённые учётные записи. Закрыть клиенты командой Beeline `!quit`.

Автоматический вариант проверки из корня репозитория на edge:

```bash
python3 hw3/verify.py --inventory inventory.json --execute \
  --output checks/hive-runtime.json
```

Проверка компилирует `ConcurrentClients.java`: два потока Java открывают по независимому JDBC-соединению и переходят к запросам только после подключения обоих клиентов. Оба соединения остаются открыты до окончания чтения обоими клиентами. Проверка записывает число одновременно открытых соединений и SQL-результаты в JSON. Метки `client_1` и `client_2` в отчёте — имена проверяющих клиентов, а не серверные идентификаторы сессий. Предварительный запуск без `--execute` не подключается к ВМ и не подтверждает работоспособность стенда.

## Остановка после ручного запуска

Сначала на **worker2**:

```bash
HIVE_BASE=/home/team/teachbase-hive
HIVE_PID="$(cat "$HIVE_BASE/run/hiveserver2.pid")"
ps -p "$HIVE_PID" -o pid,user,args
```

Убедиться, что PID принадлежит пользователю `team` и процессу HiveServer2 из каталога `teachbase-hive`, затем:

```bash
kill -TERM "$HIVE_PID"
```

После завершения процесса удалить только его PID-файл. Затем аналогично на **master**:

```bash
HIVE_BASE=/home/team/teachbase-hive
HIVE_PID="$(cat "$HIVE_BASE/run/metastore.pid")"
ps -p "$HIVE_PID" -o pid,user,args
# После проверки, что это нужный процесс Metastore:
kill -TERM "$HIVE_PID"
```

Остановка Hive не удаляет PostgreSQL, метаданные или HDFS-файлы. PostgreSQL обычно можно оставить запущенным. Если на PostgreSQL этой ВМ нет других задач и нужна полная остановка учебного сервиса, на master выполнить `sudo systemctl stop postgresql@16-main`; при следующем запуске сначала вернуть PostgreSQL, затем Metastore, затем HiveServer2. Не запускать второй экземпляр сервиса, пока предыдущий процесс ещё работает.

## Что приложено и что проверять

- `deploy.py`, `config.py`, `node.sh` — установка и конфигурация отдельных сервисов Hive.
- `load_data.py` — создание учебной таблицы и загрузка двух партиций.
- `verify.py` — проверка SQL-результатов и двух одновременно открытых JDBC-клиентов.
- Эта инструкция — ручной порядок установки, загрузки и проверки.

Успешный запуск процесса и открытый порт сами по себе не являются полной проверкой. Нужны успешный JDBC-запрос, обе партиции, шесть строк после повторной загрузки и два одновременно открытых соединения, каждое из которых успешно читает таблицу. Подтверждённые результаты конкретного стенда записываются отдельно в [VALIDATION.md](../VALIDATION.md).

## Источники

- [Apache Hive 4.0.0-alpha-2: официальный архив](https://archive.apache.org/dist/hive/hive-4.0.0-alpha-2/).
- [HiveServer2: конфигурация, многоклиентский доступ, режим аутентификации и doAs](https://hive.apache.org/docs/latest/admin/setting-up-hiveserver2/).
- [Schema Tool: инициализация и проверка схемы Metastore](https://hive.apache.org/docs/latest/admin/hive-schema-tool/).
- [LOAD DATA: загрузка файлов и партиций](https://hive.apache.org/docs/latest/language/languagemanual-dml/).

Условие ДЗ3 требует серверный Hive с возможностью одновременного использования несколькими клиентами и загрузку данных в партиционированную таблицу. PostgreSQL, размещение HiveServer2 на worker2 и учебный набор продаж — выбранная реализация этих требований.
