# ДЗ4. Spark под управлением YARN: HDFS → преобразования → таблица Hive

Решение использует кластер HDFS/YARN из [ДЗ1](../hw1/README.md) и [ДЗ2](../hw2/README.md), а также данные и HiveServer2 из [ДЗ3](../hw3/README.md). Spark читает шесть записей, уже загруженных на HDFS, преобразует типы, фильтрует и агрегирует продажи по дате и покупателю. Результат сохраняется в Parquet с двумя партициями `dt` и становится постоянной внешней таблицей `hw4_lab.daily_sales`, доступной через Beeline.

Нужен весь репозиторий: скрипты используют общий `inventory.json` и функции предыдущих заданий. Фактические результаты запусков приведены в [VALIDATION.md](../VALIDATION.md), машинный отчёт — в [checks/spark-runtime.json](../checks/spark-runtime.json).

## Как выполнены требования

| Пункт задания | Реализация |
|---|---|
| Сессия Spark под управлением YARN | `spark-submit --master yarn --deploy-mode client`; приложение проверяет `SparkContext.master` и настоящий YARN application ID |
| Подключение к существующему HDFS | `HADOOP_CONF_DIR` от работающего Hadoop; полные URI `hdfs://...`, проверка `fs.defaultFS` |
| Чтение ранее загруженных данных | CSV из двух партиций `hw3_lab.sales`, без повторной загрузки исходного набора |
| Несколько преобразований | Проверка значений, `cast`, извлечение и преобразование даты, `filter`, `groupBy`, `count`, `sum`, `avg` |
| Партиционирование при сохранении | `partitionBy("dt")`, каталоги `dt=2026-10-01` и `dt=2026-10-02` |
| Сохранение как таблицы | `saveAsTable("hw4_spark.daily_sales")`; постоянная таблица Hive `hw4_lab.daily_sales` указывает на те же файлы |
| Чтение стандартным клиентом Hive | Beeline подключается к существующему HiveServer2 и читает все пять строк результата |

## Архитектура и версии

```text
edge: spark-submit / driver
          │
          ├── YARN ResourceManager → ApplicationMaster + 2 executors
          │                              │
          │                              ├── чтение CSV в HDFS (ДЗ3)
          │                              └── запись Parquet в HDFS (ДЗ4)
          │
          └── Beeline → HiveServer2 → отдельный Metastore → PostgreSQL
                           │
                           └── таблица hw4_lab.daily_sales → те же Parquet-файлы
```

Apache Spark **3.5.9** поддерживает используемый Java 11. Выбран официальный пакет `spark-3.5.9-bin-hadoop3.tgz`: в нём клиентские библиотеки Hadoop 3.3.4; серверный кластер остаётся на Hadoop 3.4.2. Совместимость клиентов и серверов в пределах основной версии предусмотрена политикой Hadoop; работа конкретного сценария дополнительно проверяется на стенде. Spark устанавливается только на edge: YARN сам доставляет библиотеки приложения в контейнеры. Spark Standalone, новый ResourceManager и отдельный сервер Spark SQL для этого задания не нужны.

Spark 3.5.9 поддерживает версии клиента Hive Metastore до 3.1.3. Используемый в ДЗ3 Hive 4.0.0-alpha-2 не входит в этот диапазон. Поэтому приложение не вызывает `enableHiveSupport()`, не копирует `hive-site.xml` в Spark и не создаёт встроенный Metastore Derby. Формат Parquet служит общим форматом данных, а постоянные метаданные регистрируются через уже работающий HiveServer2.

У таблиц два разных срока жизни:

- `hw4_spark.daily_sales` — таблица каталога Spark `in-memory`, созданная методом `saveAsTable`. Её метаданные существуют до завершения сессии; файлы на HDFS сохраняются.
- `hw4_lab.daily_sales` — постоянная внешняя таблица в существующем Hive Metastore. Её можно читать новым подключением Beeline после остановки Spark.

Для десятичных сумм включён `spark.sql.parquet.writeLegacyFormat=true`; Hive читает десятичные значения в совместимом представлении Parquet. В расчётах используются `DECIMAL`, а не числа с плавающей точкой. PySpark входит в архив Spark, установка через `pip` не требуется. Указание документации Spark 3.5 на Python 3.8+ не означает обещание совместимости со всеми будущими версиями Python: полная поддержка Python 3.12 разрабатывалась для Spark 4.0. На стенде успешно выполнен описанный сценарий DataFrame с Python 3.12.3, без пользовательских Python UDF и RDD. Это проверка конкретного сценария, а не общей совместимости всех возможностей PySpark с Python 3.12.

## Данные и ожидаемый результат

Источник — созданный в ДЗ3 учебный набор:

```text
/user/hive/warehouse/hw3/hw3_lab.db/sales/
  dt=2026-10-01/sales-2026-10-01.csv
  dt=2026-10-02/sales-2026-10-02.csv
```

Каждая CSV-строка содержит `sale_id,customer_id,amount`. Дата берётся из имени партиции. Входные файлы и все шесть исходных записей проверяются до сохранения результата. Приложение рассчитано именно на этот учебный набор, а не на произвольный поток продаж.

| dt | customer_id | order_count | total_amount | average_amount |
|---|---:|---:|---:|---:|
| 2026-10-01 | 101 | 2 | 149.50 | 74.75 |
| 2026-10-01 | 102 | 1 | 250.50 | 250.50 |
| 2026-10-02 | 102 | 1 | 120.00 | 120.00 |
| 2026-10-02 | 103 | 1 | 80.00 | 80.00 |
| 2026-10-02 | 104 | 1 | 300.00 | 300.00 |

Итого: **5 строк результата, 6 заказов, сумма 900.00**. Партиция за 1 октября содержит две строки с суммой 400.00, за 2 октября — три строки с суммой 500.00.

## Автоматический вариант

Команды выполнять **на edge**, из корня репозитория, обычным пользователем Hadoop — в примере `team`. В локальном `inventory.json` должны быть реальные адреса узлов; у `worker1` — `local: true`. Секреты и реальный inventory публиковать не требуется. Пути по умолчанию: Hadoop — `/home/team/teachbase-hadoop`, Hive — `/home/team/teachbase-hive`, Spark — `/home/team/teachbase-spark`.

Перед запуском должны работать HDFS, YARN, Metastore и HiveServer2, а данные ДЗ3 должны быть загружены:

```bash
source /home/team/teachbase-hadoop/course-env.sh
java -version
python3 --version
hdfs dfsadmin -safemode get
hdfs dfsadmin -report
yarn node -list
hdfs dfs -ls /user/hive/warehouse/hw3/hw3_lab.db/sales
python3 hw3/deploy.py --inventory inventory.json --action status --execute
```

Если это новый стенд, сначала пройти ДЗ1–ДЗ3. На существующем стенде повторное форматирование NameNode не нужно.

Предварительный просмотр не запускает приложение и не меняет кластер:

```bash
python3 hw4/deploy.py --inventory inventory.json
python3 hw4/run.py --inventory inventory.json
```

Предпросмотр установки сохраняет `spark-defaults.conf` и `course-env.sh` в локальном `generated-spark`; предпросмотр запуска выводит DDL таблицы и партиций.

Установка и полный запуск:

```bash
python3 hw4/deploy.py --inventory inventory.json --execute
python3 hw4/run.py --inventory inventory.json --execute
```

`deploy.py` скачивает фиксированный выпуск Spark, проверяет SHA-512 и создаёт отдельное окружение Spark на edge. Конфигурации и процессы существующих Hadoop/Hive при этом не заменяются.

`run.py` проверяет исходные данные и принадлежность каталогов/таблицы заданию, выделяет новый каталог запуска, выполняет Spark-приложение, регистрирует результат в Hive и сверяет чтение через Beeline с ожидаемыми значениями. После регистрации он проверяет, что обе партиции указывают именно на данные текущего запуска. Локальная блокировка `.run.lock` запрещает одновременную работу двух экземпляров `run.py` на edge; она не блокирует внешние клиенты Hive. Отчёт текущего запуска записывается в `checks/spark-runtime.json`; можно выбрать другой файл через `--output`:

```bash
python3 hw4/run.py --inventory inventory.json --execute \
  --output /home/team/hw4-repeat-report.json
```

Повторный запуск создаёт новые файлы в новом каталоге `runs/run-<uuid>/data` и переводит две партиции принадлежащей заданию таблицы на эти файлы. Старые файлы автоматически не удаляются, исходная таблица ДЗ3 не переписывается. Поэтому повторный запуск должен дать те же пять строк, а не добавить к ним дубликаты. Сырые отчёты конкретного стенда содержат его адреса; публичный отчёт оформляется отдельно без частных адресов.

## Ручной вариант

Ниже приведена последовательность без Python-скриптов установки и оркестрации. Само приложение [transform.py](transform.py) содержит операции Spark. Все команды выполняются на edge обычным пользователем Hadoop из корня репозитория. Адреса `10.10.0.11` и `10.10.0.12` — примеры; заменить их внутренними адресами edge и HiveServer2 из своего inventory.

### 1. Установить Spark на edge

```bash
set -euo pipefail
export HADOOP_BASE=/home/team/teachbase-hadoop
export SPARK_BASE=/home/team/teachbase-spark
export SPARK_VERSION=3.5.9
export SPARK_HOME="$SPARK_BASE/spark-$SPARK_VERSION-bin-hadoop3"
export EDGE_IP=10.10.0.11
export SERVER_IP=10.10.0.12
source "$HADOOP_BASE/course-env.sh"
if test -d "$SPARK_BASE" && test -n "$(ls -A "$SPARK_BASE")"; then
  test "$(cat "$SPARK_BASE/.hw4-owner")" = hadoop-homework-hw4-v1 || exit 1
fi
mkdir -p "$SPARK_BASE"/{downloads,conf,reports}
printf '%s\n' hadoop-homework-hw4-v1 > "$SPARK_BASE/.hw4-owner"

ARCHIVE="spark-$SPARK_VERSION-bin-hadoop3.tgz"
curl --fail --location --retry 3 --proto '=https' --tlsv1.2 \
  "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/$ARCHIVE" \
  -o "$SPARK_BASE/downloads/$ARCHIVE"
printf '%s  %s\n' \
  66097faf48b12f8673dc13256a44d731c27c86a5831cd8ca930c6fd9dd9bdf31d40593095e959461351d55da1a8e3ad5ed95e0b4b51bea87ca3989ab002ea8ce \
  "$SPARK_BASE/downloads/$ARCHIVE" | sha512sum -c -
tar -xzf "$SPARK_BASE/downloads/$ARCHIVE" -C "$SPARK_BASE"
printf '%s\n' \
  66097faf48b12f8673dc13256a44d731c27c86a5831cd8ca930c6fd9dd9bdf31d40593095e959461351d55da1a8e3ad5ed95e0b4b51bea87ca3989ab002ea8ce \
  > "$SPARK_HOME/.hw4-archive-sha512"

cat > "$SPARK_BASE/course-env.sh" <<EOF
#!/usr/bin/env bash
source "$HADOOP_BASE/course-env.sh" || return
export SPARK_HOME="$SPARK_HOME"
export SPARK_CONF_DIR="$SPARK_BASE/conf"
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PATH="\$SPARK_HOME/bin:\$PATH"
EOF
source "$SPARK_BASE/course-env.sh"
unset SPARK_DIST_CLASSPATH HADOOP_CLASSPATH

cat > "$SPARK_CONF_DIR/spark-defaults.conf" <<EOF
spark.master yarn
spark.submit.deployMode client
spark.driver.memory 512m
spark.driver.host $EDGE_IP
spark.driver.bindAddress $EDGE_IP
spark.executor.instances 2
spark.executor.cores 1
spark.executor.memory 512m
spark.executor.memoryOverhead 384m
spark.yarn.am.memory 512m
spark.yarn.am.memoryOverhead 384m
spark.dynamicAllocation.enabled false
spark.yarn.populateHadoopClasspath false
spark.yarn.am.clientModeTreatDisconnectAsFailed true
spark.yarn.am.clientModeExitOnError true
spark.ui.enabled false
spark.sql.catalogImplementation in-memory
spark.sql.shuffle.partitions 2
spark.sql.parquet.writeLegacyFormat true
spark.pyspark.python /usr/bin/python3
spark.pyspark.driver.python /usr/bin/python3
EOF
spark-submit --version
```

Не смешивать библиотеки Hive с библиотеками Spark. Для варианта `bin-hadoop3` параметр `spark.yarn.populateHadoopClasspath=false` сохраняет библиотеки клиента из самого дистрибутива. Для повторной установки достаточно проверить уже сохранённый архив и установленную версию. В новой оболочке окружение загружается командой `source /home/team/teachbase-spark/course-env.sh`; `HADOOP_CONF_DIR` наследуется из окружения существующего Hadoop.

### 2. Выделить выходной каталог

Получить имя существующего HDFS из его конфигурации и подготовить собственный каталог ДЗ4. Если каталог уже существует, требуется правильный маркер принадлежности:

```bash
FS="$(hdfs getconf -confKey fs.defaultFS)"
FS="${FS%/}"
HW4_ROOT=/user/team/hw4
OWNER=hadoop-homework-hw4-v1
case "$FS" in hdfs://*) ;; *) exit 1 ;; esac
if hdfs dfs -test -e "$HW4_ROOT"; then
  test "$(hdfs dfs -cat "$HW4_ROOT/.hw4-owner")" = "$OWNER" || exit 1
else
  hdfs dfs -mkdir -p "$HW4_ROOT"
  printf '%s\n' "$OWNER" | hdfs dfs -put - "$HW4_ROOT/.hw4-owner"
fi
hdfs dfs -mkdir -p "$HW4_ROOT/runs"
RUN_ID="run-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
RUN_PATH="$FS$HW4_ROOT/runs/$RUN_ID"
INPUT="$FS/user/hive/warehouse/hw3/hw3_lab.db/sales"
OUTPUT="$RUN_PATH/data"
REPORT="$SPARK_BASE/reports/$RUN_ID.json"
```

Каталог `$OUTPUT` должен отсутствовать: приложение использует `errorifexists` и не затирает чужой результат. Вход остаётся в каталоге ДЗ3.

### 3. Выполнить преобразования в YARN

```bash
spark-submit \
  --master yarn --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m --executor-cores 1 --num-executors 2 \
  --conf spark.executor.memoryOverhead=384 \
  --conf spark.yarn.am.memory=512m \
  --conf spark.yarn.am.memoryOverhead=384 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.yarn.populateHadoopClasspath=false \
  --conf spark.driver.host="$EDGE_IP" \
  --conf spark.driver.bindAddress="$EDGE_IP" \
  --conf spark.ui.enabled=false \
  --conf spark.default.parallelism=2 \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.sql.parquet.writeLegacyFormat=true \
  --conf spark.yarn.am.clientModeTreatDisconnectAsFailed=true \
  --conf spark.yarn.am.clientModeExitOnError=true \
  hw4/transform.py --input "$INPUT" --output "$OUTPUT" --report "$REPORT"
```

В `client`-режиме драйвер работает на edge; ApplicationMaster и исполнители запускаются в контейнерах YARN. По 512 МиБ heap и 384 МиБ накладных расходов на контейнер округляются планировщиком стенда до 1024 МиБ. Три контейнера помещаются на трёх NodeManager с лимитом по 1536 МиБ. Динамическое выделение исполнителей выключено, отдельный shuffle service устанавливать не нужно.

`transform.py` создаёт SparkSession, проверяет запуск на YARN, читает два исходных файла, выполняет преобразования и сохраняет таблицу. После записи он перечитывает данные через таблицу Spark и напрямую из Parquet и проверяет все значения. JSON-отчёт создаётся по итогам этого запуска; при обработанной ошибке в нём будет `ok: false`. Если процесс прерван до записи отчёта, отсутствие файла также означает, что успех не подтверждён.

Проверить реальный статус YARN и физические партиции:

```bash
APP_ID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["application_id"])' "$REPORT")"
yarn application -status "$APP_ID"
hdfs dfs -ls "$OUTPUT"
hdfs dfs -ls "$OUTPUT/dt=2026-10-01"
hdfs dfs -ls "$OUTPUT/dt=2026-10-02"
```

Ожидаются `Application-Type: SPARK`, `State: FINISHED`, `Final-State: SUCCEEDED`, два каталога `dt=...` и Parquet-файлы. Успех подтверждается совокупностью проверок: `spark-submit` завершился с кодом 0; отчёт приложения содержит `ok: true` и точные ожидаемые записи; YARN подтвердил `FINISHED / SUCCEEDED`; после регистрации Beeline прочитал все пять строк. В клиентском режиме один лишь `SUCCEEDED` в YARN не доказывает успешное завершение Python-приложения: драйвер может корректно остановить SparkContext при обработке ошибки.

### 4. Зарегистрировать постоянную таблицу Hive

Spark к этому моменту завершил работу. Подключиться обычным клиентом Beeline к существующему HiveServer2:

```bash
source /home/team/teachbase-hive/course-env.sh
JDBC="jdbc:hive2://$SERVER_IP:10000/default"
```

При **первом** создании ДЗ4 выполнить:

```bash
beeline -u "$JDBC" -n team --silent=true --showHeader=false \
  --outputformat=tsv2 --force=false -e "
CREATE DATABASE IF NOT EXISTS hw4_lab
LOCATION '$FS$HW4_ROOT/database';
CREATE EXTERNAL TABLE hw4_lab.daily_sales (
  customer_id INT,
  order_count BIGINT,
  total_amount DECIMAL(20,2),
  average_amount DECIMAL(20,2)
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '$FS$HW4_ROOT/table'
TBLPROPERTIES ('hw4.owner'='$OWNER', 'transactional'='false');
ALTER TABLE hw4_lab.daily_sales ADD
  PARTITION (dt='2026-10-01') LOCATION '$OUTPUT/dt=2026-10-01'
  PARTITION (dt='2026-10-02') LOCATION '$OUTPUT/dt=2026-10-02';
"
```

Таблица внешняя, не транзакционная. Схема и точность десятичных полей совпадают с записанным Spark Parquet. Метаданные двух партиций явно указывают на каталоги текущего запуска, поэтому не требуется сканировать все старые каталоги через `MSCK REPAIR`.

Для **повторного** запуска сначала получить новый `$OUTPUT` успешным повторением шагов 2–3. До изменения метаданных проверить принадлежность существующей таблицы, её схему, формат Parquet и текущие расположения:

```bash
beeline -u "$JDBC" -n team --silent=true --showHeader=false \
  --outputformat=tsv2 --force=false -e "
SHOW TBLPROPERTIES hw4_lab.daily_sales ('hw4.owner');
DESCRIBE FORMATTED hw4_lab.daily_sales;
SHOW PARTITIONS hw4_lab.daily_sales;
DESCRIBE FORMATTED hw4_lab.daily_sales PARTITION (dt='2026-10-01');
DESCRIBE FORMATTED hw4_lab.daily_sales PARTITION (dt='2026-10-02');
"
```

Маркер должен быть `hadoop-homework-hw4-v1`, тип — `EXTERNAL_TABLE`, поля — как в DDL выше, формат — Parquet. Расположения должны относиться к тому же HDFS и собственному `/user/team/hw4`. При несовпадении не выполнять следующий блок. При совпадении перевести партиции на новые данные:

```bash
beeline -u "$JDBC" -n team --silent=true --showHeader=false \
  --outputformat=tsv2 --force=false -e "
ALTER TABLE hw4_lab.daily_sales PARTITION (dt='2026-10-01')
  SET LOCATION '$OUTPUT/dt=2026-10-01';
ALTER TABLE hw4_lab.daily_sales PARTITION (dt='2026-10-02')
  SET LOCATION '$OUTPUT/dt=2026-10-02';
"
```

Изменения двух партиций — отдельные операции Hive, а не одна транзакция. Это последовательный учебный запуск; параллельное обновление/чтение во время переключения не рассматривается. Старые данные остаются на HDFS.

### 5. Проверить чтение через Beeline

```bash
beeline -u "$JDBC" -n team --silent=true --showHeader=false \
  --outputformat=tsv2 --force=false -e "
SHOW PARTITIONS hw4_lab.daily_sales;
SELECT dt, customer_id, order_count, total_amount, average_amount
FROM hw4_lab.daily_sales ORDER BY dt, customer_id;
SELECT COUNT(*) AS result_rows, SUM(order_count) AS source_orders,
       SUM(total_amount) AS total_amount
FROM hw4_lab.daily_sales;
SELECT dt, COUNT(*) AS result_rows, SUM(total_amount) AS total_amount
FROM hw4_lab.daily_sales GROUP BY dt ORDER BY dt;
"
```

Сверить пять строк с таблицей ожидаемого результата выше; общий итог — `5`, `6`, `900.00`. После повторного запуска ожидаются те же значения. Запросы выполняются отдельным клиентом Hive после остановки Spark и проверяют доступность постоянной таблицы.

При диагностике можно получить журнал конкретного приложения и состояние HDFS/YARN:

```bash
yarn logs -applicationId "$APP_ID"
hdfs fsck /user/team/hw4 -files -blocks -locations
hdfs dfsadmin -report
yarn node -list
```

## Файлы решения

- [config.py](config.py) — проверка inventory и настройки Spark.
- [deploy.py](deploy.py) — установка фиксированного архива и отдельного окружения на edge.
- [transform.py](transform.py) — сессия Spark, чтение, преобразования, `saveAsTable`, проверка результата.
- [run.py](run.py) — запуск в YARN, регистрация постоянной таблицы Hive, проверка Beeline и отчёт.

## Первичные источники

- [Spark 3.5.9: поддерживаемые Java и состав выпуска](https://spark.apache.org/docs/3.5.9/).
- [Spark 3.5.9: запуск в YARN и клиентский режим](https://spark.apache.org/docs/3.5.9/running-on-yarn.html).
- [Spark 3.5.9: поддерживаемые версии Hive Metastore](https://spark.apache.org/docs/3.5.9/sql-data-sources-hive-tables.html#interacting-with-different-versions-of-hive-metastore).
- [DataFrameWriter.partitionBy](https://spark.apache.org/docs/3.5.9/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy.html) и [saveAsTable](https://spark.apache.org/docs/3.5.9/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.saveAsTable.html).
- [Spark: Parquet и формат десятичных значений](https://spark.apache.org/docs/3.5.9/sql-data-sources-parquet.html).
- [Hive: Parquet](https://hive.apache.org/docs/latest/user/parquet/) и [внешние таблицы/партиции](https://hive.apache.org/docs/latest/language/languagemanual-ddl/).
- [Spark 3.5.9 pom.xml: версии библиотек Hadoop](https://github.com/apache/spark/blob/v3.5.9/pom.xml) и [политика совместимости Hadoop](https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/Compatibility.html).
- [Apache Spark: поддержка Python 3.12](https://issues.apache.org/jira/browse/SPARK-44120).
- [Официальная SHA-512 архива](https://archive.apache.org/dist/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz.sha512).
