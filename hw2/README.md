# ДЗ2. YARN и веб-интерфейсы

Перед установкой должен работать кластер из [ДЗ1](../hw1/README.md): три DataNode, safemode выключен. Используются тот же `inventory.json`, пользователь и каталог Hadoop. ResourceManager, JobHistory Server и отдельный Web Application Proxy размещаются на master. На каждом worker работает NodeManager. ApplicationMaster запускается YARN на время выполнения отдельной задачи.

Полная последовательность для нового стенда: подготовка и установка HDFS по ДЗ1 → установка YARN по этой странице → проверка задачи → открытие всех веб-интерфейсов через SSH на localhost. Нужен весь репозиторий с папками `common`, `hw1`, `hw2`; папка `hw2` использует общую конфигурацию и уже установленный HDFS.

Для учебного задания достаточно доступа через localhost. Основной вариант ниже использует SSH-туннель с внешнего компьютера. Публичный IP для HTTP, открытые входящие веб-порты и публичный TLS-сертификат не являются обязательными условиями этого варианта.

Команды Bash ниже выполняются на Ubuntu обычным пользователем Hadoop. `INSTALL_ROOT` должен совпадать с `install_root` в inventory. В примерах используется `/home/team/teachbase-hadoop`; при другом расположении заменить значение. Python запускается командой `python3` на Ubuntu; для локальных команд на Windows можно использовать `py -3`.

## Автоматическая установка YARN

Из корня репозитория на edge:

```bash
python3 hw2/deploy.py --inventory inventory.json --execute
```

Скрипт устанавливает `yarn-site.xml` и `mapred-site.xml`, создаёт каталоги истории и агрегированных логов в HDFS, запускает серверные процессы и проверяет кластер на WordCount. При изменении конфигурации работающего YARN сначала остановить его процессы по инструкции в конце страницы. HDFS при установке YARN не форматируется.

Основные настройки: по 1536 MiB и 2 vcores на worker; по 512 MiB на ApplicationMaster, map и reduce с Java heap 384 MiB. Проверка физической памяти включена. Проверка виртуального адресного пространства отключена: Java 11 резервирует больше виртуальной памяти, чем фактически использует. На workers дополнительно требуется не менее 1800 MiB для ОС и постоянных сервисов.

## Ручная установка YARN

Этот вариант использует уже установленный в ДЗ1 Hadoop и не требует запуска Python-скриптов развёртывания.

На **каждой из четырёх ВМ** задать переменные. `MASTER_IP` одинаков на всех ВМ; `PRIVATE_IP` — внутренний адрес той ВМ, где выполняется команда. Приведённые адреса соответствуют примеру inventory, их нужно заменить адресами своего стенда.

```bash
export INSTALL_ROOT=/home/team/teachbase-hadoop
source "$INSTALL_ROOT/course-env.sh"
export MASTER_IP=10.10.0.10
export PRIVATE_IP=10.10.0.11
export YARN_MEMORY_MB=1536
export YARN_VCORES=2
mkdir -p "$INSTALL_ROOT/data/yarn/local" "$INSTALL_ROOT/data/yarn/logs"
```

Если YARN уже работает, остановить его по инструкции ниже. Перед заменой файлов сохранить резервные копии:

```bash
BACKUP_DIR="$INSTALL_ROOT/yarn-config-backup-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -m 700 "$BACKUP_DIR"
for file in yarn-site.xml mapred-site.xml; do
  if [ -f "$HADOOP_CONF_DIR/$file" ]; then
    cp -p "$HADOOP_CONF_DIR/$file" "$BACKUP_DIR/$file"
  fi
done
```

На каждой ВМ записать `yarn-site.xml`. Переменные внутри heredoc раскрываются оболочкой, поэтому каждый NodeManager получит свой адрес.

```bash
cat > "$HADOOP_CONF_DIR/yarn-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>$MASTER_IP</value></property>
  <property><name>yarn.resourcemanager.bind-host</name><value>$MASTER_IP</value></property>
  <property><name>yarn.resourcemanager.webapp.address</name><value>$MASTER_IP:8088</value></property>
  <property><name>yarn.nodemanager.hostname</name><value>$PRIVATE_IP</value></property>
  <property><name>yarn.nodemanager.bind-host</name><value>$PRIVATE_IP</value></property>
  <property><name>yarn.nodemanager.address</name><value>$PRIVATE_IP:45454</value></property>
  <property><name>yarn.nodemanager.localizer.address</name><value>$PRIVATE_IP:8040</value></property>
  <property><name>yarn.nodemanager.webapp.address</name><value>$PRIVATE_IP:8042</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name><value>org.apache.hadoop.mapred.ShuffleHandler</value></property>
  <property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>$YARN_MEMORY_MB</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>$YARN_VCORES</value></property>
  <property><name>yarn.nodemanager.pmem-check-enabled</name><value>true</value></property>
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>512</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>$YARN_MEMORY_MB</value></property>
  <property><name>yarn.scheduler.maximum-allocation-vcores</name><value>$YARN_VCORES</value></property>
  <property><name>yarn.nodemanager.local-dirs</name><value>$INSTALL_ROOT/data/yarn/local</value></property>
  <property><name>yarn.nodemanager.log-dirs</name><value>$INSTALL_ROOT/data/yarn/logs</value></property>
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
  <property><name>yarn.nodemanager.remote-app-log-dir</name><value>/app-logs</value></property>
  <property><name>yarn.log.server.url</name><value>http://$MASTER_IP:19888/jobhistory/logs</value></property>
  <property><name>yarn.web-proxy.address</name><value>$MASTER_IP:9099</value></property>
</configuration>
EOF
```

На каждой ВМ записать `mapred-site.xml`:

```bash
cat > "$HADOOP_CONF_DIR/mapred-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>mapreduce.application.classpath</name><value>$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value></property>
  <property><name>mapreduce.map.memory.mb</name><value>512</value></property>
  <property><name>mapreduce.reduce.memory.mb</name><value>512</value></property>
  <property><name>mapreduce.map.java.opts</name><value>-Xmx384m</value></property>
  <property><name>mapreduce.reduce.java.opts</name><value>-Xmx384m</value></property>
  <property><name>yarn.app.mapreduce.am.resource.mb</name><value>512</value></property>
  <property><name>yarn.app.mapreduce.am.command-opts</name><value>-Xmx384m</value></property>
  <property><name>mapreduce.jobhistory.address</name><value>$MASTER_IP:10020</value></property>
  <property><name>mapreduce.jobhistory.webapp.address</name><value>$MASTER_IP:19888</value></property>
  <property><name>mapreduce.jobhistory.intermediate-done-dir</name><value>/mr-history/tmp</value></property>
  <property><name>mapreduce.jobhistory.done-dir</name><value>/mr-history/done</value></property>
</configuration>
EOF
```

Внутри сети кластера разрешить обмен между этими четырьмя ВМ. Используются ResourceManager 8030–8033, NodeManager 45454/8040, shuffle 13562, JobHistory RPC 10020, порты HDFS и динамические порты ApplicationMaster. Эти порты не нужно открывать в Интернет.

На **master** создать каталоги HDFS и запустить службы:

```bash
timeout 180 hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /app-logs /mr-history/tmp /mr-history/done "/user/$(id -un)"
hdfs dfs -chmod 1777 /app-logs /mr-history/tmp
hdfs dfs -chmod 750 /mr-history/done
yarn --daemon start resourcemanager
mapred --daemon start historyserver
yarn --daemon start proxyserver
```

На **worker1, worker2 и worker3**:

```bash
yarn --daemon start nodemanager
```

В каждой новой SSH-сессии предварительно загрузить `course-env.sh`. На master проверить `jps -l`: ожидаются ResourceManager, JobHistoryServer и WebAppProxyServer. На workers — NodeManager. Наличие Java-процесса само по себе не подтверждает работоспособность: ниже проверяются регистрация узлов и выполнение задачи.

## Проверка YARN на задаче

На master:

```bash
export INSTALL_ROOT=/home/team/teachbase-hadoop
source "$INSTALL_ROOT/course-env.sh"
yarn node -list -all
```

Должны быть три NodeManager в состоянии `RUNNING`, без `LOST` и `UNHEALTHY`. После автоматической установки можно выполнить `bash "$INSTALL_ROOT/artifacts/node.sh" verify-yarn`. Независимая ручная проверка:

```bash
SAMPLE_FILE="$(mktemp)"
printf 'hadoop yarn\nhadoop hdfs\n' > "$SAMPLE_FILE"
TEST_DIR="/user/$(id -un)/checks/manual-$(date -u +%Y%m%dT%H%M%SZ)-$$"
hdfs dfs -mkdir -p "$TEST_DIR/input"
hdfs dfs -put "$SAMPLE_FILE" "$TEST_DIR/input/sample.txt"
rm -- "$SAMPLE_FILE"
timeout 300 hadoop jar \
  "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar" \
  wordcount "$TEST_DIR/input" "$TEST_DIR/output"
hdfs dfs -cat "$TEST_DIR/output/part-r-*"
hdfs dfs -test -e "$TEST_DIR/output/_SUCCESS"
yarn application -list -appStates FINISHED
```

Ожидаемый результат — три строки, поля разделены табуляцией:

```text
hadoop  2
hdfs    1
yarn    1
```

Записать ID приложения из вывода задания и проверить `yarn application -status application_ID`: состояние `FINISHED`, итоговый статус `SUCCEEDED`. Также открыть задачу в JobHistory и проверить успешные map/reduce без неуспешных попыток.

Первоначальный прогон 23.10.2026 завершился `FINISHED / SUCCEEDED`; результаты приведены в [отчёте](../VALIDATION.md). Отдельно проверен доступ ко всем 11 веб-интерфейсам через SSH на localhost.

## Доступ к веб-интерфейсам через SSH на localhost

SSH-туннель предоставляет доступ с внешнего компьютера к интерфейсам кластера через localhost. Это основной вариант для данного задания. Нужен SSH-доступ к edge; открывать веб-порты в Интернет не требуется.

На внешнем компьютере подготовить копию `inventory.json` с внутренними адресами кластера и выполнить из корня репозитория:

```bash
python3 hw2/open_interfaces.py --inventory inventory.json --gateway team@EDGE_PUBLIC_IP --identity /path/to/local/private_key
```

Без `--execute` скрипт только печатает адреса и команду. Чтобы открыть туннель, добавить `--execute`. На Windows использовать `py -3` и полный путь к ключу в кавычках. Этот ключ используется для входа на edge; внутренний ключ с edge переносить не требуется.

Проверка ключа сервера строгая. Для отдельного файла известных ключей есть параметр `--known-hosts`, для нестандартного SSH-порта — `--port`. Терминал оставить открытым; Ctrl+C закрывает туннель. Все локальные порты слушают только `127.0.0.1`.

| Интерфейс | Локальный адрес |
|---|---|
| NameNode | `http://127.0.0.1:19000/dfshealth.html` |
| SecondaryNameNode | `http://127.0.0.1:19001/status.html` |
| DataNode worker1 | `http://127.0.0.1:19002/datanode.html` |
| DataNode worker2 | `http://127.0.0.1:19003/datanode.html` |
| DataNode worker3 | `http://127.0.0.1:19004/datanode.html` |
| ResourceManager | `http://127.0.0.1:19005/cluster` |
| NodeManager worker1 | `http://127.0.0.1:19006/node` |
| NodeManager worker2 | `http://127.0.0.1:19007/node` |
| NodeManager worker3 | `http://127.0.0.1:19008/node` |
| JobHistory | `http://127.0.0.1:19009/jobhistory` |
| Web Application Proxy | `http://127.0.0.1:19010/proxy/application_ID/` |

Некоторые ссылки исходных интерфейсов содержат внутренние имена или IP. При прямых пробросах нужный сервис открывается по локальному адресу из таблицы. Для навигации по внутренним ссылкам можно использовать SOCKS5:

```bash
ssh -i /path/to/local/private_key -N -D 127.0.0.1:1081 team@EDGE_PUBLIC_IP
```

В отдельном браузере настроить SOCKS5 `127.0.0.1:1081` и разрешение DNS через прокси. Тогда доступны исходные внутренние адреса и перенаправления между сервисами.

В первоначальном прогоне все 11 сервисов ответили HTTP `200` через SSH; Web Application Proxy проверен на работающей задаче и после её завершения. В браузере также проверены состояния NameNode, ResourceManager, JobHistory и checkpoint SecondaryNameNode. Результаты относятся к основному варианту доступа через localhost.

## Дополнительно: публикация интерфейсов через HTTPS

Это необязательный вариант для среды, в которой нужен прямой веб-доступ без SSH. Он не входит в обязательную последовательность ДЗ2. На выданном стенде внешние входящие веб-порты недоступны; далее описан порядок настройки для среды, где такой доступ разрешён.

На edge используется Nginx, который принимает HTTPS-запросы и проксирует их к внутренним веб-портам Hadoop. Каждый интерфейс имеет отдельный внешний порт. Доступ требует Basic Auth поверх TLS; разрешены только `GET` и `HEAD` и предусмотренные конфигурацией пути интерфейсов. WebHDFS, просмотр содержимого HDFS, конфигурация задач и административные маршруты закрыты. Некоторые пункты встроенных меню поэтому вернут отказ. Это публикация состояния узлов, задач и истории, а не API для управления кластером. Для обслуживания используется SSH.

По умолчанию `web.enabled=false`: установка YARN сама по себе ничего не публикует. Для публикации нужно явно включить web, подготовить доверенный сертификат и файл пользователей на edge, проверить конфигурацию и отдельно выполнить развёртывание с `--execute`. Пароли, приватный ключ сертификата и фактический inventory не добавляются в Git.

`PUBLIC_HOST` ниже — реальный внешний DNS-адрес или IPv4 edge, присутствующий в сертификате. Для IP нужен сертификат с соответствующим IP SAN; сертификат на произвольное доменное имя не подходит. Самоподписанный сертификат и `curl -k` не подтверждают доверенный TLS. Срок действия сертификата и его автоматическое продление проверяются отдельно.

| Интерфейс | Внешний адрес |
|---|---|
| NameNode | `https://PUBLIC_HOST:18443/dfshealth.html` |
| SecondaryNameNode | `https://PUBLIC_HOST:18444/status.html` |
| DataNode worker1 | `https://PUBLIC_HOST:18445/datanode.html` |
| DataNode worker2 | `https://PUBLIC_HOST:18446/datanode.html` |
| DataNode worker3 | `https://PUBLIC_HOST:18447/datanode.html` |
| ResourceManager | `https://PUBLIC_HOST:18448/cluster` |
| NodeManager worker1 | `https://PUBLIC_HOST:18449/node` |
| NodeManager worker2 | `https://PUBLIC_HOST:18450/node` |
| NodeManager worker3 | `https://PUBLIC_HOST:18451/node` |
| JobHistory | `https://PUBLIC_HOST:18452/jobhistory` |
| Web Application Proxy | `https://PUBLIC_HOST:18453/proxy/application_ID/` |

В группе безопасности MWS и отдельно в firewall edge открыть TCP 18443–18453 для проверяющего. Для выпуска и продления сертификата по HTTP-01 ниже также нужен входящий TCP 80 из Интернета. Разрешение порта в Ubuntu не изменяет правила облачной сети. SSH оставить доступным для обслуживания. Исходные веб-порты Hadoop должны оставаться внутренними. Если используется список `allowed_cidrs`, включить внешний адрес проверяющего; Basic Auth не отменяет ограничения по IP.

### Подготовка Nginx и учётной записи

На edge:

```bash
sudo apt-get update
sudo apt-get install nginx apache2-utils openssl
sudo htpasswd -cB /etc/nginx/teachbase-htpasswd reviewer
sudo chown root:www-data /etc/nginx/teachbase-htpasswd
sudo chmod 640 /etc/nginx/teachbase-htpasswd
```

`htpasswd` запрашивает пароль интерактивно; пароль не передаётся аргументом команды и не записывается в историю оболочки. `-c` использовать только для первого создания файла: для добавления или смены пользователя выполнить `sudo htpasswd -B /etc/nginx/teachbase-htpasswd reviewer`.

Сертификат и ключ подготовить до включения конфигурации. Следовать инструкции выбранного удостоверяющего центра, проверить соответствие `PUBLIC_HOST` и настроить продление с повторной загрузкой Nginx. Путь `certificate` указывает на полную цепочку, `certificate_key` — на приватный ключ, доступный только root. Скрипт публикации не выпускает сертификаты, не устанавливает пакеты и не создаёт пароли.

В фактическом `inventory.json` заполнить секцию `web`:

```json
"web": {
  "enabled": true,
  "public_host": "PUBLIC_HOST",
  "port_base": 18443,
  "certificate": "/etc/letsencrypt/live/PUBLIC_HOST/fullchain.pem",
  "certificate_key": "/etc/letsencrypt/live/PUBLIC_HOST/privkey.pem",
  "auth_file": "/etc/nginx/teachbase-htpasswd",
  "allowed_cidrs": []
}
```

`PUBLIC_HOST` — заполнитель, который нужно заменить. Пустой `allowed_cidrs` разрешает подключения с любых IP, но не отключает TLS и Basic Auth. Для ограничения по источнику указать нужные CIDR, например адрес проверяющего с `/32`. У каждого узла при необходимости заполнить `aliases` его коротким именем и FQDN: они используются при переписывании внутренних ссылок в публичные.

### Вариант сертификата без домена: Let's Encrypt для IPv4

Этот вариант нужен, если у edge есть публичный IPv4, но нет домена. Let's Encrypt поддерживает сертификаты IP через профиль `shortlived`; срок действия — шесть дней. Для `webroot` требуется Certbot **5.4 или новее**. Используется `certonly`, поскольку установка IP-сертификата плагином Nginx здесь не выполняется. [Официальная инструкция](https://letsencrypt.org/2026/03/11/shorter-certs-certbot/).

На edge установить Certbot в отдельное окружение из PyPI:

```bash
sudo apt-get install python3-venv
sudo python3 -m venv /opt/teachbase-certbot
sudo /opt/teachbase-certbot/bin/python -m pip install --upgrade --index-url https://pypi.org/simple 'certbot>=5.4'
sudo /opt/teachbase-certbot/bin/certbot --version
```

На TCP 80 Nginx должен отдавать только файлы ACME-проверки, без интерфейсов Hadoop и Basic Auth. Следующие действия рассчитаны на выделенную edge со стандартным сайтом Ubuntu. Если там уже есть нужные HTTP-сайты, сохранить их конфигурацию и добавить ACME-location в существующий сервер; чужие сайты не отключать.

Создать каталог, сохранить стандартный сайт и установить отдельную конфигурацию. Если `teachbase-acme.conf` уже существует, сначала проверить его принадлежность этому проекту и сохранить копию.

```bash
ACME_BACKUP="/etc/nginx/teachbase-acme-backup-$(date -u +%Y%m%dT%H%M%SZ)"
sudo install -d -m 700 "$ACME_BACKUP"
sudo install -d -m 755 /var/www/html/.well-known/acme-challenge
if sudo test -e /etc/nginx/sites-enabled/default; then
  sudo mv /etc/nginx/sites-enabled/default "$ACME_BACKUP/default"
fi
if sudo test -f /etc/nginx/conf.d/teachbase-acme.conf; then
  sudo cp -p /etc/nginx/conf.d/teachbase-acme.conf "$ACME_BACKUP/previous.conf"
fi
sudo tee /etc/nginx/conf.d/teachbase-acme.conf >/dev/null <<'EOF'
# Managed ACME endpoint for teachbase-hadoop.
server {
    listen 80 default_server;
    server_name _;
    location ^~ /.well-known/acme-challenge/ {
        root /var/www/html;
        default_type text/plain;
        try_files $uri =404;
    }
    location / { return 404; }
}
EOF
sudo nginx -t && sudo systemctl reload nginx
```

Если проверка Nginx не прошла, восстановить `previous.conf` из `ACME_BACKUP` либо удалить новый `teachbase-acme.conf`, если его раньше не было; вернуть сохранённый `default` в `/etc/nginx/sites-enabled/default`. Затем снова выполнить `nginx -t` и reload.

До запроса сертификата проверить доступность challenge **с внешнего компьютера**. На edge создать временный файл:

```bash
printf 'acme-reachability\n' | sudo tee /var/www/html/.well-known/acme-challenge/reachability >/dev/null
```

На внешнем компьютере:

```bash
curl --fail --max-time 15 http://PUBLIC_IP/.well-known/acme-challenge/reachability
```

Ожидается `acme-reachability`. Проверка через `127.0.0.1` на edge недостаточна. Если локальный ответ есть, а внешний запрос зависает, проверить публичный адрес/NAT и группу безопасности MWS, затем firewall ОС. При закрытом внешнем TCP 80 выпуск через этот способ невозможен; повторные запросы Certbot причину не устранят.

После успешной внешней проверки на edge заменить `PUBLIC_IP` фактическим адресом и запросить сертификат:

```bash
sudo /opt/teachbase-certbot/bin/certbot certonly \
  --preferred-profile shortlived \
  --webroot -w /var/www/html \
  --ip-address PUBLIC_IP \
  --cert-name teachbase-edge
sudo rm -- /var/www/html/.well-known/acme-challenge/reachability
```

Certbot интерактивно запросит необходимые данные и согласие с условиями удостоверяющего центра. Для пробного выпуска можно добавить `--staging` и использовать отдельное имя `teachbase-edge-staging`; такой сертификат не доверенный и для публикации не подходит.

После успешного обычного выпуска в inventory задать `public_host` равным этому IPv4, `certificate` — `/etc/letsencrypt/live/teachbase-edge/fullchain.pem`, `certificate_key` — `/etc/letsencrypt/live/teachbase-edge/privkey.pem`. Приватный ключ не копировать из защищённого каталога. Далее установить HTTPS-конфигурацию по следующему разделу.

Поскольку сертификат короткоживущий, настроить автоматическое продление. Следующий отдельный таймер проверяет необходимость продления каждый час; новый сертификат выпускается только когда это требуется. После успешного продления deploy-hook проверяет конфигурацию и перезагружает Nginx. [Документация Certbot по продлению](https://eff-certbot.readthedocs.io/en/stable/using.html#renewing-certificates).

```bash
sudo install -d -m 755 /etc/letsencrypt/renewal-hooks/deploy
sudo tee /etc/letsencrypt/renewal-hooks/deploy/teachbase-nginx.sh >/dev/null <<'EOF'
#!/bin/sh
set -eu
/usr/sbin/nginx -t
/usr/bin/systemctl reload nginx
EOF
sudo chmod 750 /etc/letsencrypt/renewal-hooks/deploy/teachbase-nginx.sh
sudo tee /etc/systemd/system/teachbase-certbot-renew.service >/dev/null <<'EOF'
[Unit]
Description=Renew the Hadoop edge TLS certificate
[Service]
Type=oneshot
ExecStart=/opt/teachbase-certbot/bin/certbot renew --quiet --cert-name teachbase-edge
EOF
sudo tee /etc/systemd/system/teachbase-certbot-renew.timer >/dev/null <<'EOF'
[Unit]
Description=Check the Hadoop edge certificate hourly
[Timer]
OnCalendar=hourly
RandomizedDelaySec=5m
Persistent=true
[Install]
WantedBy=timers.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now teachbase-certbot-renew.timer
sudo /opt/teachbase-certbot/bin/certbot renew --cert-name teachbase-edge --dry-run
sudo /etc/letsencrypt/renewal-hooks/deploy/teachbase-nginx.sh
systemctl list-timers teachbase-certbot-renew.timer
```

TCP 80 и ACME-location должны оставаться доступными для продления. Проверять ошибки через `journalctl -u teachbase-certbot-renew.service` и срок сертификата через `sudo openssl x509 -in /etc/letsencrypt/live/teachbase-edge/fullchain.pem -noout -dates`. Установка таймера сама по себе не доказывает успешное продление: необходим успешный `--dry-run` и проверка внешнего TLS. Фактический результат выпуска и проверки стенда фиксируется в [VALIDATION.md](../VALIDATION.md).

### Автоматическая установка конфигурации Nginx

Из корня репозитория сначала получить предпросмотр без изменения сервера:

```bash
python3 hw2/deploy_web.py --inventory inventory.json
```

После подготовки сертификата и учётной записи на edge:

```bash
python3 hw2/deploy_web.py --inventory inventory.json --execute
```

Для выполнения нужны `sudo -n`, Nginx и OpenSSL. Скрипт устанавливает только принадлежащий этому проекту файл `/etc/nginx/conf.d/teachbase-hadoop.conf`, проверяет конфигурацию командой `nginx -t`, затем перезагружает Nginx. При ошибке восстанавливает предыдущую конфигурацию. Чужой файл с этим именем без метки проекта перезаписываться не должен.

### Ручная установка конфигурации Nginx

Можно отдельно сгенерировать файлы, затем установить конфигурацию обычными командами. Здесь Python только формирует конфигурацию; удалённое развёртывание не запускается:

```bash
python3 common/cluster.py render --inventory inventory.json --output generated-web
```

Проверить `generated-web/nginx.conf`: внешний адрес, диапазон портов, внутренние адреса, TLS, Basic Auth, разрешённые методы и пути. Далее на edge, из каталога репозитория:

```bash
NGINX_CONF=/etc/nginx/conf.d/teachbase-hadoop.conf
NGINX_BACKUP="/etc/nginx/teachbase-backup-$(date -u +%Y%m%dT%H%M%SZ)"
sudo install -d -m 700 "$NGINX_BACKUP"
if sudo test -f "$NGINX_CONF"; then
  sudo cp -p "$NGINX_CONF" "$NGINX_BACKUP/previous.conf"
fi
sudo install -o root -g root -m 644 generated-web/nginx.conf "$NGINX_CONF"
if sudo nginx -t && sudo systemctl reload nginx; then
  printf 'Nginx loaded; external verification is still required.\n'
else
  if sudo test -f "$NGINX_BACKUP/previous.conf"; then
    sudo cp -p "$NGINX_BACKUP/previous.conf" "$NGINX_CONF"
  else
    sudo rm -- "$NGINX_CONF"
  fi
  sudo nginx -t && sudo systemctl reload nginx
  printf 'New configuration failed; previous state restored.\n' >&2
fi
```

Перед заменой уже существующего файла убедиться, что он относится к этому проекту. Сохранить путь `NGINX_BACKUP`. Для последующего отката восстановить из него `previous.conf`; если файл до установки отсутствовал — удалить только `/etc/nginx/conf.d/teachbase-hadoop.conf`. После отката снова выполнить `sudo nginx -t && sudo systemctl reload nginx`.

### Проверка внешней публикации

Проверять с компьютера вне сети кластера, без SSH-туннеля. Для повторяемой проверки есть [verify_web.py](verify_web.py). Сохранить пароль одной строкой в защищённом локальном файле вне репозитория и выполнить:

```bash
python3 hw2/verify_web.py \
  --inventory inventory.json \
  --username reviewer \
  --password-file /local/private/password.txt \
  --application-id application_1700000000000_0001 \
  --output public-check.json
```

Подставить свой путь к файлу пароля и реальный ID приложения. Скрипт проверяет TLS системными доверенными CA, страницы и данные служб, отсутствие доступа без пароля и с неверным паролем, блокировку POST, `/conf` и WebHDFS. Для завершённого приложения проверяется единственный допустимый переход на соответствующий JobHistory того же опубликованного кластера и содержимое страницы задачи. Без ID приложения проверка прокси пропускается, итог — `partial`, а не `pass`. Отчёт сохраняется в JSON; пароль в него не записывается. Полную браузерную навигацию скрипт не заменяет.

Параметр `--ca-file /local/private/test-ca.pem` нужен только для явно доверенной частной или тестовой CA: проверка имени и подписи сертификата остаётся включённой, но результат не доказывает доверие обычных браузеров. `--connect-host 127.0.0.1` подменяет только адрес TCP-подключения для диагностики на edge, сохраняя исходное имя для TLS; такой прогон помечается `scope=internal`. Даже `pass` в этом режиме не подтверждает доступность из Интернета. Для проверки публичного доступа не использовать `--connect-host`, а для общедоверенного сертификата — и `--ca-file`.

Ручные проверки ниже дополняют скрипт. Заменить `PUBLIC_HOST`; при `curl --user reviewer` пароль вводится интерактивно.

```bash
curl --head https://PUBLIC_HOST:18443/dfshealth.html
curl --user reviewer --head https://PUBLIC_HOST:18443/dfshealth.html
curl --user reviewer --request POST --include https://PUBLIC_HOST:18443/dfshealth.html
curl --user reviewer --head https://PUBLIC_HOST:18443/webhdfs/v1/
```

Ожидания: запрос без учётных данных — `401`; с правильными данными — `200`; `POST` — `405`; запрещённый путь — `403` или `404`. Отдельно повторить запрос с неверным паролем: ожидается `401`. Проверить это для всех 11 портов. TLS должен проверяться без `-k` и без предупреждений браузера. Если настроен IP allowlist, тестировать с разрешённого адреса; с запрещённого ожидается отказ.

HTTP `200` подтверждает только ответ сервера. В браузере дополнительно проверить:

- NameNode: три live DataNode, отсутствие dead-узлов; данные таблиц действительно загрузились.
- SecondaryNameNode: состояние и сведения о checkpoint; DataNode: загрузку страницы каждого узла.
- ResourceManager: три active NodeManager, состояние и детали задачи.
- Переходы ResourceManager → NodeManager, ApplicationMaster и JobHistory: адреса остаются внешними HTTPS, ресурсы JS/CSS и JSON-запросы успешно загружаются; внутренних имён и IP в переходах нет.
- Web Application Proxy: открыть реальный `/proxy/application_ID/` во время выполнения задачи, затем проверить переход к истории после завершения. Корень `/` прокси может возвращать `404`; это не достаточная проверка работоспособности.
- JobHistory: успешную задачу, map/reduce и счётчики. Запросы, меняющие состояние кластера, должны оставаться недоступными.

Если проверка не пройдена, отключить публикацию, восстановив сохранённую конфигурацию Nginx; повторно выполнить `nginx -t` и reload. Не считать успешную локальную генерацию конфигурации подтверждением доступности из Интернета. Фактические даты, коды ответов и проверенные переходы заносятся в [VALIDATION.md](../VALIDATION.md) после прогона.

## Остановка и откат YARN

Перед остановкой проверить `yarn application -list`: нужные задачи должны завершиться. На workers после загрузки `course-env.sh` выполнить:

```bash
yarn --daemon stop nodemanager
```

На master:

```bash
yarn --daemon stop proxyserver
mapred --daemon stop historyserver
yarn --daemon stop resourcemanager
```

Для отката конфигурации на каждой ВМ восстановить `yarn-site.xml` и `mapred-site.xml` из её собственного `BACKUP_DIR`, затем повторить команды запуска и проверку YARN. Данные HDFS и каталоги NameNode при откате не удаляются и не форматируются.

После перезагрузки сначала запустить HDFS, затем YARN; автозапуск демонов Hadoop средствами ОС не настроен.

Материалы: [Cluster Setup](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/ClusterSetup.html), [Web Application Proxy](https://hadoop.apache.org/docs/r3.4.2/hadoop-yarn/hadoop-yarn-site/WebApplicationProxy.html), [учёт памяти NodeManager](https://hadoop.apache.org/docs/r3.4.2/hadoop-yarn/hadoop-yarn-site/NodeManagerCGroupsMemory.html).
