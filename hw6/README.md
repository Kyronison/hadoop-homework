# ДЗ6. Greenplum (Apache Cloudberry): загрузка через gpfdist и внешние таблицы

Данные NYC taxi загружены в MPP-СУБД через gpfdist и внешнюю таблицу, перенесены в распределённую таблицу схемы `team40`, проверено распределение по сегментам, выполнены аналитические запросы и построена витрина по дням. Весь цикл автоматизирован: [run.sh](run.sh) выполняет шаги 1–8 на сервере курса и останавливается на первой не пройденной проверке, [remote.sh](remote.sh) запускает его со своего компьютера по SSH. Журнал проверочного прогона 28.11.2026 — [results/run.log](results/run.log).

## Окружение

| Параметр | Значение |
|---|---|
| СУБД | Apache Cloudberry 2.2.0-incubating (форк Greenplum), PostgreSQL 14.10, psql 14.10, оптимизатор GPORCA |
| Кластер | координатор `team-05-nn:5432` + 2 primary-сегмента: `team-05-00:6000` (content 0), `team-05-01:6000` (content 1) |
| Вход | `ssh team40@171.22.73.82` (хост `team-05-en`), командный SSH-ключ |
| База / учётная запись / схема | `idp` / `team40` / `team40` |
| gpfdist | `team-05-en:8081`, корневой каталог `/srv/gpfdist`, каталог команды `/srv/gpfdist/team40/` |
| Исходные данные | `/srv/gpfdist/taxi_small.csv` — 100 000 строк + заголовок, 7 063 247 байт |
| Справочник | `public.taxi_zones` — 265 зон, `DISTRIBUTED BY (location_id)` |
| Таблицы команды | `team40_ext` (внешняя, временная), `team40_taxi` (детальная), `team40_daily` (витрина) |

## Подключение

```bash
ssh team40@171.22.73.82
psql
```

В интерактивной сессии `~/.bashrc` уже подключает окружение Cloudberry и задаёт `PGHOST=team-05-nn PGDATABASE=idp PGUSER=team40`, пароль берётся из `~/.pgpass`. Полная команда, если переменные не заданы (например, в неинтерактивной SSH-сессии):

```bash
source /usr/cloudberry-db/cloudberry-env.sh && psql -h team-05-nn -U team40 -d idp
```

Проверка:

```sql
SELECT current_user, current_database();
```

```text
 current_user | current_database
--------------+------------------
 team40       | idp
```

## Автоматический запуск

Со своего компьютера из корня репозитория (Linux, macOS или Git Bash в Windows):

```bash
bash hw6/remote.sh
```

Скрипт копирует `run.sh` и `sql/` на сервер в `~/hw6`, выполняет `run.sh` и сохраняет полный вывод в `hw6/results/run.log`. Параметры задаются переменными окружения: `TEAM` (по умолчанию `team40`), `PREFIX` (префикс таблиц, по умолчанию равен `TEAM`; для индивидуальной сдачи — например `PREFIX=team40_ivanov`), `HOST` (по умолчанию `171.22.73.82`), `SSH_KEY` (если ключ не используется ssh по умолчанию).

Тот же цикл можно запустить прямо на сервере, если каталог `hw6` уже скопирован:

```bash
bash ~/hw6/run.sh
```

| Шаг задания | Где выполняется | Встроенная проверка (при ошибке — остановка с ненулевым кодом) |
|---|---|---|
| 1. Подключение | [sql/01_connection.sql](sql/01_connection.sql) | `current_user = team40`, `current_database = idp`, есть право `CREATE` в схеме |
| 2. Подготовка файла | `run.sh` | копия в `/srv/gpfdist/team40/team40_taxi.csv` побайтно совпадает с исходным файлом; число строк данных = строки файла − заголовок |
| 3. Внешняя таблица | [sql/02_external.sql](sql/02_external.sql) | `count(*)` внешней таблицы = числу строк файла |
| 4. Внутренняя таблица | [sql/03_load.sql](sql/03_load.sql) | `count(*)` = числу строк файла; разность с внешней таблицей (`EXCEPT ALL` в обе стороны) пуста |
| 5. Распределение | [sql/04_distribution.sql](sql/04_distribution.sql) | данные есть на всех primary-сегментах, перекос max/min ≤ 2 |
| 6. Аналитические запросы | [sql/05_queries.sql](sql/05_queries.sql) | — (результаты в журнале и ниже) |
| 7. Витрина | [sql/06_daily.sql](sql/06_daily.sql) | сумма поездок и выручки витрины = итогам детальной таблицы |
| 8. Статистика и уборка | [sql/07_analyze_cleanup.sql](sql/07_analyze_cleanup.sql), `run.sh` | внешняя таблица удалена; CSV удалён из каталога |
| Самопроверка | [sql/08_selfcheck.sql](sql/08_selfcheck.sql) | итоговые числа и размеры; в схеме не осталось внешних таблиц |

Повторный запуск безопасен: таблицы пересоздаются (`DROP ... IF EXISTS`), дублей не возникает. CSV удаляется ловушкой `trap ... EXIT` и после ошибки на любом шаге, чтобы не занимать общее место на диске. Все SQL-файлы получают имена через переменные psql (`:"ext"`, `:"taxi"`, `:"daily"`) и работают только в своей схеме; `public` только читается.

## Пошаговая инструкция

Ниже — те же действия вручную, с подставленными именами `team40`. Команды выполняются на сервере после `ssh team40@171.22.73.82`.

### 1. Подключение

```bash
psql -c "SELECT current_user, current_database();"
psql -c "SELECT content, role, status, hostname, port FROM gp_segment_configuration ORDER BY content;"
```

Ожидается `team40 | idp` и три строки: координатор (`content = -1`) и сегменты 0 и 1 в статусе `u`.

### 2. Подготовка файла

```bash
cp /srv/gpfdist/taxi_small.csv /srv/gpfdist/team40/team40_taxi.csv
chmod 0644 /srv/gpfdist/team40/team40_taxi.csv
wc -l /srv/gpfdist/team40/team40_taxi.csv      # 100001 = 100000 строк + заголовок
stat -c %s /srv/gpfdist/team40/team40_taxi.csv # 7063247 байт
```

gpfdist работает от пользователя `team` и раздаёт `/srv/gpfdist`, поэтому файл в каталоге команды доступен по адресу `gpfdist://team-05-en:8081/team40/team40_taxi.csv`. Права `0644` нужны, потому что gpfdist читает файл как «прочие» пользователи.

### 3. Внешняя таблица

```sql
SET search_path TO team40;

CREATE READABLE EXTERNAL TABLE team40_ext (
    pickup_day date, pickup_zone_id integer, passenger_count double precision,
    trip_distance double precision, amount_cents bigint, vendor_id integer,
    pickup_at timestamp, dropoff_at timestamp
)
LOCATION ('gpfdist://team-05-en:8081/team40/team40_taxi.csv')
FORMAT 'CSV' (HEADER DELIMITER ',' NULL '')
ENCODING 'UTF8';

SELECT count(*) FROM team40_ext;   -- 100000
```

Данные остаются в файле: при запросе каждый сегмент сам забирает свою часть у gpfdist. `SEGMENT REJECT LIMIT` намеренно не задан — любая некорректная строка прерывает запрос, поэтому число строк либо полное, либо ошибка. В Cloudberry внешняя таблица хранится как foreign table, её параметры видны в `pg_foreign_table.ftoptions` (`location_uris=gpfdist://...`, `format=csv`, `header=true`, `execute_on=ALL_SEGMENTS`); системное представление `pg_exttable` учётным записям команд недоступно.

### 4. Внутренняя таблица

```sql
CREATE TABLE team40_taxi (
    pickup_day date, pickup_zone_id integer, passenger_count double precision,
    trip_distance double precision, amount_cents bigint, vendor_id integer,
    pickup_at timestamp, dropoff_at timestamp
)
WITH (appendoptimized = true, orientation = column, compresstype = zstd, compresslevel = 1)
DISTRIBUTED BY (pickup_zone_id);

INSERT INTO team40_taxi SELECT * FROM team40_ext;
ANALYZE team40_taxi;

SELECT count(*) FROM team40_taxi;  -- 100000
```

Формат хранения — append-optimized, колоночный, со сжатием zstd: таблица только дописывается при загрузке, а аналитические запросы читают несколько столбцов из восьми. Колоночное хранение со сжатием уменьшило объём в 4,1 раза по сравнению с CSV. `ANALYZE` сразу после загрузки даёт планировщику реальные объёмы для запросов шага 6; итоговый `ANALYZE` всех таблиц — в шаге 8.

### 5. Проверка распределения

```sql
SELECT gp_segment_id, count(*) AS rows,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM team40_taxi
GROUP BY gp_segment_id
ORDER BY gp_segment_id;
```

`gp_segment_id` — служебный столбец, номер сегмента, на котором физически хранится строка. Скрипт дополнительно считает перекос max/min и коэффициент вариации, сравнивает другие ключи и показывает план JOIN (см. [Выбор ключа дистрибуции](#выбор-ключа-дистрибуции)).

### 6. Аналитические запросы

Полный текст — в [sql/05_queries.sql](sql/05_queries.sql). Пример запроса с JOIN к справочнику:

```sql
SELECT z.borough, z.zone, count(*) AS trips,
       round(sum(t.amount_cents) / 100.0, 2) AS revenue_usd,
       round(avg(t.trip_distance)::numeric, 2) AS avg_distance_mi
FROM team40_taxi t
JOIN public.taxi_zones z ON z.location_id = t.pickup_zone_id
GROUP BY t.pickup_zone_id, z.borough, z.zone
ORDER BY trips DESC
LIMIT 10;
```

Ещё три запроса: поездки и выручка по районам (JOIN, у зоны 265 «Outside of NYC» район не указан — выводится название зоны), профиль по часу посадки и проверка качества исходных данных.

### 7. Витрина по дням

```sql
CREATE TABLE team40_daily AS
SELECT pickup_day AS trip_day,
       count(*) AS trips,
       round(sum(amount_cents) / 100.0, 2) AS revenue_usd,
       round(avg(trip_distance)::numeric, 3) AS avg_distance_mi
FROM team40_taxi
GROUP BY pickup_day
DISTRIBUTED BY (trip_day);
```

Выручка — сумма `amount_cents` в долларах, возвраты с отрицательной суммой её уменьшают; дистанция — в милях. Ключ `trip_day` уникален в витрине.

### 8. Статистика и уборка

```sql
ANALYZE team40_taxi;
ANALYZE team40_daily;
DROP EXTERNAL TABLE team40_ext;
```

```bash
rm /srv/gpfdist/team40/team40_taxi.csv
ls -la /srv/gpfdist/team40/
```

## Выбор ключа дистрибуции

Выбран `DISTRIBUTED BY (pickup_zone_id)`.

1. **Соединение без перемещения данных.** Справочник `public.taxi_zones` распределён по `location_id` того же типа `integer` и того же класса операторов хеширования. Строки поездок и строка их зоны попадают на один сегмент, поэтому JOIN выполняется локально. Планы из прогона:

   ```text
   -- team40_taxi, DISTRIBUTED BY (pickup_zone_id)
   Gather Motion 2:1  (slice1; segments: 2)
     ->  Hash Join
           Hash Cond: (t.pickup_zone_id = z.location_id)
           ->  Seq Scan on team40_taxi t
           ->  Hash
                 ->  Seq Scan on taxi_zones z

   -- та же таблица, DISTRIBUTED RANDOMLY
   Gather Motion 2:1  (slice1; segments: 2)
     ->  Hash Join
           Hash Cond: (t.pickup_zone_id = z.location_id)
           ->  Seq Scan on team40_keytest_random t
           ->  Hash
                 ->  Broadcast Motion 2:2  (slice2; segments: 2)
                       ->  Seq Scan on taxi_zones z
   ```

   С выбранным ключом нет ни `Redistribute Motion`, ни `Broadcast Motion` — только сбор результата на координатор. При случайном распределении справочник приходится рассылать на все сегменты; для большого справочника пришлось бы перераспределять саму таблицу фактов.
2. **Агрегаты по зоне** (`GROUP BY pickup_zone_id`) считаются целиком на сегменте, без перераспределения.
3. **Равномерность.** 226 различных зон на 2 сегмента дают перекос 1,20 — в пределах допустимых 2×. Остаточный перекос связан с популярными зонами: JFK Airport — 8 035 поездок (8 %), Penn Station — 4 302.

Сравнение кандидатов на тех же 100 000 строках (скрипт создаёт временные таблицы `team40_keytest_*` в своей схеме и сразу удаляет их):

| Ключ | Различных значений | Сегмент 0 / сегмент 1 | Перекос max/min | Вывод |
|---|---:|---|---:|---|
| **`pickup_zone_id`** | 226 | 45 385 / 54 615 | **1,203** | равномерно, JOIN со справочником без Motion |
| `pickup_day` | 6 | 29 502 / 70 498 | 2,390 | больше 2×: почти все поездки за два дня, весь день — на одном сегменте |
| `vendor_id` | 2 | 77 128 / 22 872 | 3,372 | всего два значения: сильный перекос, а на кластере с бо́льшим числом сегментов остальные простаивали бы |
| `RANDOMLY` | — | ≈ 50 000 / 50 000 | ≈ 1,00 | идеально ровно, но JOIN и агрегаты по зоне требуют Motion |

`pickup_at` (63 586 различных значений) тоже распределился бы ровно, но не даёт выигрыша ни в одном запросе: по времени не соединяют и не группируют. Если на кластере станет больше сегментов, перекос по зонам вырастет; тогда разумно перейти на составной ключ `(pickup_zone_id, pickup_at)` или `RANDOMLY` и принять Broadcast небольшого справочника.

## Результаты самопроверки

Прогон 28.11.2026, полный вывод — [results/run.log](results/run.log). Все 10 встроенных проверок пройдены, `run.sh` завершился с кодом 0. Повторный запуск поверх уже созданных таблиц дал те же числа строк, распределение, результаты запросов и витрину.

| Проверка | Результат |
|---|---|
| Подключение | `current_user = team40`, `current_database = idp`, координатор 10.5.0.11:5432, 2 рабочих primary-сегмента |
| Строки в исходном файле | 100 001 строка, из них 100 000 строк данных + заголовок |
| `count(*)` внешней таблицы `team40_ext` | **100 000** — совпадает с файлом |
| `count(*)` внутренней таблицы `team40_taxi` | **100 000** — совпадает с файлом; построчная разность с внешней таблицей — 0 строк |
| Распределение по сегментам | сегмент 0 — 45 385 строк (45,4 %, 107 зон), сегмент 1 — 54 615 строк (54,6 %, 119 зон); перекос max/min **1,203**, коэффициент вариации 0,092 |
| Витрина `team40_daily` | 6 строк; сумма поездок 100 000 и выручка 3 004 380,42 USD совпадают с детальной таблицей |
| ANALYZE | статистика планировщика: `team40_taxi` — 100 000 строк, 8 столбцов со статистикой; `team40_daily` — 6 строк, 4 столбца |
| Внешняя таблица | удалена, внешних таблиц в схеме `team40` нет |
| CSV | удалён, каталог `/srv/gpfdist/team40/` пуст |
| Размер сырых данных | **7 063 247 байт** (6,7 МиБ) |
| Размер данных в БД | `team40_taxi` — **1 712 808 байт (1673 kB)**: сегмент 0 — 746 kB, сегмент 1 — 895 kB; `team40_daily` — 160 kB; всего в схеме — 1833 kB, в 3,8 раза меньше CSV и далеко в пределах лимита 500 МБ |

Размер AO-таблицы между прогонами может отличаться на 1–2 %: gpfdist раздаёт файл сегментам параллельно, порядок строк в блоках и степень сжатия немного меняются. Витрина из 6 строк занимает 160 kB, потому что heap-таблица на каждом сегменте занимает минимум одну страницу 32 KiB плюс служебные слои.

### Запрос 6.1. Топ-10 зон посадки (JOIN с `public.taxi_zones`)

| Район | Зона | Поездок | Доля, % | Выручка, USD | Средний чек, USD | Средняя дистанция, mi |
|---|---|---:|---:|---:|---:|---:|
| Queens | JFK Airport | 8 035 | 8,04 | 620 416,07 | 77,21 | 15,64 |
| Manhattan | Penn Station/Madison Sq West | 4 302 | 4,30 | 102 142,93 | 23,74 | 2,60 |
| Manhattan | Midtown Center | 4 073 | 4,07 | 103 208,12 | 25,34 | 3,11 |
| Queens | LaGuardia Airport | 3 879 | 3,88 | 238 102,51 | 61,38 | 9,46 |
| Manhattan | Upper East Side South | 3 360 | 3,36 | 63 214,83 | 18,81 | 2,16 |
| Manhattan | Midtown East | 3 345 | 3,35 | 79 597,89 | 23,80 | 2,74 |
| Manhattan | Clinton East | 3 104 | 3,10 | 79 110,28 | 25,49 | 3,15 |
| Manhattan | Upper East Side North | 3 093 | 3,09 | 61 046,86 | 19,74 | 2,11 |
| Manhattan | Times Sq/Theatre District | 3 057 | 3,06 | 99 239,31 | 32,46 | 4,54 |
| Manhattan | Murray Hill | 2 945 | 2,95 | 69 148,65 | 23,48 | 2,68 |

Аэропорты дают непропорционально большую выручку: JFK — 8 % поездок, но 20,7 % выручки (620 тыс. из 3,0 млн USD) за счёт длинных поездок и среднего чека 77 USD против 19–25 USD внутри Манхэттена.

### Запрос 6.2. Поездки и выручка по районам (JOIN с `public.taxi_zones`)

| Район | Поездок | Доля, % | Выручка, USD | Средний чек, USD | Средняя дистанция, mi | Средняя длительность, мин |
|---|---:|---:|---:|---:|---:|---:|
| Manhattan | 84 717 | 84,72 | 1 997 917,31 | 23,58 | 2,80 | 14,4 |
| Queens | 13 464 | 13,46 | 935 125,50 | 69,45 | 12,88 | 28,5 |
| Brooklyn | 1 087 | 1,09 | 35 329,08 | 32,50 | 5,95 | 25,5 |
| Unknown | 390 | 0,39 | 13 281,46 | 34,06 | 3,90 | 17,5 |
| Bronx | 191 | 0,19 | 8 201,23 | 42,94 | 7,10 | 29,9 |
| Outside of NYC | 118 | 0,12 | 11 371,74 | 96,37 | 1,60 | 15,7 |
| EWR | 29 | 0,03 | 2 803,86 | 96,68 | 0,59 | 0,7 |
| Staten Island | 4 | 0,00 | 350,24 | 87,56 | 4,55 | 10,4 |

Все 100 000 поездок нашли зону в справочнике (сумма по районам — 100 000). Основная масса поездок — Манхэттен (85 %), Queens даёт 31 % выручки благодаря аэропортам.

### Запрос 6.3. Профиль по часу посадки

24 строки, полностью — в журнале. Выборка `taxi_small` приходится в основном на 1–2 января 2024 года, поэтому много поездок в новогоднюю ночь: 0–2 ч — 18 345 поездок. Дневной пик — 12 ч (7 708 поездок), минимум — 23 ч (1 468). Самые длинные поездки — поздним вечером и ранним утром (23 ч — 6,95 mi, 5–6 ч — 5,6–6,0 mi, средний чек 35–42 USD), днём (10–17 ч) — 3,7–4,7 mi.

### Запрос 6.4. Качество исходных данных

| Всего | Вне января 2024 | Отрицательная сумма | Нулевая дистанция | Высадка раньше посадки | Мин. дата | Макс. дата |
|---:|---:|---:|---:|---:|---|---|
| 100 000 | 13 | 1 586 | 1 811 | 1 | 2002-12-31 | 2024-01-03 |

Строки не удалялись: задание требует совпадения числа строк с исходным файлом. Отрицательные суммы — возвраты и корректировки, они уменьшают выручку.

### Витрина `team40_daily`

| Дата | Поездок | Выручка, USD | Средняя дистанция, mi |
|---|---:|---:|---:|
| 2002-12-31 | 2 | 0,00 | 0,630 |
| 2009-01-01 | 1 | 9,40 | 0,460 |
| 2023-12-31 | 10 | 224,62 | 2,601 |
| 2024-01-01 | 70 495 | 2 142 721,65 | 4,227 |
| 2024-01-02 | 29 491 | 861 406,75 | 4,152 |
| 2024-01-03 | 1 | 18,00 | 1,410 |

Содержательные дни — 1 и 2 января 2024 года (99,99 % поездок). Строки 2002-12-31, 2009-01-01 и 2023-12-31 — некорректные даты в исходных данных; 2024-01-03 — единичная поездка на границе выборки. Они оставлены в витрине, чтобы её итоги совпадали с детальной таблицей.

## Проверка скриптов без кластера

```bash
python3 -m unittest tests.test_greenplum_scripts -v
bash -n hw6/run.sh
bash -n hw6/remote.sh
```

Тесты проверяют, что `run.sh` вызывает все SQL-шаги по порядку, таблицы создаются и удаляются только через переменные с префиксом команды, схема `public` только читается, каждый шаг работает в своей схеме, CSV удаляется и при ошибке, а файлы имеют окончания строк LF.

## Файлы решения

- [run.sh](run.sh) — полный цикл на сервере: проверки окружения, копирование CSV, вызов SQL-шагов, удаление CSV.
- [remote.sh](remote.sh) — запуск со своего компьютера по SSH с сохранением журнала.
- [sql/](sql) — SQL по шагам задания (`01`–`08`) со встроенными проверками через `\gset` / `\if`.
- [results/run.log](results/run.log) — журнал проверочного прогона.

## Первичные источники

- [Apache Cloudberry: CREATE EXTERNAL TABLE](https://cloudberry.apache.org/docs/sql-stmts/create-external-table).
- [Apache Cloudberry: загрузка данных через gpfdist](https://cloudberry.apache.org/docs/data-loading/load-data-using-gpfdist).
- [Apache Cloudberry: создание таблиц и политика распределения](https://cloudberry.apache.org/docs/operate-with-data/operate-with-db-objects/create-and-manage-tables).
- [Apache Cloudberry: форматы heap и append-optimized](https://cloudberry.apache.org/docs/operate-with-data/heap-and-ao-table-formats).
- [PostgreSQL 14: psql, метакоманды \gset и \if](https://www.postgresql.org/docs/14/app-psql.html).
