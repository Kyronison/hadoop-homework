-- Шаг 3. Внешняя таблица поверх gpfdist и проверка чтения.
-- Переменные psql: schema, ext, gpfdist_url, expected_rows.

\echo '=== Шаг 3. Внешняя таблица (gpfdist) ==='

SET search_path TO :"schema";

DROP EXTERNAL TABLE IF EXISTS :"ext";

-- Читаемая внешняя таблица: данные остаются в файле, сегменты сами забирают их
-- у gpfdist параллельно. Ошибочные строки не пропускаются (нет SEGMENT REJECT LIMIT):
-- любая некорректная строка прерывает запрос, поэтому число строк либо полное, либо ошибка.
CREATE READABLE EXTERNAL TABLE :"ext" (
    pickup_day      date,
    pickup_zone_id  integer,
    passenger_count double precision,
    trip_distance   double precision,
    amount_cents    bigint,
    vendor_id       integer,
    pickup_at       timestamp,
    dropoff_at      timestamp
)
LOCATION (:'gpfdist_url')
FORMAT 'CSV' (HEADER DELIMITER ',' NULL '')
ENCODING 'UTF8';

-- В Cloudberry внешняя таблица хранится как foreign table; параметры gpfdist — в ftoptions
-- (pg_exttable командам недоступен).
SELECT c.relname AS table_name, c.relkind, opt AS option
FROM pg_foreign_table ft
JOIN pg_class c ON c.oid = ft.ftrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(ft.ftoptions) AS opt
WHERE n.nspname = :'schema' AND c.relname = :'ext'
  AND split_part(opt, '=', 1) IN ('location_uris', 'format', 'header', 'delimiter',
                                  'execute_on', 'log_errors', 'is_writable');

-- Пробное чтение нескольких строк.
SELECT * FROM :"ext" LIMIT 5;

SELECT count(*) AS ext_rows FROM :"ext" \gset
\echo 'Строк во внешней таблице:' :ext_rows '| строк данных в CSV:' :expected_rows

SELECT :ext_rows = :expected_rows AS ok \gset
\if :ok
\echo 'OK: внешняя таблица читает все строки файла'
\else
DO $$BEGIN RAISE EXCEPTION 'Число строк во внешней таблице не совпадает с файлом'; END$$;
\endif
