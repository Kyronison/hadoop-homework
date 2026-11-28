-- Шаг 4. Внутренняя распределённая таблица и перенос данных из внешней.
-- Переменные psql: schema, ext, taxi, expected_rows.

\echo '=== Шаг 4. Внутренняя таблица ==='

SET search_path TO :"schema";

DROP TABLE IF EXISTS :"taxi";

-- Append-optimized колоночная таблица со сжатием zstd: аналитические запросы читают
-- только нужные столбцы. Ключ дистрибуции pickup_zone_id совпадает с ключом
-- справочника public.taxi_zones (location_id, тот же тип integer), поэтому JOIN
-- выполняется локально на сегментах без перераспределения данных.
CREATE TABLE :"taxi" (
    pickup_day      date,
    pickup_zone_id  integer,
    passenger_count double precision,
    trip_distance   double precision,
    amount_cents    bigint,
    vendor_id       integer,
    pickup_at       timestamp,
    dropoff_at      timestamp
)
WITH (appendoptimized = true, orientation = column, compresstype = zstd, compresslevel = 1)
DISTRIBUTED BY (pickup_zone_id);

-- Данные идут параллельно: gpfdist -> сегменты -> сегмент-владелец по хешу ключа.
INSERT INTO :"taxi"
SELECT pickup_day, pickup_zone_id, passenger_count, trip_distance,
       amount_cents, vendor_id, pickup_at, dropoff_at
FROM :"ext";

-- Статистика сразу после загрузки, чтобы планировщик видел реальные объёмы
-- в запросах шага 6. Итоговый ANALYZE всех таблиц — в шаге 8.
ANALYZE :"taxi";

-- Политика хранения и дистрибуции.
SELECT c.relname AS table_name, am.amname AS access_method,
       pg_get_table_distributedby(c.oid) AS distributed_by,
       array_to_string(c.reloptions, ', ') AS storage_options
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_am am ON am.oid = c.relam
WHERE n.nspname = :'schema' AND c.relname = :'taxi';

SELECT count(*) AS taxi_rows FROM :"taxi" \gset
\echo 'Строк во внутренней таблице:' :taxi_rows '| строк данных в CSV:' :expected_rows

SELECT :taxi_rows = :expected_rows AS ok \gset
\if :ok
\echo 'OK: count(*) внутренней таблицы совпадает с исходным файлом'
\else
DO $$BEGIN RAISE EXCEPTION 'Число строк во внутренней таблице не совпадает с файлом'; END$$;
\endif

-- Содержимое совпадает с внешней таблицей: разность множеств в обе стороны пуста.
SELECT count(*) AS diff_rows
FROM (
    (SELECT * FROM :"ext" EXCEPT ALL SELECT * FROM :"taxi")
    UNION ALL
    (SELECT * FROM :"taxi" EXCEPT ALL SELECT * FROM :"ext")
) d \gset
\echo 'Строк, различающихся между внешней и внутренней таблицей:' :diff_rows

SELECT :diff_rows = 0 AS ok \gset
\if :ok
\echo 'OK: данные внутренней таблицы совпадают с внешней построчно'
\else
DO $$BEGIN RAISE EXCEPTION 'Содержимое внутренней таблицы отличается от внешней'; END$$;
\endif
