-- Самопроверка перед сдачей: итоговые числа для инструкции.
-- Переменные psql: schema, prefix, taxi, daily, expected_rows, csv_bytes.

\echo '=== Самопроверка ==='

SET search_path TO :"schema";

\echo '--- Таблицы команды в схеме' :schema '---'
SELECT c.relname AS table_name,
       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' ELSE c.relkind::text END AS kind,
       am.amname AS access_method,
       pg_get_table_distributedby(c.oid) AS distributed_by,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_am am ON am.oid = c.relam
WHERE n.nspname = :'schema' AND c.relkind IN ('r', 'v', 'm', 'p')
  AND c.relname LIKE :'prefix' || '%'
ORDER BY c.relname;

\echo '--- Строк на сегмент ---'
SELECT gp_segment_id, count(*) AS rows,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM :"taxi"
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

\echo '--- Размер таблицы на каждом сегменте ---'
SELECT gp_segment_id,
       pg_size_pretty(pg_total_relation_size(format('%I.%I', :'schema', :'taxi')::regclass)) AS taxi_size
FROM gp_dist_random('gp_id')
ORDER BY gp_segment_id;

\echo '--- Итог ---'
SELECT :expected_rows::bigint AS csv_rows,
       (SELECT count(*) FROM :"taxi") AS taxi_rows,
       (SELECT count(*) FROM :"taxi") = :expected_rows AS rows_match,
       (SELECT count(*) FROM :"daily") AS daily_rows,
       pg_size_pretty(:csv_bytes::bigint) AS raw_csv_size,
       pg_size_pretty(pg_total_relation_size(format('%I.%I', :'schema', :'taxi')::regclass)) AS taxi_size_in_db,
       pg_size_pretty(pg_total_relation_size(format('%I.%I', :'schema', :'daily')::regclass)) AS daily_size_in_db,
       pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS team_tables_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = :'schema' AND c.relkind = 'r';

SELECT pg_total_relation_size(format('%I.%I', :'schema', :'taxi')::regclass) AS taxi_bytes,
       (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :'schema' AND c.relkind = 'f') AS external_tables \gset
\echo 'Сырые данные:' :csv_bytes 'байт; таблица в БД:' :taxi_bytes 'байт; внешних таблиц в схеме:' :external_tables

SELECT :external_tables = 0 AS ok \gset
\if :ok
\echo 'OK: внешних таблиц в схеме' :schema 'не осталось'
\else
DO $$BEGIN RAISE EXCEPTION 'В схеме команды остались внешние таблицы'; END$$;
\endif
