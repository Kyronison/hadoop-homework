-- Шаг 8. Статистика и уборка: ANALYZE своих таблиц, удаление внешней таблицы.
-- CSV удаляет run.sh сразу после этого шага.
-- Переменные psql: schema, ext, taxi, daily.

\echo '=== Шаг 8. ANALYZE и удаление внешней таблицы ==='

SET search_path TO :"schema";

ANALYZE :"taxi";
ANALYZE :"daily";

-- После ANALYZE у планировщика есть число строк и статистика по столбцам.
SELECT c.relname AS table_name, c.reltuples::bigint AS planner_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = :'schema' AND c.relname IN (:'taxi', :'daily')
ORDER BY c.relname;

SELECT tablename, count(*) AS columns_with_stats
FROM pg_stats
WHERE schemaname = :'schema' AND tablename IN (:'taxi', :'daily')
GROUP BY tablename
ORDER BY tablename;

DROP EXTERNAL TABLE :"ext";

SELECT count(*) = 0 AS ok
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = :'schema' AND c.relname = :'ext' \gset
\if :ok
\echo 'OK: внешняя таблица' :ext 'удалена'
\else
DO $$BEGIN RAISE EXCEPTION 'Внешняя таблица не удалена'; END$$;
\endif
