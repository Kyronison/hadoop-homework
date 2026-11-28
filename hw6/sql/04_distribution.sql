-- Шаг 5. Проверка распределения строк по сегментам и обоснование ключа.
-- Переменные psql: schema, taxi, prefix, primary_segments.

\echo '=== Шаг 5. Распределение по сегментам ==='

SET search_path TO :"schema";

-- Сколько строк хранит каждый сегмент (gp_segment_id — служебный столбец строки).
SELECT gp_segment_id,
       count(*) AS rows,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct,
       count(DISTINCT pickup_zone_id) AS zones
FROM :"taxi"
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- Перекос: отношение самого большого сегмента к самому маленькому.
WITH per_segment AS (
    SELECT gp_segment_id, count(*) AS rows FROM :"taxi" GROUP BY gp_segment_id
)
SELECT count(*) AS segments_with_data,
       min(rows) AS min_rows,
       max(rows) AS max_rows,
       round(max(rows)::numeric / min(rows), 3) AS skew_ratio,
       round(stddev_pop(rows) / avg(rows), 3) AS skew_coefficient
FROM per_segment \gset

\echo 'Сегментов с данными:' :segments_with_data 'из' :primary_segments
\echo 'Строк на сегмент: min =' :min_rows '| max =' :max_rows
\echo 'Перекос max/min =' :skew_ratio '| коэффициент вариации =' :skew_coefficient

SELECT :segments_with_data = :primary_segments AND :skew_ratio <= 2 AS ok \gset
\if :ok
\echo 'OK: строки есть на всех сегментах, перекос не больше 2x'
\else
DO $$BEGIN RAISE EXCEPTION 'Строки распределены не по всем сегментам или перекос больше 2x'; END$$;
\endif

-- Кардинальность столбцов-кандидатов в ключ дистрибуции.
SELECT count(DISTINCT pickup_zone_id) AS pickup_zone_id,
       count(DISTINCT pickup_day) AS pickup_day,
       count(DISTINCT vendor_id) AS vendor_id,
       count(DISTINCT pickup_at) AS pickup_at
FROM :"taxi";

-- Сравнение кандидатов на практике: временные копии одного столбца с разными ключами.
-- Таблицы создаются в своей схеме с префиксом команды и сразу удаляются.
\set key_day :prefix '_keytest_day'
\set key_vendor :prefix '_keytest_vendor'
\set key_random :prefix '_keytest_random'

DROP TABLE IF EXISTS :"key_day", :"key_vendor", :"key_random";
CREATE TABLE :"key_day" AS SELECT pickup_day FROM :"taxi" DISTRIBUTED BY (pickup_day);
CREATE TABLE :"key_vendor" AS SELECT vendor_id FROM :"taxi" DISTRIBUTED BY (vendor_id);
CREATE TABLE :"key_random" AS SELECT pickup_zone_id FROM :"taxi" DISTRIBUTED RANDOMLY;

WITH segments AS (
    SELECT content AS gp_segment_id
    FROM gp_segment_configuration
    WHERE content >= 0 AND role = 'p'
),
counts AS (
    SELECT '1. pickup_zone_id (выбран)' AS distribution_key, gp_segment_id, count(*) AS rows
    FROM :"taxi" GROUP BY gp_segment_id
    UNION ALL
    SELECT '2. pickup_day', gp_segment_id, count(*) FROM :"key_day" GROUP BY gp_segment_id
    UNION ALL
    SELECT '3. vendor_id', gp_segment_id, count(*) FROM :"key_vendor" GROUP BY gp_segment_id
    UNION ALL
    SELECT '4. RANDOMLY', gp_segment_id, count(*) FROM :"key_random" GROUP BY gp_segment_id
),
grid AS (
    SELECT k.distribution_key, s.gp_segment_id, coalesce(c.rows, 0) AS rows
    FROM (SELECT DISTINCT distribution_key FROM counts) k
    CROSS JOIN segments s
    LEFT JOIN counts c USING (distribution_key, gp_segment_id)
)
SELECT distribution_key,
       string_agg(rows::text, ' / ' ORDER BY gp_segment_id) AS rows_per_segment,
       CASE WHEN min(rows) = 0 THEN 'пустой сегмент'
            ELSE round(max(rows)::numeric / min(rows), 3)::text END AS skew_ratio
FROM grid
GROUP BY distribution_key
ORDER BY distribution_key;

-- Сравнение планов JOIN: справочник public.taxi_zones распределён по location_id.
ANALYZE :"key_random";

\echo '--- План JOIN со справочником при ключе pickup_zone_id ---'
\echo '(соединение выполняется на сегментах без перемещения строк, есть только Gather Motion результата)'
EXPLAIN
SELECT t.pickup_zone_id, z.zone
FROM :"taxi" t
JOIN public.taxi_zones z ON z.location_id = t.pickup_zone_id;

\echo '--- Тот же JOIN при DISTRIBUTED RANDOMLY ---'
\echo '(строки зоны лежат на разных сегментах, поэтому справочник рассылается на все сегменты: Broadcast Motion)'
EXPLAIN
SELECT t.pickup_zone_id, z.zone
FROM :"key_random" t
JOIN public.taxi_zones z ON z.location_id = t.pickup_zone_id;

DROP TABLE :"key_day", :"key_vendor", :"key_random";
