-- Шаг 7. Витрина: агрегат по дням через CREATE TABLE ... AS SELECT ... GROUP BY.
-- Переменные psql: schema, taxi, daily.

\echo '=== Шаг 7. Витрина по дням ==='

SET search_path TO :"schema";

DROP TABLE IF EXISTS :"daily";

-- Одна строка на день; выручка — сумма amount_cents в долларах (возвраты с
-- отрицательной суммой уменьшают выручку), дистанция — в милях.
CREATE TABLE :"daily" AS
SELECT pickup_day                                     AS trip_day,
       count(*)                                       AS trips,
       round(sum(amount_cents) / 100.0, 2)            AS revenue_usd,
       round(avg(trip_distance)::numeric, 3)          AS avg_distance_mi
FROM :"taxi"
GROUP BY pickup_day
DISTRIBUTED BY (trip_day);

SELECT * FROM :"daily" ORDER BY trip_day;

-- Витрина согласована с детальной таблицей: суммы поездок и выручки совпадают.
SELECT (SELECT sum(trips) FROM :"daily") AS daily_trips,
       (SELECT count(*) FROM :"taxi") AS taxi_trips,
       (SELECT sum(revenue_usd) FROM :"daily") AS daily_revenue,
       (SELECT round(sum(amount_cents) / 100.0, 2) FROM :"taxi") AS taxi_revenue,
       (SELECT count(*) FROM :"daily") AS daily_rows \gset
\echo 'Витрина:' :daily_rows 'дней,' :daily_trips 'поездок,' :daily_revenue 'USD'
\echo 'Детальная таблица:' :taxi_trips 'поездок,' :taxi_revenue 'USD'

SELECT :daily_trips = :taxi_trips AND :daily_revenue = :taxi_revenue AS ok \gset
\if :ok
\echo 'OK: итоги витрины совпадают с детальной таблицей'
\else
DO $$BEGIN RAISE EXCEPTION 'Итоги витрины не совпадают с детальной таблицей'; END$$;
\endif
