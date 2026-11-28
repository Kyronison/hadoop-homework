-- Шаг 6. Аналитические запросы к своей таблице, в том числе JOIN со справочником зон.
-- Переменные psql: schema, taxi.

\echo '=== Шаг 6. Аналитические запросы ==='

SET search_path TO :"schema";

\echo '--- 6.1. Топ-10 зон посадки (JOIN с public.taxi_zones) ---'
SELECT z.borough,
       z.zone,
       count(*) AS trips,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS trips_pct,
       round(sum(t.amount_cents) / 100.0, 2) AS revenue_usd,
       round(avg(t.amount_cents) / 100.0, 2) AS avg_check_usd,
       round(avg(t.trip_distance)::numeric, 2) AS avg_distance_mi
FROM :"taxi" t
JOIN public.taxi_zones z ON z.location_id = t.pickup_zone_id
GROUP BY t.pickup_zone_id, z.borough, z.zone
ORDER BY trips DESC, z.zone
LIMIT 10;

\echo '--- 6.2. Поездки и выручка по районам (JOIN с public.taxi_zones) ---'
-- У зоны 265 «Outside of NYC» район не указан, вместо NULL выводим название зоны.
SELECT coalesce(z.borough, z.zone) AS borough,
       count(*) AS trips,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS trips_pct,
       round(sum(t.amount_cents) / 100.0, 2) AS revenue_usd,
       round(avg(t.amount_cents) / 100.0, 2) AS avg_check_usd,
       round(avg(t.trip_distance)::numeric, 2) AS avg_distance_mi,
       round(avg(extract(epoch FROM t.dropoff_at - t.pickup_at) / 60)::numeric, 1) AS avg_duration_min
FROM :"taxi" t
JOIN public.taxi_zones z ON z.location_id = t.pickup_zone_id
GROUP BY 1
ORDER BY trips DESC;

\echo '--- 6.3. Профиль по часу посадки ---'
SELECT extract(hour FROM pickup_at)::int AS pickup_hour,
       count(*) AS trips,
       round(sum(amount_cents) / 100.0, 2) AS revenue_usd,
       round(avg(amount_cents) / 100.0, 2) AS avg_check_usd,
       round(avg(trip_distance)::numeric, 2) AS avg_distance_mi
FROM :"taxi"
GROUP BY 1
ORDER BY 1;

\echo '--- 6.4. Качество исходных данных ---'
SELECT count(*) AS total_trips,
       count(*) FILTER (WHERE pickup_day NOT BETWEEN DATE '2024-01-01' AND DATE '2024-01-31')
           AS outside_january_2024,
       count(*) FILTER (WHERE amount_cents < 0) AS negative_amount,
       count(*) FILTER (WHERE trip_distance = 0) AS zero_distance,
       count(*) FILTER (WHERE dropoff_at < pickup_at) AS dropoff_before_pickup,
       min(pickup_day) AS min_day,
       max(pickup_day) AS max_day
FROM :"taxi";
