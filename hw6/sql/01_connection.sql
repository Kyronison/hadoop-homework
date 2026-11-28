-- Шаг 1. Подключение: проверяем пользователя, базу, версию и сегменты кластера.
-- Переменные psql: team (учётная запись и схема команды).

\echo '=== Шаг 1. Подключение ==='

SELECT current_user, current_database(), inet_server_addr() AS coordinator_ip,
       inet_server_port() AS coordinator_port;

SELECT version();

SELECT content, role, preferred_role, mode, status, hostname, port
FROM gp_segment_configuration
ORDER BY content, role;

SELECT current_user = :'team' AND current_database() = 'idp' AS ok \gset
\if :ok
\echo 'OK: current_user =' :team '| current_database = idp'
\else
DO $$BEGIN RAISE EXCEPTION 'Неверный пользователь или база: ожидались учётная запись команды и БД idp'; END$$;
\endif

SELECT count(*) AS primary_segments
FROM gp_segment_configuration
WHERE content >= 0 AND role = 'p' AND status = 'u' \gset
\echo 'Рабочих primary-сегментов:' :primary_segments

SELECT has_schema_privilege(:'team', 'CREATE') AS ok \gset
\if :ok
\echo 'OK: есть право CREATE в схеме' :team
\else
DO $$BEGIN RAISE EXCEPTION 'Нет права CREATE в схеме команды'; END$$;
\endif
