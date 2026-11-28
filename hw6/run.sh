#!/usr/bin/env bash
# ДЗ6: полный цикл загрузки в Cloudberry/Greenplum через gpfdist.
# Запускается на сервере курса под учётной записью команды:
#   bash ~/hw6/run.sh
# Параметры (переменные окружения):
#   TEAM         учётная запись и схема команды (по умолчанию — текущий пользователь)
#   PREFIX       префикс таблиц (по умолчанию = TEAM; для индивидуальной сдачи team40_ivanov)
#   SOURCE_CSV   исходный файл (по умолчанию /srv/gpfdist/taxi_small.csv)
#   GPFDIST_HOST, GPFDIST_PORT, GPFDIST_ROOT — адрес и корневой каталог gpfdist
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/sql"

TEAM="${TEAM:-$(id -un)}"
PREFIX="${PREFIX:-$TEAM}"
SOURCE_CSV="${SOURCE_CSV:-/srv/gpfdist/taxi_small.csv}"
GPFDIST_HOST="${GPFDIST_HOST:-$(hostname -s)}"
GPFDIST_PORT="${GPFDIST_PORT:-8081}"
GPFDIST_ROOT="${GPFDIST_ROOT:-/srv/gpfdist}"

TEAM_DIR="$GPFDIST_ROOT/$TEAM"
CSV_NAME="${PREFIX}_taxi.csv"
CSV_PATH="$TEAM_DIR/$CSV_NAME"
GPFDIST_URL="gpfdist://$GPFDIST_HOST:$GPFDIST_PORT/$TEAM/$CSV_NAME"

log() { printf '\n[%s] %s\n' "$(date '+%F %T')" "$*"; }
fail() { printf 'ОШИБКА: %s\n' "$*" >&2; exit 1; }

[[ "$TEAM" =~ ^team[0-9]{2}$ ]] || fail "TEAM должен иметь вид teamNN, получено: $TEAM"
[[ "$PREFIX" =~ ^${TEAM}(_[a-z0-9_.-]+)?$ ]] || fail "PREFIX должен начинаться с $TEAM, получено: $PREFIX"
[[ -r "$SOURCE_CSV" ]] || fail "нет доступа к исходному файлу $SOURCE_CSV"
[[ -d "$TEAM_DIR" && -w "$TEAM_DIR" ]] || fail "нет каталога или права записи: $TEAM_DIR"

# Окружение Cloudberry (psql) и параметры подключения.
if ! command -v psql >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source /usr/cloudberry-db/cloudberry-env.sh
fi
export PGHOST="${PGHOST:-team-05-nn}" PGPORT="${PGPORT:-5432}" PGDATABASE="${PGDATABASE:-idp}" PGUSER="${PGUSER:-$TEAM}"
export PGCLIENTENCODING=UTF8 PGAPPNAME="${PREFIX}_hw6"

# CSV временный: удаляется и при успешном завершении, и при ошибке.
cleanup_csv() { rm -f -- "$CSV_PATH"; }
trap cleanup_csv EXIT

run_sql() {
    local file="$1"
    shift
    psql -X -q -v ON_ERROR_STOP=1 -P pager=off -P null='(null)' \
        -v team="$TEAM" -v schema="$TEAM" -v prefix="$PREFIX" \
        -v ext="${PREFIX}_ext" -v taxi="${PREFIX}_taxi" -v daily="${PREFIX}_daily" \
        "$@" -f "$SQL_DIR/$file"
}

log "Параметры: TEAM=$TEAM PREFIX=$PREFIX PGHOST=$PGHOST PGDATABASE=$PGDATABASE"
psql --version

# Шаг 1. Подключение.
log "Шаг 1. Подключение к $PGHOST:$PGPORT/$PGDATABASE"
PRIMARY_SEGMENTS="$(psql -X -At -v ON_ERROR_STOP=1 -c \
    "SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 AND role = 'p'")"
run_sql 01_connection.sql

# Шаг 2. Подготовка файла в каталоге команды.
log "Шаг 2. Копирование $SOURCE_CSV -> $CSV_PATH"
cp -- "$SOURCE_CSV" "$CSV_PATH"
chmod 0644 "$CSV_PATH"   # gpfdist работает от другого пользователя и читает файл как «прочие»
cmp -s "$SOURCE_CSV" "$CSV_PATH" || fail "копия CSV отличается от исходного файла"
CSV_BYTES="$(stat -c %s "$CSV_PATH")"
CSV_LINES="$(wc -l < "$CSV_PATH")"
EXPECTED_ROWS=$((CSV_LINES - 1))   # первая строка — заголовок
ls -l "$CSV_PATH"
head -n 3 "$CSV_PATH"
echo "Размер: $CSV_BYTES байт ($(numfmt --to=iec-i --suffix=B "$CSV_BYTES")); строк всего: $CSV_LINES, строк данных: $EXPECTED_ROWS"
echo "URL gpfdist: $GPFDIST_URL"

# Шаг 3. Внешняя таблица.
log "Шаг 3. Внешняя таблица ${PREFIX}_ext"
run_sql 02_external.sql -v gpfdist_url="$GPFDIST_URL" -v expected_rows="$EXPECTED_ROWS"

# Шаг 4. Внутренняя таблица.
log "Шаг 4. Внутренняя таблица ${PREFIX}_taxi"
run_sql 03_load.sql -v expected_rows="$EXPECTED_ROWS"

# Шаг 5. Распределение.
log "Шаг 5. Проверка распределения по сегментам"
run_sql 04_distribution.sql -v primary_segments="$PRIMARY_SEGMENTS"

# Шаг 6. Аналитические запросы.
log "Шаг 6. Аналитические запросы"
run_sql 05_queries.sql

# Шаг 7. Витрина.
log "Шаг 7. Витрина ${PREFIX}_daily"
run_sql 06_daily.sql

# Шаг 8. ANALYZE, удаление внешней таблицы и CSV.
log "Шаг 8. ANALYZE и уборка"
run_sql 07_analyze_cleanup.sql
cleanup_csv
[[ ! -e "$CSV_PATH" ]] || fail "CSV не удалён: $CSV_PATH"
echo "OK: CSV удалён: $CSV_PATH"
echo "Содержимое $TEAM_DIR:"
ls -la "$TEAM_DIR"

# Самопроверка.
log "Самопроверка"
run_sql 08_selfcheck.sql -v expected_rows="$EXPECTED_ROWS" -v csv_bytes="$CSV_BYTES"

log "Готово: все шаги выполнены, проверки пройдены"
