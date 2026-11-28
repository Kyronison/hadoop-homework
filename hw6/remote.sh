#!/usr/bin/env bash
# Запуск ДЗ6 со своего компьютера: копирует скрипты на сервер курса по SSH,
# выполняет run.sh и сохраняет журнал в hw6/results/run.log.
#   bash hw6/remote.sh
# Параметры (переменные окружения):
#   TEAM      учётная запись команды (по умолчанию team40)
#   PREFIX    префикс таблиц (по умолчанию = TEAM)
#   HOST      адрес сервера (по умолчанию 171.22.73.82)
#   SSH_KEY   путь к ключу, если он не используется ssh по умолчанию
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEAM="${TEAM:-team40}"
PREFIX="${PREFIX:-$TEAM}"
HOST="${HOST:-171.22.73.82}"
LOG="$SCRIPT_DIR/results/run.log"

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=20)
[[ -n "${SSH_KEY:-}" ]] && SSH_OPTS+=(-i "$SSH_KEY")
TARGET="$TEAM@$HOST"

[[ "$TEAM" =~ ^team[0-9]{2}$ ]] || { echo "TEAM должен иметь вид teamNN" >&2; exit 1; }
[[ "$PREFIX" =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "Недопустимый PREFIX: $PREFIX" >&2; exit 1; }

echo "Копирование скриптов в $TARGET:~/hw6"
tar -C "$SCRIPT_DIR" -cf - run.sh sql \
    | ssh "${SSH_OPTS[@]}" "$TARGET" 'rm -rf ~/hw6 && mkdir -p ~/hw6 && tar -C ~/hw6 -xf -'

mkdir -p "$(dirname "$LOG")"
echo "Запуск run.sh на $HOST, журнал: $LOG"
ssh "${SSH_OPTS[@]}" "$TARGET" "PREFIX='$PREFIX' bash ~/hw6/run.sh" 2>&1 | tee "$LOG"
exit "${PIPESTATUS[0]}"
