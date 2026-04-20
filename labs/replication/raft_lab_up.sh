#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
COMPOSE_FILE=${COMPOSE_FILE:-"${SCRIPT_DIR}/docker-compose.raft-lab.yml"}
KEEP_RUNNING=${KEEP_RUNNING:-0}

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  if [[ "$KEEP_RUNNING" == "1" ]]; then
    echo "[raft-lab] KEEP_RUNNING=1, leaving cluster running"
    return
  fi
  echo "[raft-lab] stopping cluster"
  compose down -v
}

trap cleanup EXIT

echo "[raft-lab] starting 3-node OMAG raft cluster"
compose up -d --build omag1 omag2 omag3 psql

"${SCRIPT_DIR}/raft_lab_check.sh"

