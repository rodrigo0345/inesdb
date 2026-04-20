#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
COMPOSE_FILE=${COMPOSE_FILE:-"${SCRIPT_DIR}/docker-compose.raft-lab.yml"}

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

psql_exec() {
  local host=$1
  local sql=$2
  compose exec -T psql psql "postgresql://postgres@${host}:5432/postgres?sslmode=disable" -v ON_ERROR_STOP=1 -At -c "$sql"
}

exec_with_retry() {
  local host=$1
  local sql=$2
  local max_tries=${3:-30}
  local sleep_secs=${4:-1}
  local i
  local err_file
  err_file=$(mktemp)
  for ((i=1; i<=max_tries; i++)); do
    if psql_exec "$host" "$sql" >/dev/null 2>"$err_file"; then
      rm -f "$err_file"
      return 0
    fi
    echo "[raft-lab] retry ${i}/${max_tries} host=${host} sql failed: $(cat "$err_file" 2>/dev/null)" >&2
    sleep "$sleep_secs"
  done
  echo "[raft-lab] command failed after ${max_tries} attempts host=${host} sql=${sql}" >&2
  rm -f "$err_file"
  return 1
}

wait_for_query_value() {
  local host=$1
  local sql=$2
  local expected=$3
  local max_tries=${4:-40}
  local sleep_secs=${5:-1}
  local i
  local actual
  for ((i=1; i<=max_tries; i++)); do
    actual=$(psql_exec "$host" "$sql" 2>/dev/null || true)
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep "$sleep_secs"
  done
  echo "[raft-lab] value check failed host=${host} expected=${expected} got=${actual}" >&2
  echo "[raft-lab] sql: ${sql}" >&2
  return 1
}

verify_value_on_all_nodes() {
  local key=$1
  local expected=$2
  wait_for_query_value omag1 "SELECT value FROM raft_lab WHERE id = '${key}';" "$expected"
  wait_for_query_value omag2 "SELECT value FROM raft_lab WHERE id = '${key}';" "$expected"
  wait_for_query_value omag3 "SELECT value FROM raft_lab WHERE id = '${key}';" "$expected"
}

write_and_verify_all_nodes() {
  local host=$1
  local key=$2
  local value=$3
  exec_with_retry "$host" "INSERT INTO raft_lab (id, value) VALUES ('${key}', ${value});"
  verify_value_on_all_nodes "$key" "$value"
}

wait_for_snapshot() {
  local host=$1
  local expected=$2
  local max_tries=${3:-40}
  local sleep_secs=${4:-1}
  local i
  local actual
  for ((i=1; i<=max_tries; i++)); do
    actual=$(psql_exec "$host" "SELECT id, value FROM raft_lab;" 2>/dev/null || true)
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep "$sleep_secs"
  done
  echo "[raft-lab] snapshot mismatch on ${host}" >&2
  echo "[raft-lab] expected:" >&2
  printf "%s\n" "$expected" >&2
  echo "[raft-lab] actual:" >&2
  printf "%s\n" "$actual" >&2
  return 1
}

wait_for_node() {
  local host=$1
  local max_tries=${2:-60}
  local i
  for ((i=1; i<=max_tries; i++)); do
    if compose exec -T psql pg_isready -h "$host" -p 5432 -U postgres >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "node ${host} did not become ready in time" >&2
  return 1
}

echo "[raft-lab] waiting for OMAG nodes..."
wait_for_node omag1
wait_for_node omag2
wait_for_node omag3

# Give replication listeners a moment after pgwire becomes reachable.
sleep 2

echo "[raft-lab] initializing table"
exec_with_retry omag1 "DROP TABLE IF EXISTS raft_lab;"
exec_with_retry omag2 "DROP TABLE IF EXISTS raft_lab;"
exec_with_retry omag3 "DROP TABLE IF EXISTS raft_lab;"

# Create schema only on the leader; follower writes below validate DDL propagation.
exec_with_retry omag1 "CREATE TABLE IF NOT EXISTS raft_lab (id TEXT PRIMARY KEY, value BIGINT);"

RUN_ID=$(date +%s)

echo "[raft-lab] running sequential writes and convergence checks"
write_and_verify_all_nodes omag1 "k1_${RUN_ID}" "100"
write_and_verify_all_nodes omag2 "k2_${RUN_ID}" "200"
write_and_verify_all_nodes omag3 "k3_${RUN_ID}" "300"

echo "[raft-lab] writing additional keys across nodes"
write_and_verify_all_nodes omag1 "n1_${RUN_ID}" "1"
write_and_verify_all_nodes omag2 "n2_${RUN_ID}" "2"
write_and_verify_all_nodes omag3 "n3_${RUN_ID}" "3"

echo "[raft-lab] validating final snapshots include run keys"
wait_for_query_value omag1 "SELECT value FROM raft_lab WHERE id = 'k1_${RUN_ID}';" "100"
wait_for_query_value omag2 "SELECT value FROM raft_lab WHERE id = 'k2_${RUN_ID}';" "200"
wait_for_query_value omag3 "SELECT value FROM raft_lab WHERE id = 'k3_${RUN_ID}';" "300"

echo "[raft-lab] PASS: sequential 3-node raft checks passed"
echo "[raft-lab]   - Each write source node was accepted"
echo "[raft-lab]   - Every written key converged on omag1, omag2, omag3"

