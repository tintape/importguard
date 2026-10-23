#!/usr/bin/env bash
# Smoke test for the Docker Compose stack: upload demo files over HTTP and check the outcomes.
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
DATA="${DATA:-demo-data}"

upload() { # file source_id -> job id
  curl -fsS -o /dev/null -w '%{redirect_url}' -F "source_id=$2" -F "file=@$DATA/$1" "$BASE_URL/imports" | sed 's#.*/##'
}

wait_for() { # job id -> final status
  for _ in $(seq 1 300); do
    status=$(curl -fsS "$BASE_URL/imports/$1/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
    if [[ "$status" == succeeded || "$status" == failed ]]; then echo "$status"; return; fi
    sleep 0.5
  done
  echo timeout
}

expect() { # file source expected_status
  job=$(upload "$1" "$2")
  status=$(wait_for "$job")
  echo "import #$job $1 -> $status"
  [[ "$status" == "$3" ]] || { echo "expected $3"; exit 1; }
}

expect "01_pos_2026-08-31_valid.csv" 1 succeeded
expect "02_pos_2026-08-31_valid (copy).csv" 1 succeeded
expect "03_pos_2026-09-01_overlapping.csv" 1 succeeded
expect "04_pos_2026-09-02_conflicting_amount.csv" 1 failed
expect "10_marketplace_large_100k.parquet" 3 succeeded

total=$(curl -fsS "$BASE_URL/sales" | grep -o 'Export [0-9,]* row' | tr -dc '0-9')
echo "sales: $total"
[[ "$total" == "101700" ]] || { echo "expected 101700 sales"; exit 1; }

# Crash recovery inside Docker: kill the worker container mid-import, restart it.
job=$(upload "10_marketplace_large_100k.csv" 3)
for _ in $(seq 1 100); do
  [[ $(curl -fsS "$BASE_URL/imports/$job/status") == *processing* ]] && break
  sleep 0.1
done
docker compose kill -s SIGKILL worker
docker compose up --detach worker
status=$(wait_for "$job")
echo "import #$job after worker kill -> $status"
[[ "$status" == succeeded ]]
total=$(curl -fsS "$BASE_URL/sales" | grep -o 'Export [0-9,]* row' | tr -dc '0-9')
[[ "$total" == "101700" ]] || { echo "duplicates after recovery: $total"; exit 1; }
echo "compose smoke test passed"
