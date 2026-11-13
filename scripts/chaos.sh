#!/usr/bin/env bash
# Chaos test: run the pipeline with injected failures, kill processes while it
# works, then prove the end state is exactly right.
#
# Failures injected:
#   ingest     commits the event, then reports 503 (client retries -> duplicate)
#   relay      publishes to SNS, then "crashes" before marking the outbox row
#   consumers  commit their effect, then "crash" before deleting the message
#   loadgen    re-sends ~30% of events and publishes poison messages
#   this script SIGKILLs the relay and both consumers, and restarts Postgres
set -euo pipefail
cd "$(dirname "$0")/.."

EVENTS=${EVENTS:-1500}
RUN_ID=${RUN_ID:-chaos_$(date -u +%Y%m%dT%H%M%S)}
EXPECTED=bin/${RUN_ID}.json

export AWS_ENDPOINT_URL=http://localhost:55000 AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test

# Short timeouts so redeliveries and DLQ moves happen within seconds.
export VISIBILITY_TIMEOUT_SECONDS=2 MAX_RECEIVE_COUNT=5
export INGEST_CHAOS=ingest.fail_after_commit=0.2
export RELAY_CHAOS=relay.crash_after_publish=0.2
export LEDGER_CHAOS=ledger.crash_after_commit=0.2
export ANALYTICS_CHAOS=analytics.crash_after_commit=0.2

echo "==> starting a fresh stack with chaos enabled"
docker compose down -v --remove-orphans >/dev/null 2>&1
docker compose up -d --build --wait >/dev/null 2>&1 || docker compose up -d --build
mkdir -p bin && go build -o bin/ ./cmd/loadgen ./cmd/verify

echo "==> sending ${EVENTS} events (run ${RUN_ID})"
./bin/loadgen -events "$EVENTS" -concurrency 8 -rate 250 -run "$RUN_ID" -out "$EXPECTED" &
LOADGEN=$!

kill_and_restart() {
  sleep "$1"
  echo "==> SIGKILL ${*:2}"
  docker compose kill -s SIGKILL "${@:2}" >/dev/null 2>&1
  sleep 2
  docker compose start "${@:2}" >/dev/null 2>&1
}

kill_and_restart 1 relay ledger-consumer analytics-consumer
echo "==> restarting postgres under load"
docker compose restart postgres >/dev/null 2>&1
kill_and_restart 3 relay analytics-consumer
kill_and_restart 3 ledger-consumer

wait "$LOADGEN"

echo "==> waiting for the pipeline to settle, then checking invariants"
./bin/verify -expected "$EXPECTED" -timeout 4m
