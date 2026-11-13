.DEFAULT_GOAL := help
SHELL := /bin/bash

# Point the AWS SDKs at the local emulator for loadgen/verify.
LOCAL_AWS := AWS_ENDPOINT_URL=http://localhost:55000 AWS_REGION=us-east-1 \
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test

.PHONY: help
help: ## Show this help
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: ## Start the whole stack (Postgres, Redis, Moto, all services)
	docker compose up -d --build --wait

.PHONY: down
down: ## Stop the stack and delete its data
	docker compose down -v --remove-orphans

.PHONY: logs
logs: ## Follow service logs
	docker compose logs -f ingest relay ledger-consumer analytics-consumer

.PHONY: test
test: ## Unit tests (Go and Python), no Docker needed
	go test -race ./...
	cd analytics && uv run pytest -q

.PHONY: test-integration
test-integration: ## Postgres integration tests (starts Postgres)
	docker compose up -d --wait postgres
	docker compose stop relay 2>/dev/null || true
	go test -race -tags integration -count=1 ./internal/store/

.PHONY: lint
lint: ## go vet, gofmt and ruff
	go vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	cd analytics && uv run ruff check . && uv run ruff format --check .

.PHONY: load
load: up ## Send 1,000 events (no chaos) and verify the result
	@mkdir -p bin && go build -o bin/ ./cmd/loadgen ./cmd/verify
	$(LOCAL_AWS) ./bin/loadgen -events 1000 -poison 0 -out bin/load.json
	$(LOCAL_AWS) ./bin/verify -expected bin/load.json

.PHONY: chaos
chaos: ## Chaos test: inject failures, kill processes, verify exactly-once effects
	./scripts/chaos.sh

.PHONY: send
send: ## Send one sample event with curl
	curl -s -XPOST localhost:58080/v1/events -H 'Content-Type: application/json' -d \
	'{"id":"evt_demo_1","type":"payment.captured","account_id":"acct_demo","amount_cents":2500,"currency":"USD","occurred_at":"2026-11-13T10:00:00Z"}'; echo
