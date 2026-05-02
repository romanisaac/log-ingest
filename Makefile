.PHONY: build test lint seed up down

build:
	cargo build

test:
	cargo test

lint:
	cargo clippy -- -D warnings

up:
	docker compose up -d

down:
	docker compose down

seed: ## Publish 1000 synthetic log events to Redpanda
	@echo "Creating 'logs' topic..."
	@docker compose exec redpanda rpk topic create logs --partitions 4 2>/dev/null || true
	@cargo run --bin seed
