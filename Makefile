.PHONY: build test lint seed up down clean

build:
	cd frontend && npm ci && npm run build
	cargo build --release

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

clean: ## Delete local Parquet data and manifest (resets server state for testing)
	rm -rf data/ manifest.db
