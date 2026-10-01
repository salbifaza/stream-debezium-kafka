.PHONY: help up down reset logs register gold verify status smoke

help:
	@echo "Usage:"
	@echo "  make up              Start all services and register connectors"
	@echo "  make down            Stop and remove containers (keeps data)"
	@echo "  make reset           Stop, wipe all volumes, and rebuild from scratch"
	@echo "  make logs            Tail all service logs"
	@echo ""
	@echo "  make register        Register source + sink connectors (idempotent)"
	@echo "  make gold            Deploy the Flink gold layer: topics, ClickHouse tables, job (idempotent)"
	@echo "  make verify          Row counts, live insert/update/delete, gold vs. Postgres check"
	@echo "  make status          Connector status, consumer lag, WAL retention, row counts"
	@echo "  make smoke           Full end-to-end smoke test (register + gold + verify)"

# ── infrastructure ────────────────────────────────────────────────────────────

up:
	cp -n .env.example .env 2>/dev/null || true
	docker compose up -d --build
	@echo "Waiting for Kafka Connect to become healthy..."
	@./scripts/register_connectors.sh
	@./scripts/deploy_gold.sh
	@echo ""
	@echo "Stack ready."
	@echo "  kafka-ui: http://localhost:8086"
	@echo "  Flink UI: http://localhost:8088"
	@echo "  Kafka Connect REST: http://localhost:8087"
	@echo "  Source Postgres: localhost:5433"
	@echo "  ClickHouse HTTP: localhost:8124"
	@echo ""
	@echo "Run 'make verify' to test the full CDC pipeline."

down:
	docker compose down

reset:
	docker compose down -v
	@echo "All volumes wiped. Run 'make up' to rebuild from scratch."

logs:
	docker compose logs -f

# ── connectors ────────────────────────────────────────────────────────────────

register:
	./scripts/register_connectors.sh

gold:
	./scripts/deploy_gold.sh

verify:
	./scripts/verify_cdc.sh

status:
	./scripts/connector_status.sh

# ── testing ───────────────────────────────────────────────────────────────────

smoke: register gold verify
