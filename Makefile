.PHONY: help setup-env dev stop api etl lint type-check test test-api test-etl test-integration-api test-integration-etl test-integration check pre-commit neutrality seed clean download-cnpj download-cnpj-all download-tse download-transparencia download-sanctions download-all etl-cnpj etl-cnpj-dev etl-cnpj-stream etl-tse etl-tse-dev etl-transparencia etl-transparencia-dev etl-sanctions etl-all link-persons bootstrap-demo bootstrap-full bootstrap-all bootstrap-all-noninteractive bootstrap-all-report bootstrap-go bootstrap-go-noninteractive bootstrap-go-report check-public-claims check-source-urls check-pipeline-contracts check-pipeline-inputs check-bootstrap-contract generate-pipeline-status generate-source-summary generate-reference-metrics

# Default target when running `make` with no arguments.
.DEFAULT_GOAL := help

help:
	@echo "Common targets (run 'grep -E \"^[a-z_-]+:\" Makefile' for full list):"
	@echo ""
	@echo "  Setup & run"
	@echo "    setup-env        Generate .env from .env.example + secure secrets"
	@echo "    dev              docker compose up -d (core stack)"
	@echo "    stop             docker compose down"
	@echo "    seed             Seed Neo4j with dev fixtures"
	@echo ""
	@echo "  Per-module dev servers"
	@echo "    api              Run API with --reload on :8000"
	@echo "    etl              Show bracc-etl CLI help"
	@echo ""
	@echo "  Quality gates (what CI runs)"
	@echo "    check            lint + type-check + tests (api + etl)"
	@echo "    pre-commit       check + neutrality + registry/docs governance"
	@echo "    neutrality       Ban-list check on source text"
	@echo "    check-public-claims / -pipeline-contracts / -pipeline-inputs"
	@echo "                     Registry / docs governance scripts"
	@echo "    check-bootstrap-contract"
	@echo "                     bootstrap-all contract vs registry parity"
	@echo ""
	@echo "  Data loading"
	@echo "    bootstrap-demo   Small deterministic synthetic graph"
	@echo "    bootstrap-all    Heavy real-ingest orchestration (hours)"
	@echo "    bootstrap-go     Goias-scoped real-ingest orchestration (subset of bootstrap-all)"
	@echo "    download-<src>   Fetch raw files for one source"
	@echo "    etl-<src>        Run one ETL pipeline against Neo4j"
	@echo ""
	@echo "See CONTRIBUTING.md for the pre-PR workflow."

# ── Development ─────────────────────────────────────────
setup-env:
	bash scripts/init_env.sh

dev:
	docker compose up -d

stop:
	docker compose down

# ── API ─────────────────────────────────────────────────
api:
	cd api && uv run uvicorn bracc.main:app --reload --host 0.0.0.0 --port 8000

# ── ETL ─────────────────────────────────────────────────
etl:
	cd etl && uv run bracc-etl --help

seed:
	bash infra/scripts/seed-dev.sh

# ── CNPJ Data ──────────────────────────────────────────
download-cnpj:
	cd etl && uv run python scripts/download_cnpj.py --reference-only
	cd etl && uv run python scripts/download_cnpj.py --files 1

download-cnpj-all:
	cd etl && uv run python scripts/download_cnpj.py --files 10

etl-cnpj:
	cd etl && uv run bracc-etl run --source cnpj --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-cnpj-dev:
	cd etl && uv run bracc-etl run --source cnpj --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --limit 10000

etl-cnpj-stream:
	cd etl && uv run bracc-etl run --source cnpj --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming

# ── TSE Data ──────────────────────────────────────────
download-tse:
	cd etl && uv run python scripts/download_tse.py --years 2024

etl-tse:
	cd etl && uv run bracc-etl run --source tse --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-tse-dev:
	cd etl && uv run bracc-etl run --source tse --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --limit 10000

# ── Transparencia Data ────────────────────────────────
download-transparencia:
	cd etl && uv run python scripts/download_transparencia.py --year 2025

etl-transparencia:
	cd etl && uv run bracc-etl run --source transparencia --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-transparencia-dev:
	cd etl && uv run bracc-etl run --source transparencia --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --limit 10000

# ── Sanctions Data ────────────────────────────────────
download-sanctions:
	cd etl && uv run python scripts/download_sanctions.py

etl-sanctions:
	cd etl && uv run bracc-etl run --source sanctions --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

# ── All Data ──────────────────────────────────────────
download-all: download-cnpj download-tse download-transparencia download-sanctions

etl-all: etl-cnpj etl-tse etl-transparencia etl-sanctions

# ── Entity Resolution ────────────────────────────────────
link-persons:
	docker compose exec neo4j cypher-shell -u neo4j -p "$${NEO4J_PASSWORD}" -f /scripts/link_persons.cypher

# ── Quality ─────────────────────────────────────────────
lint:
	cd api && uv run ruff check src/ tests/
	cd etl && uv run ruff check src/ tests/

type-check:
	cd api && uv run mypy src/ tests/
	cd etl && uv run mypy src/ tests/

test-api:
	cd api && uv run pytest

test-etl:
	cd etl && uv run pytest

test: test-api test-etl

# ── Integration tests ─────────────────────────────────
test-integration-api:
	cd api && uv run pytest -m integration

test-integration-etl:
	cd etl && uv run pytest -m integration

test-integration: test-integration-api test-integration-etl

# ── Full check (run before commit) ─────────────────────
check: lint type-check test
	@echo "All checks passed."

# Mirror of what CI runs on every PR: lint + type + tests + neutrality +
# registry/docs governance. Use this before opening a PR to avoid green-
# local / red-CI surprises.
pre-commit: check neutrality check-public-claims check-pipeline-contracts check-pipeline-inputs check-bootstrap-contract
	@echo "Pre-commit bundle passed (lint/type/test + neutrality + governance)."

# ── Neutrality audit ───────────────────────────────────
neutrality:
	@! grep -rn \
		"suspicious\|corrupt\|criminal\|fraudulent\|illegal\|guilty\|CRITICAL\|HIGH.*severity\|MEDIUM.*severity\|LOW.*severity" \
		api/src/ etl/src/ \
		--include="*.py" --include="*.json" \
		|| (echo "NEUTRALITY VIOLATION FOUND" && exit 1)
	@echo "Neutrality check passed."

# ── Bootstrap ─────────────────────────────────────────────
bootstrap-demo:
	bash scripts/bootstrap_public_demo.sh --profile demo

bootstrap-full:
	bash scripts/bootstrap_public_demo.sh --profile full

bootstrap-all:
	bash scripts/bootstrap_all_public.sh

bootstrap-all-noninteractive:
	bash scripts/bootstrap_all_public.sh --noninteractive --yes-reset

bootstrap-all-report:
	python3 scripts/run_bootstrap_all.py --repo-root . --report-latest

bootstrap-go:
	bash scripts/bootstrap_go_public.sh

bootstrap-go-noninteractive:
	bash scripts/bootstrap_go_public.sh --noninteractive --yes-reset

bootstrap-go-report:
	python3 scripts/run_bootstrap_all.py --repo-root . --output-label bootstrap-go --report-latest

# ── Quality checks ────────────────────────────────────────
check-public-claims:
	python3 scripts/check_public_claims.py --repo-root .

check-source-urls:
	python3 scripts/check_source_urls.py --registry-path docs/source_registry_br_v1.csv --exceptions-path config/source_url_exceptions.yml --output audit-results/public-trust/latest/source-url-audit.json

check-pipeline-contracts:
	python3 scripts/check_pipeline_contracts.py

check-pipeline-inputs:
	python3 scripts/check_pipeline_inputs.py

check-bootstrap-contract:
	python3 scripts/check_bootstrap_contract.py

# ── Generators ────────────────────────────────────────────
generate-pipeline-status:
	python3 scripts/generate_pipeline_status.py

generate-source-summary:
	python3 scripts/generate_data_sources_summary.py

generate-reference-metrics:
	python3 scripts/generate_reference_metrics.py

# ── Cleanup ─────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
