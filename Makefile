.PHONY: all clean test test-unit test-connectors test-intents test-integration test-all test-ci test-coverage crap crap-fresh crap-diff crap-diff-fresh test-nightly-visual test-gateway test-backtest-service test-demo-strategies test-demo-quick test-demo-single test-accounting-matrix test-accounting-matrix-quick list-demo-strategies check-pendle-expiry set-almanak-code-version build-platform-wheels build publish lint lint-check format format-check security docs docs-cli docs-serve docs-clean install install-dev version-bump-patch version-bump-minor version-bump-major version-undo update-setup-version proto proto-check gateway dashboard dashboard-only anvil-dev typecheck typecheck-report docker-workstation-build docker-workstation-run docker-workstation-exec docker-workstation-stop audit-intent-paths check-xfail-hygiene check-config-boundary check-connector-registry check-connector-chains check-intent-coverage check-deployment-scoped-tables check-deployment-id-proto-surface

# Load .env file if it exists
-include .env
export

# Default target
all: install lint

# Run linting with auto-fix (local development)
lint:
	uv run ruff check almanak --fix
	uv run ruff format almanak

# Run linting without auto-fix - fails on errors (CI)
lint-check:
	uv run ruff check almanak
	uv run ruff format almanak --check

# Format code
format:
	uv run ruff format almanak

# Check formatting without changes (CI)
format-check:
	uv run ruff format almanak --check

# Run mypy type checks (CI)
typecheck:
	uv run mypy almanak

# Run mypy and generate a summary report (local development)
typecheck-report:
	uv run mypy almanak --no-error-summary 2>&1 | sort | uniq -c | sort -rn > mypy-report.txt
	@echo "Report written to mypy-report.txt"
	@uv run mypy almanak --error-summary-only 2>&1 || true

# Audit the intent-test path filter against actual transitive imports.
# Fails when the filter is missing a reachable import or contains a stale entry.
audit-intent-paths:
	uv run python scripts/ci/audit_intent_test_paths.py

# Enforce xfail hygiene: every @pytest.mark.xfail under tests/intents/ must
# carry a ticket ref, a dated reason, and an explicit strict=. See issue #1694.
check-xfail-hygiene:
	uv run python scripts/ci/check_xfail_hygiene.py --check --verbose

# Enforce the config-service boundary (issues #2097-#2101): no direct
# os.environ / load_dotenv reads outside almanak/config/ + a small allowlist.
# See docs/internal/config-service-plan.md.
check-config-boundary:
	uv run python scripts/ci/check_config_boundary.py --check --verbose

# Enforce that every connector dir under almanak/framework/connectors/
# registers itself in ConnectorRegistry (VIB-4298 PR 1). The registry is the
# source of truth for the (connector, intent, chain) universe consumed by
# PR 2's intent-test coverage gate and future tooling.
check-connector-registry:
	uv run python scripts/ci/check_connector_registry.py --verbose

# Intent-coverage gate (VIB-4298 PR 2 / VIB-4303). Two-in-one:
#   1. Marker hygiene — every test_* under tests/intents/ must carry
#      @pytest.mark.intent(IntentType.X, ...). Always enforced.
#   2. Coverage gap — every (connector, intent, chain) in ConnectorRegistry
#      must have an intent test OR a structural entry in
#      scripts/ci/intent-coverage-excused.yml. Runs --warn-only by default;
#      a follow-up PR flips to --enforce after the writable backlog clears
#      (~150 triples as of 2026-05-12).
check-intent-coverage:
	uv run python scripts/ci/check_intent_coverage.py --verbose

# Deployment-scoped table conformance gate (VIB-4722 / blueprint 29 §3).
# Asserts every table in schema/deployment_scoped_tables.yaml carries the
# single canonical `deployment_id` identity column in the SDK SQLite schema.
check-deployment-scoped-tables:
	uv run python scripts/ci/check_deployment_scoped_tables.py --verbose

check-deployment-id-proto-surface:
	uv run python scripts/ci/check_deployment_id_proto_surface.py

# Connector chain-support validator (VIB-4802 / epic VIB-4800). Refuses
# connector code (and strategy decorators) that key off a chain
# identifier the canonical chain resolver does not recognize. The
# allowlist for legitimate cross-chain bridge targets lives at
# scripts/ci/connector-chain-allowlist.yml.
check-connector-chains:
	uv run python scripts/ci/check_connector_chains.py

# Run security checks (bandit for Python security issues)
security:
	uv run pip install bandit 2>/dev/null || true
	uv run bandit -r almanak/ -ll --skip B101,B311 || true

# Run local pre-push test suite. Requires Anvil (Foundry) for tests/framework.
# Excludes intent tests (separate target) and visual/nightly.
test-unit:
	uv run pytest tests/ --ignore=tests/intents --ignore=tests/visual/nightly -m "not integration" -v --import-mode=importlib

# Alias for test-unit
test: test-unit

# Run connector tests (consolidated under tests/unit/connectors/ — the inline
# almanak/framework/connectors/<X>/tests/ directories were merged into the
# central tree; see CLAUDE.md "Repo Conventions").
test-connectors:
	uv run pytest tests/unit/connectors/ -v --import-mode=importlib

# Run intent tests (Anvil is auto-managed by pytest fixtures)
# Usage: make test-intents                    # all chains (one at a time)
#        make test-intents CHAIN=base         # single chain
test-intents:
	@if [ -n "$(CHAIN)" ]; then \
		echo "Running intent tests for $(CHAIN)..."; \
		uv run pytest tests/intents/$(CHAIN)/ -v -s -n0 --import-mode=importlib; \
	else \
		echo "Running intent tests for all chains..."; \
		uv run pytest tests/intents/ -v -s -n0 --import-mode=importlib; \
	fi

# Run integration tests (requires Anvil — currently manual)
test-integration:
	uv run pytest tests/framework -v -n0 --import-mode=importlib

# Run all tests (excludes intent tests and live-API integration tests)
test-all:
	uv run pytest tests/ --ignore=tests/intents -m "not integration" -v --import-mode=importlib

# COVERAGE_CORE=sysmon switches coverage.py to the PEP 669 sys.monitoring backend
# (Python 3.12+, coverage 7.7+). 5-10× faster than the default sys.settrace tracer —
# brings the CI runtime cost of `--cov` from ~doubling the test wall-clock down to
# single-digit %. Falls back to the default tracer on older Pythons. Line coverage
# only (sysmon does not yet support branch coverage; we don't use branch coverage).
export COVERAGE_CORE := sysmon

# Run all tests for CI (with JUnit XML + coverage report; coverage gate via fail_under).
# Phase 0 (2026-05-04): coverage now runs on every CI invocation. See
# docs/internal/coverage-improvement-plan.md §8 Phase 0 for the ratchet policy.
test-ci:
	uv run pytest tests/ --ignore=tests/intents --ignore=tests/visual/nightly -m "not integration" -v --import-mode=importlib \
		--cov=almanak --cov-report=xml:coverage.xml --cov-report=term \
		--junitxml=test-results.xml

# Full coverage pass used by `make crap` for the CRAP-score baseline.
# Runs the CI subset first (unit + non-intent), then APPENDS intent-test coverage
# (those tests cover most of the connector and execution code paths). Requires
# the intent-test environment (Anvil + gateway managed via tests/intents/conftest).
test-coverage:
	uv run pytest tests/ --ignore=tests/intents --ignore=tests/visual/nightly -m "not integration" -v --import-mode=importlib \
		--cov=almanak --cov-report=term
	uv run pytest tests/intents/ -v -s -n0 --import-mode=importlib \
		--cov=almanak --cov-append --cov-report=xml:coverage.xml --cov-report=term

# Compute CRAP (Change Risk Anti-Patterns) score per function. Reads the
# existing .coverage file (produced by `make test-coverage`). The CRAP script
# warns loudly if .coverage is stale or measured a narrow test scope — those
# warnings indicate phantom hotspots will appear in the report (see
# docs/internal/coverage-improvement-plan.md §7 "investigate first" lesson).
# Use `make crap-fresh` to regenerate coverage from scratch first.
crap:
	uv run scripts/crap_score.py --top 30

# Regenerate coverage from the full `make test-coverage` scope, then run the
# CRAP analysis. Use this when the .coverage file is missing, stale (>24h), or
# came from a narrow `pytest --cov` invocation. ~8 min total wall-clock.
#
# Recipe-level sub-makes (not prerequisites) so the ordering survives
# `make -j` — sibling prerequisites can run in parallel, and `crap` reading
# `.coverage` while `test-coverage` is still writing it would re-introduce
# the exact stale-data bug this script exists to prevent.
crap-fresh:
	$(MAKE) test-coverage
	$(MAKE) crap

# Diff-aware CRAP gate — fails if a PR adds or modifies a line inside any
# function whose CRAP score exceeds [tool.crap-diff].threshold (default 30).
# Requires a `.coverage` data file (produced by `make test-ci` or
# `make test-coverage`) and at least one ancestor commit on the compare branch.
# Override BASE for non-PR runs: `make crap-diff BASE=origin/feat/foo`.
#
# IMPORTANT — local vs CI parity: this target reads whatever `.coverage` happens
# to exist on disk. If that file was produced by a focused / narrow run (e.g.
# `pytest tests/unit/foo --cov=almanak`) coverage% will be inflated for files
# outside the focus and a green local result will not match CI's red. For an
# exact mirror of CI's `crap_gate` job, use `make crap-diff-fresh` (regenerates
# from `make test-ci`, the same scope CI uses).
BASE ?= origin/main
# No positional path arg — diff-quality treats those as pre-generated input
# reports (`Could not load report 'almanak/'`). Scope is enforced by
# [tool.crap-diff].package_root in pyproject.toml.
crap-diff:
	@test -f .coverage || (echo "crap-diff: .coverage missing — run 'make test-ci' or 'make test-coverage' first (or use 'make crap-diff-fresh' for a full CI-parity run)" && exit 1)
	uv run diff-quality --violations crap --fail-under 100 \
		--compare-branch=$(BASE)

# CI-parity diff-aware CRAP gate. Regenerates `.coverage` from the SAME scope
# CI uses (`make test-ci` — unit + non-intent tests with `--cov=almanak`), then
# runs the gate. Use this before pushing when the local `make crap-diff` says
# clean but you want to verify CI will agree. ~9 min wall-clock (test-ci ~8 min
# + crap-diff < 30s).
#
# Mirrors the `pr.yml` `crap_gate` job: that job reads `.coverage` produced by
# the upstream `test_pytest` job (`make test-ci`) and runs `make crap-diff`.
# The two-step recipe (sub-makes, not prerequisites) preserves ordering under
# `make -j` so the CRAP analysis never reads a partial `.coverage` file.
crap-diff-fresh:
	$(MAKE) test-ci
	$(MAKE) crap-diff

# Cyclomatic-complexity / maintainability-index report on production code.
# `make complexity`              → full almanak/ tree report.
# `make complexity FILE=path/x`  → narrow to one path (relative or absolute).
# CI gating of NEW high-complexity functions is done via `ruff check --select C901`
# (max-complexity = 15) using the in-line noqa baseline produced in Phase 0.
COMPLEXITY_PATH ?= $(if $(FILE),$(FILE),almanak/)
complexity:
	@echo "== Cyclomatic complexity (rank C+ only) =="
	uv run radon cc $(COMPLEXITY_PATH) -s -a -nc --exclude 'almanak/gateway/proto/*,almanak/demo_strategies/*'
	@echo
	@echo "== Maintainability Index (rank B and below) =="
	uv run radon mi $(COMPLEXITY_PATH) -s -nb --exclude 'almanak/gateway/proto/*,almanak/demo_strategies/*'

# Run nightly-only visual Market Data API contract tests
test-nightly-visual:
	uv run pytest tests/visual/nightly/ -q -n0 --import-mode=importlib

# Generate documentation for the CLI
docs-cli:
	rm -rf docs/cli/*.md
	uv run mdclick dumps --baseModule=almanak.cli --baseCommand=almanak --docsPath=./docs/cli
	sed -i.bak 's/\* Type: <click\.types\.Path.*>/* Type: `Path`/g' docs/cli/*.md
	rm -f docs/cli/*.md.bak

# Build documentation site (includes llms.txt generation)
docs:
	uv run mkdocs build
	@$(MAKE) docs-llms

# Generate llms.txt (separate config, no i18n - see mkdocs-llms.yml)
# Copies llms.txt, llms-full.txt, and all per-page .md files that the plugin
# generates (these are the targets of the URLs in llms.txt).
docs-llms:
	uv run mkdocs build -f mkdocs-llms.yml -d site-llms
	mkdir -p site
	test -f site-llms/llms.txt
	test -f site-llms/llms-full.txt
	cp site-llms/llms.txt site-llms/llms-full.txt site/
	cd site-llms && find . -name '*.md' ! -name 'llms*' -exec sh -c 'mkdir -p "../site/$$(dirname "{}")" && cp "{}" "../site/{}"' \;
	rm -rf site-llms

# Serve documentation locally for development
docs-serve:
	uv run mkdocs serve

# Clean documentation build output
docs-clean:
	rm -rf site/ site-llms/

# Install production dependencies
install:
	uv sync

# Install development dependencies
install-dev:
	uv sync --all-extras

# Version bumping commands
# Increment patch version (0.1.0 -> 0.1.1)
version-bump-patch:
	uv run python scripts/version_manager.py patch

# Increment minor version (0.1.0 -> 0.2.0)
version-bump-minor:
	uv run python scripts/version_manager.py minor

# Increment major version (0.1.0 -> 1.0.0)
version-bump-major:
	uv run python scripts/version_manager.py major

# Undo the last version bump
version-undo:
	uv run python scripts/version_manager.py undo

# Update version in pyproject.toml
update-setup-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION environment variable is not set"; \
		exit 1; \
	fi
	@sed -i 's/version = "[^"]*"/version = "$(VERSION)"/' pyproject.toml

run-anvil:
	@if [ -z "$(ALCHEMY_API_KEY)" ]; then \
		echo "Error: ALCHEMY_API_KEY environment variable is not set"; \
		echo "Please set it with: export ALCHEMY_API_KEY=your_api_key"; \
		exit 1; \
	fi
	anvil -f https://arb-mainnet.g.alchemy.com/v2/$(ALCHEMY_API_KEY) --no-rate-limit --transaction-block-keeper 100

run-example:
	@if [ -z "$(ALCHEMY_API_KEY)" ]; then \
		echo "Error: ALCHEMY_API_KEY environment variable is not set"; \
		echo "Please set it with: export ALCHEMY_API_KEY=your_api_key"; \
		exit 1; \
	fi
	uv run almanak strat new -n example_strategy -t mean_reversion -c arbitrum --output-dir ./example_strategy
	@echo "Example strategy created in example_strategy/"
	@echo "To test: cd example_strategy && uv run almanak strat run --once"

# Set the almanak-code (open-coder) version to bundle
# Usage: make set-almanak-code-version VERSION=v0.2.0
#        make set-almanak-code-version VERSION=latest
set-almanak-code-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is not set. Usage: make set-almanak-code-version VERSION=v0.2.0"; \
		exit 1; \
	fi
	@echo "$(VERSION)" > .almanak-code-version
	@echo "Set almanak-code version to: $(VERSION)"

build-platform-wheels:
	rm -rf dist
	uv run python scripts/build_platform_wheels.py

build-upload-gcp: build-platform-wheels
	@echo "Publishing platform wheels to Artifact Registry..."
	TWINE_REPOSITORY_URL=https://europe-west4-python.pkg.dev/almanak-production/almanak-py/ \
	TWINE_USERNAME="oauth2accesstoken" \
	TWINE_PASSWORD="$$(gcloud auth print-access-token)" \
	uv run python -m twine upload --disable-progress-bar dist/*.whl

# Gateway commands
proto:
	uv run python -m grpc_tools.protoc -I./almanak/gateway/proto --python_out=./almanak/gateway/proto --grpc_python_out=./almanak/gateway/proto --mypy_out=./almanak/gateway/proto ./almanak/gateway/proto/*.proto
	# Fix imports in generated grpc file (grpc_tools generates relative imports that break in packages)
	sed -i.bak 's/import gateway_pb2 as gateway__pb2/from almanak.gateway.proto import gateway_pb2 as gateway__pb2/' ./almanak/gateway/proto/gateway_pb2_grpc.py && rm -f ./almanak/gateway/proto/gateway_pb2_grpc.py.bak

# Check that generated proto files are up-to-date (CI)
proto-check:
	uv run pytest tests/gateway/test_proto_compatibility.py -v --import-mode=importlib
	uv run python scripts/ci/check_deployment_id_proto_surface.py

# Start gateway server (required for strategy execution)
gateway:
	@echo "Starting gateway server..."
	@echo "Press Ctrl+C to stop"
	uv run python -m almanak.gateway.server

# Start dashboard (requires gateway to be running)
dashboard-only:
	@echo "Starting dashboard (gateway must be running)..."
	@echo "Press Ctrl+C to stop"
	uv run almanak dashboard

# Start gateway and dashboard together (for development)
# Gateway runs in background, dashboard in foreground
dashboard:
	@echo "Starting gateway in background..."
	@uv run python -m almanak.gateway.server & GATEWAY_PID=$$!; \
	sleep 2; \
	echo "Starting dashboard..."; \
	echo "Press Ctrl+C to stop both"; \
	trap "kill $$GATEWAY_PID 2>/dev/null; exit" INT TERM; \
	uv run almanak dashboard; \
	kill $$GATEWAY_PID 2>/dev/null

# Start strategy on Anvil (managed gateway/anvil) and dashboard together.
# Usage:
#   make anvil-dev STRATEGY_DIR=strategies/demo/uniswap_rsi
# Optional:
#   STRAT_FLAGS="--interval 30" DASHBOARD_FLAGS="--port 8502 --no-browser"
anvil-dev:
	@if [ -z "$(STRATEGY_DIR)" ]; then \
		echo "Error: STRATEGY_DIR is required"; \
		echo "Example: make anvil-dev STRATEGY_DIR=almanak/demo_strategies/uniswap_rsi"; \
		exit 1; \
	fi
	@echo "Starting strategy (auto-starts managed gateway + Anvil)..."
	@uv run almanak strat run -d "$(STRATEGY_DIR)" --network anvil $(STRAT_FLAGS) & STRAT_PID=$$!; \
	sleep 4; \
	echo "Starting dashboard..."; \
	echo "Press Ctrl+C to stop both"; \
	trap "kill $$STRAT_PID 2>/dev/null; exit" INT TERM; \
	uv run almanak dashboard $(DASHBOARD_FLAGS); \
	kill $$STRAT_PID 2>/dev/null

# Docker commands for gateway security testing
docker-build:
	docker-compose -f deploy/docker/docker-compose.yml build

docker-up:
	docker-compose -f deploy/docker/docker-compose.yml up -d

docker-down:
	docker-compose -f deploy/docker/docker-compose.yml down

docker-logs:
	docker-compose -f deploy/docker/docker-compose.yml logs -f

# Docker commands for dashboard specifically
docker-dashboard-build:
	docker-compose -f deploy/docker/docker-compose.yml build dashboard

docker-dashboard-up:
	docker-compose -f deploy/docker/docker-compose.yml up -d gateway dashboard

docker-dashboard-logs:
	docker-compose -f deploy/docker/docker-compose.yml logs -f dashboard

# Network isolation tests - verifies strategy container cannot reach internet
test-network-isolation:
	docker-compose -f deploy/docker/docker-compose.test.yml build
	docker-compose -f deploy/docker/docker-compose.test.yml run --rm strategy-test
	docker-compose -f deploy/docker/docker-compose.test.yml down

# Gateway unit tests (excludes docker-only network isolation tests)
test-gateway:
	uv run pytest tests/gateway/ -v --import-mode=importlib -m "not docker"

# BacktestService tests (HTTP API lifecycle, fee models, paper trading)
test-backtest-service:
	uv run pytest tests/unit/services/backtest/ -v --import-mode=importlib

# Demo strategy smoke test (run on Anvil forks via the product CLI path).
# Drives `almanak strat run --network anvil --fresh --once` for every
# discoverable demo. Each demo's funding comes from its own config.json
# anvil_funding (no global dict) — see docs/internal/DemoFixing.md.
# Uses public RPCs by default; set ALCHEMY_API_KEY for better rate limits.
test-demo-strategies:
	uv run --env-file .env python scripts/run_demo.py --all

# Quick demo strategy smoke test (one per registered connector chain).
test-demo-quick:
	uv run --env-file .env python scripts/run_demo.py --quick

# Test a single demo through the product CLI path.
# Usage: make test-demo-single STRATEGY=uniswap_rsi CHAIN=arbitrum
test-demo-single:
	@if [ -z "$(STRATEGY)" ]; then \
		echo "Error: STRATEGY is not set. Usage: make test-demo-single STRATEGY=uniswap_rsi"; \
		exit 1; \
	fi
	uv run --env-file .env python scripts/run_demo.py --strategy $(STRATEGY) $(if $(CHAIN),--chain $(CHAIN),)

# List discoverable demo strategies (DemoSpec catalog).
list-demo-strategies:
	uv run python scripts/run_demo.py --list

# Run the VIB-4316 accounting matrix end-to-end across every in-scope fixture
# defined in scripts/qa/accounting-matrix.yml. Drives each strategy on managed
# Anvil, scores the 21-cell Accountant Test per row, and writes a typed gap
# report. Per-row artifacts land under notes/.tmp/accounting-matrix/<row_id>/
# (gitignored). Full matrix is ~92 min serial — use test-accounting-matrix-quick
# for a 7-min smoke gate on the two baselined rows.
test-accounting-matrix:
	uv run python scripts/qa/run_accounting_matrix.py \
		--matrix scripts/qa/accounting-matrix.yml \
		--output-dir notes/.tmp/accounting-matrix

# Quick accounting matrix smoke: only the two baselined rows (lp + looping),
# ~7-8 min total. Use as a CI gate for accounting-affecting PRs.
test-accounting-matrix-quick:
	uv run python scripts/qa/run_accounting_matrix.py \
		--matrix scripts/qa/accounting-matrix.yml \
		--output-dir notes/.tmp/accounting-matrix \
		--rows-include lp-uniswap_v3-arbitrum,looping-aave_v3-arbitrum

# Check Pendle market expiry dates in demo and incubating strategy configs.
# Fails if any demo strategy references a market expiring within 30 days.
check-pendle-expiry:
	uv run python scripts/ci/check_pendle_expiry.py --days 30 --verbose

build-upload-pypi: build-platform-wheels
	@echo "Publishing platform wheels to PyPI..."
	TWINE_USERNAME="__token__" \
	TWINE_PASSWORD="$(PYPI_API_TOKEN)" \
	uv run python -m twine upload --disable-progress-bar dist/*.whl

# Clean build artifacts and caches
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf dist/ build/ *.egg-info .eggs/ site/
	rm -rf coverage.xml test-results.xml .coverage .coverage.*
