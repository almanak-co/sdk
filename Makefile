.PHONY: all help clean test test-unit test-acceptance-pack test-connectors test-intents test-integration test-all test-ci test-coverage crap crap-fresh crap-diff crap-diff-fresh test-nightly-visual test-gateway test-backtest-service test-demo-strategies test-demo-quick test-demo-single test-accounting-matrix test-accounting-matrix-quick list-demo-strategies check-pendle-expiry set-almanak-code-version build-platform-wheels build publish lint lint-check format format-check security docs docs-cli docs-generated docs-serve docs-clean install install-dev version-bump-patch version-bump-minor version-bump-major version-undo update-setup-version proto proto-check gateway dashboard dashboard-only anvil-dev typecheck typecheck-report docker-workstation-build docker-workstation-run docker-workstation-exec docker-workstation-stop audit-intent-paths check-xfail-hygiene check-config-boundary check-connector-registry check-connector-chains check-intent-coverage check-deployment-scoped-tables check-deployment-id-proto-surface check-gateway-isolation check-decimal-policy check-decimal-policy-baseline regen-contract-baselines check-accounting-ratchet scan-coupling scan-coupling-report scan-coupling-baseline

# Load .env file if it exists
-include .env
export

# Default target
all: install lint ## Install deps and lint (default target)

# Show annotated targets. Greps only the Makefile itself (firstword) —
# MAKEFILE_LIST also contains .env via `-include .env`, and .env must
# never be echoed into help output.
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(firstword $(MAKEFILE_LIST)) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2}'

# Run linting with auto-fix (local development)
lint: ## Ruff check + format with auto-fix (local dev)
	uv run ruff check almanak --fix
	uv run ruff format almanak

# Run linting without auto-fix - fails on errors (CI)
lint-check: ## Ruff check + format, no fixes (CI)
	uv run ruff check almanak
	uv run ruff format almanak --check

# Format code
format: ## Format code with ruff
	uv run ruff format almanak

# Check formatting without changes (CI)
format-check:
	uv run ruff format almanak --check

# Run mypy type checks (CI)
typecheck: ## Run mypy type checks
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

# Enforce that every connector dir under almanak/connectors/ registers itself
# in ConnectorRegistry (VIB-4298 PR 1; lazy registration shape from VIB-4835).
# The registry is the source of truth for the (connector, intent, chain)
# universe consumed by PR 2's intent-test coverage gate and future tooling.
check-connector-registry: ## Validate connector manifests against the registry
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

# Decimal-policy ratchet gate (VIB-3164). Fails on any NET-NEW silent
# token-decimal fallback (``decimals = 18`` / ``... or 18`` / ``.get(...,18)``)
# in almanak/framework + almanak/connectors production code. Empty != Zero:
# an unresolved decimal must stay None and the caller skips / fails loud, never
# defaults to 18 (wrong for USDC=6, WBTC=8, USDT=6). Pre-existing debt is
# tracked in scripts/ci/decimal-policy-baseline.json and must shrink, not grow.
#   make check-decimal-policy            # check against baseline (CI mode)
#   make check-decimal-policy-baseline   # refresh baseline (after a cleanup)
check-decimal-policy:
	uv run python scripts/ci/check_decimal_policy.py

check-decimal-policy-baseline:
	uv run python scripts/ci/check_decimal_policy.py --baseline

# Content-keyed contract-gate baselines (bifurcation / private-cache /
# direct-constructor). Regenerate after legitimately adding or removing a
# tracked site — pure line moves no longer require any baseline change.
regen-contract-baselines:
	uv run python scripts/ci/contract_gate_baselines.py

# Accountant-Test ratchet gate (VIB-3836). Runs the 21-cell Accountant Test
# against the committed frozen fixtures (tests/fixtures/accounting/{lp,looping,
# perp}/expected_baseline.sqlite) and compares each cell's status to the
# committed manifest (expected_cells.json). Ratchet semantics: any cell that
# the manifest records as currently-passing must not regress below its floor
# (PASS->XFAIL/FAIL, XFAIL->FAIL). Existing XFAILs are tolerated — the matrix
# is ~18/21, not full-green. Improvements (XFAIL->PASS) print a NEW_PASS
# advisory but do not fail; ratchet the manifest forward in the same PR to lock
# the win in. Offline + deterministic (no live Anvil); the heavier managed-Anvil
# sweep stays nightly (make test-accounting-matrix).
check-accounting-ratchet:
	uv run python scripts/ci/check_accounting_ratchet.py --check --verbose

# Chain/protocol coupling scanner (VIB-4851 / VIB-4852). Re-scans the
# repo for chain- and protocol-coupled code outside its canonical home
# and compares against the committed baseline. CI ratchet gate: any
# net-new finding fails the build; refactors that shrink the count
# pass without re-baselining.
#
# Local workflow:
#   make scan-coupling             # check against committed baseline (CI mode)
#   make scan-coupling-report      # regenerate the dated Markdown report
#   make scan-coupling-baseline    # refresh the committed baseline JSON
#                                  # (only after eliminating net-new findings
#                                  # or having them approved as intentional)
SCAN_COUPLING_BASELINE := docs/internal/audits/chain-protocol-coupling-baseline.json

scan-coupling:
	uv run python scripts/ci/scan_chain_protocol_coupling.py \
		--check-against $(SCAN_COUPLING_BASELINE)

scan-coupling-report:
	uv run python scripts/ci/scan_chain_protocol_coupling.py --print-summary

scan-coupling-baseline:
	uv run python scripts/ci/scan_chain_protocol_coupling.py \
		--baseline $(SCAN_COUPLING_BASELINE)

# Gateway protocol-isolation guard (VIB-4812 / epic VIB-4808).
# Fails CI on protocol-keyed dispatch shapes inside ``almanak/gateway/**``:
#   * ``if protocol/venue/dex == "<protocol>":`` equality dispatch
#   * ``"<protocol>" in deployment_id_lower`` substring sniffs
#   * module-level ``_PROTOCOL_TO_X = {"uniswap_v3": ...}`` dispatch dicts
# Phase 3 (VIB-4811) migrates the remaining dispatch tables; the
# allowlist inside the test file shrinks to empty once that lands.
# Also runs the strategy-egress guard (plans/003): unmarked direct-egress
# imports/calls in ``strategies/incubating/**`` and ``strategies/experiments/**``
# fail here and in the tests/ CI sweep.
# Also runs the framework→gateway import ratchet (plan 013): freezes the
# 53 existing (path, import) sites in a content-keyed baseline and fails
# on any NEW ``almanak.gateway.*`` import from ``almanak/framework/``;
# ``almanak.gateway.proto`` (the sanctioned gRPC channel) is exempt;
# the baseline only shrinks — stale entries fail until removed.
check-gateway-isolation:
	uv run pytest tests/static/test_gateway_protocol_isolation.py tests/static/test_strategy_egress_guard.py tests/static/test_framework_gateway_import_ratchet.py --import-mode=importlib

# Run security checks (bandit for Python security issues)
security:
	uv run pip install bandit 2>/dev/null || true
	uv run bandit -r almanak/ -ll --skip B101,B311 || true

# Run local pre-push test suite. Requires Anvil (Foundry) for tests/framework.
# Excludes intent tests (separate target) and visual/nightly.
test-unit: ## Unit suite (excludes intents and visual/nightly); needs Anvil
	uv run pytest tests/ --ignore=tests/intents --ignore=tests/visual/nightly -m "not integration" -v --import-mode=importlib

# Alias for test-unit
test: test-unit ## Alias for test-unit

# VIB-4728 POOL-9 (VIB-4757) acceptance pack — 9 gateway tests + 2 framework
# mirror tests aggregated under @pytest.mark.acceptance_pack. The frozen
# collect-only snapshot at
# tests/gateway/services/test_pool_history_acceptance_pack.EXPECTED.txt is
# diff-guarded so a regression (test renamed/removed/relocated) fails the
# gate before the run starts. See docs/internal/uat-cards/VIB-4728.md §D5.A.
.PHONY: test-acceptance-pack
test-acceptance-pack:
	@echo "Running VIB-4728 POOL-9 acceptance pack..."
	@# Pass --import-mode=importlib to BOTH invocations so the collect-only
	@# snapshot and the live run resolve module IDs the same way (pr-auditor
	@# 2026-05-28 flagged the asymmetric default: collect-only fell back to
	@# pytest default 'prepend' after `-o "addopts="` wiped the pytest.ini
	@# `--import-mode=append`; the live run used `importlib`. Today the
	@# file basenames are unique so the snapshot matched, but the symmetry
	@# matters for future tests that share basenames across dirs).
	@uv run pytest -m acceptance_pack --collect-only -q --import-mode=importlib -o "addopts=" 2>&1 \
		| grep -E '^tests/.*::' | sort > /tmp/pool9-collected.txt
	@diff -u tests/gateway/services/test_pool_history_acceptance_pack.EXPECTED.txt /tmp/pool9-collected.txt \
		|| (echo "FAIL: acceptance-pack collection set drifted from EXPECTED snapshot. Update snapshot deliberately if intentional." && exit 1)
	@uv run pytest -m acceptance_pack -v --import-mode=importlib -o "addopts=" -p no:cacheprovider

# Run connector tests (consolidated under tests/unit/connectors/ — the inline
# per-connector tests/ directories were merged into the central tree;
# see CLAUDE.md "Repo Conventions").
test-connectors:
	uv run pytest tests/unit/connectors/ -v --import-mode=importlib

# Run intent tests (Anvil is auto-managed by pytest fixtures)
# Usage: make test-intents                    # all chains (one at a time)
#        make test-intents CHAIN=base         # single chain
test-intents: ## Intent tests; all chains, or scope with the CHAIN variable
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
#
# STORE_DURATIONS is empty by default (local + crap-diff-fresh runs are
# unchanged). The post-merge Main pipeline (template_test_suite.yml) passes
# `STORE_DURATIONS="--store-durations --clean-durations"` so the full unsharded
# run records per-test timings into `.test_durations`, which is cached and
# restored by the PR pipeline's sharded jobs for duration-balanced splitting
# (template_pytest.yml). Main is the only place this runs the *full* suite, so
# it is the only place that can produce a complete, clean durations file.
STORE_DURATIONS ?=
test-ci: ## CI test run with coverage (writes .coverage/coverage.xml)
	uv run pytest tests/ --ignore=tests/intents --ignore=tests/visual/nightly -m "not integration" -v --import-mode=importlib \
		--cov=almanak --cov-report=xml:coverage.xml --cov-report=term \
		--junitxml=test-results.xml $(STORE_DURATIONS)

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
crap-diff: ## Diff-scoped CRAP gate (needs .coverage from test-ci)
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
	uv run radon cc $(COMPLEXITY_PATH) -s -a -nc --exclude 'almanak/gateway/proto/*,almanak/connectors/polymarket/proto/*,almanak/demo_strategies/*'
	@echo
	@echo "== Maintainability Index (rank B and below) =="
	uv run radon mi $(COMPLEXITY_PATH) -s -nb --exclude 'almanak/gateway/proto/*,almanak/connectors/polymarket/proto/*,almanak/demo_strategies/*'

# Run nightly-only visual Market Data API contract tests
test-nightly-visual:
	uv run pytest tests/visual/nightly/ -q -n0 --import-mode=importlib

# Generate documentation for the CLI
docs-cli:
	rm -rf docs/cli/*.md
	uv run mdclick dumps --baseModule=almanak.cli --baseCommand=almanak --docsPath=./docs/cli
	sed -i.bak 's/\* Type: <click\.types\.Path.*>/* Type: `Path`/g' docs/cli/*.md
	rm -f docs/cli/*.md.bak

# Regenerate every machine-generated page used by mkdocs. These outputs are
# gitignored — they exist only after this target runs. Anything that builds
# the docs site (``docs``, ``docs-serve``, the deploy workflow) must depend
# on this first.
docs-generated: docs-cli ## Regenerate machine-generated docs pages
	uv run python scripts/docs/generate_connector_matrix.py --apply
	uv run python scripts/docs/generate_chain_table.py --apply

# Build documentation site (includes llms.txt generation)
docs: docs-generated ## Build the full docs site (incl. llms.txt)
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
docs-serve: docs-generated
	uv run mkdocs serve

# Clean documentation build output
docs-clean:
	rm -rf site/ site-llms/

# Install production dependencies
install: ## Install production deps (uv sync)
	uv sync

# Install development dependencies
install-dev: ## Install dev deps (uv sync --all-extras)
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
# VIB-4813 (Phase 5 of VIB-4808): Polymarket-specific RPCs live under
# ``almanak/connectors/polymarket/proto/`` so the gateway proto layer no
# longer names individual connectors. The connector-owned proto keeps the
# same ``package = almanak.gateway.proto`` declaration so the wire-level
# service name is byte-identical to the pre-move version.
proto: ## Regenerate gateway gRPC stubs from proto files
	uv run python -m grpc_tools.protoc -I./almanak/gateway/proto --python_out=./almanak/gateway/proto --grpc_python_out=./almanak/gateway/proto --mypy_out=./almanak/gateway/proto ./almanak/gateway/proto/*.proto
	# Fix imports in generated grpc file (grpc_tools generates relative imports that break in packages)
	sed -i.bak 's/import gateway_pb2 as gateway__pb2/from almanak.gateway.proto import gateway_pb2 as gateway__pb2/' ./almanak/gateway/proto/gateway_pb2_grpc.py && rm -f ./almanak/gateway/proto/gateway_pb2_grpc.py.bak
	uv run python -m grpc_tools.protoc -I./almanak/connectors/polymarket/proto --python_out=./almanak/connectors/polymarket/proto --grpc_python_out=./almanak/connectors/polymarket/proto --mypy_out=./almanak/connectors/polymarket/proto ./almanak/connectors/polymarket/proto/*.proto
	sed -i.bak 's/import polymarket_pb2 as polymarket__pb2/from almanak.connectors.polymarket.proto import polymarket_pb2 as polymarket__pb2/' ./almanak/connectors/polymarket/proto/polymarket_pb2_grpc.py && rm -f ./almanak/connectors/polymarket/proto/polymarket_pb2_grpc.py.bak

# Check that generated proto files are up-to-date (CI)
proto-check:
	uv run pytest tests/gateway/test_proto_compatibility.py -v --import-mode=importlib
	uv run python scripts/ci/check_deployment_id_proto_surface.py

# Start gateway server (required for strategy execution)
gateway: ## Start the gateway server
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
# report. Per-row artifacts land under docs/internal/notes/.tmp/accounting-matrix/<row_id>/
# (gitignored). Full matrix is ~92 min serial — use test-accounting-matrix-quick
# for a 7-min smoke gate on the two baselined rows.
test-accounting-matrix:
	uv run python scripts/qa/run_accounting_matrix.py \
		--matrix scripts/qa/accounting-matrix.yml \
		--output-dir docs/internal/notes/.tmp/accounting-matrix

# Quick accounting matrix smoke: only the two baselined rows (lp + looping),
# ~7-8 min total. Use as a CI gate for accounting-affecting PRs.
test-accounting-matrix-quick:
	uv run python scripts/qa/run_accounting_matrix.py \
		--matrix scripts/qa/accounting-matrix.yml \
		--output-dir docs/internal/notes/.tmp/accounting-matrix \
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
