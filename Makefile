.PHONY: all clean test test-unit test-connectors test-intents test-integration test-all test-coverage test-gateway test-demo-strategies test-demo-quick test-demo-single list-demo-strategies set-almanak-code-version build-platform-wheels build publish lint lint-check format format-check security docs docs-cli docs-serve docs-clean install install-dev version-bump-patch version-bump-minor version-bump-major version-undo update-setup-version proto proto-check gateway dashboard dashboard-only anvil-dev typecheck typecheck-report docker-workstation-build docker-workstation-run docker-workstation-exec docker-workstation-stop

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

# Run security checks (bandit for Python security issues)
security:
	uv run pip install bandit 2>/dev/null || true
	uv run bandit -r almanak/ -ll --skip B101,B311 || true

# Run unit tests only (no Anvil required)
test-unit:
	uv run pytest tests/ --ignore=tests/framework --ignore=tests/intents -v --import-mode=importlib

# Alias for test-unit
test: test-unit

# Run connector tests (SDK, Adapter, Receipt Parser tests within connectors)
test-connectors:
	uv run pytest almanak/framework/connectors/*/tests/ -v

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

# Run all tests with coverage report
test-coverage:
	uv run pytest tests/ -v --import-mode=importlib \
		--cov=almanak --cov-report=html:coverage-html --cov-report=xml:coverage.xml --cov-report=term \
		--junitxml=test-results.xml

# Generate documentation for the CLI
docs-cli:
	rm -rf docs/cli/*.md
	uv run mdclick dumps --baseModule=almanak.cli --baseCommand=almanak --docsPath=./docs/cli
	sed -i.bak 's/\* Type: <click\.types\.Path.*>/* Type: `Path`/g' docs/cli/*.md
	rm -f docs/cli/*.md.bak

# Build documentation site
docs:
	uv run mkdocs build

# Serve documentation locally for development
docs-serve:
	uv run mkdocs serve

# Clean documentation build output
docs-clean:
	rm -rf site/

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
	uv run almanak strat new -n example_strategy -t mean_reversion -c arbitrum
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
	uv run pytest tests/gateway/test_proto_compatibility.py -v

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
		echo "Example: make anvil-dev STRATEGY_DIR=strategies/demo/uniswap_rsi"; \
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

# Demo strategy tests through gateway (run on Anvil forks)
# These tests verify strategies work correctly through the gateway architecture
test-demo-strategies:
	@if [ -z "$(ALCHEMY_API_KEY)" ]; then \
		echo "Error: ALCHEMY_API_KEY environment variable is not set"; \
		exit 1; \
	fi
	uv run --env-file .env python scripts/test_demo_strategies_gateway.py --all

# Quick demo strategy gateway test (one per chain)
test-demo-quick:
	@if [ -z "$(ALCHEMY_API_KEY)" ]; then \
		echo "Error: ALCHEMY_API_KEY environment variable is not set"; \
		exit 1; \
	fi
	uv run --env-file .env python scripts/test_demo_strategies_gateway.py

# Test a single strategy through gateway
# Usage: make test-demo-single STRATEGY=uniswap_rsi CHAIN=arbitrum
test-demo-single:
	@if [ -z "$(ALCHEMY_API_KEY)" ]; then \
		echo "Error: ALCHEMY_API_KEY environment variable is not set"; \
		exit 1; \
	fi
	@if [ -z "$(STRATEGY)" ]; then \
		echo "Error: STRATEGY is not set. Usage: make test-demo-single STRATEGY=uniswap_rsi"; \
		exit 1; \
	fi
	uv run --env-file .env python scripts/test_demo_strategies_gateway.py --strategy $(STRATEGY) $(if $(CHAIN),--chain $(CHAIN),)

# List available gateway-compatible demo strategies
list-demo-strategies:
	uv run python scripts/test_demo_strategies_gateway.py --list

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
	rm -rf coverage-html/ coverage.xml test-results.xml .coverage
