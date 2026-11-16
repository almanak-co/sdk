.PHONY: all help install install-dev lint lint-check typecheck fixtures \
	test test-ci contract-examples crap crap-ci check security hygiene public-tree check-strategies \
	crap-from-coverage qa-plan qa-daily docs-cli docs-support docs-generated docs docs-llms docs-serve docs-check docs-clean \
	build build-platform-wheels binaries almanak-code-version publish-pypi version clean

UV := uv run --locked
PYTHON_TREES := src tests strategies scripts

all: install-dev lint-check typecheck ## Install and run the commit checks

help: ## Show annotated targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(firstword $(MAKEFILE_LIST)) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

install: ## Install the locked runtime environment
	uv sync --locked

install-dev: ## Install the locked environment with every dependency group
	uv sync --locked --all-groups

lint: ## Ruff check with fixes and format (local development)
	$(UV) ruff check --fix $(PYTHON_TREES)
	$(UV) ruff format $(PYTHON_TREES)

lint-check: ## Ruff check and format check without changes (CI)
	$(UV) ruff check $(PYTHON_TREES)
	$(UV) ruff format --check $(PYTHON_TREES)

typecheck: ## Mypy over src, strategies and scripts
	$(UV) mypy

# Tests that read retained captures through tests/retained_fixtures.py use the copies
# committed under tests/fixtures/evidence/; tests/fixtures/evidence.json pins them.
fixtures: ## Verify the committed evidence copies, restoring changed ones from a local source
	$(UV) python scripts/evidence_fixtures.py fetch

test: fixtures ## Offline test suite; missing retained evidence is a failure
	$(UV) pytest -n auto

# Hosted CI only. Modules that need the gitignored evidence tree are reported as
# skipped, never as passed; the summary line records the reduced scope.
test-ci: fixtures ## Offline suite with evidence-dependent modules skipped (hosted CI)
	$(UV) pytest -n auto --skip-missing-evidence -rfEs --junitxml=test-results.xml

contract-examples: ## Rewrite the committed contract examples from their producers
	ALMANAK_WRITE_CONTRACT_EXAMPLES=1 $(UV) pytest tests/test_contract_examples.py -n auto -k emit

crap: fixtures ## Fresh coverage run, CRAP <= 30 for every function and the coverage floors
	$(UV) python scripts/check_crap.py -n auto

# Hosted scope: modules that need absent retained evidence are skipped and left
# unscored, and BASE=<ref> gates only functions with an executable line changed
# since the merge base, as the PR gate does. Coverage floors always score their
# whole directory.
crap-ci: fixtures ## CRAP gate over the hosted-CI test scope (BASE=origin/main for the PR scope)
	$(UV) python scripts/check_crap.py -n auto --skip-missing-evidence \
		$(if $(BASE),--changed-since $(BASE))

# Hosted CI: shards run scripts/ci/run_pytest_shard.sh, the aggregator combines
# their data into .coverage, and this scores it without rerunning the suite.
crap-from-coverage: ## Score combined shard coverage in .coverage (BASE=<ref> for the PR scope)
	$(UV) python scripts/check_crap.py --coverage-data .coverage --skip-missing-evidence \
		$(if $(BASE),--changed-since $(BASE)) \
		$(addprefix --source-manifest ,$(wildcard coverage.shard-*.sources.json))

check: lint-check typecheck hygiene test crap docs-check ## Every local gate

security: ## Advisory bandit scan; never fails the build
	uvx bandit -r src -ll || true

hygiene: ## Refuse private material in the tree published to the public repository
	$(UV) python scripts/ci/public_hygiene.py

public-tree: hygiene ## Export the committed public tree into an empty OUT=<dir>
	@test -n "$(OUT)" || { echo "usage: make public-tree OUT=<empty directory>"; exit 2; }
	@test -z "$$(git status --porcelain)" || { echo "commit or discard changes first: the export is HEAD"; exit 2; }
	@mkdir -p "$(OUT)" && test -z "$$(ls -A "$(OUT)")" || { echo "$(OUT) is not empty"; exit 2; }
	git archive --format=tar HEAD | tar -x -C "$(OUT)"
	@echo "exported $$(git rev-parse HEAD) to $(OUT)"

check-strategies: ## Offline `almanak check` over every reference strategy against the support matrix
	$(UV) python scripts/ci/check_strategies.py


qa-plan: ## Validate and emit the daily QA matrix without provider access
	$(UV) python -m scripts.qa_bench --plan --output artifacts/qa-plan

qa-daily: fixtures ## Run the daily QA bench with explicit provider bindings
	$(UV) python -m scripts.qa_bench --env-file .env --output artifacts/qa-daily

docs-cli: ## Regenerate the CLI reference pages from the argparse tree
	rm -rf docs/cli
	$(UV) python scripts/docs/generate_cli_reference.py --output docs/cli

docs-support: ## Regenerate the connector and chain support page
	$(UV) python -m scripts.docs.generate_support_matrix --output docs/support.md

docs-generated: docs-cli docs-support ## Regenerate every generated docs page

docs: docs-generated ## Build the docs site including llms.txt
	$(UV) mkdocs build
	@$(MAKE) docs-llms

# llms.txt needs a separate config because the llmstxt plugin cannot run under
# mkdocs-static-i18n. The per-page Markdown files it emits are the targets of the
# URLs inside llms.txt, so they are copied into the site as well. The generated
# pages are gitignored, so the bundle regenerates them rather than shipping without.
docs-llms: docs-generated
	$(UV) mkdocs build -f mkdocs-llms.yml -d site-llms
	mkdir -p site
	test -f site-llms/llms.txt
	test -f site-llms/llms-full.txt
	cp site-llms/llms.txt site-llms/llms-full.txt site/
	cd site-llms && find . -name '*.md' ! -name 'llms*' -exec sh -c 'mkdir -p "../site/$$(dirname "{}")" && cp "{}" "../site/{}"' \;
	rm -rf site-llms

docs-serve: docs-generated ## Serve the docs site locally
	$(UV) mkdocs serve

docs-check: docs-generated ## Docs drift checks (nav, mkdocstrings paths, locale structure)
	$(UV) python scripts/docs/check_mkdocs_nav.py
	$(UV) python scripts/docs/check_mkdocstrings_paths.py
	$(UV) python scripts/docs/check_i18n_structure.py

docs-clean: ## Remove built and generated docs
	rm -rf site site-llms docs/cli docs/support.md

build: ## Build sdist and wheel into dist/
	rm -rf dist
	uv build

# Release only. `build` stays a plain uv build because the packaging smoke test
# globs dist/almanak-*.whl and expects exactly one match.
build-platform-wheels: ## Build the sdist and one wheel per bundled binary into dist/
	rm -rf dist
	uv build --sdist
	$(UV) python scripts/build_platform_wheels.py

binaries: ## Download the pinned Almanak Code binaries into src/almanak/bin/
	bash scripts/update_binaries.sh

almanak-code-version: ## Pin the bundled Almanak Code release (VERSION=v1.0.66)
	@test -n "$(VERSION)" || { echo "VERSION is required, for example VERSION=v1.0.66"; exit 1; }
	@echo "$(VERSION)" > .almanak-code-version
	@echo "Pinned Almanak Code to $(VERSION)"

publish-pypi: ## Upload dist/* to PyPI (needs PYPI_API_TOKEN)
	uv publish --token "$(PYPI_API_TOKEN)" dist/*

version: ## Print the package version from pyproject.toml
	@uv version --short

clean: ## Remove caches and build output
	rm -rf dist build site site-llms .pytest_cache .ruff_cache .mypy_cache test-results.xml
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
