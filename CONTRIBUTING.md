# Contributing to Almanak SDK

Thank you for your interest in contributing to the Almanak SDK. This guide covers development setup, coding standards, and the pull request process.

## Reporting Issues

- **Bugs**: Open an issue on [GitHub Issues](https://github.com/almanak-co/sdk/issues) with steps to reproduce, expected vs actual behavior, and your environment (Python version, OS, SDK version).
- **Feature requests**: Open an issue describing the use case and proposed solution.
- **Security vulnerabilities**: Do NOT open a public issue. See [SECURITY.md](SECURITY.md) for responsible disclosure.

## Development Setup

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (package manager)
- [Foundry](https://book.getfoundry.sh/) with Anvil (for on-chain tests)
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/almanak-co/sdk.git
cd sdk

# Install dependencies (including dev extras)
make install-dev

# Verify setup
make typecheck
make test
```

### Running Tests

The project uses `pytest` with several Makefile targets:

```bash
make test              # Run core unit tests
make test-connectors   # Run protocol connector tests
make test-intents      # Run intent compilation tests
make test-coverage     # Run tests with coverage report
make typecheck         # Run mypy type checking
make lint-check        # Run ruff linter (check only)
make lint              # Run ruff linter with auto-fix
```

For a specific test file:

```bash
uv run pytest tests/unit/path/to/test_file.py -v
```

## Code Style

- **Formatter/Linter**: [ruff](https://docs.astral.sh/ruff/)
- **Line length**: 120 characters
- **Type checking**: [mypy](https://mypy-lang.org/) in strict mode
- **Docstrings**: Google style, required for public APIs
- **Imports**: Sorted by ruff (isort-compatible)

Run `make lint` before submitting to auto-fix formatting issues.

## Pull Request Process

1. **Fork** the repository and create a branch from `main`:
   ```bash
   git checkout -b feat/my-feature main
   ```

2. **Keep changes focused** - one logical change per PR. Split large changes into stacked PRs.

3. **Write tests** for new functionality. Maintain or improve existing coverage.

4. **Run the full check suite** before pushing:
   ```bash
   make lint-check
   make typecheck
   make test
   ```

5. **Open a PR** against `main` with:
   - A clear title summarizing the change
   - Description of what and why (not just how)
   - Link to any related issues

6. **Address review feedback** by pushing additional commits (do not force-push during review).

## Commit Messages

Use conventional commit prefixes:

| Prefix | Purpose |
|--------|---------|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `docs:` | Documentation only |
| `test:` | Adding or updating tests |
| `refactor:` | Code change that neither fixes a bug nor adds a feature |
| `chore:` | Build, CI, or tooling changes |

Examples:
```
feat: add Balancer V2 connector
fix: resolve token decimals for bridged USDC on Base
docs: update backtesting README with sweep examples
test: add golden tests for perp funding calculation
```

## What Makes a Good Contribution

- **Bug fixes** with a regression test
- **New protocol connectors** following the existing adapter pattern in `almanak/framework/connectors/`
- **New demo strategies** in `strategies/demo/` with documentation
- **Documentation improvements** - typos, missing examples, clarifications
- **Test coverage** for untested paths

## Code of Conduct

All contributors must follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
