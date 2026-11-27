# Contributing to the Almanak SDK

This guide covers development setup, the checks a change must pass and the pull
request process. [AGENTS.md](AGENTS.md) holds the project's standing rules; read
it before changing execution, authority or financial data paths.

## Reporting issues

- **Bugs**: open an issue at
  [GitHub Issues](https://github.com/almanak-co/sdk/issues) with steps to
  reproduce, expected and actual behavior, and your environment (Python
  version, operating system, SDK version).
- **Feature requests**: open an issue describing the use case.
- **Security vulnerabilities**: do not open a public issue. See
  [SECURITY.md](SECURITY.md).

## Development setup

You need:

- Python 3.12 or later
- [uv](https://docs.astral.sh/uv/)
- Linux with Bubblewrap and `libseccomp2` for strategy isolation
  (`sudo apt-get install bubblewrap libseccomp2` on Debian or Ubuntu)
- [Foundry](https://book.getfoundry.sh/)'s `anvil`, for fork runs only

```sh
git clone https://github.com/almanak-co/sdk.git
cd sdk
make install-dev
uv run --locked pre-commit install
```

macOS can lint, type-check and build, but the strategy worker refuses to start
there, so tests that run author code need Linux.

## Checks

```sh
make lint-check    # Ruff lint and format check
make typecheck     # mypy over src, strategies and scripts
make test-ci       # offline test suite, as release tags run it
make test-changed BASE=origin/main  # what a pull request runs: changed test modules only
make docs-check    # docs navigation and reference checks
make hygiene       # no credentials, private names or workstation paths
```

`make help` lists every target. Run the narrowest tests that cover your change
while developing:

```sh
uv run --locked pytest tests/test_check_warnings.py -n auto
```

Default pytest is offline and needs no provider key. Fork tests need an
explicit provider env file (copy `.env.example` to `.env`):

```sh
uv run --locked pytest tests/strategy_fork -n auto -m strategy_fork --fork-env=.env
```

Some tests read retained evidence that is not in the repository. `make test-ci`
skips them and reports the count; a skipped test is not a pass. `make test` and
`make crap` expect that evidence and fail without it, so they are maintainer
gates.

Every function must keep a CRAP score of 30 or lower, and statement coverage
of `src/` must stay at or above 87%. Pull request CI checks the functions you
changed.

## Code style

- Ruff formats and lints; `make lint` applies fixes.
- mypy must pass.
- Chains and assets are CAIP-2 and CAIP-19 identifiers, never bare chain IDs,
  token addresses or symbols.
- Comments explain non-obvious invariants, units and failure semantics. They
  do not narrate the code.
- Plain language, no emojis and no em dashes in code, comments and docs.

## Pull requests

1. Fork the repository and branch from `main`.
2. Keep each pull request to one logical change, with tests for new behavior
   and a regression test for a bug fix.
3. Run the checks above.
4. Open the pull request against `main`. Say what changes and why, which
   checks you ran, and link the related issue.
5. Address review by adding commits. Do not force-push during review.

Pull requests from forks run without provider secrets.

## What makes a good contribution

- Bug fixes with a regression test
- Documentation fixes and clearer examples
- Test coverage for untested paths
- Reference strategies under `strategies/` for supported connectors

A new connector, chain or intent changes what the SDK is trusted to sign.
Open an issue first so the scope and the evidence it needs can be agreed.

## Code of conduct

All contributors follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
