# Almanak SDK

Almanak is a Python SDK for writing, checking, backtesting and operating
autonomous DeFi strategies. A strategy is two author-maintained files:
`strategy.py`, which makes decisions, and `config.yaml`, which states the
economic choices the strategy may use.

The same strategy runs in historical evaluation, on identified forks and in
supported production environments. Strategy code holds no secrets, does not
sign and reaches the network only through a trusted gateway, which checks
execution authority independently. Funds can sit in a Safe with generated
Zodiac Roles permissions or in a plain EOA.

Documentation: [docs.almanak.co](https://docs.almanak.co)

## Install

Requires Python 3.12 or later.

```sh
uv add almanak            # as a project dependency
uv tool install almanak   # the command-line tool only
```

Running strategy code is verified on Linux and needs Bubblewrap with
unprivileged user namespaces and `libseccomp2`
(`sudo apt-get install bubblewrap libseccomp2` on Debian or Ubuntu). Execution
refuses to start without isolation. On macOS the strategy worker currently
refuses to start. Fork runs also need Foundry's `anvil`.

`almanak support` lists the connectors, chains, actions and run modes the SDK
accepts. `almanak support --references` lists the bundled reference strategies.

## First fork run

From a checkout of this repository:

```sh
uv sync --locked
cp .env.example .env      # then set ALCHEMY_API_KEY
uv run --locked almanak check strategies/aave_v4_weth_supply
uv run --locked almanak run strategies/aave_v4_weth_supply --fork \
  --gateway-config examples/operators/aave_v4_weth_supply.fork.yaml \
  --env-file .env --output runs/demo
```

The strategy directory holds the author inputs. The operator file selects
providers, account bindings and the fork fixture, and the env file holds the
provider key; both are private operator inputs. A fork uses simulated funds and
generated Safe and Roles authority.

Ctrl-C pauses the coordinator and leaves the gateway, fork nodes and positions
available. In another terminal:

```sh
uv run --locked almanak status --run runs/demo
uv run --locked almanak resume --run runs/demo
uv run --locked almanak teardown --run runs/demo
uv run --locked almanak report --run runs/demo --format html --output report.html
uv run --locked almanak shutdown --run runs/demo
```

Wait for recorded closure before shutdown. Shutdown refuses unresolved
obligations unless forced. Stopping a fork makes that deployment
non-resumable.

## Backtest

```sh
uv run --locked almanak backtest run strategies/aave_v4_weth_supply \
  --backtest-config strategies/aave_v4_weth_supply/backtest-economic.yaml \
  --gateway-config examples/operators/backtest/eip155-1.yaml \
  --env-file .env --output runs/test
uv run --locked almanak backtest reproduce --run runs/test
uv run --locked almanak report --run runs/test --format html --output backtest.html
```

Economic mode prepares or reuses historical data, then executes offline. The
backtest file selects its window, inventory, sampling, financial sources, costs
and end treatment. It needs no wallet addresses or signing authority.

## Guides

- [Getting started](docs/getting-started.md)
- [How the SDK works](docs/how-it-works.md)
- [Authoring](docs/authoring.md): hooks, named actions and typed state
- [Configuration](docs/configuration.md): YAML, exact quantities and financial identity
- [Backtesting](docs/backtesting.md): models, datasets and reproduction
- [Operating](docs/operating.md): supervision, production credentials and recovery
- [Agents](docs/agents.md): the skill that lets an AI agent manage positions from plain requests
- [Dashboards](docs/dashboards.md)
- [Example strategies](docs/examples.md) and the references in [`strategies/`](strategies)
- [Execution API](docs/api/index.md): runtime, gateways and adapter contracts

## Repository layout

| Path | Contents |
|---|---|
| `src/almanak` | The SDK package |
| `strategies` | Reference strategies, one directory each |
| `examples` | Operator files and resolved market examples for the references |
| `docs` | Guides, JSON schemas (`docs/schemas`) and output contracts (`docs/contracts`) |
| `tests` | Offline tests, fork lifecycles and the QA bench |
| `scripts` | Build, CI, docs and evidence tooling |

## Development

```sh
make install-dev
uv run --locked pre-commit install
make lint-check typecheck test-ci
```

Default pytest is offline. `make test-ci` skips the tests that need retained
evidence, which is not in the repository, and reports how many it skipped; a
skipped test is not a pass. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
checks and the pull request process, [docs/qa.md](docs/qa.md) for the QA bench,
and [AGENTS.md](AGENTS.md) for the project's standing rules.

Fork and backtest proofs do not establish a funded production run. Nothing in
this repository spends funds without an explicit operator invocation.

## Versions

This repository holds the 3.x SDK. The 2.x line is closed and receives no
fixes; its last state is on the [`v2`](https://github.com/almanak-co/sdk/tree/v2)
branch, and projects built on it should pin `almanak<3`.

## Security and license

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
The SDK is licensed under the [Apache License 2.0](LICENSE).
