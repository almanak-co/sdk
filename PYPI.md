# Almanak SDK

Almanak is a Python SDK for writing, checking, backtesting and operating
autonomous DeFi strategies. A strategy consists of two author-maintained files:
`strategy.py`, which makes decisions, and `config.yaml`, which states the
economic choices the strategy may use.

The same strategy definition runs in historical evaluation, identified forks
and supported production environments. The SDK keeps network access, secrets,
signing, durable execution and protocol mechanics outside strategy code.

Version 3.0.0 is the first release of the rewritten SDK. Projects built on the
2.x line should pin `almanak<3`.

## Installation

Requires Python 3.12+.

```sh
uv add almanak            # as a project dependency
uv tool install almanak   # the command-line tool only
```

Optional extras:

| Extra | Enables |
|---|---|
| `ta` | Technical analysis over declared strategy price series (numpy, TA-Lib) |
| `code` | Python LSP for the bundled Almanak Code agent (pyright) |

Local strategy hook execution is verified on Linux and requires Bubblewrap
(`sudo apt-get install bubblewrap libseccomp2` on Debian or Ubuntu). Fork runs
also require Foundry's `anvil`.

## Usage

```sh
almanak check path/to/strategy
almanak backtest run path/to/strategy --backtest-config backtest.yaml \
  --gateway-config operator.yaml --env-file .env --output runs/test
almanak run path/to/strategy --fork --gateway-config operator.fork.yaml \
  --env-file .env --output runs/demo
```

`almanak support` lists the connectors, chains, actions and run modes that
admission accepts.

## Documentation

Guides, the CLI reference and the strategy API are at
[docs.almanak.co](https://docs.almanak.co).
