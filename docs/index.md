# Almanak SDK

Almanak is a Python SDK for writing, checking, backtesting and operating
autonomous DeFi strategies. A strategy consists of two author-maintained files:
`strategy.py`, which makes decisions, and `config.yaml`, which states the
economic choices the strategy may use.

The same strategy definition runs in historical evaluation, identified forks
and supported production environments. The SDK keeps network access, secrets,
signing, durable execution and protocol mechanics outside strategy code.

## Learn how it works

- [How the SDK works](how-it-works.md) explains the decision loop, trusted
  components, durable state and the differences between backtest, fork and
  production runs.
- [Getting started](getting-started.md) walks through a complete Uniswap V4
  trading reference, validates it offline, and then runs a backtest or optional
  fork rehearsal.

Start here if you are evaluating the SDK or need to understand why its author,
operator and execution boundaries are separate.

## Build a strategy

- [Strategy authoring](authoring.md) is the contract for hooks, decision
  context, typed memory, outcomes and teardown.
- [Configuration](configuration.md) describes author YAML, exact quantities,
  named accounts and markets, and trusted resolution.
- [Backtesting](backtesting.md) covers explicit models, prepared data and
  offline reproduction.
- [Example strategies](examples.md) provides a curated route through the
  executable references by concept and complexity.
- [Supported connectors](support.md) is generated from the declarations used by
  admission. Use it to verify a connector, chain, action and run mode before
  choosing an example.

These pages are the preferred path for both human authors and coding agents.
Complete executable behavior remains in the linked reference strategies.

## Operate and inspect

- [Operating](operating.md) covers private operator configuration, retained
  workspaces, pause, resume, teardown, shutdown and production prerequisites.
- [CLI reference](cli/almanak.md) lists every command and option from the current
  parser.
- [API reference](api/authoring.md) documents the public Python contracts used
  by strategy code.
- [JSON schemas](api/schemas.md) are the machine-readable author and backtest
  configuration contracts.
- [Daily QA](qa.md) describes the evidence and coverage used to qualify declared
  support.

## Core guarantees

Safe funds own protocol positions and proceeds. Operators submit and pay gas;
EOA execution is also supported. Strategies cannot access secrets, sign or
contact providers. The gateway independently checks authority, while the runtime
retains pending identities, accounting evidence and outcome delivery.

Every action keeps its named fund, CAIP-2 chain and CAIP-19 asset scope. Missing
measurements remain missing rather than becoming zero or an unrequested fallback.

The package is published on PyPI as `almanak` from version 3.0.0. The import
module is also `almanak`.
