# Uniswap V4 Avalanche trading

> Single Uniswap V4 swap on a hookless Avalanche WAVAX/USDC PoolKey: buy WAVAX with USDC, hold for a fixed time, then sell back to USDC.

A named Avalanche Safe swaps configured USDC capital, holds purchased inventory and exits back to USDC.

[strategy.py](strategy.py) and [config.yaml](config.yaml) are the author inputs.
The configuration explicitly selects the pool, CAIP assets, funding, limits and
timers. Providers, account bindings and the fork fixture are operator inputs in
[examples/operators](../../examples/operators/). Resolved market facts and
generated deployment files are runtime artifacts.

## Run on a fork

The selected `.env` must contain `ALCHEMY_API_KEY`. Install `uv`, Foundry `anvil`
and the supported local strategy sandbox as described in the
[getting-started guide](../../docs/getting-started.md).
From the repository root:

```sh
uv run --locked almanak check strategies/uniswap_v4_avalanche_trading --fork --online \
  --gateway-config examples/operators/uniswap_v4_avalanche_trading.fork.yaml --env-file .env
uv run --locked almanak run strategies/uniswap_v4_avalanche_trading --fork \
  --gateway-config examples/operators/uniswap_v4_avalanche_trading.fork.yaml \
  --env-file .env --output runs/uniswap_v4_avalanche_trading
```

The online check resolves the configured pool and assets through a read-only
gateway, runs the strategy hooks on the resolved parameters and previews the
compiled fixture. It installs no authority. The run freezes those inputs in the
workspace before provisioning the identified fork fixture.

Ctrl-C pauses the coordinator and keeps services and positions available.
Use `resume --run runs/uniswap_v4_avalanche_trading` to continue, `teardown --run runs/uniswap_v4_avalanche_trading`
to request closure, and `status --run runs/uniswap_v4_avalanche_trading` to inspect retained progress.
After closure, `shutdown --run runs/uniswap_v4_avalanche_trading` stops services. A stopped fork
cannot be replaced on resume.

Safes own protocol positions and proceeds. Generated Zodiac Roles authority binds
the configured operators, who submit and pay gas. Owners remain separate.
No owner-key or EOA fallback is used for Safe execution.

## Historical backtest

[backtest-economic.yaml](backtest-economic.yaml) explicitly selects the prepared
model, window, hypothetical inventory, cadence, sources, resolution and costs.

```sh
uv run --locked almanak backtest run strategies/uniswap_v4_avalanche_trading \
  --backtest-config strategies/uniswap_v4_avalanche_trading/backtest-economic.yaml \
  --gateway-config examples/operators/backtest/eip155-43114.yaml \
  --env-file .env --output runs/uniswap_v4_avalanche_trading-backtest
uv run --locked almanak backtest reproduce --run runs/uniswap_v4_avalanche_trading-backtest
uv run --locked almanak report --run runs/uniswap_v4_avalanche_trading-backtest \
  --format html --output uniswap_v4_avalanche_trading.html
```

Preparation uses the explicit operator provider; economic execution runs
offline. The [backtesting guide](../../docs/backtesting.md) covers dataset reuse,
model limits, progress and reproduction. Hypothetical activity does not modify
later market history.

[backtest.yaml](backtest.yaml) selects the detailed historical model. It retains archival Quoter and operation-cost assumptions.

See [authoring](../../docs/authoring.md), [configuration](../../docs/configuration.md)
and [operations](../../docs/operating.md) for current interfaces and production
prerequisites. `almanak support --connector uniswap_v4 --chain eip155:43114` states the
declaration; fork proofs use simulated funds.
