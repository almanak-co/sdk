# Aave V4 independent two-chain borrowing

> Aave V4 borrowing on Ethereum and Avalanche, one Safe per chain: supply WETH or WAVAX, borrow USDC, hold, unwind above the health floor, repay, withdraw.

One strategy supplies collateral and borrows independently on Ethereum and Avalanche. Each fund and action retains chain scope; healthy-chain work can continue through an admitted peer-chain outage.

[strategy.py](strategy.py) and [config.yaml](config.yaml) are the author inputs.
The configuration explicitly selects markets, CAIP assets, precision, funding,
timers and execution profiles. Generated deployment files are runtime artifacts.

Each debt unwinds (repay, then withdraw) once its health factor falls below
`minimum_health_factor` plus a margin (`UNWIND_MARGIN` in `strategy.py`), not at
the floor itself: the floor also refuses the unwind's own withdrawals, so the
trigger sits strictly above it. Borrowing also stops below that trigger.

## Run on a fork

The selected `.env` must contain `ALCHEMY_API_KEY` and `AVALANCHE_RPC_URL`. Install `uv`, Foundry `anvil`
and the supported local strategy sandbox as described in the [setup guide](../../README.md).
From the repository root:

```sh
uv run --locked almanak check strategies/aave_v4_multichain_borrowing --fork
uv run --locked almanak run strategies/aave_v4_multichain_borrowing \
  --fork --env-file .env --output runs/aave_v4_multichain_borrowing
```

Ctrl-C pauses the coordinator and keeps services and positions available.
Use `resume --run runs/aave_v4_multichain_borrowing` to continue, `teardown --run runs/aave_v4_multichain_borrowing`
to request closure, and `status --run runs/aave_v4_multichain_borrowing` to inspect retained progress.
After closure, `shutdown --run runs/aave_v4_multichain_borrowing` stops services. A stopped fork
cannot be replaced on resume.

Safes own protocol positions and proceeds. Generated Zodiac Roles authority binds
the configured operators, who submit and pay gas. Owners remain separate.
No owner-key or EOA fallback is used for Safe execution. The configuration carries
one chain-specific gas policy per chain; do not copy either to another chain.

## Historical backtest

[backtest-economic.yaml](backtest-economic.yaml) explicitly selects the prepared
model, window, hypothetical inventory, cadence, sources, resolution and costs.

```sh
uv run --locked almanak backtest run strategies/aave_v4_multichain_borrowing \
  --backtest-config strategies/aave_v4_multichain_borrowing/backtest-economic.yaml \
  --env-file .env --output runs/aave_v4_multichain_borrowing-backtest
uv run --locked almanak backtest reproduce --run runs/aave_v4_multichain_borrowing-backtest
uv run --locked almanak report --run runs/aave_v4_multichain_borrowing-backtest \
  --format html --output aave_v4_multichain_borrowing.html
```

Preparation uses the explicit providers; economic execution runs offline. The
[backtesting guide](../../docs/backtesting.md) covers dataset reuse, model limits,
progress and reproduction. Hypothetical activity does not modify later market history.

See [authoring](../../docs/authoring.md), [configuration](../../docs/configuration.md)
and [operations](../../docs/operating.md) for current interfaces and production
prerequisites. Fork proofs use simulated funds and do not establish funded production evidence.
