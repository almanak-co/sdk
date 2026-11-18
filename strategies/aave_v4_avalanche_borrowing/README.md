# Aave V4 Avalanche borrowing

> Aave V4 borrowing on Avalanche from a Safe: supply WAVAX, borrow USDC, hold, with a health-factor unwind above the floor, then repay and withdraw.

A named Avalanche Safe supplies WAVAX collateral, borrows USDC, holds proceeds, repays and releases collateral.

[strategy.py](strategy.py) and [config.yaml](config.yaml) are the author inputs.
The configuration explicitly selects markets, CAIP assets, precision, funding,
timers and execution profiles. Generated deployment files are runtime artifacts.

The loan unwinds (repay, then withdraw) once the health factor falls below
`minimum_health_factor` plus a margin (`UNWIND_MARGIN` in `strategy.py`), not at
the floor itself: the floor also refuses the unwind's own withdrawals, so the
trigger sits strictly above it. Borrowing also stops below that trigger.

## Run on a fork

The selected `.env` must contain `AVALANCHE_RPC_URL`. Install `uv`, Foundry `anvil`
and the supported local strategy sandbox as described in the [setup guide](../../README.md).
From the repository root:

```sh
uv run --locked almanak check strategies/aave_v4_avalanche_borrowing --fork
uv run --locked almanak run strategies/aave_v4_avalanche_borrowing \
  --fork --env-file .env --output runs/aave_v4_avalanche_borrowing
```

Ctrl-C pauses the coordinator and keeps services and positions available.
Use `resume --run runs/aave_v4_avalanche_borrowing` to continue, `teardown --run runs/aave_v4_avalanche_borrowing`
to request closure, and `status --run runs/aave_v4_avalanche_borrowing` to inspect retained progress.
After closure, `shutdown --run runs/aave_v4_avalanche_borrowing` stops services. A stopped fork
cannot be replaced on resume.

Safes own protocol positions and proceeds. Generated Zodiac Roles authority binds
the configured operators, who submit and pay gas. Owners remain separate.
No owner-key or EOA fallback is used for Safe execution. The configuration carries
an Avalanche-specific gas policy; do not copy it to another chain.

## Historical backtest

[backtest-economic.yaml](backtest-economic.yaml) explicitly selects the prepared
model, window, hypothetical inventory, cadence, sources, resolution and costs.

```sh
uv run --locked almanak backtest run strategies/aave_v4_avalanche_borrowing \
  --backtest-config strategies/aave_v4_avalanche_borrowing/backtest-economic.yaml \
  --env-file .env --output runs/aave_v4_avalanche_borrowing-backtest
uv run --locked almanak backtest reproduce --run runs/aave_v4_avalanche_borrowing-backtest
uv run --locked almanak report --run runs/aave_v4_avalanche_borrowing-backtest \
  --format html --output aave_v4_avalanche_borrowing.html
```

Preparation uses the explicit providers; economic execution runs offline. The
[backtesting guide](../../docs/backtesting.md) covers dataset reuse, model limits,
progress and reproduction. Hypothetical activity does not modify later market history.

See [authoring](../../docs/authoring.md), [configuration](../../docs/configuration.md)
and [operations](../../docs/operating.md) for current interfaces and production
prerequisites. Fork proofs use simulated funds and do not establish funded production evidence.

## Loan-asset reserve

Interest is not prefunded: the full repay (Aave V4 has no partial repay) needs the debt
plus the interest accrued since the borrow. A debt allocation has no `capital`, so the
reserve is loan asset funded to the fund beside the collateral: the fork operator file
funds 1 USDC for it, and live, a repay the fund cannot cover refuses with
`repayment_funding` until loan asset is sent to the fund. To state the reserve in the
author config instead, use the composed form with a loan-asset `inventory` entry, as
`avalanche_aave_v4_borrow_and_trade` does.
