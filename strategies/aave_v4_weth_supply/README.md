# Aave V4 WETH supply and borrowing

> Aave V4 on Ethereum: two funds supply WETH to the Main and Bluechip Spokes; optional Main USDC borrow with a health-factor unwind above the floor.

Two named Ethereum funds supply independent Main and Bluechip WETH allocations. Optional Main USDC borrowing holds proceeds and repays accrued debt before collateral release.

[strategy.py](strategy.py) and [config.yaml](config.yaml) are the author inputs.
The configuration names the Aave markets by chain, Spoke and reserve, the funding
accounts by chain, and the economic choices: amounts, native balance floors,
timers and borrowing bounds. Providers, account bindings and the fork fixture are
operator inputs in [examples/operators](../../examples/operators/). Resolved market
facts and generated deployment files are runtime artifacts.

The optional debt unwinds (repay, then withdraw) once the health factor falls below
`minimum_health_factor` plus a margin (`UNWIND_MARGIN` in `strategy.py`), not at
the floor itself: the floor also refuses the unwind's own withdrawals, so the
trigger sits strictly above it. Borrowing also stops below that trigger.

## Run on a fork

The selected `.env` must contain `ALCHEMY_API_KEY`. Install `uv`, Foundry `anvil`
and the supported local strategy sandbox as described in the [setup guide](../../README.md).
From the repository root:

```sh
uv run --locked almanak check strategies/aave_v4_weth_supply --fork --online \
  --gateway-config examples/operators/aave_v4_weth_supply.fork.yaml --env-file .env
uv run --locked almanak run strategies/aave_v4_weth_supply --fork \
  --gateway-config examples/operators/aave_v4_weth_supply.fork.yaml \
  --env-file .env --output runs/aave_v4_weth_supply
```

The online check resolves the current Spoke, Hub and asset facts through a
read-only gateway, runs the strategy hooks once on them and reports the compiled
fixture; it installs nothing. The run freezes the same inputs in the workspace,
provisions the fixture accounts and admits execution.

Ctrl-C pauses the coordinator and keeps services and positions available.
Use `resume --run runs/aave_v4_weth_supply` to continue, `teardown --run runs/aave_v4_weth_supply`
to request closure, and `status --run runs/aave_v4_weth_supply` to inspect retained progress.
After closure, `shutdown --run runs/aave_v4_weth_supply` stops services. A stopped fork
cannot be replaced on resume.

Safes own protocol positions and proceeds. Generated Zodiac Roles authority binds
the configured operators, who submit and pay gas. Owners remain separate.
No owner-key or EOA fallback is used for Safe execution.

## Historical backtest

[backtest-economic.yaml](backtest-economic.yaml) explicitly selects the prepared
model, window, hypothetical inventory, cadence, sources, resolution and costs.

```sh
uv run --locked almanak backtest run strategies/aave_v4_weth_supply \
  --backtest-config strategies/aave_v4_weth_supply/backtest-economic.yaml \
  --gateway-config examples/operators/backtest/eip155-1.yaml \
  --env-file .env --output runs/aave_v4_weth_supply-backtest
uv run --locked almanak backtest reproduce --run runs/aave_v4_weth_supply-backtest
uv run --locked almanak report --run runs/aave_v4_weth_supply-backtest \
  --format html --output aave_v4_weth_supply.html
```

Preparation resolves the markets at the window start through the operator's
providers and retains those facts with the dataset; economic execution and
reproduction run offline. The [backtesting guide](../../docs/backtesting.md)
covers dataset reuse, model limits, progress and reproduction. Hypothetical
activity does not modify later market history.

[backtest.yaml](backtest.yaml) selects the detailed historical model. For Aave, remove optional debts from author parameters before using this supply/withdraw-only model.

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
