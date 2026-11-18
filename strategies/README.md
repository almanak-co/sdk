# Reference strategies

This directory contains executable references used to develop and qualify the
SDK. Only the allowlisted references (`PUBLISHED` in
`src/almanak/references/__init__.py`) are listed by `almanak support --references`
and shipped in the wheel; the other directories are test fixtures and QA proofs,
not references to copy. A directory's existence does not establish support. Run
`almanak support` or use the generated [support catalog](https://docs.almanak.co/support.html) to verify the
connector, chain, actions, account profile and run mode.

The public [example guide](../docs/examples.md) provides a curated route through
the library. Good starting points are:

- [Uniswap V4 trading](uniswap_v4_arbitrum_trading/README.md): one named fund buys
  WETH with USDC, holds through restart, exits and revokes allowances.
- [Uniswap V4 full-range LP](uniswap_v4_arbitrum_lp/README.md): acquire both
  currencies, mint, collect fees, close the NFT and return to USDC.
- [Aave V4 WETH supply and borrowing](aave_v4_weth_supply/README.md): multiple
  named positions and funds, optional debt, repayment and independent withdrawal.
- [Avalanche Aave V4 borrowing](aave_v4_avalanche_borrowing/README.md): WAVAX
  collateral, USDC debt, repayment and collateral release.
- [Robinhood Uniswap V3 banded LP](uniswap_v3_robinhood_lp_band/README.md):
  recentre one position inside an owner-approved tick band.
- [Uniswap V3 Base RSI](uniswap_v3_base_rsi/README.md): a declared price series
  and repeating indicator crossings.
- [Aave V3 leverage loop](arbitrum_aave_leverage_loop/README.md): configurable
  rounds in `strategy.parameters` and a health-factor unwind above the floor.

Each complete directory keeps `strategy.py` and `config.yaml` as the author
inputs. A separate backtest YAML selects historical evaluation settings.
Providers, account bindings, fork fixtures and production authority belong in
operator configuration under [`examples/operators`](../examples/operators/).
Resolved facts and generated deployment files are runtime artifacts.

Strategy files define `capabilities(config)` and `decide(context)`. They may
also define validation, dependency, outcome, external-event and dashboard hooks.
The SDK owns execution, state, recovery and accounting. Strategy code can import
the installed public SDK, but local helper files and arbitrary filesystem or
network access are not supported by the sandbox.

The source and effective configuration are captured when a run is created. The
same recorded definition runs after restart and during teardown. Editing the
source affects new deployments only. SDK and dependency versions must remain
fixed while a deployment is open because the source snapshot does not freeze the
entire Python environment.

Every discovered strategy has an explicit daily-QA disposition. Published
references map to lifecycle and historical evidence; other directories remain
visible as fixtures or blocked cases. Missing proof is not converted into support.
See the [daily QA guide](../docs/qa.md) for the executable coverage contract.
