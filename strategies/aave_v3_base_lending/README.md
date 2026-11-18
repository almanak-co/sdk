# Aave V3 Base lending

> Aave V3 supply-and-borrow lifecycle on Base: supply WETH, enable collateral, borrow USDC, hold, partially repay and withdraw, then close fully.

This reference supplies WETH, toggles its collateral setting, borrows fixed USDC
and holds the proceeds. Teardown partially repays, partially withdraws, then
repays the remaining debt before releasing all collateral and revoking approvals.
The strategy uses the same economic actions as the Ethereum reference.

`config.yaml` explicitly identifies the Base Pool, reserves, oracle feeds,
named fund and capital. Providers, account bindings and the fork fixture are
operator inputs in [examples/operators](../../examples/operators/).
`backtest-economic.yaml` selects a separate short archival window, preparation
bounds and an explicit exclusion of transaction costs. Its economic result
therefore cannot be used as a net-of-fees profitability measurement.

`almanak support --connector aave_v3 --chain eip155:8453` states the
declaration; fork proofs use simulated funds.
