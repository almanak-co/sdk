# Arbitrum Aave V3 borrowing and Curve trading

> Composed Aave V3 borrow then one Curve StableSwap NG swap on Arbitrum: supply WETH, borrow USDC, swap part to crvUSD, hold, swap back, repay, withdraw.

This actual reference supplies its configured WETH collateral, borrows five USDC,
swaps 2.5 USDC through the explicitly configured Curve StableSwap NG pool, and
holds the acquired asset in the same named fund. It keeps the remaining borrowed
USDC and one unit of explicitly funded USDC as repayment reserve. No peg or price
source substitution is assumed.

Restart preserves the shared inventory and debt. Teardown reverses the trade
before repayment and collateral withdrawal. A refused reversal leaves the lending
obligations in place. Generated Safe/Roles authority comes from the sandboxed
capability hook; EOA execution uses the same connector path.

The separate backtest YAML selects archival Aave and Curve observations, explicit
sampling, age and collection bounds, latency and excluded gas costs. Execution
uses only the prepared dataset. These two-connector proofs do not add new
protocol implementations. `almanak support --connector aave_v3 --chain eip155:42161` and `almanak support --connector curve --chain eip155:42161` state the declarations; fork proofs use simulated funds.
