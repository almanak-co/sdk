# Polygon Aave borrowing and Uniswap V4 trading

> Composed Aave V3 borrow then one Uniswap V4 swap on Polygon: supply WETH, borrow USDC, swap part of the loan to WETH, hold, swap back, repay, withdraw.

One fund supplies WETH to Aave V3, borrows 5 USDC and trades 2.5 USDC through the
configured USDC/WETH fee-500 V4 pool; teardown reverses the trade before repaying
debt and releasing collateral. This actual reference uses the explicit eip155:137
pool selected from Alchemy archive block 93460480. Exact compiled contract revisions
and both quote directions are retained. `almanak support --connector aave_v3 --chain eip155:137`
and `almanak support --connector uniswap_v4 --chain eip155:137` state the
declarations; fork proofs use simulated funds.
