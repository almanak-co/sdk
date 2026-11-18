# Robinhood Morpho borrowing and Uniswap V4 trading

> Composed Morpho Blue borrow then one Uniswap V4 swap on Robinhood: supply WETH, borrow USDG, swap half the loan to WETH, hold, swap back, repay, withdraw.

One fund supplies WETH to the configured Morpho market, borrows 0.05 USDG and trades
0.025 USDG through the configured WETH/USDG fee-500 V4 pool; teardown reverses the
trade before repaying debt and releasing collateral. This actual reference uses the
explicit eip155:4663 pool selected from Alchemy archive block 58238929. Exact
compiled contract revisions and both quote directions are retained.
`almanak support --connector morpho_blue --chain eip155:4663` and
`almanak support --connector uniswap_v4 --chain eip155:4663` state the declarations;
fork proofs use simulated funds.
