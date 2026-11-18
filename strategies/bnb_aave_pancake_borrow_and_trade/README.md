# BNB Aave borrowing and PancakeSwap V3 trading

> Composed Aave V3 borrow then one PancakeSwap V3 swap on BNB Chain: supply WBNB, borrow USDT, swap part of the loan to WBNB, hold, swap back, repay, withdraw.

The shared fund supplies all configured initial collateral, borrows 5 units of
the configured loan asset, and trades 2.5 units through the explicit pool. The
initial loan cash is 1 unit, so the swap depends on borrowed proceeds. Pool
currencies retain their separate identities, including a third asset where
configured. Teardown reverses the trade, clears exchange authority, repays the
loan and withdraws collateral.

The separate economic configuration requests both archive sources explicitly.
Costs are excluded explicitly. No common valuation or token peg is assumed.
`almanak support --connector aave_v3 --chain eip155:56` and
`almanak support --connector pancakeswap_v3 --chain eip155:56` state the
declarations; fork proofs use simulated funds.
