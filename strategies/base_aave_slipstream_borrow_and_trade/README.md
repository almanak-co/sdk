# Base Aave V3 borrowing and Aerodrome Slipstream trading

> Composed Aave V3 borrow then one Aerodrome Slipstream swap on Base: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

The shared fund supplies all configured initial collateral, borrows 5 units of
the configured loan asset, and trades 2.5 units through the explicit pool. The
initial loan cash is 1 unit, so the swap depends on borrowed proceeds. Pool
currencies retain their separate identities, including a third asset where
configured. Teardown reverses the trade, clears exchange authority, repays the
loan and withdraws collateral.

The separate economic configuration requests both archive sources explicitly.
Costs are excluded explicitly. No common valuation or token peg is assumed.
`almanak support --connector aave_v3 --chain eip155:8453` and `almanak support --connector aerodrome_slipstream --chain eip155:8453` state the declarations; fork proofs use simulated funds.
