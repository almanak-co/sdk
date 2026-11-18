# Polygon Aave borrowing and Curve trading

> Composed Aave V3 borrow then one Curve StableSwap NG swap on Polygon: supply WETH, borrow USDC, swap part to crvUSD, hold, swap back, repay, withdraw.

Composed reference: one fund supplies Aave collateral, borrows, trades 2.5 of 5 borrowed units, reverses, repays and withdraws. Explicit configured market, source and repayment reserve. `almanak support --connector aave_v3 --chain eip155:137` and `almanak support --connector curve --chain eip155:137` state the declarations; fork proofs use simulated funds.
