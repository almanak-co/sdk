# Avalanche Aave V3 borrowing and Curve trading

> Composed Aave V3 borrow then one Curve StableSwap NG swap on Avalanche: supply WAVAX, borrow USDC, swap part to USDS, hold, swap back, repay, withdraw.

Composed reference: one fund supplies 5 WAVAX as Aave collateral, borrows 5 USDC, trades 2.5 of the borrowed USDC for USDS through the configured Curve pool, reverses, repays and withdraws. Explicit configured market, source and 1 USDC repayment reserve. `almanak support --connector aave_v3 --chain eip155:43114` and `almanak support --connector curve --chain eip155:43114` state the declarations; fork proofs use simulated funds.
