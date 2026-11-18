# Optimism Aave borrowing and Uniswap V3 trading

> Composed Aave V3 borrow then one Uniswap V3 swap on Optimism: supply WETH, borrow USDC, swap part of the loan to WETH, hold, swap back, repay, withdraw.

Composed reference: one fund supplies Aave collateral, borrows, trades 2.5 of 5 borrowed units, reverses, repays and withdraws. Explicit configured market, source and repayment reserve. `almanak support --connector aave_v3 --chain eip155:10` and `almanak support --connector uniswap_v3 --chain eip155:10` state the declarations; fork proofs use simulated funds.
