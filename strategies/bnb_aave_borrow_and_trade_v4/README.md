# BNB Aave V3 and Uniswap V4 borrowing and trading

> Composed Aave V3 borrow then one Uniswap V4 swap on BNB Chain: supply WBNB, borrow USDT, swap part of the loan to WBNB, hold, swap back, repay, withdraw.

One fund supplies WBNB, borrows 5 USDT, trades 2.5 USDT from a 1-USDT initial
reserve, reverses the trade, repays and withdraws collateral. Both markets and
financial data sources are explicit. `almanak support --connector aave_v3 --chain eip155:56`
and `almanak support --connector uniswap_v4 --chain eip155:56` state the
declarations; fork proofs use simulated funds.
