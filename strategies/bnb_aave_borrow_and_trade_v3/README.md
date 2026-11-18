# BNB Aave borrowing and Uniswap V3 trading

> Composed Aave V3 borrow then one Uniswap V3 swap on BNB Chain: supply WBNB, borrow USDT, swap part of the loan to WBNB, hold, swap back, repay, withdraw.

One fund supplies configured collateral, borrows and trades part of the proceeds.
Exchange reversal precedes debt repayment and collateral release.
`almanak support --connector aave_v3 --chain eip155:56` and
`almanak support --connector uniswap_v3 --chain eip155:56` state the declarations;
fork proofs use simulated funds.
