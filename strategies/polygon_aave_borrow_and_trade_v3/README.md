# Polygon Aave borrowing and Uniswap V3 trading

> Composed Aave V3 borrow then one Uniswap V3 swap on Polygon: supply WETH, borrow USDC, swap part of the loan to WETH, hold, swap back, repay, withdraw.

One fund supplies configured collateral, borrows and trades part of the proceeds.
Exchange reversal precedes debt repayment and collateral release.
`almanak support --connector aave_v3 --chain eip155:137` and
`almanak support --connector uniswap_v3 --chain eip155:137` state the declarations;
fork proofs use simulated funds.
