# Robinhood Morpho borrowing and Uniswap V3 trading

> Composed Morpho Blue borrow then one Uniswap V3 swap on Robinhood: supply WETH, borrow USDG, swap half the loan to WETH, hold, swap back, repay, withdraw.

One fund supplies configured collateral, borrows and trades part of the proceeds.
Exchange reversal precedes debt repayment and collateral release.
`almanak support --connector morpho_blue --chain eip155:4663` and
`almanak support --connector uniswap_v3 --chain eip155:4663` state the declarations;
fork proofs use simulated funds.
