# Ethereum Aave V4 borrowing and Uniswap V4 trading

> Composed Aave V4 borrow then one Uniswap V4 swap on Ethereum: supply WETH, borrow USDC, swap part of the loan to WETH, hold, swap back, repay, withdraw.

One named fund supplies collateral, borrows 5 USDC and trades 2.5 USDC. One initial
USDC remains an explicit repayment buffer in addition to the untraded proceeds.
The trade reverses before repayment and collateral withdrawal. Failed reversal
retains the lending obligation. The runtime owns shared cash and protocol
positions separately from strategy memory.

`almanak support --connector aave_v4 --chain eip155:1` and
`almanak support --connector uniswap_v4 --chain eip155:1` state the declarations;
fork proofs use simulated funds.
