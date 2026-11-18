# Arbitrum Aave V3 borrowing and Uniswap V4 trading

> Composed Aave V3 borrow then one hookless Uniswap V4 swap on Arbitrum: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

The actual mixed strategy supplies WETH, borrows native USDC, uses borrowed proceeds to trade in the explicitly configured V4 fee-500 pool, then reverses before repayment and collateral release. Initial USDC provides a repayment buffer. One named fund owns all shared cash and lending obligations; the operator pays gas.

This reference reuses the verified mixed strategy body with Arbitrum lending and V4 targets. Generated Safe/Roles permissions install on identified fork fixtures. `almanak support --connector aave_v3 --chain eip155:42161` and `almanak support --connector uniswap_v4 --chain eip155:42161` state the declarations; fork proofs use simulated funds.
