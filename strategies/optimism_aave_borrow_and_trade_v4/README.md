# Optimism Aave borrowing and Uniswap V4 trading

> Composed Aave V3 borrow then one Uniswap V4 swap on Optimism: supply WETH, borrow USDC, swap part of the loan to WETH, hold, swap back, repay, withdraw.

The actual strategy supplies WETH, borrows native USDC, trades part of the proceeds through the configured V4 pool, reverses, repays and releases collateral. One fund owns shared cash and debt; the operator pays gas. Inputs come from the immutable proved Aave reference and independent V4 deployment discovery. `almanak support --connector aave_v3 --chain eip155:10` and `almanak support --connector uniswap_v4 --chain eip155:10` state the declarations; fork proofs use simulated funds.
