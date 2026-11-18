# Base Aave V3 borrowing and PancakeSwap V3 trading

> Composed Aave V3 borrow then one PancakeSwap V3 swap on Base: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

This reference supplies 0.01 WETH to Aave V3, borrows 5 USDC and trades 2.5
USDC through the configured PancakeSwap V3 pool. The initial 1 USDC cash reserve
is smaller than the trade, so borrowed proceeds must reach the shared fund
inventory before the swap. Teardown reverses the trade, clears exchange
authority, repays debt and withdraws collateral.

The author configuration selects both markets and their exact assets on
`eip155:8453`. The economic configuration requests both archive sources and
excludes costs explicitly. It does not select a vendor or assume a stablecoin
peg. `almanak support --connector aave_v3 --chain eip155:8453` and `almanak support --connector pancakeswap_v3 --chain eip155:8453` state the declarations; fork proofs use simulated funds.
