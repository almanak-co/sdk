# Uniswap V4 Arbitrum trading

> Single Uniswap V4 swap on a hookless Arbitrum WETH/USDC PoolKey: buy WETH with USDC, hold for a fixed time, then sell back to USDC.

This reference selects the configured hookless WETH/USDC fee-500 pool on `eip155:42161`. It buys WETH, holds and returns to USDC. The fund owns inventory; the operator pays gas. Generated Safe/Roles permissions install once on identified forks. Reference Safes are dedicated.

Prepared simulation uses explicit sampled pool state and integer price impact. Costs are explicitly excluded. `almanak support --connector uniswap_v4 --chain eip155:42161` states the declaration; fork proofs use simulated funds.
