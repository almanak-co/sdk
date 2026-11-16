# Uniswap V4 Arbitrum LP

> Full-range Uniswap V4 LP on a hookless Arbitrum WETH/USDC PoolKey: swap, mint one NFT, collect fees once, hold, then close and return to USDC.

This reference selects the configured hookless WETH/USDC fee-500 pool on `eip155:42161`. It swaps, mints a full-range NFT, collects, closes and returns to USDC. The fund owns inventory and positions; the operator pays gas. Generated Safe/Roles permissions install once on identified forks. Compatible NFT IDs remain permitted within exact bounded exit templates; the gateway restricts execution to the retained owned NFT. Reference Safes are dedicated.

Prepared simulation uses explicit sampled pool state, integer price impact and fixed historical fee growth. Costs are explicitly excluded. `almanak support --connector uniswap_v4 --chain eip155:42161` states the declaration; fork proofs use simulated funds.
