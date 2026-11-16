# Uniswap V4 Base LP

> Full-range Uniswap V4 LP on a hookless Base WETH/USDC PoolKey: swap, mint one NFT, collect fees once, hold, then close and return to USDC.

This reference uses the explicitly configured hookless WETH/USDC fee-3000 pool on `eip155:8453`, Router v2.0 and the configured named fund. It swaps USDC, opens liquidity at the explicitly selected full usable ticks `[-887220, 887220]`, collects, closes the NFT and exits residual inventory.

Generated Safe/Roles permissions install once on identified Anvil fixtures. The fund owns assets and positions; the operator pays gas. LP exit grants allow compatible NFT IDs inside bounded recipient templates; the gateway admits only the retained strategy NFT, so sample Safes are dedicated.

Economic backtesting selects archival pool state explicitly, uses integer price impact and fixed historical fee growth, and excludes execution costs explicitly. Hypothetical activity does not change later external history. `almanak support --connector uniswap_v4 --chain eip155:8453` states the declaration; fork proofs use simulated funds.
