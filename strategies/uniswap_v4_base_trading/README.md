# Uniswap V4 Base trading

> Single Uniswap V4 swap on a hookless Base WETH/USDC PoolKey: buy WETH with USDC, hold for a fixed time, then sell back to USDC.

This reference uses the explicitly configured hookless WETH/USDC fee-3000 pool on `eip155:8453`, Router v2.0 and the configured named fund. It buys WETH with USDC, holds and exits to USDC.

Generated Safe/Roles permissions install once on identified Anvil fixtures. The fund owns inventory; the operator pays gas. Reference Safes are dedicated.

Economic backtesting selects archival pool state explicitly, uses integer price impact, and excludes execution costs explicitly. Hypothetical activity does not change later external history. `almanak support --connector uniswap_v4 --chain eip155:8453` states the declaration; fork proofs use simulated funds.
