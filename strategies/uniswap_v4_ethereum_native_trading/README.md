# Uniswap V4 Ethereum native-ETH trading

> Uniswap V4 swap through a native-ETH PoolKey on Ethereum while the fund holds only WETH: sell WETH for USDC, hold, then return to WETH.

This reference selects the hookless ETH/USDC fee-3000 pool on `eip155:1`, whose PoolKey names native ETH (`address(0)`). The fund holds WETH: capital is WETH, and the fund never sends native value. It buys USDC with WETH, holds and returns to WETH. The reviewed Universal Router unwraps the WETH input and wraps the native output within each swap, so the Safe only ever pays and receives ERC-20s. The fund owns inventory; the operator pays gas. Generated Safe/Roles permissions install once on identified forks.

Prepared simulation uses explicit sampled pool state and integer price impact, with the native leg held as WETH one to one. Costs are explicitly excluded. `almanak support --connector uniswap_v4 --chain eip155:1` states the declaration; fork proofs use simulated funds.
