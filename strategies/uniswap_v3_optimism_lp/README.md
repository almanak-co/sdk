# Uniswap V3 Optimism LP

> Fixed-range Uniswap V3 LP on Optimism USDC/WETH: swap half the USDC, mint, increase, partially decrease, collect, hold, then exit to USDC.

Reference on Optimism (`eip155:10`) using the explicitly configured native USDC/WETH 0.05% pool. The five-minute hold and one-minute decision interval belong to the author configuration. Economic history uses explicit archival pool and tick coverage with fixed historical LP fees. `almanak support --connector uniswap_v3 --chain eip155:10` states the declaration; fork proofs use simulated funds.
