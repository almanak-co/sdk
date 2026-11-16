# Avalanche Uniswap V3 LP

> Fixed-range Uniswap V3 LP on Avalanche WAVAX/USDC: swap half the USDC, mint, increase, partially decrease, collect, hold, then exit to USDC.

WAVAX/USDC reference using the configured fee-500 pool; providers and the fork
fixture are operator inputs.
`almanak support --connector uniswap_v3 --chain eip155:43114` states the
declaration; fork proofs use simulated funds.
The LP reference explicitly requests direct close before residual cleanup.
