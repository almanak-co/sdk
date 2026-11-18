# Robinhood Uniswap V3 LP

> Fixed-range Uniswap V3 LP on Robinhood Chain WETH/USDG: swap half the USDG, mint, increase, partially decrease, collect, hold, then exit to USDG.

WETH/USDG reference using an explicitly configured fee-500 pool; providers and the
fork fixture are operator inputs.
`almanak support --connector uniswap_v3 --chain eip155:4663` states the
declaration; fork proofs use simulated funds.
