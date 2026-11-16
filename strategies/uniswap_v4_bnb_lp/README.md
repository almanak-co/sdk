# BNB Chain Uniswap V4 LP

> Fixed-range Uniswap V4 LP on a hookless BNB Chain USDT/WBNB PoolKey: swap, mint one NFT, collect fees once, hold, then close and return to USDT.

This reference uses the explicit USDT/WBNB fee-500 pool and source-verified
periphery on `eip155:56`. It starts with 5 USDT units, and retains the actual
asset identities, fixed strategy cadence and bounded historical tick coverage.
Providers and the fork fixture are operator inputs.
`almanak support --connector uniswap_v4 --chain eip155:56` states the
declaration; fork proofs use simulated funds.
