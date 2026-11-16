# BNB Chain Uniswap V4 trading

> Single Uniswap V4 swap on a hookless BNB Chain USDT/WBNB PoolKey: buy WBNB with USDT, hold for a fixed time, then sell back to USDT.

This reference uses the explicit USDT/WBNB fee-500 pool and source-verified
periphery on `eip155:56`. It starts with 5 USDT units, and retains the actual
asset identities, fixed strategy cadence and bounded historical tick coverage.
Providers and the fork fixture are operator inputs.
`almanak support --connector uniswap_v4 --chain eip155:56` states the
declaration; fork proofs use simulated funds.
