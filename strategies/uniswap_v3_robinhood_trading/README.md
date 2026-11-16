# Robinhood Uniswap V3 trading

> Single Uniswap V3 swap on Robinhood Chain WETH/USDG: buy WETH with all the USDG once, hold for a fixed time, then sell back to USDG and revoke approvals.

WETH/USDG reference using an explicitly configured fee-500 pool; providers and the
fork fixture are operator inputs.
`almanak support --connector uniswap_v3 --chain eip155:4663` states the
declaration; fork proofs use simulated funds.
