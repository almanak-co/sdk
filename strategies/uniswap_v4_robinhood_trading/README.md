# Robinhood Uniswap V4 trading

> Single Uniswap V4 swap on a hookless Robinhood Chain WETH/USDG PoolKey: buy WETH with USDG, hold for a fixed time, then sell back to USDG.

This reference uses the explicit hookless fee-500 WETH/USDG pool on `eip155:4663`; the operator fork fixture pins Alchemy archive block 58238929. Exact compiled contract revisions and both quote directions are retained. `almanak support --connector uniswap_v4 --chain eip155:4663` states the declaration; fork proofs use simulated funds.
