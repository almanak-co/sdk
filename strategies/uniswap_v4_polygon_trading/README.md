# Polygon Uniswap V4 trading

> Single Uniswap V4 swap on a hookless Polygon USDC/WETH PoolKey: buy WETH with USDC, hold for a fixed time, then sell back to USDC.

This reference uses the explicit hookless fee-500 USDC/WETH pool on `eip155:137`; the operator fork fixture pins Alchemy archive block 93460480. Exact compiled contract revisions and both quote directions are retained. `almanak support --connector uniswap_v4 --chain eip155:137` states the declaration; fork proofs use simulated funds.
