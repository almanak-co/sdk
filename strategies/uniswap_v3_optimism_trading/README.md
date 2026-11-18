# Uniswap V3 Optimism trading

> Single Uniswap V3 swap on Optimism USDC/WETH: buy WETH with all the USDC once, hold for a fixed time, then sell back to USDC and revoke approvals.

Reference on Optimism (`eip155:10`) using the explicitly configured native USDC/WETH 0.05% pool. The five-minute hold and one-minute decision interval belong to the author configuration. Economic history uses explicit archival pool and tick coverage. `almanak support --connector uniswap_v3 --chain eip155:10` states the declaration; fork proofs use simulated funds.
