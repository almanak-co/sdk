# Polygon Uniswap V3 trading

> Single Uniswap V3 swap on Polygon USDC/WETH: buy WETH with all the USDC once, hold for a fixed time, then sell back to USDC and revoke approvals.

USDC/WETH reference using an explicitly configured fee-500 pool; providers and the
fork fixture are operator inputs.
`almanak support --connector uniswap_v3 --chain eip155:137` states the
declaration; fork proofs use simulated funds.
