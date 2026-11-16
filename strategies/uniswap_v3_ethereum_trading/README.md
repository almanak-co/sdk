# Uniswap V3 Ethereum trading

> Single Uniswap V3 swap on Ethereum USDC/WETH: buy WETH with all the USDC once, hold for a fixed time, then sell back to USDC and revoke approvals.

Reference using the explicit USDC/WETH pool and capital in `config.yaml`;
providers and the fork fixture are operator inputs.
`almanak support --connector uniswap_v3 --chain eip155:1` states the
declaration; fork proofs use simulated funds.

Buy WETH, hold, then return retained inventory to USDC and revoke approvals.
Strategy memory records completed economic actions; the connector owns
inventory accounting. Restart retains both. Teardown exits outstanding obligations
regardless of the normal hold period.

`backtest-economic.yaml` explicitly selects archival Uniswap V3 pool data,
60-second samples, a 72-second maximum age, bounded tick coverage and collection
limits. Decisions retain the author's decision interval. Costs are explicitly
excluded. These inputs require preparation and verification before they
establish a historical lifecycle proof.
