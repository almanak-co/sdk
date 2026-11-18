# Uniswap V3 Ethereum LP

> Fixed-range Uniswap V3 LP on Ethereum USDC/WETH: swap half the USDC, mint, increase, partially decrease, collect, hold, then exit to USDC.

Reference using the explicit USDC/WETH pool, fixed range and capital in
`config.yaml`; providers and the fork fixture are operator inputs.
`almanak support --connector uniswap_v3 --chain eip155:1` states the
declaration; fork proofs use simulated funds.

Swap part of USDC capital, mint with half the available inventory, increase, partially decrease, collect, hold, then close, return retained inventory to USDC and revoke approvals.
Strategy memory records completed economic actions; the connector owns NFT and
inventory accounting. Restart retains both. Teardown exits outstanding obligations
regardless of the normal hold period.

`backtest-economic.yaml` explicitly selects archival Uniswap V3 pool data,
60-second samples, a 72-second maximum age, bounded tick coverage and collection
limits. Decisions retain the author's decision interval. Costs are explicitly
excluded. LP fees use fixed historical fee growth without hypothetical dilution
or fees from the strategy's own trades. These inputs require preparation and
verification before they establish a historical lifecycle proof.
