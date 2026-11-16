# Uniswap V3 Arbitrum LP

> Fixed-range Uniswap V3 LP on Arbitrum USDC/WETH: swap half the USDC, mint, increase, partially decrease, collect, hold, then exit to USDC.

Reference on Arbitrum One (`eip155:42161`). Uses the explicitly configured native USDC/WETH 0.05% pool and verified factory, SwapRouter02, QuoterV2 and position-manager revisions.

The single author configuration fixes the fund, inventory, fixed tick range, five-minute hold and one-minute decision interval. The separate economic backtest configuration freezes archival pool observations and integer tick coverage. Historical LP fees use fixed external fee growth without hypothetical dilution or fees from simulated trades. Execution costs are explicitly excluded from economic results.

The configuration carries an Arbitrum-specific gas policy; do not copy it to another chain. Fork execution uses generated Safe/Roles authority on an identified Anvil fixture. `almanak support --connector uniswap_v3 --chain eip155:42161` states the declaration; fork proofs use simulated funds.
