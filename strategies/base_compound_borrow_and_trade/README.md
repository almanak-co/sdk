# Base Compound V3 borrowing and Uniswap V3 trading

> Composed Compound V3 borrow then one Uniswap V3 swap on Base: supply WETH, borrow USDC, swap part to WETH, hold, swap back, repay, withdraw.

One named fund supplies WETH collateral, borrows USDC, trades part of the proceeds,
holds through restart, reverses the trade, repays and releases collateral. Shared
cash is recorded once even when multiple connector targets observe it. The author
configuration retains a 40-USDC repayment reserve and exact configured markets.

The lending position uses Compound signed principal and separately held collateral. It has no Aave
collateral toggle. Generated Safe/Zodiac authority and EOA execution use identified
fork fixtures. Prepared economic settings freeze the archive sources, sampling,
input ages, hypothetical inventory and excluded transaction costs.

Failed reverse trading must retain the loan and collateral obligations; a
successful trade is not assumed during teardown. `almanak support --connector compound_v3 --chain eip155:8453` and `almanak support --connector uniswap_v3 --chain eip155:8453` state the declarations; fork proofs use simulated funds.
