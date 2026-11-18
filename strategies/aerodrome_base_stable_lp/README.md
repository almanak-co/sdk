# Aerodrome Base stable LP

> Aerodrome stable-pool LP on Base USDC/USDbC with fungible pool shares: swap half, add, increase, partially withdraw, claim fees, hold, then exit to USDC.

Reference for the explicitly configured stable Base pool. The author configuration fixes capital, the entry slice and cadence; providers and the fork fixture are operator inputs. Fork execution uses a dedicated Safe and generated Zodiac Roles permissions. `almanak support --connector aerodrome --chain eip155:8453` states the declaration; fork proofs use simulated funds.

LP shares are the pool ERC-20 asset. Uncollected fees remain a separate fund claim after withdrawal; teardown claims fees, returns counter-currency inventory and clears allowances. No staking, rewards, assumed peg or common-currency valuation is included.

`backtest-economic.yaml` explicitly selects archive sampling and hypothetical
inventory. It preserves the author configuration and decision interval, then
exits through the short run's end treatment. Pool fees and pause state are
sampled within a fixed age limit. LP principal and fees use fixed external supply
and fee indices; own-trade fees, staking, rewards and gas costs are excluded.

