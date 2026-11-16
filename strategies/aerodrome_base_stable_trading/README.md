# Aerodrome Base stable trading

> Single swap on an Aerodrome stable pool on Base: trade all the USDC for USDbC once, hold for a fixed time, then swap back to USDC.

Reference for the explicitly configured stable Base pool. The author configuration fixes capital, the entry slice and cadence; providers and the fork fixture are operator inputs. Fork execution uses a dedicated Safe and generated Zodiac Roles permissions. `almanak support --connector aerodrome --chain eip155:8453` states the declaration; fork proofs use simulated funds.

The strategy swaps the configured entry slice once, holds, then returns the counter-currency inventory to the funding asset and clears allowances. No liquidity authority, staking, rewards, assumed peg or common-currency valuation is included.

`backtest-economic.yaml` explicitly selects archive sampling and hypothetical
inventory. It preserves the author configuration and decision interval, then
exits through the short run's end treatment. Pool fees and pause state are
sampled within a fixed age limit; gas costs are excluded.

