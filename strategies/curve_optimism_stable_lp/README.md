# Optimism Curve stable LP

> Curve StableSwap NG LP on Optimism crvUSD/USDC: two funds swap, add, increase, partially remove, hold, then exit proportionally or to USDC.

This reference uses the reusable Curve author helper with two named fund accounts.
It swaps, adds liquidity, increases, partially removes and closes liquidity, then
reverses the remaining trade. One account exits proportionally; the other uses a
single-coin exit. The configuration selects the pool, ordered coins and explicit
funding. Operators pay gas independently from fund assets.

`backtest-economic.yaml` prepares explicit archive observations for offline integer
simulation. External market history stays fixed. Costs are explicitly excluded
from this sample backtest; native fork reports retain payer costs. Missing inputs
refuse simulation.

`almanak support --connector curve --chain eip155:10` states the declaration;
fork proofs use simulated funds.
