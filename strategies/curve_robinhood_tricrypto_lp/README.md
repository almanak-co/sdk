# Robinhood Curve TriCrypto LP

> Curve TriCrypto NG (January 2024) LP on Robinhood Chain NVDA/AMD/SNDK: two funds swap, add, increase, partially remove, hold, then exit proportionally or to NVDA.

This reference uses the reusable Curve author helper with two named fund accounts.
Each starts with 0.0001 NVDA stock token, swaps part into AMD, adds liquidity,
increases, partially removes and closes liquidity, then reverses the remaining
trade. One account exits proportionally; the other uses an NVDA-only exit. The
configuration selects the pool, ordered coins and explicit funding. Operators
pay gas independently from fund assets. Symbols describe assets only; no peg or
external valuation is assumed.

The pool is a TriCrypto core v2.0.0 deployment using the January 4, 2024
pure-ERC-20 revision (`tricrypto_ng_2024`), not the earlier native-coin custody
revision. Fork capital uses the guarded, source-verified stock-token funding
fixture.

`backtest-economic.yaml` prepares explicit archive observations for offline integer
simulation. External market history stays fixed. Costs are explicitly excluded
from this sample backtest; native fork reports retain payer costs. Missing inputs
refuse simulation.

`almanak support --connector curve --chain eip155:4663` states the declaration;
fork proofs use simulated funds.
