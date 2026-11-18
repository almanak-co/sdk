# Polygon PoS Curve TriCrypto LP

> Curve TriCrypto NG (January 2024) LP on Polygon PoS PNT/USDC/SAND: two funds swap, add, increase, partially remove, hold, then exit proportionally or to USDC.

This reference uses the reusable Curve author helper with two named fund accounts.
It swaps, opens liquidity, increases, partially removes and closes liquidity, then
reverses the remaining trade. One account exits proportionally; the other uses a
single-coin exit. The author configuration selects the pool, ordered assets and
explicit funding. Operators pay native POL gas independently from fund assets. Bridged WETH is not a native wrapping operation.

The required backtest configuration prepares explicit archive observations for
offline integer simulation. External market history stays fixed. This sample
explicitly excludes simulated costs; fork reports retain measured payer costs.
Missing inputs refuse simulation.

`almanak support --connector curve --chain eip155:137` states the declaration;
fork proofs use simulated funds.
