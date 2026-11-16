# Robinhood Curve StableSwap NG LP

> Curve StableSwap NG LP on Robinhood Chain USDG/pUSD: two funds swap, add, increase, partially remove, hold, then exit proportionally or to pUSD.

Reference for the configured USDG/pUSD plain StableSwap pool on `eip155:4663`, using separately named proportional and single-coin LP exit funds. Initial capital is 0.01 pUSD per fund. Symbols describe assets only; no peg is assumed. Fork capital uses exact verified pUSD runtime and its compiled slot-0 balance layout in an identified Anvil fixture. `almanak support --connector curve --chain eip155:4663` states the declaration; fork proofs use simulated funds.

The configured USDG/pUSD pool `0xaef38aec2b7daf4df4561a61e82b57d710e5b60b` is the only USDG/pUSD pool the reviewed Robinhood StableSwap NG factory registers. It never held more than about 3 USD and has held no balances or LP supply since about 15 September 2026, so `ax pool` reports it as `unfunded_pool` and fork and production checks refuse it until it is funded again. Its backtest window predates that; this reference demonstrates the connector, not a market to earn fees in.
