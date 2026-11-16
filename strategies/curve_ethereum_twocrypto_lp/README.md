# Ethereum Curve TwoCrypto NG LP

> Curve TwoCrypto NG LP on Ethereum WETH/SDT: two funds swap, add, increase, partially remove, hold, then exit proportionally or to WETH.

Reference for the verified WETH/SDT TwoCrypto NG v2.1.0 pool.
Each of two named funds swaps wrapped ETH into SDT, opens and increases
fungible LP exposure, partially removes it, holds across restart, and exits.
One fund removes proportionally; the other selects wrapped ETH. Remaining
SDT is exchanged into wrapped ETH before allowance cleanup.

Author inputs are `strategy.py` and `config.yaml`; `backtest-economic.yaml`
selects the prepared window, sample interval, source age, inventory and exclusions.
The sample uses existing pool liquidity and requires funds without unrelated
LP shares. Automatic admin coin transfers are attributed separately to the
retained fee receiver. `almanak support --connector curve --chain eip155:1`
states the declaration; fork proofs use simulated funds.
