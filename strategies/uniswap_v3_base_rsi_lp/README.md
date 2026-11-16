# Uniswap V3 Base RSI-gated LP

> Repeating RSI-gated banded Uniswap V3 LP on Base: open a 5%-wide position while hourly RSI-14 is in 35-65; close when it leaves 25-75 or after seven days.

Technical-analysis reference that gates liquidity provision on an indicator. A
concentrated position earns fees while the price oscillates inside it and loses
to holding when the price trends away. The strategy declares the hourly WETH
closes of the Base USDC/WETH pool it provides liquidity in (`PoolPrice`), 120
points, and computes RSI-14 with TA-Lib. While RSI is between 35 and 65 it
converts part of the USDC capital and opens a banded position centred on the
price; once RSI leaves 25 to 75, or after seven days, it closes the position and
returns to USDC. Once the position is closed and the exit has completed, memory
resets and the next RSI reading inside 35 to 65 opens again. `almanak support --connector uniswap_v3 --chain eip155:8453`
states the declaration; fork proofs use simulated funds.

Both rules are levels, not crossings: the same series always gives the same
answer, and holding writes no memory between decisions. An entry that has
started finishes regardless of the next reading. While the price is outside the
band's tick envelope, the centred range would be clamped inside it and would not
contain the tick, so the strategy opens nothing and waits for the price to
return. If an open fails, for example because the price moved after the funding
swap, the next decision swaps to the current ratio again before opening. A short, stale or undefined
window decides nothing. Teardown closes before reading prices; teardown
decisions receive no series.

The band envelope (ticks -200500 to -195500, 5% wide positions) contains the
price at the fork anchor and across the backtest window. `backtest-economic.yaml`
covers 2026-09-19 00:00 to 2026-09-21 06:00 UTC with the bracketed historical
fee-growth model; decisions run every five minutes there, reported as reduced
cadence fidelity. `examples/operators/uniswap_v3_base_rsi_lp.fork.yaml` runs it
on a Base fork at block 50974851, where RSI is 61.8, so it enters.
