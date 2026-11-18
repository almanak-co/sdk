# Uniswap V3 Base ATR breakout

> Repeating ATR breakout from hourly OHLCV swap candles on Base Uniswap V3: buy WETH above the 24-hour channel midline in calm markets, exit on a 2.5-ATR trailing stop.

Technical-analysis reference that reads full candles. It declares the hourly
OHLCV candles of the traded Base USDC/WETH pool (`PoolSwapCandles`), built from
the pool's Swap events, 72 candles, WETH priced in USDC. It buys WETH with the
USDC capital while the latest close sits above the midline of the previous 24
candles' high-low range and ATR-14 is below 2% of the close, and returns it to
USDC once the close falls 2.5 ATR below the highest close since entry, or after
two days. The window spans the 24-candle channel and the whole two-day hold, and
`validate_config` refuses a longer hold, so the highest close since entry is
always inside it. Once the exit has completed, memory resets and the next entry
signal (a close above the channel midline in calm markets) buys again. `almanak support --connector uniswap_v3 --chain eip155:8453`
states the declaration; fork proofs use simulated funds.

Entry and exit are levels, not crossings: the same series always gives the same
answer. The trailing stop's highest close is read from the series since the
entry rather than remembered, so holding writes no memory between decisions. A
signalled buy is marked pending and retried until `on_outcome` sees it complete.
A short, stale or undefined window decides nothing. Teardown exits before
reading prices; teardown decisions receive no series.

`backtest-economic.yaml` covers 2026-09-19 00:00 to 2026-09-20 12:00 UTC with
candles from 72 hours before the start. Decisions run every five minutes there,
reported as reduced cadence fidelity, instead of the configured minute. The run
buys at the first decision and leaves on the trailing stop at 2026-09-20 03:05.
Candles cost one bounded log read per hour of this busy pool; closes-only
strategies (`PoolPrice`) are cheaper. `examples/operators/uniswap_v3_base_atr_breakout.fork.yaml`
runs it on a Base fork at block 50974851, where the latest close is above the
midline, so it enters.
