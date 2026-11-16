# Uniswap V3 Base RSI

> Repeating RSI crossings on Base Uniswap V3: buy WETH on each hourly RSI-14 cross below 30, sell on each cross above 70 or after seven days.

Technical-analysis reference on the explicit USDC/WETH pool of the Base trading
reference. The strategy declares a price series in `data_dependencies`: the
hourly closes of the traded pool (`PoolPrice`), WETH priced in USDC from the
pool's own on-chain state at each hour, 120 points. It computes RSI-14 over the
closes with TA-Lib, buys WETH with the USDC capital on each RSI cross below 30
and returns it to USDC on each cross above 70 or after seven days. Once the exit
has completed and nothing is left to exit, memory resets and the next oversold
cross buys again. `almanak support --connector uniswap_v3 --chain eip155:8453`
states the declaration; fork proofs use simulated funds.

The SDK delivers the series in `ctx.prices` and computes no indicator. Install
TA-Lib where strategies run with `almanak[ta]`. A close counts once its hour has
ended and five minutes have passed. A decision on a short or stale series holds.
Memory keeps the previous RSI value, so a trade happens on a crossing, not on
every hour past a level, and the entry time for the holding limit. Memory is
committed with the decision, before the swap executes, so a signalled buy is
marked pending and retried until `on_outcome` sees it complete; once the exit
starts it continues until nothing is left. Teardown exits regardless of the
signal.

`backtest-economic.yaml` prepares two days of archival pool samples every ten
minutes and the hourly closes from 120 hours before the start, so the first
decision already has a full window. Costs are excluded. The series needs only
the archive provider the backtest operator configuration
`examples/operators/backtest/eip155-8453.yaml` already binds; no other data
service or key. `examples/operators/uniswap_v3_base_rsi.fork.yaml` runs it on a
fork, where the gateway reads the closes as of the fork block.
