# Robinhood Uniswap V4 banded LP

> Banded/recentring Uniswap V4 LP on a hookless Robinhood Chain PoolKey: recentre a 5%-wide position when the tick leaves it; wait while the price is outside the tick envelope.

The explicitly configured hookless fee-500 Uniswap V4 pool on `eip155:4663`, held as one position about 5% wide inside an owner-approved tick band.
When the pool tick leaves the held range the strategy closes, swaps to the ratio
the next centred position consumes and opens again; it never leaves the band's
envelope, which the gateway enforces before every mint. While the price is
outside the envelope, the centred range would be clamped inside it and would not
contain the tick, so the strategy holds no position and waits for the price to
return. If an open fails, for example because the price moved after the
rebalance swap, the next decision swaps to the current ratio again before
opening.

Economic backtests use `backtest-economic.yaml` with
`bracketed_historical_fee_growth`, which credits only the fee growth that
continuously initialized sampled ticks prove the held range earned and reports
the remaining upper bracket. `almanak support --connector uniswap_v4 --chain eip155:4663`
states the declaration; fork proofs use simulated funds.

## `minimum_native_balance: 0` is for ERC-20 pools only

`minimum_native_balance` is the native-coin balance the fund must hold before any new
exposure (each open and recentre); below it, the entry is refused while exits still run.
This pool trades two ERC-20 tokens, so the fund's native coin is spent only on gas, which
the gas block's `total_budget` and `exit_reserve` already guard, and `0` disables the gate
on purpose. On a pool with the native coin as
one currency (native ETH on Uniswap V4, say) the fund spends that coin into the position,
so set the minimum to at least the native amount gas needs (the gas block's
`exit_reserve` is a good floor) rather than copying `0`.
