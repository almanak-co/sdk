# Aave V3 Base SMA-gated borrowing

> Repeating SMA-gated Aave V3 borrowing on Base: supply WETH, borrow USDC each time WETH trades 0.5% above its 48-hour average, repay each time it falls 1% below.

Technical-analysis reference for a lending strategy. It supplies WETH collateral
on Aave V3 Base and holds a 5 USDC loan only while the latest hourly WETH close
is more than 0.5% above its 48-hour simple moving average, repaying once the
close falls more than 1% below it. The gap keeps a price hovering at the average
from borrowing and repaying every hour. A falling collateral price is what
brings a loan toward liquidation, so the trend filter takes the loan off early in
a downtrend; it reduces that exposure but cannot guarantee it, since a fall faster
than the hourly decisions can reach liquidation first. The loan is taken each time the close rises above the upper
level and repaid each time it falls below the lower one, while the collateral
stays supplied; teardown repays and withdraws.
`almanak support --connector aave_v3 --chain eip155:8453` states the
declaration; fork proofs use simulated funds.

The closes come from the Base WETH/USDC Uniswap V3 pool's on-chain state at each
hour (`PoolPrice`): a lending strategy may read any supported pool on its chain,
through the same archive provider as its lending data. Both rules are levels,
not crossings, so the same series always gives the same answer. A short, stale
or undefined window decides nothing. Teardown repays and withdraws before
reading prices; teardown decisions receive no series.

`backtest-economic.yaml` covers six minutes from 2026-09-20 02:00 UTC, when the
close is 1% above its average: the strategy supplies, borrows, and teardown
repays and withdraws. Aave risk is read at every block, so a window long enough
to repay on the signal (about five hours on Base) would take many hours to
prepare; that path is covered by unit tests. Closes start 60 hours before the
window; decisions run every five minutes there, reported as reduced cadence
fidelity. The fund holds 1 USDC beyond the loan, so a full repayment covers
accrued interest.
`examples/operators/aave_v3_base_sma_borrow.fork.yaml` runs it on a Base fork at
block 50974851, where the close is 1.2% above its average, so it borrows.

The loan allocation accepts an optional
[loan-asset reserve](../../docs/authoring.md#a-loan-asset-reserve-for-interest) for the
interest the teardown's full repay needs.
