# Base Curve TriCrypto NG LP

> Curve TriCrypto NG LP on Base crvUSD/tBTC/WETH: two funds swap, add, increase, partially remove, hold, then exit proportionally or to WETH.

The configured v2.0.0 pool uses
crvUSD, tBTC and WETH in explicit CAIP asset order on `eip155:8453`. Each of two
named fund accounts starts with 0.01 WETH, swaps part into crvUSD, opens and
increases fungible liquidity, partially removes it, then restarts and exits.
One account uses proportional removal; the other uses a configured WETH-only exit.

The strategy uses shared SDK economic-action helpers. The Curve integration
selects protocol steps and exact approvals. Fund accounts own tokens and LP
shares; operators pay submission gas. Fork funding and keys are identified
fixtures. `almanak support --connector curve --chain eip155:8453`
states the declaration; fork proofs use simulated funds.

`config.yaml` owns strategy parameters; providers and the fork fixture are
operator inputs. `backtest-economic.yaml` selects explicit archive samples, inventory, latency
and end treatment. The pool uses the verified Vyper Istanbul build; the shared
model retains integer math, price scales, dynamic fees and profit accounting.
Wrapped-token actions account for the pool's internal native custody. LP fees
remain fixed external share economics: hypothetical liquidity does not change
subsequent external history. Costs are explicitly excluded from this backtest.
