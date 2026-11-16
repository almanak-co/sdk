# Base Curve StableSwap NG LP

> Curve StableSwap NG LP on Base USDC/crvUSD: two funds swap, add, increase, partially remove, hold, then exit proportionally or to USDC.

The configured pool uses USDC and crvUSD in explicit CAIP asset order on `eip155:8453`. Each of two named fund
accounts starts with 10 USDC, swaps part into crvUSD, opens and increases
fungible liquidity, partially removes it, then restarts and exits. One account
uses proportional removal; the other uses a configured USDC-only exit.

The strategy uses shared SDK economic-action helpers. The Curve integration
selects protocol steps and exact approvals. Fund accounts own tokens and LP
shares; operators pay submission gas. Fork funding and keys are identified
fixtures. `almanak support --connector curve --chain eip155:8453`
states the declaration; fork proofs use simulated funds.

`config.yaml` owns strategy parameters; providers and the fork fixture are
operator inputs. `backtest-economic.yaml` selects explicit archive samples, inventory, latency
and end treatment. The model retains the verified December 2023 pool revision
and its integer division ordering. LP fees are embedded in fixed external
share economics; hypothetical liquidity does not change subsequent external
history. Costs are explicitly excluded from this sample backtest.
