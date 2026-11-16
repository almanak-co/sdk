# Base Curve TwoCrypto NG LP

> Curve TwoCrypto NG LP on Base LCAP/eUSD: two funds swap, add, increase, partially remove, hold, then exit proportionally or to eUSD.

The configured v2.1.0 pool uses
LCAP and eUSD in explicit on-chain CAIP asset order on `eip155:8453`. Each of two
named fund accounts starts with 10 eUSD, swaps part into LCAP, opens and increases
fungible liquidity, partially removes it, then restarts and exits. One account
uses proportional removal; the other uses a configured eUSD-only exit.

The strategy uses shared SDK economic-action helpers. The Curve integration
selects protocol steps and exact approvals. Fund accounts own tokens and LP
shares; operators pay submission gas. Fork funding and keys are identified
fixtures. `almanak support --connector curve --chain eip155:8453`
states the declaration; fork proofs use simulated funds.

`config.yaml` owns strategy parameters; providers and the fork fixture are
operator inputs. `backtest-economic.yaml` selects explicit archive samples, inventory, latency
and end treatment. The prepared model retains verified v2.1.0 integer math,
price scales, dynamic fees and profit accounting. LP fees are embedded in fixed
external share economics; hypothetical liquidity does not change subsequent
external history. Costs are explicitly excluded from this sample backtest.
