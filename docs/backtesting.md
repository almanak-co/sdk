# Backtesting

Backtests execute economic intents in the shared simulator, with durable
hypothetical inventory and connector-owned positions. They do not use a Foundry
fork. Strategy hooks retain sandbox isolation.

The required backtest YAML contains `mode`, `window`, `inventory`, `execution`,
`costs` and, for economic models, the connector `data` policy. Named funds use the
author's account names and explicit asset balances; their simulation addresses
are deterministic artifacts and cannot grant production authority. A modeled
payer and its inventory are required when the selected cost model needs them.
Historical provider bindings come from the operator configuration, one per
chain. A backtest that requests vendor data names the vendor and kind in its data
policy, while the operator binds that exact name and kind to an access tier and
credential environment variable. Author and backtest files never contain access
settings.

A strategy's [price series](authoring.md#price-series-and-technical-analysis) come
from its `data_dependencies` hook, never from the backtest file. Resolution runs
the hook and adds each declared series to the data policy. A backtest file that
lists a series is refused; only an Aave V4 economic backtest file may list a
single strategy price, which must equal the hook's. Preparation reads each series
from the pool's state through the operator's archive provider for the series'
chain, from `window` boundaries before the window start, so the first decision
already has a full window. Its historical gateway resolves the declared pool with
the connector's resolution and admits reads of that pool only. The dataset keeps
compact evidence: the resolved pool, each boundary's block, its successor and the
raw state reads, and for swap candles each log window's trade events with a digest of
the provider's answer; opening a dataset rebuilds every point from it. The series and
their warm-up are part of the dataset identity, and a strategy that declares no
series prepares exactly as before.

## Run and reproduce

```sh
uv run --locked almanak backtest prepare strategies/aave_v4_weth_supply \
  --backtest-config strategies/aave_v4_weth_supply/backtest-economic.yaml \
  --gateway-config examples/operators/backtest/eip155-1.yaml \
  --env-file .env --output datasets/aave
uv run --locked almanak backtest run strategies/aave_v4_weth_supply \
  --backtest-config strategies/aave_v4_weth_supply/backtest-economic.yaml \
  --gateway-config examples/operators/backtest/eip155-1.yaml \
  --dataset datasets/aave --output runs/aave
uv run --locked almanak backtest resume --run runs/aave
uv run --locked almanak backtest reproduce --run runs/aave
```

Preparation and a fresh run first resolve the named markets at the last block
at or before the window start through a read-only gateway, retain the verified
facts as `resolution.json`, and construct the parameters from them. A dataset
records the facts digest it was prepared with; a run with `--dataset` reuses
that retained resolution and needs no provider credentials. Without `--dataset`,
economic runs prepare or reuse a matching dataset before execution. Preparation
uses the operator's archive providers and the explicitly requested vendors
through the read-only gateway. Execution makes no data network calls. Missing
operator bindings, samples, tick coverage or price sources cannot trigger online
quotes, select another vendor or substitute models. Dataset identity includes effective settings, the resolved facts and
input evidence. Every fresh run requires the selected operator file, including
when a retained dataset supplies the resolved facts without provider access.
Every shipped reference uses the public author and backtest inputs.
Archived legacy YAML is read only by private component and baseline tooling; the
runtime consumes retained JSON and has no legacy author fallback.

A `resolution.json` left in the output or dataset directory by an earlier or
interrupted preparation with other inputs is refused, and the error names the file:
prepare into a fresh output directory, or remove that directory and prepare again.

Reproduction requires the captured implementation, frozen settings and the
retained resolution whose facts digest the manifest names; it replays the
retained reads offline and refuses missing or changed evidence. Resume uses
retained state, pending actions and outcome delivery. Author edits cannot change
that run. Do not replace datasets or silently change sampling on resume.

## Models and assumptions

- `historical_path` retains detailed operation and account-profile assumptions.
  Uniswap uses historical Quoter calls; Aave detailed lending currently supports
  Ethereum supply/withdraw only. Remove optional debts from the author configuration
  when selecting that Aave model.
- `economic` uses prepared Aave lending samples, including borrowing, collateral,
  repayment and liquidation, or bounded integer Uniswap pool state with price
  impact and tick crossings. LP fees use explicit fixed historical fee growth.
  The Aave V3 model and its datasets carry `revision11` in their identifiers
  (`aave-v3-revision11-economic-indices-v1`, `immediate_underlying_v3_revision11`).
  The name is the accounting model's: Pool revision 12 runs the same supply,
  borrow and liquidation logic, so the model serves history under either revision
  and a window may span a Pool's upgrade from one to the other.

Hypothetical trades and liquidity do not change later external market history.
LP fees exclude hypothetical dilution and fees from the strategy's own trades.
Explicit vendor prices may approximate economic risk but never replace a requested
oracle or venue observation. Missing measurements and model exclusions remain
reported. No model here establishes production execution authority.

The strategy interval is inherited by default. A coarser override is explicit,
fixed and reported as reduced cadence fidelity. Unchanged observations do not skip
decisions. Keep sampling interval, maximum source age, collection limits, tick
coverage, latency, costs and end treatment explicit. Collection-limit failures
require an author decision; the SDK does not coarsen resolution automatically.

Progress appears on stderr; redirected stdout remains JSON. `--progress json`
produces versioned snapshots, also retained beside the run directory. ETA is a
heuristic from comparable timings and current throughput; unsupported estimates
remain absent. Observer failures do not affect financial state.

Contract comparisons, sampled-path fidelity and measured preparation and execution
timings are separate kinds of evidence. Component replay and fork execution are different evidence.

### Liquidity bands

Uniswap V3, PancakeSwap V3, Aerodrome Slipstream and Uniswap V4 `band`
allocations run in `economic` mode only, with
`liquidity_model: bracketed_historical_fee_growth`. That value requires a band
allocation; fixed ranges in the same run keep exact historical range growth.
Each allocation's `data.coverage` must contain the whole band envelope. A band
without an envelope follows the price, so its coverage is the explicit tick range
the run measures: it must hold at least one aligned band width and the position
centred on the first sample's tick, and a full-range coverage measures every
position the strategy can open. A run that would open a position outside the
coverage stops with an error instead of crediting unmeasured fees. Preparation
samples every initialized tick of the coverage with its fee growth plus every
liquidity event of the pool, within the usual sampling, age and collection
limits.

A position the strategy opens can sit on ticks that no historical position
initialized, so its fee growth is not directly observable. Between two samples,
the SDK measures the growth accrued below every tick that stayed initialized for
the whole interval and brackets each position boundary between its neighbours.
The lower bracket is credited; it equals the exact growth when both boundaries
are continuously initialized ticks, which is common on pools with dense
liquidity. The report labels band positions `fee_attribution:
bracketed_lower_bound` and retains `uncollected_fee_upper_bound` and
`collected_fee_upper_bound_excess`, the fees the sampled state cannot rule out.
On sparse pools the credited fees can be far below the upper bound; nothing is
interpolated between samples. Fees accrue between the latest samples at or
before each liquidity action. Rebalance swaps use the same prepared integer pool
calculator as other swaps. Recentring is bounded by the allocation's inventory,
as in live execution. `historical_path` backtests and composed
strategies refuse band allocations.

### Bridges

A bridge is modeled, not replayed. The model reads no history and asks no
venue, so a backtest of a bridge needs no bridge data and every amount it
reports is labeled `modeled`.

```yaml
execution:
  latency: 1s
  models:
    bridge_model: full_fee_ceiling_fixed_latency
data:
  arrival_latency_seconds: 30s
costs:
  kind: excluded
```

Both settings are required and nothing defaults them. In a composed backtest
they sit under the connector's name: `execution.models.across`,
`data.connectors.across` and `costs.across`.

- **Fee.** Every bridge pays the market's whole `max_fee_bps`: the amount
  received is the amount sent less `amount * max_fee_bps // 10000`. This is the
  most a live bridge may cost, not a measured fee. It stands in for every
  relayer cost, and no destination gas is charged.
- **Admission.** Every deposit is admitted. A live run bridges only when the
  venue's fee is within the ceiling, and no history of that fee is read, so a
  ceiling below the venue's real fee bridges in a backtest and would not
  bridge live. Do not choose `max_fee_bps` by sweeping it in a backtest: the
  model always prefers the lowest. `max_fee_bps: 0` is refused, because no live
  run admits it and it would model a free bridge.
- **Arrival.** The deposit debits the source account when it executes, under
  the usual `execution.latency`. The destination account is credited
  `arrival_latency_seconds` later, and `on_outcome` receives an
  `ArrivalResult` then. The latency is a stated assumption, not a measured
  fill time.
- **In flight.** Between the two the value is in neither balance and cannot be
  spent. The decision input shows the transfer with what was sent and offered
  and no received amount, as a live run does. Its fill deadline is the
  deposit's second plus one hour, as a fork's constructed quote states it. It
  says nothing about when the transfer arrives.
- **No refund.** Every transfer arrives. Unfilled transfers, refunds and slow
  fills are not modeled, so a strategy's refund path is not exercised. An
  `arrival_latency_seconds` of one hour or more is refused, since it would
  place the arrival at or past the fill deadline.
- **End of the window.** A run that ends with a transfer in flight reports it
  as in flight, with its sent amount and no received amount; the position's
  `received_by_arrived_transfers` total is absent until a transfer has arrived. With
  `end_policy: teardown` the run waits for the arrival and sends nothing new;
  if `cleanup` ends first the run fails as unresolved, so make `cleanup` longer
  than the latency.
- **Gas.** `costs` is `excluded`, or `fixed_native` with a `bridge` amount
  charged on the source chain for each bridge. `backtest recent` builds no
  window for a strategy that bridges and refuses it with
  `unsupported_connector`.
- **Approvals.** The model keeps none and does not serve `revoke`. A strategy
  that decides `revoke` in a backtest stops the run with an availability
  error, as any action a backtest model does not declare does. Request it only
  when the observation shows an allowance, which a backtest never does.
- **Result type.** `on_outcome` receives a `ModeledArrival`. It is an
  `ArrivalResult`, with `received`, `offered` and `transfer`, plus `sent` and
  `modeled_fee`. It has none of a measured arrival's `identity`, `fill_type`,
  `block` and `tx_hash`.

The route is still resolved at the window start through the operator's
providers for both chains. On each chain this verifies the SpokePool's
proxy, implementation and code hash against the pin: the source SpokePool with
the sent asset, and the destination SpokePool with the received asset. A
destination SpokePool that runs another implementation refuses the route.
Across declares the model on Arbitrum from 15 August 2026: the first whole day
after both pinned implementations were in place, Arbitrum's at 18:24 UTC and
Base's at 18:32 UTC on 14 August 2026. An earlier window is refused.
Preparation after resolution makes no provider
call, and the dataset holds only the frozen statement of the routes, the window
and the latency: a resumed run cannot change them.

### Results in the funding asset

An economic Uniswap V3, PancakeSwap V3 or Aerodrome Slipstream run whose
allocations share one funding asset is valued in that asset. The other pool asset
is priced by the prepared pool sample in force at each mark, so the valuation
reads nothing outside the run's own dataset. The report then carries:

| Field | Meaning |
| --- | --- |
| `valuation`, `valuation_quote`, `valuation_asset` | `pool_price`, the funding asset's symbol (or its CAIP-19 identity when it has none) and its identity. |
| `equity_curve` | Wallet balances of the pool assets plus each open position's principal and credited pending fees, at every action and on an even grid of at most 480 marks. |
| `price_series` | The other pool asset's price in the funding asset at the same marks. |
| `metrics` | `total_return_pct`, `max_drawdown_pct` and `sharpe_ratio` (daily marks, annualized; absent below three days). |
| `net_quote_return` | The net result and return in the funding asset. |
| `performance` | What produced the result: `fees_value` (fees at the pool price they were collected at), `swap_cost_value` and `swap_volume_value` (each swap against the pool price it executed on), `price_effect_value` (the rest: the price change of the inventory held, impermanent loss included), time in market and in range, holding benchmarks and one row per position. |
| `valuation_excludes` | Assets with a balance that the pools do not price, such as the gas asset. They stay in asset units. |

`fees_value - swap_cost_value + price_effect_value` equals the net result. Gas is
not converted: an `excluded` cost policy leaves it out and a `fixed_native` policy
reports it in the native asset under `gas_payers`. Allocations with different
funding assets keep the asset-unit report, as do Uniswap V4, Curve and Aerodrome
runs.

`boundary_evidence` names each pool's continuity proof by its dataset digest with
the block interval and the number of liquidity events and inputs it covers. The
proof itself stays in the prepared dataset.

### HyperCore perps

The `hyperliquid` connector runs economic backtests on `eip155:999`; it also runs on
identified HyperEVM forks, with the same intents and observation, so one strategy file
runs in both, and production waits. A
HyperCore account trades one first-dex perp market (`market: BTC`) at a fixed cross
`leverage`; the strategy deposits USDC from HyperEVM, sets the leverage while flat,
opens with an `Ioc` order, closes with a reduce-only `Ioc` order and withdraws
(`strategies/hyperliquid_hyperevm_perp`). The history is HyperCore's own `/info`
answers, read anonymously during preparation and retained in the dataset: trade
candles at the backtest file's `data.candles` interval and hourly funding rates.

HyperCore keeps no book, mark or oracle history, so the latest trade close stands in for
the book and the oracle, and each candle's low and high for the mark. An order fills at
that close moved against it by `data.fills.slippage_bps`, or not at all past its limit,
and pays `data.fills.taker_fee_bps` (4.5 by default). The first deposit pays the 1 USDC
activation fee; funding is paid each hour at the latest close; a candle that crosses the
maintenance price liquidates the position. The report names every stand-in under
`labels`. The venue serves only its latest 5000 candles of an interval (about 3.5 days
at one minute), so prepare a window while it is that recent; a retained dataset replays
at any time. `almanak backtest recent`, resting and trigger orders, and strategy price
series are not supported for HyperCore.

### Recent windows

`almanak backtest recent` rewrites an economic backtest file so that it covers the
latest `--days` days (30 by default) and charges measured gas.

```sh
uv run --locked almanak backtest recent strategies/uniswap_v3_base_trading \
  --backtest-config strategies/uniswap_v3_base_trading/backtest-economic.yaml \
  --env-file .env --output runs/uniswap_v3_base_trading.recent.yaml
```

The window ends on the hour at least two hours before every chain's finalized head, or
at an explicit `--end`. Everything sized for the authored window is sized again:

| Setting | Recent window |
| --- | --- |
| `execution.decision_interval` | An hour, when the strategy decides more often; the run reports the reduced cadence. |
| Pool `sample_seconds`, `max_age_seconds` | Hourly sampling with the authored age margin. |
| `max_samples` and event or log-request limits | Grown with the window. |
| Tick `coverage` | The authored bounds widened to 4,000 ticks either side of the pool tick at the window start. |
| `costs` | `fixed_native`: each reachable operation's measured gas (`connectors/gas_references.json`) at the window's median base fee plus the median tip before its end, paid by a hypothetical payer per chain. Operations a swap-only allocation cannot reach cost 0. |

Every read is pinned to a block of the window, so the same `--end` writes the same
file. The command prints what it changed, including each chain's gas price and the
pool ticks it used. It refuses, with `{"error": "recent_unsupported", "reason": ...}`,
an example whose connector reads every block of the window (`per_block_history`), whose
run is not valued in its funding asset (`unvalued`) or whose reachable operations have
no measured gas (`unmeasured_gas`). Gas is execution gas: rollup L1 data fees are not
part of a measured receipt and are not modeled.
