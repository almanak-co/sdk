# Strategy authoring

A strategy directory contains `strategy.py` and `config.yaml`. There is no
strategy base class. The SDK captures the validated parameters and Python source
for each run, so editing the original files does not change an open or resumed
deployment.

Use [Getting started](getting-started.md) for an end-to-end walkthrough and
[Example strategies](examples.md) to choose an executable starting point. This
page is the authoring contract to keep beside the code.

## Public imports

Import shared author types from `almanak.api` and connector decision types from
the selected connector's public API:

```python
from pydantic import Field

from almanak.api import Memory, StrategyState, declared
from almanak.connectors.uniswap_v4.api import Decision, DecisionInput, OutcomeInput
```

Do not import runtime, gateway, execution or signer implementations into a
strategy. Connector-specific parameters and observation types are resolved from
the author configuration before hooks run.

## Hook contract

| Hook | Required | Contract |
| --- | --- | --- |
| `capabilities(parameters)` | Yes | Return the result of public `declared(parameters)`. It declares the economic actions the resolved configuration allows. |
| `decide(ctx)` | Yes | Return the connector's typed decision with at most one economic intent and the proposed memory. |
| `validate_config(parameters)` | No | Return `None` or raise `InvalidConfig` with an actionable message. It runs in the sandbox after trusted resolution. |
| `data_dependencies(parameters)` | No | Return explicit historical or financial observation dependencies, such as the [price series](#price-series-and-technical-analysis) decisions read. An omitted hook means no additional dependencies. |
| `on_outcome(ctx)` | No | Return memory after an admitted action stage. An omitted hook leaves memory unchanged. |
| `on_event(ctx)` | No | Return memory after a connector-owned external event such as a liquidation. An omitted hook leaves memory unchanged. |
| `dashboard(ctx)` | No | Return a typed read-only dashboard over retained evidence, in `dashboard.py` ([Dashboards](dashboards.md)). Failure cannot change execution state or suppress the shared report. |

Hooks are plain module-level functions. Expected neutral market conditions
should return a decision without an intent. Programming errors should raise and
remain visible rather than being converted into healthy no-action results.

## Declaring capabilities

Every strategy must declare the bounded actions implied by its resolved author
configuration:

```python
def capabilities(config):
    return declared(config)
```

The trusted connector derives exact assets, protocol calls, recipients and
cleanup permissions from this declaration. Strategy code cannot add authority,
install permissions or provide a handwritten permission override.

`validate_config()` can enforce policy that is specific to the strategy rather
than the connector schema:

```python
from almanak.api import InvalidConfig


def validate_config(config):
    if any(allocation.range is not None for allocation in config.allocations):
        raise InvalidConfig("trading allocations must not request liquidity permissions")
```

A refusal is reported as `invalid_config` with its message and line, and is final:
`check --online` fails the `strategy` stage with it, and `test run` or `run` stops
setup without a retry. `InvalidConfig` counts as a refusal from any function in
strategy.py. A plain `raise ValueError(...)` counts only when a `raise` statement in
`validate_config` itself raises it. Any other exception, including a ValueError from
unpacking, a conversion or a pydantic model, is a crash, reported as a traceback
starting at the strategy's own frame.

Schema validation and trusted gateway checks still apply. Strategy validation
does not replace either boundary.

Author knobs live in [`strategy.parameters`](configuration.md#strategy-parameters)
and arrive as `config.parameters`, a read-only mapping in every hook and as
`ctx.config.parameters` in `decide`, `on_outcome` and `on_event`. It is `{}` when
the section is omitted. A list arrives as a tuple, and an unquoted decimal as a
`float`: quote a decimal and convert it with `Decimal` when it must be exact.
`almanak check` validates only their shape, so validate the values here:

```python
from decimal import Decimal


def validate_config(config):
    if not 1 <= config.parameters["loops"] <= 5:
        raise ValueError("parameters.loops must be between 1 and 5")
    Decimal(config.parameters["target_ltv"])
```

### Typed parameters

Or declare them as a typed model, and the SDK validates them for you. Subclass
`TypedParameters` once in strategy.py, as a top-level class by convention named
`StrategyParams` (every reference and template uses that name). The SDK finds it by its
base class. It may extend a shared base model; two unrelated models are refused:

```python
from decimal import Decimal

from pydantic import Field, field_validator

from almanak.api import PositiveDecimal, TypedParameters, UnitFraction, parameter_base_units


class StrategyParams(TypedParameters):
    borrow_quantity: PositiveDecimal
    target_ltv: UnitFraction
    loops: int = Field(default=3, ge=1, le=5)

    @field_validator("loops")
    @classmethod
    def single_above_80(cls, loops, info):
        if loops > 1 and info.data.get("target_ltv", 0) > Decimal("0.8"):
            raise ValueError("target_ltv above 0.8 allows a single loop only")
        return loops


def borrow_amount(config):
    decimals = config.asset_metadata[config.allocation("loan").reserve.asset].decimals
    return parameter_base_units(config.parameters, "borrow_quantity", decimals)
```

With a model, `config.parameters` is its validated, frozen instance in every hook:
`validate_config`, `capabilities`, `data_dependencies`, `decide`, `on_outcome`,
`on_event`, teardown decisions and `dashboard`, in forks, production and backtests. The
SDK validates it before any hook runs, wherever `validate_config` runs. Each YAML scalar
converts to its field's type (`"1"` or `1.0` to an `int`, `0.2` or `"0.2"` to a
`Decimal`, `3` to a `str`); `1.5` for an `int`, infinities and NaN are refused. As
without a model, a `bool` field takes only `true` or `false`, and a number field refuses
a boolean or a `_` in its digits; a tuple's elements convert as pydantic's lax mode does. A key the
model does not declare is refused, and a missing key takes the field's default or is
refused when it has none. A refusal is the strategy's final `invalid_config`, naming the
field, such as `strategy.parameters.borrow_quantity: Input should be greater than 0`,
never a traceback. `PositiveDecimal` is a `Decimal` above 0 and `UnitFraction` one
strictly between 0 and 1. A field takes any pydantic scalar type or constraint, or a
`tuple[...]` of scalars for a flat list; a nested mapping or list is refused at input,
and a mutable `list`, `set` or `dict` field or a field alias is refused as
`invalid_config` naming the field.

Put a rule between knobs in a `field_validator` on the later-declared knob, reading the
earlier ones from `info.data` (a knob that failed its own check is absent there), so the
refusal names a field: `strategy.parameters.loops: target_ltv above 0.8 allows a single
loop only`. A `model_validator` refusal names only `strategy.parameters`.
`parameter_base_units` converts a quantity knob to the asset's base units and refuses
one finer than its decimals, naming the field, rather than truncating it; call it from
`validate_config` so the refusal comes before any action. Rules that also read the
rest of the configuration, such as a knob above an allocation's
`minimum_health_factor`, stay in `validate_config`. `--param` and `test_parameters`
may set any field the model declares, including one config.yaml leaves to its default:
`--param loops=1` works for `loops: int = Field(default=2, ge=1)` with no `loops` in
config.yaml, only in the fork workspace's copy. A key the model doesn't declare is
refused with the model's fields listed, and the model checks every value it is given,
so `--param loops=0` is an `invalid_config` naming `strategy.parameters.loops`. The SDK
reads the field names from strategy.py's source without running it, and loads the
strategy in the sandbox when the source alone can't tell them: a model under `if` or
`try`, built with `type(...)`, with a base that isn't a model (a plain mixin's
annotations are fields too), or extending a base imported from outside the SDK. Without a model, `config.parameters` stays the read-only mapping described above.

Declare the fork-test values next to them in
[`strategy.test_parameters`](configuration.md#strategy-parameters): the fewest-decision
values that still exercise the strategy's paths, such as `loops: 1`. `almanak test run`
applies them by default and its verdict names what ran. With a typed model,
those values keep their YAML form and the model converts and checks them, so a test
value it refuses is an `invalid_config` before the fork starts. Strategy code never
sees `test_parameters` outside that fork test. Fork prices don't move, so a price
trigger can't be made to fire there by tightening it; leave such knobs at their
production values.

## Reading decision context

`decide(ctx)` receives the current resolved view. The important fields and
helpers are:

| API | Meaning |
| --- | --- |
| `ctx.config` | One authoritative resolved parameter instance, fixed for the run. |
| `ctx.config.parameters` | The read-only author [`strategy.parameters`](configuration.md#strategy-parameters), `{}` when omitted, or the strategy's validated `TypedParameters` instance. |
| `ctx.memory` | The strategy's last committed memory. |
| `ctx.owned` | Connector-owned position records, separate from strategy memory. In a composed strategy each also reports the fund's [owned inventory](#wallet-balance-and-owned-inventory) of its assets. |
| `ctx.observations` | Current connector observations made available for this evaluation. |
| `ctx.availability` | Per-chain readiness or retained fault state. |
| `ctx.teardown` | `True` when the durable deployment is closing. |
| `ctx.prices` | The declared [price series](#price-series-and-technical-analysis), as available at this decision. |
| `ctx.price(asset)` | The one declared series for an asset, or raise. |
| `ctx.config.allocation(name)` | Resolve a named validated allocation. |
| `ctx.observation(name)` | Return an observation or `None`, while tracking its scope. |
| `ctx.require_observation(name)` | Return an observation or raise a scoped missing-data error. |
| `ctx.position(name)` | Return a retained owned position or raise. It never invents zero holdings. |
| `ctx.actions` | Connector-owned typed economic actions bound to this context. |
| `ctx.decision(intent=..., memory=..., reason=...)` | Build the typed decision and retain every observation scope accessed by the intent. `reason` optionally says why the strategy holds or takes the intent; only a test run's coordinator logs it (`decision_evaluated`), and nothing acts on or keeps it. |

A decision must not treat missing observations as zero. For a multi-chain
strategy, unavailable work on one chain can be skipped only when the intended
action and its evidence are independent of that chain.

A compact decision loop looks like this:

```python
def decide(context: DecisionInput) -> Decision:
    holds = []
    for item in context.observations:
        observed = context.require_observation(item.allocation)
        allocation = context.config.allocation(observed.allocation)
        intent = decide_allocation(context, allocation, observed)
        if intent is not None:
            return context.decision(intent=intent)
        holds.append(hold_reason(context, allocation, observed))
    return context.decision(reason="; ".join(dict.fromkeys(holds)) or None)
```

A test run shows the reason on its live card, so write it for the person watching:
short, in plain words, with the numbers that decided it, such as
`reason=f"RSI {rsi:.0f}; buying below {knobs.oversold:g}"` or
`"Price left the range, recentring"`. Give a hold a reason whenever its cause is known;
give an intent one only when it says something the intent does not, such as why a stop
or an unwind fired.

The complete implementation is in the
[Uniswap V4 trading reference](https://github.com/almanak-co/sdk/blob/main/strategies/uniswap_v4_trading/strategy.py).

### Health factors are wad integers

Strategy code reads `minimum_health_factor` as an integer at wad precision
(`10**18` is 1.0), on every lending connector. The author writes `'2'`; code
reading `allocation.minimum_health_factor` receives `2 * 10**18`. Compare it with
other wad values, or divide by `10**18` before using it as a ratio:

```python
WAD = 10**18
floor = context.config.allocation("collateral").minimum_health_factor  # 2 * WAD
# collateral value that keeps the floor, with the threshold in basis points
keep = -(-debt_value * floor * 10000 // (liquidation_threshold_bps * WAD))
```

Using the wad integer as if it were the ratio itself (leaving out the division by
`WAD`) makes every bound derived from it about `10**18` times too large, so a
withdrawal sized from it is never safe and the strategy holds forever. Writing the
literal `2` in place of `floor` in the formula above makes `keep` about `10**18`
times too small and can permit an unsafe withdrawal.

### Wallet balance and owned inventory

A composed strategy may spend only what it owns: its configured `inventory` plus
what its own actions produced (borrowed, withdrawn or swapped into the fund). An
exchange allocation's `owned.inventory0` and `owned.inventory1` report that amount
for the pair's assets; a lending allocation's `owned.inventory` reports it for the
market's asset. Each reflects the last committed outcome, including interest a
withdrawal returned. Allocations of one account and asset report the same single
amount, which one spend consumes, so a rotation withdraws from one market and then
supplies `ctx.position("next").inventory.amount` to the other. Single-connector lending
strategies have no separate owned cash; their `inventory` is `None`.
A lending observation's `wallet_balance` is the raw on-chain
balance of the fund, including anything else sent to it. The two can differ on a
fork: a generated fork fixture funds each asset 1% (at least one base unit) above
the requirement, while a hand-written fixture funds the amounts it declares. Size supplies, swaps and partial repays from owned inventory,
never from `wallet_balance`: an action that spends more than the fund retains is
refused as `inventory_unavailable`.

A full `repay` is the exception. It needs the whole debt, accrued interest
included, in the fund wallet, and it may spend loan asset sent to the fund beyond
owned inventory. Short of that it is refused with `repayment_funding`, naming the
shortfall; a transfer to the fund resolves it. Use `repay_amount`, where the
connector has one, to repay part of a debt from owned inventory.

### A loan-asset reserve for interest

Interest is not prefunded: a teardown's full repay needs the debt plus the interest
accrued since the borrow, so the borrowed amount alone is always a little short. A
strategy can accept `capital` on the loan allocation (in the loan asset's units, for
example `'1'`) as a loan-asset reserve it never supplies or spends, so the full repay
uses it for the interest; the lending references that link here do. Without it, the
repay refuses with `repayment_funding` until loan asset is sent to the fund. Size it to
the expected interest over the holding time; the fork funding must cover it.

## Price series and technical analysis

A strategy that needs price history, for an indicator such as RSI or a moving
average, declares each series in `data_dependencies`. Every decision then
receives the latest points of that series in `ctx.prices`. The SDK reads the
prices from a pool's own on-chain state through the operator's RPC provider,
with no other data service, and computes no indicator; the strategy computes one
itself, typically with TA-Lib.

```python
import numpy as np
import talib

from almanak.api import AssetType, DataDependencies, PoolPrice, PriceBinding

WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")


def data_dependencies(config):
    return DataDependencies(
        prices=(
            PriceBinding(
                purpose="strategy",
                chain=config.target("position").fund.chain,
                asset=WETH,  # priced in the pool's other token, here USDC
                window=120,  # hourly closes each decision sees
                source=PoolPrice(
                    kind="pool_state",
                    connector="uniswap_v3",
                    pool="0xd0b53d9277642d899df5c87a3966a349a798f224",
                    timeframe="1h",
                    max_age_seconds=7200,
                    availability_lag_seconds=300,
                ),
            ),
        )
    )


def decide(ctx):
    series = ctx.price(WETH)
    if not series.complete:
        return ctx.decision()  # a short or stale window decides nothing
    rsi = float(talib.RSI(np.array([float(v) for v in series.values]), 14)[-1])
    ...
```

**Declaration.**

- **Purpose:** `purpose` is `strategy`.
- **One pool per series**, on the binding's chain, named for its connector:
  `connector` is `uniswap_v3`, `pancakeswap_v3`, `aerodrome_slipstream`,
  `aerodrome` (classic volatile or stable pools) or `curve` (StableSwap NG,
  TwoCrypto NG and TriCrypto NG pools) with the pool contract address as `pool`,
  or `uniswap_v4` with the pool id. The connector's own pool resolution verifies
  the pool, so a pool the connector refuses (a Curve metapool, a pool no reviewed
  factory registers, a hooked V4 pool) is refused as a price source too. The
  declared `asset` must be one of the pool's coins. The pool need not be one the
  strategy trades: a strategy on any connector may name a supported pool on the
  same chain. `ax pool` checks a V3-style pool and its tokens.
- **Quote:** `quote` names the coin the asset is priced in. It defaults to the
  other coin of a two-coin pool and is required for a Curve pool of three or
  more coins (TriCrypto, multi-coin StableSwap), where it must be another coin of
  the pool; the SDK never picks one.
- **Two sources:**
  - `PoolPrice` (`kind: pool_state`): one close per candle, the pool's price at
    the end of the last block at or before the candle boundary. A boundary
    with no trades still has a close, the pool's actual state, so the series
    has no gaps. The close is the pool's spot price: the V3/V4 pool price;
    for Aerodrome the reserves' ratio (volatile) or the marginal price of the
    x³y + y³x invariant on decimal-normalized reserves (stable); for Curve
    StableSwap the invariant's marginal price from balances, coin decimals and
    the current A; for Curve TwoCrypto and TriCrypto the pool's own
    `last_prices`, its spot price after its last trade. All exclude fees.
  - `PoolSwapCandles` (`kind: pool_swaps`): OHLCV candles. A candle opens at
    the previous boundary's close and closes at this boundary's; its high and
    low also cover every trade price in between, and its volume is the sum of
    the trades' absolute amounts of the quote coin, in whole tokens (USDC
    above). A trade's price is the pool's resulting spot price for V3/V4 Swap
    events and Aerodrome `Sync` events (Aerodrome volume is the larger of each `Swap`'s
    quote amounts in and out). For Curve it is the execution price of each `TokenExchange`
    between the asset and the quote, the bought amount over the sold amount,
    so it includes the pool fee: Curve highs and lows can lie outside the spot
    closes by about the fee, and trades between other coins of the pool are
    not counted. A candle without trades is flat with zero volume. It reads
    every trade log of the pool, so it costs more provider reads than closes.
- **Price unit:** the declared asset in its quote coin, decimal-adjusted
  (WETH in USDC above), rounded to 30 significant digits. `series.quote` names
  the quote. The SDK converts to no other unit. Ratio and relative
  indicators (RSI, MACD, EMA/SMA crosses, Bollinger bands, Stochastic,
  percentage moves) do not depend on the unit. A strategy that needs absolute
  USD levels, or USD comparisons across assets, names a stablecoin-quoted pool
  on the same chain as its price source; any supported pool may be named, not
  only the traded one.
- **Timeframe:** `1m`, `5m`, `15m`, `1h`, `4h`, `12h` or `1d`.
- **Window:** `window` is how many points each decision sees, at most 8760
  (`MAX_WINDOW` in `almanak.api`).
- **Age and lag:** `availability_lag_seconds` is how long after its boundary a
  point counts as available: block-finality and confirmation slack.
  `max_age_seconds` must cover the timeframe plus the lag; it is how old the
  latest point may be for the series to count as `complete`.
- **One series per asset.**
- **Single prices (Aave V4 economic backtests only):** a strategy price without
  a window may instead read a CoinGecko coin price (`CoinGeckoPrice`) or a Spoke
  oracle (`ProtocolPrice`). It arrives in `ctx.prices` as one
  `PriceObservation` (`value`, `source_time`, `available_at`), may also be
  listed in the backtest file (it must then equal the hook's declaration), and
  is not delivered on forks or live.

A backtest configuration never repeats a strategy's series; the backtest reads
them from this hook.

**What a decision sees.** `ctx.prices` holds one `PriceSeries` per declaration:
the latest `window` points, oldest first, that were available at the decision.

- **Cutoff:** a point is available once its boundary has passed and its
  availability lag has elapsed. Nothing later is visible.
- **No substitution:** a failed read is never replaced by another source.
- **Members:**
  - `values`: the closes as `Decimal`.
  - `opens`, `highs`, `lows`, `volumes` (swap candles only; a closes series
    raises): the other candle members as `Decimal`.
  - `points`: `source_time` (the boundary), `value` (the close) and, for swap
    candles, `opened_at`, `open`, `high`, `low` and `volume`.
  - `quote`: the token the points are priced in.
  - `latest`: the newest point.
  - `gaps`: consecutive points further apart than the timeframe.
  - `complete`: true when the window is full, evenly spaced and its latest
    point is within `max_age_seconds`.

Decide nothing on an incomplete series, as above. `ctx.price(asset)` raises
for an asset without exactly one declared series.

**Every run mode sees the same points.**

- **Backtests:** preparation reads each series through the operator's archive
  provider from `window` points before the start, so the first decision
  already has a full window.
- **Fork and live runs:** the trusted gateway reads the series from its chain
  before each decision, cut off at the earlier of the decision time and the
  observed block time, and retains what it served. A fork therefore sees the
  points as of its fork block. The gateway refuses to start when a declared
  pool does not resolve on its chain or does not hold the declared asset. If a
  read fails at a decision, that decision holds (no action) and the next one
  tries again. Teardown decisions receive no series in any run mode, backtests
  included, so a failed read never blocks an unwind: exit before reading `ctx.price` when
  `ctx.teardown` is true, as the reference does.

**Indicators.**

- **Library:** install `almanak[ta]` for numpy and TA-Lib. Convert `values` (and
  `highs`, `lows` or `volumes` for indicators such as ATR, stochastics or OBV,
  which need `PoolSwapCandles`) to float arrays.
- **Window size:** smoothed indicators settle only after several periods, so
  declare a window several times the indicator's period (`window=120` for RSI-14
  on hourly closes).
- **Crossings:** keep the previous indicator value in memory and act when it
  crosses a level, not on every decision past the level, which would repeat the
  trade. A previous value or an entry price is a decision record, which memory
  may hold. Memory is committed with the decision, before its action executes:
  to retry an entry whose action failed, record it as pending and confirm it in
  `on_outcome`, as the reference does. Decisions more frequent than the
  timeframe see the same latest point until the next boundary.
- **Levels or crossings:** a crossing needs the series to move between two
  decisions. On a fork the series stands still at the fork block, so a strategy
  that acts only on crossings never acts there. A rule on a level (RSI inside a
  band, a close above its average) gives the same answer for the same series and
  can be exercised on a fork anchored where the level holds. Keep memory unchanged
  while holding: derive values such as the highest close since entry from the
  series instead of writing them every decision.
- **The strategy bounds its own trades.** An exchange allocation is offered an
  entry quote whenever it holds funding inventory, and authority carries no
  amount bound, so a signal that fires again can enter again. `entry_complete`
  records first exposure; it does not stop re-entry. A strategy that should
  trade one cycle records that in memory, as the reference does.
- **Timed exits are the strategy's.** `allocation.hold_seconds` is optional and
  only strategy code reads it; no connector closes a position when it elapses.
  When it is `None` there is no scheduled exit and the allocation holds until
  teardown, so a stop-only strategy omits it.

The
[Uniswap V3 Base RSI reference](https://github.com/almanak-co/sdk/blob/main/strategies/uniswap_v3_base_rsi/strategy.py)
is a complete strategy: it declares the hourly WETH closes of its own pool,
computes RSI-14 with TA-Lib, buys on a cross below 30 and sells on a cross above 70.
Three more Base references use levels: an
[RSI-gated LP](https://github.com/almanak-co/sdk/blob/main/strategies/uniswap_v3_base_rsi_lp/strategy.py),
a [loan held above a moving average](https://github.com/almanak-co/sdk/blob/main/strategies/aave_v3_base_sma_borrow/strategy.py)
and an [ATR breakout on full candles](https://github.com/almanak-co/sdk/blob/main/strategies/uniswap_v3_base_atr_breakout/strategy.py).

## Requesting economic actions

Use `ctx.actions` instead of constructing protocol transactions. Actions remain
bound to the configured allocation and the observations accessed by the
decision.

Lending connectors expose actions such as `supply`, `borrow`, `repay` and
`withdraw`. Exchange connectors expose actions such as `swap`,
`open_liquidity`, `collect`, `close_liquidity` and `exit`. The exact supported
set is connector-specific; check [Supported connectors](support.md) and the
nearest reference before using one. The generated
[strategy actions](support.md#strategy-actions) list gives each connector's exact
`ctx.actions` signatures. In the bundled docs it is the "Strategy actions"
section of Supported connectors and chains, with one heading per connector such
as "uniswap_v3 actions" or "gmx_v2 actions" listing each method and what it does,
for example what `exit` teardown closes, converts and revokes, and `repay` versus
`repay_amount`.

Concentrated-liquidity allocations configured with a `band` instead of a fixed
`range` recentre from strategy code. `open_liquidity` selects the position
centred on the observed tick, the retained `owned.position.range` says where it
sits, and `swap(name, rebalance=True)` follows the `rebalance_quote` that brings
inventory to the ratio the next centred position consumes:

```python
position = observed.owned.position
if position is not None:
    if position.range.lower <= observed.pool.tick < position.range.upper:
        return None
    return ctx.actions.close_liquidity(name)
if observed.rebalance_quote is not None and not already_rebalanced:
    return ctx.actions.swap(name, rebalance=True)
return ctx.actions.open_liquidity(name)
```

Every selection is checked against the band by the planner and the gateway; a
strategy cannot move liquidity outside it.

An action is economic, not a promise of one transaction. For example, an exit
can close an owned LP position, collect fees, convert residual strategy
inventory to the configured funding asset and revoke remaining spending
authority. The connector owns that sequence and its completion test.

Some counter inventory is too small to swap. Fees left behind by an
out-of-range close are a common example. When its exact-size exit quote returns
nothing, the observation carries it as `exit_residual` (`exit_residuals` for
Curve) instead of an `exit_quote`. No swap is offered for it, `actions.exit`
returns `None` once it is the only remaining obligation, and the dust stays in
owned inventory and reports. A zero-output entry or rebalance quote is simply
absent.

An entry slice the market cannot fill at the observed block, because the
Quoter reverts, the pool has no liquidity for it or a backtest swap would leave
the prepared ticks, offers no `entry_quote`. The observation records why as
`entry_quote_error`, and its exit and rebalance quotes are unaffected. Exit
observations and teardown in backtests measure no entry quote.

### Bridging between accounts

A bridge allocation moves one asset from one named account to another on a
different chain. `ctx.actions.bridge(name, amount)` sends `amount` base units of
the market's asset from the allocation's `account` to its `destination_account`.
The connector owns the steps: an exact approval, the quoted deposit and the
revocation of any residue. `ctx.actions.revoke(name)` clears an approval whose
deposit was never mined. It increases no exposure, so teardown admits it.

A strategy with accounts on two chains is evaluated once per chain. The bridge
allocation is observed on its source chain only, so on the destination chain's
turn `ctx.observation(name)` is `None` and the strategy requests nothing:

```python
observed = ctx.observation("move")
if observed is None:
    return ctx.decision()
if state(ctx).sent is None:
    return ctx.decision(intent=ctx.actions.bridge("move", amount))
return ctx.decision()
```

The amount received is what the deposit's quote states, never above the amount
sent and never below it by more than the market's `max_fee_bps`. The gateway
refuses a quote above that ceiling before the approval is signed. This is a
gateway check, not on-chain enforcement.

The market's `quote` names the venue's own quote API, and a fork run does not
ask it: no relayer quotes a fork. A fork gateway constructs the quote instead,
from a stated fee of one basis point and the fork's own block time, and the
same checks admit it. Reports and dashboards show its source as
`across_fork_constructed` beside each transfer. Such a quote says nothing about
what the venue would have quoted. A production run signs the venue's own
quote from its API, shown as `across_api`. A backtest asks no venue either: it
[models the bridge](backtesting.md#bridges).

A backtest calls `decide` once with the observations of every chain, where a
fork or live run calls it once per chain. A strategy that chooses its step from
the allocation it sees should try the destination's step when the source's asks
for nothing, as the composed bridge reference does.

`on_outcome` receives three kinds of result for a bridge:

- An `ActionResult` for the strategy's own steps. A completed `bridge` carries
  the opened `transfer` and `sent`, what left the source account. The received
  amount is not known yet.
- An `ArrivalResult` when the transfer arrived. `received` is what the
  destination account was credited and `offered` is what the deposit asked
  for. In a fork or live run it is a `MeasuredArrival`: a relayer filled the
  transfer, `received` is measured on the destination chain, and the result
  also carries `identity`, `fill_type`, `block`, `tx_hash` and
  `accounting_issues`. In a backtest it is a `ModeledArrival`: `received` is
  modeled and equals `offered`, the result carries `sent`, `modeled_fee` and
  `measurement_status: modeled`, and the measured fields do not exist. Test
  `isinstance(result, ArrivalResult)` and read only `received`, `offered` and
  `transfer` in code that must run in both.
- A `RefundResult` when nobody filled it. `returned` is measured on the source
  chain. A refund is read only after the destination chain has shown that no
  fill can follow, which is after the quote's fill deadline.
  One result can name several transfers: a refund pays whole sent amounts, and
  refunds mined in one block arrive as one result with their sum. A payment
  that fits no transfer, or that the venue paid in another amount than its
  refund leaf names, is no result: it is listed in the report as an
  unattributed refund payment and the transfer stays in flight.

Between the deposit and one of the last two, the source account holds a transfer
in flight. It is not a balance on either chain, and a missing outcome never
sends the deposit again. Teardown waits for every open transfer to arrive or be
refunded and does not bridge back.

#### Bridged cash in a composed strategy

A bridge allocation can share a strategy with allocations of other connectors.
The bridged amount then moves between the two accounts'
[owned inventory](#wallet-balance-and-owned-inventory):

- `inventory` states the sent asset on the source account and the received
  asset on the `destination_account`, with quantity `'0'` when the account
  starts without it. Another allocation on the destination account must hold
  the received asset, because that allocation's observation measures the
  arrival. A configuration without either is refused.
- A mined deposit takes the sent amount out of the source account's owned
  inventory. While the transfer is in flight the amount is in neither
  account's inventory. The gateway refuses a supply, swap, liquidity or bridge
  action on the destination account sized from the bridge's offer, with
  `inventory_unavailable`.
- A full repayment is the one action that may spend loan asset sent to the
  fund beyond its owned inventory. While a transfer that has neither arrived
  nor been refunded names a wallet asset as its source or destination, or
  another chain has planned or signed a bridge that delivers into it, a full
  repayment of that asset spends owned inventory only. One the inventory does
  not cover is refused with `repayment_funding`, and continues without a
  restart once the transfer is recorded as arrived or refunded.
- The measured arrival adds `received` to the destination account's owned
  inventory, and another allocation there can spend it from its next decision.
  A measured refund adds `returned` back to the source account's.
- On the source chain's turn, `ctx.position(name)` of the bridge allocation
  reports `inventory`, the source account's owned cash of the sent asset, and
  `in_flight`, the allocation's transfers that have neither arrived nor been
  refunded. Each states `sent`, `offered`, `fill_deadline` and `unfilled`, and
  no received amount. On the destination chain's turn the bridge allocation has
  no position and no observation; size a spend there from the `ArrivalResult`
  recorded in `on_outcome`, or from the spending allocation's own
  `owned.inventory`.

```python
def on_outcome(ctx):
    if isinstance(ctx.result, ArrivalResult):
        return state(ctx).updated(received=ctx.result.received.amount)
    ...


def decide(ctx):
    memory = state(ctx)
    if ctx.observation("earn") is not None and memory.received and memory.supplied is None:
        return ctx.decision(intent=ctx.actions.supply("earn", memory.received))
    ...
```

Teardown unwinds the other allocations as usual, waits for every open transfer
and does not bridge back: funds that arrived stay on the destination account.
Reports and dashboards list transfers apart from fund cash, and an open
transfer has no received amount there, never a zero. See the
[bridge and lend reference](https://github.com/almanak-co/sdk/tree/main/strategies/arbitrum_base_bridge_and_lend).

Across is the first bridge connector. The sent asset is an ERC-20 other than
the wrapped native asset, and both assets have the same decimals. Either account
may be an EOA or a Safe with Zodiac Roles, as the operator binds it; the author
file is the same. An EOA that receives is paid the native asset for a wrapped
native output, so such a route needs a Safe as its receiving account. See [Supported connectors](support.md) for the chains and run
modes it declares, and the
[Across USDC bridge reference](https://github.com/almanak-co/sdk/tree/main/strategies/across_usdc_arbitrum_base_bridge).

## Typed memory

Define `State` as a subclass of `StrategyState` when a strategy needs durable
decision state:

```python
from typing import cast


class State(StrategyState):
    version: int = 1
    bought_at: dict[str, int] = Field(default_factory=dict)


def state(context) -> State:
    return cast(State, context.memory)
```

The worker hydrates `State` inside the sandbox. Return an immutable update with
`state.updated(...)`:

```python
return context.decision(memory=state(context).updated(bought_at=new_times))
```

The durable store retains a JSON envelope containing the version, schema
identity and values. Incoming and returned state are validated. The fields
`values` and `schema_id` are reserved for that envelope. A strategy without a
`State` class uses generic `Memory`.

JSON has no infinity or NaN, so memory returned from any hook, `on_outcome` and
`on_event` included, is refused with an error naming the field when it holds
`inf`, `-inf` or `nan`. Store `None` or a finite sentinel deliberately instead.
A refusal in `on_outcome` leaves that outcome undelivered, and the runtime
delivers pending outcomes before any decision, so no later decision runs,
teardown included, while the refusal repeats.

Memory records decisions, phases and timers. It must not duplicate authoritative
balances, protocol positions, debt, transaction identities or execution
progress. Those belong to connectors and the runtime.

Resume uses the captured source and its memory schema. Existing memory cannot
silently become another schema, reset to defaults or migrate automatically.

## Outcomes and intermediate work

`on_outcome(ctx)` receives the retained result of the admitted economic action:

```python
def on_outcome(context: OutcomeInput) -> Memory:
    if context.result.status != "completed":
        return context.memory
    return state(context).updated(...)
```

`ctx.result.status` is:

- `completed` when the connector's economic completion condition is satisfied.
- `intermediate` when an admitted action still has steps to perform.
- `failed` when the operation did not succeed.

Do not advance economic state after an approval or another intermediate step.
The runtime continues intermediate work without another call to `decide()`. A
crash between external completion and outcome delivery replays the retained
outcome rather than the economic action.

Connector obligation helpers report `clear`, `remaining` or `unknown`. Unknown
is not clear and cannot authorize more risk or shutdown without an explicit
operator decision.

## Teardown

When `ctx.teardown` is true, the strategy should stop opening risk and request
the appropriate exit for any known position. Teardown bypasses the ordinary
decision interval but uses the same actions, authorization, durable work and
outcome handling as normal execution.

Prefer the connector's full-exit action when it exists. It can own protocol
cleanup that a strategy cannot safely infer from a single balance. What each
connector's `exit` closes, converts and revokes, and whether it must be called
again until it returns None, is listed under that connector's heading in
[Strategy actions](support.md#strategy-actions). If required
observations are missing, preserve the obligation rather than returning a false
closure claim.

## Isolation and external data

All author hooks run without network access, credentials, signing or filesystem
writes. They share bounded resources and output limits. Missing or unusable OS
isolation refuses execution; there is no unsandboxed fallback.

External information must be declared and delivered as typed observations
through the trusted gateway or retained historical inputs. A requested venue,
pool, oracle or vendor source cannot silently become another price source.

## Author checklist

- Import only shared and connector public author contracts.
- Declare capabilities from the same resolved parameters used by decisions.
- Access observations before returning an intent so their scopes are retained.
- Keep CAIP chain and asset identity intact throughout configuration and state.
- Keep economic positions and transaction progress out of strategy memory.
- Declare price history in `data_dependencies` and decide nothing on an
  incomplete series.
- Advance memory only on the outcome status that actually proves the transition.
- Handle teardown and missing evidence explicitly.
- Run `almanak check` after every small change, then add backtest and fork
  evidence in that order.
