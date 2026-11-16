# Dashboards

A strategy's dashboard is what its owner looks at once it runs: a page of numbers,
charts and tables drawn from the deployment's own retained evidence. You write it in
`dashboard.py` beside `strategy.py`:

```python
from almanak import dashboard as ui
from almanak.dashboard import build as b


def dashboard(context: ui.Context) -> ui.Dashboard:
    return b.dashboard("WETH/USDC liquidity", ..., description="Range and value against holding.")
```

`almanak report --dashboard` evaluates it from the strategy's latest state, or from the last
fork test, so its author sees it before deploying. The hook
is read-only and runs in a sandbox: no network, no files, no effect on execution. A
missing reading stays unavailable; never show it as zero.

## What a good dashboard shows

Write it for the person who deployed the strategy, not for its code.

- **Lead with the answers.** A handful of numbers that say whether the strategy is doing
  what they asked, whether it is safe, and whether it is making or losing money against
  just holding. For an LP: price, range edges, distance to each edge, value against
  holding. For a loan: health, LTV against the limit, debt, rates. For a hedge: the
  net exposure against its tolerance.
- **Chart what moves.** Where a reading over time explains the strategy (price against
  the range, health against the liquidation line, value against holding, an indicator
  against its levels), draw it.
- **Keep the mechanics out of the way.** Reserves, budgets, request escrow, retry flags
  and other things the strategy needs but its owner does not belong in a collapsed group
  (`b.group(..., collapsed=True)`) or nowhere.
- **Plain, short words.** Labels like "Value", "If held", "Health factor". One short text
  block says what is measured and what is not (fees not read, values in USDC not USD,
  gas shown separately). Do not claim fiat PnL the data does not carry.

Start from the closest reference's `dashboard.py` (below) and adapt it to the
allocations and what the user asked to see.

## What `context` carries

| Field | What it is |
|---|---|
| `config` | The strategy's configuration. Parameters are read by name: a plain mapping (`parameters["period"]`), or the typed model when the workspace declares `parameters.py` (`parameters.period`); the references' `param(config, "period")` reads both. |
| `memory` | The strategy's own memory (`context.memory.values`). |
| `positions` | Each allocation's position: its target and inventory values (token amounts with asset, decimals and symbol). |
| `values`, `datasets` | Values and tables the connector projects from the retained state (`exchange_inventory`, `payer_costs`, `transactions`, `lending_positions`, ...). Every connector's `transactions` lists each committed transaction the strategy submitted (newest 200) with what it did in the connector's terms ("Swap USDC → WETH", "Supply USDC", "Open long ETH"), its receipt gas and time; `payer_costs` (exchange, composed), `fund_costs` (lending) or `submitters` (GMX) is the gas total. Tables name the fund account only when the strategy has more than one. Show a dataset with `ui.Table(id=..., title=..., data=context.datasets["..."])`. |
| `history` | Readings taken at each decision, oldest first, for charts. `b.history(context, "price")` gives `(time, value)` pairs. |
| `prices` | The price series the strategy declared, as its latest decision read it (`b.prices(context)`): the whole window from the first decision, so an indicator can be recomputed from the same closes the strategy used. |
| `trades` | Committed transactions that moved tokens (`b.trades(context, "position")`): time, intent (`swap_exact_input`, `exit_exchange`, `open_liquidity`, ...) and signed amounts, for markers on a chart. |

`positions`, `values`, `datasets` and `history` are separate reads: inventory is the retained state's
latest, a history row is one decision's reading, and their blocks need not match. Combine them as
retained; never mark a value unavailable only because its block differs from the latest row's.

### History fields by connector

- **Uniswap V3, PancakeSwap V3, Aerodrome Slipstream:** `price` (currency1 per
  currency0), `wallet0`/`wallet1`; with a position `range_lower`/`range_upper` and
  `position0`/`position1` (principal at that price); `value` (wallet plus position, in
  currency1) and `hold` (the starting wallet at that price). Uncollected fees are not read.
- **Uniswap V4:** the same, plus uncollected `fees0`/`fees1`, counted in `value`.
- **Aerodrome:** `price`, `wallet0`/`wallet1`; with a position `shares`,
  `position0`/`position1` and claimable `fees0`/`fees1`; `value` and `hold` in currency1.
- **Curve:** `wallet0`..; with a position `shares` and `position0`..; `value` and `hold` in
  coin0 (StableSwap at the pool's peg rates, Crypto pools at the price oracle, which also
  read `price1`..).
- **Lending (Aave V3, SparkLend, Compound V3, Morpho Blue, Aave V4):** `wallet`,
  `supplied`, `collateral`, `debt` in the asset's units; `health` (with debt), `ltv` and
  `max_ltv`; `supply_apy`/`borrow_apy` (0.05 is 5%); `price_usd`, or `price` where the
  market does not price in USD (a Compound WETH market prices in WETH; a Morpho collateral
  leg in the loan asset); `value` and `hold` in that same unit. Aave V4 has no prices or
  rates.
- **GMX:** `size_usd`, `size_tokens`, `collateral`, `entry_price`,
  `funding_long`/`funding_short`, `wallet0`..; a GM allocation reads `gm_shares`. Mark
  price, PnL and liquidation price are not observed.
- **Composed strategies** prefix each part's fields with its allocation
  (`liquidity:price`, `hedge:size_usd`, `loan:debt`); lending legs read together. Their
  total is `value` and `hold` when every part lends, and `lending_value` and
  `lending_hold` when other parts sit beside the loans (it does not include those parts).

## Building blocks

`almanak.dashboard.build` turns plain numbers into blocks:

- **Values:** `b.token` (a token amount), `b.price`, `b.usd`, `b.percent`, `b.ratio`,
  `b.index` (an indicator reading), `b.count`. Each takes a reason; a value you compute is
  `evidence="author_derived"`.
- **Metrics:** `b.metric(id, label, value)`, gathered in `b.group(id, title, ...)`.
- **Charts:** `b.line_chart(id, title, [b.series(...)], caption=...)` with `bands`
  (a range), `references` (a level such as health 1.0), `markers` (trades) and a `lower`
  panel for an indicator under a price (`lower_marks` names its levels).
- **Text:** `b.callout` for a status line (`tone="success"`, `"warning"`, `"danger"`),
  `ui.Text` for the scope sentence, `ui.Table` for a dataset.

A time series keeps at most 150 points (`b.series` thins a longer one, keeping each stretch's
lowest and highest reading, every gap and the latest point): a dashboard response is capped at
256 KiB in the fork test and preview, and each point repeats its value's metadata. The draw
applies the same bound to a `Series` built by hand, and keeps a table's newest 200 rows (marked
truncated), so a dashboard does not outgrow its limit as history accumulates. A failed draw says
why: the exception type `dashboard.py` raised, its time or CPU limit, or its size against the
1 MiB limit.

A series needs at least one point: on the first decision there is no history yet, so
draw the chart only when its points exist.

## Testing it

- `run_test` reports whether the dashboard drew from the fork test (`ready`,
  `absent`, or `unavailable` with the reason). Fix an `unavailable` dashboard before
  deploying.
- `almanak report --run <fork run> --dashboard --workspace .` draws the workspace's
  current `dashboard.py` over a finished fork test, so an edit is checked without
  running the fork again.
- A dashboard is checked again in the browser. Integers larger than 2^53 cannot be read
  there exactly; keep raw protocol integers out of values you build yourself.

## References

| Strategy type | Reference |
|---|---|
| Concentrated LP (fixed range) | `uniswap_v3_arbitrum_lp`, `aerodrome_slipstream_base_lp` |
| Concentrated LP with fees | `uniswap_v4_arbitrum_lp` |
| Classic AMM LP | `aerodrome_base_volatile_lp` |
| Stable pool LP | `curve_arbitrum_stable_lp` |
| Indicator trading | `uniswap_v3_base_rsi` |
| Lending supply and borrow | `aave_v3_arbitrum_lending`, `compound_v3_base_lending`, `morpho_blue_base_lending`, `aave_v4_weth_supply` |
| Leverage loop | `arbitrum_aave_leverage_loop` |
| Perpetual position | `gmx_v2_arbitrum_position` |
| GM liquidity | `gmx_v2_arbitrum_liquidity` |
| Composed (borrow and LP) | `arbitrum_aave_borrow_lp` |

`almanak support --references --connector <id> --json` lists them with their files.
