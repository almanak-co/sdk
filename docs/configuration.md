# Author configuration

`config.yaml` is the directory convention. `--config PATH` selects another YAML
file explicitly. Backtests always require `--backtest-config PATH`.
Runtime manifests, resolved facts, effective parameters and persistence remain JSON.

## Files and ownership

The SDK keeps economic choices separate from operational authority:

| Input | Maintained by | Contains |
| --- | --- | --- |
| `strategy.py` | Strategy author | Decision hooks, capability declaration and optional typed memory |
| `config.yaml` | Strategy author | Markets, named funds, allocations, quantities, timing and explicit financial sources |
| Backtest YAML | Backtest author or evaluator | Historical window, hypothetical inventory, model, data bounds, costs and end treatment |
| Operator configuration | Deployment operator | Provider bindings, account addresses, fork fixtures, payer ceilings and production signing references |
| Private environment file | Deployment operator | Provider and vendor credentials referenced by name from operator configuration |

Provider URLs, keys, wallet addresses and signing files do not belong in author
configuration. Conversely, the operator file cannot silently change the
strategy's market, quantity, risk or financial-source choices.

See the [Uniswap V4 trading configuration](https://github.com/almanak-co/sdk/blob/main/strategies/uniswap_v4_trading/config.yaml)
for a compact exchange example and the
[Aave V4 configuration](https://github.com/almanak-co/sdk/blob/main/strategies/aave_v4_weth_supply/config.yaml)
for multiple named funds, supply positions and debt.

## Author sections

| Section | Meaning |
| --- | --- |
| `strategy` | Decision interval, named allocations, optional debts, optional gas policy, optional [parameters](#strategy-parameters) and their optional fork-test values. |
| `markets` | Named market selections: the connector, CAIP-2 chain and the connector's exact target (for Aave V4 the Spoke and reserve ID). |
| `accounts` | Named fund accounts with their CAIP-2 chain. |
| `assets` | Optional assertions: a CAIP-19 identity mapped to expected decimals or a display symbol. An ERC-20 asset without one is named by its own `symbol()`. |
| `inventory` | Explicit account-scoped owned cash for composed strategies, with verified asset precision. |

The author file holds choices only. Everything a run must verify is resolved
from the chain through a read-only gateway and retained beside the run: the
reserve's Hub, the underlying asset and its precision, implementation identities
and the anchor block. Providers, account bindings, fork fixtures and production
authority live in the operator configuration described in the
[operating guide](operating.md#operator-configuration). All reference variants
use this author root, including composed strategies with multiple connectors.

Allocations refer to local market names. Names and symbols are never on-chain
asset identity. Markets and accounts must agree on chain. Multiple allocations
can share a named fund, but names do not split ownership of one protocol balance.
Each DEX connector (Uniswap V3 and V4, PancakeSwap V3, Aerodrome, Aerodrome
Slipstream, Curve and GMX V2) takes one allocation per fund, so a composed strategy
that needs a second pool on the same fund uses another DEX connector or another
fund.
A bridge market is one direction: `chain` and `asset` are the source, and
`destination_chain` and `destination_asset` say what arrives where. It also
states `max_fee_bps`, the most the whole bridge may cost in basis points of the
amount sent, and `quote`, the one source of the received amount (`across_api`
for Across; it is named, never defaulted). Its allocation names the `account`
that sends, on the market's chain, and a `destination_account` on the
destination chain. Both are accounts of the same strategy, and bridging back is
another market. One allocation is taken per source account and sent asset.
When a bridge shares a strategy with another connector, `inventory` states the
sent asset on the source account and the received asset on the destination
account (quantity `'0'` when it starts without it), and another allocation on
the destination account must hold the received asset. A file without the
inventory entry is refused with `composed_inventory_incomplete`. A file whose
destination account has no allocation that holds the received asset is refused
with `composed_arrival_unheld`; where the only other allocation there is a pool,
whose assets are known after resolution, the resolved configuration refuses it
instead.

`strategy.gas` names payer accounts, the chain, the quote source and the exact
fee bounds. Production runs require it. Only `accounts`, `chain`,
`max_transaction_fee` and `exit_reserve` are required, plus `total_budget` for a
lifetime cap: the amounts are spending authority and never default. The fields
that tune how execution bids default as below, and a check lists every default it
applied under `gas_defaults`.

| Field | Default |
|---|---|
| `source` | `network_rpc_fee_history` |
| `base_fee_basis` | `next_block`; `observed_block` on Avalanche, whose fee history reports no next block |
| `reward_percentile` | `50` |
| `base_fee_multiplier_bps` | `20000`; `12500` on Arbitrum and Robinhood Chain, whose Nitro sequencer refunds a bid down to the base fee |
| `max_gas_limit` | `1200000`, every action measured through Safe/Roles with the default buffer except GMX order creation |
| `max_gas_price_wei` | `max_transaction_fee` in wei divided by `max_gas_limit`, rounded down |
| `gas_limit_buffer_bps` | `2500` |

The derived `max_gas_price_wei` is the highest price the fee ceiling admits at
`max_gas_limit`, so the defaults never raise `gas_fee_ceiling`. Above it every
transaction, exits included, waits for gas to fall; nothing is lost. It is also
the strictest price that fee admits: a 200k-gas transaction could afford six times
that price within the same fee, but waits. On L2s the market price sits far below
it; on Ethereum, state `max_gas_price_wei` and a `max_gas_limit` near your largest
action if smaller transactions should not wait. Set any of these fields to override
its default. Fork runs with an authored gas policy
require matching operator ceilings under `fork.gas`; the policy is retained and
enforced against the generated fixture payers. Production runs on Base and
Optimism support authored gas policies: the gateway bounds the L1 data and
operator fees at quote time inside the same payer ceilings, and reconciles the
exact charges from each receipt. Only fork runs on Base and Optimism refuse an
authored gas policy, because the fork fixtures have no verified bound for those
fees. Gasless forks on those chains remain supported.

Gas amounts are chain-specific: native gas prices differ by orders of magnitude
between chains, so never reuse another chain's reference values, and size even a
same-chain reference's amounts for the market you deploy into. Size
`max_transaction_fee` to pay for `max_gas_limit` at a few times the highest bid of
the last 100 blocks (five times leaves room for spikes), plus on Base and Optimism
the L1 data and operator fee; the defaulted price ceiling follows from it. The
payer's entry minimum is entry transactions x `max_transaction_fee` +
`exit_reserve`, so a fee far above what a transaction costs inflates the funding
requirement by the same factor. `check --online` reports a ceiling below the
current quote under `gas_price` or `gas_fee_quote`, and a `max_transaction_fee`
more than 20 times the executable fee (`max_gas_limit` x the highest bid of the
last 100 blocks, plus the L1 data and operator fee bound on Base and Optimism)
under `gas_ceiling_oversized`, with a suggested value. A
stated `max_gas_price_wei` must keep `max_gas_limit` x `max_gas_price_wei` <=
`max_transaction_fee`. On Base and Optimism leave room in `max_transaction_fee`
for the L1 data fee as well. When the
live quote rises above `max_gas_price_wei`, or a transaction's quoted fee above
`max_transaction_fee`, every transaction, exits included, waits until gas falls;
nothing is lost, but nothing moves either. The
Uniswap V3 LP references for Base, Arbitrum and Optimism carry L2 gas policies; the
Optimism one is sized from October 2026 quotes, and Base and Arbitrum keep earlier
values, so run `check --online` before reusing any of them. The Avalanche and multichain
Aave V4 and Uniswap V4 references keep generous legacy values that their fixtures pin;
never size a policy from them (`check --online` can warn `gas_ceiling_oversized` on
their Ethereum and Avalanche policies). Most other published references carry an
example policy sized from October 2026 quotes at the default gas limit (2.3M on GMX
V2), with an `exit_reserve` for the exit transactions `check --production` counts;
these are starting points, so run `check --online` before deploying a copy.

### Strategy parameters

`strategy.parameters` holds author knobs the strategy code reads, such as a loop
count or a target ratio:

```yaml
strategy:
  parameters:
    loops: 3
    target_ltv: '0.65'
    rebalance_hours: [8, 20]
```

Keys match `^[a-z][a-z0-9_]*$`. A value is a scalar (integer, decimal, boolean,
string or null) or a flat list of scalars. Parameters are never resolved, priced
or part of capabilities, and authorize nothing. `almanak check` validates only
their shape; the strategy validates the values in `validate_config`, or declares a
typed model the SDK validates them against before any hook runs (see
[typed parameters](authoring.md#typed-parameters)). An omitted section is `{}` and
leaves every digest unchanged.

A fork lifecycle can run the same code with other values. `strategy.test_parameters`
declares them next to the production values:

```yaml
strategy:
  parameters:
    loops: 3
    target_ltv: '0.65'
  test_parameters:
    loops: 1
```

Each key must be declared in `strategy.parameters`, and its value takes the declared
type (`'1'` for an integer knob becomes `1`). When strategy.py declares a
[typed parameter model](authoring.md#typed-parameters), a key may instead be any field
of that model, including one `strategy.parameters` leaves to the model's default; the
value stays as written and the model's field type converts and checks it. Declare the fewest-decision values that
still exercise the strategy's paths: one loop round instead of three still supplies,
borrows, swaps, resupplies and unwinds. Don't add a value the fork can't use: fork
prices don't move, so a closer price stop still never fires, and an interval shorter
than the fork's advance adds decisions without testing anything new. `almanak check`
validates the shape and the keys. Only `almanak test run` applies the section: strategy
code never sees it in production or in backtests, and it leaves every digest
unchanged.

`almanak test run` applies `test_parameters` by default, in the fork workspace's copy
of the inputs, never in `config.yaml`. `--param loops=2` overrides one key on top of
them, and `--production-parameters` runs with the production values plus any
`--param`. The verdict records the applied values under `parameter_overrides` and
where each came from (`test_parameters` or `--param`) under `parameter_sources`, so it
says what ran. An undeclared key is refused: one config.yaml doesn't list, or, with a
typed model, one the model doesn't declare. Either way only the fork copy changes, so
`config.yaml` and its digest stay as written.

## Exact values

```yaml
assets:
  eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2:
    decimals: 18
    symbol: WETH
```

A token quantity is a quoted decimal such as `'0.01'`, or an explicit
`{base_units: 10000000000000000}`. An asset amount contains `asset` and `quantity`.
Conversion rejects excess precision. Trusted live or historical metadata must
match declared decimals before economic activity. Display symbols cannot alter
identity or precision. Resolution reads each ERC-20 market asset's `symbol()` at the
anchor block and uses it where the author declares none; a token that answers no
short plain symbol stays unnamed. The symbol is contract-controlled text for
display: it is kept outside the verified facts and never selects an asset. Financial and strategy parameters do not expand environment
variables.

Durations accept whole seconds or strings such as `5s`, `10m`, `2h`, `1d`.
Dates use whole-second ISO timestamps with a timezone, such as
`'2026-08-20T00:00:00Z'`. YAML rejects duplicate keys, aliases, merges, executable
tags, non-finite numbers and unknown schema fields.

Trading allocations select `kind: trading` and omit LP range and collection time.
LP allocations select `kind: liquidity` with an explicit tick range or band.
`collect_seconds` is optional on a Uniswap V4 liquidity allocation: only strategy
code reads it, to time its own fee collection, and connectors never collect on a
schedule. Omit it when the strategy collects no fees before exit; closing the
position takes the fees it accrued. Native new-risk balance thresholds are allocation policy; payer budgets
are shared execution policy and remain distinct.

### Liquidity bands

Uniswap V3, PancakeSwap V3, Aerodrome Slipstream and Uniswap V4 LP allocations
declare either a fixed `range` or a `band`:

```yaml
band:
  width_bps: 500
```

Each position opens centred on the observed tick, either `width` ticks wide or,
with `width_bps`, that many basis points either side of the current price rounded
up to the tick spacing. A strategy recentres by closing the position, swapping
with `swap(name, rebalance=True)` and opening again, at any price level. The
gateway admits only an aligned range of exactly the band width centred on the
tick the decision observed.

An author who wants to provide liquidity only within a price range adds an
optional envelope, `lower` and `upper` ticks in multiples of the pool tick spacing:

```yaml
band:
  lower: -200120
  upper: -196120
  width_bps: 500
```

Positions are then clamped inside the envelope, and the gateway refuses any mint
outside it before submission. Once the price leaves the envelope, positions open
at its nearest edge and stay out of range until the price returns or the envelope
changes; whether the strategy holds or reopens them is its own rule. The envelope is a gateway price
policy, not on-chain authority, so changing it needs no new permissions.
Economic backtests of a band select
`liquidity_model: bracketed_historical_fee_growth` and a tick coverage that
contains the whole envelope, or for a band without one, the price range the run
should measure; see [Backtesting](backtesting.md#liquidity-bands).

## Validation and normalization

```sh
uv run --locked almanak check strategies/aave_v4_weth_supply --normalized --json
uv run --locked almanak check strategies/aave_v4_weth_supply --fork \
  --gateway-config examples/operators/aave_v4_weth_supply.fork.yaml --env-file .env
uv run --locked almanak check strategies/aave_v4_weth_supply --fork --online \
  --gateway-config examples/operators/aave_v4_weth_supply.fork.yaml --env-file .env
uv run --locked almanak support --json
```

Offline checking parses the author file with source locations, checks every
market, account and collateral reference, refuses a second allocation of one
DEX connector on a fund and a composed `inventory` missing an asset the file names
(a lending market's `asset` or an exchange `capital` asset; pool coins are checked
online), and confirms the strategy sandbox. It makes no provider calls and runs no
strategy hooks, `validate_config` included, because the hooks need the resolved
parameters. `--production` also refuses a file without `strategy.gas`
in the configuration stage, since production runs require it. A check without
`--fork`, `--production` or `--backtest-config` cannot refuse that rule, so its
result carries a `strategy.gas` entry under `warnings` instead. With
`--gateway-config` and `--fork` or `--production` it also binds the authored
choices to the operator file. `--online` starts a
read-only gateway with the operator's providers, resolves the named markets,
runs validation, capability and dependency hooks once on the resolved
parameters, and adds the mode stage: the compiled fork fixture, or production
gas-policy compatibility with the operator ceilings. `--backtest-config`
checks the backtest inputs offline and, online, the historical model.
`--normalized` adds the parsed inputs and, online, the resolved parameters.
Every check whose file omits a defaulted gas field reports a `gas_defaults` list of
`{field, value}` entries with the values the SDK supplied.
Online `--fork` and `--production` checks also report a `funding` block: for each
bound account, the CAIP-19 assets and base units it must hold before its first
entry, and for each gas payer, the native entry minimum
`max(entry_transactions x max_transaction_fee + exit_reserve, largest minimum_native_balance)`
with any gas policy amounts. `entry_transactions` is an upper bound on the
transactions entering every allocation the payer pays for submits, through the
last one that adds exposure (an Aave supply-and-borrow allocation pair counts its
approvals, supplies, collateral enablement and borrow; a trade or liquidity
allocation with `entry_swap_bps` below 10000 counts each entry slice of its
`capital` as a swap with its approvals), because each pays its fee and each that
adds exposure must leave `exit_reserve` unspent. In a composed strategy a trade is
also offered slices of shared or borrowed funding inventory beyond its `capital`;
those swaps are not counted and draw on what entry leaves. It states
requirements only; compare it with your own balances.
For Aave V3, SparkLend, Compound V3, Morpho Blue and Aave V4 supply allocations
the token amount is each allocation's `capital`, in the market asset: what the
allocation starts with. Production admission, the fork fixture funding and the
economic backtest inventory check use the same amount. Composed strategies state
their `inventory`. Interest on borrowing is not prefunded: repaying the fund's own
debt takes no configured amount, and a repayment the fund cannot cover refuses
with the shortfall, which a transfer to the fund resolves without a restart.

Allocations state no spending budgets. Generated Safe permissions pin contracts,
functions, assets, markets, pools and the Safe as recipient or beneficiary, with
no amount limits (scope-only authority).
A lending allocation sets `borrow: true` to borrow and repay; the amounts it
supplies and borrows are the strategy's decisions. On Aave V3, SparkLend, Morpho
and Compound V3, `minimum_health_factor` is required when `borrow: true`, and every
allocation sharing that fund and Pool, market or Comet states the same value. A lend-
or supply-only allocation in a scope where none borrows may omit it: there is no
debt for it to bound, and while the fund carries any debt there an action the
connector risk-checks is refused. Aave V4 already asks for it only on debts. The
author writes the ratio (`'2'`); strategy code reads it as a wad integer
(`2 * 10**18`), see [Health factors are wad integers](authoring.md#health-factors-are-wad-integers). An exchange allocation is
offered an entry quote whenever it holds funding inventory, one `capital x
entry_swap_bps / 10000` slice at a time; a strategy that should enter once checks
`entry_complete`. On the Uniswap V3, PancakeSwap V3 and Aerodrome Slipstream pools
the slice is the swap, and a liquidity mint offers all inventory held, so 5000 swaps
about half and invests about all. On Uniswap V4 the slice sizes the whole entry
quote: a trading entry swaps it, while a liquidity entry quote swaps only the part of
it the mint pairs with and is offered only while no position is held. The mint still
uses all inventory held, so with the usual single entry swap then `open_liquidity` a
value below 10000 (the V4 default) leaves about the rest of capital idle; a banded
allocation that recentres through `rebalance_quote`, or a strategy that swaps
repeatedly before minting, is not limited this way. `entry_swap_bps: 0` offers no
entry quote, for a liquidity allocation that deposits without an entry swap (a
single-sided Curve deposit, or a band that enters only through `rebalance_quote`,
which sizes itself from the centred position and ignores `entry_swap_bps`); a
trading allocation still requires at least 1. `hold_seconds` is optional on every
allocation that offers it: only strategy code reads it, for its own timed exit, and
connectors never act on it. Omit it to hold until teardown. `strategy.gas.total_budget`
is optional; without it there is no lifetime gas cap, while `max_transaction_fee` and
`exit_reserve` still apply.
If the operator policy or fixture stage is refused, the error output still carries
it. Backtest checks omit it.
Online admission of funding and installed authority is never checked here;
each result identifies passed, failed or not-checked stages.

Every check also reports findings under `warnings`, each with `code`, `field`
and `message`. They are advisory:

| Code | Finding |
| --- | --- |
| `gas_limit` | A `strategy.gas` `max_gas_limit` is below the largest measured gas of any action the policy's connectors plan through Safe/Roles, raised by its own `gas_limit_buffer_bps`. The measurements ship as reviewed fork receipts with their provenance. |
| `gas_amount_units` | A gas fee, budget or reserve exceeds 10 whole native units (10,000 POL on Polygon, whose native coin is cheap), which usually means base units were written as a quoted decimal. |
| `gas_fee_ceiling` | A `max_transaction_fee` is below `max_gas_limit` x `max_gas_price_wei`, so the fee ceiling refuses transactions both other ceilings admit. |
| `gas_budget` | A `total_budget`, when set, is below one `max_transaction_fee` per entry transaction plus `exit_reserve`. Execution keeps `exit_reserve` unspent while it enters, so at the fee ceiling the deployment stops partway through entry. The entry count is a lower bound per allocation the policy pays for: 2 for a V3 pool trade (approve, swap), 4 for a V3 pool liquidity allocation (approve, swap, approve, mint), 1 for anything else. |
| `gas_exit_reserve` | An `exit_reserve` is below one `max_transaction_fee` per exit transaction: each allocation's `gas_lifecycle` exit from `almanak support` (V3-style pools: trade 2 (approve, swap), liquidity 5 (close, approve, swap, and a revoke for each of the two position-manager allowances the mint can leave); Uniswap V4: trade 4 (approve, Permit2 approve, swap, Permit2 revoke), liquidity 5 (burn first); Aerodrome: trade 2, liquidity 5 (approve shares, remove, approve, swap, revoke); Curve: trade 2, liquidity 3 (remove, approve, swap); Aave V3, SparkLend, Compound V3 and Morpho Blue: a supply 1 (withdraw), a `borrow` allocation 3 (approve, repay, revoke); Aave V4: a supply 1, a debt 3 plus 1 per collateral it disables; GMX: a position 3 (approve the order fee, create the close order, claim accrued funding)), and 1 for anything else. These count one repayment; a debt unwound in several cycles (a leverage loop) needs more, see `exit_per_unwind_cycle` below, which the configuration cannot state, so the finding is a floor and its message names the cycle term when the policy pays a borrow. |
| `gas_price` | Online only: a `max_gas_price_wei` is below the gas price execution would quote now with the policy's own `reward_percentile` and `base_fee_multiplier_bps`, so every transaction is refused until the market falls. A failed quote is reported under this code too. |
| `gas_fee_quote` | Online only: at the current quote, `max_transaction_fee` pays for less gas than `max_gas_limit`, so a larger transaction waits until gas falls. |
| `gas_ceiling_oversized` | Online only: `max_transaction_fee` is more than 20 times the executable fee, `max_gas_limit` x W plus, on Base and Optimism, the SDK's L1 data and operator fee bound L1 for a transaction of `max_gas_limit` gas and 2000 calldata bytes. W is the highest bid over the last 100 blocks ending at the checked block: per block, the base fee (the next block's, or the block's own for an `observed_block` basis) x `base_fee_multiplier_bps` / 10000, rounded up, plus the `reward_percentile` tip, and on BNB at least `eth_gasPrice`. The payer's entry minimum is entry transactions x `max_transaction_fee` + `exit_reserve`, so the oversized fee inflates the funding requirement by the stated factor. The message suggests `max_transaction_fee` = `max_gas_limit` x W x 5 + 5 x L1, rounded up to two significant digits: a spike beyond 5x makes transactions, exits included, wait, and nothing is lost. `exit_reserve` is then exit transactions x that fee; an omitted `max_gas_price_wei` follows the fee, a stated one must come down with it. When the node refuses the 100-block window the finding uses the single-block quote and says so. Never reported with `gas_price` or `gas_fee_quote`. |
| `market_read` | Online only: an unexpected failure while reading the current market for the gas price findings. The findings made before it are kept; it never fails a check. |
| `gas_entry_slices` | An allocation's `entry_swap_bps` splits capital into more than 10 entry slices (`ceil(10000 / entry_swap_bps)`, below 1000 bps). Each slice is its own swap with its approvals, so the payer's entry minimum includes at least that many `max_transaction_fee`s for the allocation; the message states the amount. |
| `entry_budget_partial` | A Uniswap V4 liquidity allocation without a band sets `entry_swap_bps` below 10000. Its entry quote is sized from that share of capital, so with the usual single entry swap then `open_liquidity` about `capital - capital x entry_swap_bps / 10000` (rounded down) is not swapped into the pair and stays idle; a strategy that swaps repeatedly before minting is not limited this way. Banded allocations are not flagged: they recentre through `rebalance_quote` and mint all inventory. |
| `production_gas` | A check without a mode names the production requirement for `strategy.gas`. |
| `decision_interval` | Production and mode-agnostic checks: `strategy.decision_interval` is shorter than 30 seconds. Every live decision starts a fresh sandboxed worker, whose startup alone took 8 to 11 seconds on a CPU-limited container, so a shorter interval can starve a live deployment of CPU until its evaluations time out. Fork and backtest checks do not report it. |

## Support

`almanak support` reads the same immutable connector/chain declarations used
at admission. These distinguish execution modes, economic intents, account
profiles and historical starting bounds. A supported combination does not imply
that the selected provider is currently available or that every market has history.

Filter declarations with exact public connector and CAIP-2 chain identities:

```sh
uv run --locked almanak support --connector uniswap_v3 --chain eip155:8453 --json
```

Every chain row also carries the connector's `gas_lifecycle`: for each allocation
kind it trades, pools, lends, borrows or holds (`trade`, `liquidity`, `lending`,
`debt`, `position`, `swap`), the transactions one entry and one exit submit. `entry`
is the count for a single entry slice; `per_entry_slice` is added for each further
`entry_swap_bps` slice of `capital`, `per_item` for each counted item (Curve pool
`coins`, Aave V4 debt `collateral`, and GMX deposit `input_assets`: the distinct
long, short and wrapped-native assets of its market) and `when_granted` for each named permission the allocation enables
(`collateral`, `borrow`). `exit` is the transactions one exit submits: closing the
position, the swap back with its approvals, repaying a debt (approve the held
balance, repay, revoke what the repayment leaves) and revoking the allowance left
behind; `exit_per_item` and `exit_when_granted` add to it the same way (an Aave V4
debt disables each `collateral`; a lending allocation granted `borrow` repays: 1 + 2).
`almanak check` sizes `exit_reserve` with these. Summed over the allocations a payer pays for, the entries are the
`entry_transactions` the `funding` block of `almanak check --json` reports, read
from the same connector tables, so gas can be sized before a configuration exists:

```json
{"trade": {"entry": 2, "exit": 2, "per_entry_slice": 2, "per_item": {}, "when_granted": {},
           "exit_per_item": {}, "exit_when_granted": {}, "exit_per_unwind_cycle": 0},
 "liquidity": {"entry": 5, "exit": 5, "per_entry_slice": 2, "per_item": {}, "when_granted": {},
               "exit_per_item": {}, "exit_when_granted": {}, "exit_per_unwind_cycle": 0}}
```

The exits count one repayment: the trading allocation swaps what it holds back to
the debt asset, the debt is repaid and the collateral withdrawn. A strategy that
resupplies what it bought as collateral (a leverage loop: supply, borrow, swap,
supply, repeated) holds nothing to swap until it withdraws, and while a minimum
health factor refuses withdrawing everything at once it unwinds in cycles: withdraw
part of the collateral, swap it, repay part of the debt. `exit_per_unwind_cycle`
is what one such cycle submits on the lending connector (Aave V3, SparkLend,
Compound V3, Morpho Blue: a partial withdrawal, the repayment's approval and the
partial repayment, 3; Aave V4 plans no partial steps, 0). A loop unwound in C
cycles, the last ending in the full repayment, exits in the summed exits + 1 (the
first cycle's withdrawal) + (C - 1) x (`exit_per_unwind_cycle` + the swapping
allocation's `exit`). On Aave V3 with a V3 pool trade: 1 + 3 + 2 = 6 for one
repayment, 7 for C = 1, 12 for C = 2. C is set by the strategy's unwind and its
parameters (entry rounds, borrow fraction, health-factor floor), not by the
configuration: count the partial withdrawals its unwind submits at production
parameters (a fork run's `teardown_sequence` lists them for the parameters it ran;
more rounds or a higher floor take more cycles, and the rig saw two cycles after
one round). `almanak check` counts no cycle, so add the cycles to `exit_reserve` yourself.

An empty result means there is no declared support matching the filters. Unknown
connector IDs are errors with supported candidates and a spelling suggestion
when available. Suggestions never change the selected protocol. Configuration
errors identify the author file, connector field and line, including named markets
in composed strategies.

The [reference directories](https://github.com/almanak-co/sdk/tree/main/strategies) contain explicit market examples.
Their choices are author inputs, not connector defaults. `almanak support --references`
lists the copies the installed SDK carries, with the same `--connector` and
`--chain` filters; see [Example strategies](examples.md). Editor schemas live in
`docs/schemas/`; `check --normalized` shows the resolved configuration used by the SDK.
`almanak support --schema strategy` (or `backtest`) prints the bundled schema set of
the installed version as one JSON document, `root` naming the entry schema and
`files` holding every referenced schema by file name. `--schema dashboard` prints the
schema of the dashboard response the evaluator emits, for hosts that validate and
render retained dashboards.
