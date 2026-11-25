# Example strategies

The repository contains executable references used for development and
qualification. Only the allowlisted references are published: `almanak support
--references` lists them and the wheel ships them; other strategy directories
are test fixtures and QA proofs. The existence of a directory does not establish
SDK support. Always confirm the
connector, chain, action and run mode in [Supported connectors](support.md) or
with `almanak support`.

This page is a curated starting point, not a catalog of every strategy directory.
An installed SDK also carries a read-only copy of every published reference's author files,
so the full catalog is available without a source checkout:

```sh
almanak support --references --connector uniswap_v3 --chain eip155:4663 --json
```

Each entry names the reference, its connector and CAIP-2 chain markets, the
installed directory and the files in it: `strategy.py`, `config.yaml`, the
backtest YAML files and the README where present, plus the README's title and
one-line summary of what the reference teaches. Without filters the command
lists every published reference. Copy a reference into your own directory before changing
it; the listing never loads or runs a strategy and does not establish support.

## Recommended starting points

| Example | Chain | Concepts demonstrated | Relative complexity |
| --- | --- | --- | --- |
| [Uniswap V4 trading](https://github.com/almanak-co/sdk/tree/main/strategies/uniswap_v4_arbitrum_trading) | Arbitrum | Exact-input entry, timed hold, complete exit, typed memory and intermediate approvals | Basic |
| [Uniswap V4 full-range LP](https://github.com/almanak-co/sdk/tree/main/strategies/uniswap_v4_arbitrum_lp) | Arbitrum | Acquire both currencies, open an NFT position, collect, close, convert residuals and revoke authority | Intermediate |
| [Aave V4 WETH supply and borrowing](https://github.com/almanak-co/sdk/tree/main/strategies/aave_v4_weth_supply) | Ethereum | Multiple named funds and positions, collateral, debt, repayment and independent withdrawal | Intermediate |
| [Aave V4 Avalanche borrowing](https://github.com/almanak-co/sdk/tree/main/strategies/aave_v4_avalanche_borrowing) | Avalanche | Collateralized borrowing, native gas scope, repayment and collateral release | Intermediate |
| [Uniswap V3 Robinhood banded LP](https://github.com/almanak-co/sdk/tree/main/strategies/uniswap_v3_robinhood_lp_band) | Robinhood Chain | Recentring one position inside an owner-approved tick band; waiting while the price is outside it | Advanced |
| [Uniswap V3 Base RSI](https://github.com/almanak-co/sdk/tree/main/strategies/uniswap_v3_base_rsi) | Base | Declared price series, RSI with TA-Lib, repeating crossing detection in memory, holding on an incomplete series | Intermediate |
| [Uniswap V3 Base RSI-gated LP](https://github.com/almanak-co/sdk/tree/main/strategies/uniswap_v3_base_rsi_lp) | Base | An indicator gating a banded LP position: open while RSI ranges, close when it trends | Intermediate |
| [Aave V3 Base SMA-gated borrowing](https://github.com/almanak-co/sdk/tree/main/strategies/aave_v3_base_sma_borrow) | Base | A lending strategy reading a pool series: borrow above a moving average, repay below it with a margin | Intermediate |
| [Uniswap V3 Base ATR breakout](https://github.com/almanak-co/sdk/tree/main/strategies/uniswap_v3_base_atr_breakout) | Base | Full OHLCV candles (`PoolSwapCandles`): channel midline entry, ATR trailing stop derived from the series | Intermediate |
| [Aave V3 borrow and Uniswap V3 trade](https://github.com/almanak-co/sdk/tree/main/strategies/arbitrum_aave_borrow_and_trade) | Arbitrum | Composed borrow-and-trade in one fund: supply, borrow, swap part, reverse, repay, withdraw, with an unsupplied loan-asset reserve for accrued interest | Intermediate |
| [Morpho Blue borrow and Uniswap V3 trade](https://github.com/almanak-co/sdk/tree/main/strategies/arbitrum_morpho_borrow_and_trade) | Arbitrum | The same composed shape on one explicit Morpho market, with a USDC reserve in inventory for the full repay | Intermediate |
| [Aave V4 borrow and Uniswap V4 trade](https://github.com/almanak-co/sdk/tree/main/strategies/avalanche_aave_v4_borrow_and_trade) | Avalanche | Composed Aave V4 (full withdraw and full repay only) with a hookless V4 swap; the unsupplied loan-asset buffer is how a V4 strategy covers interest and buy-backs | Intermediate |
| [Uniswap V3 trading](https://github.com/almanak-co/sdk/tree/main/strategies/uniswap_v3_arbitrum_trading) | Arbitrum | One Uniswap V3 exact-input buy, a timed hold measured from the fill, then the exit and revoke | Basic |
| [Aave V3 leverage loop](https://github.com/almanak-co/sdk/tree/main/strategies/arbitrum_aave_leverage_loop) | Arbitrum | Composed lending and swap: knobs in `strategy.parameters`, steps derived from owned inventory, trade capital as a slice size, a repeated withdraw-swap-repay unwind with a trigger above the floor | Advanced |
| [Morpho Blue leverage loop](https://github.com/almanak-co/sdk/tree/main/strategies/base_morpho_leverage_loop) | Base | The same loop on one Morpho market, with debt and health computed from the market's shares and oracle | Advanced |
| [Aave V3 borrow, then Uniswap V3 liquidity](https://github.com/almanak-co/sdk/tree/main/strategies/arbitrum_aave_borrow_lp) | Arbitrum | A band position funded in the borrowed asset so its exit yields the repay asset; a loan-asset reserve (or, optionally, collateral) covers a shortfall | Advanced |
| [Aave V3 lending short](https://github.com/almanak-co/sdk/tree/main/strategies/arbitrum_aave_lending_short) | Arbitrum | Borrow and sell the loan asset; buy back and repay at an oracle-price stop or take-profit | Advanced |
| [Aave V3 health-factor-managed borrow](https://github.com/almanak-co/sdk/tree/main/strategies/aave_v3_arbitrum_managed_borrow) | Arbitrum | Repay or borrow to a target health factor inside a band whose repay trigger sits above the gateway floor | Intermediate |
| [GMX V2 price stop](https://github.com/almanak-co/sdk/tree/main/strategies/gmx_v2_arbitrum_price_stop) | Arbitrum | One perpetual position closed at a stop-loss or take-profit from one-minute pool closes | Intermediate |
| [Across USDC bridge](https://github.com/almanak-co/sdk/tree/main/strategies/across_usdc_arbitrum_base_bridge) | Arbitrum to Base | Two accounts on two chains, one bridge, a decision per chain, and arrival or refund recorded from `on_outcome`; fork runs, production with EOA accounts and a modeled backtest | Intermediate |
| [Across bridge, then Compound V3 supply](https://github.com/almanak-co/sdk/tree/main/strategies/arbitrum_base_bridge_and_lend) | Arbitrum to Base | Composed bridge and lending over two accounts: owned inventory of both accounts, value in flight in neither, and a supply of exactly the measured arrival; fork runs and a backtest with a modeled bridge | Advanced |

Start with the Uniswap trading reference when learning the hook contract. It has
one account, market and allocation, and its durable state is a single purchase
timestamp. Use the Aave WETH reference for named lending positions and the
banded LP reference only after the fixed-range LP lifecycle is clear.

## What an example contains

A complete reference normally contains:

| File | Purpose |
| --- | --- |
| `strategy.py` | Sandboxed decision, capability, outcome and optional dashboard hooks |
| `config.yaml` | Author-owned economic choices and named chain scope |
| `backtest-economic.yaml` | Prepared economic model, historical window and hypothetical inventory |
| `backtest.yaml` | Optional detailed historical-path model where supported |
| `README.md` | The lifecycle, commands, assumptions and important authority limits |

Operator provider bindings, account addresses and fork fixtures live under
[`examples/operators/`](https://github.com/almanak-co/sdk/tree/main/examples/operators),
not in the strategy directory. Generated deployments and resolved facts are
runtime artifacts.

## Choosing an example

Choose by economic lifecycle before choosing by protocol name:

1. Decide whether the strategy lends, borrows, trades, provides liquidity or
   composes several of those actions.
2. Filter the support declaration by exact connector and CAIP-2 chain.
3. Confirm every required action, account profile and backtest model is declared.
4. Select the closest published reference and preserve its connector-owned action
   sequence while changing author policy.
5. Run the offline check before adding provider or execution inputs.

For example:

```sh
uv run --locked almanak support \
  --connector uniswap_v4 --chain eip155:42161 --format text
uv run --locked almanak check strategies/uniswap_v4_arbitrum_trading --format text
```

An empty support result means the requested combination is not declared. A
directory that `almanak support --references` does not list is a fixture or QA
proof, not evidence of support.

## Adapting safely

- Keep `strategy.py` and `config.yaml` as the only author-maintained runtime
  inputs.
- Preserve strict CAIP-2 and CAIP-19 identities. Do not replace them with chain
  numbers, token symbols or guessed addresses.
- Keep providers, secrets, wallet bindings and execution authority out of author
  files.
- Use public `ctx.actions` methods instead of copying protocol transaction
  construction from SDK internals.
- Treat the example's configuration as explicit sample choices, not connector
  defaults or recommended financial parameters.
- Retain teardown, failure and intermediate-outcome behavior when simplifying an
  example.
- Add new actions only when the support declaration and connector public API
  expose them.

Continue with [Strategy authoring](authoring.md) for the hook contract,
[Configuration](configuration.md) for author inputs, and
[Backtesting](backtesting.md) for model and reproduction requirements.
