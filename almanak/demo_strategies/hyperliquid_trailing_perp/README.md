# Hyperliquid Trailing-Stop Perp

A single-direction perpetual on **Hyperliquid** (HyperEVM, chain 999) that holds
one position at a time and manages the exit with a **trailing stop that
ratchets** — it locks in gains once the trade moves in your favour instead of
round-tripping a good move back to the stop.

It is the reference demo for the Hyperliquid connector's two distinctive pipes:

- **Read** — the HyperCore **position precompile** (`0x0800`). The framework
  values the live position by reading it off HyperCore (a settlement *observer*:
  a CoreWriter order settles off the EVM and only "appears" on a later read).
- **Write** — **CoreWriter** (`0x3333…3333`). Every open/close is an IOC order
  submitted through CoreWriter and settled asynchronously on HyperCore.

## What it does

Fixed direction (`is_long`), one position at a time:

1. **Open** a `size_usd` position (market IOC via CoreWriter).
2. Each tick, mark the position against the HyperCore price and evaluate, in order:
   - **Take-profit** — close when PnL ≥ `take_profit_pct` (bank the win).
   - **Hard stop-loss** — close when PnL ≤ `-stop_loss_pct` (the liquidation buffer).
   - **Trailing stop** — once PnL has reached `trail_activation_pct`, the exit
     ratchets up behind the high-water PnL and closes if the trade gives back
     `trail_pct` from its peak.
3. **Re-enter** next tick after a close (continuous scalper), unless
   `reenter_after_close` is `false` (one-shot lifecycle).

## Market orders only — the TP/SL is strategy-side, not on-chain

Hyperliquid — like **every perp venue the SDK supports today** — only accepts
**market-style IOC orders** through the intent vocabulary. There is no resting
limit / stop-limit / take-profit *order type* an intent can carry. HyperCore is
natively a limit-order book, so a "market" order is synthesised as an aggressive
IOC limit crossing the book, anchored to the HyperCore oracle price (`0x0807`)
± slippage.

Consequently the take-profit, hard-stop, and trailing-stop are all evaluated
**in `decide()` each tick** and executed as a **market reduce-only close** when a
threshold trips. Do **not** read them as on-chain bracket orders resting on the
book — there are none.

## Funding (HyperCore margin is off-EVM)

There is **no EVM-wallet collateral gate** in this strategy: margin lives on
HyperCore, not in the HyperEVM wallet, so the EVM balance is not the perp margin.
The account must be funded on HyperCore out of band before running:

- **Perp margin** — bridge native Circle USDC to HyperCore via the Arbitrum
  Bridge2 (`0x2Df1c51E…163dF7`); the transfer credits the *sender's* HyperCore
  perp account (min $5).
- **Gas** — native HYPE on chain 999 (route a sliver via HyperCore spot → the
  `0x2222…2222` system address).

`size_usd` must clear the HyperCore **~$10 minimum order value** (reduce-only
closes are exempt); the strategy warns at construction if it does not.

## Configuration (`config.json`)

| Key | Default | Meaning |
|---|---|---|
| `market` | `ETH/USD` | HyperCore perp market |
| `base_token` | `ETH` | Symbol the price oracle is keyed on for PnL |
| `size_usd` | `15` | Position notional (must be ≥ ~$10) |
| `leverage` | `2.0` | Advisory on HyperCore (≥ 1x, ≤ 50x) |
| `is_long` | `true` | Fixed direction |
| `take_profit_pct` | `0.02` | Close at +2% |
| `stop_loss_pct` | `0.03` | Hard stop at −3% |
| `trail_activation_pct` | `0.01` | Trailing stop engages once +1% |
| `trail_pct` | `0.015` | Exit if 1.5% is given back from the peak |
| `reenter_after_close` | `true` | Re-open after each close (else one-shot) |
| `max_slippage` | `0.01` | IOC band width around the oracle anchor |
| `force_action` | `""` | `open` / `open_long` / `open_short` / `close` for deterministic lifecycle tests |

`trail_activation_pct` must be `< take_profit_pct`, or the take-profit would
always close first and the trailing stop could never engage.

## Running

> **Not runnable on a managed Anvil fork.** The HyperCore precompiles only exist
> on the live node, and the gateway data layer for `hyperevm` (price / balance)
> is not yet wired (**VIB-5576**). This demo is quarantined for CI until that
> lands, and is the natural acceptance test for it.

Once VIB-5576 lands:

```bash
almanak strat run -d almanak/demo_strategies/hyperliquid_trailing_perp --network mainnet --interval 15
```
