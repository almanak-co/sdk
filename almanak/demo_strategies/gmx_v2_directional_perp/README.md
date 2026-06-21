# GMX V2 Directional Perp

A directional perpetual-futures strategy on **GMX V2 (Arbitrum)** that trades an
EMA-crossover signal. It is the reference implementation for the three things a
directional perp must get right:

1. **Close-before-reverse** — when the signal flips, the open position is
   **closed first**; the opposite side is opened only on a later tick, after the
   close confirms. The strategy never opens an opposite leg while a position is
   still live (the "stranded leg" bug).
2. **Funding-rate gate** — entries are refused when funding is adverse beyond
   `funding_entry_threshold_hourly`, and an open position is closed if funding
   turns strongly against it (`funding_exit_threshold_hourly`).
3. **Liquidation buffer** — a stop-loss on **fill-price** PnL (`stop_loss_pct`)
   closes the position before the liquidation price. Keep `stop_loss_pct` below
   the liquidation distance (~`1 / leverage`); the strategy warns if it is not.

Position state (`position_side`, `entry_price`) is committed **only** after a
fill confirms (`on_intent_executed`), never speculatively in `decide()`.

## Run on Anvil

```bash
almanak strat run -d almanak/demo_strategies/gmx_v2_directional_perp --network anvil --interval 5
```

> **Note:** GMX V2 fills are completed by a keeper, which does not run on a
> managed Anvil fork — an opened position may not fill locally. The reversal /
> funding / stop-loss control flow is venue-agnostic and is covered by the
> strategy's behavioral tests; full fill behaviour is validated on mainnet.

## Key config

| Field | Meaning | Default |
|---|---|---|
| `position_size_usd` | Notional per position | 100 |
| `leverage` | Perp leverage | 2.0 |
| `ema_fast_period` / `ema_slow_period` | Crossover signal periods | 9 / 21 |
| `funding_entry_threshold_hourly` | Max adverse funding to open into | 0.0005 |
| `funding_exit_threshold_hourly` | Adverse funding that forces a close | 0.0015 |
| `stop_loss_pct` | Stop-loss on fill-price PnL (liq buffer) | 0.10 |
| `force_action` | `open_long` / `open_short` / `close` for testing | "" |
