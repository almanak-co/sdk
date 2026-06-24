# Metamorpho Base Yield (Demo)

A **yield-timing** ERC4626 vault strategy on Base (Moonwell Flagship USDC
MetaMorpho vault, curated by Gauntlet over Morpho Blue) — archetype #10.

Unlike a deposit-and-forget tutorial, it **times entry and exit on the live
yield**:

```
IDLE      → DEPOSIT only when supply APY ≥ min_apy_floor (read via lending_rate)
DEPOSITED → HOLD while APY ≥ floor (+ auto-compound idle USDC every 24h)
          → REDEEM when APY < floor for exit_confirm_checks consecutive ticks
            (hysteresis — a single transient dip does not churn)
          → after exit, return to IDLE and only re-enter once APY recovers
```

So capital rotates in and out with the yield rather than sitting in a vault
whose rate has collapsed. A missing APY read never forces a churn (entry is
allowed; exit holds).

## Chain

base

## Configuration (`config.json`)

| Key | Meaning |
|---|---|
| `vault_address` / `deposit_token` | Target ERC4626 vault + asset (USDC). |
| `deposit_amount` / `min_deposit_usd` | Deposit size + minimum to act. |
| `max_vault_allocation_pct` | Cap on % of wallet balance deployed. |
| `compound_interval_hours` | Auto-compound idle USDC cadence (24h). |
| `min_apy_floor` | Enter/stay only while supply APY ≥ this (percent). |
| `exit_confirm_checks` | Consecutive sub-floor reads before exiting (hysteresis). |
| `rate_protocol` / `rate_token` | Which `lending_rate` market is the yield proxy (default `morpho_blue` / `USDC`). |

To **demonstrate the exit** in a short test, set `min_apy_floor` above the live
USDC supply APY (e.g. `50`) so the position redeems mid-run.

> A share-price *drawdown* guard (exit on a vault loss / de-peg) needs a correct
> ERC4626 NAV read, blocked by VIB-5392 — a deliberate follow-up.

## Quick Start

```bash
almanak strat demo --name metamorpho_base_yield
cd metamorpho_base_yield
uv run almanak strat run --network anvil --interval 15
```
