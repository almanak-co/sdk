# EdgeUsdtSupplyEthStrategy - Agent Guide

> AI coding agent context for the `edge_usdt_supply_eth` strategy.

## Overview

- **Template:** supply-only (based on lending_loop scaffold)
- **Chain:** ethereum
- **Class:** `EdgeUsdtSupplyEthStrategy` in `strategy.py`
- **Config:** `config.json`
- **Signal:** `85fa7410-bd84-4e73-83cd-2f68a23d9145` (HIGH_CONVICTION_SYNTHESIS)

This is a self-contained Python project with its own `pyproject.toml`, `.venv/`, and `uv.lock`.
The same `pyproject.toml` + `uv.lock` drive both local development and cloud deployment.

## Protocol Fallback

The original Edge signal targets **Radiant V2** for USDT supply. However, radiant_v2 is
not a supported protocol in the Almanak SDK intent vocabulary. This strategy falls back
to **aave_v3** on Ethereum, which also supports USDT supply.

## Files

| File | Purpose |
|------|---------|
| `strategy.py` | Main strategy - supply-only state machine (idle -> supplying -> supplied -> withdrawing -> done) |
| `config.json` | Runtime parameters (collateral_token, supply_amount, stop_loss, time_horizon) |
| `pyproject.toml` | Dependencies plus metadata (`framework`, `version`, `run.interval`) |
| `uv.lock` | Locked dependencies for reproducible builds |
| `.venv/` | Per-strategy virtual environment (created by `uv sync`) |
| `.env` | Secrets (private key, API keys) - never commit this |
| `.gitignore` | Git ignore rules (excludes `.venv/`, `.env`, etc.) |
| `.python-version` | Python version pin (3.12) |
| `tests/test_strategy.py` | Unit tests for the strategy |

## How to Run

```bash
# Single iteration on Anvil fork (safe, no real funds)
almanak strat run --network anvil --once

# Single iteration on mainnet
almanak strat run --once

# Continuous with 30s interval
almanak strat run --network anvil --interval 30

# Dry run (no transactions)
almanak strat run --dry-run --once
```

## Config Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `collateral_token` | string | Token to supply (USDT) |
| `supply_amount` | string (Decimal) | Amount to supply (2 USDT, playground safety) |
| `min_collateral_usd` | string (Decimal) | Minimum USD value to enter position |
| `stop_loss_pct` | string (Decimal) | Max loss before exit (-0.10 = -10% USDT depeg) |
| `time_horizon_hours` | int | Max holding period (168h = 7 days) |
| `signal_id` | string | Edge signal ID for tracking |

## Intent Types Used

- `Intent.supply(protocol, token, amount, use_as_collateral=True)`
- `Intent.withdraw(protocol, token, amount, withdraw_all=True)`
- `Intent.hold(reason="...")`

## Key Patterns

- `decide(market)` receives a `MarketSnapshot` with `market.price()`, `market.balance()`, etc.
- State machine: idle -> supplying -> supplied -> withdrawing -> done
- Stop-loss monitors USDT depeg (price deviation from entry)
- Time horizon: exits after 168 hours (7 days)
- On failed intent execution, state reverts to previous stable state

## Teardown (Required)

| Method | Purpose |
|--------|---------|
| `get_open_positions() -> TeardownPositionSummary` | Lists the USDT supply position if active |
| `generate_teardown_intents(mode, market) -> list[Intent]` | Withdraws all USDT from Aave V3 |

## Testing

```bash
# Run unit tests
uv run pytest tests/ -v

# Paper trade (Anvil fork with PnL tracking)
almanak strat backtest paper --duration 3600 --interval 60
```
