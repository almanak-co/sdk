# EdgeEthFundingLongStrategy - Agent Guide

> AI coding agent context for the `edge_eth_funding_long` strategy.

## Overview

- **Template:** perps
- **Chain:** arbitrum (GMX V2 perps)
- **Class:** `EdgeEthFundingLongStrategy` in `strategy.py`
- **Config:** `config.json`
- **Signal:** 4869b240-e380-42c6-95dd-a70c913a35a8 (FUNDING_EXTREME, alpha 90)

This is a self-contained Python project with its own `pyproject.toml`, `.venv/`, and `uv.lock`.
The same `pyproject.toml` + `uv.lock` drive both local development and cloud deployment.

## Files

| File | Purpose |
|------|---------|
| `strategy.py` | Main strategy - edit `decide()` to change trading logic |
| `config.json` | Runtime parameters (tokens, thresholds, chain) |
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

## Adding Dependencies

```bash
# Add a package (updates pyproject.toml + uv.lock + .venv/)
uv add pandas-ta

# Run tests via uv
uv run pytest tests/ -v
```

## Config Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `perp_market` | string | Perpetual market (e.g. ETH/USD) |
| `collateral_token` | string | Collateral token (e.g. USDC) |
| `collateral_amount` | string (Decimal) | Collateral per trade ($2.50) |
| `position_size_usd` | string (Decimal) | Position size in USD ($5) |
| `leverage` | string (Decimal) | Leverage multiplier (2x) |
| `max_slippage` | string (Decimal) | Max slippage as decimal (0.005 = 50bps) |
| `take_profit_pct` | string (Decimal) | Take profit percentage (0.25 = 25%) |
| `stop_loss_pct` | string (Decimal) | Stop loss percentage (0.15 = 15%) |
| `time_horizon_hours` | int | Max hold time before exit (168 = 7 days) |
| `base_token` | string | Token for price checks (ETH) |


All values in `config.json` are read via `self.config.get("key", default)` in `__init__`.
String-typed Decimals (e.g. `"0.005"`) are used to avoid floating-point precision issues.

## Intent Types Used

This strategy uses these intent types:

- `Intent.perp_open(market, collateral_token, collateral_amount, size_usd, is_long=True)`
- `Intent.perp_close(market, collateral_token, is_long, size_usd=None)`
- `Intent.hold(reason="...")`

All intents are created via `from almanak.framework.intents import Intent`.

## Key Patterns

- `decide(market)` receives a `MarketSnapshot` with `market.price()`, `market.balance()`, `market.rsi()`, etc.
- Return an `Intent` object or `Intent.hold(reason=...)` from `decide()`
- Always wrap `decide()` logic in try/except, returning `Intent.hold()` on error
- Config values are read via `self.config.get("key", default)` in `__init__`
- State persists between iterations via `self.state` dict

## Teardown (Required)

Every `IntentStrategy` **must** implement two abstract teardown methods.
Strategies that hold no positions can extend `StatelessStrategy` instead.

| Method | Purpose |
|--------|---------|
| `get_open_positions() -> TeardownPositionSummary` | List positions to close (query on-chain state, not cache) |
| `generate_teardown_intents(mode, market) -> list[Intent]` | Return ordered intents to unwind positions |

**Execution order** (if multiple position types): PERP -> BORROW -> SUPPLY -> LP -> TOKEN

The generated `strategy.py` includes teardown stubs with TODO comments -- fill them in.
See `docs/internal/blueprints/14-teardown-system.md` for the full teardown system reference.

## Testing

```bash
# Run unit tests
uv run pytest tests/ -v

# Paper trade (Anvil fork with PnL tracking)
almanak strat backtest paper --duration 3600 --interval 60

# PnL backtest (historical prices)
almanak strat backtest pnl --start 2024-01-01 --end 2024-06-01
```

## Full SDK Reference

For the complete intent vocabulary, market data API, and advanced patterns,
install the full agent skill:

```bash
almanak agent install
```

Or read the bundled skill directly:

```bash
almanak docs agent-skill --dump
```
