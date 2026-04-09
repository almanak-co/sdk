# EdgeSwethMeanRevStrategy - Agent Guide

> AI coding agent context for the `edge_sweth_mean_rev` strategy.

## Overview

- **Template:** ta_swap (heavily customized for LST mean reversion)
- **Chain:** ethereum
- **Class:** `EdgeSwethMeanRevStrategy` in `strategy.py`
- **Config:** `config.json`
- **Edge Signal:** `cfc82bb4-eb44-42b0-b085-84a7095a42a5`
- **Signal Type:** HIGH_CONVICTION_SYNTHESIS (LST_DEPEG_RISK x2)

This is a self-contained Python project with its own `pyproject.toml`, `.venv/`, and `uv.lock`.

## Strategy Design

**Thesis:** swETH (Swell liquid staking token) is trading at ~11.5% premium to its 1:1 ETH peg. The premium should mean-revert. Sell swETH for ETH to lock in the premium.

**State machine:**
- `idle` -> Check swETH/ETH premium. If premium > `min_premium_pct` (5%), swap swETH for WETH.
- `swapped` -> Trade complete. Hold ETH. Strategy is finished.

**One-shot trade:** This is NOT a recurring strategy. Once the swap executes, the position is complete and the strategy holds indefinitely.

**CRITICAL NOTE:** The swETH token address (`0xf951E335afb289353dc249e82926178EaC7DEd78`) is hardcoded — it is NOT in the SDK's static token registry. Verify this address before deploying with real funds.

## Files

| File | Purpose |
|------|---------|
| `strategy.py` | Main strategy — state machine + decide() logic |
| `config.json` | Signal-derived parameters (tokens, thresholds, amounts) |
| `pyproject.toml` | Dependencies plus metadata |
| `tests/test_strategy.py` | Unit tests covering all state transitions |

## Config Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `signal_id` | string | Edge signal that originated this strategy |
| `sell_token` | string | swETH contract address |
| `buy_token` | string | Token to receive (WETH) |
| `sell_amount` | string | Amount of swETH to sell (default "0.001") |
| `min_premium_pct` | string | Minimum premium % to trigger entry (default "5.0") |
| `stop_loss_pct` | string | Max loss tolerance as decimal (default "-0.05") |
| `time_horizon_hours` | int | Max hours to wait for entry (default 72) |
| `max_slippage_bps` | int | Max slippage in basis points (default 50 = 0.5%) |

## Intent Types Used

- `Intent.swap(from_token, to_token, amount=, max_slippage=, protocol="uniswap_v3")`
- `Intent.hold(reason="...")`

## How to Run

```bash
# Single iteration on Anvil fork (safe, no real funds)
almanak strat run --network anvil --once

# Single iteration on mainnet
almanak strat run --once

# Dry run (no transactions)
almanak strat run --dry-run --once
```

## Testing

```bash
uv run pytest tests/ -v
```

## Teardown

| State | Teardown Action |
|-------|----------------|
| `idle` | Sell all swETH for WETH (emergency exit) |
| `swapped` | No action (already holding ETH) |
