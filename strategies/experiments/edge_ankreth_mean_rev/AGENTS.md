# EdgeAnkrethMeanRevStrategy - Agent Guide

> AI coding agent context for the `edge_ankreth_mean_rev` strategy.

## Overview

- **Template:** ta_swap (heavily customized for LST mean reversion)
- **Chain:** ethereum
- **Class:** `EdgeAnkrethMeanRevStrategy` in `strategy.py`
- **Config:** `config.json`
- **Edge Signal:** `92e14fed-96ac-4b3c-9ae3-d4b4bde1203c`
- **Signal Type:** HIGH_CONVICTION_SYNTHESIS (LST_DEPEG_RISK x2)
- **Alpha:** 83/100, Regime: BEAR

This is a self-contained Python project with its own `pyproject.toml`, `.venv/`, and `uv.lock`.

## Strategy Design

**Thesis:** ankrETH (Ankr liquid staking token) is trading at 16.55% premium to its expected peg of 1.05 ETH. The premium should mean-revert. Swap ankrETH -> ETH to lock in the premium.

**State machine:**
- `idle` -> Check ankrETH/ETH premium above 1.05 peg. If premium > `min_premium_pct` (10%), swap ankrETH for WETH.
- `swapped` -> Trade complete. Hold ETH. Strategy is finished.

**One-shot trade:** This is NOT a recurring strategy. Once the swap executes, the position is complete and the strategy holds indefinitely.

**Peg ratio:** Unlike 1:1 LSTs (swETH, stETH), ankrETH accrues staking yield in its exchange rate. Fair value is 1.05 ETH per ankrETH. Premium is calculated as deviation above this peg, not above 1:1.

**CRITICAL NOTE:** The ankrETH token address (`0xE95A203B1a91a908F9B9CE46459d101078c2c3cb`) is hardcoded — it is NOT in the SDK's static token registry. Verify this address before deploying with real funds.

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
| `sell_token` | string | ankrETH contract address |
| `sell_token_address` | string | Same as sell_token (explicit address) |
| `buy_token` | string | Token to receive (WETH) |
| `sell_amount` | string | Amount of ankrETH to sell (default "0.001") |
| `peg_ratio` | string | ankrETH/ETH fair-value peg (default "1.05") |
| `min_premium_pct` | string | Minimum premium % above peg to trigger entry (default "10.0") |
| `stop_loss_pct` | string | Max loss tolerance as decimal (default "-0.075") |
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
| `idle` | Sell all ankrETH for WETH (emergency exit) |
| `swapped` | No action (already holding ETH) |
