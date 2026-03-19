"""Generate per-strategy AGENTS.md files for AI coding agent context.

Each scaffolded strategy gets a tailored AGENTS.md that tells agents:
- What this strategy does and which intents it uses
- How to run and test it
- Which files to read and modify
- Links to the full SDK skill reference
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyGuideConfig:
    """Parameters for generating a strategy-specific AGENTS.md."""

    strategy_name: str
    template_name: str
    chain: str
    class_name: str


# Maps template -> list of intent types the template actually uses
TEMPLATE_INTENT_MAP: dict[str, list[str]] = {
    "blank": ["SWAP", "HOLD"],
    "ta_swap": ["SWAP", "HOLD"],
    "dynamic_lp": ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES", "HOLD"],
    "lending_loop": ["SUPPLY", "BORROW", "SWAP", "REPAY", "WITHDRAW", "HOLD"],
    "basis_trade": ["SWAP", "PERP_OPEN", "PERP_CLOSE", "HOLD"],
    "vault_yield": ["VAULT_DEPOSIT", "VAULT_REDEEM", "HOLD"],
    "copy_trader": [
        "SWAP",
        "LP_OPEN",
        "LP_CLOSE",
        "SUPPLY",
        "WITHDRAW",
        "BORROW",
        "REPAY",
        "PERP_OPEN",
        "PERP_CLOSE",
        "HOLD",
    ],
    "perps": ["PERP_OPEN", "PERP_CLOSE", "HOLD"],
    "multi_step": ["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"],
    "staking": ["STAKE", "UNSTAKE", "SWAP", "HOLD"],
}

# Maps intent type -> the Intent factory method signature (simplified)
_INTENT_QUICK_REF: dict[str, str] = {
    "SWAP": 'Intent.swap(from_token, to_token, amount_usd=, max_slippage=Decimal("0.005"))',
    "LP_OPEN": "Intent.lp_open(pool, amount0, amount1, range_lower, range_upper, protocol=)",
    "LP_CLOSE": "Intent.lp_close(position_id, collect_fees=True, protocol=)",
    "LP_COLLECT_FEES": 'Intent.collect_fees(pool, protocol="traderjoe_v2")',
    "SUPPLY": "Intent.supply(protocol, token, amount, use_as_collateral=True)",
    "BORROW": "Intent.borrow(protocol, collateral_token, collateral_amount, borrow_token, borrow_amount)",
    "REPAY": "Intent.repay(protocol, token, amount, repay_full=False)",
    "WITHDRAW": "Intent.withdraw(protocol, token, amount, withdraw_all=False)",
    "PERP_OPEN": "Intent.perp_open(market, collateral_token, collateral_amount, size_usd, is_long=True)",
    "PERP_CLOSE": "Intent.perp_close(market, collateral_token, is_long, size_usd=None)",
    "STAKE": "Intent.stake(protocol, token_in, amount, receive_wrapped=True, chain=None)",
    "UNSTAKE": "Intent.unstake(protocol, token_in, amount, chain=None)",
    "VAULT_DEPOSIT": "Intent.vault_deposit(protocol, vault_address, amount, deposit_token=None, chain=None)",
    "VAULT_REDEEM": "Intent.vault_redeem(protocol, vault_address, shares, deposit_token=None, chain=None)",
    "HOLD": 'Intent.hold(reason="...")',
}

# Maps template -> config parameter documentation (name, type, description)
TEMPLATE_CONFIG_DOCS: dict[str, list[tuple[str, str, str]]] = {
    "blank": [
        ("base_token", "string", "Token to buy (e.g. WETH)"),
        ("quote_token", "string", "Token to sell / quote currency (e.g. USDC)"),
        ("trade_size_usd", "string (Decimal)", "Trade size in USD"),
    ],
    "ta_swap": [
        ("indicator", "string", 'Which indicator to use: "rsi", "bollinger", or "rsi_bb"'),
        ("base_token", "string", "Token to trade"),
        ("quote_token", "string", "Quote currency"),
        ("trade_size_usd", "int", "Trade size in USD"),
        ("max_slippage_bps", "int", "Max slippage in basis points (default 50 = 0.5%)"),
        ("rsi_period", "int", "RSI lookback period (RSI/rsi_bb mode)"),
        ("rsi_oversold", "int", "RSI buy threshold (default 30)"),
        ("rsi_overbought", "int", "RSI sell threshold (default 70)"),
        ("bb_period", "int", "Bollinger Bands period (bollinger/rsi_bb mode, default 20)"),
        ("bb_std_dev", "float", "Bollinger Bands std deviation multiplier (default 2.0)"),
    ],
    "dynamic_lp": [
        ("pool_address", "string", "Uniswap V3 / DEX pool address"),
        ("protocol", "string", "LP protocol (uniswap_v3, aerodrome, etc.)"),
        ("base_token", "string", "Pool base token"),
        ("quote_token", "string", "Pool quote token"),
        ("range_width_pct", "int", "LP range width as % of current price"),
        ("rebalance_threshold_pct", "int", "Rebalance when position drifts this % from center"),
        ("min_position_usd", "int", "Minimum USD value to open a position"),
    ],
    "lending_loop": [
        ("collateral_token", "string", "Token to supply as collateral (e.g. WETH)"),
        ("borrow_token", "string", "Token to borrow (e.g. USDC)"),
        ("supply_amount", "string (Decimal)", "Initial collateral amount to supply"),
        ("borrow_amount", "string (Decimal)", "First-loop borrow amount in borrow_token"),
        ("target_leverage", "string (Decimal)", "Target leverage (e.g. 2.0 = 2x)"),
        ("borrow_ratio", "string (Decimal)", "LTV usage per loop (0.7 = 70%), controls borrow decay"),
        ("min_health_factor", "string (Decimal)", "Minimum health factor before repay (e.g. 1.5)"),
        ("min_collateral_usd", "string (Decimal)", "Minimum collateral USD to start"),
    ],
    "basis_trade": [
        ("base_token", "string", "Spot token to buy (e.g. WETH)"),
        ("quote_token", "string", "Quote currency (e.g. USDC)"),
        ("perp_market", "string", "Perpetual market identifier (e.g. ETH/USD)"),
        ("spot_size_usd", "string (Decimal)", "USD size of the spot leg"),
        ("hedge_ratio", "string (Decimal)", "Perp size / spot size (1.0 = delta neutral)"),
        ("funding_entry_threshold", "string (Decimal)", "Min hourly funding rate to enter (e.g. 0.0001 = 0.01%/hr)"),
        ("funding_exit_threshold", "string (Decimal)", "Exit if funding drops below this (e.g. -0.00005)"),
    ],
    "vault_yield": [
        ("vault_address", "string", "ERC-4626 vault contract address"),
        ("deposit_token", "string", "Token to deposit (e.g. USDC)"),
        ("deposit_amount", "int", "Amount to deposit"),
        ("min_deposit_usd", "int", "Minimum USD value to deposit"),
        ("max_vault_allocation_pct", "int", "Max % of balance to allocate to vault"),
    ],
    "perps": [
        ("perp_market", "string", "Perpetual market (e.g. ETH/USD)"),
        ("collateral_token", "string", "Collateral token (e.g. USDC)"),
        ("collateral_amount", "string (Decimal)", "Collateral per trade"),
        ("position_size_usd", "string (Decimal)", "Position size in USD"),
        ("leverage", "string (Decimal)", "Leverage multiplier"),
        ("take_profit_pct", "string (Decimal)", "Take profit percentage (e.g. 0.05 = 5%)"),
        ("stop_loss_pct", "string (Decimal)", "Stop loss percentage (e.g. 0.03 = 3%)"),
        ("base_token", "string", "Token for price checks (e.g. ETH)"),
    ],
    "multi_step": [
        ("pool_address", "string", "DEX pool address for LP"),
        ("protocol", "string", "LP protocol (uniswap_v3, aerodrome, etc.)"),
        ("base_token", "string", "Pool base token"),
        ("quote_token", "string", "Pool quote token"),
        ("range_width_pct", "int", "LP range width as % of current price"),
        ("rebalance_threshold_pct", "int", "Price drift % to trigger rebalance"),
        ("min_position_usd", "int", "Minimum USD value to open a position"),
    ],
    "staking": [
        ("staking_protocol", "string", "Staking protocol (e.g. lido)"),
        ("stake_token", "string", "Token to stake (e.g. ETH)"),
        ("quote_token", "string", "Quote currency to swap from when swap_before_stake=true"),
        ("stake_amount", "string (Decimal)", "Amount to stake"),
        ("swap_before_stake", "bool", "Whether to swap quote_token to stake_token first"),
    ],
    "copy_trader": [
        ("copy_trading.leaders", "list", "Leader wallets to copy [{address, chain}]"),
        ("copy_trading.sizing.mode", "string", "Sizing mode (fixed_usd)"),
        ("copy_trading.sizing.fixed_usd", "int", "Fixed USD per trade"),
        ("copy_trading.risk.max_trade_usd", "int", "Max single trade size"),
        ("copy_trading.risk.max_slippage", "string (Decimal)", "Max slippage (e.g. 0.01 = 1%)"),
    ],
}


def generate_strategy_agents_md(config: StrategyGuideConfig) -> str:
    """Generate a per-strategy AGENTS.md file.

    Returns the markdown content as a string.
    """
    intent_types = TEMPLATE_INTENT_MAP.get(config.template_name, ["SWAP", "HOLD"])

    # Build intent quick reference
    intent_lines = []
    for it in intent_types:
        ref = _INTENT_QUICK_REF.get(it, f"Intent.{it.lower()}(...)")
        intent_lines.append(f"- `{ref}`")
    intent_ref = "\n".join(intent_lines)

    # Build config parameter documentation
    config_docs = TEMPLATE_CONFIG_DOCS.get(config.template_name, [])
    if config_docs:
        config_table = "| Parameter | Type | Description |\n|-----------|------|-------------|\n"
        for name, type_, desc in config_docs:
            config_table += f"| `{name}` | {type_} | {desc} |\n"
    else:
        config_table = "See `config.json` and `strategy.py` `__init__` for available parameters."

    return f"""# {config.class_name} - Agent Guide

> AI coding agent context for the `{config.strategy_name}` strategy.

## Overview

- **Template:** {config.template_name}
- **Chain:** {config.chain}
- **Class:** `{config.class_name}` in `strategy.py`
- **Config:** `config.json`

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

{config_table}

All values in `config.json` are read via `self.config.get("key", default)` in `__init__`.
String-typed Decimals (e.g. `"0.005"`) are used to avoid floating-point precision issues.

## Intent Types Used

This strategy uses these intent types:

{intent_ref}

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
See `blueprints/14-teardown-system.md` for the full teardown system reference.

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
"""
