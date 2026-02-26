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
    "dynamic_lp": ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES", "HOLD"],
    "mean_reversion": ["SWAP", "HOLD"],
    "bollinger": ["SWAP", "HOLD"],
    "basis_trade": ["SWAP", "PERP_OPEN", "PERP_CLOSE", "HOLD"],
    "lending_loop": ["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
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
    "blank": ["SWAP", "HOLD"],
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
    "HOLD": 'Intent.hold(reason="...")',
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

    return f"""# {config.class_name} - Agent Guide

> AI coding agent context for the `{config.strategy_name}` strategy.

## Overview

- **Template:** {config.template_name}
- **Chain:** {config.chain}
- **Class:** `{config.class_name}` in `strategy.py`
- **Config:** `config.json`

## Files

| File | Purpose |
|------|---------|
| `strategy.py` | Main strategy - edit `decide()` to change trading logic |
| `config.json` | Runtime parameters (tokens, thresholds, chain) |
| `.env` | Secrets (private key, API keys) - never commit this |
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
