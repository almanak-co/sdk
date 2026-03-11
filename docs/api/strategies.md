# Strategies

Strategy base classes and the market snapshot interface.

## Implementing Teardown

Every strategy **must** implement three teardown methods so operators can safely close positions. Without them, close-requests are silently ignored.

### Required Methods

| Method | Purpose |
|--------|---------|
| `supports_teardown() -> bool` | Return `True` to enable teardown |
| `get_open_positions()` | Return a `TeardownPositionSummary` listing all open positions |
| `generate_teardown_intents(mode, market)` | Return an ordered list of `Intent` objects that unwind positions |

### Execution Order

If your strategy holds multiple position types, teardown intents must follow this order:

1. **PERP** -- close perpetual positions first (highest risk)
2. **BORROW** -- repay borrows to free collateral
3. **SUPPLY** -- withdraw supplied collateral
4. **LP** -- close liquidity positions
5. **TOKEN** -- swap remaining tokens to stable

### Example: Swap Strategy Teardown

```python
from decimal import Decimal
from almanak import IntentStrategy, Intent

class MyStrategy(IntentStrategy):
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from datetime import UTC, datetime
        from almanak.framework.teardown import (
            PositionInfo, PositionType, TeardownPositionSummary,
        )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "my_strategy"),
            timestamp=datetime.now(UTC),
            positions=[
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="my_strategy_eth",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=Decimal("1000"),  # query on-chain balance, not cache
                    details={"asset": "WETH"},
                )
            ],
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
        return [
            Intent.swap(
                from_token="WETH", to_token="USDC",
                amount="all", max_slippage=max_slippage, protocol="uniswap_v3",
            )
        ]
```

!!! warning "Always query on-chain state"
    `get_open_positions()` must query live on-chain balances, not cached values. Stale data can cause teardown to skip positions or attempt to close positions that no longer exist.

## State Persistence

Strategies that track internal state across iterations (position IDs, phase tracking, trade counters) must implement two hooks so that state survives restarts.

The framework persists runner-level metadata automatically (iteration counts, consecutive errors, execution progress for multi-step intents). But **strategy-specific state is opt-in** -- without these hooks, instance variables are lost when the process stops.

### Required Hooks

| Method | Called | Purpose |
|--------|--------|---------|
| `get_persistent_state() -> dict` | After each iteration | Return a dict of state to save |
| `load_persistent_state(state: dict)` | On startup / resume | Restore state from the saved dict |

### Example: LP Strategy with Position Tracking

```python
from decimal import Decimal
from typing import Any
from almanak import IntentStrategy, Intent, MarketSnapshot

class MyLPStrategy(IntentStrategy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._position_id: int | None = None
        self._has_position: bool = False
        self._total_fees_collected: Decimal = Decimal("0")

    def decide(self, market: MarketSnapshot) -> Intent | None:
        if self._has_position:
            return Intent.collect_fees(position_id=self._position_id, protocol="uniswap_v3")
        return Intent.lp_open(
            token_a="WETH", token_b="USDC",
            amount_usd=Decimal("1000"), protocol="uniswap_v3",
        )

    def on_intent_executed(self, intent, success: bool, result):
        """Update state after execution -- persisted via get_persistent_state()."""
        if success and result.position_id:
            self._position_id = result.position_id
            self._has_position = True

    # -- State persistence hooks --

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "position_id": self._position_id,
            "has_position": self._has_position,
            "total_fees_collected": str(self._total_fees_collected),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._position_id = state.get("position_id")
        self._has_position = state.get("has_position", False)
        self._total_fees_collected = Decimal(state.get("total_fees_collected", "0"))
```

### What the Framework Persists Automatically

These are saved by the runner without any strategy-author code:

| State | Persisted to | Survives restart |
|-------|-------------|-----------------|
| Iteration count, success count, error count | `strategy_state` table | Yes |
| Multi-step execution progress (which step completed, serialized intents) | `strategy_state.execution_progress` | Yes -- runner resumes from the failed step |
| Operator pause flag | `strategy_state.is_paused` | Yes |
| Portfolio snapshots (total value, positions) | `portfolio_snapshots` table | Yes |
| Portfolio PnL baseline (initial value, deposits, gas) | `portfolio_metrics` table | Yes |
| Timeline events (execution audit trail) | `timeline_events` table | Yes |

### Guidelines

- **Use defensive `.get()` with defaults** in `load_persistent_state()` so older saved state doesn't crash when you add new fields.
- **Store `Decimal` as strings** (`str(amount)`) and parse back (`Decimal(state["amount"])`) for safe JSON round-tripping.
- **`on_intent_executed()` is the natural place to update state** after a trade (e.g., storing a position ID). `get_persistent_state()` then picks it up for the next save.
- **All values must be JSON-serializable.** The state dict is stored as a JSON blob in the database.

!!! warning "Without these hooks, strategy state is lost on restart"
    If you store state in instance variables but don't implement the persistence hooks, a restart means your strategy has no memory of open positions, completed trades, or internal phase. This is especially dangerous for LP and lending strategies where losing a position ID means the strategy cannot close its own positions.

## IntentStrategy

The primary base class for writing strategies. Implement the `decide()` method to return an `Intent`.

::: almanak.framework.strategies.IntentStrategy
    options:
      show_root_heading: true
      members_order: source

## StrategyBase

Lower-level base class for strategies that need direct action control.

::: almanak.framework.strategies.StrategyBase
    options:
      show_root_heading: true
      members_order: source

## MarketSnapshot

Unified interface for accessing market data within `decide()`.

::: almanak.framework.strategies.MarketSnapshot
    options:
      show_root_heading: true
      members_order: source

## RiskGuard

Non-bypassable risk validation that runs before every execution.

::: almanak.framework.strategies.RiskGuard
    options:
      show_root_heading: true
      members_order: source

## RiskGuardConfig

::: almanak.framework.strategies.RiskGuardConfig
    options:
      show_root_heading: true
      members_order: source

## DecideResult

::: almanak.framework.strategies.DecideResult
    options:
      show_root_heading: true
      members_order: source

## IntentSequence

::: almanak.framework.strategies.IntentSequence
    options:
      show_root_heading: true
      members_order: source

## ExecutionResult

::: almanak.framework.strategies.ExecutionResult
    options:
      show_root_heading: true
      members_order: source

## Market Data Types

Data types returned by `MarketSnapshot` getters and accepted by `set_*` methods for unit testing.

### TokenBalance

::: almanak.framework.strategies.TokenBalance
    options:
      show_root_heading: true
      members_order: source

### PriceData

::: almanak.framework.strategies.PriceData
    options:
      show_root_heading: true
      members_order: source

### RSIData

::: almanak.framework.strategies.RSIData
    options:
      show_root_heading: true
      members_order: source

### MACDData

::: almanak.framework.strategies.MACDData
    options:
      show_root_heading: true
      members_order: source

### BollingerBandsData

::: almanak.framework.strategies.BollingerBandsData
    options:
      show_root_heading: true
      members_order: source

### StochasticData

::: almanak.framework.strategies.StochasticData
    options:
      show_root_heading: true
      members_order: source

### ATRData

::: almanak.framework.strategies.ATRData
    options:
      show_root_heading: true
      members_order: source

### MAData

::: almanak.framework.strategies.MAData
    options:
      show_root_heading: true
      members_order: source

### ADXData

::: almanak.framework.strategies.ADXData
    options:
      show_root_heading: true
      members_order: source

### OBVData

::: almanak.framework.strategies.OBVData
    options:
      show_root_heading: true
      members_order: source

### CCIData

::: almanak.framework.strategies.CCIData
    options:
      show_root_heading: true
      members_order: source

### IchimokuData

::: almanak.framework.strategies.IchimokuData
    options:
      show_root_heading: true
      members_order: source

### ChainHealthStatus

::: almanak.framework.strategies.ChainHealthStatus
    options:
      show_root_heading: true
      members_order: source

### ChainHealth

::: almanak.framework.strategies.ChainHealth
    options:
      show_root_heading: true
      members_order: source
