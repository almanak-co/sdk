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
