# API Reference

This section documents the public Python API of the Almanak SDK.

## Import Cheat Sheet

```python
# Top-level exports (most common)
from almanak import (
    Almanak,
    Chain, Network, Protocol, ActionType,
    IntentStrategy, MarketSnapshot,
    SwapIntent, HoldIntent, LPOpenIntent, LPCloseIntent,
    BorrowIntent, RepayIntent,
    StateManager, RiskGuard,
    BacktestMetrics, BacktestResult,
)

# Deep imports for specific functionality
from almanak.framework.strategies import IntentStrategy, StrategyBase
from almanak.framework.intents import SwapIntent, IntentCompiler
from almanak.framework.state import StateManager
from almanak.framework.execution import ExecutionOrchestrator
from almanak.framework.data import MarketSnapshot
from almanak.framework.data.tokens import get_token_resolver, TokenResolver

# Backtesting
from almanak.framework.backtesting import PnLBacktester, PnLBacktestConfig

# Logging
from almanak.framework.utils.logging import configure_logging, get_logger
```

## Module Overview

| Module | Description |
|--------|-------------|
| [Enums](enums.md) | `Chain`, `Network`, `Protocol`, `ActionType`, and more |
| [Strategies](strategies.md) | `IntentStrategy`, `StrategyBase`, `MarketSnapshot` |
| [Intents](intents.md) | `SwapIntent`, `LPOpenIntent`, `HoldIntent`, and all intent types |
| [Compiler](compiler.md) | `IntentCompiler` - compiles intents to transactions |
| [State](state.md) | `StateManager` - persistence and migrations |
| [Execution](execution.md) | `ExecutionOrchestrator` - transaction execution pipeline |
| [Data](data.md) | `PriceOracle`, `BalanceProvider`, `MarketSnapshot` |
| [Tokens](tokens.md) | `TokenResolver` - unified token resolution |
| [Indicators](indicators.md) | RSI, MACD, Bollinger Bands, and more |
| [Connectors](connectors/index.md) | Protocol adapters (Uniswap, Aave, Morpho, etc.) |
| [Services](services.md) | `StuckDetector`, `EmergencyManager` |
| [Alerting](alerting.md) | `AlertManager`, Slack/Telegram channels |
| [Backtesting](backtesting.md) | `PnLBacktester`, `PaperTrader`, parameter sweeps |
| [Deployment](deployment.md) | `CanaryDeployment` |
| [Logging](logging.md) | `configure_logging`, `get_logger` |
