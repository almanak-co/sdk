# Strategies

Strategy base classes and the market snapshot interface.

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
