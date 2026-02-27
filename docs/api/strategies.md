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
