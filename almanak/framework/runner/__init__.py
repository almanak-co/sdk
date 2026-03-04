"""Strategy Runner Module.

This module provides the StrategyRunner class for executing trading strategies
in production. It orchestrates the full execution pipeline:

1. Market data fetching via PriceOracle and BalanceProvider
2. Strategy decision-making via strategy.decide()
3. Intent compilation via IntentCompiler
4. Transaction execution via ExecutionOrchestrator
5. State persistence via StateManager
6. Error alerting via AlertManager

Example:
    from almanak.framework.runner import StrategyRunner, RunnerConfig
    from almanak.framework.strategies import MomentumStrategy

    # Create runner with dependencies
    runner = StrategyRunner(
        price_oracle=price_oracle,
        balance_provider=balance_provider,
        execution_orchestrator=orchestrator,
        state_manager=state_manager,
        alert_manager=alert_manager,
        config=RunnerConfig(
            default_interval_seconds=60,
            dry_run=False,
        ),
    )

    # Run continuously with graceful shutdown support
    runner.setup_signal_handlers()
    await runner.run_loop(strategy, interval_seconds=60)

    # Or run a single iteration
    result = await runner.run_iteration(strategy)
    if result.success:
        print(f"Iteration succeeded: {result.status.value}")
"""

from almanak.framework.runner.inner_runner import (
    EnrichedExecutionResult,
    IntentExecutionService,
    RetryPolicy,
    SadflowEvent,
)
from almanak.framework.runner.strategy_runner import (
    CriticalCallbackError,
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyProtocol,
    StrategyRunner,
)

__all__ = [
    "CriticalCallbackError",
    "EnrichedExecutionResult",
    "IntentExecutionService",
    "IterationResult",
    "IterationStatus",
    "RetryPolicy",
    "RunnerConfig",
    "SadflowEvent",
    "StrategyProtocol",
    "StrategyRunner",
]
