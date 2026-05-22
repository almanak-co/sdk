"""Phase helpers for :class:`PnLBacktester` (Phases 6C.2 + 6C.3).

This module contains phase-level helpers extracted from the main body of
``PnLBacktester`` to reduce cyclomatic complexity and isolate responsibilities.
Every helper preserves the EXACT original behavior captured by the
characterization tests in
``tests/unit/backtesting/pnl/test_engine_characterization.py``.

Extracted surfaces
------------------
* ``_run_backtest`` — preflight, initialization, iteration loop, error path,
  and finalization (Phase 6C.2).
* ``_calculate_token_flows`` — per-intent-type token-inflow / token-outflow
  helpers plus a :func:`calculate_token_flows` dispatch sequencer (Phase
  6C.3).

Design notes
------------
* Helpers are module-level functions (not methods) that take the backtester
  instance explicitly. This mirrors the pattern established by
  ``runner/_run_loop_helpers.py`` (Phase 6A.2) -- keeps ``self.`` noise out
  of the slim orchestrator while still respecting the backtester's private
  state (``self._adapter``, ``self._error_handler`` etc.).
* ``BacktestState`` is a mutable container for all local state that flows
  between the initialization, iteration, error-path, and finalization
  phases. It is NOT part of any public API and is deliberately only a
  dataclass so helpers can mutate it in place exactly like the original
  ``_run_backtest`` body mutated its locals.
* Log messages, ``bt_logger.phase(...)`` boundaries, error-path partial
  ``BacktestResult`` fields, and ``run_started_at`` / ``run_ended_at``
  timestamps are reproduced byte-for-byte from the pre-extraction body.
* ``_engine_helpers`` does NOT import from ``engine`` at module load time --
  it uses ``TYPE_CHECKING`` to avoid a circular import while still offering
  typed signatures.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    IntentType,
    ParameterSourceTracker,
    PreflightReport,
)
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataCapability,
    HistoricalDataConfig,
    MarketState,
)
from almanak.framework.backtesting.pnl.data_quality import DataQualityTracker
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestErrorConfig,
    BacktestErrorHandler,
    PreflightValidationError,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.engine import (
        BacktestableStrategy,
        PnLBacktester,
    )
    from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine
    from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger


logger = logging.getLogger(__name__)


# =============================================================================
# Shared mutable state container
# =============================================================================


@dataclass
class BacktestState:
    """Mutable container for state that flows through ``_run_backtest`` phases.

    Populated by :func:`initialize_backtest`, mutated in place by
    :func:`execute_iteration_loop`, and consumed by
    :func:`finalize_backtest_result` / :func:`build_error_result`.
    """

    # Populated by initialize_backtest
    portfolio: SimulatedPortfolio
    data_config: HistoricalDataConfig
    data_source_capabilities: dict[str, HistoricalDataCapability]
    data_source_warnings: list[str]
    compliance_violations: list[str]
    data_quality_tracker: DataQualityTracker
    indicator_engine: BacktestIndicatorEngine
    strategy_config: dict[str, Any]
    parameter_sources: ParameterSourceTracker
    total_ticks: int

    # Mutated during execute_iteration_loop
    pending_intents: list[tuple[Any, datetime, int]] = field(default_factory=list)
    last_market_state: MarketState | None = None
    tick_count: int = 0
    execution_delayed_at_end: int = 0


# =============================================================================
# Preflight
# =============================================================================


async def run_preflight(
    backtester: PnLBacktester,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
) -> tuple[PreflightReport | None, bool]:
    """Execute preflight validation if enabled.

    Returns ``(preflight_report, preflight_passed)``. ``preflight_passed``
    defaults to ``True`` when validation is disabled, mirroring the
    pre-extraction behavior.

    Raises:
        PreflightValidationError: if ``config.fail_on_preflight_error`` is
            True and any check failed.
    """
    preflight_report: PreflightReport | None = None
    preflight_passed: bool = True  # Default to True if validation is disabled
    if config.preflight_validation:
        with bt_logger.phase("preflight_validation"):
            bt_logger.info("Running preflight validation checks...")
            preflight_report = await backtester.run_preflight_validation(config)
            preflight_passed = preflight_report.passed

            if preflight_report.passed:
                bt_logger.info(
                    f"Preflight validation passed: "
                    f"{len(preflight_report.tokens_available)} tokens available, "
                    f"estimated coverage {preflight_report.estimated_coverage:.1%}"
                )
            else:
                # Log details about what failed
                bt_logger.warning(
                    f"Preflight validation issues detected: "
                    f"{preflight_report.error_count} errors, "
                    f"{preflight_report.warning_count} warnings"
                )
                for check in preflight_report.failed_checks:
                    bt_logger.warning(f"  - [{check.severity.upper()}] {check.check_name}: {check.message}")

                # Fail fast if configured to do so
                if config.fail_on_preflight_error:
                    failed_check_names = [c.check_name for c in preflight_report.failed_checks]
                    raise PreflightValidationError(
                        message=(
                            f"Preflight validation failed with {preflight_report.error_count} errors "
                            f"and {preflight_report.warning_count} warnings. "
                            "Set fail_on_preflight_error=False to continue with degraded mode."
                        ),
                        failed_checks=failed_check_names,
                        recommendations=preflight_report.recommendations,
                        error_count=preflight_report.error_count,
                        warning_count=preflight_report.warning_count,
                    )
                else:
                    bt_logger.warning(
                        "Continuing in degraded mode (fail_on_preflight_error=False). "
                        "Results may be inaccurate due to data quality issues."
                    )
    return preflight_report, preflight_passed


# =============================================================================
# Initialization
# =============================================================================


def initialize_backtest(
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
) -> BacktestState:
    """Run the ``bt_logger.phase("initialization")`` block.

    Mirrors the original init block: error handler, MEV simulator,
    strategy adapter, parameter source tracker, portfolio, historical data
    config, data source capabilities, compliance-violation seeding for
    ``CURRENT_ONLY`` providers, gas-price record tracking, data quality
    tracker, indicator engine, strategy config dict, and tick counters.
    """
    with bt_logger.phase("initialization"):
        # Initialize error handler for consistent error classification
        backtester._error_handler = BacktestErrorHandler(BacktestErrorConfig())
        bt_logger.debug("Initialized BacktestErrorHandler for error classification")

        # Initialize MEV simulator based on config
        backtester._init_mev_simulator(config)

        # Initialize strategy adapter for strategy-specific backtesting
        backtester._init_adapter(strategy)

        # Create parameter source tracker for audit trail
        # This must be created after _init_adapter so we can track adapter-specific params
        parameter_sources = backtester._create_parameter_source_tracker(config)
        bt_logger.debug(
            f"Tracked {len(parameter_sources.records)} parameter sources "
            f"({len(parameter_sources.config_sources)} config, "
            f"{len(parameter_sources.liquidation_sources)} liquidation, "
            f"{len(parameter_sources.apy_funding_sources)} apy/funding)"
        )

        # Initialize portfolio
        portfolio = SimulatedPortfolio(
            initial_capital_usd=config.initial_capital_usd,
        )

        # Create historical data config
        data_config = HistoricalDataConfig(
            start_time=config.start_time,
            end_time=config.end_time,
            interval_seconds=config.interval_seconds,
            tokens=config.tokens,
            chains=[config.chain],
        )

        # Collect data source capabilities and generate warnings
        data_source_capabilities, data_source_warnings = backtester._collect_data_source_capabilities(bt_logger)

        # Track compliance violations for institutional reporting
        # These indicate potential issues with backtest accuracy/reproducibility
        compliance_violations: list[str] = []

        # Check for CURRENT_ONLY providers which affect historical accuracy
        for provider_name, capability in data_source_capabilities.items():
            if capability == HistoricalDataCapability.CURRENT_ONLY:
                compliance_violations.append(
                    f"CURRENT_ONLY data provider used: '{provider_name}'. "
                    "Historical prices are not available; backtest uses runtime prices."
                )

        # Initialize gas price records tracking (if enabled)
        backtester._gas_price_records = [] if config.track_gas_prices else None

        # Initialize data quality tracker
        data_quality_tracker = DataQualityTracker(
            staleness_threshold_seconds=config.staleness_threshold_seconds,
        )

        # Initialize indicator engine for populating MarketSnapshot with TA indicators
        # This enables strategies using market.rsi(), market.macd(), market.bollinger_bands()
        # to work identically in live and backtest modes.
        indicator_engine = backtester._create_indicator_engine(strategy)
        strategy_config = backtester._get_strategy_config_dict(strategy)

        # Iteration counter for logging
        total_ticks = config.estimated_ticks

    return BacktestState(
        portfolio=portfolio,
        data_config=data_config,
        data_source_capabilities=data_source_capabilities,
        data_source_warnings=data_source_warnings,
        compliance_violations=compliance_violations,
        data_quality_tracker=data_quality_tracker,
        indicator_engine=indicator_engine,
        strategy_config=strategy_config,
        parameter_sources=parameter_sources,
        total_ticks=total_ticks,
    )


# =============================================================================
# Iteration loop
# =============================================================================


async def execute_iteration_loop(
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
    state: BacktestState,
) -> None:
    """Run the ``bt_logger.phase("simulation")`` iteration loop.

    Mirrors the async-for loop body: per-tick progress logging, snapshot
    creation, indicator engine warm-up, data-quality tracking, pending
    intent execution, strategy decide (with warm-up + error-handler
    classification), intent queuing, adapter position updates, mark to
    market, and end-of-simulation pending-intent drain.

    Mutates ``state`` in place (``pending_intents``, ``last_market_state``,
    ``tick_count``, ``execution_delayed_at_end``).
    """
    # Local import to avoid cyclic import at module load
    from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state

    with bt_logger.phase("simulation"):
        # Iterate through historical data
        async for timestamp, market_state in backtester.data_provider.iterate(state.data_config):
            state.tick_count += 1

            # Log progress periodically
            if state.tick_count % 100 == 0 or state.tick_count == 1:
                bt_logger.info(
                    f"Backtest progress: {state.tick_count}/{state.total_ticks} ticks "
                    f"({100 * state.tick_count / state.total_ticks:.1f}%)"
                )

            # Create market snapshot for strategy
            snapshot = create_market_snapshot_from_state(
                market_state=market_state,
                chain=config.chain,
                portfolio=state.portfolio,
            )

            # Cache available_tokens once per tick: the property returns a
            # fresh list on every access, and we use it in multiple loops
            # below. Also build an upper-case set so the membership check
            # inside the expected_tokens loop is O(1) instead of O(N) per
            # expected token (see #1781).
            available_tokens = market_state.available_tokens
            available_tokens_upper = {t.upper() for t in available_tokens}

            # Append prices to indicator engine and populate snapshot
            tick_tokens: set[str] = set()
            for token in available_tokens:
                try:
                    price = market_state.get_price(token)
                    state.indicator_engine.append_price(token, price)
                    tick_tokens.add(token)
                except KeyError:
                    pass
            state.indicator_engine.populate_snapshot(snapshot, state.strategy_config, active_tokens=tick_tokens)

            # Track data quality: record successful price lookups
            # Count tokens with available prices in this tick
            expected_tokens = config.tokens
            provider_name = getattr(backtester.data_provider, "provider_name", "unknown")

            # Record successful lookups for each available token
            for token in expected_tokens:
                if token.upper() in available_tokens_upper:
                    state.data_quality_tracker.record_lookup(
                        success=True,
                        source=provider_name,
                    )
                else:
                    state.data_quality_tracker.record_lookup(success=False)

            # Execute any pending intents that have waited long enough
            state.pending_intents = await backtester._process_pending_intents(
                pending_intents=state.pending_intents,
                portfolio=state.portfolio,
                market_state=market_state,
                config=config,
                data_quality_tracker=state.data_quality_tracker,
                strategy=strategy,
            )

            # Get strategy decision (warm-up + error-handler branch)
            decide_result = _invoke_strategy_decide(
                backtester=backtester,
                strategy=strategy,
                snapshot=snapshot,
                tick_tokens=tick_tokens,
                tick_count=state.tick_count,
                timestamp=timestamp,
                indicator_engine=state.indicator_engine,
                strategy_config=state.strategy_config,
                bt_logger=bt_logger,
            )

            # Extract intent from decide result
            intent = backtester._extract_intent(decide_result)

            # Queue intent for execution (with inclusion delay)
            if intent is not None and not backtester._is_hold_intent(intent):
                state.pending_intents.append((intent, timestamp, config.inclusion_delay_blocks))

            # Update positions via adapter if available
            backtester._update_positions_via_adapter(state.portfolio, market_state, timestamp)

            # Mark portfolio to market (uses adapter for valuation if available)
            state.portfolio.mark_to_market(market_state, timestamp, adapter=backtester._adapter)

            # Store the market state for use after simulation completes
            state.last_market_state = market_state  # noqa: F841 (used in US-062b)

        # Execute any remaining pending intents at end of simulation
        # (Use last market state for final execution)
        await _drain_pending_intents_at_end(
            backtester=backtester,
            strategy=strategy,
            config=config,
            bt_logger=bt_logger,
            state=state,
        )


def _invoke_strategy_decide(
    *,
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    snapshot: Any,
    tick_tokens: set[str],
    tick_count: int,
    timestamp: datetime,
    indicator_engine: BacktestIndicatorEngine,
    strategy_config: dict[str, Any],
    bt_logger: BacktestLogger,
) -> Any:
    """Call ``strategy.decide(snapshot)`` with warm-up / error-handler logic.

    Returns the ``decide_result`` (or ``None`` on non-fatal errors / warm-up).
    Raises ``RuntimeError`` if the error handler classifies the error as
    fatal (``should_stop`` True).
    """
    try:
        return strategy.decide(snapshot)
    except Exception as e:
        # Check if this is an indicator warm-up error (expected during initial ticks).
        # The indicator engine's is_warming_up() is the authoritative signal:
        # if the engine hasn't accumulated enough data points AND the strategy
        # raised a ValueError, it's almost certainly because indicators aren't
        # ready yet (e.g. "Cannot calculate RSI", "MACD data not available").
        # We only suppress ValueError to avoid masking real bugs (AttributeError,
        # KeyError, etc.).
        is_warmup = isinstance(e, ValueError) and any(
            indicator_engine.is_warming_up(t, strategy_config) for t in tick_tokens
        )
        if is_warmup:
            # Expected: not enough data points yet for indicators.
            # Log at debug (not warning) to avoid alarming users.
            bt_logger.debug(f"Tick {tick_count}: indicator warm-up ({e}) - holding")
        elif backtester._error_handler:
            # Use error handler for consistent classification
            result = backtester._error_handler.handle_error(
                e,
                context=f"strategy_decide:tick_{tick_count}:{timestamp.isoformat()}",
            )
            if result.should_stop:
                raise RuntimeError(f"Fatal error in strategy.decide() at tick {tick_count}: {e}") from e
            # Non-fatal: log warning and continue with hold
            bt_logger.warning(f"Strategy decide() error at tick {tick_count}: {e} - continuing with hold")
        else:
            bt_logger.warning(f"Strategy decide() raised exception at {timestamp}: {e}")
        return None


async def _drain_pending_intents_at_end(
    *,
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
    state: BacktestState,
) -> None:
    """Execute any remaining pending intents using ``last_market_state``.

    Mirrors the post-loop block that either drains queued intents against
    the final market state or warns that no valid market state is
    available. Mutates ``state.execution_delayed_at_end``.
    """
    if state.pending_intents and state.last_market_state is not None:
        bt_logger.warning(
            f"Executing {len(state.pending_intents)} pending intent(s) at simulation end "
            f"(delayed execution using last market state from {state.last_market_state.timestamp})"
        )
        for intent, decision_time, _ in state.pending_intents:
            try:
                trade_record = await backtester._execute_intent(
                    intent=intent,
                    portfolio=state.portfolio,
                    market_state=state.last_market_state,
                    timestamp=state.last_market_state.timestamp,
                    config=config,
                    delayed_at_end=True,
                    data_quality_tracker=state.data_quality_tracker,
                )
                state.execution_delayed_at_end += 1
                # Record successful execution in error handler
                if backtester._error_handler:
                    backtester._error_handler.record_success()
                bt_logger.debug(
                    f"Executed pending intent at simulation end "
                    f"(decided at {decision_time}): "
                    f"type={trade_record.intent_type.value}, "
                    f"amount=${trade_record.amount_usd:,.2f}"
                )
                # Notify strategy of successful execution
                if hasattr(strategy, "on_intent_executed"):
                    try:
                        callback_result = backtester._build_callback_result(intent, trade_record, success=True)
                        strategy.on_intent_executed(intent, True, callback_result)
                    except Exception as notify_err:
                        bt_logger.debug(f"on_intent_executed raised: {notify_err}")
            except Exception as e:
                # Notify strategy of execution failure
                if hasattr(strategy, "on_intent_executed"):
                    try:
                        callback_result = backtester._build_callback_result(intent, None, success=False, error=str(e))
                        strategy.on_intent_executed(intent, False, callback_result)
                    except Exception as notify_err:
                        bt_logger.debug(f"on_intent_executed (failure) raised: {notify_err}")
                # Use error handler for intent execution errors
                if backtester._error_handler:
                    result = backtester._error_handler.handle_error(
                        e,
                        context=f"execute_pending_intent:end:{type(intent).__name__}",
                    )
                    if result.should_stop:
                        bt_logger.error(f"Fatal error executing pending intent at simulation end: {e}")
                        raise
                    # Non-fatal: log warning and skip this intent
                    bt_logger.warning(f"Failed to execute pending intent at simulation end: {e} - skipping")
                else:
                    bt_logger.warning(f"Failed to execute pending intent at simulation end: {e}")
    elif state.pending_intents:
        bt_logger.warning(
            f"Cannot execute {len(state.pending_intents)} remaining pending intents: no valid market state available"
        )


# =============================================================================
# Error-path result
# =============================================================================


def build_error_result(
    *,
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    backtest_id: str,
    bt_logger: BacktestLogger,
    run_started_at: datetime,
    state: BacktestState,
    preflight_report: PreflightReport | None,
    preflight_passed: bool,
    error: Exception,
) -> BacktestResult:
    """Build the partial ``BacktestResult`` returned on simulation failure.

    Mirrors the ``except Exception as e`` branch exactly -- ``error`` is
    typed as :class:`Exception` (not :class:`BaseException`) because the
    original code only caught ``Exception``. The error handler
    ``handle_error`` call is performed here for consistent classification
    and tracking.
    """
    if backtester._error_handler:
        result = backtester._error_handler.handle_error(
            error,
            context="simulation_phase:main_loop",
        )
        bt_logger.error(
            f"Backtest failed with "
            f"{result.error_record.classification.error_type.value if result.error_record else 'unknown'} "
            f"error: {error}"
        )
    else:
        bt_logger.error(f"Backtest failed with error: {error}")

    run_ended_at = datetime.now(UTC)
    # On error, compliance is False and we add the error as a violation
    error_compliance_violations = state.compliance_violations + [f"Backtest failed with error: {error}"]
    error_fallback_usage = backtester._fallback_usage.copy() if backtester._fallback_usage else {}
    return BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id=strategy.deployment_id,
        start_time=config.start_time,
        end_time=config.end_time,
        metrics=BacktestMetrics(),
        initial_capital_usd=config.initial_capital_usd,
        final_capital_usd=config.initial_capital_usd,
        chain=config.chain,
        run_started_at=run_started_at,
        run_ended_at=run_ended_at,
        run_duration_seconds=(run_ended_at - run_started_at).total_seconds(),
        config=config.to_dict_with_metadata(data_provider_info=backtester._get_data_provider_info()),
        error=str(error),
        backtest_id=backtest_id,
        phase_timings=[t.to_dict() for t in bt_logger.phase_timings],
        config_hash=config.calculate_config_hash(),
        errors=backtester._error_handler.get_errors_as_dicts() if backtester._error_handler else [],
        data_source_capabilities=state.data_source_capabilities,
        data_source_warnings=state.data_source_warnings,
        data_quality=state.data_quality_tracker.to_data_quality_report(),
        institutional_compliance=False,
        compliance_violations=error_compliance_violations,
        fallback_usage=error_fallback_usage,
        preflight_report=preflight_report,
        preflight_passed=preflight_passed,
        gas_prices_used=backtester._gas_price_records or [],
        gas_price_summary=None,  # No trades on error
        parameter_sources=state.parameter_sources,
    )


# =============================================================================
# Finalization
# =============================================================================


def enforce_data_quality_gate(
    config: PnLBacktestConfig,
    bt_logger: BacktestLogger,
    state: BacktestState,
) -> None:
    """Enforce the post-simulation data coverage threshold.

    Appends to ``state.compliance_violations`` when coverage is below the
    configured minimum. Raises ``ValueError`` in institutional mode only;
    otherwise logs a warning.
    """
    coverage_ratio = state.data_quality_tracker.coverage_ratio
    if coverage_ratio < config.min_data_coverage:
        # Track as compliance violation regardless of institutional_mode
        state.compliance_violations.append(
            f"Data coverage below minimum threshold: {coverage_ratio:.2%} < {config.min_data_coverage:.2%} "
            f"({state.data_quality_tracker.successful_lookups}/{state.data_quality_tracker.total_price_lookups} "
            f"successful price lookups)"
        )

        if config.institutional_mode:
            error_msg = (
                f"Data quality gate failed in institutional mode: "
                f"coverage ratio {coverage_ratio:.2%} is below minimum threshold "
                f"{config.min_data_coverage:.2%}. "
                f"({state.data_quality_tracker.successful_lookups}/{state.data_quality_tracker.total_price_lookups} "
                f"successful price lookups)"
            )
            bt_logger.error(error_msg)
            raise ValueError(error_msg)
        else:
            # Not in institutional mode - log warning only
            bt_logger.warning(
                f"Data coverage below threshold: {coverage_ratio:.2%} < {config.min_data_coverage:.2%}. "
                f"({state.data_quality_tracker.successful_lookups}/{state.data_quality_tracker.total_price_lookups} "
                f"successful price lookups). "
                f"Enable institutional_mode=True to enforce data quality requirements."
            )
    elif config.institutional_mode:
        bt_logger.info(
            f"Data quality gate passed in institutional mode: "
            f"coverage ratio {coverage_ratio:.2%} >= {config.min_data_coverage:.2%}"
        )


def _append_fallback_compliance_violations(
    fallback_usage: dict[str, int],
    compliance_violations: list[str],
) -> None:
    """Add one compliance-violation entry per known fallback category."""
    if fallback_usage.get("hardcoded_price", 0) > 0:
        count = fallback_usage["hardcoded_price"]
        compliance_violations.append(
            f"Hardcoded price fallback used {count} time(s). "
            "Set strict_reproducibility=True for institutional-grade backtests."
        )
    if fallback_usage.get("default_gas_price", 0) > 0:
        count = fallback_usage["default_gas_price"]
        compliance_violations.append(f"Default gas price fallback used {count} time(s).")
    if fallback_usage.get("default_usd_amount", 0) > 0:
        count = fallback_usage["default_usd_amount"]
        compliance_violations.append(
            f"Default USD amount fallback used {count} time(s). "
            "Set strict_reproducibility=True for institutional-grade backtests."
        )


def finalize_backtest_result(
    *,
    backtester: PnLBacktester,
    strategy: BacktestableStrategy,
    config: PnLBacktestConfig,
    backtest_id: str,
    bt_logger: BacktestLogger,
    run_started_at: datetime,
    state: BacktestState,
    preflight_report: PreflightReport | None,
    preflight_passed: bool,
) -> BacktestResult:
    """Run metrics calculation + ``BacktestResult`` assembly on success.

    Mirrors the post-simulation success block:
    ``bt_logger.phase("metrics_calculation")``, final equity lookup,
    phase/error summary logging, fallback compliance violations, and
    ``BacktestResult`` construction (including
    ``data_coverage_metrics`` from the portfolio).
    """
    # Metrics calculation phase
    with bt_logger.phase("metrics_calculation"):
        metrics = backtester._calculate_metrics(state.portfolio, state.portfolio.trades, config)

        # Get final portfolio value
        final_value = (
            state.portfolio.equity_curve[-1].value_usd if state.portfolio.equity_curve else config.initial_capital_usd
        )

    run_ended_at = datetime.now(UTC)

    bt_logger.info(
        f"Backtest completed for {strategy.deployment_id}: "
        f"PnL=${metrics.net_pnl_usd:,.2f}, "
        f"Return={metrics.total_return_pct:.2f}%, "
        f"Sharpe={metrics.sharpe_ratio:.3f}"
    )

    # Log phase summary
    phase_summary = bt_logger.get_phase_summary()
    bt_logger.info(f"Phase timing summary - Total: {phase_summary['total_duration_seconds']:.2f}s")

    # Log error summary if any non-fatal errors occurred
    if backtester._error_handler and backtester._error_handler.error_count > 0:
        error_summary = backtester._error_handler.get_error_summary()
        bt_logger.info(
            f"Error summary: {error_summary['total_errors']} total "
            f"({error_summary['non_critical_errors']} non-critical, "
            f"{error_summary['recoverable_errors']} recoverable)"
        )

    # Get fallback usage and add compliance violations for any fallbacks used
    fallback_usage = backtester._fallback_usage.copy() if backtester._fallback_usage else {}
    _append_fallback_compliance_violations(fallback_usage, state.compliance_violations)

    # Determine institutional compliance status
    # Compliance is True only if there are no violations
    institutional_compliance = len(state.compliance_violations) == 0

    return BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id=strategy.deployment_id,
        start_time=config.start_time,
        end_time=config.end_time,
        metrics=metrics,
        trades=state.portfolio.trades,
        equity_curve=state.portfolio.equity_curve,
        initial_capital_usd=config.initial_capital_usd,
        final_capital_usd=final_value,
        chain=config.chain,
        run_started_at=run_started_at,
        run_ended_at=run_ended_at,
        run_duration_seconds=(run_ended_at - run_started_at).total_seconds(),
        config=config.to_dict_with_metadata(data_provider_info=backtester._get_data_provider_info()),
        backtest_id=backtest_id,
        phase_timings=[t.to_dict() for t in bt_logger.phase_timings],
        config_hash=config.calculate_config_hash(),
        errors=backtester._error_handler.get_errors_as_dicts() if backtester._error_handler else [],
        execution_delayed_at_end=state.execution_delayed_at_end,
        data_source_capabilities=state.data_source_capabilities,
        data_source_warnings=state.data_source_warnings,
        data_quality=state.data_quality_tracker.to_data_quality_report(),
        institutional_compliance=institutional_compliance,
        compliance_violations=state.compliance_violations,
        fallback_usage=fallback_usage,
        preflight_report=preflight_report,
        preflight_passed=preflight_passed,
        gas_prices_used=backtester._gas_price_records or [],
        gas_price_summary=backtester._create_gas_price_summary(state.portfolio.trades),
        parameter_sources=state.parameter_sources,
        data_coverage_metrics=state.portfolio.calculate_data_coverage_metrics(),
    )


# =============================================================================
# Token-flow helpers (Phase 6C.3)
# =============================================================================
#
# Each helper below owns a single ``IntentType`` branch previously inlined in
# ``PnLBacktester._calculate_token_flows``. They share a consistent shape:
#
#     * Accept the intent + scalar USD numbers + market state.
#     * Return a ``(tokens_in, tokens_out)`` tuple of
#       ``dict[str, Decimal]`` — empty dict for the side that does not flow.
#     * Preserve uppercase token normalization (via :func:`_normalize_token`),
#       ``price > 0`` guards, and the ``KeyError`` fallback that substitutes
#       the raw USD amount for tokens whose price is missing from
#       ``market_state`` — byte-for-byte identical to the pre-extraction body
#       (see characterization tests in
#       ``tests/unit/backtesting/pnl/test_engine_characterization.py``).
#
# LP helpers split the USD amount 50/50 and do NOT gate on ``price > 0``
# (matching the original behavior; a zero price would raise
# ``ZeroDivisionError`` exactly as it did before).
#
# Dispatch is performed by :func:`calculate_token_flows` through the
# :data:`_SIMPLE_FLOW_HANDLERS` mapping (plus an explicit SWAP branch, which
# is the only handler that consumes ``fee_usd`` / ``slippage_usd``).


def _normalize_token(token: Any) -> Any:
    """Uppercase a token symbol if it is a string; otherwise return as-is.

    Centralizes the ``if isinstance(token, str): token = token.upper()``
    idiom used by every flow branch. Non-string inputs pass through
    unchanged, matching the pre-extraction behavior (e.g. an object used
    as a token key by a custom intent would survive untouched).
    """
    if isinstance(token, str):
        return token.upper()
    return token


def _calculate_swap_flows(
    intent: Any,
    amount_usd: Decimal,
    fee_usd: Decimal,
    slippage_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """SWAP: one token leaves (``from_token``), another arrives (``to_token``).

    Outflow uses ``amount_usd`` at ``from_token`` price. Inflow uses
    ``amount_usd - fee_usd - slippage_usd`` at ``to_token`` price. Unknown
    prices fall back to the raw USD amount as a unit count.
    """
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}

    from_token = _normalize_token(getattr(intent, "from_token", "USDC"))
    to_token = _normalize_token(getattr(intent, "to_token", "WETH"))

    # Amount out is the trade amount
    amount_out = amount_usd
    try:
        from_price = market_state.get_price(from_token)
        if from_price > 0:
            tokens_out[from_token] = amount_out / from_price
    except KeyError:
        tokens_out[from_token] = amount_out  # Assume $1 price

    # Amount in is after fees and slippage
    amount_in_usd = amount_usd - fee_usd - slippage_usd
    try:
        to_price = market_state.get_price(to_token)
        if to_price > 0:
            tokens_in[to_token] = amount_in_usd / to_price
    except KeyError:
        tokens_in[to_token] = amount_in_usd  # Assume $1 price

    return tokens_in, tokens_out


def _resolve_single_token(intent: Any, default: str) -> Any:
    """Look up ``intent.token`` or ``intent.asset`` (first wins), normalized.

    Mirrors ``getattr(intent, "token", getattr(intent, "asset", default))``
    with uppercase normalization via :func:`_normalize_token` for string
    symbols.
    """
    return _normalize_token(getattr(intent, "token", getattr(intent, "asset", default)))


def _calculate_supply_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """SUPPLY: token leaves the wallet into the protocol."""
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}

    token = _resolve_single_token(intent, "WETH")

    try:
        price = market_state.get_price(token)
        if price > 0:
            tokens_out[token] = amount_usd / price
    except KeyError:
        tokens_out[token] = amount_usd

    return tokens_in, tokens_out


def _calculate_withdraw_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """WITHDRAW: token arrives back from the protocol."""
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}

    token = _resolve_single_token(intent, "WETH")

    try:
        price = market_state.get_price(token)
        if price > 0:
            tokens_in[token] = amount_usd / price
    except KeyError:
        tokens_in[token] = amount_usd

    return tokens_in, tokens_out


def _calculate_borrow_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """BORROW: borrowed token arrives in the wallet."""
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}

    token = _resolve_single_token(intent, "USDC")

    try:
        price = market_state.get_price(token)
        if price > 0:
            tokens_in[token] = amount_usd / price
    except KeyError:
        tokens_in[token] = amount_usd

    return tokens_in, tokens_out


def _calculate_repay_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """REPAY: token leaves the wallet to pay down debt."""
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}

    token = _resolve_single_token(intent, "USDC")

    try:
        price = market_state.get_price(token)
        if price > 0:
            tokens_out[token] = amount_usd / price
    except KeyError:
        tokens_out[token] = amount_usd

    return tokens_in, tokens_out


def _resolve_lp_tokens(intent: Any) -> tuple[Any, Any]:
    """Resolve ``(token0, token1)`` for LP intents, uppercased if strings."""
    token0 = _normalize_token(getattr(intent, "token0", getattr(intent, "token_a", "WETH")))
    token1 = _normalize_token(getattr(intent, "token1", getattr(intent, "token_b", "USDC")))
    return token0, token1


def _calculate_lp_open_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """LP_OPEN: both tokens leave the wallet, USD split 50/50."""
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}

    token0, token1 = _resolve_lp_tokens(intent)

    # Split the USD amount roughly 50/50
    half_amount = amount_usd / Decimal("2")

    try:
        price0 = market_state.get_price(token0)
        tokens_out[token0] = half_amount / price0
    except KeyError:
        tokens_out[token0] = half_amount

    try:
        price1 = market_state.get_price(token1)
        tokens_out[token1] = half_amount / price1
    except KeyError:
        tokens_out[token1] = half_amount

    return tokens_in, tokens_out


def _calculate_lp_close_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """LP_CLOSE: both tokens return to the wallet, USD split 50/50.

    Approximate tokens received (actual depends on impermanent loss).
    """
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}

    token0, token1 = _resolve_lp_tokens(intent)

    # Approximate tokens received (actual depends on IL)
    half_amount = amount_usd / Decimal("2")

    try:
        price0 = market_state.get_price(token0)
        tokens_in[token0] = half_amount / price0
    except KeyError:
        tokens_in[token0] = half_amount

    try:
        price1 = market_state.get_price(token1)
        tokens_in[token1] = half_amount / price1
    except KeyError:
        tokens_in[token1] = half_amount

    return tokens_in, tokens_out


def _resolve_vault_token(intent: Any) -> Any:
    """Resolve ``intent.deposit_token`` for vault intents, warning on fallback."""
    token = getattr(intent, "deposit_token", None)
    if not token:
        token = "USDC"
        logger.warning(
            "Vault intent missing deposit_token, defaulting to USDC — set deposit_token for accurate backtesting"
        )
    return _normalize_token(token)


def _calculate_vault_token_amount(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
) -> tuple[Any, Decimal]:
    """Resolve vault token and convert ``amount_usd`` to token units.

    Mirrors the shared preamble of ``_calculate_vault_deposit_flows`` and
    ``_calculate_vault_redeem_flows``: the only per-branch difference is
    whether the resulting amount lands in ``tokens_in`` or ``tokens_out``.
    """
    token = _resolve_vault_token(intent)

    try:
        price = market_state.get_price(token)
        amount = amount_usd / price if price > 0 else amount_usd
    except KeyError:
        amount = amount_usd

    return token, amount


def _calculate_vault_deposit_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """VAULT_DEPOSIT: deposit token flows out of the wallet into the vault."""
    token, amount = _calculate_vault_token_amount(intent, amount_usd, market_state)
    return {}, {token: amount}


def _calculate_vault_redeem_flows(
    intent: Any,
    amount_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """VAULT_REDEEM: deposit token flows back from the vault into the wallet."""
    token, amount = _calculate_vault_token_amount(intent, amount_usd, market_state)
    return {token: amount}, {}


# Dispatch table used by :func:`calculate_token_flows`. The SWAP handler has
# a distinct signature (it consumes fee/slippage); every other handler takes
# the same ``(intent, amount_usd, market_state)`` shape and is invoked
# through :data:`_SIMPLE_FLOW_HANDLERS`.
#
# Using a module-level mapping (rather than an ``if/elif`` chain) makes the
# sequencer constant-time in the number of intent types and makes it
# mechanically clear which intent types are covered by a dedicated helper
# versus falling through to the collateral-based no-flow default.
_SIMPLE_FLOW_HANDLERS: dict[IntentType, object] = {
    IntentType.SUPPLY: _calculate_supply_flows,
    IntentType.WITHDRAW: _calculate_withdraw_flows,
    IntentType.BORROW: _calculate_borrow_flows,
    IntentType.REPAY: _calculate_repay_flows,
    IntentType.LP_OPEN: _calculate_lp_open_flows,
    IntentType.LP_CLOSE: _calculate_lp_close_flows,
    IntentType.VAULT_DEPOSIT: _calculate_vault_deposit_flows,
    IntentType.VAULT_REDEEM: _calculate_vault_redeem_flows,
}


def calculate_token_flows(
    intent: Any,
    intent_type: IntentType,
    amount_usd: Decimal,
    fee_usd: Decimal,
    slippage_usd: Decimal,
    market_state: MarketState,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """Dispatch ``intent_type`` to the matching per-intent-type flow helper.

    Returns ``({}, {})`` for any intent type not covered by a dedicated
    helper (HOLD, PERP, and any future types handled via collateral rather
    than explicit token flows) — matching the pre-extraction fall-through
    semantics.
    """
    # SWAP is the only branch that consumes fee / slippage.
    if intent_type == IntentType.SWAP:
        return _calculate_swap_flows(intent, amount_usd, fee_usd, slippage_usd, market_state)

    handler = _SIMPLE_FLOW_HANDLERS.get(intent_type)
    if handler is not None:
        return handler(intent, amount_usd, market_state)  # type: ignore[operator]

    # For PERP, HOLD, and other types, token flows are handled via collateral
    return {}, {}
