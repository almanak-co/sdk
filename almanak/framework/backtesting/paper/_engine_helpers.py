"""Phase helpers for ``PaperTrader.run``.

``PaperTrader.run`` orchestrates a multi-phase paper-trade session:

* per-run state reset + backtest-id / error-handler creation
* effective-duration resolution (arg -> config -> 1h default)
* session-started / session-ended event emission
* setup (fork -> orchestrator -> portfolio valuer -> market snapshot -> initial
  equity point)
* main iteration loop (advance -> execute_tick -> reconciler -> sleep ->
  refresh, with duration and tick-limit gates)
* error classification (CancelledError + generic Exception through
  BacktestErrorHandler)
* final-value fallback (rich -> last equity point -> simple)
* result assembly (TradeRecord mapping, compliance violations, BacktestResult)

These helpers exist so the main ``run`` entry point stays at CC <= 12 and each
phase is unit-testable in isolation. See
``blueprints/04-strategy-layer.md`` for the strategy / PaperTrader contract
and ``tests/unit/backtesting/paper/test_paper_trader_characterization.py``
for the byte-for-byte pinning tests.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
    IntentType,
    TradeRecord,
)
from almanak.framework.backtesting.paper.config import (
    ForkLifecycle,
    PaperTraderConfig,
)
from almanak.framework.backtesting.paper.models import PaperTrade
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestErrorConfig,
    BacktestErrorHandler,
)

if TYPE_CHECKING:
    from almanak.framework.backtesting.paper.engine import (
        PaperTradeableStrategy,
        PaperTrader,
    )

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-run state reset + duration resolution
# ---------------------------------------------------------------------------


def reset_run_state(trader: PaperTrader, strategy: PaperTradeableStrategy) -> datetime:
    """Reset all per-run mutable state on ``trader``.

    Mirrors the state-reset block at the top of the original ``run`` (the
    part before the try-block). Returns the ``run_started_at`` timestamp so
    callers can pass it down to the main loop and result-assembly helpers.
    """
    run_started_at = datetime.now(UTC)
    trader._session_start = run_started_at
    trader._running = True
    trader._current_strategy = strategy
    trader._trades = []
    trader._errors = []
    trader._equity_curve = []
    trader._tick_count = 0
    trader._reconciler_discrepancies = []
    trader._last_execution_result = None
    # Health telemetry counters (VIB-1957)
    trader._ticks_with_fork = 0
    trader._ticks_with_indicators = 0
    trader._ticks_with_action = 0
    trader._last_successful_decision_at = None
    trader._last_trade_at = None
    # Unique backtest_id for correlation.
    trader._backtest_id = str(uuid.uuid4())
    # Consistent error handler.
    trader._error_handler = BacktestErrorHandler(BacktestErrorConfig())
    return run_started_at


def resolve_effective_duration(
    config: PaperTraderConfig,
    duration_seconds: float | None,
) -> float:
    """Pick the effective session duration.

    Precedence: explicit argument -> config.max_duration_seconds -> 3600s.
    Mirrors the block at lines 980-987 of the original ``run``.
    """
    if duration_seconds is not None:
        return duration_seconds
    return float(config.max_duration_seconds or 3600.0)


# ---------------------------------------------------------------------------
# Setup phase
# ---------------------------------------------------------------------------


async def setup_session(trader: PaperTrader) -> None:
    """Run the setup phase before the main loop.

    Order is load-bearing: fork -> orchestrator -> portfolio valuer ->
    seeded market snapshot -> initial equity point. The initial equity point
    ensures the first entry of the equity curve is recorded even if the loop
    exits before any tick (e.g., max_ticks=0 or duration=0).
    """
    await trader._initialize_fork()
    await trader._initialize_orchestrator()
    trader._init_portfolio_valuer()
    await trader._seed_initial_market_snapshot()
    await trader._record_equity_point()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def run_main_loop(
    trader: PaperTrader,
    strategy: PaperTradeableStrategy,
    effective_duration: float,
    max_ticks: int | None,
    run_started_at: datetime,
) -> None:
    """Execute the per-bar iteration loop.

    Per-bar order (preserved byte-for-byte for equity-curve determinism):

    1. check time-limit (``end_time = run_started_at + effective_duration``)
    2. check tick-limit
    3. advance persistent fork (only if fork_lifecycle == PERSISTENT and
       tick_count > 0)
    4. execute_tick(strategy) + tick_count += 1
    5. run position reconciler (only if reconciler enabled AND persistent)
    6. asyncio.sleep(tick_interval_seconds)
    7. refresh fork (if _should_refresh_fork() is True)
    """
    from datetime import timedelta

    end_time = run_started_at + timedelta(seconds=effective_duration)

    while trader._running:
        now = datetime.now(UTC)
        if now >= end_time:
            logger.info(f"[{trader._backtest_id}] Duration limit reached, stopping")
            break

        if max_ticks is not None and trader._tick_count >= max_ticks:
            logger.info(f"[{trader._backtest_id}] Max ticks ({max_ticks}) reached, stopping")
            break

        if trader.config.fork_lifecycle == ForkLifecycle.PERSISTENT and trader._tick_count > 0:
            await trader._advance_persistent_fork()

        await trader._execute_tick(strategy)
        trader._tick_count += 1

        if trader.config.position_reconciler_enabled and trader.config.fork_lifecycle == ForkLifecycle.PERSISTENT:
            await trader._run_position_reconciler()

        await asyncio.sleep(trader.config.tick_interval_seconds)

        if await trader._should_refresh_fork():
            await trader._refresh_fork()


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


def classify_run_exception(trader: PaperTrader, exc: BaseException) -> str:
    """Convert a loop-phase exception into the user-visible error string.

    * ``asyncio.CancelledError`` -> "Session cancelled"
    * Any other ``Exception`` -> ``str(exc)``. The registered
      ``BacktestErrorHandler`` decides whether the error is fatal; logging
      level is picked accordingly but the result string is always ``str(exc)``.
    """
    if isinstance(exc, asyncio.CancelledError):
        logger.info(f"[{trader._backtest_id}] Paper trading session cancelled")
        return "Session cancelled"

    # After CancelledError is handled above, the remaining branch is logically
    # a concrete Exception (callers catch ``(CancelledError, Exception)``).
    assert isinstance(exc, Exception)
    if trader._error_handler is not None:
        handler_result = trader._error_handler.handle_error(exc, context="paper_trading_session")
        if handler_result.should_stop:
            logger.error(f"[{trader._backtest_id}] Fatal error in paper trading session: {exc}")
        else:
            logger.warning(f"[{trader._backtest_id}] Non-critical error in paper trading session: {exc}")
    else:
        logger.exception(f"[{trader._backtest_id}] Paper trading session failed: {exc}")
    return str(exc)


# ---------------------------------------------------------------------------
# Final-value fallback chain
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FinalValuation:
    """Result of the pre-cleanup final-value snapshot."""

    value_usd: Decimal
    source: str  # "portfolio_valuer" | "simple" | equity-point source label


async def capture_final_portfolio_value(trader: PaperTrader) -> FinalValuation:
    """Cache the final portfolio value BEFORE cleanup clears valuer state.

    Fallback order:
    1. ``_value_portfolio_rich()`` (LP + lending aware) -> label
       ``"portfolio_valuer"``
    2. last equity-curve point -> label from that point's ``valuation_source``
    3. ``_calculate_portfolio_value()`` (simple token * price) -> label
       ``"simple"``
    """
    # Refresh price cache for portfolio tokens before PnL calc (VIB-2550).
    try:
        await trader._get_portfolio_prices()
    except Exception:
        logger.debug(
            "[%s] Failed to refresh portfolio prices before PnL calc",
            trader._backtest_id,
            exc_info=True,
        )

    rich = trader._value_portfolio_rich()
    if rich is not None:
        return FinalValuation(value_usd=rich[0], source="portfolio_valuer")

    if trader._equity_curve:
        last = trader._equity_curve[-1]
        return FinalValuation(value_usd=last.value_usd, source=last.valuation_source)

    return FinalValuation(
        value_usd=trader._calculate_portfolio_value(),
        source="simple",
    )


# ---------------------------------------------------------------------------
# Result assembly
# ---------------------------------------------------------------------------


def build_trade_records(trades: list[PaperTrade]) -> list[TradeRecord]:
    """Map ``PaperTrade`` instances to ``TradeRecord`` for ``BacktestResult``.

    Identity-preserving:

    * ``tokens = tokens_in.keys() + tokens_out.keys()`` (in-order)
    * ``pnl_usd = net_token_flow_usd`` (pre-gas; ``TradeRecord.net_pnl_usd``
      is what subtracts gas itself)
    * unknown / empty intent_type -> ``IntentType.UNKNOWN``
    * ``executed_price / fee_usd / slippage_usd`` are zeroed by design — per
      the engine comment they are "embedded in execution"
    * ``success=True`` — every entry in ``_trades`` is a successful execution
    """
    records: list[TradeRecord] = []
    for trade in trades:
        tokens = list(trade.tokens_in.keys()) + list(trade.tokens_out.keys())
        records.append(
            TradeRecord(
                timestamp=trade.timestamp,
                intent_type=(IntentType(trade.intent_type) if trade.intent_type else IntentType.UNKNOWN),
                executed_price=Decimal("0"),
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=trade.gas_cost_usd,
                pnl_usd=trade.net_token_flow_usd,
                success=True,
                amount_usd=Decimal(trade.metadata.get("amount_usd", "0")),
                protocol=trade.protocol,
                tokens=tokens,
                tx_hash=trade.tx_hash,
                metadata=trade.metadata,
            )
        )
    return records


def collect_compliance_violations(
    fallback_usage: dict[str, int],
) -> tuple[list[str], bool]:
    """Translate fallback-usage counters into compliance violations.

    Returns ``(violations, institutional_compliant)`` where
    ``institutional_compliant`` is ``True`` only when ``violations`` is empty.

    Violation phrasing matches the original engine wording exactly — changing
    it would break downstream operator dashboards that grep these strings.
    """
    violations: list[str] = []

    if fallback_usage.get("hardcoded_price", 0) > 0:
        count = fallback_usage["hardcoded_price"]
        violations.append(
            f"Hardcoded price fallback used {count} time(s). "
            "Set strict_price_mode=True for institutional-grade backtests."
        )
    if fallback_usage.get("default_gas_price", 0) > 0:
        count = fallback_usage["default_gas_price"]
        violations.append(f"Default gas price fallback used {count} time(s).")
    if fallback_usage.get("default_usd_amount", 0) > 0:
        count = fallback_usage["default_usd_amount"]
        violations.append(f"Default USD amount fallback used {count} time(s).")
    if fallback_usage.get("zero_output_placeholder", 0) > 0:
        count = fallback_usage["zero_output_placeholder"]
        violations.append(
            f"Zero output placeholder used {count} time(s) due to missing receipt data. "
            "PnL calculations may be inaccurate."
        )

    return violations, len(violations) == 0


def assemble_backtest_result(
    *,
    trader: PaperTrader,
    strategy_id: str,
    run_started_at: datetime,
    run_ended_at: datetime,
    metrics: BacktestMetrics,
    trade_records: list[TradeRecord],
    equity_curve: list[EquityPoint],
    final_value: Decimal,
    error: str | None,
    initial_capital: Decimal,
    config_dict: dict[str, Any],
    fallback_usage: dict[str, int],
    compliance_violations: list[str],
    institutional_compliance: bool,
) -> BacktestResult:
    """Build the final ``BacktestResult`` — pure data assembly, no I/O."""
    return BacktestResult(
        engine=BacktestEngine.PAPER,
        strategy_id=strategy_id,
        start_time=run_started_at,
        end_time=run_ended_at,
        metrics=metrics,
        trades=trade_records,
        equity_curve=equity_curve,
        initial_capital_usd=initial_capital,
        final_capital_usd=final_value,
        chain=trader.config.chain,
        run_started_at=run_started_at,
        run_ended_at=run_ended_at,
        run_duration_seconds=(run_ended_at - run_started_at).total_seconds(),
        config=config_dict,
        error=error,
        backtest_id=trader._backtest_id,
        fallback_usage=fallback_usage,
        institutional_compliance=institutional_compliance,
        compliance_violations=compliance_violations,
    )
