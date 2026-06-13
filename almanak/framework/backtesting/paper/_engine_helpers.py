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
``docs/internal/blueprints/04-strategy-layer.md`` for the strategy / PaperTrader contract
and ``tests/unit/backtesting/paper/test_paper_trader_characterization.py``
for the byte-for-byte pinning tests.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import uuid
from collections.abc import Awaitable, Callable
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
from almanak.framework.backtesting.paper.models import (
    PaperTrade,
    PaperTradeError,
    PaperTradeErrorType,
)
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestErrorConfig,
    BacktestErrorHandler,
)

if TYPE_CHECKING:
    from almanak.framework.backtesting.paper.engine import (
        PaperTradeableStrategy,
        PaperTrader,
    )
    from almanak.framework.execution.orchestrator import ExecutionResult
    from almanak.framework.models.reproduction_bundle import TransactionReceipt

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
    trader._reconciler_checks = 0
    trader._divergence_records = {}
    # Drop the reconciler so the next run re-baselines against its own fork
    # instead of the previous session's positions (VIB-2634).
    trader._position_reconciler = None
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
# VIB-3164 — atomic token-flow decimal resolution (Empty != Zero)
# ---------------------------------------------------------------------------


async def resolve_token_flows(
    flows_in: dict[str, int],
    flows_out: dict[str, int],
    *,
    backtest_id: str | None,
    flow_kind: str,
    decimals_resolver: Callable[[str], Awaitable[int | None]],
    symbol_resolver: Callable[[str], Awaitable[str]],
) -> tuple[dict[str, Decimal], dict[str, Decimal]] | None:
    """Atomically resolve address-keyed raw token flows into symbol-keyed Decimals.

    Shared core of ``PaperTrader._extract_token_flows`` (receipt path) and
    ``PaperTrader._compute_balance_deltas`` (balance-snapshot path). Both callers
    iterate token legs, resolve each leg's decimals, and — per the Empty != Zero
    discipline (blueprint 27 §10.10) — refuse to half-record a trade: if ANY leg's
    decimals are unresolved (``None``, i.e. unmeasured), the WHOLE flow is aborted
    so ``record_trade`` never applies a one-sided swap and corrupts balances/PnL.

    Each leg's decimals are resolved via ``decimals_resolver`` (returns ``None``
    when the token's decimals cannot be measured — never a guessed 18). Each leg's
    output key is mapped via ``symbol_resolver`` so the resulting dicts are keyed by
    human-readable portfolio symbols (US-065c).

    Args:
        flows_in: Address-keyed inflow legs (address -> raw smallest-unit amount).
        flows_out: Address-keyed outflow legs (address -> raw smallest-unit amount).
        backtest_id: Correlation id for the warning log line.
        flow_kind: Human label for the abort warning ("receipt-based" / "balance-delta").
        decimals_resolver: ``addr -> decimals | None`` (None == unmeasured leg).
        symbol_resolver: ``addr -> symbol`` for output keys.

    Returns:
        ``(tokens_in, tokens_out)`` symbol-keyed Decimal dicts when EVERY leg
        resolves, or ``None`` when any leg is unmeasured (caller records nothing
        and falls back to its next estimation source).
    """
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}
    unresolved_tokens: list[str] = []

    for address, raw_amount in flows_in.items():
        decimals = await decimals_resolver(address)
        if decimals is None:
            unresolved_tokens.append(address)
            continue
        symbol = await symbol_resolver(address)
        tokens_in[symbol] = Decimal(str(raw_amount)) / Decimal(10**decimals)

    for address, raw_amount in flows_out.items():
        decimals = await decimals_resolver(address)
        if decimals is None:
            unresolved_tokens.append(address)
            continue
        symbol = await symbol_resolver(address)
        tokens_out[symbol] = Decimal(str(raw_amount)) / Decimal(10**decimals)

    if unresolved_tokens:
        logger.warning(
            f"[{backtest_id}] Skipping ENTIRE {flow_kind} trade flow: "
            f"token decimals unresolved for {unresolved_tokens} "
            "(refusing to assume 18). Recording a partial one-sided flow "
            "would corrupt balances/PnL; falling back to the next estimation source."
        )
        return None

    return tokens_in, tokens_out


async def discover_intent_token_balances(
    trader: PaperTrader,
    intent: Any,
    before: dict[str, int],
    after: dict[str, int],
    all_symbols: set[str],
) -> None:
    """Augment balance snapshots with intent tokens not yet tracked.

    Mirrors the intent-token discovery block of ``_compute_balance_deltas``:
    for each token-bearing intent attribute, if the (uppercased) symbol is not
    already tracked and is not native ETH, query its post-execution balance and
    seed ``before``/``after``/``all_symbols`` so the delta loop can measure it.
    Mutates ``before``, ``after``, and ``all_symbols`` in place; balance-query
    failures are swallowed exactly as before (best-effort discovery).
    """
    for attr in ("from_token", "to_token", "token", "asset", "token0", "token1", "token_a", "token_b"):
        token_val = getattr(intent, attr, None)
        if not token_val:
            continue
        sym = str(token_val).upper()
        if sym in all_symbols or sym == "ETH":
            continue
        token_address = trader._resolve_token_address(sym)
        if not token_address:
            continue
        try:
            after[sym] = await trader.fork_manager._get_token_balance(
                token_address,
                trader._orchestrator.signer.address if trader._orchestrator else "",
            )
            before.setdefault(sym, 0)
            all_symbols.add(sym)
        except Exception:
            pass


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

    ``effective_duration == float('inf')`` means "run indefinitely" (the
    contract ``PaperTrader.start()`` relies on): in that case ``end_time`` is
    ``None`` and the time-limit gate is skipped. Using ``timedelta`` with
    ``inf`` raises ``OverflowError`` (see issue #1839).
    """
    from datetime import timedelta

    end_time: datetime | None = None
    if effective_duration != float("inf"):
        end_time = run_started_at + timedelta(seconds=effective_duration)

    while trader._running:
        now = datetime.now(UTC)
        if end_time is not None and now >= end_time:
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
        elif trader.config.position_reconciler_enabled:
            # VIB-2634: reconciling against a fork that resets to latest every
            # tick is meaningless (no persistent state to drift) — skip.
            logger.debug(
                "[%s] Skipping PositionReconciler: fork_lifecycle=%s resets state every tick",
                trader._backtest_id,
                trader.config.fork_lifecycle.value,
            )

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
    deployment_id: str,
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
        deployment_id=deployment_id,
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


# ---------------------------------------------------------------------------
# run_loop helpers (CC reduction for PaperTrader.run_loop)
# ---------------------------------------------------------------------------


def init_run_loop_state(trader: PaperTrader, strategy: PaperTradeableStrategy) -> datetime:
    """Reset trader state for a ``run_loop`` session and return ``session_start``."""
    session_start = datetime.now(UTC)
    trader._session_start = session_start
    trader._running = True
    trader._current_strategy = strategy
    trader._trades = []
    trader._errors = []
    trader._equity_curve = []
    trader._reconciler_discrepancies = []
    trader._reconciler_checks = 0
    trader._divergence_records = {}
    # Drop the reconciler so the next run re-baselines against its own fork
    # instead of the previous session's positions (VIB-2634).
    trader._position_reconciler = None
    trader._tick_count = 0
    trader._last_execution_result = None
    trader._ticks_with_fork = 0
    trader._ticks_with_indicators = 0
    trader._ticks_with_action = 0
    trader._last_successful_decision_at = None
    trader._last_trade_at = None
    trader._backtest_id = str(uuid.uuid4())
    trader._error_handler = BacktestErrorHandler(BacktestErrorConfig())
    return session_start


async def run_loop_setup(trader: PaperTrader) -> None:
    """Run the pre-loop setup phase: fork, orchestrator, valuer, snapshot, equity."""
    await trader._initialize_fork()
    await trader._initialize_orchestrator()
    trader._init_portfolio_valuer()
    await trader._seed_initial_market_snapshot()
    await trader._record_equity_point()


async def run_loop_iterate(trader: PaperTrader, effective_max_ticks: int | None) -> None:
    """Execute the main tick loop until ``_running`` is cleared or limit hit.

    ``trader.tick()`` is the canonical owner of ``_tick_count`` — it increments
    once per call. Do not increment here as well; doing so would budget
    ``max_ticks`` against 2x the actual tick count.
    """
    while trader._running:
        if effective_max_ticks is not None and trader._tick_count >= effective_max_ticks:
            logger.info(f"[{trader._backtest_id}] Max ticks ({effective_max_ticks}) reached, stopping")
            break
        await trader.tick()
        if trader._running and (effective_max_ticks is None or trader._tick_count < effective_max_ticks):
            await asyncio.sleep(trader.config.tick_interval_seconds)


def handle_run_loop_exception(trader: PaperTrader, exc: BaseException) -> None:
    """Log and record a loop-phase exception. ``CancelledError`` logs only."""
    if isinstance(exc, asyncio.CancelledError):
        logger.info(f"[{trader._backtest_id}] Paper trading loop cancelled")
        return

    assert isinstance(exc, Exception)
    if trader._error_handler is not None:
        handler_result = trader._error_handler.handle_error(exc, context="paper_trading_loop")
        if handler_result.should_stop:
            logger.error(f"[{trader._backtest_id}] Fatal error in paper trading loop: {exc}")
        else:
            logger.warning(f"[{trader._backtest_id}] Non-critical error in paper trading loop: {exc}")
    else:
        logger.exception(f"[{trader._backtest_id}] Paper trading loop failed: {exc}")

    fork_manager = getattr(trader, "fork_manager", None)
    block_number = (
        fork_manager.current_block if fork_manager is not None and getattr(fork_manager, "is_running", False) else None
    )
    trader._errors.append(
        PaperTradeError(
            timestamp=datetime.now(UTC),
            intent={},
            error_type=PaperTradeErrorType.INTERNAL_ERROR,
            error_message=f"Loop error: {exc}",
            block_number=block_number,
            metadata={"exception_type": type(exc).__name__},
        )
    )


@dataclass(frozen=True, slots=True)
class CachedTeardownValuation:
    """Snapshot of final portfolio value/PnL captured before cleanup wipes state."""

    final_value_usd: Decimal
    valuation_source: str
    pnl_usd: Decimal | None


async def cache_run_loop_teardown_valuation(trader: PaperTrader) -> CachedTeardownValuation:
    """Refresh prices and snapshot final value/PnL before ``_cleanup`` clears state.

    Fallback chain: ``_value_portfolio_rich`` -> last equity point -> simple value.
    Mirrors the ``finally`` block of ``run_loop`` byte-for-byte.
    """
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
        final_value = rich[0]
        valuation_source = "portfolio_valuer"
    elif trader._equity_curve:
        final_value = trader._equity_curve[-1].value_usd
        valuation_source = trader._equity_curve[-1].valuation_source
    else:
        final_value = trader._calculate_portfolio_value()
        valuation_source = "simple"

    pnl: Decimal | None = None
    try:
        pnl = final_value - trader._calculate_initial_capital()
    except Exception:
        logger.debug(
            "[%s] Failed to compute cached PnL in run_loop teardown",
            trader._backtest_id,
            exc_info=True,
        )

    return CachedTeardownValuation(
        final_value_usd=final_value,
        valuation_source=valuation_source,
        pnl_usd=pnl,
    )


# ---------------------------------------------------------------------------
# Oracle divergence helpers (CC reduction for PaperTrader._check_oracle_divergence)
# ---------------------------------------------------------------------------


@functools.cache
def _chainlink_divergence_chains() -> frozenset[str]:
    """Chains where the on-fork Chainlink-vs-TWAP divergence check runs.

    The check compares a Chainlink feed against a DEX TWAP pool, so
    membership is exactly "chains that have BOTH" — the same intersection
    the engine's ``_PRICE_SOURCE_CHAINS`` uses (VIB-4851 CS-7; replaces a
    hand-kept 6-chain identity map). Deferred imports: dex_twap and
    paper.engine have a known order-dependent import cycle.
    """
    from almanak.core.chains._helpers import chainlink_usd_feeds_map
    from almanak.framework.data.price.dex_twap import UNISWAP_V3_POOLS

    return frozenset(chainlink_usd_feeds_map()) & frozenset(UNISWAP_V3_POOLS)


# Per-token info logging threshold: any individual divergence above this gets
# a per-token info line (separate from the hard threshold gate which raises).
_DIVERGENCE_INFO_LOG_THRESHOLD = Decimal("0.02")


def resolve_chainlink_divergence_chain(chain: str) -> str | None:
    """Return the Chainlink chain key for ``chain``, or ``None`` if unsupported."""
    return chain if chain in _chainlink_divergence_chains() else None


async def compute_max_oracle_divergence(
    chainlink_provider: Any,
    cached_prices: dict[str, Decimal],
    backtest_id: str | None,
) -> tuple[Decimal, str]:
    """Walk cached live prices, compare to fork-bound Chainlink, return ``(max, token)``.

    Per-token failures are logged at debug and skipped; the loop never raises.
    """
    max_divergence = Decimal("0")
    worst_token = ""
    for token, live_price in cached_prices.items():
        if live_price <= 0:
            continue
        try:
            fork_price = await chainlink_provider.get_price(token, timestamp=None)
        except Exception as e:
            logger.debug(f"[{backtest_id}] Failed to get on-fork price for {token} from Chainlink: {e}")
            continue
        if not fork_price or fork_price <= 0:
            continue
        divergence = abs(live_price - fork_price) / live_price
        if divergence > max_divergence:
            max_divergence = divergence
            worst_token = token
        if divergence > _DIVERGENCE_INFO_LOG_THRESHOLD:
            logger.info(
                f"[{backtest_id}] Oracle divergence for {token}: "
                f"live=${live_price:.2f} vs fork=${fork_price:.2f} "
                f"({divergence * 100:.1f}%)"
            )
    return max_divergence, worst_token


def build_divergence_error_message(
    worst_token: str,
    max_divergence: Decimal,
    threshold: Decimal,
) -> str:
    """Build the ``RuntimeError`` message raised when divergence exceeds threshold."""
    return (
        f"Oracle divergence exceeds threshold for {worst_token}: "
        f"{max_divergence * 100:.1f}% > {threshold * 100:.0f}%. "
        f"The persistent fork's on-chain prices have drifted too far from reality. "
        f"Paper trading results would be unreliable. "
        f"Increase oracle_divergence_threshold or reduce session duration."
    )


# ---------------------------------------------------------------------------
# Token flow helpers (CC reduction for PaperTrader._extract_token_flows)
# ---------------------------------------------------------------------------


def intent_flows_for_swap(
    intent: Any,
    expected_amount_out: Decimal | None,
    track_fallback: Any,
    backtest_id: str | None,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """Estimate token flows for a SWAP intent without a receipt."""
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}
    from_token = getattr(intent, "from_token", None)
    to_token = getattr(intent, "to_token", None)
    intent_amount = getattr(intent, "amount", None) or getattr(intent, "amount_in", None)

    if from_token and intent_amount:
        tokens_out[str(from_token).upper()] = Decimal(str(intent_amount))
    if to_token:
        if expected_amount_out is not None:
            tokens_in[str(to_token).upper()] = expected_amount_out
        else:
            logger.warning(
                f"[{backtest_id}] Cannot determine swap output amount for {to_token} "
                "without receipt. Using zero placeholder - this may affect PnL accuracy."
            )
            track_fallback("zero_output_placeholder")
            tokens_in[str(to_token).upper()] = Decimal("0")
    return tokens_in, tokens_out


def intent_flows_for_lending(
    intent: Any,
    direction: str,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """Estimate token flows for SUPPLY/REPAY (``direction='out'``) or WITHDRAW/BORROW (``'in'``)."""
    tokens_in: dict[str, Decimal] = {}
    tokens_out: dict[str, Decimal] = {}
    token = getattr(intent, "token", None) or getattr(intent, "asset", None)
    intent_amount = getattr(intent, "amount", None)
    if token and intent_amount:
        bucket = tokens_out if direction == "out" else tokens_in
        bucket[str(token).upper()] = Decimal(str(intent_amount))
    return tokens_in, tokens_out


def intent_flows_for_lp_open(intent: Any) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """Estimate token outflows for an LP_OPEN intent without a receipt."""
    tokens_out: dict[str, Decimal] = {}
    token0 = getattr(intent, "token0", None) or getattr(intent, "token_a", None)
    token1 = getattr(intent, "token1", None) or getattr(intent, "token_b", None)
    amount0 = getattr(intent, "amount0", None)
    amount1 = getattr(intent, "amount1", None)
    if token0 and amount0:
        tokens_out[str(token0).upper()] = Decimal(str(amount0))
    if token1 and amount1:
        tokens_out[str(token1).upper()] = Decimal(str(amount1))
    return {}, tokens_out


def intent_flows_for_lp_close(
    intent: Any,
    track_fallback: Any,
    backtest_id: str | None,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """Build placeholder token inflows for an LP_CLOSE intent without a receipt."""
    tokens_in: dict[str, Decimal] = {}
    token0 = getattr(intent, "token0", None) or getattr(intent, "token_a", None)
    token1 = getattr(intent, "token1", None) or getattr(intent, "token_b", None)
    if token0 or token1:
        logger.warning(
            f"[{backtest_id}] Cannot determine LP close output amounts "
            "without receipt. Using zero placeholder - this may affect PnL accuracy."
        )
        track_fallback("zero_output_placeholder")
    if token0:
        tokens_in[str(token0).upper()] = Decimal("0")
    if token1:
        tokens_in[str(token1).upper()] = Decimal("0")
    return tokens_in, {}


def intent_fallback_token_flows(
    intent_type: IntentType,
    intent: Any,
    expected_amount_out: Decimal | None,
    track_fallback: Any,
    backtest_id: str | None,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """Dispatch to the per-intent-type intent-only fallback estimator."""
    if intent_type == IntentType.SWAP:
        return intent_flows_for_swap(intent, expected_amount_out, track_fallback, backtest_id)
    if intent_type in (IntentType.SUPPLY, IntentType.REPAY):
        return intent_flows_for_lending(intent, direction="out")
    if intent_type in (IntentType.WITHDRAW, IntentType.BORROW):
        return intent_flows_for_lending(intent, direction="in")
    if intent_type == IntentType.LP_OPEN:
        return intent_flows_for_lp_open(intent)
    if intent_type == IntentType.LP_CLOSE:
        return intent_flows_for_lp_close(intent, track_fallback, backtest_id)
    return {}, {}


# ---------------------------------------------------------------------------
# Intent execution helpers (CC reduction for PaperTrader._execute_intent)
# ---------------------------------------------------------------------------


def make_compile_failure_error(
    *,
    timestamp: datetime,
    intent_dict: dict[str, Any],
    block_number: int | None,
    intent_type_value: str,
) -> PaperTradeError:
    """Build the PaperTradeError raised when intent compilation returns no bundle."""
    return PaperTradeError(
        timestamp=timestamp,
        intent=intent_dict,
        error_type=PaperTradeErrorType.INTENT_INVALID,
        error_message="Failed to compile intent to ActionBundle",
        block_number=block_number,
        metadata={"intent_type": intent_type_value},
    )


def classify_execution_error_type(result: ExecutionResult) -> PaperTradeErrorType:
    """Map an ``ExecutionResult.error_phase`` to a ``PaperTradeErrorType``."""
    if not result.error_phase:
        return PaperTradeErrorType.INTERNAL_ERROR
    phase_name = result.error_phase.value.upper()
    if "SIMULATION" in phase_name:
        return PaperTradeErrorType.SIMULATION_FAILED
    if "SUBMIT" in phase_name:
        return PaperTradeErrorType.RPC_ERROR
    return PaperTradeErrorType.INTERNAL_ERROR


def make_execution_failure_error(
    *,
    timestamp: datetime,
    intent_dict: dict[str, Any],
    result: ExecutionResult,
    block_number: int | None,
    intent_type_value: str,
) -> PaperTradeError:
    """Build the PaperTradeError for a non-success ExecutionResult."""
    return PaperTradeError(
        timestamp=timestamp,
        intent=intent_dict,
        error_type=classify_execution_error_type(result),
        error_message=result.error or "Unknown error",
        block_number=block_number,
        metadata={
            "phase": result.phase.value,
            "intent_type": intent_type_value,
        },
    )


def log_intent_execution_exception(
    trader: PaperTrader,
    exc: Exception,
    intent_type_value: str,
) -> None:
    """Log an intent-execution exception via the registered error handler."""
    if trader._error_handler:
        handler_result = trader._error_handler.handle_error(exc, context=f"intent_execution:{intent_type_value}")
        if handler_result.should_stop:
            logger.error(f"[{trader._backtest_id}] Fatal error executing intent: {exc}")
        elif handler_result.should_retry:
            logger.warning(f"[{trader._backtest_id}] Recoverable error executing intent (retry possible): {exc}")
        else:
            logger.warning(f"[{trader._backtest_id}] Non-critical error executing intent: {exc}")
    else:
        logger.exception(f"[{trader._backtest_id}] Error executing intent: {exc}")


def make_intent_exception_error(
    *,
    timestamp: datetime,
    intent_dict: dict[str, Any],
    exc: Exception,
    block_number: int | None,
    intent_type_value: str,
) -> PaperTradeError:
    """Build the PaperTradeError for an exception raised during intent execution."""
    return PaperTradeError(
        timestamp=timestamp,
        intent=intent_dict,
        error_type=PaperTradeErrorType.INTERNAL_ERROR,
        error_message=str(exc),
        block_number=block_number,
        metadata={
            "exception_type": type(exc).__name__,
            "intent_type": intent_type_value,
        },
    )


def extract_receipt_tx_details(
    result: ExecutionResult,
    fallback_block: int,
) -> tuple[str, int, int, TransactionReceipt | None]:
    """Pull ``(tx_hash, block_number, gas_used, receipt)`` from the first tx result."""
    tx_hash = ""
    block_number = fallback_block
    gas_used = 0
    receipt: TransactionReceipt | None = None
    if result.transaction_results:
        first_result = result.transaction_results[0]
        tx_hash = first_result.tx_hash or ""
        if first_result.receipt:
            receipt = first_result.receipt  # type: ignore[assignment]
            if receipt.block_number is not None:  # type: ignore[union-attr]
                block_number = receipt.block_number  # type: ignore[union-attr]
            if receipt.gas_used is not None:  # type: ignore[union-attr]
                gas_used = receipt.gas_used  # type: ignore[union-attr]
    return tx_hash, block_number, gas_used, receipt
