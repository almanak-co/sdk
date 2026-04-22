"""State, metrics, and observability methods for StrategyRunner.

Extracted from strategy_runner.py for maintainability. Each function takes
``runner`` (a StrategyRunner instance) as its first argument and is called
via a thin delegation stub in StrategyRunner.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..intents.vocabulary import AnyIntent, HoldIntent
from ..portfolio import PortfolioMetrics, PortfolioSnapshot, ValueConfidence
from ..state.exceptions import AccountingPersistenceError
from ..state.state_manager import StateData, StateNotFoundError
from .reconciliation import BalanceSnapshot, build_reconciliation_report

if TYPE_CHECKING:
    from ..execution.orchestrator import ExecutionResult
    from .runner_models import IterationResult, StatefulActivityProviderProtocol, StrategyProtocol

# Use the original strategy_runner logger so existing log-capture tests and
# log-filtering rules continue to work after the extraction.
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# -------------------------------------------------------------------------
# State persistence
# -------------------------------------------------------------------------


async def update_state(
    runner: Any,
    strategy_id: str,
    result: IterationResult,
    strategy: object | None = None,
) -> None:
    """Update persisted state after an iteration."""
    try:
        # Try to load current state, create new if not found
        try:
            state = await runner.state_manager.load_state(strategy_id)
            # GatewayStateManager returns None instead of raising StateNotFoundError
            if state is None:
                raise StateNotFoundError(strategy_id)
            expected_version = state.version
        except StateNotFoundError:
            # First run - create new state
            state = StateData(
                strategy_id=strategy_id,
                version=1,
                state={},
            )
            expected_version = None  # No version check for new state
            logger.debug(f"Creating initial state for {strategy_id}")

        # Merge strategy's persistent state first (position_id, etc.)
        # strategy.save_state() uses ensure_future (fire-and-forget) which
        # races with this method. Merge here to avoid clobbering.
        if hasattr(strategy, "get_persistent_state"):
            try:
                strat_state = strategy.get_persistent_state()
                if strat_state:
                    state.state.update(strat_state)
            except Exception:
                logger.warning(
                    "Failed to merge strategy persistent state for %s, position tracking data may be stale",
                    strategy_id,
                    exc_info=True,
                )

        # Update state with iteration info
        state.state["last_iteration"] = {
            "timestamp": result.timestamp.isoformat(),
            "status": result.status.value,
            "intent_type": result.intent.intent_type.value if result.intent else None,
            "duration_ms": result.duration_ms,
        }
        state.state["total_iterations"] = runner._total_iterations
        state.state["successful_iterations"] = runner._successful_iterations
        state.state["consecutive_errors"] = runner._consecutive_errors

        # Save with CAS (or create if new)
        await runner.state_manager.save_state(state, expected_version=expected_version)

        logger.debug(f"State updated for {strategy_id}")

    except Exception as e:
        logger.error(f"Failed to update state for {strategy_id}: {e}")


async def persist_copy_trading_state(
    runner: Any,
    strategy_id: str,
    activity_provider: StatefulActivityProviderProtocol,
) -> None:
    """Persist copy trading cursor state into the strategy state dict."""
    try:
        state = await runner.state_manager.load_state(strategy_id)
        if state is None:
            return
        expected_version = state.version
        state.state["copy_trading_state"] = activity_provider.get_state()
        await runner.state_manager.save_state(state, expected_version=expected_version)
        logger.debug("Copy trading state persisted")
    except Exception as e:
        logger.warning(f"Failed to persist copy trading state: {e}")


async def persist_vault_state(
    runner: Any,
    strategy_id: str,
    vault_state_dict: dict,
    vault_state_key: str,
) -> None:
    """Persist vault lifecycle state into the strategy state dict."""
    try:
        state = await runner.state_manager.load_state(strategy_id)
        if state is None:
            # First run -- create state so vault lifecycle is not lost
            state = StateData(
                strategy_id=strategy_id,
                version=1,
                state={},
            )
            expected_version = None
        else:
            expected_version = state.version
        state.state[vault_state_key] = vault_state_dict
        await runner.state_manager.save_state(state, expected_version=expected_version)
        logger.debug("Vault state persisted (phase=%s)", vault_state_dict.get("settlement_phase", "?"))
    except Exception as e:
        logger.warning(f"Failed to persist vault state: {e}")


# -------------------------------------------------------------------------
# Portfolio snapshots
# -------------------------------------------------------------------------


async def capture_portfolio_snapshot(
    runner: Any,
    strategy: StrategyProtocol,
    iteration_number: int,
    force_snapshot: bool = False,
) -> PortfolioSnapshot | None:
    """Capture and persist portfolio snapshot after iteration.

    Uses the framework-owned PortfolioValuer as the primary valuation path.
    Falls back to strategy.get_portfolio_snapshot() if the valuer cannot
    produce a valid snapshot (migration fallback for Week 1).

    Pipeline: PortfolioValuer -> PortfolioSnapshot -> StateManager -> Dashboard

    Args:
        runner: StrategyRunner instance
        strategy: The strategy to capture snapshot from
        iteration_number: Current iteration count
        force_snapshot: If True, bypass the throttle (e.g., trade executed this cycle)

    Returns:
        PortfolioSnapshot if captured, None if skipped or not supported
    """
    now = datetime.now(UTC)

    # Rate-limit snapshot persistence (store every 5 min for time-series).
    # Always snapshot when a trade executed (force_snapshot=True) so every
    # trade gets before/after valuation for accounting.
    if not force_snapshot and runner._last_snapshot_time is not None:
        elapsed = (now - runner._last_snapshot_time).total_seconds()
        if elapsed < runner._snapshot_interval_seconds:
            return None

    try:
        snapshot: PortfolioSnapshot | None = None

        # Primary path: framework-owned PortfolioValuer
        # Skip for multi-chain strategies -- their MarketSnapshot requires
        # chain= argument that PortfolioValuer doesn't pass yet.
        if (
            hasattr(strategy, "_get_tracked_tokens")
            and hasattr(strategy, "create_market_snapshot")
            and not runner._is_multi_chain
        ):
            try:
                # Ensure valuer has gateway client for LP re-pricing
                gw = runner._get_gateway_client()
                if gw is not None:
                    runner._portfolio_valuer.set_gateway_client(gw)

                market = strategy.create_market_snapshot()
                snapshot = runner._portfolio_valuer.value(
                    strategy=strategy,
                    market=market,
                    iteration_number=iteration_number,
                )
                # If valuer produced a valid snapshot, use it
                if snapshot and snapshot.value_confidence != ValueConfidence.UNAVAILABLE:
                    logger.debug(
                        "Portfolio valued by PortfolioValuer for %s: $%.2f (%s)",
                        strategy.strategy_id,
                        snapshot.total_value_usd,
                        snapshot.value_confidence.value,
                    )
            except Exception as e:
                logger.debug("PortfolioValuer failed, trying fallback: %s", e)
                snapshot = None

        # Fallback: strategy's own get_portfolio_snapshot (migration path)
        if (snapshot is None or snapshot.value_confidence == ValueConfidence.UNAVAILABLE) and hasattr(
            strategy, "get_portfolio_snapshot"
        ):
            fallback = strategy.get_portfolio_snapshot()
            if fallback is not None:
                fallback.iteration_number = iteration_number
            if fallback is not None and fallback.value_confidence != ValueConfidence.UNAVAILABLE:
                snapshot = fallback
                logger.debug(
                    "Portfolio valued by strategy fallback for %s: $%.2f",
                    strategy.strategy_id,
                    snapshot.total_value_usd,
                )
            elif snapshot is None:
                snapshot = fallback

        # Failure contract: never skip a snapshot -- construct UNAVAILABLE if needed
        if snapshot is None:
            snapshot = PortfolioSnapshot(
                timestamp=now,
                strategy_id=strategy.strategy_id,
                total_value_usd=Decimal("0"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.UNAVAILABLE,
                error="No valuation path produced a portfolio snapshot",
                chain=getattr(strategy, "chain", ""),
                iteration_number=iteration_number,
            )

        # Build metrics for atomic co-write (VIB-2765)
        metrics = await _build_metrics_for_snapshot(runner, strategy.strategy_id, snapshot)

        # Atomic co-write: snapshot + metrics in one transaction when supported.
        if metrics and hasattr(runner.state_manager, "save_snapshot_and_metrics"):
            snapshot_id = await runner.state_manager.save_snapshot_and_metrics(snapshot, metrics)
        else:
            # Fallback: separate writes (GatewayStateManager, etc.)
            snapshot_id = await runner.state_manager.save_portfolio_snapshot(snapshot)
            if metrics:
                await runner.state_manager.save_portfolio_metrics(metrics)

        if snapshot_id > 0:
            runner._last_snapshot_time = now
            logger.debug(
                "Portfolio snapshot persisted for %s: $%.2f (id=%d, confidence=%s)",
                strategy.strategy_id,
                snapshot.total_value_usd,
                snapshot_id,
                snapshot.value_confidence.value,
            )

        # Write valuation fields into strategy state so DashboardService can read them.
        # Always persist (even zero) to avoid stale dashboard values.
        try:
            state = await runner.state_manager.load_state(strategy.strategy_id)
            if state is not None:
                state.state["total_value_usd"] = str(snapshot.total_value_usd)
                state.state["value_confidence"] = snapshot.value_confidence.value
                _RECONCILIATION_STATE_KEYS = (
                    "valuation_source",
                    "external_provider",
                    "external_total_value_usd",
                    "framework_total_value_usd",
                    "reconciliation_status",
                )
                for key in _RECONCILIATION_STATE_KEYS:
                    if snapshot.snapshot_metadata and key in snapshot.snapshot_metadata:
                        state.state[key] = str(snapshot.snapshot_metadata[key])
                    else:
                        state.state.pop(key, None)
                await runner.state_manager.save_state(state, expected_version=state.version)
        except Exception as ve:
            logger.debug("Failed to write valuation into strategy state: %s", ve)

        return snapshot

    except AccountingPersistenceError:
        # VIB-3157: snapshot/metrics backend write failed. Surface to the
        # runner so it can halt with ACCOUNTING_FAILED in live mode -- the
        # mode-aware decision (paper/dry-run may continue) lives upstream so
        # this layer never silently drops the failure.
        raise
    except Exception as e:
        logger.warning(f"Failed to capture portfolio snapshot: {e}")
        # Failure contract: persist UNAVAILABLE snapshot rather than skipping
        try:
            # Use getattr for all strategy accessors -- the main path may have
            # failed because one of these properties raised.
            sid = getattr(strategy, "strategy_id", "unknown")
            chain = getattr(strategy, "chain", "")
            unavailable_snapshot = PortfolioSnapshot(
                timestamp=now,
                strategy_id=sid,
                total_value_usd=Decimal("0"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.UNAVAILABLE,
                error=str(e),
                chain=chain,
                iteration_number=iteration_number,
            )
            await runner.state_manager.save_portfolio_snapshot(unavailable_snapshot)
            runner._last_snapshot_time = now
        except AccountingPersistenceError:
            raise
        except Exception as persist_err:
            logger.warning("Failed to persist UNAVAILABLE snapshot: %s", persist_err)
        return None


async def _build_metrics_for_snapshot(
    runner: Any,
    strategy_id: str,
    snapshot: PortfolioSnapshot,
) -> PortfolioMetrics | None:
    """Build a PortfolioMetrics object for the given snapshot.

    On first run, establishes ``initial_value_usd`` as baseline.
    On subsequent runs, preserves the baseline and updates current value.

    Returns:
        A PortfolioMetrics ready to persist, or None if metrics shouldn't
        be written (e.g., unavailable snapshot, unsupported state manager).
    """
    try:
        if not hasattr(runner.state_manager, "get_portfolio_metrics"):
            return None

        if snapshot.error or snapshot.value_confidence == ValueConfidence.UNAVAILABLE:
            logger.info(f"Skipping portfolio metrics for {strategy_id}: snapshot unavailable")
            return None

        # Phase 4: derive deployment_id, execution_mode, and cycle_id from runner context.
        # VIB-3157: shared helper keeps the tri-state mapping aligned with
        # ledger entries (``StrategyRunner._derive_execution_mode``).
        from almanak.framework.runner.strategy_runner import derive_execution_mode_from_config

        execution_mode = derive_execution_mode_from_config(runner.config)

        # Get cycle_id: prefer runner._last_cycle_id (survives clear_cycle_id in finally block)
        # Fall back to observability context for non-runner callers
        cycle_id = getattr(runner, "_last_cycle_id", "") or ""
        if not cycle_id:
            try:
                from almanak.framework.observability.context import get_cycle_id

                cycle_id = get_cycle_id() or ""
            except Exception as e:
                logger.debug("cycle_id context fallback failed: %s", e)

        # Resolve deployment_id: prefer runner's deployment_id, fall back to strategy_id
        deployment_id = getattr(runner, "deployment_id", "") or snapshot.strategy_id

        existing = await runner.state_manager.get_portfolio_metrics(strategy_id)

        if existing is None:
            metrics = PortfolioMetrics(
                strategy_id=strategy_id,
                timestamp=snapshot.timestamp,
                total_value_usd=snapshot.total_value_usd,
                initial_value_usd=snapshot.total_value_usd,
                deployment_id=deployment_id,
                execution_mode=execution_mode,
                cycle_id=cycle_id,
            )
            logger.info(f"Portfolio baseline established for {strategy_id}: ${snapshot.total_value_usd:.2f}")
            return metrics

        existing.timestamp = snapshot.timestamp
        existing.total_value_usd = snapshot.total_value_usd
        # Phase 4: always refresh execution_mode, deployment_id, and cycle_id
        existing.execution_mode = execution_mode
        existing.cycle_id = cycle_id
        if not existing.deployment_id:
            existing.deployment_id = deployment_id
        return existing

    except Exception as e:
        logger.warning(f"Failed to build portfolio metrics: {e}")
        return None


async def update_portfolio_metrics(
    runner: Any,
    strategy_id: str,
    snapshot: PortfolioSnapshot,
) -> None:
    """Update portfolio metrics for PnL tracking (legacy entry point).

    Delegates to ``_build_metrics_for_snapshot`` + save.
    Kept for backward compatibility with code paths that don't use
    the atomic co-write.
    """
    metrics = await _build_metrics_for_snapshot(runner, strategy_id, snapshot)
    if metrics is not None:
        try:
            await runner.state_manager.save_portfolio_metrics(metrics)
        except AccountingPersistenceError:
            # VIB-3157: propagate so the runner's ACCOUNTING_FAILED path fires.
            raise
        except Exception as e:
            logger.warning(f"Failed to save portfolio metrics: {e}")


# -------------------------------------------------------------------------
# Metrics
# -------------------------------------------------------------------------


def get_metrics(runner: Any) -> dict[str, Any]:
    """Get current runner metrics.

    Returns:
        Dictionary with iteration counts, error counts, and success rate
    """
    success_rate = runner._successful_iterations / runner._total_iterations if runner._total_iterations > 0 else 0.0

    return {
        "total_iterations": runner._total_iterations,
        "successful_iterations": runner._successful_iterations,
        "consecutive_errors": runner._consecutive_errors,
        "success_rate": success_rate,
        "shutdown_requested": runner._shutdown_requested,
    }


# -------------------------------------------------------------------------
# Pause check
# -------------------------------------------------------------------------


async def is_strategy_paused(runner: Any, strategy_id: str) -> tuple[bool, str | None]:
    """Check persisted control state to determine if strategy is paused."""
    try:
        state_obj = await runner.state_manager.load_state(strategy_id)
    except Exception as e:  # noqa: BLE001
        # Fail-open by design: if state is temporarily unavailable, continue strategy execution.
        logger.warning("Unable to load pause state for %s; continuing as unpaused: %s", strategy_id, e)
        return False, None

    if state_obj is None or not isinstance(state_obj.state, dict):
        return False, None

    state = state_obj.state
    if not bool(state.get("is_paused", False)):
        return False, None

    reason = state.get("pause_reason")
    return True, str(reason) if isinstance(reason, str) and reason else None


# -------------------------------------------------------------------------
# Balance reconciliation
# -------------------------------------------------------------------------


async def snapshot_balances_for_intent(
    runner: Any,
    intent: AnyIntent,
) -> BalanceSnapshot | None:
    """Capture a balance snapshot for every token named by the intent.

    Returns a ``BalanceSnapshot`` (with the timestamp of when balances were
    actually queried) for tokens whose balance query succeeded, or ``None``
    if the intent names no tokens or every balance query failed. Individual
    balance failures are skipped (non-fatal) so a flaky RPC for one token
    does not blind reconciliation on the others.
    """
    tokens = extract_intent_tokens(intent)
    if not tokens:
        return None

    balances: dict[str, Decimal] = {}
    for token_symbol in tokens:
        try:
            bal = await runner.balance_provider.get_balance(token_symbol)
            balances[token_symbol] = bal.balance
        except Exception as exc:  # noqa: BLE001
            logger.debug("Balance snapshot: failed to fetch %s balance: %s", token_symbol, exc)
            continue
    # If every balance query failed, treat the pre-snapshot as unavailable so
    # callers fall back to the legacy post-only mode rather than silently
    # running real-delta reconciliation with zero tokens to compare.
    if not balances:
        return None
    return BalanceSnapshot(timestamp=datetime.now(UTC), balances=balances)


async def reconcile_post_execution_balances(
    runner: Any,
    strategy: StrategyProtocol,
    intent: AnyIntent,
    execution_result: ExecutionResult | None,
    pre_snapshot: BalanceSnapshot | None = None,
) -> dict[str, Any] | None:
    """Verify post-execution token balances match intent expectations.

    When ``pre_snapshot`` is supplied the reconciliation runs in real-delta
    mode (VIB-3158): actual deltas are computed from pre vs post, and for
    supported intent types (currently SwapIntent) an expected-range check is
    performed. Any mismatch is flagged as an incident in the returned dict.

    When ``pre_snapshot`` is ``None`` the legacy post-only behavior is
    preserved so older call sites continue to work; in that case only
    warnings (not incidents) are produced.
    """
    try:
        tokens = extract_intent_tokens(intent)
        if not tokens:
            return None

        post_balances: dict[str, Decimal] = {}
        for token_symbol in tokens:
            try:
                bal = await runner.balance_provider.get_balance(token_symbol)
                post_balances[token_symbol] = bal.balance
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "Balance reconciliation: failed to fetch %s balance: %s",
                    token_symbol,
                    exc,
                )
                continue
        # Capture the post-query timestamp immediately after the loop so the
        # reported pre/post timestamps reflect when the balances were
        # actually read, not when the report is materialised.
        post_timestamp = datetime.now(UTC)

        if not post_balances:
            return None

        if pre_snapshot is not None:
            # Real-delta mode — build a structured report with deltas and
            # SwapIntent expected-range enforcement.
            post_snapshot = BalanceSnapshot(timestamp=post_timestamp, balances=post_balances)

            # Resolve the chain's native gas token + actual gas spend so the
            # reconciliation can absorb gas outflow for native-from swaps
            # (e.g. ETH->USDC on Arbitrum). Without this, successful
            # native-gas-token swaps are falsely flagged as overspends.
            gas_token, gas_cost_native = _resolve_gas_context(intent, execution_result)

            report = build_reconciliation_report(
                pre=pre_snapshot,
                post=post_snapshot,
                intent=intent,
                execution_result=execution_result,
                gas_token=gas_token,
                gas_cost_native=gas_cost_native,
            )
            recon = report.to_dict()

            if report.incident:
                logger.error(
                    "Balance reconciliation incident for %s: %s",
                    strategy.strategy_id,
                    recon["mismatches"],
                )
            elif report.warnings:
                logger.warning(
                    "Balance reconciliation warnings for %s: %s",
                    strategy.strategy_id,
                    report.warnings,
                )

            return recon

        # Legacy post-only fallback (no pre-snapshot available).
        recon = {
            "tokens_checked": list(post_balances.keys()),
            "post_balances": {k: str(v) for k, v in post_balances.items()},
            "warnings": [],
            "incident": False,
            "enforced": False,
        }

        if execution_result and execution_result.swap_amounts:
            sa = execution_result.swap_amounts
            if sa.amount_out_decimal is not None and sa.amount_out_decimal <= 0:
                recon["warnings"].append(f"Swap output amount is zero or negative: {sa.amount_out_decimal}")
            if sa.amount_in_decimal is not None and sa.amount_in_decimal <= 0:
                recon["warnings"].append(f"Swap input amount is zero or negative: {sa.amount_in_decimal}")

        if recon["warnings"]:
            logger.warning(
                "Balance reconciliation warnings for %s: %s",
                strategy.strategy_id,
                recon["warnings"],
            )

        return recon

    except Exception as e:
        logger.debug(f"Balance reconciliation skipped: {e}")
        return None


def extract_intent_tokens(intent: AnyIntent) -> list[str]:
    """Extract token symbols involved in an intent."""
    tokens: list[str] = []
    # SwapIntent
    if hasattr(intent, "from_token") and hasattr(intent, "to_token"):
        tokens.extend([intent.from_token, intent.to_token])
    # LP intents
    elif hasattr(intent, "token0") and hasattr(intent, "token1"):
        tokens.extend([intent.token0, intent.token1])
    # Supply/Borrow intents
    elif hasattr(intent, "token"):
        tokens.append(intent.token)
    return tokens


def _resolve_gas_context(
    intent: AnyIntent,
    execution_result: ExecutionResult | None,
) -> tuple[str | None, Decimal | None]:
    """Resolve (native_gas_symbol, gas_cost_native) for the intent's chain.

    Returns ``(None, None)`` when the chain is unknown, the execution result
    lacks gas data, or the chain has no registered native-token entry. The
    reconciliation logic only stretches the from-token bound when
    ``gas_token == intent.from_token``, so a conservative default of ``None``
    simply means "do not absorb gas" — which matches the prior behavior for
    non-native-from swaps.
    """
    if execution_result is None:
        return None, None
    chain = getattr(intent, "chain", None)
    if not chain:
        return None, None

    gas_cost_wei = getattr(execution_result, "total_gas_cost_wei", 0) or 0
    if gas_cost_wei <= 0:
        return None, None

    try:
        from almanak.gateway.services.onchain_lookup import NATIVE_TOKEN_INFO
    except Exception:  # noqa: BLE001 — optional dep path
        return None, None

    info = NATIVE_TOKEN_INFO.get(str(chain).lower())
    if not info:
        return None, None

    symbol = info.get("symbol")
    if not symbol:
        return None, None

    # EVM native gas tokens are always 18 decimals by protocol design
    # (gas_cost_wei is in wei); this is not the same as the ERC-20 "never
    # default to 18 decimals" rule.
    gas_cost_native = Decimal(gas_cost_wei) / Decimal(10**18)
    return symbol, gas_cost_native


# -------------------------------------------------------------------------
# Success/duration helpers
# -------------------------------------------------------------------------


def record_success(runner: Any, *, execution_proved: bool = False) -> None:
    """Record a successful iteration in metrics and circuit breaker.

    Args:
        runner: StrategyRunner instance
        execution_proved: True when an actual on-chain execution succeeded.
            Only execution-proved successes count toward closing a HALF_OPEN
            circuit breaker, so HOLD/DRY_RUN cannot prematurely close the
            breaker without proving the execution path works.
    """
    runner._total_iterations += 1
    runner._successful_iterations += 1
    runner._consecutive_errors = 0
    if runner._circuit_breaker is not None and execution_proved:
        runner._circuit_breaker.record_success()


def record_failure(runner: Any) -> None:
    """Record a failed iteration in lifetime metrics.

    Mirrors :func:`record_success` for the failure path: increments ONLY
    ``_total_iterations`` so every iteration that produces an
    ``IterationResult`` — success or failure — is visible in the lifetime
    count. This is the companion to ``_create_error_result`` for failure
    sites that build an ``IterationResult`` directly rather than routing
    through the error-result helper (issue #1780, Gemini finding on PR
    #1777).

    ``_consecutive_errors`` and the circuit breaker are NOT touched here —
    those remain owned by ``_run_loop_helpers.handle_iteration_failure``,
    which runs unconditionally for any ``result.success is False`` in the
    run loop (fix #1771). Incrementing them here would double-count every
    failure that flows back through ``run_loop``.

    Args:
        runner: StrategyRunner instance
    """
    runner._total_iterations += 1


def calculate_duration_ms(runner: Any, start_time: datetime) -> float:
    """Calculate duration in milliseconds since start_time."""
    elapsed = datetime.now(UTC) - start_time
    return elapsed.total_seconds() * 1000


# -------------------------------------------------------------------------
# Stuck detection and alerting
# -------------------------------------------------------------------------


async def detect_stuck_and_alert(runner: Any, strategy: StrategyProtocol, result: IterationResult) -> None:
    """Run stuck detection on a failed iteration and generate an OperatorCard if stuck.

    Lazy-initializes StuckDetector and OperatorCardGenerator on first call to
    avoid import overhead on every iteration.

    Args:
        runner: StrategyRunner instance
        strategy: The strategy that failed
        result: The failed iteration result
    """
    try:
        # Lazy import and init to avoid overhead on the happy path
        if runner._stuck_detector is None:
            from ..services.stuck_detector import StuckDetector

            runner._stuck_detector = StuckDetector(emit_events=True)

        if runner._operator_card_generator is None:
            from ..services.operator_card_generator import OperatorCardGenerator

            runner._operator_card_generator = OperatorCardGenerator()

        from ..services.stuck_detector import StrategySnapshot

        # Build a lightweight snapshot from available runner state
        state_entered_at = runner._first_error_at or datetime.now(UTC)
        snapshot = StrategySnapshot(
            strategy_id=strategy.strategy_id,
            chain=getattr(strategy, "chain", "unknown"),
            current_state=result.status.value,
            state_entered_at=state_entered_at,
            pending_transactions=[],
            circuit_breaker_triggered=(
                runner._circuit_breaker is not None and runner._circuit_breaker.state.value != "closed"
            ),
        )

        detection = runner._stuck_detector.detect_stuck(snapshot)
        if not detection.is_stuck:
            return

        logger.warning(
            "StuckDetector: %s is stuck (reason=%s, duration=%.0fs)",
            strategy.strategy_id,
            detection.reason.value if detection.reason else "unknown",
            detection.time_in_state_seconds,
        )

        # Generate OperatorCard
        from ..services.operator_card_generator import ErrorContext, StrategyState

        total_value, available_balance = runner._query_portfolio_value(strategy)
        strategy_state = StrategyState(
            strategy_id=strategy.strategy_id,
            status="stuck",
            total_value_usd=total_value,
            available_balance_usd=available_balance,
            stuck_since=state_entered_at,
        )
        error_context = ErrorContext(
            error_type=result.status.value,
            error_message=result.error or "unknown",
        )
        card = runner._operator_card_generator.generate_card(
            strategy_state=strategy_state,
            error_context=error_context,
        )

        # Route card to AlertManager
        if runner.alert_manager is not None:
            try:
                await runner.alert_manager.send_alert(card)
            except Exception as alert_err:
                logger.debug("Failed to send stuck alert (non-fatal): %s", alert_err)

    except Exception as e:
        # Stuck detection is non-fatal — never block the runner
        logger.debug("Stuck detection failed (non-fatal): %s", e)


# -------------------------------------------------------------------------
# Iteration summary emission
# -------------------------------------------------------------------------


def emit_iteration_summary(runner: Any, result: IterationResult, chain: str | None = None) -> None:
    """Emit a structured iteration_summary log record for JSONL analysis.

    This provides a single, machine-readable record per iteration containing
    all key fields needed for post-hoc analysis by AI agents or dashboards.
    """
    # Extract intent info
    intent_type = None
    intents_serialized: list[dict[str, Any]] = []
    hold_reason_code: str | None = None
    hold_reason: str | None = None
    if result.intent:
        intent_type = result.intent.intent_type.value if hasattr(result.intent, "intent_type") else None
        try:
            intents_serialized = [result.intent.serialize()] if hasattr(result.intent, "serialize") else []
        except Exception:  # noqa: BLE001
            logger.debug("Failed to serialize intent for iteration_summary", exc_info=True)
        # Extract HOLD reason fields
        if isinstance(result.intent, HoldIntent):
            hold_reason = result.intent.reason
            hold_reason_code = result.intent.reason_code

    # Extract execution info
    tx_hashes: list[str] = []
    txs_planned = 0
    txs_sent = 0
    gas_used = 0
    if result.execution_result:
        er = result.execution_result
        if hasattr(er, "transaction_results") and er.transaction_results:
            tx_hashes = [tr.tx_hash for tr in er.transaction_results if hasattr(tr, "tx_hash") and tr.tx_hash]
            txs_sent = len(tx_hashes)
        if hasattr(er, "tx_hashes") and er.tx_hashes:
            tx_hashes = tx_hashes or er.tx_hashes
            txs_sent = txs_sent or len(er.tx_hashes)
        # txs_planned: count from action bundle if available
        if hasattr(er, "receipts"):
            txs_planned = max(txs_planned, len(er.receipts))
        txs_planned = max(txs_planned, txs_sent)
        gas_used = getattr(er, "total_gas_used", 0) or 0

    # Extract reconciliation status (tri-state: None=unchecked, True=clean, False=mismatch)
    # VIB-3158: a report is only "clean" when there is neither an incident
    # NOR outstanding warnings — warning-only reports mean coverage was
    # degraded (missing balance, stale cache, unenforceable intent type) and
    # must not be summarized as OK.
    reconciliation_ok: bool | None = None
    if result.balance_reconciliation is not None:
        recon = result.balance_reconciliation
        has_incident = bool(recon.get("incident", False))
        has_warnings = bool(recon.get("warnings"))
        reconciliation_ok = not has_incident and not has_warnings

    logger.info(
        "iteration_summary",
        extra={
            "event_type": "iteration_summary",
            "strategy_id": result.strategy_id,
            "chain": chain,
            "iteration": runner._total_iterations,
            "decision": intent_type,
            "intents": intents_serialized,
            "dry_run": runner.config.dry_run,
            "txs_planned": txs_planned,
            "txs_sent": txs_sent,
            "tx_hashes": tx_hashes,
            "gas_used": gas_used,
            "status": result.status.value,
            "duration_ms": round(result.duration_ms, 1),
            "hold_reason": hold_reason,
            "hold_reason_code": hold_reason_code,
            "reconciliation_ok": reconciliation_ok,
            "error": result.error,
        },
    )
