"""Phase helpers for :meth:`StrategyRunner.run_loop` (Phase 6A.2).

This module contains phase-level helpers extracted from the main loop body
of ``StrategyRunner.run_loop`` to reduce cyclomatic complexity and isolate
responsibilities. Every helper preserves the EXACT original behavior
captured by the characterization tests in
``tests/unit/runner/test_run_loop_characterization.py``.

Design notes
------------
* Helpers are module-level functions (not methods) that take the runner
  instance explicitly. This keeps them free of ``self.`` noise inside
  ``run_loop`` while still respecting the runner's private state.
* All ``_consecutive_errors`` / ``_first_error_at`` semantics, circuit
  breaker call sites, lifecycle write precedence, and log messages are
  reproduced byte-for-byte from the pre-extraction body.
* ``_run_loop_helpers`` does NOT import from ``strategy_runner`` at module
  load time — it uses ``TYPE_CHECKING`` to avoid a circular import while
  still offering typed signatures.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..state.exceptions import AccountingPersistenceError

if TYPE_CHECKING:
    from .runner_models import (
        IterationResult,
        StatefulActivityProviderProtocol,
        StrategyProtocol,
    )
    from .strategy_runner import StrategyRunner

logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# =============================================================================
# Cross-entry-point startup helpers
# =============================================================================


def reconstruct_lending_basis_store(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
) -> int:
    """Replay durable accounting_events into the in-memory FIFO basis store.

    Cross-process restart hole (VIB-3944): when a strategy restarts inside a
    new CLI process — e.g. the harness's ``--once --teardown-after`` Phase 2
    that follows a ``--max-iterations N`` Phase 1 — the in-memory
    ``runner._lending_basis_store`` is empty. Without this rebuild step the
    teardown REPAY's ``match_repay()`` returns ``unmatched=full_amount``, the
    writer cannot emit ``interest_delta_usd``, and the L4 Accountant Test
    cell fails with "FIFO basis store may not have a matching BORROW lot."

    ``initialize_run_loop`` (the continuous ``run_loop`` entry) calls this
    helper inline; the ``_run_once`` and ``_run_test_lifecycle`` CLI paths
    in ``framework/cli/run_helpers.py`` call it after ``setup_gateway_integration``
    so the gRPC channel is up before we issue ``GetAccountingEvents``.

    Returns the number of lot operations replayed (``0`` if there are no
    events, the persistence backend is disabled, or any error is suppressed
    in non-live mode). In live mode any failure is converted to ``RuntimeError``
    so we never silently start a strategy with an inconsistent FIFO view.
    """
    if not runner.config.enable_state_persistence:
        return 0

    # State-manager readiness gate (Claude pr-auditor 2026-05-04 review).
    # ``initialize_run_loop`` only invokes this helper when its own
    # ``state_manager_ready`` flag is True; the ``--once`` and
    # ``_run_test_lifecycle`` CLI paths bypass that check, so the helper
    # owns its own precondition. ``runner.state_manager is None`` is the
    # observable shape when persistence is disabled or initialize() failed
    # — early return matches the legacy "events=[] → no-op" path.
    if getattr(runner, "state_manager", None) is None:
        return 0

    deployment_id = ""
    try:
        deployment_id = getattr(strategy, "deployment_id", "") or strategy_id
        if not deployment_id:
            return 0
        events = runner.state_manager.get_accounting_events_sync(deployment_id)
        if not events:
            return 0
        replayed = runner._lending_basis_store.reconstruct_from_events(events)
        if replayed:
            logger.info(
                "Reconstructed %d FIFO lot operations for %s from accounting_events",
                replayed,
                deployment_id,
            )
        return replayed
    except Exception as e:
        if runner._is_live_mode():
            raise RuntimeError(f"Failed to reconstruct FIFO basis store for {deployment_id}: {e}") from e
        logger.warning("Failed to reconstruct FIFO basis store on startup: %s", e)
        return 0


def _open_event_payload(ev: dict) -> dict:
    """Project a position_events row into the runner's cache shape."""
    return {
        "value_usd": str(ev.get("value_usd") or ""),
        "ledger_entry_id": str(ev.get("ledger_entry_id") or ""),
        "timestamp": str(ev.get("timestamp") or ""),
        "tick_lower": ev.get("tick_lower"),
        "tick_upper": ev.get("tick_upper"),
        "liquidity": str(ev.get("liquidity") or ""),
        "token0": str(ev.get("token0") or ""),
        "token1": str(ev.get("token1") or ""),
    }


def _collect_open_positions(
    events: list[dict],
) -> dict[tuple[str, str], dict]:
    """Group ``position_events`` rows into the still-open set.

    A position is "still open" when its most-recent event is NOT a CLOSE.
    Events arrive timestamp-ASC, so later rows correctly overwrite earlier
    ones for the same key.
    """
    by_position: dict[tuple[str, str], dict] = {}
    for ev in events:
        pid = str(ev.get("position_id") or "")
        ptype = str(ev.get("position_type") or "")
        if not pid or not ptype:
            continue
        key = (pid, ptype)
        et = str(ev.get("event_type") or "").upper()
        if et == "CLOSE":
            by_position.pop(key, None)
        elif et == "OPEN":
            by_position[key] = _open_event_payload(ev)
    return by_position


async def hydrate_recent_open_events_cache(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
) -> int:
    """VIB-4086 / VIB-4085 — pre-populate ``runner._recent_open_events`` from disk.

    The cache is the authority for two lifecycle decisions made at write time:

    * LP_CLOSE column carry-forward (VIB-3919 / VIB-4086): tick_lower /
      tick_upper / liquidity / token0 / token1 are immutable across the
      position lifetime; the close-receipt parser doesn't re-emit them, so
      the runner reads them from this cache when building the CLOSE event.
    * Lending OPEN-vs-INCREASE (VIB-4085): a SUPPLY/BORROW emits OPEN on
      cache miss and INCREASE on cache hit.

    Without this hydration, a fresh process that closes a position opened
    by a prior process (operator restart mid-strategy, the harness's
    ``--once --teardown-after`` two-phase invocation, hosted scheduler
    restart, etc.) lands the CLOSE row with empty token columns and
    misclassifies a 2nd SUPPLY post-restart as an unrelated OPEN.

    Returns the number of cache entries populated. Non-fatal: any error
    is logged at WARN and the runner continues with an empty cache —
    in-process flows still work, only cross-process continuity degrades.
    """
    state_manager = getattr(runner, "state_manager", None)
    if state_manager is None or not hasattr(state_manager, "get_position_events_sync"):
        # GatewayStateManager (hosted) doesn't expose the sync getter.
        # Hosted mode runs without restart-mid-position semantics today
        # (VIB-3866 truth correction §15a.3), so this is acceptable.
        return 0

    deployment_id = getattr(strategy, "deployment_id", "") or strategy.strategy_id
    if not deployment_id:
        return 0

    try:
        all_events = state_manager.get_position_events_sync(deployment_id)
    except Exception as e:  # noqa: BLE001 — best-effort startup hydration
        logger.warning("Failed to hydrate recent_open_events cache: %s", e)
        return 0

    if not all_events:
        return 0

    by_position = _collect_open_positions(all_events)
    for key, payload in by_position.items():
        runner._recent_open_events[key] = payload
    populated = len(by_position)

    if populated:
        logger.info(
            "Hydrated %d open position(s) into recent_open_events cache for %s",
            populated,
            deployment_id,
        )
    return populated


# =============================================================================
# Pre-loop initialization
# =============================================================================


async def initialize_run_loop(  # noqa: C901
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
    interval: int,
) -> StatefulActivityProviderProtocol | None:
    """Run the one-shot setup before the ``while`` loop begins.

    Mirrors the original setup block from ``run_loop`` lines ~1411-1472:
    state manager init, incomplete session recovery, copy-trading cursor
    restore, shutdown flag reset, gateway wiring, lifecycle RUNNING write,
    and the STRATEGY_STARTED timeline event.

    Returns the resolved ``activity_provider`` (may be ``None``) so the
    caller can feed it to the success-branch copy-trading persist step.
    """
    # Initialize state if enabled
    state_manager_ready = False
    if runner.config.enable_state_persistence:
        try:
            await runner.state_manager.initialize()
            state_manager_ready = True
            logger.debug(f"State manager initialized for {strategy_id}")
        except Exception as e:
            if runner._is_live_mode():
                raise RuntimeError(f"Failed to initialize state manager for {strategy_id}: {e}") from e
            logger.error(f"Failed to initialize state manager: {e}")

    # Reconstruct FIFO basis store from durable accounting_events so REPAY and
    # PT_REDEEM attribution is correct after a runner restart (VIB-3484).
    # Gate on state_manager_ready: an uninitialized backend returns [] silently,
    # which would leave the FIFO store empty — the exact restart hole VIB-3484 fixes.
    # Shared helper (VIB-3944) so the ``--once``/``test-lifecycle`` CLI paths,
    # which bypass run_loop entirely, can reuse the same rebuild step.
    if state_manager_ready:
        reconstruct_lending_basis_store(runner, strategy, strategy_id)

    # VIB-4086 — hydrate the runner's ``_recent_open_events`` cache from
    # disk so a process-restart between OPEN and CLOSE preserves
    # lifecycle continuity. Without this, an LP_OPEN written in process A
    # then closed in process B (the canonical operator-restart-mid-
    # position scenario AND the harness's ``--once`` pattern) lands the
    # CLOSE row with empty token0/token1/value_usd — the carry-forward
    # path in ``_apply_lp_close_columns`` has no in-memory bracket to
    # carry forward. Shared with VIB-4085's lending lifecycle: a SUPPLY
    # after a process restart correctly emits INCREASE rather than a
    # second OPEN when the prior open leg is on disk.
    if state_manager_ready:
        await hydrate_recent_open_events_cache(runner, strategy)

    # VIB-4198 / T12 — registry-mode cutover boot guard + registry-lookup
    # install. Both extracted into ``_run_cutover_boot_guard`` so they don't
    # contribute to ``initialize_run_loop``'s already-D-rated cyclomatic
    # complexity. See that helper's docstring for the contract.
    if state_manager_ready:
        await _run_cutover_boot_guard(runner, strategy, strategy_id)

    # VIB-3467: drain pending/failed outbox rows from the previous run.
    if runner.config.enable_state_persistence and state_manager_ready:
        try:
            processor = getattr(runner, "_accounting_processor", None)
            if processor is not None:
                deployment_id = getattr(strategy, "deployment_id", "") or strategy_id
                processor._deployment_id = deployment_id
                drained = await processor.drain_pending()
                if drained:
                    logger.info("AccountingProcessor: drained %d pending outbox rows on startup", drained)
        except Exception as e:
            if runner._is_live_mode():
                raise RuntimeError(f"AccountingProcessor.drain_pending failed: {e}") from e
            logger.warning("AccountingProcessor.drain_pending failed on startup: %s", e)

    # Recover incomplete sessions from previous runs
    try:
        recovered = await runner._recover_incomplete_sessions()
        if recovered > 0:
            logger.info(f"Recovered {recovered} incomplete sessions on startup")
    except Exception as e:
        logger.error(f"Failed to recover incomplete sessions: {e}")

    # Restore copy trading cursor state if configured
    from .runner_models import StatefulActivityProviderProtocol

    activity_provider = cast(
        StatefulActivityProviderProtocol | None,
        getattr(strategy, "_wallet_activity_provider", None),
    )
    if activity_provider is not None and runner.config.enable_state_persistence:
        try:
            state = await runner.state_manager.load_state(strategy_id)
            if state is not None and "copy_trading_state" in state.state:
                activity_provider.set_state(state.state["copy_trading_state"])
                logger.info("Copy trading: cursor state restored from persistence")
        except Exception as e:
            logger.warning(f"Failed to restore copy trading state: {e}")

    runner._shutdown_requested = False
    runner._signal_received = False
    runner._terminal_lifecycle_state = None
    runner._terminal_lifecycle_error_message = None

    # Set up dual-write for timeline events (gateway persistence)
    gateway_client = runner._get_gateway_client()
    if gateway_client is not None:
        from ..api.timeline import set_event_gateway_client

        set_event_gateway_client(gateway_client)
        logger.debug("Enabled gateway dual-write for timeline events")

    # Register this strategy instance with the gateway
    runner._register_with_gateway(strategy)

    # Write RUNNING state to LifecycleStore
    runner._lifecycle_write_state(strategy_id, "RUNNING")

    # Emit strategy started event
    start_event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.STRATEGY_STARTED,
        description=f"Strategy {strategy_id} started with interval={interval}s",
        strategy_id=strategy_id,
        chain=getattr(runner.config, "chain", ""),
        details={
            "interval_seconds": interval,
            "enable_state_persistence": runner.config.enable_state_persistence,
        },
    )
    add_event(start_event)
    logger.debug(f"Emitted STRATEGY_STARTED event for {strategy_id}")

    return activity_provider


# =============================================================================
# Per-iteration helpers
# =============================================================================


def invoke_pre_iteration_callback(
    pre_iteration_callback: Callable[[], None] | None,
) -> None:
    """Invoke the user-supplied pre-iteration callback.

    Regular ``Exception`` subclasses are logged and swallowed so the loop
    continues with the iteration. ``CriticalCallbackError`` is re-raised
    by the caller's try/except so the loop exits (fail-closed).

    Note: ``CriticalCallbackError`` is NOT caught here — it is allowed to
    propagate. The caller's outer try/except handles it. We catch
    ``Exception`` (base class) and let ``CriticalCallbackError`` bypass
    this handler because ``CriticalCallbackError`` inherits from
    ``Exception``; the original code had a dedicated ``except
    CriticalCallbackError`` clause before the generic ``except Exception``.
    """
    if pre_iteration_callback is None:
        return

    # Local import to avoid circular dependency at module load time.
    from .runner_models import CriticalCallbackError

    try:
        pre_iteration_callback()
    except CriticalCallbackError:
        # Fail-closed: safety-critical callbacks stop the loop
        raise
    except Exception as e:
        logger.error(f"Pre-iteration callback error: {e}")


async def capture_snapshot_with_accounting(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
    result: IterationResult,
    iteration_start_monotonic: float | None = None,
) -> IterationResult:
    """Capture portfolio snapshot after an iteration, applying the
    live-mode ACCOUNTING_FAILED escalation contract.

    Returns the (possibly rebuilt) ``IterationResult``. In non-live mode
    or when persistence succeeds, the input ``result`` is returned
    unchanged. In live mode, a raised ``AccountingPersistenceError`` is
    converted into a fresh ``IterationResult`` with
    ``IterationStatus.ACCOUNTING_FAILED``. The rebuilt result's
    ``duration_ms`` covers the FULL wall-clock window from iteration
    start through the snapshot phase that failed (issue #1782 -- the
    #1770 fix preserved the iteration-body duration but still excluded
    the snapshot-phase time, undercounting ``duration_ms`` by the
    wall-clock spent in the post-iteration snapshot that actually
    failed). The caller passes ``iteration_start_monotonic``, captured
    at the top of the iteration body via ``time.monotonic()``; we
    compute ``duration_ms`` from that anchor at the moment of failure.
    If the caller doesn't supply the anchor we fall back to
    ``result.duration_ms`` (iteration-body only) -- this preserves the
    post-#1770 behaviour for any external caller that hasn't been
    updated, but all in-tree callers pass the anchor. The rebuilt
    result also carries over ``result.intent``,
    ``result.execution_result``, and ``result.balance_reconciliation``
    so operators retain the on-chain tx hash, gas metrics, and
    reconciliation context that preceded the accounting failure. The
    helper bypasses ``_create_error_result`` to avoid a redundant
    ``_total_iterations`` bump -- ``run_iteration`` already counted
    this iteration via ``_record_success`` before the snapshot phase
    ran.
    """
    # Local import to avoid circular dependency at module load time.
    from .runner_models import IterationResult, IterationStatus

    if not runner.config.enable_state_persistence:
        return result

    try:
        await runner._capture_portfolio_snapshot(
            strategy=strategy,
            iteration_number=runner._total_iterations,
        )
    except AccountingPersistenceError as acc_err:
        # Mode-aware: only escalate to ACCOUNTING_FAILED in
        # live mode, matching the contract used by
        # _write_ledger_entry (live raises, paper/dry-run
        # logs). In non-live modes, swallow + ERROR-log so
        # pre-prod drift is visible without halting the loop.
        if runner._is_live_mode():
            # Capture the failure timestamp BEFORE any alert / logging
            # side effects so ``duration_ms`` reflects only the
            # iteration-body + snapshot-phase wall-clock. The alert
            # hook (``_alert_accounting_failure``) performs network
            # I/O for Slack / PagerDuty and can add noticeable
            # latency; including that latency in the reported
            # duration would skew operator dashboards and misrepresent
            # the cost of the iteration + snapshot work that actually
            # failed (Gemini / Codex review of PR #1786).
            #
            # Report the FULL wall-clock cost of the failed
            # iteration, including the snapshot phase that
            # actually failed. Issue #1770 fixed the obvious
            # undercount (snapshot-only duration); issue
            # #1782 finishes the job by also including the
            # snapshot-phase time that elapsed between
            # ``run_iteration`` returning and
            # ``AccountingPersistenceError`` firing. When the
            # caller passes ``iteration_start_monotonic``
            # (the anchor captured at the top of the
            # iteration body), we measure from that anchor
            # through ``time.monotonic()`` now; otherwise we
            # fall back to ``result.duration_ms`` so an
            # external caller that hasn't been updated still
            # gets the #1770 behaviour.
            if iteration_start_monotonic is not None:
                duration_ms = (time.monotonic() - iteration_start_monotonic) * 1000.0
            else:
                duration_ms = result.duration_ms
            logger.exception(
                "Accounting persistence failed in live mode for %s (write_kind=%s)",
                strategy_id,
                acc_err.write_kind,
            )
            await runner._alert_accounting_failure(strategy, acc_err)
            # Forensic metadata (``intent``,
            # ``execution_result``, ``balance_reconciliation``)
            # is carried across unchanged: the iteration
            # succeeded on-chain and operators need the tx
            # hash, gas metrics, and reconciliation context
            # to diagnose what preceded the accounting
            # failure.
            #
            # We still build the result directly rather
            # than via ``_create_error_result`` -- that
            # helper, post fix #1771, no longer mutates
            # ``_consecutive_errors``, but it DOES still
            # bump ``_total_iterations`` which would
            # double-count this iteration (``run_iteration``
            # already counted it via ``_record_success``
            # before the snapshot phase ran).
            return IterationResult(
                status=IterationStatus.ACCOUNTING_FAILED,
                error=f"Accounting persistence failed ({acc_err.write_kind}): {acc_err}",
                strategy_id=strategy_id,
                duration_ms=duration_ms,
                intent=result.intent,
                execution_result=result.execution_result,
                balance_reconciliation=result.balance_reconciliation,
            )
        logger.error(
            "Snapshot accounting persistence failed in non-live mode for %s "
            "(write_kind=%s, continuing; pre-prod drift): %s",
            strategy_id,
            acc_err.write_kind,
            acc_err,
        )
    return result


def _portfolio_snapshot_to_price_oracle(snapshot: Any | None) -> dict | None:
    """Convert a PortfolioSnapshot.token_prices dict into the price_oracle
    shape consumed by ``build_ledger_entry``.

    PortfolioSnapshot stores prices keyed by ``chain:address`` with values
    ``{"price_usd": str, "symbol": str, "decimals": int|None}``. The ledger
    writer expects either a flat ``{symbol: usd}`` dict or the shaped
    ``{symbol: {price_usd, oracle_source, fetched_at, confidence}}`` form
    (Accounting-AttemptNo17 §1.2 G12).

    This converter emits the shaped form, stamps ``oracle_source="portfolio_valuer"``
    so auditors can grep "exposure by oracle" against teardown rows, and
    threads the snapshot timestamp through as ``fetched_at``. Confidence is
    inherited from the snapshot's ValueConfidence.

    Returns ``None`` when the snapshot is missing or has no token prices —
    callers fall back to the ``price_oracle=None`` path on
    ``_write_ledger_entry``, which leaves ``price_inputs_json=""`` (the
    pre-fix behaviour). That's preferable to fabricating a price.
    """
    if snapshot is None:
        return None
    token_prices = getattr(snapshot, "token_prices", None) or {}
    if not token_prices:
        return None

    # ValueConfidence -> Accountant Test confidence taxonomy
    confidence_attr = getattr(snapshot, "value_confidence", None)
    confidence_str = getattr(confidence_attr, "value", None) or getattr(confidence_attr, "name", None) or "ESTIMATED"
    confidence_str = str(confidence_str).upper()
    # Map "HIGH" through; collapse anything else to ESTIMATED so the
    # downstream confidence vocabulary stays bounded.
    if confidence_str not in {"HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"}:
        confidence_str = "ESTIMATED"

    timestamp = getattr(snapshot, "timestamp", None)
    fetched_at = timestamp.isoformat() if timestamp is not None and hasattr(timestamp, "isoformat") else ""

    oracle: dict[str, dict[str, Any]] = {}
    for _key, val in token_prices.items():
        if not isinstance(val, dict):
            continue
        symbol = val.get("symbol") or ""
        price_usd = val.get("price_usd")
        if not symbol or price_usd is None:
            continue
        # Last-write-wins on duplicate symbol across chains: teardown is
        # single-chain so this collision is rare, but stamp determinism.
        oracle[str(symbol)] = {
            "price_usd": str(price_usd),
            "oracle_source": "portfolio_valuer",
            "fetched_at": fetched_at,
            "confidence": confidence_str,
        }
    return oracle or None


async def _ensure_native_gas_in_teardown_oracle(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    oracle: dict | None,
) -> dict | None:
    """Top off the teardown price-oracle stash with the chain's native gas token.

    PortfolioSnapshot.token_prices is keyed by the assets the strategy holds —
    USDC + WETH for an LP, USDC + aUSDC for a lending loop, etc. The native
    gas symbol (``ETH`` on Arbitrum, ``MATIC`` on Polygon, …) is rarely a
    held asset, so it is absent from the snapshot. ``compute_gas_usd`` looks
    up the native symbol exactly and returns ``None`` when it is missing,
    leaving ``transaction_ledger.gas_usd`` empty for every teardown row.

    The iteration lane closes the same hole at
    :file:`strategy_runner.py:_build_single_chain_price_oracle` via
    ``market.price(native_symbol)``. The teardown lane has no per-iteration
    market, so we go through ``runner.price_oracle.get_aggregated_price``
    instead — same source, same gateway boundary.

    Best-effort: failure is logged at DEBUG and returns the oracle untouched
    (mirrors the iteration lane's silent-skip on its native pre-fetch).
    """
    if not oracle:
        return oracle
    chain = getattr(strategy, "chain", None) or getattr(runner.config, "chain", "")
    if not chain:
        return oracle

    from ..accounting.gas_pricing import native_token_for_chain

    native_symbol = native_token_for_chain(chain)
    if not native_symbol:
        return oracle
    if any(key in oracle for key in (native_symbol, native_symbol.upper(), native_symbol.lower())):
        return oracle

    price_oracle_obj = getattr(runner, "price_oracle", None)
    if price_oracle_obj is None or not hasattr(price_oracle_obj, "get_aggregated_price"):
        return oracle

    try:
        result = await price_oracle_obj.get_aggregated_price(native_symbol, "USD", chain=chain)
    except Exception as exc:  # noqa: BLE001 — best-effort top-off
        logger.debug(
            "teardown native-gas top-off failed for chain=%s symbol=%s: %s",
            chain,
            native_symbol,
            exc,
        )
        return oracle

    price = getattr(result, "price", None)
    if price is None:
        return oracle
    timestamp = getattr(result, "timestamp", None)
    fetched_at = timestamp.isoformat() if timestamp is not None and hasattr(timestamp, "isoformat") else ""
    confidence_attr = getattr(result, "confidence", None)
    confidence_str = str(confidence_attr or "ESTIMATED").upper()
    if confidence_str not in {"HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"}:
        confidence_str = "ESTIMATED"
    oracle[native_symbol] = {
        "price_usd": str(price),
        "oracle_source": getattr(result, "source", "") or "gateway",
        "fetched_at": fetched_at,
        "confidence": confidence_str,
    }
    return oracle


@dataclass(frozen=True)
class TeardownSnapshotOutcome:
    """Outcome of a single pre- or post-teardown snapshot bracket.

    See :func:`capture_teardown_snapshot_with_accounting` for the contract.
    Returned to the caller in lieu of raising on failure.

    Attributes
    ----------
    snapshot_captured:
        ``True`` iff a non-throttled, non-skipped snapshot was actually
        persisted. Cosmetic — used for assertions in tests; the absence of
        a snapshot in normal operation is rarely worth alerting on (e.g.
        ``enable_state_persistence=False`` returns ``False`` here).
    accounting_degraded:
        ``True`` iff a writer failure occurred. The caller (Phase 3 wiring)
        bumps the TeardownResult's degraded counter on this signal.
    degraded_reason:
        Compact summary of the failure for log lines and TeardownResult
        context. ``None`` when ``accounting_degraded`` is ``False``.
    phase:
        ``"pre"`` for the bracket call before the first teardown intent,
        ``"post"`` for the bracket after the last intent. Echoed back so
        callers can route both invocations through the same code without
        bookkeeping.
    """

    snapshot_captured: bool
    accounting_degraded: bool
    degraded_reason: str | None
    phase: str


async def capture_teardown_snapshot_with_accounting(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    *,
    teardown_cycle_id: str,
    pre_teardown: bool,
) -> TeardownSnapshotOutcome:
    """Bracket a teardown with snapshot + metrics writes (VIB-3773 Phase 2).

    Teardown twin of :func:`capture_snapshot_with_accounting`. Differs in
    two important ways:

    1. **Never raises.** The teardown lane diverges from VIB-3762's halt-
       on-write-failure contract because halting mid-unwind would strand a
       partially-closed position. Failures are captured into the returned
       :class:`TeardownSnapshotOutcome` and recorded in the deferred-write
       log. The teardown loop continues either way.
    2. **Stamps the cycle id on both surfaces** (P1-4 — see
       :file:`runner_state.py:486` which prefers ``runner._last_cycle_id``
       over the contextvar). For the duration of the snapshot capture we
       set both to ``teardown_cycle_id`` and restore them in ``finally``,
       so the resulting ``portfolio_snapshots`` / ``portfolio_metrics``
       rows carry the teardown's cycle id rather than the iteration's.

    The snapshot is forced (``force_snapshot=True``) — teardown brackets
    are always meaningful. The throttle that protects the iteration loop
    from a snapshot every cycle is irrelevant here.

    Parameters
    ----------
    runner:
        StrategyRunner instance owning the state manager + valuer.
    strategy:
        Strategy being torn down.
    teardown_cycle_id:
        Stable cycle id (e.g. ``f"teardown-{teardown_id}"``). Stamped onto
        both ``runner._last_cycle_id`` and the cycle-id contextvar.
    pre_teardown:
        ``True`` for the bracket call before the first teardown intent;
        ``False`` for the post-teardown bracket. Echoed back via
        :class:`TeardownSnapshotOutcome.phase`.
    """
    from ..accounting.deferred_log import append_now as _deferred_append_now
    from ..observability.context import (
        clear_cycle_id,
        get_cycle_id,
        set_cycle_id,
    )
    from .runner_state import capture_portfolio_snapshot

    phase = "pre" if pre_teardown else "post"

    if not runner.config.enable_state_persistence:
        # Persistence disabled — nothing to write, nothing to degrade.
        return TeardownSnapshotOutcome(
            snapshot_captured=False,
            accounting_degraded=False,
            degraded_reason=None,
            phase=phase,
        )

    saved_ctx_cycle_id = get_cycle_id()
    saved_last_cycle_id = getattr(runner, "_last_cycle_id", "") or ""

    set_cycle_id(teardown_cycle_id)
    runner._last_cycle_id = teardown_cycle_id

    snapshot_captured = False
    accounting_degraded = False
    degraded_reason: str | None = None

    deployment_id = getattr(strategy, "deployment_id", "") or getattr(strategy, "strategy_id", "") or ""
    strategy_id = getattr(strategy, "strategy_id", "") or ""

    def _append_deferred_safely(*, error: str, extra: dict[str, str]) -> None:
        """Wrap the deferred-log append so even *its* failure cannot raise.

        VIB-3773 contract: teardown's snapshot bracket is degraded-but-
        continue. The deferred-write log is the durable backstop for
        failed accounting writes — but the backstop itself can fail
        (disk full, permission error, race on log rotation). If we let
        that bubble up, the unwind halts mid-flight, which is exactly
        the silent-failure shape we are eliminating. Log the secondary
        failure at ERROR (operators still need to know the durable
        backstop dropped a record) and continue.
        """
        try:
            _deferred_append_now(
                kind="snapshot",
                strategy_id=strategy_id,
                deployment_id=deployment_id,
                cycle_id=teardown_cycle_id,
                error=error,
                extra=extra,
            )
        except Exception:  # noqa: BLE001 — never propagate
            logger.exception(
                "capture_teardown_snapshot_with_accounting[%s]: deferred-write log "
                "append failed for %s; original error=%s; continuing teardown",
                phase,
                strategy_id,
                error,
            )

    try:
        try:
            snapshot = await capture_portfolio_snapshot(
                runner,
                strategy,
                iteration_number=runner._total_iterations,
                force_snapshot=True,
            )
            snapshot_captured = snapshot is not None
            # G12 wiring: stash the per-cycle price oracle for the teardown
            # commit pipeline. ``commit_teardown_intent`` reads
            # ``runner._teardown_price_oracle`` and threads it into
            # ``_write_ledger_entry`` so every teardown row carries
            # ``price_inputs_json``. Set on the pre-bracket; cleared in the
            # post-bracket below so an iteration after teardown never sees
            # stale teardown prices.
            if pre_teardown:
                runner._teardown_price_oracle = _portfolio_snapshot_to_price_oracle(snapshot)
                runner._teardown_price_oracle = await _ensure_native_gas_in_teardown_oracle(
                    runner, strategy, runner._teardown_price_oracle
                )
            elif not getattr(runner, "_teardown_price_oracle", None):
                # Fallback: pre-snapshot failed but post produced prices.
                # Better to record post-teardown prices than to leave the
                # row's price_inputs_json empty.
                runner._teardown_price_oracle = _portfolio_snapshot_to_price_oracle(snapshot)
                runner._teardown_price_oracle = await _ensure_native_gas_in_teardown_oracle(
                    runner, strategy, runner._teardown_price_oracle
                )
        except AccountingPersistenceError as acc_err:
            accounting_degraded = True
            degraded_reason = (
                f"snapshot/{phase}: AccountingPersistenceError (write_kind={acc_err.write_kind}): {acc_err}"
            )
            logger.error(
                "capture_teardown_snapshot_with_accounting[%s]: persistence failed for %s "
                "(write_kind=%s) — recording deferred + continuing teardown: %s",
                phase,
                strategy_id,
                acc_err.write_kind,
                acc_err,
            )
            _append_deferred_safely(
                error=str(acc_err),
                extra={"phase": phase, "write_kind": str(acc_err.write_kind)},
            )
        except Exception as exc:  # noqa: BLE001 — never propagate
            accounting_degraded = True
            degraded_reason = f"snapshot/{phase}: {type(exc).__name__}: {exc}"
            logger.error(
                "capture_teardown_snapshot_with_accounting[%s]: snapshot capture failed for %s: %s",
                phase,
                strategy_id,
                exc,
                exc_info=True,
            )
            _append_deferred_safely(
                error=str(exc),
                extra={"phase": phase},
            )
    finally:
        if saved_ctx_cycle_id is None:
            clear_cycle_id()
        else:
            set_cycle_id(saved_ctx_cycle_id)
        runner._last_cycle_id = saved_last_cycle_id
        # G12 teardown stash lifecycle (Accounting-AttemptNo17 §A4): the
        # post-bracket clears the stash so the next iteration's lane
        # never reads teardown prices that may be stale by then. If the
        # post-bracket itself failed, the stash still lived through every
        # commit_teardown_intent call (where it matters), so clearing
        # here is safe regardless of accounting_degraded.
        if not pre_teardown:
            runner._teardown_price_oracle = None

    return TeardownSnapshotOutcome(
        snapshot_captured=snapshot_captured,
        accounting_degraded=accounting_degraded,
        degraded_reason=degraded_reason,
        phase=phase,
    )


async def handle_iteration_failure(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
    result: IterationResult,
) -> None:
    """Post-iteration bookkeeping for the failure branch.

    Increments ``_consecutive_errors``, records the first-error timestamp,
    records the failure on the circuit breaker (skipping statuses that
    were already recorded inline to avoid double-counting), maybe
    triggers emergency stop if the breaker just tripped OPEN, and emits
    the max-consecutive-errors alert + ERROR lifecycle write when the
    streak threshold is reached.

    Ownership of ``_consecutive_errors`` (fix for issue #1771): this
    helper is the SOLE owner of the consecutive-error streak counter for
    iteration results. ``StrategyRunner._create_error_result`` does NOT
    increment it. The one remaining increment outside this helper lives
    in ``run_loop``'s outer ``except Exception`` clause, which handles
    raised (as opposed to returned-as-result) errors -- a separate flow
    that never reaches ``run_iteration``'s result path and therefore
    never touches this helper.
    """
    from .runner_models import IterationStatus

    runner._consecutive_errors += 1
    if runner._first_error_at is None:
        runner._first_error_at = datetime.now(UTC)

    # Record failure in circuit breaker (skip statuses that already
    # recorded inline to avoid double-counting)
    if runner._circuit_breaker is not None and result.status not in (
        IterationStatus.CIRCUIT_BREAKER_OPEN,
        IterationStatus.STRATEGY_TIMEOUT,  # already recorded in decide() handler
        IterationStatus.STRATEGY_ERROR,  # already recorded in decide() handler
    ):
        runner._circuit_breaker.record_failure(
            error_message=result.error or f"Iteration failed: {result.status.value}",
        )

    # Auto-trigger emergency stop if breaker just tripped to OPEN
    # (checked after both inline and run_loop recording paths)
    if runner._circuit_breaker is not None:
        await runner._maybe_trigger_emergency(strategy, result)

    if runner._consecutive_errors >= runner.config.max_consecutive_errors:
        await runner._alert_consecutive_errors(strategy, result)
        runner._lifecycle_write_state(strategy_id, "ERROR", error_message=str(result.error) if result.error else None)


def handle_iteration_success(
    runner: StrategyRunner,
    strategy_id: str,
    was_in_error_streak: bool,
) -> None:
    """Post-iteration bookkeeping for the success branch.

    Recovers lifecycle state if the runner was previously in an error
    streak and is not about to shut down / transition to a terminal
    state. Then resets the consecutive-error counter, clears the
    first-error timestamp, and resets the emergency trigger guard when
    the circuit breaker is not OPEN.
    """
    # Recover lifecycle state if we were in an error streak before
    # this iteration succeeded. The counter has already been reset to
    # 0 inside `run_iteration` via `_record_success`, so we rely on the
    # pre-iteration snapshot captured above. Skip the recovery write
    # when the same iteration has already transitioned to a terminal
    # state (e.g., teardown writes TERMINATED and requests shutdown) --
    # otherwise we would clobber that terminal state with RUNNING.
    if was_in_error_streak and not runner._shutdown_requested and runner._terminal_lifecycle_state is None:
        runner._lifecycle_write_state(strategy_id, "RUNNING")
        logger.info(
            "Strategy %s recovered after error streak (max_consecutive_errors=%d) - lifecycle state reset to RUNNING",
            strategy_id,
            runner.config.max_consecutive_errors,
        )
    elif was_in_error_streak:
        logger.debug(
            "Skipping lifecycle recovery write for %s: shutdown/terminal state active",
            strategy_id,
        )
    runner._consecutive_errors = 0
    runner._first_error_at = None
    # Reset emergency guard so a future HALF_OPEN->OPEN relapse can re-fire
    if runner._circuit_breaker is not None:
        from ..execution.circuit_breaker import CircuitBreakerState

        if runner._circuit_breaker.state != CircuitBreakerState.OPEN:
            runner._emergency_triggered_for_open = False


async def handle_lifecycle_command(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
    command: str | None,
) -> None:
    """Route a polled lifecycle command (STOP / PAUSE / RESUME).

    ``STOP``: delegate to ``_lifecycle_handle_stop`` (which will call
    ``request_shutdown`` itself — the outer loop drains on the next
    iteration).

    ``PAUSE``: write PAUSED state, send a last position heartbeat, then
    enter an inner loop that sends heartbeats at
    ``config.lifecycle_poll_interval`` while waiting for ``RESUME``.
    ``STOP`` received during pause also calls ``_lifecycle_handle_stop``
    and exits the inner loop. ``_shutdown_requested`` (set externally by
    signal handler) also exits the inner loop.
    """
    if command == "STOP":
        logger.info("Received STOP command for %s", strategy_id)
        runner._lifecycle_handle_stop(strategy_id, strategy)
        return

    if command != "PAUSE":
        return

    logger.info("Received PAUSE command for %s", strategy_id)
    runner._lifecycle_write_state(strategy_id, "PAUSED")
    runner._gateway_update_status(strategy_id, "PAUSED")
    # Preserve position snapshot so the dashboard doesn't lose it during pause
    runner._gateway_heartbeat(strategy_id, positions=runner._collect_position_snapshot(strategy))
    # Wait for RESUME command (send heartbeats so operator sees liveness)
    while not runner._shutdown_requested:
        runner._lifecycle_heartbeat(strategy_id)
        resume_cmd = runner._lifecycle_poll_command(strategy_id)
        if resume_cmd == "RESUME":
            logger.info("Received RESUME command for %s", strategy_id)
            runner._lifecycle_write_state(strategy_id, "RUNNING")
            runner._gateway_update_status(strategy_id, "RUNNING")
            runner._gateway_heartbeat(strategy_id, positions=runner._collect_position_snapshot(strategy))
            break
        if resume_cmd == "STOP":
            logger.info("Received STOP command while paused for %s", strategy_id)
            runner._lifecycle_handle_stop(strategy_id, strategy)
            break
        await asyncio.sleep(runner.config.lifecycle_poll_interval)


# =============================================================================
# Post-loop finalization
# =============================================================================


async def finalize_run_loop(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
) -> None:
    """Run the teardown block after the ``while`` loop exits.

    Mirrors the original teardown block at ~line 1681-1720: final
    lifecycle write (preserving any ERROR set by the circuit breaker),
    gateway deregistration, STRATEGY_STOPPED timeline event, strategy
    ``flush_pending_saves`` (if provided), and the state manager
    ``close``.
    """
    # Write final state to LifecycleStore (preserve ERROR if set by circuit breaker)
    runner._lifecycle_write_state(
        strategy_id,
        runner._terminal_lifecycle_state or "TERMINATED",
        error_message=runner._terminal_lifecycle_error_message,
    )

    # Deregister from gateway (mark as INACTIVE)
    runner._deregister_from_gateway(strategy_id)

    # Emit strategy stopped event
    stop_event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.STRATEGY_STOPPED,
        description=f"Strategy {strategy_id} stopped",
        strategy_id=strategy_id,
        chain=getattr(runner.config, "chain", ""),
        details={
            "shutdown_requested": runner._shutdown_requested,
            "consecutive_errors": runner._consecutive_errors,
        },
    )
    add_event(stop_event)
    logger.debug(f"Emitted STRATEGY_STOPPED event for {strategy_id}")

    logger.info(f"Run loop ended for strategy {strategy_id}")

    # Flush any pending state saves before cleanup
    if hasattr(strategy, "flush_pending_saves"):
        try:
            await strategy.flush_pending_saves()
        except Exception as e:
            logger.warning(f"Error flushing pending saves: {e}")

    # Drain any in-flight accounting tasks before closing the state manager.
    # The strong-ref set (self._pending_drain_tasks) prevents GC, but the tasks
    # must complete before state_manager.close() so drain_one doesn't write to a
    # closed backend.  5 s timeout: if tasks are still running after that, cancel
    # them and log a warning rather than blocking shutdown indefinitely.
    pending_tasks: set[asyncio.Task[bool]] = getattr(runner, "_pending_drain_tasks", set())
    if pending_tasks:
        try:
            done, pending = await asyncio.wait(list(pending_tasks), timeout=5.0)
            if pending:
                logger.warning(
                    "Shutdown: %d accounting drain task(s) did not complete in 5 s, cancelling",
                    len(pending),
                )
                for task in pending:
                    task.cancel()
        except Exception as e:
            logger.warning("Error waiting for accounting drain tasks: %s", e)

    # Cleanup
    if runner.config.enable_state_persistence:
        try:
            await runner.state_manager.close()
        except Exception as e:
            logger.error(f"Error closing state manager: {e}")


# =============================================================================
# Registry-lookup installer (VIB-4198 / T12)
# =============================================================================


async def _run_cutover_boot_guard(
    runner: StrategyRunner,
    strategy: Any,
    strategy_id: str,
) -> None:
    """Run the VIB-4198 / T12 registry-mode cutover boot guard.

    For every primitive whose cutover is active for this build, the
    runner refuses to enter the iteration loop until the per-primitive
    backfill from ``position_events`` is complete. The guard runs the
    backfill inline on first call (cutover spec §2.2 outcome (b)). On
    success, the registry is the authoritative answer to "is this
    open?" for the cutover-flipped slice; on failure, the runner exits
    non-zero and the operator restarts.

    T12 only flips UniV3 LP. Future cutover tickets (T16 / T23 / T28)
    add their own primitives to ``ACTIVE_CUTOVERS`` and ride this
    same loop without further code change.

    Acceptance #6 — once the cutover is cleared, install a registry-
    first NFT-id lookup on the strategy's ``LPPositionTracker`` so
    ``LP_CLOSE`` intent injection (and the teardown lifecycle) read
    ``token_id`` from ``position_registry`` instead of the in-memory
    tracker. The tracker stays as shadow per blueprint 28 §5.

    Audit M2 (CodeRabbit): fail closed if the lookup install fails
    while the cutover is ACTIVE. Post-cutover the registry is the
    source of truth — silently swallowing an install failure would
    downgrade LP_CLOSE token-id resolution back to the in-memory
    tracker, which is exactly the surface that loses preexisting-LP
    state across a restart. The cutover spec D3.F6 silent-error class.

    - cutover ACTIVE + install fails → raise
      ``RegistryLookupInstallError`` (runner halts loud).
    - cutover NOT active (controlled-degrade path on a backend that
      doesn't support migration_state — boot guard already degraded
      via :class:`CutoverStorageNotSupported`) → log + continue;
      tracker fallback is the legacy behavior and is correct.

    Extracted from ``initialize_run_loop`` (CRAP refactor, round 9)
    so the cutover block doesn't push the run-loop initialiser past
    its CRAP-gate threshold. Full design intent is preserved one-to-
    one — this is a structural extraction, not a redesign.
    """
    from almanak.framework.migration import RegistryLookupInstallError
    from almanak.framework.primitives.types import Primitive
    from almanak.framework.runner.cutover import (
        ACTIVE_CUTOVERS,
        enforce_or_run_cutover,
        is_cutover_active,
    )
    from almanak.framework.strategies.lp_position_tracker import (
        LPPositionTracker,
    )

    deployment_id = getattr(strategy, "deployment_id", "") or strategy_id
    for cutover_spec in ACTIVE_CUTOVERS:
        await enforce_or_run_cutover(
            runner=runner,
            deployment_id=deployment_id,
            primitive=cutover_spec.primitive,
            cutover_key=cutover_spec.cutover_key,
        )

    tracker = getattr(strategy, "_lp_position_tracker", None)
    if not isinstance(tracker, LPPositionTracker):
        return

    cutover_live = is_cutover_active(runner, Primitive.LP, "lp")
    try:
        await _install_registry_lookup_for_lp_tracker(runner, tracker, deployment_id)
    except RegistryLookupInstallError:
        # Already-structured error — propagate AS-IS so the runner's
        # outer error handler halts the strategy.
        raise
    except Exception as exc:  # noqa: BLE001 — convert to structured error
        if cutover_live:
            raise RegistryLookupInstallError(
                deployment_id=deployment_id,
                primitive=Primitive.LP,
                cutover_key="lp",
                cause=f"{type(exc).__name__}: {exc}",
            ) from exc
        # Cutover not active — tracker fallback is the legacy contract.
        logger.warning(
            "Could not install registry lookup on LPPositionTracker "
            "(non-fatal — cutover not active for this build): %s",
            exc,
        )


async def _install_registry_lookup_for_lp_tracker(
    runner: StrategyRunner,
    tracker: Any,
    deployment_id: str,
) -> None:
    """Install a sync registry-lookup callback on ``tracker`` and prime the
    registry-id cache before returning.

    The lookup signature is ``(protocol, chain, pool) -> str | None`` — sync
    by design because :meth:`LPPositionTracker.maybe_inject` runs inside the
    runner's intent-extraction step (sync). We snapshot the registry state
    once at boot via the state-manager surface and resolve the
    (protocol, chain, pool) → token_id map in O(1).

    Audit P2 (CodeRabbit): the prime is awaited HERE rather than fired
    off as a task. The previous fire-and-forget left a race where the
    first post-restart LP_CLOSE / LP_COLLECT_FEES could run before the
    cache was populated.

    Audit M2 (CodeRabbit): the prime failure surface is now mode-aware.
    When the cutover is ACTIVE for ``(Primitive.LP, "lp")``, a prime
    failure raises :class:`RegistryLookupInstallError` so the runner
    halts loud — silent fallback to the in-memory tracker is exactly
    the D3.F6 silent-error class the cutover spec prohibits. When the
    cutover is NOT active (graceful-degrade path on a backend that
    doesn't support migration_state), the prime failure is informational
    — tracker fallback is the legacy / correct behavior.

    Refreshes after boot ride on the registry-mode write site
    (``_maybe_save_ledger_with_registry``) which keeps the cache in sync
    as a side effect. Cache-driven rather than per-injection async I/O
    so we don't introduce a per-intent state-manager call.
    """
    from almanak.framework.migration import RegistryLookupInstallError
    from almanak.framework.primitives.types import Primitive
    from almanak.framework.runner.cutover import is_cutover_active

    runner_ref = runner

    def _sync_lookup(*, protocol: str, chain: str, pool: str) -> str | None:
        try:
            # Ask the runner for the registry rows. The cache here is the
            # state manager's; calling the async accessor isn't possible
            # in a sync hook, so we consult the runner's
            # ``_lp_registry_id_cache`` (populated at boot via
            # ``_refresh_lp_registry_id_cache`` and refreshed on every
            # registry-mode write).
            cache: dict[tuple[str, str, str], str] = getattr(runner_ref, "_lp_registry_id_cache", {})
            return cache.get((protocol.lower(), chain.lower(), pool.lower()))
        except Exception:  # noqa: BLE001 — defensive
            return None

    tracker.attach_registry_lookup(_sync_lookup)
    # Prime the cache once at boot.
    runner_ref._lp_registry_id_cache = {}
    try:
        await _refresh_lp_registry_id_cache(runner_ref, deployment_id)
    except Exception as exc:  # noqa: BLE001
        if is_cutover_active(runner_ref, Primitive.LP, "lp"):
            raise RegistryLookupInstallError(
                deployment_id=deployment_id,
                primitive=Primitive.LP,
                cutover_key="lp",
                cause=f"prime failed: {type(exc).__name__}: {exc}",
            ) from exc
        logger.warning(
            "Initial registry-id cache prime failed (non-fatal — cutover "
            "not active for this build); tracker fallback active: %s",
            exc,
        )


# UniV3-family protocol slugs the registry-id cache indexes under.
# The registry doesn't carry ``protocol`` directly — UniV3 LP rows
# are tagged primitive='lp', accounting_category='lp', and the NPM
# address in the payload identifies the family. We index under every
# UniV3 family slug so a strategy registered as ``uniswap_v3`` /
# ``sushiswap_v3`` / etc. on the same NFT manager finds the same row.
_UNIV3_FAMILY_PROTOCOL_SLUGS: tuple[str, ...] = (
    "uniswap_v3",
    "sushiswap_v3",
    "pancakeswap_v3",
    "aerodrome_slipstream",
    "velodrome_slipstream",
)


def _index_lp_registry_row_into_cache(
    *,
    row: dict[str, Any],
    cache: dict[tuple[str, str, str], str],
    ambiguous: set[tuple[str, str, str]],
) -> None:
    """Index one OPEN ``position_registry`` row into the runner's
    registry-id cache (in place).

    Audit P2 (CodeRabbit): the cache key
    ``(protocol, chain, pool_address)`` is the same shape
    :class:`LPPositionTracker._PositionKey` uses, so multiple open
    NFTs in the same pool collide on it. Using last-write-wins would
    let teardown target an arbitrary token_id when a delta-neutral
    hedge or any other multi-NFT-per-pool strategy is active. We
    detect ambiguity here and DROP the cache entry so the sync lookup
    returns ``None`` and the in-memory tracker fallback fires (legacy
    behavior preserved). The registry's authoritative read is still
    ``get_position_registry_open_rows`` keyed on
    ``physical_identity_hash``; this cache is the legacy projection
    for the tracker-injection compatibility shim. A complete fix
    plumbs a per-position discriminator (registry_handle) through the
    LPPositionTracker contract — out of scope for T12 cutover.

    No-op when:
    - ``row.payload`` is not a dict (corrupt payload).
    - ``token_id`` or ``pool_address`` is missing / falsy.
    """
    payload = row.get("payload") or {}
    if not isinstance(payload, dict):
        return
    token_id = payload.get("token_id")
    pool = payload.get("pool_address")
    if not token_id or not pool:
        return
    chain = (row.get("chain") or "").lower()
    pool_lower = str(pool).lower()
    token_str = str(token_id)
    for protocol_slug in _UNIV3_FAMILY_PROTOCOL_SLUGS:
        key = (protocol_slug, chain, pool_lower)
        if key in ambiguous:
            continue
        existing = cache.get(key)
        if existing is not None and existing != token_str:
            # Multi-NFT-per-pool collision detected — mark ambiguous
            # so subsequent rows don't last-write-wins. Drop the entry
            # so the lookup returns None (tracker fallback).
            ambiguous.add(key)
            cache.pop(key, None)
            logger.warning(
                "LP registry-id cache: multiple OPEN NFTs in same pool "
                "(protocol=%s chain=%s pool=%s); cache entry dropped, "
                "tracker fallback active. Multi-NFT-per-pool injection "
                "needs a per-position discriminator (follow-up).",
                protocol_slug,
                chain,
                pool,
            )
        else:
            cache[key] = token_str


async def _refresh_lp_registry_id_cache(runner: StrategyRunner, deployment_id: str) -> None:
    """Repopulate ``runner._lp_registry_id_cache`` from
    ``position_registry``.

    Indexed by ``(protocol, chain, pool_address)`` keys — the same lookup
    triple the legacy in-memory tracker uses. Only OPEN rows. The cache
    is a per-runner instance attribute; the runner's snapshot is
    independent of any concurrent strategy that might happen to share
    the chain (though the 1:1 strategy-gateway invariant precludes
    that).

    Audit M2 (CodeRabbit): when the cutover is active, a refresh
    failure is a hard error — the tracker shadow has lost preexisting
    state across the restart. Re-raise so the installer can surface
    ``RegistryLookupInstallError``. When the cutover is not active
    (controlled-degrade path), debug-log + return so the boot
    continues with an empty cache (no harm — registry-mode dispatch
    is OFF on that path anyway).

    Per-row indexing lives in :func:`_index_lp_registry_row_into_cache`
    so the orchestrator stays narrow (cc <=4) and each indexing
    branch is unit-testable in isolation.
    """
    from almanak.framework.primitives.types import Primitive
    from almanak.framework.runner.cutover import is_cutover_active

    try:
        rows = await runner.state_manager.get_position_registry_open_rows(
            deployment_id,
            primitive="lp",
            accounting_category="lp",
        )
    except Exception as exc:  # noqa: BLE001
        if is_cutover_active(runner, Primitive.LP, "lp"):
            # Re-raise — the installer wraps this as
            # ``RegistryLookupInstallError`` so the runner halts loud.
            raise
        logger.debug(
            "Registry-id cache refresh failed for %s (non-fatal — cutover not active): %s",
            deployment_id,
            exc,
        )
        return
    cache: dict[tuple[str, str, str], str] = {}
    ambiguous: set[tuple[str, str, str]] = set()
    for row in rows:
        _index_lp_registry_row_into_cache(row=row, cache=cache, ambiguous=ambiguous)
    runner._lp_registry_id_cache = cache
