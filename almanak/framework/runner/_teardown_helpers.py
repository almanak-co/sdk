"""Phase helpers for :func:`execute_teardown_via_manager` (Phase 6A.4).

This module holds the phase-level helpers extracted from
``execute_teardown_via_manager`` to reduce cyclomatic complexity and isolate
responsibilities. Every helper preserves the EXACT original behavior captured
by the characterization tests in
``tests/unit/runner/test_teardown_flow.py::TestExecuteTeardownViaManagerCharacterization``.

Design notes
------------
* Helpers are module-level functions that take the runner instance explicitly
  so ``execute_teardown_via_manager`` reads as a clean sequencer without
  ``self.`` noise.
* The outer ``try/except`` in ``execute_teardown_via_manager`` is preserved in
  the caller because its ``except`` branch needs access to locals
  (``teardown_state``, ``teardown_state_adapter``) from the execute/verify
  phase. Moving the ``try`` into a helper would break that semantic.
* Log messages, error strings, and ``state_manager.mark_*`` ordering are
  reproduced byte-for-byte from the pre-extraction body, including the
  double ``mark_failed`` call on the verify-fail path (pinned by char tests).
* Module uses ``TYPE_CHECKING`` to avoid circular imports at load time.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path as _Path
from typing import TYPE_CHECKING, Any

from ..teardown.decision_log import TeardownDecisionPhase, log_teardown_decision

if TYPE_CHECKING:
    from ..teardown import TeardownMode
    from ..teardown.models import TeardownResult, TeardownState
    from .runner_models import IterationResult, StrategyProtocol

# Use the same logger as runner_teardown so existing log-capture tests keep
# working after extraction.
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# =============================================================================
# Phase 1: Compiler + positions resolution (with fallback branches)
# =============================================================================


async def resolve_compiler_or_fallback(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_market: Any | None,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> tuple[Any | None, IterationResult | None]:
    """Build the teardown compiler. If compiler build fails, either return an
    early STRATEGY_ERROR (when ``allow_unsafe_teardown_fallback=False``) or
    delegate to the inline fallback path.

    Returns
    -------
    (compiler, early_result):
        - ``compiler`` is truthy and ``early_result`` is ``None`` on success.
        - ``compiler`` is ``None`` when the caller should return
          ``early_result`` immediately.
    """
    from .runner_models import IterationStatus

    deployment_id = strategy.deployment_id

    # Call through runner method so instance-level mock patching in tests works.
    compiler = runner._build_teardown_compiler(strategy, teardown_market)
    if compiler is not None:
        return compiler, None

    if not runner.config.allow_unsafe_teardown_fallback:
        error_msg = (
            f"Cannot build TeardownManager compiler for {deployment_id}. "
            f"Inline fallback is disabled (allow_unsafe_teardown_fallback=False). "
            f"Fix compiler dependencies or enable fallback for local testing."
        )
        logger.error(error_msg)
        if request:
            from .runner_teardown import _safe_mark

            _safe_mark(state_manager, "mark_failed", deployment_id, error=error_msg)
        runner._request_teardown_failure_shutdown(error_msg)
        return None, runner._create_error_result(deployment_id, IterationStatus.STRATEGY_ERROR, error_msg, start_time)

    logger.warning(
        f"Cannot build compiler for TeardownManager — falling back to inline teardown "
        f"for {deployment_id} (unsafe fallback enabled)"
    )
    fallback_result = await runner._execute_teardown_inline(
        strategy, teardown_intents, teardown_market, start_time, request, state_manager
    )
    return None, fallback_result


async def fetch_positions_or_fallback(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_market: Any | None,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> tuple[Any | None, IterationResult | None]:
    """Get open positions for safety validation. If positions can't be
    fetched, either return an early STRATEGY_ERROR (when
    ``allow_unsafe_teardown_fallback=False``) or delegate to the inline
    fallback path. NOTE: passing an empty portfolio through safety validation
    is unsafe (loss cap of 3% of $0 = $0 passes trivially).

    Returns
    -------
    (positions, early_result):
        - ``positions`` is truthy and ``early_result`` is ``None`` on success.
        - ``positions`` is ``None`` when the caller should return
          ``early_result`` immediately.
    """
    from ..teardown.registry_enumeration import resolve_open_positions_with_registry
    from .runner_models import IterationStatus

    deployment_id = strategy.deployment_id
    try:
        # VIB-5459 / TD-01: reconcile the strategy's enumeration against the
        # position_registry WARM read path so safety validation + closure
        # verification see the cut-over LP set the registry still remembers
        # after a restart. Additive (never drops a strategy-reported position);
        # non-LP / non-cut-over primitives fall through to get_open_positions
        # unchanged, and the read degrades to legacy enumeration on a backend
        # without cutover storage.
        positions = await resolve_open_positions_with_registry(strategy)
    except Exception as pos_err:
        if not runner.config.allow_unsafe_teardown_fallback:
            error_msg = (
                f"Cannot fetch positions for safety validation for {deployment_id}: {pos_err}. "
                f"Inline fallback is disabled (allow_unsafe_teardown_fallback=False)."
            )
            logger.error(error_msg)
            if request:
                from .runner_teardown import _safe_mark

                _safe_mark(state_manager, "mark_failed", deployment_id, error=error_msg)
            runner._request_teardown_failure_shutdown(error_msg)
            return None, runner._create_error_result(
                deployment_id, IterationStatus.STRATEGY_ERROR, error_msg, start_time
            )
        logger.warning(
            f"Cannot fetch positions for safety validation — "
            f"falling back to inline teardown for {deployment_id} (unsafe fallback enabled): {pos_err}"
        )
        fallback_result = await runner._execute_teardown_inline(
            strategy, teardown_intents, teardown_market, start_time, request, state_manager
        )
        return None, fallback_result

    return positions, None


# =============================================================================
# Phase 2: TeardownManager construction
# =============================================================================


def _teardown_config_from_request(request: Any | None) -> Any:
    """Build the :class:`TeardownConfig` for a teardown run from the operator's
    ``TeardownRequest`` (VIB-5011).

    Pre-fix, ``build_teardown_manager`` passed no ``config=`` — the request's
    ``asset_policy`` / ``target_token`` were persisted but never reached the
    manager, so the token-consolidation phase had no configuration to act on.

    ``request=None`` (strategy self-signalled / risk-guard teardown) →
    consolidation DISABLED, close-only.
    """
    from ..teardown.config import TeardownConfig, TokenConsolidationConfig
    from ..teardown.models import TeardownAssetPolicy

    if request is None:
        # No operator request → NO token consolidation (pr-auditor blocker).
        # Consolidation swaps are wallet-scoped per token (``amount="all"``);
        # on a wallet shared across deployments that includes sibling
        # strategies' balances of the same token. An explicit TeardownRequest
        # carries the operator's asset policy and is the consent for that
        # sweep semantic (the same consent model as the long-standing
        # strategy-emitted ``amount="all"`` teardown sweeps, VIB-4587).
        # Self-signalled teardowns have no such consent — they keep the
        # pre-VIB-5011 close-only behaviour.
        cfg = TeardownConfig.default()
        cfg.token_consolidation.enabled = False
        logger.info(
            "Teardown has no operator request — token consolidation disabled "
            "(close-only); request a teardown with an asset policy to consolidate."
        )
        return cfg

    raw_policy = getattr(request, "asset_policy", None) or TeardownAssetPolicy.TARGET_TOKEN
    try:
        asset_policy = TeardownAssetPolicy(raw_policy)
    except ValueError:
        logger.warning(
            "Unknown teardown asset_policy %r on request — defaulting to target_token",
            raw_policy,
        )
        asset_policy = TeardownAssetPolicy.TARGET_TOKEN
    target_token = getattr(request, "target_token", None) or "USDC"

    return TeardownConfig(
        asset_policy=asset_policy,
        target_token=target_token,
        token_consolidation=TokenConsolidationConfig(target_token=target_token),
    )


def build_teardown_manager(
    runner: Any, compiler: Any, state_manager: Any, request: Any | None = None
) -> tuple[Any, Any | None]:
    """Instantiate the teardown state adapter and ``TeardownManager``.

    Runtime persistence is SQLite in local mode and gateway-backed in hosted
    mode. Returns the pair ``(teardown_manager, teardown_state_adapter)``.

    Prefer an explicit DB path from the StateManager when it's a real
    filesystem path (SQLite only).

    VIB-3773: builds a :class:`TeardownRunnerHelpers` bag and threads it
    into the manager so per-intent commit + pre/post snapshot bracket
    fire on the runner's full accounting pipeline. Without this, the
    teardown lane bypasses every accounting writer (the original April-29
    silent-failure class).

    VIB-5011: threads the operator's ``TeardownRequest`` (asset policy +
    target token) into the manager's ``TeardownConfig`` so the
    token-consolidation phase honours the request. ``request=None`` derives
    defaults (consolidate to USDC).
    """
    from ..teardown import create_teardown_state_adapter_for_runtime
    from ..teardown.runner_helpers import build_runner_helpers
    from ..teardown.teardown_manager import TeardownManager

    _raw_db_path = getattr(state_manager, "db_path", None)
    _adapter_db_path = _raw_db_path if isinstance(_raw_db_path, str | _Path) else None
    teardown_state_adapter = create_teardown_state_adapter_for_runtime(
        gateway_client=runner._get_gateway_client(),
        sqlite_path=_adapter_db_path,
    )

    teardown_mgr = TeardownManager(
        orchestrator=runner.execution_orchestrator,
        compiler=compiler,
        alert_manager=runner.alert_manager,
        state_manager=teardown_state_adapter,
        config=_teardown_config_from_request(request),
        runner_helpers=build_runner_helpers(runner),
    )
    return teardown_mgr, teardown_state_adapter


# =============================================================================
# Phase 3: Safety validation
# =============================================================================


def validate_safety_or_error(
    runner: Any,
    teardown_mgr: Any,
    strategy: StrategyProtocol,
    positions: Any,
    teardown_mode: TeardownMode,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> IterationResult | None:
    """Run safety validation. Returns ``None`` on pass, or a pre-built
    ``IterationResult(STRATEGY_ERROR)`` on fail. On fail, also emits the
    ``mark_failed`` row (if request present) and the teardown-failure
    shutdown side effect.
    """
    from .runner_models import IterationStatus

    deployment_id = strategy.deployment_id
    validation = teardown_mgr.safety_guard.validate_teardown_request(positions, teardown_mode)
    if validation.all_passed:
        return None

    logger.error(f"🛑 Teardown safety validation failed: {validation.blocked_reason}")
    if request:
        from .runner_teardown import _safe_mark

        _safe_mark(
            state_manager,
            "mark_failed",
            deployment_id,
            error=f"Safety validation failed: {validation.blocked_reason}",
        )
    runner._request_teardown_failure_shutdown(f"Teardown safety validation failed: {validation.blocked_reason}")
    return runner._create_error_result(
        deployment_id,
        IterationStatus.STRATEGY_ERROR,
        f"Teardown safety validation failed: {validation.blocked_reason}",
        start_time,
    )


# =============================================================================
# Phase 4: Cancel window + state persist
# =============================================================================


async def run_cancel_window_and_persist(
    runner: Any,
    teardown_mgr: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_mode: TeardownMode,
    is_auto_mode: bool,
    start_time: datetime,
) -> tuple[TeardownState | None, IterationResult | None]:
    """Persist state, run the cancel window, and — on non-cancelled flow —
    transition state to EXECUTING.

    Returns
    -------
    (teardown_state, cancel_result):
        - On non-cancelled: ``teardown_state`` is the EXECUTING state;
          ``cancel_result`` is ``None``.
        - On cancelled: ``teardown_state`` is ``None``; ``cancel_result`` is a
          pre-built TEARDOWN ``IterationResult`` the caller must return.
    """
    from ..teardown.models import TeardownStatus
    from .runner_models import IterationResult, IterationStatus

    deployment_id = strategy.deployment_id

    teardown_id = f"td_{uuid.uuid4().hex[:12]}"
    teardown_state = await teardown_mgr._persist_state(
        teardown_id=teardown_id,
        strategy=strategy,
        mode=teardown_mode,
        intents=teardown_intents,
    )

    # Run cancel window — gives operator time to abort
    cancel_result = await teardown_mgr.cancel_window.run_cancel_window(
        teardown_id=teardown_id,
        is_auto_mode=is_auto_mode,
    )
    if cancel_result.was_cancelled:
        logger.info(f"🛑 Teardown {teardown_id} cancelled during window")
        runner._record_success()
        short_circuit = IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            deployment_id=deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )
        return None, short_circuit

    # Update state to EXECUTING after cancel window
    teardown_state.status = TeardownStatus.EXECUTING
    if teardown_mgr.state_manager:
        await teardown_mgr.state_manager.save_teardown_state(teardown_state)
    return teardown_state, None


# =============================================================================
# Phase 5: Price oracle resolution (pure helper)
# =============================================================================


def resolve_price_oracle(teardown_market: Any | None) -> dict | None:
    """Extract a price oracle dict from the market snapshot, falling back to
    stablecoin defaults when the market is missing or returns an empty/None
    mapping. Note: an empty ``{}`` falls through to the fallback because
    ``if not price_oracle`` treats empty dicts as falsy. The fallback itself
    may return ``None`` if no stablecoins are resolvable — mirrors the
    pre-extraction behavior at the call site in ``execute_teardown_via_manager``.
    """
    from .runner_teardown import get_fallback_teardown_prices

    price_oracle: dict | None = None
    if teardown_market is not None and hasattr(teardown_market, "get_price_oracle_dict"):
        fetched = teardown_market.get_price_oracle_dict()
        price_oracle = fetched if fetched is not None else None
    if not price_oracle:
        price_oracle = get_fallback_teardown_prices(teardown_market)
    return price_oracle


# =============================================================================
# Phase 6: Execute intents + post-execution verify
# =============================================================================


def _warm_teardown_pt_yt_prices(
    strategy: Any,
    teardown_market: Any | None,
    teardown_intents: list,
    price_oracle: dict | None,
) -> None:
    """Warm Pendle PT/YT prices into the runner-supplied price oracle in place.

    VIB-5537: warm PT/YT prices into the runner-supplied price oracle before
    ``_execute_intents`` calls ``update_prices()`` + ``assert_prices_available()``
    (the VIB-2928 guard). The runner's Phase 6 oracle is populated by
    ``get_price_oracle_dict()`` which does NOT carry Pendle PT/YT prices — those
    require the dedicated GetPtPrice RPC (``market.pt_price``). Without this
    warmup the VIB-2928 guard always hard-stops a Pendle PT teardown SWAP.

    Placement: this runs at the runner teardown execution seam (Phase 6) rather
    than in ``resolve_price_oracle()`` (the synchronous Phase 6 helper) because
    that helper has no access to the teardown intent list. This is best-effort:
    a failure to warm only warns and lets ``_execute_intents`` handle the missing
    price (the guard fires loud) rather than silently discarding a recoverable
    teardown.

    Empty != Zero: ``_warm_pt_yt_prices`` only merges a real MEASURED price; an
    UNAVAILABLE / ``None`` / zero PT price is left absent so the guard still
    hard-stops on a genuinely unpriceable PT. Mutates ``price_oracle`` in place.
    No-op when ``teardown_market`` or ``price_oracle`` is ``None``.
    """
    if teardown_market is None or price_oracle is None:
        return
    try:
        from ..teardown.oracle_warmup import (
            _warm_pt_yt_prices,
            extract_required_token_chains,
        )

        chain: str | None = getattr(strategy, "chain", None) or getattr(teardown_market, "chain", None)
        token_chains = extract_required_token_chains(teardown_intents, chain)
        pt_priced_ok: set[str] = set()
        pt_warm_errors: dict[str, str] = {}
        _warm_pt_yt_prices(
            teardown_market,
            set(token_chains.keys()),
            token_chains,
            price_oracle,
            pt_priced_ok,
            pt_warm_errors,
        )
        if pt_priced_ok:
            logger.info(
                "Teardown runner: PT/YT prices warmed into oracle: %s",
                sorted(pt_priced_ok),
            )
        if pt_warm_errors:
            logger.warning(
                "Teardown runner: PT/YT price warm errors (best-effort): %s",
                pt_warm_errors,
            )
    except Exception as _pt_warm_exc:  # noqa: BLE001
        logger.warning(
            "Teardown runner: PT/YT oracle warmup failed (best-effort, continuing): %s",
            _pt_warm_exc,
        )


async def execute_and_verify(
    runner: Any,
    teardown_mgr: Any,
    teardown_state_adapter: Any,
    teardown_state: TeardownState,
    strategy: StrategyProtocol,
    teardown_intents: list,
    positions: Any,
    teardown_mode: TeardownMode,
    teardown_market: Any | None,
    is_auto_mode: bool,
    price_oracle: dict | None,
    request: Any | None,
    state_manager: Any,
) -> TeardownResult:
    """Run ``_execute_intents`` and the post-execution closure verification.

    Fail-closed (VIB-2925): if execution succeeded but positions remain,
    mark the ``TeardownResult`` as failed and persist ``TeardownStatus.FAILED``
    so the SQLite row reflects reality. Skip verification when execution
    already failed — the original error is more actionable than
    "positions still open".

    Verify exceptions are caught here so they don't discard successful
    on-chain execution stats in the ``TeardownResult``.

    Returns the (possibly-replaced) ``TeardownResult``. Does NOT handle
    alerts or cleanup — those are pipelined after this helper.
    """
    from ..teardown.completeness import check_intent_coverage
    from ..teardown.models import ClosureVerification, TeardownStatus, VerificationStatus
    from .runner_teardown import _make_approval_callback, _safe_mark

    deployment_id = strategy.deployment_id

    # Build approval callback for slippage escalation (VIB-2927).
    # Only wire for manual mode — auto mode uses hard slippage limits.
    # Both local SQLite and hosted Postgres adapters publish the
    # approval channel through the same Protocol (VIB-4049), so the
    # callback wires the same way in both modes.
    #
    # If a manual teardown reaches this point without an adapter, we must NOT
    # silently downgrade the request — slippage escalation has to be gated by
    # operator consent. Fail fast instead.
    approval_callback = None
    if not is_auto_mode:
        if teardown_state_adapter is None:
            raise RuntimeError(
                "Manual teardown requires a teardown state adapter for the operator "
                "approval channel — refusing to proceed without slippage-escalation gating. "
                "Check that the hosted gateway is reachable, or that the strategy folder "
                "is resolvable in local mode."
            )
        approval_callback = _make_approval_callback(runner, teardown_state_adapter)

    # VIB-5537: warm PT/YT prices into the runner-supplied price oracle before
    # _execute_intents fires the VIB-2928 price guard. See
    # _warm_teardown_pt_yt_prices for the rationale (best-effort, Empty != Zero).
    _warm_teardown_pt_yt_prices(strategy, teardown_market, teardown_intents, price_oracle)

    # Execute intents with escalating slippage
    teardown_result = await teardown_mgr._execute_intents(
        teardown_id=teardown_state.teardown_id,
        strategy=strategy,
        intents=teardown_intents,
        positions=positions,
        mode=teardown_mode,
        teardown_state=teardown_state,
        on_approval_needed=approval_callback,
        is_auto_mode=is_auto_mode,
        price_oracle=price_oracle,
        market=teardown_market,
    )

    # Post-execution verification: check positions are actually closed.
    if teardown_result.success:
        verify_error_msg: str | None = None
        try:
            # VIB-3742: thread the pre-execution ``positions`` snapshot so
            # protocol-specific on-chain post-condition checks can run for
            # each position the teardown should have closed.
            # VIB-5085: use the detailed verifier so lifecycle counters report
            # how many *positions* (not intents) actually closed.
            verification = await teardown_mgr._verify_closure_detailed(
                strategy,
                pre_execution_positions=positions,
                close_receipt_block=teardown_result.last_receipt_block,
            )
        except Exception as verify_err:
            logger.exception(
                "Post-teardown verification raised for %s — treating as verify-fail",
                deployment_id,
            )
            verification = ClosureVerification(
                all_closed=False,
                positions_total=len(getattr(positions, "positions", []) or []),
                positions_closed=0,
                has_position_breakdown=True,
                verification_status=VerificationStatus.FAILED,
            )
            verify_error_msg = f"Post-teardown verification error: {verify_err}. Manual check required."

        # TD-15 (VIB-5473): fail-closed on-chain POST-teardown verification. After
        # every closing intent has fired (risk reduction FIRST — inverted
        # semantics preserved), re-read the KNOWN position set on-chain. A
        # position the chain still reports OPEN flips the teardown to FAILED
        # (catching the hook-less lending strand the post-condition path counts
        # closed-by-execution); the PRE-teardown TD-08 reconciliation
        # (``runner._teardown_reconciliation``) folds in so a never-existed /
        # stale-enumeration closure is never certified CHAIN_VERIFIED. Composes
        # with TD-14's status (only ever lowers confidence), never raises.
        verification = await teardown_mgr.verify_closure_against_chain(
            strategy,
            verification=verification,
            pre_execution_positions=positions,
            market=teardown_market,
            pre_teardown_reconciliation=getattr(runner, "_teardown_reconciliation", None),
        )

        # TD-11 (VIB-5469): fold the pre-execution intent-coverage check into the
        # verification result. A KNOWN tracked-open position with no closing
        # intent must FAIL the teardown even when every emitted intent executed
        # and on-chain verification of the COVERED positions passed — the gap is
        # in coverage, not execution. Forcing FAILED routes it through the same
        # fail-closed persistence below (mark_failed + status=FAILED). Applied
        # AFTER the TD-15 chain re-read so the coverage gap is the final, loudest
        # word — an uncovered KNOWN position FAILs regardless of what the chain
        # says about the positions that DID get a closing intent.
        # VIB-5494 Item 1: thread the Phase-2 consolidation target so a held
        # STAKE/TOKEN position already denominated in the target (for which
        # full_close emits no swap) is credited a no-op close, not false-failed.
        completeness = check_intent_coverage(
            positions,
            teardown_intents,
            consolidation_target_token=teardown_mgr._consolidation_noop_target(),
        )
        if not completeness.complete:
            # The uncovered positions are definitively NOT closed (no intent even
            # targeted them), so cap positions_closed so the persisted
            # positions_failed = total - closed reflects them rather than reading
            # 0 failed on a FAILED teardown (VIB-5469).
            uncovered_count = len(completeness.uncovered)
            # Carry the uncovered positions into the denominator: if the
            # verifier had no position breakdown (positions_total=0), the
            # downstream mark_failed (positions_failed = total - closed) would
            # otherwise record 0 failed on a teardown that FAILED specifically
            # because known-open positions had no closing intent (VIB-5469).
            positions_total = max(verification.positions_total, completeness.total_enforceable)
            adjusted_closed = max(
                min(verification.positions_closed, positions_total - uncovered_count),
                0,
            )
            verification = replace(
                verification,
                all_closed=False,
                positions_total=positions_total,
                positions_closed=adjusted_closed,
                has_position_breakdown=True,
                verification_status=VerificationStatus.FAILED,
            )
            verify_error_msg = completeness.error_message()

        # VIB-5085: stamp the verified position counts onto the result so the
        # success result_json + the Phase-2 progress mark + any failure mark
        # source ``positions_closed`` from positions, not ``intents_succeeded``.
        # ``has_position_breakdown`` is only True when the verifier had a real
        # pre-execution snapshot — on the in-memory fallback (empty snapshot)
        # it stays False so callers fall back to the intent count rather than
        # persist a misleading ``positions_closed=0`` on a successful teardown.
        # VIB-2932 / VIB-5472: also stamp the verification confidence so the
        # lifecycle surface can flag an unverifiable closure.
        teardown_result = replace(
            teardown_result,
            positions_total=verification.positions_total,
            positions_closed=verification.positions_closed,
            has_position_breakdown=verification.has_position_breakdown,
            verification_status=verification.verification_status,
        )

        # VIB-5478: structured VERIFY decision entry (runner signal-driven lane).
        # Mirrors the CLI lane's entry in TeardownManager.execute so both lanes
        # produce the same auditable closure-confidence record (TD-14 + TD-15).
        log_teardown_decision(
            deployment_id=deployment_id,
            teardown_id=teardown_state.teardown_id,
            phase=TeardownDecisionPhase.VERIFY,
            outcome="verified" if verification.all_closed else "verify_failed",
            description=(
                f"closure verification: {verification.positions_closed}/"
                f"{verification.positions_total} closed "
                f"({verification.verification_status.value})"
            ),
            position_count=verification.positions_total,
            positions_closed=verification.positions_closed,
            verification_status=verification.verification_status.value,
        )

        if not verification.all_closed:
            if verify_error_msg is None:
                verify_error_msg = "Post-teardown verification failed: positions still open. Manual check required."
            logger.warning(f"Post-teardown verification: {deployment_id} incomplete. Marking as failed.")
            teardown_result = replace(
                teardown_result,
                success=False,
                error=verify_error_msg,
                recovery_options=["Verify positions on-chain", "Re-run teardown"],
            )
            # Persist the failure so the SQLite row reflects reality —
            # `_execute_intents` already set status=COMPLETED; flip it to
            # FAILED so a postmortem reader doesn't see a row claiming
            # success while the teardown actually failed. Hosted mode has
            # no SQLite adapter (build_teardown_manager returns None);
            # the platform owns teardown lifecycle tracking there.
            teardown_state.status = TeardownStatus.FAILED
            teardown_state.updated_at = datetime.now(UTC)
            if teardown_state_adapter is not None:
                try:
                    await teardown_state_adapter.save_teardown_state(teardown_state)
                except Exception:
                    logger.warning(
                        "Failed to persist FAILED status for teardown %s after verify-fail",
                        teardown_state.teardown_id,
                        exc_info=True,
                    )
            if request:
                # VIB-5085: this is the mark_failed that actually persists — the
                # row is still active here, so it must carry the breakdown. The
                # terminal mark_failed in ``map_teardown_result`` runs later
                # against an already-inactive row (get_active_request returns
                # None) and is a no-op for persistence; relying on it would drop
                # these counts (Codex). When the verifier had a real snapshot,
                # report positions; otherwise fall back to the intent-landing
                # signal rather than a misleading 0/0.
                if verification.has_position_breakdown:
                    _fail_closed = verification.positions_closed
                    _fail_failed = max(verification.positions_total - _fail_closed, 0)
                else:
                    _fail_closed = teardown_result.intents_succeeded or 0
                    _fail_failed = max((teardown_result.intents_total or 0) - _fail_closed, 0)
                _safe_mark(
                    state_manager,
                    "mark_failed",
                    deployment_id,
                    error=verify_error_msg,
                    positions_closed=_fail_closed,
                    positions_failed=_fail_failed,
                )

    # VIB-5011: token-consolidation phase (Phase 2) — runs ONLY when closure
    # AND verification both succeeded. This is the runner-lane hook; the CLI
    # execute lane gets the same phase from ``TeardownManager.execute`` Step
    # 7.5 (this lane calls ``_execute_intents`` directly, so the two hooks
    # never overlap for one teardown). Failure semantics are inverted by
    # contract: a failed consolidation swap keeps ``success=True`` — the
    # closure already removed on-chain risk — and surfaces via the
    # ``consolidation_*`` result fields + ``result_json["consolidation"]``.
    if teardown_result.success:
        from ..teardown.consolidation import fold_consolidation_outcome
        from ..teardown.models import TeardownPhase

        # VIB-5085: report verified positions closed, not intents landed.
        _safe_mark(
            state_manager,
            "update_progress",
            deployment_id,
            positions_closed=teardown_result.positions_closed,
            current_phase=TeardownPhase.TOKEN_CONSOLIDATION,
        )
        consolidation_outcome = await teardown_mgr.run_token_consolidation(
            strategy,
            teardown_id=teardown_state.teardown_id,
            teardown_state=teardown_state,
            mode=teardown_mode,
            market=teardown_market,
            price_oracle=price_oracle,
            positions=positions,
            closing_intents=teardown_intents,
            is_auto_mode=is_auto_mode,
            on_approval_needed=approval_callback,
        )
        teardown_result = fold_consolidation_outcome(teardown_result, consolidation_outcome)

    return teardown_result


async def send_alert_and_cleanup(teardown_mgr: Any, teardown_result: TeardownResult, teardown_id: str) -> None:
    """Send completion alert (on success) and clean up persisted state.

    Both operations are best-effort — exceptions are logged and swallowed.
    """
    if teardown_mgr.alert_manager and teardown_result.success:
        try:
            await teardown_mgr.alert_manager.send_teardown_complete(teardown_result)
        except Exception as alert_err:
            logger.warning(f"Failed to send teardown completion alert: {alert_err}")

    if teardown_mgr.state_manager and teardown_result.success:
        try:
            await teardown_mgr.state_manager.delete_teardown_state(teardown_id)
        except Exception as cleanup_err:
            logger.warning(f"Failed to clean up teardown state: {cleanup_err}")


# =============================================================================
# Phase 7: Exception-handler for the outer try
# =============================================================================


async def handle_executor_exception(
    runner: Any,
    strategy: StrategyProtocol,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
    teardown_state: TeardownState | None,
    teardown_state_adapter: Any | None,
    exc: Exception,
) -> IterationResult:
    """Side effects for the outer try/except in ``execute_teardown_via_manager``.

    Logs the error, marks the state-manager row as failed (if a request is
    present), reflects the failure in the ``TeardownStateAdapter`` row (best
    effort — the exception may have fired before the state row or adapter was
    initialized), and requests a teardown-failure shutdown. Returns a pre-
    built STRATEGY_ERROR ``IterationResult`` the caller must return.
    """
    from ..teardown.models import TeardownStatus as _TS
    from .runner_models import IterationStatus
    from .runner_teardown import _safe_mark

    deployment_id = strategy.deployment_id
    logger.error(f"🛑 TeardownManager execution failed for {deployment_id}: {exc}")
    if request:
        _safe_mark(state_manager, "mark_failed", deployment_id, error=str(exc))

    # Best effort: reflect the failure in the adapter's row so postmortem
    # readers don't see an EXECUTING teardown_execution_state row paired
    # with a FAILED teardown_requests row.
    try:
        if teardown_state is not None and teardown_state_adapter is not None:
            teardown_state.status = _TS.FAILED
            teardown_state.updated_at = datetime.now(UTC)
            await teardown_state_adapter.save_teardown_state(teardown_state)
    except Exception:
        logger.warning(
            "Failed to persist FAILED teardown_execution_state for %s after exception",
            deployment_id,
            exc_info=True,
        )
    runner._request_teardown_failure_shutdown(str(exc))
    return runner._create_error_result(deployment_id, IterationStatus.STRATEGY_ERROR, str(exc), start_time)


# =============================================================================
# Phase 8: Final TeardownResult -> IterationResult mapping
# =============================================================================


def map_teardown_result(
    runner: Any,
    strategy: StrategyProtocol,
    start_time: datetime,
    teardown_result: TeardownResult,
    teardown_mode: TeardownMode,
    request: Any | None,
    state_manager: Any,
) -> IterationResult:
    """Map a TeardownResult to the runner's IterationResult, firing the
    terminal side effects (shutdown, lifecycle write, mark_completed /
    mark_failed + teardown-failure shutdown).
    """
    from ..teardown import TeardownMode
    from ..teardown.models import VerificationStatus
    from .runner_models import IterationResult, IterationStatus
    from .runner_teardown import _safe_mark

    deployment_id = strategy.deployment_id
    mode_str = "graceful" if teardown_mode == TeardownMode.SOFT else "emergency"

    if teardown_result.success:
        logger.info(
            f"🛑 {deployment_id} teardown complete via TeardownManager "
            f"({teardown_result.intents_succeeded}/{teardown_result.intents_total} intents, "
            f"{teardown_result.duration_seconds:.1f}s)"
        )
        # VIB-5011: surface the token-consolidation summary. Consolidation
        # failure never flips success — log loud so the operator knows the
        # wallet holds residual non-target tokens.
        if teardown_result.consolidation_failed > 0:
            logger.warning(
                "🛑 %s teardown completed with consolidation warnings: "
                "%d of %d consolidation swap(s) failed — wallet holds residual tokens. Warnings: %s",
                deployment_id,
                teardown_result.consolidation_failed,
                teardown_result.consolidation_planned,
                "; ".join(teardown_result.consolidation_warnings) or "none",
            )
        elif teardown_result.consolidation_succeeded > 0:
            logger.info(
                "🛑 %s token consolidation: %d swap(s) executed",
                deployment_id,
                teardown_result.consolidation_succeeded,
            )
        elif teardown_result.consolidation_warnings:
            # VIB-5393 (Case A): a below-dust material residual produces
            # planned=succeeded=failed=0, so it hits neither branch above. On a
            # hosted run the passive operator surface IS this runner log — the
            # CLI `teardown status` / --wait render is only seen by someone who
            # actively polls. Surface the consolidation warnings loud here so a
            # stranded sub-floor residual (e.g. ~$4 WETH) is visible without an
            # operator sweep. Non-failure warnings only — the failure case is
            # handled by the first branch.
            logger.warning(
                "🛑 %s teardown completed with consolidation warnings (no swap failed): %s",
                deployment_id,
                "; ".join(teardown_result.consolidation_warnings) or "none",
            )
        # VIB-2932 / VIB-5472: surface the closure-verification confidence on the
        # passive operator log. An UNVERIFIED success means positions were closed
        # by execution but NOT chain-confirmed (no on-chain post-condition for the
        # protocol, or no pre-exec snapshot) — flag it loud so the count is never
        # read as proven. CHAIN_VERIFIED stays at info; FAILED never reaches this
        # success branch (it flips success=False upstream).
        if teardown_result.verification_status == VerificationStatus.UNVERIFIED:
            # Only quote the position counts when the verifier had a trustworthy
            # pre-exec breakdown — on the in-memory fallback they are 0/0 and
            # ``positions_closed`` falls back to the intent count for persistence,
            # so quoting "0/0 closed" here would be misleading (CodeRabbit).
            if teardown_result.has_position_breakdown:
                logger.warning(
                    "🛑 %s teardown closure UNVERIFIED: %d/%d position(s) reported closed "
                    "by execution but NOT chain-confirmed — verify on-chain before trusting the count.",
                    deployment_id,
                    teardown_result.positions_closed,
                    teardown_result.positions_total,
                )
            else:
                logger.warning(
                    "🛑 %s teardown closure UNVERIFIED: positions reported closed by execution "
                    "but NOT chain-confirmed (no trustworthy position breakdown) — verify on-chain.",
                    deployment_id,
                )
        runner.request_shutdown()
        runner._lifecycle_write_state(deployment_id, "TERMINATED")
        if request:
            # VIB-5085: ``positions_closed`` reports verified positions closed,
            # not intents landed. ``mark_completed`` lifts ``result["positions_closed"]``
            # onto the column (preferring it over the legacy ``result["intents"]``).
            # The intent signal is preserved alongside under intent-named keys.
            positions_closed_count = (
                teardown_result.positions_closed
                if teardown_result.has_position_breakdown
                else teardown_result.intents_succeeded
            )
            _safe_mark(
                state_manager,
                "mark_completed",
                deployment_id,
                result={
                    "positions_closed": positions_closed_count,
                    "positions_total": teardown_result.positions_total,
                    # VIB-2932 / VIB-5472: closure-verification confidence rides
                    # the existing result_json (no proto / schema change) so the
                    # CLI `status` / --wait surface can flag an unverifiable
                    # closure rather than present the count as chain-confirmed.
                    "verification_status": teardown_result.verification_status.value,
                    "intents": teardown_result.intents_succeeded,  # back-compat alias
                    "intents_succeeded": teardown_result.intents_succeeded,
                    "intents_total": teardown_result.intents_total,
                    "mode": mode_str,
                    "duration_s": teardown_result.duration_seconds,
                    # VIB-5011: consolidation summary for result_json — read
                    # back by the CLI --wait terminal print + `status`.
                    "consolidation": {
                        "planned": teardown_result.consolidation_planned,
                        "succeeded": teardown_result.consolidation_succeeded,
                        "failed": teardown_result.consolidation_failed,
                        "warnings": list(teardown_result.consolidation_warnings),
                        # The RESOLVED target (request may omit the field; the
                        # phase then consolidates into the USDC default) — the
                        # status surface must report what actually happened.
                        "target_token": getattr(request, "target_token", None) or "USDC",
                    },
                },
            )
        runner._record_success()
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            deployment_id=deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    logger.warning(f"🛑 {deployment_id} teardown incomplete via TeardownManager: {teardown_result.error}")
    if request:
        if teardown_result.has_position_breakdown:
            # VIB-5085: verification ran (e.g. a verify-fail flipped success to
            # failure) — report the position-level breakdown, not intents.
            closed = teardown_result.positions_closed
            failed = max(teardown_result.positions_total - closed, 0)
        else:
            # VIB-4542 fallback: execution failed before verification ran, so
            # there is no position-level breakdown. Preserve the intent-landing
            # signal ("6 of 7 landed") instead of "0 / 0" — better than nothing
            # for a postmortem reader. ``intents_total / intents_succeeded`` are
            # populated by ``_execute_intents`` on both partial-success and
            # full-failure terminal paths.
            closed = teardown_result.intents_succeeded or 0
            failed = max((teardown_result.intents_total or 0) - closed, 0)
        _safe_mark(
            state_manager,
            "mark_failed",
            deployment_id,
            error=teardown_result.error or "teardown failed",
            positions_closed=closed,
            positions_failed=failed,
        )
    runner._request_teardown_failure_shutdown(teardown_result.error or "teardown failed")
    return IterationResult(
        status=IterationStatus.STRATEGY_ERROR,
        error=teardown_result.error,
        deployment_id=deployment_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )
