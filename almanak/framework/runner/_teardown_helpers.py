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
from decimal import Decimal, InvalidOperation
from pathlib import Path as _Path
from typing import TYPE_CHECKING, Any

from almanak.core.lifecycle import LifecycleState

from ..teardown.decision_log import TeardownDecisionPhase, log_teardown_decision
from ..teardown.models import CLOSURE_UNKNOWN_ERROR

if TYPE_CHECKING:
    from ..teardown import TeardownMode
    from ..teardown.models import TeardownResult, TeardownState
    from .runner_models import IterationResult, StrategyProtocol

# Share runner_teardown's logger so teardown events stay on one operator stream.
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


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
        # Additive registry reconciliation preserves cut-over LP positions across
        # restarts; unsupported backends and other primitives retain strategy enumeration.
        positions = await resolve_open_positions_with_registry(strategy)
    except Exception as pos_err:
        strategy_hook = f"{type(strategy).__module__}.{type(strategy).__qualname__}.get_open_positions()"
        if not runner.config.allow_unsafe_teardown_fallback:
            error_msg = (
                f"Strategy hook {strategy_hook} failed while fetching positions for safety validation "
                f"for {deployment_id}: {type(pos_err).__name__}: {pos_err}. "
                f"Inline fallback is disabled (allow_unsafe_teardown_fallback=False)."
            )
            logger.exception(error_msg)
            if request:
                from .runner_teardown import _safe_mark

                _safe_mark(state_manager, "mark_failed", deployment_id, error=error_msg)
            runner._request_teardown_failure_shutdown(error_msg)
            return None, runner._create_error_result(
                deployment_id, IterationStatus.STRATEGY_ERROR, error_msg, start_time
            )
        logger.warning(
            f"Strategy hook {strategy_hook} failed while fetching positions for safety validation — "
            f"falling back to inline teardown for {deployment_id} (unsafe fallback enabled): "
            f"{type(pos_err).__name__}: {pos_err}",
            exc_info=True,
        )
        fallback_result = await runner._execute_teardown_inline(
            strategy, teardown_intents, teardown_market, start_time, request, state_manager
        )
        return None, fallback_result

    return positions, None


def _teardown_config_from_request(request: Any | None, strategy: Any | None = None) -> Any:
    """Build the :class:`TeardownConfig` for a teardown run from the operator's
    ``TeardownRequest`` (VIB-5011).

    Pre-fix, ``build_teardown_manager`` passed no ``config=`` — the request's
    ``asset_policy`` / ``target_token`` were persisted but never reached the
    manager, so the token-consolidation phase had no configuration to act on.

    ``request=None`` (strategy self-signalled / risk-guard teardown) →
    consolidation DISABLED, close-only.
    """
    from ..teardown.config import (
        DEFAULT_MIN_SWAP_VALUE_USD,
        ChainConsolidationConfig,
        TeardownConfig,
        TokenConsolidationConfig,
    )
    from ..teardown.models import TARGET_TOKEN_CHAIN_DEFAULT, TeardownAssetPolicy

    configured = None
    if strategy is not None:
        get_config = getattr(strategy, "get_config", None)
        if callable(get_config):
            configured = get_config("teardown", None)
    if isinstance(configured, dict):
        # Malformed strategy config must not block risk-reducing teardown; use
        # production defaults instead.
        try:
            cfg = TeardownConfig.from_dict(dict(configured))
        except Exception as exc:
            logger.warning(
                "Strategy teardown config is malformed (%s) — ignoring it and using "
                "TeardownConfig.default(); the production $%s dust floor applies.",
                exc,
                DEFAULT_MIN_SWAP_VALUE_USD,
            )
            cfg = TeardownConfig.default()
    else:
        cfg = TeardownConfig.default()

    # Explicit null nested configs survive parsing, but downstream code requires
    # concrete consolidation settings.
    if not isinstance(cfg.token_consolidation, TokenConsolidationConfig):
        cfg.token_consolidation = TokenConsolidationConfig()
    if not isinstance(cfg.chain_consolidation, ChainConsolidationConfig):
        cfg.chain_consolidation = ChainConsolidationConfig()

    # Parsing does not range-check the dust floor. Reject negative or non-finite
    # values so malformed config cannot drive real consolidation swaps.
    raw_floor = cfg.token_consolidation.min_swap_value_usd
    try:
        floor = Decimal(str(raw_floor))
        floor_valid = floor.is_finite() and floor >= 0
    except (InvalidOperation, ValueError, TypeError):
        floor_valid = False
    if not floor_valid:
        logger.warning(
            "Strategy teardown min_swap_value_usd=%r is not a finite non-negative "
            "number — ignoring it and using the production $%s dust floor.",
            raw_floor,
            DEFAULT_MIN_SWAP_VALUE_USD,
        )
        cfg.token_consolidation.min_swap_value_usd = DEFAULT_MIN_SWAP_VALUE_USD

    if request is None:
        # Consolidation sweeps wallet-wide token balances, so only an explicit
        # operator request grants consent. Self-signalled teardowns stay close-only,
        # including cross-chain consolidation.
        cfg.token_consolidation.enabled = False
        cfg.chain_consolidation.enabled = False
        logger.info(
            "Teardown has no operator request — token + chain consolidation disabled "
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
    # Preserve the sentinel until consolidation, where chain context makes target
    # resolution authoritative.
    target_token = getattr(request, "target_token", None) or TARGET_TOKEN_CHAIN_DEFAULT

    cfg.asset_policy = asset_policy
    cfg.target_token = target_token
    cfg.token_consolidation.target_token = target_token
    # The request's asset policy is operator consent; strategy config controls the
    # dust floor but cannot veto consolidation. KEEP_OUTPUTS and emergency policy
    # skip the phase downstream.
    cfg.token_consolidation.enabled = True
    return cfg


def build_teardown_manager(
    runner: Any, compiler: Any, state_manager: Any, request: Any | None = None, strategy: Any | None = None
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
        config=_teardown_config_from_request(request, strategy=strategy),
        runner_helpers=build_runner_helpers(runner),
    )
    return teardown_mgr, teardown_state_adapter


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

    teardown_state.status = TeardownStatus.EXECUTING
    if teardown_mgr.state_manager:
        await teardown_mgr.state_manager.save_teardown_state(teardown_state)
    return teardown_state, None


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
            _entry_label,
            _entry_sort_key,
            _required_token_chain_entries,
            _warm_pt_yt_prices,
        )

        chain: str | None = getattr(strategy, "chain", None) or getattr(teardown_market, "chain", None)
        required_entries = set(_required_token_chain_entries(teardown_intents, chain))
        symbol_chains: dict[str, set[str | None]] = {}
        for symbol, token_chain in required_entries:
            symbol_chains.setdefault(symbol, set()).add(token_chain)
        duplicate_symbols = {symbol for symbol, chains in symbol_chains.items() if len(chains) > 1}
        pt_priced_ok: set[tuple[str, str | None]] = set()
        pt_warm_errors: dict[tuple[str, str | None], str] = {}
        _warm_pt_yt_prices(
            teardown_market,
            required_entries,
            price_oracle,
            pt_priced_ok,
            pt_warm_errors,
            duplicate_symbols,
        )
        if pt_priced_ok:
            logger.info(
                "Teardown runner: PT/YT prices warmed into oracle: %s",
                [_entry_label(entry) for entry in sorted(pt_priced_ok, key=_entry_sort_key)],
            )
        if pt_warm_errors:
            logger.warning(
                "Teardown runner: PT/YT price warm errors (best-effort): %s",
                {_entry_label(entry): reason for entry, reason in pt_warm_errors.items()},
            )
    except Exception as _pt_warm_exc:  # noqa: BLE001
        logger.warning(
            "Teardown runner: PT/YT oracle warmup failed (best-effort, continuing): %s",
            _pt_warm_exc,
        )


def _resolve_closure_refusal(
    verification: Any, *, deployment_id: str, verify_error_msg: str | None
) -> tuple[str | None, bool]:
    """Decide whether closure verification refuses to certify, and why.

    Returns ``(verify_error_msg, must_refuse)``. Extracted from
    ``execute_and_verify`` unchanged — behaviour-preserving, and the caller's
    single ``if must_refuse:`` replaces the two predicates that previously read
    the same two flags in two places.

    VIB-6285 (W0.1): a teardown that measured nothing must not certify, but the
    reason is a SEPARATE branch from ``all_closed`` on purpose.
    ``all_closed=False`` asserts residual on-chain risk; ``closure_unknown``
    asserts only that closure was not proven. The two must never share a
    message — conflating them is the VIB-6198 false-failure class, which tells
    an operator their money is still exposed when the only established fact is
    an absence of proof.

    Ordering is load-bearing: the measured residual is checked first so the
    actionable, louder signal wins the error slot when both are true.
    """
    if not verification.all_closed:
        if verify_error_msg is None:
            verify_error_msg = "Post-teardown verification failed: positions still open. Manual check required."
        logger.warning(f"Post-teardown verification: {deployment_id} incomplete. Marking as failed.")
    elif verification.closure_unknown:
        verify_error_msg = CLOSURE_UNKNOWN_ERROR
        logger.warning(
            "Post-teardown verification: %s closure is UNPROVEN for protocol(s) %s (no "
            "measured on-chain evidence either way). Refusing to certify success — this is "
            "NOT a claim that positions are open. Terminal: re-run teardown manually after "
            "verifying on-chain.",
            deployment_id,
            ", ".join(verification.unproven_protocols) or "<unnamed>",
        )
    return verify_error_msg, (not verification.all_closed or verification.closure_unknown)


def closure_chain_evidence(verification: Any) -> dict[str, Any]:
    """Serialize what the CHAIN measured about closure, for consumers outside this lane (ALM-3109).

    ``ClosureVerification`` is composed here from three independent chain signals
    (TD-14 post-condition hooks, the TD-15 POST-teardown Plan-A re-read, the TD-08
    PRE-teardown report) and then discarded — only ``verification_status`` and the
    two counters survive onto ``TeardownResult``, and the POST-teardown
    ``ReconciliationReport`` itself is a local inside
    ``TeardownManager.verify_closure_against_chain``. Callers that need to know
    whether the chain PROVED closure — as opposed to whether the teardown merely
    finished — had nothing to read, which is how ``strat test`` came to publish
    the strategy's own position cache as if it were a chain measurement
    (ALM-3109).

    Emitted as a plain JSON-serializable dict rather than the dataclass so it can
    ride the ``strat test --json`` artifact unchanged. ``closure_unknown`` and
    ``unproven_protocols`` are derived properties, materialized here so a consumer
    never has to re-derive the VIB-6285 rule (and get it subtly wrong).
    """
    return {
        "verification_status": verification.verification_status.value,
        "all_closed": bool(verification.all_closed),
        "closure_unknown": bool(verification.closure_unknown),
        "has_position_breakdown": bool(verification.has_position_breakdown),
        "positions_total": int(verification.positions_total),
        "positions_closed": int(verification.positions_closed),
        "protocols_to_prove": list(verification.protocols_to_prove),
        "measured_closed_protocols": list(verification.measured_closed_protocols),
        "unproven_protocols": list(verification.unproven_protocols),
    }


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
    *,
    resume_accepted_async: bool = False,
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
    from ..teardown.single_close_guard import collapse_duplicate_perp_closes
    from ..teardown.teardown_manager import _teardown_wallet_for_chain
    from .runner_teardown import _make_approval_callback, _safe_mark

    deployment_id = strategy.deployment_id

    # Dispatch one close per physical perp position, but run coverage against the
    # original plan so omitted duplicates still prove the position was accounted for.
    _single_close = collapse_duplicate_perp_closes(teardown_intents)
    _coverage_intents = _single_close.for_coverage
    teardown_intents = _single_close.dispatch

    # Manual slippage escalation requires operator consent through the state
    # adapter; never silently downgrade when that channel is unavailable.
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

    # Warm before the price guard; failures leave its fail-closed behavior intact.
    _warm_teardown_pt_yt_prices(strategy, teardown_market, teardown_intents, price_oracle)

    if resume_accepted_async:
        teardown_result = await teardown_mgr.resume(
            deployment_id,
            strategy,
            on_approval_needed=approval_callback,
            market=teardown_market,
            is_auto_mode=is_auto_mode,
            accepted_async_recovery_intents=teardown_intents,
        )
        if teardown_result is None:
            raise RuntimeError("Accepted async teardown state disappeared before production resume")
    else:
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

    if teardown_result.success:
        verify_error_msg: str | None = None
        try:
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

        # Re-read known positions after execution and only lower confidence;
        # pre-teardown reconciliation prevents stale enumeration from certifying closure.
        verification = await teardown_mgr.verify_closure_against_chain(
            strategy,
            verification=verification,
            pre_execution_positions=positions,
            market=teardown_market,
            pre_teardown_reconciliation=getattr(runner, "_teardown_reconciliation", None),
        )

        # Fold coverage after chain verification so any uncovered known-open
        # position remains the final fail-closed verdict. Use the original plan
        # and the same chain and wallet identity as enumeration.
        completeness = check_intent_coverage(
            positions,
            _coverage_intents,
            consolidation_target_token=teardown_mgr._consolidation_noop_target(strategy, teardown_intents),
            wallet_for_chain=lambda c: _teardown_wallet_for_chain(strategy, c) or None,
        )
        if not completeness.complete:
            uncovered_count = len(completeness.uncovered)
            # Include uncovered positions in the denominator and cap the closed
            # count so a failed teardown cannot persist zero failed positions.
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

        # Only measured position breakdowns replace intent-based fallback counts;
        # propagate verification confidence from the same snapshot.
        teardown_result = replace(
            teardown_result,
            positions_total=verification.positions_total,
            positions_closed=verification.positions_closed,
            has_position_breakdown=verification.has_position_breakdown,
            verification_status=verification.verification_status,
        )

        # Publish only the final verdict after all downgrade gates. The caller
        # resets this field each run, and failure paths leave it unset so prior
        # evidence cannot certify the current teardown.
        runner._teardown_closure_verification = closure_chain_evidence(verification)

        # Match the CLI lane's auditable closure-confidence record.
        log_teardown_decision(
            deployment_id=deployment_id,
            teardown_id=teardown_state.teardown_id,
            phase=TeardownDecisionPhase.VERIFY,
            # Unmeasured closure is neither verified nor evidence of an open position.
            outcome=(
                "verify_failed"
                if not verification.all_closed
                else ("verify_unmeasured" if verification.closure_unknown else "verified")
            ),
            description=(
                f"closure verification: {verification.positions_closed}/"
                f"{verification.positions_total} closed "
                f"({verification.verification_status.value}"
                f"{', closure_unknown' if verification.closure_unknown else ''})"
            ),
            position_count=verification.positions_total,
            positions_closed=verification.positions_closed,
            verification_status=verification.verification_status.value,
        )

        verify_error_msg, must_refuse = _resolve_closure_refusal(
            verification, deployment_id=deployment_id, verify_error_msg=verify_error_msg
        )

        if must_refuse:
            teardown_result = replace(
                teardown_result,
                success=False,
                error=verify_error_msg,
                recovery_options=["Verify positions on-chain", "Re-run teardown"],
            )
            # Execution may already have persisted COMPLETED; overwrite it so
            # durable state reflects the verification failure.
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
                # Persist counts while the request is still active; the later
                # terminal mark is idempotent. Fall back to intent counts when no
                # position breakdown was measured.
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

    # Consolidate only after closure verification. A consolidation failure warns
    # without reopening risk that was already closed successfully.
    if teardown_result.success:
        from ..teardown.consolidation import fold_consolidation_outcome
        from ..teardown.models import TeardownPhase

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

    # Keep execution-state persistence aligned with the failed request when both
    # rows exist.
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

    if teardown_result.completed_at is None and teardown_result.async_settlement_pending:
        # The async order remains live; keep the request and runner active so a
        # correlated resume reuses it instead of resubmitting.
        logger.warning(
            "🛑 %s teardown is awaiting terminal async settlement; keeping request active for correlated resume",
            deployment_id,
        )
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            error=teardown_result.error,
            deployment_id=deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    if teardown_result.success:
        logger.info(
            f"🛑 {deployment_id} teardown complete via TeardownManager "
            f"({teardown_result.intents_executed}/{teardown_result.intents_total} intents executed, "
            f"{teardown_result.duration_seconds:.1f}s)"
        )
        if teardown_result.intents_skipped > 0:
            logger.warning(
                "%s teardown execution: %d planned intent(s) skipped; "
                "no transaction submitted for the skipped intent(s)",
                deployment_id,
                teardown_result.intents_skipped,
            )
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
        if teardown_result.consolidation_skipped > 0:
            logger.warning(
                "%s token consolidation: %d planned swap(s) skipped; "
                "no transaction submitted for the skipped swap(s). Warnings: %s",
                deployment_id,
                teardown_result.consolidation_skipped,
                "; ".join(teardown_result.consolidation_warnings) or "none",
            )
        elif (
            teardown_result.consolidation_failed == 0
            and teardown_result.consolidation_succeeded == 0
            and teardown_result.consolidation_warnings
        ):
            # Below-dust residuals may warn without attempting a swap; surface
            # them for unattended runs.
            logger.warning(
                "🛑 %s teardown completed with consolidation warnings (no swap failed): %s",
                deployment_id,
                "; ".join(teardown_result.consolidation_warnings) or "none",
            )
        # Unverified success is execution evidence, not chain proof; warn
        # unattended operators.
        if teardown_result.verification_status == VerificationStatus.UNVERIFIED:
            # Avoid misleading 0/0 counts when no position breakdown was measured.
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
        runner._lifecycle_write_state(deployment_id, LifecycleState.TERMINATED)
        if request:
            # Use measured position counts when available; skipped intent totals
            # cannot distinguish already-flat from stranded positions.
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
                    "verification_status": teardown_result.verification_status.value,
                    "intents": teardown_result.intents_succeeded,  # compatibility alias
                    "intents_succeeded": teardown_result.intents_succeeded,
                    "intents_skipped": teardown_result.intents_skipped,
                    "intents_executed": teardown_result.intents_executed,
                    "intents_total": teardown_result.intents_total,
                    "mode": mode_str,
                    "duration_s": teardown_result.duration_seconds,
                    "consolidation": {
                        "planned": teardown_result.consolidation_planned,
                        "succeeded": teardown_result.consolidation_succeeded,
                        "skipped": teardown_result.consolidation_skipped,
                        "failed": teardown_result.consolidation_failed,
                        "warnings": list(teardown_result.consolidation_warnings),
                        # Record the resolved target, not the request sentinel.
                        # None means no target was used.
                        "target_token": teardown_result.consolidation_target,
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
    # An unverified failure can mean absence of proof rather than residual risk;
    # keep that distinction visible to unattended operators.
    if teardown_result.verification_status in (VerificationStatus.UNVERIFIED, VerificationStatus.NOT_RUN):
        logger.warning(
            "🛑 %s teardown closure was NOT chain-confirmed (verification_status=%s). If the error "
            "above is the unproven-closure reason, this is an ABSENCE of proof, NOT evidence that "
            "positions are open — verify on-chain before acting.",
            deployment_id,
            teardown_result.verification_status.value,
        )
    if request:
        if teardown_result.has_position_breakdown:
            closed = teardown_result.positions_closed
            failed = max(teardown_result.positions_total - closed, 0)
        else:
            # If verification never ran, preserve intent-landing evidence for
            # postmortems instead of reporting an unmeasured 0/0.
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
