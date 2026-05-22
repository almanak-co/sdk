"""Teardown lane commit pipeline — VIB-3773 Phase 0.

Per-intent twin of :py:meth:`StrategyRunner._single_chain_handle_success`'s
post-execution body. Runs the same four steps (enrich → ledger → outbox+fire
→ sidecar) **but with degraded-but-continue semantics**:

* The iteration lane raises :class:`AccountingPersistenceError` when any
  writer fails in live mode (VIB-3762 §C). That contract is correct for
  iteration — running the next iteration on broken accounting compounds
  the damage, so halting is safer than continuing.

* The teardown lane inverts the priority. Teardown's first job is to
  *remove on-chain risk*. If LP_CLOSE succeeds on-chain but the ledger
  write fails, halting now would strand a partially-unwound position
  (REPAY/WITHDRAW would never run). So the teardown commit pipeline
  **never raises**: failures are captured into a
  :class:`TeardownCommitOutcome`, recorded into the deferred-write log,
  and the next risk-reducing intent runs.

The deferred-write log (``almanak.framework.accounting.deferred_log``)
is the durable backstop. An operator (or a future
``almanak ax accounting reconcile`` CLI) replays those records into the
state store after the chain-side work is done.

Cycle-id handling
-----------------
``commit_teardown_intent`` stamps the cycle_id contextvar with the supplied
``teardown_cycle_id`` for the duration of its work. Phase 3 wiring in
``execute_teardown_via_manager`` also sets it at the outer level (paired with
``runner._last_cycle_id`` — see :file:`runner_state.py:486`); this helper
re-applies it locally so unit tests / direct callers don't have to.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..accounting.deferred_log import (
    DeferredWrite,
)
from ..accounting.deferred_log import (
    append as deferred_append,
)
from ..observability.context import get_cycle_id, set_cycle_id

if TYPE_CHECKING:  # pragma: no cover
    from ..intents.vocabulary import AnyIntent
    from .runner_models import StrategyProtocol
    from .strategy_runner import StrategyRunner

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TeardownCommitOutcome:
    """Outcome of the per-intent teardown commit pipeline.

    Returned by :func:`commit_teardown_intent` for every successful on-chain
    teardown intent. Carries the IDs the caller needs (ledger_entry_id) plus
    a structured degradation report for the TeardownManager loop and the
    eventual TeardownResult.

    Attributes
    ----------
    ledger_entry_id:
        The persisted ``LedgerEntry.id`` if the ledger write succeeded,
        otherwise ``None``. Outbox + accounting-event writes only fire when
        this is non-None (mirrors the iteration lane's gate at
        :file:`strategy_runner.py:2879`).
    accounting_degraded:
        ``True`` iff any of the four pipeline steps (enrich, ledger, outbox,
        sidecar) failed. Drives the TeardownResult's degraded flag and
        operator-visible alerting.
    degraded_reason:
        Compact summary of which steps failed, suitable for log lines and
        TeardownResult.error context. ``None`` when ``accounting_degraded``
        is ``False``.
    degraded_writes:
        The :class:`DeferredWrite` records the helper appended to the
        deferred-write log. Surfaced for tests and richer TeardownResult
        propagation; the TeardownManager loop only needs the count.
    """

    ledger_entry_id: str | None
    accounting_degraded: bool
    degraded_reason: str | None
    degraded_writes: tuple[DeferredWrite, ...] = field(default_factory=tuple)


def _intent_type_str(intent: Any) -> str | None:
    """Stringify an intent's ``intent_type`` field. Mirrors the runner's
    inline pattern at :file:`strategy_runner.py:1942`.
    """
    it = getattr(intent, "intent_type", None)
    if it is None:
        return None
    return it.value if hasattr(it, "value") else str(it)


def _first_tx_hash(result: Any) -> str | None:
    """First on-chain tx hash from a successful execution result, or None.

    Mirrors :func:`almanak.framework.observability.ledger._extract_tx_and_gas`
    so the deferred-log row uses the same value the ledger row would have
    used had the write succeeded.
    """
    if not result:
        return None
    tx_results = getattr(result, "transaction_results", None)
    if not tx_results:
        return None
    tx = getattr(tx_results[0], "tx_hash", "") or ""
    return tx or None


async def commit_teardown_intent(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    intent: AnyIntent,
    *,
    execution_result: Any,
    execution_context: Any,
    bundle_metadata: dict[str, Any] | None = None,
    teardown_cycle_id: str,
    pre_snapshot: Any | None = None,
    recon: dict[str, Any] | None = None,
    lending_pre_state: Any | None = None,
) -> TeardownCommitOutcome:
    """Run the full success-path commit pipeline for one teardown intent.

    Mirrors :py:meth:`StrategyRunner._single_chain_handle_success`'s body
    for steps 1–4 (enrich → ledger → outbox+fire → sidecar) but **never
    raises**. Step failures are captured into the returned outcome; the
    next step still runs where it can (e.g. ledger failure means we skip
    outbox+fire for that intent — outbox needs ``ledger_entry_id``).

    Parameters
    ----------
    runner:
        The :class:`StrategyRunner` that owns the writers. Passed
        explicitly (not implicit ``self``) so the function is easy to plumb
        as a callable into the teardown lane and to fake in tests.
    strategy:
        The strategy being torn down — used for ``deployment_id``,
        ``deployment_id``, ``chain``, ``wallet_address``.
    intent:
        The teardown intent that just executed on-chain.
    execution_result:
        The orchestrator's :class:`ExecutionResult` (success). Will be
        enriched in place; the enriched copy is what flows into the
        ledger / sidecar writes.
    execution_context:
        :class:`ExecutionContext` for the receipt parser. Required by
        :class:`ResultEnricher`.
    bundle_metadata:
        ``ActionBundle.metadata`` from the teardown compiler. Threaded into
        :func:`ResultEnricher.enrich` for VIB-3203 realized-slippage math.
    teardown_cycle_id:
        Stable cycle id for the teardown — typically
        ``f"teardown-{teardown_id}"``. Set on the contextvar for the
        duration of the helper so the underlying writers stamp the same
        value on their rows.
    """
    intent_type = _intent_type_str(intent)
    tx_hash = _first_tx_hash(execution_result)
    deployment_id = strategy.deployment_id

    degraded_records: list[DeferredWrite] = []
    degraded_reasons: list[str] = []

    def _record(kind: str, err: BaseException, *, ledger_entry_id: str | None = None) -> None:
        """Append a deferred-log record and remember the reason."""
        rec = DeferredWrite.now(
            kind=kind,
            deployment_id=deployment_id,
            cycle_id=teardown_cycle_id,
            intent_type=intent_type,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            error=str(err) or err.__class__.__name__,
        )
        deferred_append(rec)
        degraded_records.append(rec)
        degraded_reasons.append(f"{kind}: {err.__class__.__name__}: {err}")

    # Save + swap cycle_id — restored in finally. Phase 3 wiring also sets
    # this at the outer level; resetting to ``saved`` is a no-op there.
    saved_cycle_id = get_cycle_id()
    set_cycle_id(teardown_cycle_id)

    ledger_entry_id: str | None = None
    enriched_result = execution_result
    try:
        # ----- Step 1: ResultEnricher (best-effort) ----------------------
        try:
            from ..execution.result_enricher import ResultEnricher

            # VIB-4477 (T08): thread V4 pool_key_lookup bridge so teardown-lane
            # V4 LP_CLOSE receipts get the same PoolKey-driven attribution as
            # the iteration-lane closes.
            enricher = ResultEnricher(
                live_mode=runner._is_live_mode(),
                pool_key_lookup=runner._build_v4_pool_key_lookup(),
            )
            enriched_result = enricher.enrich(
                execution_result,
                intent,
                execution_context,
                bundle_metadata=bundle_metadata,
            )
        except Exception as exc:  # noqa: BLE001 — never propagate
            logger.error(
                "commit_teardown_intent: enrichment failed for %s/%s: %s",
                deployment_id,
                intent_type or "unknown-intent",
                exc,
                exc_info=True,
            )
            _record("enrich", exc)

        # ----- Step 2: ledger entry --------------------------------------
        # G12 wiring (Accounting-AttemptNo17 §A4): the teardown lane has no
        # ``state.price_oracle`` because there's no per-iteration state, so
        # the pre-teardown bracket stashes the priced PortfolioSnapshot's
        # token_prices on ``runner._teardown_price_oracle`` after re-shaping
        # to the build_ledger_entry contract. Without this, every teardown
        # row landed with empty ``price_inputs_json`` and ``gas_usd``.
        #
        # VIB-4318 — the pre-teardown stash only contains assets HELD at
        # pre-teardown time. A teardown intent whose token_in is a non-stable
        # token that is NOT yet in the wallet (e.g. a swap WETH → USDC that
        # consolidates LP-returned WETH back to stablecoin after the
        # LP_CLOSE) has no WETH price in the stash, so the ledger row's
        # ``price_inputs_json`` lands WITHOUT WETH and the SWAP handler's
        # fail-closed contract (VIB-3886) leaves
        # ``accounting_events.payload_json.amount_in_usd=NULL`` even though
        # the close-time WETH price was available on the gateway. Merge
        # intent-token prices into the stash BEFORE the ledger write so
        # every priced token the intent touches lands on the row. The
        # merge updates ``runner._teardown_price_oracle`` in place so
        # subsequent teardown intents in the same loop benefit (the
        # post-teardown bracket clears the stash so an iteration after
        # teardown never reads stale teardown prices).
        from ._run_loop_helpers import _ensure_intent_tokens_in_teardown_oracle

        runner._teardown_price_oracle = await _ensure_intent_tokens_in_teardown_oracle(
            runner,
            strategy,
            intent,
            getattr(runner, "_teardown_price_oracle", None),
        )
        teardown_price_oracle = runner._teardown_price_oracle

        # VIB-3918: build pre/post state dicts from the per-intent balance
        # captures so ``transaction_ledger.pre_state_json`` and
        # ``post_state_json`` land populated on every teardown row, lane-
        # symmetric with iteration. ``pre_snapshot`` is captured by the
        # teardown manager BEFORE ``orchestrator.execute``; ``recon`` is
        # captured AFTER. Either may be ``None`` when the runner_helpers
        # bag wasn't fully wired (older callers / unit-test stubs) — we
        # silently fall back to ``None``, preserving the pre-VIB-3918
        # empty-string column.
        pre_state_dict: dict[str, Any] | None = None
        post_state_dict: dict[str, Any] | None = None
        try:
            from .strategy_runner import (
                _build_post_state_for_ledger,
                _build_pre_state_for_ledger,
            )

            intent_protocol = (getattr(intent, "protocol", "") or "").lower()

            # VIB-3934 — capture lending POST-state on the teardown lane so
            # ``transaction_ledger.post_state_json`` carries
            # collateral/debt/HF for REPAY/WITHDRAW/DELEVERAGE intents,
            # lane-symmetric with iteration. Without this the lending handler
            # falls back to ESTIMATED confidence on every teardown row even
            # though the protocol state is readable on-chain. ``pre_state``
            # is supplied by the teardown manager (captured BEFORE
            # submission); attempting to read it here would return
            # post-state values because the TX has already landed.
            lending_post_state = None
            try:
                gateway_client = runner._get_gateway_client()
                teardown_price_oracle_for_state = getattr(runner, "_teardown_price_oracle", None)
                # VIB-4589 / F7 — pin post-state read to receipt block.
                # ``strategy_runner._last_receipt_block`` is the single
                # source of truth; we import lazily to keep the
                # teardown-commit module free of strategy_runner import-time
                # coupling.
                from .strategy_runner import _last_receipt_block

                lending_post_state = runner._capture_lending_state_safe(
                    intent=intent,
                    chain=getattr(strategy, "chain", "") or "",
                    wallet_address=getattr(strategy, "wallet_address", "") or "",
                    gateway_client=gateway_client,
                    price_oracle=teardown_price_oracle_for_state,
                    phase="post",
                    block=_last_receipt_block(execution_result),
                )
            except Exception as exc:  # noqa: BLE001 — never propagate
                logger.debug(
                    "commit_teardown_intent: lending post-state capture failed for %s: %s",
                    deployment_id,
                    exc,
                )

            if pre_snapshot is not None or lending_pre_state is not None:
                pre_state_dict = _build_pre_state_for_ledger(
                    pre_snapshot,
                    lending_pre_state,
                    protocol=intent_protocol,
                )
            if recon is not None or lending_post_state is not None:
                post_state_dict = _build_post_state_for_ledger(
                    recon,
                    lending_post_state,
                    protocol=intent_protocol,
                )
        except Exception as exc:  # noqa: BLE001 — never propagate state-building
            logger.debug(
                "commit_teardown_intent: pre/post state build failed for %s: %s",
                deployment_id,
                exc,
            )

        try:
            ledger_entry_id = await runner._write_ledger_entry(
                strategy,
                intent,
                result=enriched_result,
                success=True,
                price_oracle=teardown_price_oracle,
                pre_state=pre_state_dict,
                post_state=post_state_dict,
            )
        except Exception as exc:  # noqa: BLE001 — never propagate
            logger.error(
                "commit_teardown_intent: ledger write failed for %s/%s tx=%s: %s",
                deployment_id,
                intent_type or "unknown-intent",
                tx_hash or "-",
                exc,
                exc_info=True,
            )
            _record("ledger", exc)

        # ----- Step 3: outbox + fire processor ---------------------------
        if ledger_entry_id:
            try:
                await runner._write_outbox_and_fire_processor(strategy, intent, ledger_entry_id)
            except Exception as exc:  # noqa: BLE001 — never propagate
                logger.error(
                    "commit_teardown_intent: outbox+fire failed for %s ledger=%s: %s",
                    deployment_id,
                    ledger_entry_id,
                    exc,
                    exc_info=True,
                )
                _record("outbox", exc, ledger_entry_id=ledger_entry_id)

        # ----- Step 4: sidecar (best-effort) -----------------------------
        try:
            from ..accounting.sidecar import AccountingSidecarWriter

            chain = getattr(strategy, "chain", "") or getattr(runner.config, "chain", "")
            # Pass the SAME teardown-stash oracle the ledger write used
            # (Accounting-AttemptNo17 §A4) so the local-dashboard sidecar
            # row carries the same priced asset set as the canonical SQLite
            # row. Without this, the teardown sidecar JSONL line was missing
            # the price_oracle entirely.
            AccountingSidecarWriter().append(
                deployment_id=deployment_id,
                intent=intent,
                result=enriched_result,
                chain=chain,
                price_oracle=teardown_price_oracle,
            )
        except Exception as exc:  # noqa: BLE001 — never propagate
            logger.error(
                "commit_teardown_intent: sidecar append failed for %s: %s",
                deployment_id,
                exc,
                exc_info=True,
            )
            _record("sidecar", exc, ledger_entry_id=ledger_entry_id)
    finally:
        # Restore the contextvar. ``set_cycle_id`` accepts ``str``; for
        # ``None`` we use ``clear_cycle_id``-equivalent semantics
        # (the underlying ContextVar holds None at default).
        if saved_cycle_id is None:
            from ..observability.context import clear_cycle_id

            clear_cycle_id()
        else:
            set_cycle_id(saved_cycle_id)

    accounting_degraded = bool(degraded_records)
    degraded_reason = "; ".join(degraded_reasons) if degraded_reasons else None
    return TeardownCommitOutcome(
        ledger_entry_id=ledger_entry_id,
        accounting_degraded=accounting_degraded,
        degraded_reason=degraded_reason,
        degraded_writes=tuple(degraded_records),
    )


__all__ = [
    "TeardownCommitOutcome",
    "commit_teardown_intent",
]
