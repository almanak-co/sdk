"""Vault-settlement commit pipeline — VIB-5666.

Runner-owned twin of :func:`almanak.framework.runner.teardown_commit.commit_teardown_intent`,
for the vault-settlement lane. Vault settlement (``settleDeposit`` /
``settleRedeem`` / ``updateNewTotalAssets``) is lifecycle-owned and pre-``decide()``
— :mod:`almanak.framework.vault.lifecycle` calls ``ExecutionOrchestrator.execute``
directly, so historically every settlement tx landed on-chain with **zero** rows
in ``transaction_ledger`` / ``accounting_events`` (the identical "pre-``decide()``
therefore unaccounted" hole that bit teardown — but firing every settlement
interval). This module routes each settlement tx through the same commit pipeline
the iteration and teardown lanes use: **ledger → outbox+fire → sidecar** (no
``ResultEnricher`` — the settlement outputs are parsed by the connector's
settlement receipt parser upstream and passed in as raw ints).

Failure semantics (blueprint 27 §Teardown, applied to settlement)
-----------------------------------------------------------------
Settlement's first job — like teardown's — is to move real depositor capital
on-chain safely. The share-issuing / share-burning tx has ALREADY confirmed by
the time this runs; halting the settlement state machine because a *ledger write*
failed would strand a half-settled epoch (e.g. deposits settled but the redeem
leg never runs). So this pipeline **never raises**: step failures are captured
into a :class:`SettlementCommitOutcome` and surfaced via ``accounting_degraded``
(loud ERROR + deferred-write log), but the caller (``VaultLifecycleManager``)
continues the state machine. This inverts the iteration lane's halt-on-failure
contract, matching teardown.

Capital-event discipline
-------------------------
The emitted ``SETTLE_DEPOSIT`` / ``SETTLE_REDEEM`` accounting events are CAPITAL
events, not returns (see
:class:`almanak.framework.accounting.settlement_accounting.SettlementAccountingEvent`).
This module records the exact receipt-measured asset/share deltas so the books
tie, without letting a depositor inflow read as strategy profit.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, cast

import structlog

from ..accounting.deferred_log import DeferredWrite
from ..accounting.deferred_log import append as deferred_append
from ..observability.context import clear_cycle_id, get_cycle_id, set_cycle_id
from ..vault.capability import default_vault_protocol

if TYPE_CHECKING:  # pragma: no cover
    from ..intents.vocabulary import AnyIntent
    from .runner_models import StrategyProtocol
    from .strategy_runner import StrategyRunner

logger = logging.getLogger(__name__)

# Settlement legs → the accounting/ledger intent_type string. The propose leg
# (``updateNewTotalAssets``) moves no capital and mints no shares — it books a
# ledger row (gas / tx visibility so the books tie) under a NO_ACCOUNTING
# intent_type that is absent from the taxonomy (``classify`` → NO_ACCOUNTING),
# so it produces no accounting event. Only the two capital-moving legs emit a
# typed SETTLE event.
_LEG_TO_INTENT_TYPE: dict[str, str] = {
    "deposit": "SETTLE_DEPOSIT",
    "redeem": "SETTLE_REDEEM",
    "propose": "SETTLE_PROPOSE",
}
_ACCOUNTING_LEGS = frozenset({"deposit", "redeem"})


@dataclass(frozen=True)
class SettlementCommitOutcome:
    """Outcome of the per-tx settlement commit pipeline.

    Attributes
    ----------
    ledger_entry_id:
        The persisted ``LedgerEntry.id`` when the ledger write succeeded, else
        ``None``. Outbox+fire only runs when this is non-None.
    accounting_degraded:
        ``True`` iff any pipeline step (ledger / outbox / sidecar) failed. Drives
        the ``SettlementResult.accounting_degraded`` flag surfaced to the runner
        and operator alerting. NEVER blocks the settlement state machine.
    degraded_reason:
        Compact summary of which steps failed, or ``None``.
    degraded_writes:
        The :class:`DeferredWrite` records appended to the deferred-write log.
    """

    ledger_entry_id: str | None
    accounting_degraded: bool
    degraded_reason: str | None
    degraded_writes: tuple[DeferredWrite, ...] = field(default_factory=tuple)


class _SettlementIntent:
    """Minimal intent-like object the commit pipeline threads into
    ``build_ledger_entry`` / ``_write_outbox_and_fire_processor``.

    Settlement is NOT a first-class ``Intent`` verb (it is lifecycle-owned and
    pre-``decide()``; design doc §Pillar-1), but the commit pipeline is
    intent-shaped. This carries exactly the attributes those two code paths read:

    * ``intent_type`` (plain str — the pipeline stringifies it),
    * ``protocol`` (``"lagoon"``),
    * ``vault_address`` (consumed by ``_compute_outbox_position_key``'s SETTLE
      branch → outbox ``market_id`` + ``position_key``),
    * ``from_token`` / ``amount`` (the ``_extract_from_intent_fallback`` chain, so
      ``transaction_ledger.token_in`` / ``amount_in`` land populated with the
      underlying + human asset amount).
    """

    def __init__(
        self,
        *,
        intent_type: str,
        protocol: str,
        vault_address: str,
        from_token: str | None,
        amount: Decimal | None,
    ) -> None:
        self.intent_type = intent_type
        self.protocol = protocol
        self.vault_address = vault_address
        # ``from_token`` is first in the fallback token precedence chain
        # (from_token > borrow_token > supply_token > token). ``None`` → token_in
        # stays "" (propose leg moves no asset).
        if from_token:
            self.from_token = from_token
        # ``amount`` drives ``transaction_ledger.amount_in`` (human units of the
        # underlying). Omitted (not set) for the propose leg so amount_in stays "".
        if amount is not None:
            self.amount = amount


def _raw_to_human(raw: int | None, decimals: int | None) -> Decimal | None:
    """Scale a raw on-chain integer amount to a human Decimal.

    Empty ≠ Zero: ``raw is None`` (unmeasured) or ``decimals is None``
    (undeterminable scale) → ``None``, NEVER ``Decimal("0")``. A measured
    ``raw == 0`` with known decimals → ``Decimal("0")`` (measured zero).
    """
    if raw is None or decimals is None:
        return None
    try:
        return Decimal(int(raw)) / (Decimal(10) ** int(decimals))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _money_str(value: Decimal | None) -> str | None:
    """Serialize a human Decimal to a canonical string, or ``None`` if unmeasured."""
    return str(value) if value is not None else None


async def commit_settlement_intent(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    *,
    leg: str,
    execution_result: Any,
    settlement_cycle_id: str,
    vault_address: str,
    underlying_token: str,
    assets_raw: int | None = None,
    shares_raw: int | None = None,
    new_total_assets_raw: int | None = None,
    fee_shares_raw: int | None = None,
    epoch_id: int | None = None,
    underlying_decimals: int | None = None,
    share_decimals: int | None = None,
    underlying_price: Decimal | None = None,
) -> SettlementCommitOutcome:
    """Run the ledger → outbox+fire → sidecar commit pipeline for one settlement tx.

    Parameters mirror the data the ``VaultLifecycleManager`` has after a
    successful ``orchestrator.execute`` for a settlement leg. ``*_raw`` are raw
    on-chain integer amounts (receipt-measured); they are scaled to human units
    here. ``leg`` is ``"deposit"`` / ``"redeem"`` / ``"propose"``. Never raises —
    every failure is captured into the returned outcome.
    """
    intent_type = _LEG_TO_INTENT_TYPE.get(leg)
    if intent_type is None:
        logger.error("commit_settlement_intent: unknown settlement leg %r; skipping commit", leg)
        return SettlementCommitOutcome(None, True, f"unknown settlement leg {leg!r}")

    deployment_id = strategy.deployment_id
    chain = getattr(strategy, "chain", "") or getattr(runner.config, "chain", "") or ""

    # ── Raw → human, USD ─────────────────────────────────────────────────────
    assets_human = _raw_to_human(assets_raw, underlying_decimals)
    shares_human = _raw_to_human(shares_raw, share_decimals)
    nta_human = _raw_to_human(new_total_assets_raw, underlying_decimals)
    fee_shares_human = _raw_to_human(fee_shares_raw, share_decimals)
    assets_usd: Decimal | None = None
    if assets_human is not None and underlying_price is not None:
        try:
            assets_usd = assets_human * underlying_price
        except (InvalidOperation, TypeError):
            assets_usd = None

    # ── Synthetic intent + stamped settlement extracted_data ─────────────────
    # Protocol comes from the vault-connector registry (single registered
    # lifecycle-vault protocol today), never a hardcoded string (blueprint 22).
    # This function never raises: an unresolvable registry degrades to "" —
    # unmeasured per Empty ≠ Zero — and the row still books tx/gas.
    try:
        vault_protocol = default_vault_protocol()
    except Exception as exc:  # noqa: BLE001 — never propagate (Lane B)
        logger.error("commit_settlement_intent: vault protocol resolution failed: %s", exc)
        vault_protocol = ""
    intent = _SettlementIntent(
        intent_type=intent_type,
        protocol=vault_protocol,
        vault_address=vault_address,
        from_token=underlying_token if leg in _ACCOUNTING_LEGS else None,
        amount=assets_human if leg in _ACCOUNTING_LEGS else None,
    )
    # ``_SettlementIntent`` duck-types the exact attribute surface the two
    # runner pipeline methods read (see its docstring); settlement is not a
    # first-class Intent verb, so it is not (and must not be) in ``AnyIntent``.
    pipeline_intent = cast("AnyIntent", intent)

    # Stamp the human-unit settlement outputs onto the result's extracted_data so
    # ``build_ledger_entry`` serializes them into ``extracted_data_json`` and the
    # settlement category handler (which makes no chain calls) can read them back.
    # Preserve any pre-existing extracted_data on the result envelope.
    if leg in _ACCOUNTING_LEGS and execution_result is not None:
        existing = getattr(execution_result, "extracted_data", None)
        extracted: dict[str, Any] = dict(existing) if isinstance(existing, dict) else {}
        extracted["settlement"] = {
            "leg": leg,
            "assets": _money_str(assets_human),
            "shares": _money_str(shares_human),
            "new_total_assets": _money_str(nta_human),
            "fee_shares": _money_str(fee_shares_human),
            "assets_usd": _money_str(assets_usd),
            "epoch_id": epoch_id,
        }
        try:
            execution_result.extracted_data = extracted
        except (AttributeError, TypeError):
            logger.warning(
                "commit_settlement_intent: could not stamp settlement extracted_data on result "
                "for %s/%s — accounting event will land with unmeasured deltas",
                deployment_id,
                intent_type,
            )

    # Price oracle used by the ledger write: carry the underlying's USD price so
    # ``price_inputs_json`` records the settlement-time valuation. (Gas USD needs
    # the NATIVE token price, which the settlement lane does not resolve — gas_usd
    # stays "" / unmeasured per Empty ≠ Zero; tx_hash + gas_used are still booked.)
    price_oracle: dict[str, Any] | None = None
    if underlying_price is not None and underlying_token:
        price_oracle = {underlying_token: underlying_price}

    degraded_records: list[DeferredWrite] = []
    degraded_reasons: list[str] = []
    tx_hash = _first_tx_hash(execution_result)

    def _record(kind: str, err: BaseException, *, ledger_entry_id: str | None = None) -> None:
        rec = DeferredWrite.now(
            kind=kind,
            deployment_id=deployment_id,
            cycle_id=settlement_cycle_id,
            intent_type=intent_type,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            error=str(err) or err.__class__.__name__,
        )
        deferred_append(rec)
        degraded_records.append(rec)
        degraded_reasons.append(f"{kind}: {err.__class__.__name__}: {err}")

    saved_cycle_id = get_cycle_id()
    set_cycle_id(settlement_cycle_id)
    ledger_entry_id: str | None = None
    with structlog.contextvars.bound_contextvars(cycle_id=settlement_cycle_id, correlation_id=settlement_cycle_id):
        try:
            # ── Step 1: transaction_ledger row ───────────────────────────────
            try:
                ledger_entry_id = await runner._write_ledger_entry(
                    strategy,
                    pipeline_intent,
                    result=execution_result,
                    success=True,
                    price_oracle=price_oracle,
                    pre_state=None,
                    post_state=None,
                    # Settlement is a CAPITAL event, not a position — no
                    # position_events row (a depositor deposit is not a strategy
                    # position OPEN/CLOSE).
                    emit_position_event=False,
                )
            except Exception as exc:  # noqa: BLE001 — never propagate (Lane B)
                logger.error(
                    "commit_settlement_intent: ledger write failed for %s/%s tx=%s: %s",
                    deployment_id,
                    intent_type,
                    tx_hash or "-",
                    exc,
                    exc_info=True,
                )
                _record("ledger", exc)

            # ── Step 2: outbox + fire processor (accounting legs only) ───────
            # The propose leg is NO_ACCOUNTING — a ledger row (gas/tx) suffices;
            # firing the drain would only classify to NO_ACCOUNTING and no-op.
            if leg in _ACCOUNTING_LEGS and ledger_entry_id:
                try:
                    await runner._write_outbox_and_fire_processor(strategy, pipeline_intent, ledger_entry_id)
                except Exception as exc:  # noqa: BLE001 — never propagate
                    logger.error(
                        "commit_settlement_intent: outbox+fire failed for %s ledger=%s: %s",
                        deployment_id,
                        ledger_entry_id,
                        exc,
                        exc_info=True,
                    )
                    _record("outbox", exc, ledger_entry_id=ledger_entry_id)

            # ── Step 3: sidecar (best-effort local-dashboard mirror) ─────────
            if leg in _ACCOUNTING_LEGS:
                try:
                    from ..accounting.sidecar import AccountingSidecarWriter

                    AccountingSidecarWriter().append(
                        deployment_id=deployment_id,
                        intent=intent,
                        result=execution_result,
                        chain=chain,
                        price_oracle=price_oracle,
                    )
                except Exception as exc:  # noqa: BLE001 — never propagate
                    logger.error(
                        "commit_settlement_intent: sidecar append failed for %s: %s",
                        deployment_id,
                        exc,
                        exc_info=True,
                    )
                    _record("sidecar", exc, ledger_entry_id=ledger_entry_id)
        finally:
            if saved_cycle_id is None:
                clear_cycle_id()
            else:
                set_cycle_id(saved_cycle_id)

    accounting_degraded = bool(degraded_records)
    degraded_reason = "; ".join(degraded_reasons) if degraded_reasons else None
    return SettlementCommitOutcome(
        ledger_entry_id=ledger_entry_id,
        accounting_degraded=accounting_degraded,
        degraded_reason=degraded_reason,
        degraded_writes=tuple(degraded_records),
    )


def _first_tx_hash(result: Any) -> str | None:
    """First on-chain tx hash from a successful execution result, or None."""
    if not result:
        return None
    tx_results = getattr(result, "transaction_results", None)
    if not tx_results:
        return None
    tx = getattr(tx_results[0], "tx_hash", "") or ""
    return tx or None


def build_settlement_commit(runner: StrategyRunner) -> Any:
    """Bind ``runner`` into an async settlement-commit callable for the
    ``VaultLifecycleManager``.

    Returns ``commit(strategy, *, leg, execution_result, settlement_cycle_id,
    vault_address, underlying_token, ...) -> SettlementCommitOutcome`` — the
    lifecycle manager calls a plain callable and never sees the runner, mirroring
    :func:`almanak.framework.teardown.runner_helpers.build_runner_helpers`.
    """
    from functools import partial

    return partial(commit_settlement_intent, runner)


__all__ = [
    "SettlementCommitOutcome",
    "build_settlement_commit",
    "commit_settlement_intent",
]
