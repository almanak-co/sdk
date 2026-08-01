"""Perp keeper-settlement reconciler — VIB-3872 (WI-3).

A non-blocking, restart-safe pre-decide runner step (placement mirrors the
fill-reconciliation pump). Each tick it:

1. **Re-derives the pending-settlement watch set from the DB** (zero new storage):
   perp ``transaction_ledger`` rows whose ``extracted_data_json.async_orders``
   carry order keys, MINUS rows that already have a terminal ``PERP_SETTLEMENT``
   accounting event. Restart-safe by construction — a kill between submission and
   fill is recovered at the next tick / boot because the watch set is a pure
   function of persisted rows, not in-memory state.
2. Asks the owning connector for a per-order verdict via WI-2's frozen
   ``RunnerHookRegistry.resolve_perp_settlements`` (all chain reads gateway-routed).
3. Commits every TERMINAL verdict through the ``perp_settlement_commit`` lane
   (append-only ``PERP_SETTLEMENT`` event, drain-first ordering guard).

**Catch boundary (design §3 D2, BINDING).** This step is the catch boundary for
``AccountingPersistenceError``: it catches per-watch-entry, OUTSIDE the
decide/execute risk loop — logs ERROR + raises an operator alert, leaves the entry
non-terminal (a row was NOT written → next tick's re-derivation re-includes it →
idempotent retry via the deterministic event id), and returns so the iteration's
next risk-reducing action proceeds unconditionally. A settlement-write failure is
loud but NEVER halts the runner (the same inverted-failure-semantics as teardown:
the settlement event describes money that already moved).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from almanak.framework.state.exceptions import AccountingPersistenceError

from .perp_settlement_commit import commit_perp_settlement

if TYPE_CHECKING:  # pragma: no cover
    from ..observability.ledger import LedgerEntry
    from .runner_models import StrategyProtocol
    from .strategy_runner import StrategyRunner

logger = logging.getLogger(__name__)

# Perp submission intent types whose keeper settlement we reconcile.
_PERP_SUBMISSION_INTENTS = ("PERP_OPEN", "PERP_CLOSE")
# Parse the order key / direction out of an ``AsyncOrderData`` repr string (the
# form ``serialize_extracted_data`` persists a ``list[AsyncOrderData]`` as, via
# ``json.dumps(default=str)``). Structured dicts (future-proof) are handled too.
_ORDER_ID_RE = re.compile(r"order_id=['\"](0x[0-9a-fA-F]{64})['\"]")
_IS_LONG_RE = re.compile(r"is_long=(True|False|None)")


@dataclass(frozen=True)
class PerpSettlementReconcileOutcome:
    """Aggregate visibility for an immediate teardown reconciliation tick."""

    attempted: int = 0
    booked: int = 0
    attempted_order_keys: tuple[str, ...] = ()
    booked_order_keys: tuple[str, ...] = ()
    degraded_reasons: tuple[str, ...] = ()

    @property
    def accounting_degraded(self) -> bool:
        return bool(self.degraded_reasons)


async def reconcile_perp_settlements(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    *,
    deployment_id: str,
    cycle_id: str,
    gateway_client: Any,
    chain: str | None = None,
    wallet_address: str | None = None,
) -> PerpSettlementReconcileOutcome:
    """One reconciler tick. Never raises (the catch boundary is internal)."""
    if gateway_client is None:
        # No gateway (paper/dry-run without a managed fork) — cannot read keeper
        # events; the watch set persists and is retried when a gateway is present.
        return PerpSettlementReconcileOutcome()

    resolved_chain = str(chain or getattr(strategy, "chain", "") or getattr(runner.config, "chain", "") or "").lower()
    resolved_wallet = str(wallet_address or getattr(strategy, "wallet_address", "") or "")
    if not resolved_chain:
        return PerpSettlementReconcileOutcome()

    watch = await _derive_watch_set(runner, deployment_id)
    if not watch:
        if not getattr(watch, "complete", watch.measured):
            return PerpSettlementReconcileOutcome(
                degraded_reasons=("settlement watch set was unmeasured; durable reconciliation will retry",)
            )
        return PerpSettlementReconcileOutcome()

    # Resolve keeper verdicts off the event loop (WI-2 resolve does gateway RPCs).
    verdicts_by_protocol = await asyncio.to_thread(
        _resolve_all_verdicts,
        gateway_client=gateway_client,
        chain=resolved_chain,
        wallet_address=resolved_wallet,
        watch=watch,
    )

    attempted = 0
    booked = 0
    attempted_order_keys: list[str] = []
    booked_order_keys: list[str] = []
    degraded_reasons: list[str] = (
        ["one or more settlement ledger rows were unmeasured; durable reconciliation will retry"]
        if not getattr(watch, "complete", watch.measured)
        else []
    )
    for protocol, verdicts in verdicts_by_protocol.items():
        for verdict in verdicts:
            entry = watch.get((protocol, str(getattr(verdict, "order_key", "")).lower()))
            if entry is None or not getattr(verdict, "terminal", False):
                continue  # unknown / still-PENDING / non-terminal UNMEASURED → keep watching
            attempted += 1
            attempted_order_keys.append(entry.order_key.lower())
            outcome = await _commit_verdict(
                runner,
                strategy,
                verdict=verdict,
                entry=entry,
                cycle_id=cycle_id,
                chain=resolved_chain,
                protocol=protocol,
                wallet_address=resolved_wallet,
            )
            if outcome is None:
                degraded_reasons.append(f"order {entry.order_key}: settlement commit failed")
            else:
                if outcome.booked:
                    booked += 1
                    booked_order_keys.append(entry.order_key.lower())
                if outcome.accounting_degraded or not outcome.booked:
                    degraded_reasons.append(
                        f"order {entry.order_key}: {outcome.degraded_reason or 'settlement was not booked'}"
                    )
    return PerpSettlementReconcileOutcome(
        attempted=attempted,
        booked=booked,
        attempted_order_keys=tuple(attempted_order_keys),
        booked_order_keys=tuple(booked_order_keys),
        degraded_reasons=tuple(degraded_reasons),
    )


class _WatchEntry:
    """One derived watch entry: the order + its submission ledger row."""

    __slots__ = ("order_key", "is_open", "is_long", "protocol", "ledger", "submission_tx_hash", "submission_timestamp")

    def __init__(
        self,
        *,
        order_key: str,
        is_open: bool,
        is_long: bool | None,
        protocol: str,
        ledger: LedgerEntry,
        submission_tx_hash: str,
        submission_timestamp: datetime | None,
    ) -> None:
        self.order_key = order_key
        self.is_open = is_open
        self.is_long = is_long
        self.protocol = protocol
        self.ledger = ledger
        self.submission_tx_hash = submission_tx_hash
        self.submission_timestamp = submission_timestamp


class _DerivedWatch(dict[tuple[str, str], _WatchEntry]):
    """Watch entries plus whether the backing reads were authoritative."""

    def __init__(
        self,
        *args: Any,
        measured: bool = True,
        unmeasured_candidates: tuple[dict[str, Any], ...] = (),
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.measured = measured
        self.unmeasured_candidates = unmeasured_candidates

    @property
    def complete(self) -> bool:
        """Whether every measured candidate row was hydrated authoritatively."""
        return self.measured and not self.unmeasured_candidates


@dataclass(frozen=True)
class _Phase1CloseInventory:
    """All successful exact-cycle close ledgers, before the settled MINUS."""

    rows: tuple[dict[str, Any], ...] = ()
    measured: bool = True
    degraded_reason: str | None = None


async def _read_phase1_close_inventory(
    runner: StrategyRunner, deployment_id: str, expected_cycle_id: str
) -> _Phase1CloseInventory:
    """Hydrate every possible exact-cycle close row without filtering settled rows."""
    sm = runner.state_manager
    ledger_reader = getattr(sm, "read_ledger_entries_measured", None)
    hydrate = getattr(sm, "get_ledger_entry_by_id", None)
    if not callable(ledger_reader) or not callable(hydrate):
        return _Phase1CloseInventory(measured=False, degraded_reason="Phase-1 ledger readers are unavailable")
    candidates = await _read_perp_candidates(ledger_reader, deployment_id)
    if candidates is None:
        return _Phase1CloseInventory(measured=False, degraded_reason="Phase-1 ledger inventory was unmeasured")

    exact_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if str(candidate.get("intent_type") or "").upper() != "PERP_CLOSE" or not candidate.get("success", False):
            continue
        cycle_hint = str(candidate.get("cycle_id") or "")
        if cycle_hint and cycle_hint != expected_cycle_id:
            continue
        ledger_id = str(candidate.get("id") or "")
        if not ledger_id:
            continue
        try:
            full = await hydrate(ledger_id)
        except Exception as exc:  # noqa: BLE001 — candidate identity is unmeasured
            return _Phase1CloseInventory(
                measured=False,
                degraded_reason=f"Phase-1 close ledger {ledger_id} hydrate failed: {exc}",
            )
        if not isinstance(full, dict):
            return _Phase1CloseInventory(
                measured=False,
                degraded_reason=f"Phase-1 close ledger {ledger_id} hydrate was unmeasured",
            )
        if str(full.get("cycle_id") or "") == expected_cycle_id:
            exact_rows.append(full)
    return _Phase1CloseInventory(rows=tuple(exact_rows))


# Warn-once (per state_manager class, per process) that the runtime backend lacks the
# measured-read API — VIB-6107: this class of interface mismatch must NEVER be silent
# again (the WI-3 reconciler was dead code in every real strat run because the probe
# returned {} with no log). Keyed by class name so distinct backends each warn once.
_MISSING_READER_WARNED: set[str] = set()


def _warn_missing_reader_once(sm: Any) -> None:
    key = type(sm).__name__ if sm is not None else "None"
    if key in _MISSING_READER_WARNED:
        return
    _MISSING_READER_WARNED.add(key)
    logger.warning(
        "perp settlement reconciler: state_manager %s lacks the measured-read API "
        "(read_ledger_entries_measured / read_accounting_events_measured / get_ledger_entry_by_id) — "
        "reconciler is INERT, NO PERP_SETTLEMENT rows will be booked. This must never be silent (VIB-6107).",
        key,
    )


async def _derive_watch_set(runner: StrategyRunner, deployment_id: str) -> _DerivedWatch:
    """Re-derive the pending-settlement watch set from persisted rows (no new storage).

    watch = {perp ledger rows with async-order keys} MINUS {rows that already have a
    terminal PERP_SETTLEMENT accounting event (joined by submission ledger id)}.

    VIB-6107: consumes the PRODUCTION backend's canonical measured-read API. A real
    ``almanak strat run`` ALWAYS injects ``GatewayStateManager`` (``_run_components.py``
    — "always use gateway-backed state manager"), which exposes
    ``read_ledger_entries_measured`` / ``read_accounting_events_measured`` (both return
    ``(list[dict], measured)``) and ``get_ledger_entry_by_id`` — NOT the plain
    ``StateManager``'s ``get_ledger_entries`` / ``get_accounting_events``. The old
    reconciler probed for those latter names and returned ``{}`` silently on every real
    tick, so it never booked a single settlement (proven on mainnet, WI-5).

    **Empty ≠ Zero (BINDING).** ``measured=False`` on either read means the backend is
    structurally absent / errored / an old gateway — an UNMEASURED read. We MUST NOT
    treat its empty result as an authoritative "nothing to settle" (that would fabricate
    a zero watch set): we skip the tick and retry next tick. Only a ``measured=True``
    empty is an authoritative "nothing to do".

    The measured list read (``read_ledger_entries_measured`` → the ``LedgerEntryInfo``
    projection) does NOT carry ``extracted_data_json`` (where ``async_orders`` live): the
    gateway-server builder ``_ledger_entry_to_proto`` (``state_service.py`` — stops at
    ``error``) never emits it, so surfacing it on the list read would be a gateway-server
    change on the **security perimeter** (Infra). Instead we hydrate the small post-MINUS
    candidate set CLIENT-SIDE via ``get_ledger_entry_by_id`` (``GetLedgerEntry`` →
    ``LedgerEntryData``, whose dict already carries decoded ``extracted_data_json`` —
    ``gateway_state_manager.py``), rebuilding a real ``LedgerEntry`` so the downstream
    commit lane keeps its object contract. Zero perimeter change.

    Decomposed into ``_read_settled_ledger_ids`` / ``_read_perp_candidates`` /
    ``_needs_hydrate`` / ``_hydrate_watch_entries`` (blueprint 06 — the state read path):
    the two reads carry the ``None ⇒ UNMEASURED ⇒ skip tick`` contract; only rows passing
    the MINUS filter are hydrated. Behaviour-preserving vs the inline form.
    """
    sm = runner.state_manager
    ledger_reader = getattr(sm, "read_ledger_entries_measured", None)
    events_reader = getattr(sm, "read_accounting_events_measured", None)
    hydrate = getattr(sm, "get_ledger_entry_by_id", None)
    if sm is None or not callable(ledger_reader) or not callable(events_reader) or not callable(hydrate):
        _warn_missing_reader_once(sm)
        return _DerivedWatch(measured=False)

    settled_ledger_ids = await _read_settled_ledger_ids(events_reader, deployment_id)
    if settled_ledger_ids is None:  # UNMEASURED read ⇒ skip tick (never fabricate an empty set)
        return _DerivedWatch(measured=False)
    candidates = await _read_perp_candidates(ledger_reader, deployment_id)
    if candidates is None:  # UNMEASURED read ⇒ skip tick
        return _DerivedWatch(measured=False)

    # Only unsettled successful perp submission rows need a hydrate RPC. The MINUS keeps
    # this tiny in steady state; surface an unexpectedly large fan-out (e.g. a fresh boot
    # with a long unsettled backlog) so a per-candidate N+1 can never balloon silently.
    to_hydrate = [row for row in candidates if _needs_hydrate(row, settled_ledger_ids)]
    logger.debug("perp settlement reconciler: %d perp candidate row(s) need hydration", len(to_hydrate))
    _notice_large_fanout_once(len(to_hydrate))

    watch = _DerivedWatch()
    unmeasured_candidates: list[dict[str, Any]] = []
    for row in to_hydrate:
        hydrated = await _hydrate_watch_entries(hydrate, row)
        if hydrated is None:
            unmeasured_candidates.append(row)
            continue
        for key, entry in hydrated:
            watch[key] = entry
    watch.unmeasured_candidates = tuple(unmeasured_candidates)
    return watch


async def _read_settled_ledger_ids(events_reader: Any, deployment_id: str) -> set[str] | None:
    """Ledger ids that already carry a terminal PERP_SETTLEMENT event, or ``None`` when
    the read is UNMEASURED (backend absent/errored ⇒ skip the tick, Empty≠Zero)."""
    try:
        events, measured = await asyncio.to_thread(events_reader, deployment_id)
    except Exception as exc:  # noqa: BLE001 — read failure ⇒ UNMEASURED, skip this tick
        logger.debug(
            "perp settlement reconciler: accounting-events read failed (skipping tick): %s", exc, exc_info=True
        )
        return None
    if not measured:
        logger.info(
            "perp settlement reconciler: accounting-events read UNMEASURED (backend absent/errored) — "
            "skipping tick (Empty≠Zero), retried next tick"
        )
        return None
    return {
        str(ev.get("ledger_entry_id"))
        for ev in events or []
        if isinstance(ev, dict)
        and str(ev.get("event_type") or "").upper() == "PERP_SETTLEMENT"
        and ev.get("ledger_entry_id")
    }


async def _read_perp_candidates(ledger_reader: Any, deployment_id: str) -> list[dict[str, Any]] | None:
    """The measured ledger-row list (dict rows), or ``None`` when the read is UNMEASURED
    (backend absent/errored ⇒ skip the tick, Empty≠Zero)."""
    try:
        rows, measured = await asyncio.to_thread(ledger_reader, deployment_id)
    except Exception as exc:  # noqa: BLE001 — read failure ⇒ UNMEASURED, skip this tick
        logger.debug("perp settlement reconciler: ledger read failed (skipping tick): %s", exc, exc_info=True)
        return None
    if not measured:
        logger.info(
            "perp settlement reconciler: ledger read UNMEASURED (backend absent/errored) — "
            "skipping tick (Empty≠Zero), retried next tick"
        )
        return None
    return [row for row in rows or [] if isinstance(row, dict)]


# A perp candidate fan-out above this many hydrate RPCs in one tick is unexpected in
# steady state (the MINUS keeps it near zero); surface it once so a boot-time N+1 is
# visible rather than silent. NOT a cap — every candidate is still hydrated (dropping
# some would silently miss settlements).
_HYDRATE_FANOUT_NOTICE = 50
_LARGE_FANOUT_NOTICED = False


def _notice_large_fanout_once(count: int) -> None:
    global _LARGE_FANOUT_NOTICED
    if count <= _HYDRATE_FANOUT_NOTICE or _LARGE_FANOUT_NOTICED:
        return
    _LARGE_FANOUT_NOTICED = True
    logger.info(
        "perp settlement reconciler: %d unsettled perp candidate rows this tick (> %d) — "
        "hydrating each via get_ledger_entry_by_id; expected only on a fresh boot with a "
        "settlement backlog, converges as rows settle (VIB-6107).",
        count,
        _HYDRATE_FANOUT_NOTICE,
    )


def _needs_hydrate(row: dict[str, Any], settled_ledger_ids: set[str]) -> bool:
    """True iff a candidate ledger row needs a ``get_ledger_entry_by_id`` hydrate: a
    SUCCESSFUL perp submission row (``PERP_OPEN`` / ``PERP_CLOSE``) with an id that is not
    already settled (the MINUS)."""
    if str(row.get("intent_type") or "").upper() not in _PERP_SUBMISSION_INTENTS:
        return False
    ledger_id = str(row.get("id") or "")
    if not ledger_id or ledger_id in settled_ledger_ids:
        return False
    return bool(row.get("success", False))


async def _hydrate_watch_entries(hydrate: Any, row: dict[str, Any]) -> list[tuple[tuple[str, str], _WatchEntry]] | None:
    """Zero or more watch entries for one candidate row (assumed to pass ``_needs_hydrate``).

    Hydrates the full ledger row via ``get_ledger_entry_by_id`` (the list projection lacks
    ``extracted_data_json``) and parses its ``async_orders``. A perp submission row can
    carry multiple async orders → multiple entries. Returns ``None`` when the hydrate is
    UNMEASURED (retry next tick; never fabricate an empty order list), and ``[]`` only
    when the measured hydrated row has no usable protocol/order entries.
    """
    from ..observability.ledger import LedgerEntry

    ledger_id = str(row.get("id") or "")
    # A per-row hydrate failure (RAISE) is the SAME "this row is unmeasured this tick" as a
    # None return: skip THIS row and let the other candidates proceed — one flaky
    # per-candidate RPC must never drop every other order's progress this tick (Empty≠Zero:
    # defer the row, never fabricate). Mirrors the WI-3 per-entry catch-boundary philosophy.
    try:
        full = await hydrate(ledger_id)
    except Exception as exc:  # noqa: BLE001 — unmeasured row this tick ⇒ skip it, retry next tick
        logger.debug(
            "perp settlement reconciler: hydrate failed for ledger %s, skipping row this tick: %s",
            ledger_id,
            exc,
            exc_info=True,
        )
        return None
    if not isinstance(full, dict):
        return None
    protocol = str(full.get("protocol") or row.get("protocol") or "").lower()
    if not protocol:
        return []
    intent_type = str(row.get("intent_type") or "").upper()
    ledger = LedgerEntry.from_dict(full)
    is_open = intent_type == "PERP_OPEN"
    submission_tx_hash = str(getattr(ledger, "tx_hash", "") or "")
    submission_timestamp = _ledger_timestamp(ledger)
    entries: list[tuple[tuple[str, str], _WatchEntry]] = []
    for order_key, is_long in _parse_async_orders(full.get("extracted_data_json") or ""):
        entries.append(
            (
                (protocol, order_key.lower()),
                _WatchEntry(
                    order_key=order_key,
                    is_open=is_open,
                    is_long=is_long,
                    protocol=protocol,
                    ledger=ledger,
                    submission_tx_hash=submission_tx_hash,
                    submission_timestamp=submission_timestamp,
                ),
            )
        )
    return entries


def _parse_async_orders(extracted_data_json: str) -> list[tuple[str, bool | None]]:
    """Extract ``(order_key, is_long)`` pairs from a ledger row's async_orders.

    ``serialize_extracted_data`` persists a ``list[AsyncOrderData]`` as a JSON array
    of dataclass repr strings (``json.dumps(default=str)``); a future structured
    form (list of dicts) is also handled. Returns only well-formed bytes32 keys.
    """
    if not extracted_data_json:
        return []
    try:
        data = json.loads(extracted_data_json)
    except (TypeError, ValueError):
        return []
    orders = data.get("async_orders") if isinstance(data, dict) else None
    if not isinstance(orders, list):
        return []
    out: list[tuple[str, bool | None]] = []
    for item in orders:
        if isinstance(item, dict):
            key = item.get("order_id") or item.get("order_key")
            if isinstance(key, str) and re.fullmatch(r"0x[0-9a-fA-F]{64}", key):
                out.append((key, item.get("is_long") if isinstance(item.get("is_long"), bool) else None))
            continue
        if isinstance(item, str):
            m = _ORDER_ID_RE.search(item)
            if m:
                long_m = _IS_LONG_RE.search(item)
                is_long: bool | None = None
                if long_m:
                    is_long = True if long_m.group(1) == "True" else False if long_m.group(1) == "False" else None
                out.append((m.group(1), is_long))
    return out


def _ledger_timestamp(ledger: LedgerEntry) -> datetime | None:
    ts = getattr(ledger, "timestamp", None)
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=UTC)
    if isinstance(ts, str):
        try:
            parsed = datetime.fromisoformat(ts)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            return None
    return None


def _resolve_all_verdicts(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    watch: dict[tuple[str, str], _WatchEntry],
) -> dict[str, list[Any]]:
    """Resolve keeper verdicts per protocol (runs in a worker thread).

    Resolves each order's ``submission_block`` from its createOrder tx receipt,
    then dispatches to the connector's ``resolve_perp_settlements`` (WI-2).
    """
    from almanak.connectors._base.types import ProtocolName
    from almanak.connectors._strategy_base.runner_hook_registry import PerpSettlementWatchEntry

    # Import the DISCOVERY module (not the raw singleton) so connector runner-hooks
    # register as a side effect — matches the fill-recon pump. Idempotent: hook
    # registration runs once at first import of this module.
    from almanak.connectors._strategy_runner_hook_registry import STRATEGY_RUNNER_HOOK_REGISTRY
    from almanak.framework.web3.gateway_provider import get_gateway_web3

    try:
        web3 = get_gateway_web3(gateway_client, chain)
    except Exception as exc:  # noqa: BLE001 — no gateway handle ⇒ nothing measurable this tick
        logger.debug("perp settlement reconciler: gateway web3 unavailable: %s", exc, exc_info=True)
        return {}

    now = datetime.now(UTC)
    by_protocol: dict[str, list[PerpSettlementWatchEntry]] = {}
    for (protocol, _key), entry in watch.items():
        block = _resolve_submission_block(web3, entry.submission_tx_hash)
        if block is None:
            continue  # cannot scan keeper logs without a fromBlock — retry next tick
        elapsed = None
        if entry.submission_timestamp is not None:
            elapsed = max(0, int((now - entry.submission_timestamp).total_seconds()))
        by_protocol.setdefault(protocol, []).append(
            PerpSettlementWatchEntry(
                order_key=entry.order_key,
                submission_block=block,
                is_open=entry.is_open,
                seconds_since_submission=elapsed,
            )
        )

    out: dict[str, list[Any]] = {}
    for protocol, watch_entries in by_protocol.items():
        try:
            verdicts = STRATEGY_RUNNER_HOOK_REGISTRY.resolve_perp_settlements(
                protocol=ProtocolName(protocol),
                gateway_client=gateway_client,
                chain=chain,
                wallet_address=wallet_address,
                watch_entries=tuple(watch_entries),
            )
        except Exception as exc:  # noqa: BLE001 — connector resolve failure ⇒ retry next tick
            logger.debug("perp settlement reconciler: resolve_perp_settlements failed for %s: %s", protocol, exc)
            continue
        if verdicts:
            out[protocol] = list(verdicts)
    return out


def _resolve_submission_block(web3: Any, tx_hash: str) -> int | None:
    """Resolve the createOrder tx's block number (the eth_getLogs lower bound)."""
    if not tx_hash:
        return None
    try:
        receipt = web3.eth.get_transaction_receipt(tx_hash)
        block = receipt.get("blockNumber") if isinstance(receipt, dict) else getattr(receipt, "blockNumber", None)
        return int(block) if block is not None else None
    except Exception as exc:  # noqa: BLE001 — unmeasured block ⇒ skip (never scan from genesis)
        logger.debug("perp settlement reconciler: submission block read failed for %s: %s", tx_hash, exc)
        return None


async def _commit_verdict(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    *,
    verdict: Any,
    entry: _WatchEntry,
    cycle_id: str,
    chain: str,
    protocol: str,
    wallet_address: str,
) -> Any | None:
    """Commit one terminal verdict, catching the accounting-persistence boundary."""
    try:
        outcome = await commit_perp_settlement(
            runner,
            strategy,
            verdict=verdict,
            submission_ledger=entry.ledger,
            is_open=entry.is_open,
            settlement_cycle_id=cycle_id,
            chain=chain,
            protocol=protocol,
            wallet_address=wallet_address,
        )
    except AccountingPersistenceError as exc:
        # Catch boundary (design §3 D2): loud + operator alert, entry stays
        # non-terminal (no row written → re-derived next tick), runner continues.
        logger.error(
            "perp settlement reconciler: PERP_SETTLEMENT persist FAILED for order=%s ledger=%s "
            "(write_kind=%s) — entry stays non-terminal, retried next tick: %s",
            entry.order_key,
            getattr(entry.ledger, "id", "?"),
            getattr(exc, "write_kind", "?"),
            exc,
        )
        return None
    except Exception as exc:  # noqa: BLE001 — never let one order's commit crash the pre-decide step
        logger.error(
            "perp settlement reconciler: unexpected commit failure for order=%s (continuing): %s",
            entry.order_key,
            exc,
            exc_info=True,
        )
        return None

    if outcome.booked:
        logger.info(
            "perp settlement booked: order=%s state=%s event=%s%s",
            entry.order_key,
            getattr(getattr(verdict, "state", None), "value", verdict.state if hasattr(verdict, "state") else "?"),
            outcome.event_id[:8],
            f" (degraded: {outcome.degraded_reason})" if outcome.accounting_degraded else "",
        )
    else:
        logger.error(
            "perp settlement reconciler: PERP_SETTLEMENT not booked for order=%s (non-live persist failure): %s",
            entry.order_key,
            outcome.degraded_reason,
        )
    return outcome


__all__ = ["PerpSettlementReconcileOutcome", "reconcile_perp_settlements"]
