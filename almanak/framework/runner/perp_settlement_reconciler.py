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
# Bound the per-tick ledger scan; the MINUS keeps the active (unsettled) set tiny.
_LEDGER_SCAN_LIMIT = 200
# Parse the order key / direction out of an ``AsyncOrderData`` repr string (the
# form ``serialize_extracted_data`` persists a ``list[AsyncOrderData]`` as, via
# ``json.dumps(default=str)``). Structured dicts (future-proof) are handled too.
_ORDER_ID_RE = re.compile(r"order_id=['\"](0x[0-9a-fA-F]{64})['\"]")
_IS_LONG_RE = re.compile(r"is_long=(True|False|None)")


async def reconcile_perp_settlements(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    *,
    deployment_id: str,
    cycle_id: str,
    gateway_client: Any,
) -> None:
    """One reconciler tick. Never raises (the catch boundary is internal)."""
    if gateway_client is None:
        # No gateway (paper/dry-run without a managed fork) — cannot read keeper
        # events; the watch set persists and is retried when a gateway is present.
        return

    chain = str(getattr(strategy, "chain", "") or getattr(runner.config, "chain", "") or "").lower()
    wallet_address = str(getattr(strategy, "wallet_address", "") or "")
    if not chain:
        return

    watch = await _derive_watch_set(runner, deployment_id)
    if not watch:
        return

    # Resolve keeper verdicts off the event loop (WI-2 resolve does gateway RPCs).
    verdicts_by_protocol = await asyncio.to_thread(
        _resolve_all_verdicts,
        gateway_client=gateway_client,
        chain=chain,
        wallet_address=wallet_address,
        watch=watch,
    )

    for protocol, verdicts in verdicts_by_protocol.items():
        for verdict in verdicts:
            entry = watch.get((protocol, str(getattr(verdict, "order_key", "")).lower()))
            if entry is None or not getattr(verdict, "terminal", False):
                continue  # unknown / still-PENDING / non-terminal UNMEASURED → keep watching
            await _commit_verdict(
                runner,
                strategy,
                verdict=verdict,
                entry=entry,
                cycle_id=cycle_id,
                chain=chain,
                protocol=protocol,
                wallet_address=wallet_address,
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


async def _derive_watch_set(runner: StrategyRunner, deployment_id: str) -> dict[tuple[str, str], _WatchEntry]:
    """Re-derive the pending-settlement watch set from persisted rows (no new storage).

    watch = {perp ledger rows with async-order keys} MINUS {rows that already have a
    terminal PERP_SETTLEMENT accounting event (joined by submission ledger id)}.
    """
    sm = runner.state_manager
    ledger_reader = getattr(sm, "get_ledger_entries", None)
    events_reader = getattr(sm, "get_accounting_events", None)
    if sm is None or not callable(ledger_reader) or not callable(events_reader):
        return {}

    # Ledger ids already settled (terminal PERP_SETTLEMENT event exists).
    settled_ledger_ids: set[str] = set()
    try:
        events = await events_reader(deployment_id, event_type="PERP_SETTLEMENT", limit=_LEDGER_SCAN_LIMIT)
        for ev in events or []:
            lid = ev.get("ledger_entry_id") if isinstance(ev, dict) else getattr(ev, "ledger_entry_id", None)
            if lid:
                settled_ledger_ids.add(str(lid))
    except Exception as exc:  # noqa: BLE001 — UNMEASURED settled-set ⇒ do not fabricate; skip this tick
        logger.debug("perp settlement reconciler: settled-event read failed (skipping tick): %s", exc, exc_info=True)
        return {}

    watch: dict[tuple[str, str], _WatchEntry] = {}
    for intent_type in _PERP_SUBMISSION_INTENTS:
        try:
            entries = await ledger_reader(deployment_id, intent_type=intent_type, limit=_LEDGER_SCAN_LIMIT)
        except Exception as exc:  # noqa: BLE001 — ledger read failure ⇒ skip this intent this tick
            logger.debug("perp settlement reconciler: ledger read failed for %s: %s", intent_type, exc, exc_info=True)
            continue
        for ledger in entries or []:
            ledger_id = str(getattr(ledger, "id", "") or "")
            if not ledger_id or ledger_id in settled_ledger_ids:
                continue
            if not bool(getattr(ledger, "success", False)):
                continue
            protocol = str(getattr(ledger, "protocol", "") or "").lower()
            if not protocol:
                continue
            is_open = intent_type == "PERP_OPEN"
            for order_key, is_long in _parse_async_orders(getattr(ledger, "extracted_data_json", "") or ""):
                watch[(protocol, order_key.lower())] = _WatchEntry(
                    order_key=order_key,
                    is_open=is_open,
                    is_long=is_long,
                    protocol=protocol,
                    ledger=ledger,
                    submission_tx_hash=str(getattr(ledger, "tx_hash", "") or ""),
                    submission_timestamp=_ledger_timestamp(ledger),
                )
    return watch


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
) -> None:
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
        return
    except Exception as exc:  # noqa: BLE001 — never let one order's commit crash the pre-decide step
        logger.error(
            "perp settlement reconciler: unexpected commit failure for order=%s (continuing): %s",
            entry.order_key,
            exc,
            exc_info=True,
        )
        return

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


__all__ = ["reconcile_perp_settlements"]
