"""AccountingProcessor — async outbox drain with typed event dispatch (VIB-3467).

Architecture:
  1. Execution hot path writes transaction_ledger + accounting_outbox, then fires
     asyncio.create_task(processor.drain_one(ledger_entry_id)).
  2. drain_one reads the outbox row, reads the ledger row, classifies the intent,
     dispatches to a category handler, and writes the accounting_events row.
  3. On runner startup, drain_pending() drains all pending/failed outbox rows so
     events written before a crash are not permanently lost.

Design constraints:
  - No live chain calls inside the processor.  All inputs come from the ledger row
    (extracted_data_json, price_inputs_json, pre_state_json, post_state_json).
  - Idempotent: drain_one on an already-processed row is a no-op — the outbox row
    is marked processed without re-writing the event or modifying the FIFO store.
  - The processor maintains its own FIFOBasisStore, reconstructed at startup via
    reconstruct_from_events() so REPAY / PT_REDEEM interest attribution is correct
    across restarts.

Idempotency (VIB-3478: _try_write_* legacy writers removed):
  - drain_one checks for an existing accounting_events row keyed on ledger_entry_id.
    If one exists the outbox row is marked processed without re-writing the event or
    modifying the FIFO store.
  - This guarantees the processor's FIFO store stays consistent with accounting_events
    regardless of restart timing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.accounting_treatment_registry import AccountingTreatmentRegistry
from almanak.framework.accounting.category_handlers import HANDLERS, HandlerContext
from almanak.framework.accounting.classifier import AccountingCategory, classify
from almanak.framework.accounting.models import LendingAccountingEvent
from almanak.framework.accounting.writer import AccountingWriter

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3


def _is_lending_event(event: Any) -> bool:
    """True iff the typed accounting event is a lending event (VIB-4977)."""
    return isinstance(event, LendingAccountingEvent)


def _position_event_field(row: Any, name: str) -> Any:
    """Read ``name`` from a position-event row regardless of dict / ORM shape.

    ``get_position_history`` returns dicts on the SQLite backend and may
    return ORM-ish objects on the gateway backend — tolerate both.
    """
    if isinstance(row, dict):
        return row.get(name)
    return getattr(row, name, None)


def _find_position_event_for_ledger(history: list[Any], ledger_entry_id: str) -> Any | None:
    """Return the position-event row whose ``ledger_entry_id`` matches.

    The ledger_entry_id is 1:1 between an accounting event and its position
    event for the same on-chain action (VIB-4977), so this isolates THIS
    action's row — a partial DECREASE never picks up the final CLOSE's row
    or vice versa.
    """
    for row in history:
        if _position_event_field(row, "ledger_entry_id") == ledger_entry_id:
            return row
    return None


def _merge_net_pnl(raw_attribution: str | dict[str, Any], net_pnl_usd: Decimal) -> str | None:
    """Merge ``net_pnl_usd`` into an existing attribution payload.

    Accepts either a JSON string (SQLite + gateway-proto backends return the
    ``attribution_json`` column as a string) OR an already-deserialized
    ``dict`` (some state-manager / DB layers auto-deserialize a JSON column).
    Tolerating both avoids a silent no-op: a dict reaching ``json.loads``
    would raise ``TypeError`` → caught → ``None`` → back-fill skips — the
    same silent-skip class made loud for the key-mismatch case. The input
    dict is copied (never mutated in place) so the caller's row object is
    untouched.

    Preserves every other key already on the payload (the ``lending_v1``
    after-state fields stamped at seed time). Returns ``None`` when the
    existing payload is not a JSON object (defensive — never clobber an
    unexpected shape).
    """
    if isinstance(raw_attribution, dict):
        parsed: Any = dict(raw_attribution)
    else:
        try:
            parsed = json.loads(raw_attribution) if raw_attribution else {}
        except (json.JSONDecodeError, TypeError):
            return None
    if not isinstance(parsed, dict):
        return None
    parsed["net_pnl_usd"] = str(net_pnl_usd)
    return json.dumps(parsed)


class AccountingProcessor:
    """Drains the accounting_outbox and writes typed accounting_events.

    Instantiated once per strategy runner, tied to a specific deployment_id.
    The FIFOBasisStore is shared with the runner so BORROW lot tracking is
    consistent between the legacy inline writers (during dual-write) and the
    processor path.
    """

    def __init__(
        self,
        state_manager: Any,
        basis_store: FIFOBasisStore,
        deployment_id: str = "",
    ) -> None:
        self._state_manager = state_manager
        self._basis_store = basis_store
        self._deployment_id = deployment_id
        self._writer = AccountingWriter(state_manager)

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    async def drain_one(self, ledger_entry_id: str) -> bool:
        """Process one outbox row end-to-end.

        Returns True when the row was processed (or was already done), False on
        unrecoverable failure (attempts >= MAX_RETRIES).

        Algorithm:
          1. Fetch outbox row by ledger_entry_id.
          2. Skip if already processed.
          3. Set status = 'processing'.
          4. Idempotency check: if accounting_events already has a row for this
             ledger_entry_id, mark processed and return (dual-write safe).
          5. Read ledger row.
          6. Classify intent.
          7. Dispatch to category handler → event (or None).
          8. Write event (if any).
          9. Mark outbox processed.

        Exceptions from category handlers or writer are caught; the row is
        marked 'failed' and retried up to _MAX_RETRIES times.
        """
        if not self._state_manager:
            return False

        try:
            outbox_row = await self._get_outbox_row(ledger_entry_id)
        except Exception:
            logger.warning("drain_one: failed to fetch outbox row for %s", ledger_entry_id, exc_info=True)
            return False

        if outbox_row is None:
            logger.debug("drain_one: no outbox row for ledger_entry_id=%s", ledger_entry_id)
            return False

        outbox_id = outbox_row["id"]
        status = outbox_row.get("status", "pending")

        if status == "processed":
            return True

        attempts = int(outbox_row.get("attempts", 0))
        if status == "failed" and attempts >= _MAX_RETRIES:
            logger.warning("drain_one: giving up on outbox row %s after %d attempts", outbox_id, attempts)
            return False

        # Mark as processing (prevents concurrent drain_one for the same row)
        await self._update_outbox(outbox_id, "processing")

        try:
            # Idempotency: if the legacy writers already wrote this event, skip.
            already_written = await self._has_accounting_event_for_ledger(ledger_entry_id)
            if already_written:
                await self._update_outbox(outbox_id, "processed")
                return True

            ledger_row = await self._get_ledger_row(ledger_entry_id)
            if ledger_row is None:
                raise ValueError(f"ledger row not found: {ledger_entry_id}")

            event = self._dispatch(outbox_row, ledger_row)

            if event is not None:
                ok = await self._writer.write(event)
                if not ok:
                    logger.debug(
                        "drain_one: writer.write returned False for %s (backend may not support write)",
                        ledger_entry_id,
                    )
                elif _is_lending_event(event):
                    # VIB-4977: back-fill the matching lending PositionEvent's
                    # attribution_json with the signed realized net_pnl_usd
                    # (the FIFO interest split this drain just computed). The
                    # position event was saved by the runner BEFORE this drain
                    # task fired, so the row exists. Best-effort: a failure
                    # degrades win-rate attribution but never blocks the books
                    # (mirrors run_attribution_on_close for LP/perp).
                    await self._backfill_lending_position_pnl(event)

            await self._update_outbox(outbox_id, "processed")
            return True

        except Exception as exc:
            new_attempts = attempts + 1
            logger.warning(
                "drain_one: error processing outbox row %s (attempt %d/%d): %s",
                outbox_id,
                new_attempts,
                _MAX_RETRIES,
                exc,
                exc_info=True,
            )
            await self._update_outbox(outbox_id, "failed", error=str(exc), attempts=new_attempts)
            return False

    async def drain_pending(self) -> int:
        """Drain all pending/failed outbox rows at startup.

        Processes rows sequentially to avoid flooding the state manager.
        Returns the count of rows successfully drained.
        """
        if not self._state_manager:
            return 0

        try:
            rows = await self._get_pending_rows()
        except Exception:
            logger.warning("drain_pending: failed to fetch pending rows", exc_info=True)
            return 0

        if not rows:
            return 0

        logger.info("drain_pending: found %d pending/failed outbox rows", len(rows))
        drained = 0
        for row in rows:
            ledger_entry_id = row.get("ledger_entry_id", "")
            if not ledger_entry_id:
                continue
            try:
                ok = await self.drain_one(ledger_entry_id)
                if ok:
                    drained += 1
            except Exception:
                logger.warning("drain_pending: uncaught error for ledger_entry_id=%s", ledger_entry_id, exc_info=True)
        if drained:
            logger.info("drain_pending: drained %d/%d rows", drained, len(rows))
        return drained

    # ──────────────────────────────────────────────────────────────────────────
    # Intent dispatch
    # ──────────────────────────────────────────────────────────────────────────

    def _dispatch(self, outbox_row: dict[str, Any], ledger_row: dict[str, Any]) -> Any:
        """Classify the intent and dispatch to the registered category handler.

        VIB-4163 (T3): the legacy if-ladder is replaced by a single registry
        lookup. Each handler module registers itself with ``@register(category)``
        at package import time; ``HANDLERS`` is then a dense
        ``dict[AccountingCategory, HandlerFn]``.

        VIB-4931: a connector-owned *treatment* stage runs first. A connector may
        publish (via ``AccountingTreatmentRegistry``) how to categorize and treat
        its own events; when it claims an event its treatment runs in place of the
        generic category handler and dispatch returns before ``classify`` /
        ``HANDLERS`` are consulted — keeping protocol-specific accounting (e.g.
        Pendle's LP / PT mechanics) in the connector rather than as protocol-named
        ``AccountingCategory`` members in the framework.

        Behaviour for ``NO_ACCOUNTING`` and for any (future) category whose
        handler is missing from the registry: return ``None`` (no event
        written). Missing-handler-for-classified-category emits an ERROR log
        line so the silent-degradation case is loud.
        """
        intent_type = ledger_row.get("intent_type") or ""
        protocol = ledger_row.get("protocol") or ""
        token_out = ledger_row.get("token_out") or ""

        ctx = HandlerContext(
            outbox_row=outbox_row,
            ledger_row=ledger_row,
            basis_store=self._basis_store,
            prior_open_lookup=self._lookup_prior_lp_open,
        )

        # Stage 1 — connector-owned accounting treatment (VIB-4931). A connector
        # publishes how to categorize + treat its own events (e.g. Pendle's LP / PT
        # mechanics) via the strategy-side registry, so the framework routes them
        # without naming the protocol or carrying a protocol-named AccountingCategory.
        # The first connector that claims the event wins; its treatment runs in place
        # of the generic category handler. If a connector claims an event but has no
        # treatment for the key it returned (a stale/typoed ``treatment_key`` — a
        # connector wiring bug), we log loudly and FALL THROUGH to the generic stage-2
        # path so the event is still accounted (generically) rather than silently
        # dropped (CodeRabbit review on #2598).
        decision = AccountingTreatmentRegistry.categorize(intent_type, protocol, token_out)
        if decision is not None:
            treatment = AccountingTreatmentRegistry.treatment_for(decision.treatment_key)
            if treatment is not None:
                return treatment(ctx)
            logger.error(
                "_dispatch: a connector claimed intent_type=%s protocol=%s as "
                "treatment=%s but no treatment is registered (ledger_entry_id=%s) — "
                "falling back to generic accounting dispatch",
                intent_type,
                protocol,
                decision.treatment_key,
                ledger_row.get("id") or "",
            )

        # Stage 2 — generic taxonomy dispatch via the category-handler registry.
        category = classify(intent_type, protocol, token_out)
        logger.debug(
            "drain_one: intent_type=%s protocol=%s → category=%s",
            intent_type,
            protocol,
            category,
        )

        if category == AccountingCategory.NO_ACCOUNTING:
            return None

        handler = HANDLERS.get(category)
        if handler is None:
            logger.error(
                "_dispatch: no handler registered for category=%s "
                "(ledger_entry_id=%s, intent_type=%s) — accounting event NOT written",
                category.value,
                ledger_row.get("id") or "",
                intent_type,
            )
            return None

        return handler(ctx)

    def _lookup_prior_lp_open(self, position_key: str, discriminator: str | None = None) -> dict[str, Any] | None:
        """Resolve the prior LP_OPEN payload for a closing LP position (VIB-4275).

        Used to compute ``realized_pnl_usd``, ``hodl_value_usd`` / ``il_usd``,
        and backfill tick metadata on LP_CLOSE / LP_COLLECT_FEES events.

        Resolution policy
        -----------------
        ``position_key`` is POOL-LEVEL — identical for every concurrent position
        in one pool. When a wallet holds N>1 positions in the same pool (the
        confirmed co-pool bug, deployment ``4d0fd01e``), the pool key alone
        cannot identify WHICH open belongs to the closing leg.

        * **Discriminator provided** (the closing leg's NFT token id): filter the
          same-``position_key`` candidate opens to those whose open payload
          carries a matching ``position_id``. Return the unique match.
          - exactly one match → that open.
          - zero matches, or more than one match → **None** (fail closed). The
            close cannot be attributed with certainty, so the handler emits
            ``None`` for the attribution-dependent money fields rather than
            guessing.
        * **No discriminator** (None / "") — non-CL / fungible-LP venues or a
          legacy row: resolve ONLY the single-open case. Exactly one prior open
          for this key → return it (the legacy 1:1 behaviour is preserved).
          Two or more opens with no discriminator to choose between them →
          **None** (fail closed).

        **NEVER falls back to "most-recent open by timestamp."** That is the
        VIB-4275 bug this method exists to remove — substituting a sibling /
        latest open under ambiguity cross-contaminated co-pool legs'
        hodl/IL/realized_pnl. When the closing position's own open cannot be
        identified with certainty, the answer is ``None`` (unmeasured, per
        Empty ≠ Zero) — never another leg's open.

        Returns the parsed ``payload_json`` dict, or ``None`` (no prior open,
        ambiguous, state manager lacks ``get_accounting_events_sync``, or a
        read-side error — fail-quiet: "no PnL number" beats "fabricated PnL").
        """
        if not position_key or not self._deployment_id:
            return None
        if not hasattr(self._state_manager, "get_accounting_events_sync"):
            return None
        try:
            events = self._state_manager.get_accounting_events_sync(self._deployment_id, position_key=position_key)
        except Exception as exc:  # noqa: BLE001
            logger.debug("prior LP_OPEN lookup failed for %s: %s", position_key, exc)
            return None

        # Single pass over the pool key's events (timestamp-ASC) to derive both
        # the full open set (for discriminator matching) and the ACTIVE open set
        # (for the legacy/no-id fallback). active is a stack: an LP_CLOSE retires
        # the lone live open when exactly one is active; a close while >=2 opens
        # are live is AMBIGUOUS without a per-position id (we cannot tell which
        # leg it closed) so we refuse the fallback rather than guess. A naive
        # FIFO drop would mis-assign OPEN A -> OPEN B -> CLOSE B and recreate
        # sibling attribution (Codex Finding 2 + CodeRabbit on #2459).
        # LP_COLLECT_FEES does not retire a position.
        opens: list[dict[str, Any]] = []
        active: list[dict[str, Any]] = []
        ambiguous_close = False
        for row in events or []:
            if self._is_lp_open(row):
                parsed = self._parse_open_payload(row)
                if parsed is not None:
                    opens.append(parsed)
                    active.append(parsed)
            elif self._is_lp_close(row):
                if len(active) == 1:
                    active.pop()
                elif len(active) >= 2:
                    ambiguous_close = True
                # len(active) == 0: a close with no live open we can see — ignore.
        if not opens:
            return None

        # Do ANY candidate opens carry a usable per-position id? If none do, the
        # data predates discriminator stamping (pre-fix migration window — Codex
        # Finding 1) or the venue is fungible: a close discriminator cannot match
        # it, so degrade to active-open resolution. If id-bearing opens DO exist
        # we trust the id match and fail closed on a miss (never guess a sibling).
        # 0 / "0" is normalized to "no discriminator" on BOTH sides (gemini +
        # CodeRabbit on #2459) — a real minted NFT id is a positive integer.
        disc = self._normalize_position_id(discriminator)
        opens_carry_id = any(self._normalize_position_id(p.get("position_id")) for p in opens)

        if disc and opens_carry_id:
            # NFT ids are unique, so match across ALL opens — a historical
            # (already-closed) open never aliases a live one.
            matches = [p for p in opens if self._normalize_position_id(p.get("position_id")) == disc]
            if len(matches) == 1:
                return matches[0]
            # Id-bearing data exists but the close's id matches 0 (genuine miss /
            # closing position's open absent) or >1 (duplicate ids — should not
            # happen) ⇒ fail closed.
            logger.error(
                "VIB-4275: LP close prior-open resolution found %d opens matching "
                "discriminator=%r for position_key=%s (need exactly 1); attributing "
                "to None rather than guessing a sibling/latest open",
                len(matches),
                disc,
                position_key,
            )
            return None

        # No usable discriminator (None/0, or every candidate open is legacy/no-id):
        # resolve ONLY the provably-unique single-active-open case. A close that
        # fired while >=2 opens were live makes the active set ambiguous ⇒ fail
        # closed; NEVER guess a sibling/latest.
        if not ambiguous_close and len(active) == 1:
            return active[0]
        logger.error(
            "VIB-4275: LP close prior-open resolution could not isolate a single active "
            "open for position_key=%s (%d historical opens, %d active, ambiguous_close=%s) "
            "and no usable discriminator; attributing to None rather than guessing a sibling/latest",
            position_key,
            len(opens),
            len(active),
            ambiguous_close,
        )
        return None

    @staticmethod
    def _is_lp_open(row: dict[str, Any]) -> bool:
        return (row.get("event_type") or "").upper() == "LP_OPEN"

    @staticmethod
    def _is_lp_close(row: dict[str, Any]) -> bool:
        return (row.get("event_type") or "").upper() == "LP_CLOSE"

    @staticmethod
    def _normalize_position_id(value: Any) -> str:
        """Canonicalize a per-position id to its discriminator string, or ``""``
        for "no discriminator".

        A real minted NFT id is a positive integer; ``None`` / ``""`` / ``0`` /
        ``"0"`` are degenerate and normalize to ``""`` so they never match — applied
        uniformly to the close-side discriminator AND the open-side ids (gemini +
        CodeRabbit on #2459), mirroring the open/close discriminator resolvers.
        """
        if value is None:
            return ""
        normalized = str(value).strip()
        return "" if normalized in ("", "0") else normalized

    @staticmethod
    def _parse_open_payload(row: dict[str, Any]) -> dict[str, Any] | None:
        payload = row.get("payload_json")
        if isinstance(payload, str):
            import json as _json

            try:
                parsed = _json.loads(payload)
            except (ValueError, TypeError):
                # Malformed stored payload_json is a data-integrity anomaly — we
                # serialized it. Skip this candidate open (fail-closed per
                # VIB-4275) but surface it: a silent swallow here would hide
                # ledger corruption on a money path.
                logger.warning(
                    "LP-open candidate has unparseable payload_json; skipping it (fail-closed)",
                    exc_info=True,
                )
                return None
            return parsed if isinstance(parsed, dict) else None
        if isinstance(payload, dict):
            return payload
        return None

    # ──────────────────────────────────────────────────────────────────────────
    # State manager helpers
    # ──────────────────────────────────────────────────────────────────────────

    async def _get_outbox_row(self, ledger_entry_id: str) -> dict[str, Any] | None:
        if hasattr(self._state_manager, "get_outbox_by_ledger_id"):
            return await self._call_async(self._state_manager.get_outbox_by_ledger_id, ledger_entry_id)
        return None

    async def _update_outbox(self, outbox_id: str, status: str, error: str = "", attempts: int | None = None) -> None:
        if hasattr(self._state_manager, "update_outbox_entry"):
            await self._call_async(self._state_manager.update_outbox_entry, outbox_id, status, error, attempts)

    async def _has_accounting_event_for_ledger(self, ledger_entry_id: str) -> bool:
        if hasattr(self._state_manager, "has_accounting_events_for_ledger"):
            return bool(await self._call_async(self._state_manager.has_accounting_events_for_ledger, ledger_entry_id))
        return False

    async def _get_ledger_row(self, ledger_entry_id: str) -> dict[str, Any] | None:
        if hasattr(self._state_manager, "get_ledger_entry_by_id"):
            return await self._call_async(self._state_manager.get_ledger_entry_by_id, ledger_entry_id)
        return None

    async def _get_pending_rows(self) -> list[dict[str, Any]]:
        if hasattr(self._state_manager, "get_outbox_pending"):
            rows = await self._call_async(
                self._state_manager.get_outbox_pending,
                self._deployment_id,
                _MAX_RETRIES,
            )
            return rows or []
        return []

    async def _backfill_lending_position_pnl(self, event: Any) -> None:
        """VIB-4977 — stamp signed realized ``net_pnl_usd`` onto the lending
        PositionEvent (Layer 3) whose attribution lane omitted it.

        The realized PnL is the FIFO interest split (``interest_delta_usd``)
        that the lending handler computed for THIS action; the position event
        was seeded earlier (Layer 3, before this drain task fired) with a
        ``lending_v1`` payload that has no ``net_pnl_usd`` key, so
        ``almanak strat pnl`` scored every lending close as unattributed
        (win rate ``0/0``).

        Join is on ``(position_key, ledger_entry_id)``:

        * ``position_key`` (Layer 5, ``_derive_position_key``) equals
          ``position_id`` (Layer 3, ``lending_position_id``) for BOTH
          NON-market-scoped (Aave-style) AND market-scoped (isolated)
          lending — Morpho Blue and friends. VIB-4981 aligned the two
          derivations: ``lending_position_id`` now inserts ``market_id``
          between wallet and asset when present, byte-identical to
          ``_derive_position_key`` (canonical form
          ``lending:chain:protocol:wallet:market_id:asset``; the segment is
          omitted, key unchanged, when ``market_id`` is falsy). The
          ``get_position_history(...)`` lookup keyed on the L5
          ``position_key`` therefore now finds the L3 row for Morpho closes
          too, so this back-fill stamps ``net_pnl_usd`` for the
          isolated-lending class as well (win rate no longer stuck 0/0). A
          remaining miss is logged at WARNING (not a silent skip) so any new
          divergence stays visible in prod log pipelines.
        * ``ledger_entry_id`` is 1:1 between the accounting event and the
          position event for the same on-chain action — so a partial DECREASE
          and the final CLOSE each get ONLY their own action's realized PnL.
          No summing, no double-counting; the win-rate scorer reads only the
          terminal CLOSE row (stamping a DECREASE row is inert across current
          readers, which filter to CLOSE — see ``strat_pnl``).

        ``net_pnl_usd is None`` (interest UNAVAILABLE — no matching FIFO lots,
        or a non-interest leg) ⇒ leave the payload untouched (Empty ≠ Zero).

        Best-effort: every failure is swallowed with a debug log. A missing
        back-fill degrades win-rate attribution but must never block the
        books or fail the drain (mirrors ``run_attribution_on_close``).
        """
        from almanak.framework.observability.position_events import lending_realized_net_pnl_usd

        try:
            event_type_value = getattr(event.event_type, "value", str(event.event_type))
            net_pnl = lending_realized_net_pnl_usd(event_type_value, event.interest_delta_usd)
            if net_pnl is None:
                return

            position_key = event.position_key or ""
            ledger_entry_id = event.identity.ledger_entry_id or ""
            deployment_id = event.identity.deployment_id or self._deployment_id
            if not position_key or not ledger_entry_id or not deployment_id:
                return

            if not hasattr(self._state_manager, "get_position_history"):
                return
            history = await self._call_async(self._state_manager.get_position_history, deployment_id, position_key)
            target = _find_position_event_for_ledger(history or [], ledger_entry_id)
            if target is None:
                # WARNING (not debug) so the gap is visible in prod log
                # pipelines — mirrors run_attribution_on_close's warning level.
                # VIB-4981 aligned the L3/L5 lending keys (market_id is now in
                # both), so the historical market-scoped (Morpho) divergence no
                # longer causes this miss. A remaining miss here means the L3
                # OPEN/INCREASE row genuinely isn't on disk yet (timing) or the
                # ledger_entry_id didn't propagate — left visible rather than
                # silently swallowed.
                logger.warning(
                    "_backfill_lending_position_pnl: no Layer-3 position event for "
                    "position_key=%s ledger_entry_id=%s — net_pnl_usd not stamped",
                    position_key,
                    ledger_entry_id,
                )
                return

            event_id = _position_event_field(target, "id")
            if not event_id:
                return
            raw_attr = _position_event_field(target, "attribution_json") or "{}"
            merged = _merge_net_pnl(raw_attr, net_pnl)
            if merged is None:
                return

            if hasattr(self._state_manager, "update_position_attribution"):
                version = _position_event_field(target, "attribution_version") or 1
                try:
                    version_int = int(version)
                except (TypeError, ValueError):
                    version_int = 1
                await self._call_async(
                    self._state_manager.update_position_attribution,
                    event_id,
                    merged,
                    version_int,
                    deployment_id,
                )
        except Exception:  # noqa: BLE001 — best-effort attribution back-fill
            logger.debug("_backfill_lending_position_pnl failed (non-blocking)", exc_info=True)

    @staticmethod
    async def _call_async(fn: Any, *args: Any) -> Any:
        """Call fn(*args), awaiting if it returns a coroutine."""
        result = fn(*args)
        if asyncio.iscoroutine(result):
            return await result
        return result


# ──────────────────────────────────────────────────────────────────────────────
# Outbox write helper (called from strategy_runner)
# ──────────────────────────────────────────────────────────────────────────────


async def write_outbox_entry(
    state_manager: Any,
    *,
    deployment_id: str,
    cycle_id: str,
    ledger_entry_id: str,
    intent_type: str,
    wallet_address: str,
    position_key: str = "",
    market_id: str = "",
) -> str | None:
    """Write a row to accounting_outbox and return the outbox row id.

    Best-effort: logs and returns None on all failures (including gateway not
    yet supporting save_outbox_entry — VIB-3482) so the execution path is not
    interrupted. No legacy _try_write_* fallback exists (removed in VIB-3478),
    so a None return means the accounting event will be lost until VIB-3482 ships.
    """
    if not state_manager or not ledger_entry_id:
        return None
    if not hasattr(state_manager, "save_outbox_entry"):
        return None
    now = datetime.now(UTC).isoformat()
    outbox_id = str(uuid.uuid4())
    try:
        fn = state_manager.save_outbox_entry
        result = fn(
            outbox_id,
            deployment_id,
            cycle_id,
            ledger_entry_id,
            intent_type,
            wallet_address,
            position_key,
            market_id,
            now,
        )
        if asyncio.iscoroutine(result):
            await result
        return outbox_id
    except NotImplementedError:
        # Re-raise so _write_outbox_and_fire_processor can distinguish
        # "backend not yet deployed" from a real write failure.
        raise
    except Exception as _e:
        from almanak.framework.state.exceptions import AccountingPersistenceError

        if isinstance(_e, AccountingPersistenceError):
            # Propagate with original error details so the runner can surface
            # the gRPC error code and server-side message to the operator.
            # Swallowing here would replace the cause with a generic synthetic message.
            raise
        logger.warning("write_outbox_entry: failed to persist outbox row", exc_info=True)
        return None
