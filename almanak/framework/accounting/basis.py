"""FIFO lot matching for interest and yield attribution.

Used for:
  - REPAY: match against BORROW lots to compute interest_paid
  - PT_REDEEM: match against PT_BUY lots to compute realized_yield

Policy is FIFO by (position_key, token). schema_version tracks the matching
policy so that future changes do not silently invalidate old records.

MATCHING_POLICY_VERSION must be bumped any time the matching algorithm changes.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

MATCHING_POLICY_VERSION = 3

_DECIMALS_18 = Decimal(10**18)


def _parse_decimal(value: Any) -> Decimal | None:
    """Safely parse a Decimal; return None on any conversion failure or non-finite value."""
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None
    # Reject NaN and Infinity — downstream comparisons (e.g. <= 0) raise
    # InvalidOperation for NaN and produce wrong results for infinities.
    return parsed if parsed.is_finite() else None


def _first_parsed_decimal(payload_dict: dict[str, Any], *keys: str) -> Decimal | None:
    """Return the first key whose payload value parses to a Decimal.

    Replaces ``_parse_decimal(...) or _parse_decimal(...) or ...`` chains
    that wrongly treat a parsed ``Decimal('0')`` as falsy and fall through
    to the next candidate (CodeRabbit 2026-05-04). Distinguishes "key
    absent / unparseable" (try next) from "key present and parsed to 0"
    (use it — measured zero is a valid USD basis).
    """
    for k in keys:
        parsed = _parse_decimal(payload_dict.get(k))
        if parsed is not None:
            return parsed
    return None


@dataclass
class _ReplayContext:
    """Normalised view of an accounting_events row used by per-type replay helpers."""

    event_type: str
    deployment_id: str
    position_key: str
    payload: dict[str, Any]
    timestamp: datetime | None
    swap_wallet_key: str
    ledger_entry_id: str | None
    # VIB-4487 audit Fold B: the row's chain (lowercased), used by
    # ``_replay_swap`` to canonicalize a persisted address-shaped token to
    # its symbol on read so OLD address-keyed acquisition lots match NEW
    # symbol-keyed disposals after a runner restart. Empty string when the
    # row carries no chain (resolution then no-ops and the raw value is used).
    chain: str = ""


@dataclass
class LotMatch:
    """Per-lot consumption record for FIFO reconstruction.

    consumed_quantity is in token units (not USD) — sufficient to reconstruct
    lot state on restart without a price oracle.  consumed_basis_usd is
    populated when the price was known at record time, None otherwise.
    """

    lot_id: str
    consumed_quantity: Decimal
    consumed_basis_usd: Decimal | None = None


@dataclass
class MatchResult:
    """Result of a FIFO match operation."""

    repaid_principal: Decimal
    interest_or_yield: Decimal
    lot_matches: list[LotMatch]
    unmatched_amount: Decimal
    matching_policy_version: int = MATCHING_POLICY_VERSION
    earliest_lot_timestamp: datetime | None = None


class FIFOBasisStore:
    """In-memory FIFO lot store backed by the accounting_events table.

    On runner startup, call reconstruct_from_events() with the full accounting_events
    history for the deployment to rebuild open lots from durable storage. This keeps
    FIFO interest and yield attribution correct across runner restarts.
    """

    def __init__(self) -> None:
        self._lots: dict[str, list[dict[str, Any]]] = {}

    def reconstruct_from_events(self, events: list[dict[str, Any]]) -> int:
        """Replay durable accounting events to rebuild open FIFO lots.

        Call once on runner startup with get_accounting_events_sync() results for
        the deployment, ordered by timestamp ASC (the default query order).

        Replays BORROW → REPAY pairs and PT_BUY → PT_SELL/PT_REDEEM pairs.
        Returns the number of lot operations replayed.

        Unrecognised event types are silently skipped so new types added in future
        schema versions do not break older runners replaying a mixed history.

        Policy v1 events (written before VIB-3484, lacking amount_token in the payload)
        cannot be reconstructed — each such event is skipped with a WARNING so callers
        know that the FIFO store may be incomplete after restart.
        """
        import logging as _logging

        _log = _logging.getLogger(__name__)

        self._lots.clear()

        replayed = 0
        v1_skipped: dict[str, int] = {}  # event_type → count, for aggregated warning at end

        for row in events:
            ctx = self._row_context(row)
            if ctx is None:
                continue
            replay = _REPLAY_DISPATCH.get(ctx.event_type)
            if replay is None:
                continue
            replayed += replay(self, ctx, v1_skipped, _log)

        if replayed:
            _log.info("FIFOBasisStore: reconstructed %d lot operations from accounting_events", replayed)
        if v1_skipped:
            total = sum(v1_skipped.values())
            breakdown = ", ".join(f"{k}={v}" for k, v in sorted(v1_skipped.items()))
            _log.warning(
                "FIFOBasisStore: skipped %d policy-v1 event(s) during reconstruction (%s) — "
                "amount_token missing (legacy schema); FIFO store may be incomplete on restart.",
                total,
                breakdown,
            )
        return replayed

    @staticmethod
    def _row_context(row: dict[str, Any]) -> _ReplayContext | None:
        """Parse a raw accounting_events row into a normalised replay context.

        Returns None when the row is missing required identity fields or has an
        unparseable payload — those rows are silently skipped by the caller.
        """
        import json as _json

        event_type = row.get("event_type", "")
        position_key = row.get("position_key", "")
        deployment_id = row.get("deployment_id", "")
        # Rows without a deployment identity cannot be keyed into the lot store.
        if not deployment_id:
            return None

        try:
            payload = _json.loads(row.get("payload_json") or "{}")
            # A non-object payload (JSON number/string/list) would raise
            # AttributeError at every ``payload.get`` below — treat it like
            # an unparseable payload and skip the row (Gemini review).
            if not isinstance(payload, dict):
                return None
        except Exception:  # noqa: BLE001
            return None

        # VIB-3964: derive the swap-key the BORROW / WITHDRAW credit was minted
        # under. The accounting_events row carries `chain` and `wallet_address`
        # at the top level, so the key is reconstructible without re-encoding it
        # in the payload.
        chain_norm = (row.get("chain") or "").lower().strip()
        wallet_norm = (row.get("wallet_address") or "").lower().strip()
        swap_wallet_key = f"swap:{chain_norm}:{wallet_norm}" if chain_norm and wallet_norm else ""

        # VIB-5010: the lot key is not always the row-level position_key.
        # SWAP events are persisted with position_key='' (a swap has no lasting
        # position — its FIFO key lives in payload.swap_position_key) and
        # prediction events may carry payload.position_key. Requiring the row
        # key here silently dropped every SWAP row, so no swap acquisition lot
        # survived a runner restart and post-restart disposals booked
        # realized_pnl=None / fully unmatched.
        #
        # The admission criterion mirrors each replay handler's ACTUAL key
        # source (pr-auditor: a gate broader than the handlers would let a
        # keyless lending/PT row through to ``record_borrow(position_key="")``
        # and mint a lot under the empty key):
        #   * SWAP         → payload.swap_position_key, else row key, else the
        #                    row-derivable swap:{chain}:{wallet}.
        #   * PREDICTION_* → payload.position_key, else row key.
        #   * everything else (lending / PT) → row position_key ONLY.
        if not position_key:
            if event_type == "SWAP":
                if not payload.get("swap_position_key") and not swap_wallet_key:
                    return None
            elif event_type.startswith("PREDICTION_"):
                if not payload.get("position_key"):
                    return None
            else:
                return None

        timestamp_str = row.get("timestamp")
        try:
            # Normalise UTC offset — Python <3.11 fromisoformat cannot parse trailing "Z"
            ts_norm = timestamp_str.replace("Z", "+00:00") if timestamp_str else None
            ts: datetime | None = datetime.fromisoformat(ts_norm) if ts_norm else None
        except (ValueError, TypeError):
            ts = None

        # ledger_entry_id links a lot back to the on-chain transaction (VIB-3468).
        ledger_entry_id: str | None = row.get("ledger_entry_id") or None

        return _ReplayContext(
            event_type=event_type,
            deployment_id=deployment_id,
            position_key=position_key,
            payload=payload,
            timestamp=ts,
            swap_wallet_key=swap_wallet_key,
            ledger_entry_id=ledger_entry_id,
            chain=chain_norm,
        )

    def _replay_borrow(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        raw_amount_token = ctx.payload.get("amount_token")
        amount_token = _parse_decimal(raw_amount_token)
        asset = ctx.payload.get("asset", "")
        # amount_token key absent → policy v1 event (pre-VIB-3484); count for summary.
        # amount_token present but non-positive → v2 extraction bug; skip silently (debug).
        if raw_amount_token is None:
            v1_skipped["BORROW"] = v1_skipped.get("BORROW", 0) + 1
            return 0
        if amount_token is None or amount_token <= 0:
            log.debug(
                "FIFOBasisStore: BORROW event %s/%s has non-positive amount_token — skipping",
                ctx.deployment_id,
                ctx.position_key,
            )
            return 0
        if not asset:
            return 0
        # Derive principal_usd from payload so the swap-key replay lot has
        # the same cost basis as the live-write path. Falls back to None
        # when the payload predates VIB-3964 — the lot still mints, just
        # without basis (downstream disposals will return cost_basis=None).
        borrowed_amount_usd = _first_parsed_decimal(
            ctx.payload, "borrowed_amount_usd", "amount_usd", "principal_delta_usd"
        )
        self.record_borrow(
            deployment_id=ctx.deployment_id,
            position_key=ctx.position_key,
            token=asset,
            principal_amount=amount_token,
            principal_usd=borrowed_amount_usd,
            timestamp=ctx.timestamp,
            source_ledger_entry_id=ctx.ledger_entry_id,
        )
        # VIB-3964: the borrowed token also lands in the wallet — mint a
        # swap-key acquisition lot so a SWAP that disposes the borrowed
        # token can compute realized PnL instead of returning null.
        if ctx.swap_wallet_key:
            self.record_swap_acquisition(
                deployment_id=ctx.deployment_id,
                position_key=ctx.swap_wallet_key,
                token=asset,
                amount=amount_token,
                cost_usd=borrowed_amount_usd,
                timestamp=ctx.timestamp,
                source="BORROW",
            )
        return 1

    def _replay_withdraw(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        # VIB-3964: WITHDRAW credits the wallet with collateral (principal +
        # accrued supply interest). Two basis-store effects:
        #   1. Mirror it as a swap-key acquisition lot so a follow-up SWAP
        #      that disposes the withdrawn token (e.g. the USDC→USDT
        #      swap-back leg of a teardown) gets a non-null basis.
        #   2. Match against the supply-key principal lots so interest
        #      accrued (withdraw - principal) is reconstructable on restart.
        raw_amount_token = ctx.payload.get("amount_token") or ctx.payload.get("amount")
        amount_token = _parse_decimal(raw_amount_token)
        asset = ctx.payload.get("asset", "")
        if amount_token is None or amount_token <= 0 or not asset:
            return 0
        # CodeRabbit 2026-05-04: the live writer (lending_handler.py)
        # mints the wallet-basis lot at the FULL withdraw USD value,
        # not just the matched-principal portion (which becomes
        # ``principal_delta_usd`` after the split fix above). Replay
        # must use the same total or a SWAP that disposes the
        # withdrawn token after a runner restart computes a different
        # ``realized_pnl_usd`` than the live path. Read ``amount_usd``
        # if present (preferred); otherwise reconstruct the total as
        # ``principal_delta_usd + interest_delta_usd`` so post-split
        # payloads still replay correctly.
        withdraw_amount_usd = _parse_decimal(ctx.payload.get("amount_usd"))
        if withdraw_amount_usd is None:
            principal_usd = _parse_decimal(ctx.payload.get("principal_delta_usd"))
            interest_usd = _parse_decimal(ctx.payload.get("interest_delta_usd"))
            if principal_usd is not None:
                withdraw_amount_usd = principal_usd + (interest_usd or Decimal("0"))
        if ctx.swap_wallet_key:
            self.record_swap_acquisition(
                deployment_id=ctx.deployment_id,
                position_key=ctx.swap_wallet_key,
                token=asset,
                amount=amount_token,
                cost_usd=withdraw_amount_usd,
                timestamp=ctx.timestamp,
                source="WITHDRAW",
            )
        # Symmetric to BORROW/REPAY: drain the supply-key principal lots.
        self.match_repay(
            deployment_id=ctx.deployment_id,
            position_key=f"supply:{ctx.position_key}",
            token=asset,
            repay_amount=amount_token,
        )
        return 1

    def _replay_supply(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        # VIB-3964: SUPPLY removes wallet inventory (the supplied token
        # leaves the wallet for the lending pool). Two basis-store effects:
        #   1. Dispose the swap-key acquisition lot so phantom inventory
        #      doesn't bleed into a later WITHDRAW-then-SWAP.
        #   2. Record a principal lot under supply:<position_key> so a
        #      future WITHDRAW can FIFO-match and surface accrued interest.
        raw_amount_token = ctx.payload.get("amount_token") or ctx.payload.get("amount")
        amount_token = _parse_decimal(raw_amount_token)
        asset = ctx.payload.get("asset", "")
        if amount_token is None or amount_token <= 0 or not asset:
            return 0
        if ctx.swap_wallet_key:
            self.match_swap_disposal(
                deployment_id=ctx.deployment_id,
                position_key=ctx.swap_wallet_key,
                token=asset,
                amount=amount_token,
            )
        supply_amount_usd = _first_parsed_decimal(ctx.payload, "amount_usd", "principal_delta_usd")
        self.record_borrow(
            deployment_id=ctx.deployment_id,
            position_key=f"supply:{ctx.position_key}",
            token=asset,
            principal_amount=amount_token,
            principal_usd=supply_amount_usd,
            timestamp=ctx.timestamp,
            source_ledger_entry_id=ctx.ledger_entry_id,
        )
        return 1

    def _replay_repay(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        # DELEVERAGE is structurally a repay — it reduces an open borrow lot.
        raw_amount_token = ctx.payload.get("amount_token")
        amount_token = _parse_decimal(raw_amount_token)
        asset = ctx.payload.get("asset", "")
        if raw_amount_token is None:
            v1_skipped[ctx.event_type] = v1_skipped.get(ctx.event_type, 0) + 1
            return 0
        if amount_token is None or amount_token <= 0:
            log.debug(
                "FIFOBasisStore: %s event %s/%s has non-positive amount_token — skipping",
                ctx.event_type,
                ctx.deployment_id,
                ctx.position_key,
            )
            return 0
        if not asset:
            return 0
        self.match_repay(
            deployment_id=ctx.deployment_id,
            position_key=ctx.position_key,
            token=asset,
            repay_amount=amount_token,
        )
        # VIB-3964: REPAY also drains wallet inventory of the repaid
        # token. Dispose the swap-key acquisition lot so the wallet basis
        # pool stays consistent with actual on-chain wallet balance.
        if ctx.swap_wallet_key:
            self.match_swap_disposal(
                deployment_id=ctx.deployment_id,
                position_key=ctx.swap_wallet_key,
                token=asset,
                amount=amount_token,
            )
        return 1

    def _replay_pt_buy(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        pt_token = ctx.payload.get("pt_token", "")
        if not pt_token:
            return 0
        # PT_BUY stores HUMAN amounts (the uniform PT payload convention) — read
        # directly, no /1e18 (the builder converts raw→human before persisting).
        pt_human = _parse_decimal(ctx.payload.get("pt_amount"))
        sy_human = _parse_decimal(ctx.payload.get("sy_amount"))
        if pt_human is None or sy_human is None:
            return 0
        if pt_human <= 0:
            return 0
        # VIB-5316: the buy-time underlying/USD price (``sy_price`` on the PendleAccountingEvent
        # payload) anchors the USD cost basis of the held PT. ``None`` for pre-fix
        # persisted lots (the field was always-None before this fix) → the lot carries
        # no buy-time price and its USD cost stays unmeasured (Empty ≠ Zero). NEVER
        # re-marked at the current underlying price.
        sy_price = _parse_decimal(ctx.payload.get("sy_price"))
        self.record_pt_buy(
            deployment_id=ctx.deployment_id,
            position_key=ctx.position_key,
            pt_token=pt_token,
            pt_amount=pt_human,
            sy_cost=sy_human,
            sy_price=sy_price,
            timestamp=ctx.timestamp,
            source_ledger_entry_id=ctx.ledger_entry_id,
        )
        return 1

    def _replay_pt_sell(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        # PT_SELL stores HUMAN amounts (the uniform PT payload convention, same as
        # PT_BUY / PT_REDEEM) — read directly, no /1e18.
        pt_token = ctx.payload.get("pt_token", "")
        if not pt_token:
            return 0
        pt_human = _parse_decimal(ctx.payload.get("pt_amount"))
        if pt_human is None or pt_human <= 0:
            return 0
        sy_human = _parse_decimal(ctx.payload.get("sy_amount"))
        # sy_amount is required for PT_SELL: it's the actual market proceeds.
        # Defaulting to pt_amount (1:1 assumption) would invent cost-basis data.
        if sy_human is None or sy_human <= 0:
            return 0
        self.match_pt_redeem(
            deployment_id=ctx.deployment_id,
            position_key=ctx.position_key,
            pt_token=pt_token,
            pt_redeemed=pt_human,
            sy_received=sy_human,
        )
        return 1

    def _replay_pt_redeem(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        pt_token = ctx.payload.get("pt_token", "")
        if not pt_token:
            return 0
        # PT_REDEEM stores HUMAN amounts (the uniform PT payload convention, same
        # as PT_BUY / PT_SELL) — read directly, no /1e18. When py_redeemed was
        # missing from the receipt, pt_amount is None and the builder fell back to
        # sy_amount — mirror that fallback here.
        pt_raw = _parse_decimal(ctx.payload.get("pt_amount"))
        sy_raw = _parse_decimal(ctx.payload.get("sy_amount"))
        if pt_raw is not None:
            pt_human = pt_raw
        elif sy_raw is not None:
            pt_human = sy_raw
        else:
            return 0
        if pt_human <= 0:
            return 0
        sy_human = sy_raw if sy_raw is not None else pt_human
        self.match_pt_redeem(
            deployment_id=ctx.deployment_id,
            position_key=ctx.position_key,
            pt_token=pt_token,
            pt_redeemed=pt_human,
            sy_received=sy_human,
        )
        return 1

    def _replay_swap(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        # Position key for swap lots is stored in the payload (swap:<chain>:<wallet>).
        # Fall back to row position_key for events written before VIB-3473, then
        # to the row-derived swap-wallet key (VIB-5010): by construction the
        # live write path keys swap lots under swap:{chain}:{wallet}, which is
        # exactly ctx.swap_wallet_key, so a row whose payload predates
        # swap_position_key still replays under the correct key.
        swap_position_key = ctx.payload.get("swap_position_key") or ctx.position_key or ctx.swap_wallet_key
        if not swap_position_key:
            return 0

        # VIB-4487 audit Fold B — retroactive FIFO-key healing.
        #
        # Pre-VIB-4487 the 4 address-emitting connectors persisted a raw
        # contract address in the SWAP payload's ``token_in`` / ``token_out``.
        # Replaying those verbatim keys the lot under the address (lowercased
        # by ``_key``), so a NEW symbol-keyed disposal written post-upgrade
        # would orphan it (unmatched basis → realized_pnl None). Run the SAME
        # canonical resolution the live path now uses on the persisted token
        # before keying, so an OLD address-keyed acquisition lot resolves to
        # its symbol on read and matches the new symbol-keyed disposal — the
        # fix becomes retroactive and the upgrade transition window vanishes.
        #
        # The resolver fast path is cache + static registry (``skip_gateway``
        # inside the helper), so this works offline at boot. A payload already
        # carrying a symbol passes through unchanged (idempotent); a row with
        # no chain no-ops back to the raw value (no regression vs. today).
        from almanak.connectors._strategy_base.base import resolve_swap_token_symbol

        # 1. Replay disposal of token_in to consume any prior acquisition lots,
        #    keeping the FIFO store consistent with the state before this swap.
        token_in_r = resolve_swap_token_symbol(ctx.payload.get("token_in", ""), ctx.chain) or ""
        amount_in_r = _parse_decimal(ctx.payload.get("amount_in"))
        if token_in_r and amount_in_r is not None and amount_in_r > 0:
            self.match_swap_disposal(
                deployment_id=ctx.deployment_id,
                position_key=swap_position_key,
                token=token_in_r,
                amount=amount_in_r,
            )

        # 2. Replay acquisition lot for token_out so future disposals can match it.
        token_out = resolve_swap_token_symbol(ctx.payload.get("token_out", ""), ctx.chain) or ""
        if not token_out:
            return 0
        amount_out = _parse_decimal(ctx.payload.get("amount_out"))
        if amount_out is None or amount_out <= 0:
            return 0
        cost_usd = _parse_decimal(ctx.payload.get("amount_out_usd"))
        self.record_swap_acquisition(
            deployment_id=ctx.deployment_id,
            position_key=swap_position_key,
            token=token_out,
            amount=amount_out,
            cost_usd=cost_usd,
            timestamp=ctx.timestamp,
        )
        return 1

    def _replay_prediction(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        # Replay prediction-market aggregate from the post-trade
        # snapshot stored on the event (position_size_after,
        # position_basis_after). We assign the snapshot directly
        # rather than re-applying record_prediction_buy /
        # match_prediction_sell because:
        #   1. Events are processed in timestamp ASC order — the
        #      latest event for a (market, outcome) wins.
        #   2. A snapshot-based replay survives missed intermediate
        #      events (e.g. policy-v1 records) without compounding
        #      drift from re-derivation.
        pos_key = ctx.payload.get("position_key") or ctx.position_key
        if not pos_key:
            return 0
        size_after = _parse_decimal(ctx.payload.get("position_size_after"))
        basis_after = _parse_decimal(ctx.payload.get("position_basis_after"))
        if size_after is None or basis_after is None:
            return 0
        # #2146: restore the VIB-3710 loaded-extras accumulator from the
        # post-trade snapshot. Without it, a cross-restart SELL/REDEEM prices
        # realized PnL against bare basis and overstates it by Σ loaded_extras.
        # Legacy payloads (pre-field) parse as None -> treated as zero extras,
        # preserving their prior arithmetic.
        extras_after = _parse_decimal(ctx.payload.get("position_loaded_extras_after")) or Decimal("0")
        k = self._prediction_key(ctx.deployment_id, pos_key)
        # Position closed (zero size) — drop the row entirely so
        # match_prediction_sell on a closed position correctly
        # returns "no prior basis" rather than a stale zero row.
        if size_after <= Decimal("0"):
            self._lots.pop(k, None)
        else:
            self._lots[k] = [
                {
                    "kind": "prediction",
                    "size": size_after,
                    "basis": basis_after,
                    "loaded_extras": extras_after,
                }
            ]
        return 1

    def _key(self, deployment_id: str, position_key: str, token: str) -> str:
        return f"{deployment_id}:{position_key}:{token.lower()}"

    def record_borrow(
        self,
        deployment_id: str,
        position_key: str,
        token: str,
        principal_amount: Decimal,
        timestamp: datetime | None = None,
        principal_usd: Decimal | None = None,
        lot_id: str | None = None,
        source_ledger_entry_id: str | None = None,
    ) -> str:
        lot_id = lot_id or str(uuid.uuid4())
        key = self._key(deployment_id, position_key, token)
        if key not in self._lots:
            self._lots[key] = []
        self._lots[key].append(
            {
                "lot_id": lot_id,
                "principal": principal_amount,
                "remaining": principal_amount,
                "timestamp": (timestamp or datetime.now(UTC)).isoformat(),
                "price_usd_per_token": (
                    principal_usd / principal_amount if principal_usd is not None and principal_amount else None
                ),
                "source_ledger_entry_id": source_ledger_entry_id,
            }
        )
        return lot_id

    def match_repay(
        self,
        deployment_id: str,
        position_key: str,
        token: str,
        repay_amount: Decimal,
    ) -> MatchResult:
        """FIFO match repay_amount against open borrow lots.

        Returns the principal component consumed and the interest (excess over principal).

        No-lot case: if no lots exist for this key, the entire repay_amount is
        returned as unmatched_amount with interest_or_yield = 0. Missing basis
        data must never be fabricated as realized interest — unmatched_amount
        signals to the caller that the interest figure is UNAVAILABLE.
        """
        key = self._key(deployment_id, position_key, token)
        lots = self._lots.get(key, [])

        if not lots:
            return MatchResult(
                repaid_principal=Decimal("0"),
                interest_or_yield=Decimal("0"),
                lot_matches=[],
                unmatched_amount=repay_amount,
            )

        remaining_repay = repay_amount
        principal_consumed = Decimal("0")
        lot_matches: list[LotMatch] = []

        for lot in lots:
            if remaining_repay <= 0:
                break
            available = lot["remaining"]
            if available <= 0:
                continue
            consume = min(available, remaining_repay)
            lot["remaining"] -= consume
            principal_consumed += consume
            remaining_repay -= consume
            price = lot.get("price_usd_per_token")
            consumed_basis_usd = (Decimal(str(price)) * consume) if price is not None else None
            lot_matches.append(
                LotMatch(lot_id=lot["lot_id"], consumed_quantity=consume, consumed_basis_usd=consumed_basis_usd)
            )

        # interest = excess of repayment over total outstanding principal consumed.
        # If repay_amount <= total outstanding principal, interest = 0 (partial repay).
        # If repay_amount > total outstanding principal, excess = interest paid.
        # If principal_consumed == 0 (all lots exhausted), treat as unmatched —
        # interest cannot be attributed without a consumed principal basis.
        interest = max(Decimal("0"), repay_amount - principal_consumed) if principal_consumed > 0 else Decimal("0")
        unmatched = repay_amount if principal_consumed == 0 else Decimal("0")
        return MatchResult(
            repaid_principal=principal_consumed,
            interest_or_yield=interest,
            lot_matches=lot_matches,
            unmatched_amount=unmatched,
        )

    def record_pt_buy(
        self,
        deployment_id: str,
        position_key: str,
        pt_token: str,
        pt_amount: Decimal,
        sy_cost: Decimal,
        sy_price: Decimal | None = None,
        timestamp: datetime | None = None,
        lot_id: str | None = None,
        source_ledger_entry_id: str | None = None,
    ) -> str:
        lot_id = lot_id or str(uuid.uuid4())
        key = self._key(deployment_id, position_key, pt_token)
        if key not in self._lots:
            self._lots[key] = []
        self._lots[key].append(
            {
                "lot_id": lot_id,
                # ``pt_token`` (the PT symbol) is stamped on the lot so the
                # read-only inventory accessor (:meth:`iter_open_pt_lots`,
                # VIB-5316) can yield the symbol — the identity + join +
                # FIFO-match key (spine §3.1) — WITHOUT colon-splitting the
                # composite store key (whose ``deployment_id`` segment itself
                # contains a colon, making the split ambiguous). Preserves the
                # original case for display.
                "pt_token": pt_token,
                "pt_amount": pt_amount,
                "sy_cost": sy_cost,
                # VIB-5316: the underlying/USD price captured AT BUY TIME (the
                # PendleAccountingEvent ``sy_price``). The held-PT USD cost basis is
                # ``cost_per_pt × remaining_pt × underlying_price_at_buy`` — anchored to
                # this price, NOT re-marked at the current underlying (which sign-flips
                # unrealized PnL for volatile underlyings). ``None`` = unmeasured buy
                # price (pre-fix lot or missing ``price_inputs_json``) → USD cost stays
                # unmeasured downstream (Empty ≠ Zero); never substitute the current price.
                "underlying_price_at_buy": sy_price,
                "remaining_pt": pt_amount,
                "cost_per_pt": sy_cost / pt_amount if pt_amount else Decimal("0"),
                "timestamp": (timestamp or datetime.now(UTC)).isoformat(),
                "source_ledger_entry_id": source_ledger_entry_id,
            }
        )
        return lot_id

    def match_pt_redeem(
        self,
        deployment_id: str,
        position_key: str,
        pt_token: str,
        pt_redeemed: Decimal,
        sy_received: Decimal,
    ) -> MatchResult:
        """FIFO match a PT disposal (sell / redeem) against open PT buy lots.

        ``realized_yield`` (``interest_or_yield``) is the SY/underlying-denominated
        yield on the **matched** quantity only:

            realized_yield = proceeds_for_matched_qty − cost_basis_of_matched_qty

        On a PARTIAL disposal — where ``pt_redeemed`` exceeds the open lots'
        available PT (``unmatched_amount > 0``) — ``sy_received`` is the proceeds
        for the FULL disposed quantity, but only ``matched_pt`` of it has a tracked
        cost lot. Attributing the FULL ``sy_received`` against the cost of only the
        matched lots overstates realized yield by the proceeds of the unmatched PT
        (VIB-5377). We therefore PRO-RATE ``sy_received`` to the matched fraction,
        exactly mirroring the SWAP partial-match contract (``_split_proceeds`` in
        ``swap_handler.py``, VIB-4905): the matched portion gets its proportional
        proceeds; the residual lots stay open with their correct residual basis.

        When nothing matched (``lot_matches`` empty), ``interest_or_yield`` is
        ``Decimal("0")`` but is NOT surfaced — the consumer
        (``connectors.pendle.accounting_spec._realized_yield_from_match``) returns
        ``(None, None)`` on empty ``lot_matches`` (Empty ≠ Zero: realized yield on
        an unmatched disposal is UNMEASURED, never a fabricated zero).
        """
        key = self._key(deployment_id, position_key, pt_token)
        lots = self._lots.get(key, [])
        remaining = pt_redeemed
        original_cost = Decimal("0")
        lot_matches: list[LotMatch] = []
        earliest_ts: datetime | None = None

        for lot in lots:
            if remaining <= 0:
                break
            available = lot.get("remaining_pt", Decimal("0"))
            if available <= 0:
                continue
            consume = min(available, remaining)
            lot["remaining_pt"] -= consume
            cost_share = lot["cost_per_pt"] * consume
            original_cost += cost_share
            remaining -= consume
            lot_matches.append(LotMatch(lot_id=lot["lot_id"], consumed_quantity=consume))
            ts_str = lot.get("timestamp")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str)
                    if earliest_ts is None or ts < earliest_ts:
                        earliest_ts = ts
                except (ValueError, TypeError):
                    pass

        # Matched PT quantity = disposed − unmatched. Pro-rate the disposal's
        # proceeds to the matched fraction so a partial disposal books yield only
        # on the lots it actually consumed (VIB-5377). For a full match
        # (``remaining == 0``) ``proceeds_for_matched == sy_received`` exactly, so
        # this is a no-op on the existing full-disposal path.
        matched_pt = pt_redeemed - remaining
        if matched_pt > 0 and pt_redeemed > 0:
            proceeds_for_matched = sy_received * (matched_pt / pt_redeemed)
            realized_yield = proceeds_for_matched - original_cost
        else:
            # Nothing matched: yield is unmeasured (Empty ≠ Zero). ``lot_matches``
            # is empty, so the consumer never surfaces this value.
            realized_yield = Decimal("0")
        return MatchResult(
            repaid_principal=original_cost,
            interest_or_yield=realized_yield,
            lot_matches=lot_matches,
            unmatched_amount=max(Decimal("0"), remaining),
            earliest_lot_timestamp=earliest_ts,
        )

    def record_swap_acquisition(
        self,
        deployment_id: str,
        position_key: str,
        token: str,
        amount: Decimal,
        cost_usd: Decimal | None = None,
        timestamp: datetime | None = None,
        lot_id: str = "",
        source: str = "SWAP",
    ) -> str:
        """Record a wallet-basis acquisition lot for an inbound token.

        Called after a SWAP credits ``token_out`` (default ``source="SWAP"``) AND
        — since VIB-3964 — after a BORROW or WITHDRAW deposits a borrowed/withdrawn
        token into the wallet (``source="BORROW"`` / ``source="WITHDRAW"``).

        The wallet pool is fungible across SWAP and lending lanes: a USDT BORROW
        and a USDT acquired from SWAP both land in the same wallet inventory and
        either can be spent by a later SWAP / SUPPLY / REPAY. Mirroring that here
        keeps ``match_swap_disposal`` from returning ``None`` when the disposed
        token came from a BORROW or WITHDRAW (the case that left looping
        reconciliation G6 RED with ``Σ_swaps_usd_null_count >= 1``).

        ``source`` is stamped on the lot for forensic / L6 attribution; the
        matching path itself is source-agnostic.
        """
        effective_lot_id = lot_id or str(uuid.uuid4())
        key = self._key(deployment_id, position_key, token)
        if key not in self._lots:
            self._lots[key] = []
        self._lots[key].append(
            {
                "lot_id": effective_lot_id,
                "amount": amount,
                "remaining": amount,
                "cost_usd": cost_usd,
                "source": source,
                "timestamp": (timestamp or datetime.now(UTC)).isoformat(),
            }
        )
        return effective_lot_id

    def seed_wallet_inventory(
        self,
        deployment_id: str,
        swap_position_key: str,
        rows: list[dict[str, Any]],
        *,
        timestamp: datetime | None = None,
        source: str = "OPENING_BALANCE",
    ) -> int:
        """Seed pre-existing wallet inventory as wallet-basis FIFO lots (VIB-4394).

        FIFO lot replay (:meth:`reconstruct_from_events`) only mints lots from
        SWAP / BORROW / WITHDRAW / PT events. Inventory the wallet already held
        *before* the strategy started never produced such an event, so the first
        disposal of that inventory via a SWAP had no lot to consume →
        ``match_swap_disposal`` returned ``(None, amount)`` → ``realized_pnl=None``
        → the Accountant Test G6 reconciliation conflated a legitimate
        no-prior-basis state with a measurement gap and FAILed the cell.

        This seeds one ``source="OPENING_BALANCE"`` wallet-basis lot per token in
        the strategy's earliest portfolio snapshot, AFTER event replay, so the
        first disposal of opening inventory realizes against a basis.

        ``rows`` are the parsed first-snapshot ``wallet_balances_json`` entries
        (``{symbol, balance, price_usd}``); the caller builds them from
        ``PortfolioSnapshot.wallet_balances`` and supplies the
        ``swap:<chain>:<wallet>`` key (the snapshot carries the chain but not the
        wallet — see ``_run_loop_helpers.reconstruct_lending_basis_store``).

        Cost-basis convention (blueprint 27 §7.11.1 Consumer-A; §11.5): the lot's
        ``cost_usd`` is ``balance × price_usd`` — the first-observed snapshot
        price, the SAME mark the wallet-equity method already used at the initial
        endpoint. Realizing a later disposal against it yields the genuine
        boot→disposal price move, symmetric with the §11.5 ambient lane which
        marks *untraded* idle inventory boot→final.

        Empty ≠ Zero: when ``price_usd`` is absent / unparseable the lot's
        ``cost_usd`` is ``None`` (unmeasured) — NEVER ``Decimal("0")``, which
        would fabricate a 100%-gain on first disposal. The lot is still seeded
        (quantity known, basis ``None``): ``match_swap_disposal`` handles a
        ``cost_usd=None`` lot by returning ``(None, remaining)``, giving the
        disposal a quantity to consume while keeping ``realized_pnl`` honestly
        ``None``. A row whose ``balance`` is absent / unparseable / ``<= 0`` is
        skipped — there is no inventory to seed.

        Source tag & iterator behaviour: ``source="OPENING_BALANCE"`` mirrors the
        VIB-3964 BORROW / WITHDRAW source split. The lot lands under the same
        fungible ``swap:<chain>:<wallet>`` key (so a later SWAP disposal matches —
        matching is source-agnostic), is EXCLUDED from :meth:`iter_open_swap_lots`
        (the VIB-4984 swap-trading dashboard tile is SWAP-source only — opening
        inventory is not swap-trading PnL) and INCLUDED in
        :meth:`iter_open_wallet_basis_lots` (it IS tracked wallet inventory for the
        teardown clamp).

        Restart idempotency (de-dup against prior OPENING_BALANCE lots only): the
        snapshot reflects the boot balance, which is immutable across restarts.
        OPENING_BALANCE lots are NOT persisted as accounting events, so they are
        re-seeded from the snapshot on every boot rather than replayed. The de-dup
        therefore suppresses only the quantity already held by a PRIOR
        ``OPENING_BALANCE`` seed of this same snapshot (floored at zero) — making
        a repeated seed a no-op. It deliberately does NOT net against replayed
        SWAP/BORROW/WITHDRAW lots: those are post-boot deltas orthogonal to the
        boot balance (a post-boot acquisition is additive; a post-boot disposal
        of opening inventory already consumed lots during replay). Netting the
        opening balance against them would under-seed (additive acquisition) or
        leave a re-seeded balance after disposal. Idempotent: safe to call on
        every boot.

        Returns the number of lots seeded.
        """
        seeded = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym = canonical_symbol(row.get("symbol"))
            if not sym:
                continue
            balance = _parse_decimal(row.get("balance"))
            # Empty ≠ Zero: a missing / unparseable / non-positive balance is no
            # seedable inventory (a measured zero balance is likewise nothing).
            if balance is None or balance <= 0:
                continue

            # Same-boot idempotency de-dup: never re-seed quantity already
            # represented by a PRIOR OPENING_BALANCE seed of this same boot
            # snapshot. Only ``source == OPENING_BALANCE`` lots are counted —
            # NOT replayed SWAP/BORROW/WITHDRAW lots. Those are post-boot deltas
            # orthogonal to the immutable boot balance: a SWAP that ACQUIRED
            # more of the token is additive (it must not suppress opening basis),
            # and a SWAP that DISPOSED opening inventory already consumed lots
            # during replay (its effect lives in the on-chain balance, not in a
            # duplicate of the snapshot). De-duping against ALL lots conflated
            # those post-boot deltas with a re-run of the seed and under-/over-
            # seeded the opening balance. The runner calls
            # ``reconstruct_lending_basis_store`` on every boot, so this guard
            # makes a repeated seed of the same snapshot a no-op.
            key = self._key(deployment_id, swap_position_key, sym)
            already_open = Decimal("0")
            for lot in self._lots.get(key, []):
                if lot.get("source") != source:
                    continue
                remaining = lot.get("remaining")
                if not isinstance(remaining, Decimal):
                    remaining = _parse_decimal(remaining)
                if remaining is not None and remaining > 0:
                    already_open += remaining
            seed_qty = balance - already_open
            if seed_qty <= 0:
                continue

            price = _parse_decimal(row.get("price_usd"))
            cost_usd = (seed_qty * price) if price is not None else None
            self.record_swap_acquisition(
                deployment_id=deployment_id,
                position_key=swap_position_key,
                token=sym,
                amount=seed_qty,
                cost_usd=cost_usd,
                timestamp=timestamp,
                source=source,
            )
            seeded += 1
        return seeded

    # ──────────────────────────────────────────────────────────────────────
    # Prediction-market aggregation (VIB-3707)
    #
    # Unlike FIFO lot tracking used for BORROW/REPAY/PT/SWAP, prediction-market
    # positions are aggregated weighted-average per (market_id, outcome). Repeat
    # BUYs combine into one running aggregate (size, basis); SELLs and REDEEMs
    # consume that aggregate proportionally.
    #
    # The same `_lots` dict is reused as the in-memory backing store. A
    # prediction key holds at most ONE row representing the current aggregate
    # — not a list of lots — distinguished by the row dict keys
    # ("size" / "basis" / "kind"="prediction"). Persistence backend is
    # untouched: rows are reconstructed from PREDICTION_* accounting events
    # on startup via reconstruct_from_events.
    # ──────────────────────────────────────────────────────────────────────

    def _prediction_key(self, deployment_id: str, position_key: str) -> str:
        """Composite key for the prediction aggregate row.

        position_key already encodes (market_id, outcome) — no token leg here
        because prediction shares are tracked as a single conditional position,
        not a token balance.
        """
        return f"{deployment_id}|prediction|{position_key}"

    def get_prediction_position(
        self,
        deployment_id: str,
        position_key: str,
    ) -> tuple[Decimal, Decimal] | None:
        """Return (size, basis_usd) for the open aggregate, or None if no row."""
        key = self._prediction_key(deployment_id, position_key)
        row_list = self._lots.get(key)
        if not row_list:
            return None
        row = row_list[0]
        size = row.get("size", Decimal("0"))
        basis = row.get("basis", Decimal("0"))
        if not isinstance(size, Decimal):
            size = Decimal(str(size))
        if not isinstance(basis, Decimal):
            basis = Decimal(str(basis))
        return size, basis

    def iter_open_swap_lots(self) -> Iterable[tuple[str, str, Decimal, Decimal | None]]:
        """Yield every open swap-inventory lot for read-only valuation (VIB-4984).

        Yields ``(position_key, token, remaining, cost_usd_for_remaining)`` for
        each lot with ``remaining > 0`` whose ``position_key`` is ``swap:``-prefixed
        AND whose ``source == "SWAP"``. Lending (``supply:``), prediction
        (``<dep>|prediction|...``) and PT keys are excluded — only the directional
        swap-inventory residual is surfaced.

        NOTE on the source filter: the ``swap:<chain>:<wallet>`` key is a *fungible
        wallet-basis pool*, so ``_replay_borrow`` / ``_replay_withdraw`` also mint
        ``swap:``-keyed lots (``source`` = ``"BORROW"`` / ``"WITHDRAW"``) for
        borrowed/withdrawn tokens that land in the wallet (VIB-3964). Those are
        genuine wallet inventory, but attributing their mark-to-market to a
        *swap-inventory* tile would mislabel a looping strategy's transient
        borrowed-token MTM as swap PnL. VIB-4984 is scoped to directional **swap**
        inventory, so non-SWAP-sourced lots are excluded here. Whether the
        dashboard should surface unrealized MTM for the full wallet-basis pool
        (incl. borrowed-then-held tokens) is deferred to VIB-4997.

        ``cost_usd_for_remaining`` is the lot's stored ``cost_usd`` pro-rated by
        ``remaining / amount``. It is ``None`` when the lot's ``cost_usd`` is
        ``None`` (Empty≠Zero — missing basis is unmeasured, NOT zero cost).

        Read-only accessor: does NOT mutate lot state. Callers must not reach into
        the private ``_lots`` dict (mirrors ``get_prediction_position``).

        The composite store key is ``{deployment_id}:{position_key}:{token}`` and
        the swap ``position_key`` itself is ``swap:<chain>:<wallet>``. We locate the
        ``:swap:`` marker to split off the leading ``deployment_id``, then take the
        final colon-segment as the token; everything between is the ``position_key``.
        """
        marker = ":swap:"
        for composite_key, lots in self._lots.items():
            idx = composite_key.find(marker)
            if idx < 0:
                continue
            # position_key starts just after the deployment_id + ':' boundary.
            remainder = composite_key[idx + 1 :]  # "swap:<chain>:<wallet>:<token>"
            last_colon = remainder.rfind(":")
            if last_colon <= 0:
                continue
            position_key = remainder[:last_colon]
            token = remainder[last_colon + 1 :]
            if not position_key.startswith("swap:") or not token:
                continue
            for lot in lots:
                # Scope to directional swap inventory only (VIB-4984). The
                # swap-keyed pool is fungible across SWAP/BORROW/WITHDRAW
                # sources (VIB-3964); borrowed/withdrawn-then-held tokens are
                # excluded here so their MTM is not mislabeled as swap PnL.
                # Broadening to the full wallet-basis pool → VIB-4997.
                if lot.get("source") != "SWAP":
                    continue
                remaining = lot.get("remaining")
                if not isinstance(remaining, Decimal):
                    remaining = _parse_decimal(remaining)
                if remaining is None or remaining <= 0:
                    continue
                amount = lot.get("amount")
                if not isinstance(amount, Decimal):
                    amount = _parse_decimal(amount)
                cost_usd: Decimal | None = lot.get("cost_usd")
                if cost_usd is not None and not isinstance(cost_usd, Decimal):
                    cost_usd = _parse_decimal(cost_usd)
                if cost_usd is None or amount is None or amount <= 0:
                    cost_for_remaining: Decimal | None = None
                else:
                    cost_for_remaining = cost_usd * (remaining / amount)
                yield position_key, token, remaining, cost_for_remaining

    def iter_open_wallet_basis_lots(self) -> Iterable[tuple[str, str, Decimal, Decimal | None]]:
        """Yield every open wallet-basis lot, REGARDLESS of source (ALM-2766).

        Source-agnostic twin of :meth:`iter_open_swap_lots`: same
        ``swap:<chain>:<wallet>``-keyed pool, same ``(position_key, token,
        remaining, cost_for_remaining)`` shape, but WITHOUT the
        ``source == "SWAP"`` filter. Borrowed-then-held (``source="BORROW"``)
        and withdrawn-then-held (``source="WITHDRAW"``) tokens (VIB-3964) ARE
        genuine wallet inventory and MUST be counted — a looping teardown's
        swap-back of withdrawn collateral would otherwise be treated as
        untracked and stranded.

        This is deliberately broader than :meth:`iter_open_swap_lots`, whose
        SWAP-only scope exists so VIB-4984's *swap-inventory* dashboard tile
        does not mislabel transient borrowed-token MTM as swap PnL. The
        teardown clamp is the opposite question — "how much of this wallet
        balance is OURS (tracked) to swap back?" — so every wallet-basis lot
        counts.

        Lending (``supply:``) and prediction keys are excluded (they are not
        wallet inventory). Read-only; does not mutate lot state.
        """
        marker = ":swap:"
        for composite_key, lots in self._lots.items():
            idx = composite_key.find(marker)
            if idx < 0:
                continue
            remainder = composite_key[idx + 1 :]  # "swap:<chain>:<wallet>:<token>"
            last_colon = remainder.rfind(":")
            if last_colon <= 0:
                continue
            position_key = remainder[:last_colon]
            token = remainder[last_colon + 1 :]
            if not position_key.startswith("swap:") or not token:
                continue
            for lot in lots:
                # No source filter (ALM-2766) — SWAP / BORROW / WITHDRAW lots
                # all count as tracked wallet inventory.
                remaining = lot.get("remaining")
                if not isinstance(remaining, Decimal):
                    remaining = _parse_decimal(remaining)
                if remaining is None or remaining <= 0:
                    continue
                amount = lot.get("amount")
                if not isinstance(amount, Decimal):
                    amount = _parse_decimal(amount)
                cost_usd: Decimal | None = lot.get("cost_usd")
                if cost_usd is not None and not isinstance(cost_usd, Decimal):
                    cost_usd = _parse_decimal(cost_usd)
                if cost_usd is None or amount is None or amount <= 0:
                    cost_for_remaining: Decimal | None = None
                else:
                    cost_for_remaining = cost_usd * (remaining / amount)
                yield position_key, token, remaining, cost_for_remaining

    def iter_open_pt_lots(self) -> Iterable[tuple[str, str, Decimal, Decimal | None, Decimal | None]]:
        """Yield every open principal-token (PT) lot for read-only valuation (VIB-5316).

        Yields ``(position_key, pt_token, remaining_pt, sy_cost_for_remaining,
        usd_cost_for_remaining)`` for each PT acquisition lot with ``remaining_pt > 0``
        — the unmatched
        residual of ``PT_BUY`` after ``PT_SELL`` / ``PT_REDEEM`` FIFO consumption
        (:meth:`record_pt_buy` / :meth:`match_pt_redeem`). This residual IS the
        held-PT inventory: a held PT is not a ``position_event`` (the
        ``PENDLE_PT`` PositionType was removed in VIB-4931 and ``SWAP`` is absent
        from ``INTENT_TO_EVENT_TYPE``), so the FIFO lot is the only durable record
        of a currently-held PT (design spine §2 VIB-5316).

        PT lots are discriminated by SHAPE (the ``remaining_pt`` key) — never by
        protocol name — keeping this accessor free of connector-name coupling;
        the ``_lots`` dict is shared across BORROW / SWAP / prediction lots, which
        carry different keys. ``pt_token`` (the PT symbol) is read from the lot
        directly (stamped by :meth:`record_pt_buy`), so the symbol survives even
        though the composite store key's ``deployment_id`` segment contains a
        colon.

        ``sy_cost_for_remaining`` is the SY/underlying-denominated cost of the
        open portion (``cost_per_pt × remaining_pt``) — the MEASURED accounting
        primitive. It is ``None`` when ``cost_per_pt`` is unmeasured (Empty ≠ Zero
        — missing basis is unmeasured, NOT zero cost).

        ``usd_cost_for_remaining`` is the open portion's USD cost basis anchored at
        the **BUY-TIME** underlying/USD price stamped on the lot
        (``cost_per_pt × remaining_pt × underlying_price_at_buy``, VIB-5316). It is
        ``None`` when EITHER ``cost_per_pt`` OR ``underlying_price_at_buy`` is
        unmeasured (pre-fix lot, or ``price_inputs_json`` lacked the base token at
        buy time) — Empty ≠ Zero. Critically this is NEVER computed from the
        CURRENT underlying price: re-marking the buy-time SY cost at today's price
        sign-flips unrealized PnL for volatile underlyings (the VIB-5316 bug). The
        mark (current value) is the gateway price's job at the valuation boundary;
        the COST stays pinned to what was paid.

        Read-only accessor: does NOT mutate lot state. Callers must not reach into
        the private ``_lots`` dict (mirrors :meth:`iter_open_swap_lots`).
        """
        for composite_key, lots in self._lots.items():
            for lot in lots:
                # Shape gate: only PT lots carry ``remaining_pt``. Borrow lots
                # use ``remaining`` / ``principal``; swap lots ``remaining`` /
                # ``amount``; prediction rows ``kind == "prediction"``.
                if "remaining_pt" not in lot:
                    continue
                remaining = lot.get("remaining_pt")
                if not isinstance(remaining, Decimal):
                    remaining = _parse_decimal(remaining)
                if remaining is None or remaining <= 0:
                    continue
                pt_token = lot.get("pt_token") or ""
                if not pt_token:
                    # Defensive: a lot recorded before the ``pt_token`` stamp —
                    # recover the symbol from the composite key's final
                    # colon-segment (``{deployment_id}:{position_key}:{token}``).
                    last_colon = composite_key.rfind(":")
                    pt_token = composite_key[last_colon + 1 :] if last_colon >= 0 else composite_key
                if not pt_token:
                    continue
                suffix = ":" + pt_token.lower()
                position_key = composite_key[: -len(suffix)] if composite_key.endswith(suffix) else composite_key
                cost_per_pt: Decimal | None = lot.get("cost_per_pt")
                if cost_per_pt is not None and not isinstance(cost_per_pt, Decimal):
                    cost_per_pt = _parse_decimal(cost_per_pt)
                sy_cost_for_remaining = cost_per_pt * remaining if cost_per_pt is not None else None
                # VIB-5316: buy-time-anchored USD cost. None if EITHER leg unmeasured
                # (Empty ≠ Zero); never the current underlying price.
                underlying_at_buy: Decimal | None = lot.get("underlying_price_at_buy")
                if underlying_at_buy is not None and not isinstance(underlying_at_buy, Decimal):
                    underlying_at_buy = _parse_decimal(underlying_at_buy)
                if cost_per_pt is None or underlying_at_buy is None:
                    usd_cost_for_remaining: Decimal | None = None
                else:
                    usd_cost_for_remaining = cost_per_pt * remaining * underlying_at_buy
                yield position_key, pt_token, remaining, sy_cost_for_remaining, usd_cost_for_remaining

    def record_prediction_buy(
        self,
        deployment_id: str,
        position_key: str,
        shares: Decimal,
        cost_basis_usd: Decimal,
        gas_cost_usd: Decimal = Decimal("0"),
        fee_pusd: Decimal = Decimal("0"),
    ) -> tuple[Decimal, Decimal, bool]:
        """Apply a PREDICTION_BUY to the weighted-average aggregate.

        Returns (new_size, new_basis_usd, is_open) where is_open is True when
        this was the first BUY for the (market_id, outcome) — used by the
        handler to choose between PREDICTION_OPEN and PREDICTION_INCREASE.

        Weighted-average accounting: averaging up combines size and basis
        directly (basis is in USD terms, so summation already yields the
        correct aggregate cost). Average price = new_basis / new_size.

        VIB-3710: ``gas_cost_usd`` and ``fee_pusd`` are accumulated alongside
        ``cost_basis_usd`` into a separate ``loaded_extras`` field on the
        aggregate. SELL/REDEEM uses
        ``fully_loaded_basis = basis + loaded_extras`` to compute realized
        PnL — proportionally consumed on partial sells the same way the bare
        basis is. Defaults to ``Decimal("0")`` so existing callers (and
        replay paths that lack the new fields) keep their current arithmetic
        unchanged. None / negative values are clamped to 0 to keep the
        invariant that fully_loaded_basis ≥ basis.
        """
        # Clamp non-positive extras to zero so a buggy upstream measurement
        # cannot silently subtract from realized PnL.
        if gas_cost_usd is None or gas_cost_usd < 0:
            gas_cost_usd = Decimal("0")
        if fee_pusd is None or fee_pusd < 0:
            fee_pusd = Decimal("0")
        loaded_extras_delta = gas_cost_usd + fee_pusd

        key = self._prediction_key(deployment_id, position_key)
        existing = self._lots.get(key)
        if not existing:
            self._lots[key] = [
                {
                    "kind": "prediction",
                    "size": shares,
                    "basis": cost_basis_usd,
                    # VIB-3710: gas + fees accumulated separately from the
                    # headline pUSD basis. SELL/REDEEM consumes both
                    # proportionally so realized PnL reflects the truly
                    # fully-loaded cost. Average-up sums these alongside
                    # basis (NOT a weighted-average — adding the cost of
                    # the second BUY's setup-txs to the position is the
                    # actually-incurred spend, not a "blend").
                    "loaded_extras": loaded_extras_delta,
                }
            ]
            return shares, cost_basis_usd, True

        row = existing[0]
        old_size = row.get("size", Decimal("0"))
        old_basis = row.get("basis", Decimal("0"))
        old_extras = row.get("loaded_extras", Decimal("0"))
        if not isinstance(old_size, Decimal):
            old_size = Decimal(str(old_size))
        if not isinstance(old_basis, Decimal):
            old_basis = Decimal(str(old_basis))
        if not isinstance(old_extras, Decimal):
            old_extras = Decimal(str(old_extras))
        new_size = old_size + shares
        new_basis = old_basis + cost_basis_usd
        row["size"] = new_size
        row["basis"] = new_basis
        row["loaded_extras"] = old_extras + loaded_extras_delta
        return new_size, new_basis, False

    def get_prediction_loaded_extras(
        self,
        deployment_id: str,
        position_key: str,
    ) -> Decimal:
        """Return the per-position cumulative gas + fee accumulator (VIB-3710).

        Returns ``Decimal("0")`` when no aggregate row exists or when an
        existing row predates VIB-3710 (no ``loaded_extras`` key) — both
        cases are equivalent to "no extras attributed yet".
        """
        key = self._prediction_key(deployment_id, position_key)
        existing = self._lots.get(key)
        if not existing:
            return Decimal("0")
        row = existing[0]
        extras = row.get("loaded_extras", Decimal("0"))
        if not isinstance(extras, Decimal):
            try:
                extras = Decimal(str(extras))
            except (InvalidOperation, ValueError):
                return Decimal("0")
        return extras

    def match_prediction_sell(
        self,
        deployment_id: str,
        position_key: str,
        shares_sold: Decimal,
        proceeds_usd: Decimal,
    ) -> tuple[Decimal | None, Decimal, Decimal, bool]:
        """Apply a PREDICTION_SELL (or REDEEM) to the aggregate.

        Returns (realized_pnl_usd, new_size, new_basis_usd, is_close).

        - realized_pnl_usd is None when no prior basis was recorded — the
          caller MUST surface this as an accounting gap (e.g. the strategy
          was deployed with an existing on-chain position).
        - Proportional basis consumption: cost_consumed =
          (shares_sold/old_size) * fully_loaded_basis  where
          fully_loaded_basis = basis + loaded_extras (gas + fees) [VIB-3710].
          realized = proceeds - cost_consumed.
        - Position is fully closed (basis row deleted, is_close=True) when
          the post-trade size is non-positive (within Decimal epsilon).
        - Partial sells decrement the row in place — basis AND loaded_extras
          shrink proportionally so a later sell of the rest produces the
          arithmetically expected residual realized PnL.

        The handler uses this for both PREDICTION_SELL and PREDICTION_REDEEM
        — REDEEM is structurally a sell where proceeds = CTF payout and the
        position always closes.
        """
        key = self._prediction_key(deployment_id, position_key)
        existing = self._lots.get(key)
        if not existing:
            return None, Decimal("0"), Decimal("0"), True

        row = existing[0]
        old_size = row.get("size", Decimal("0"))
        old_basis = row.get("basis", Decimal("0"))
        old_extras = row.get("loaded_extras", Decimal("0"))
        if not isinstance(old_size, Decimal):
            old_size = Decimal(str(old_size))
        if not isinstance(old_basis, Decimal):
            old_basis = Decimal(str(old_basis))
        if not isinstance(old_extras, Decimal):
            old_extras = Decimal(str(old_extras))

        if old_size <= 0:
            # Stale zero row — treat as no prior basis.
            self._lots.pop(key, None)
            return None, Decimal("0"), Decimal("0"), True

        # Clamp shares_sold to old_size: an over-sell (e.g. SELL "all" mismatch)
        # consumes the whole basis rather than producing negative size or a
        # cost-consumed > basis.
        consumed_shares = min(shares_sold, old_size)
        share_fraction = consumed_shares / old_size
        # VIB-3710: realized PnL must be measured against the fully-loaded
        # basis. Both legs are consumed proportionally so partial-then-full
        # sells produce the same total realized PnL as a single full sell.
        fully_loaded_basis = old_basis + old_extras
        cost_consumed = share_fraction * fully_loaded_basis
        realized_pnl = proceeds_usd - cost_consumed

        new_size = old_size - consumed_shares
        new_basis = old_basis - (share_fraction * old_basis)
        new_extras = old_extras - (share_fraction * old_extras)

        # Decimal epsilon — anything within 1e-9 share is considered zero.
        # Prediction-market share counts are USDC-tick aware (4-decimal),
        # so 1e-9 is comfortably below any real residual.
        epsilon = Decimal("1e-9")
        if new_size <= epsilon:
            self._lots.pop(key, None)
            return realized_pnl, Decimal("0"), Decimal("0"), True

        row["size"] = new_size
        row["basis"] = new_basis
        row["loaded_extras"] = new_extras
        return realized_pnl, new_size, new_basis, False

    def match_swap_disposal(
        self,
        deployment_id: str,
        position_key: str,
        token: str,
        amount: Decimal,
    ) -> tuple[Decimal | None, Decimal]:
        """FIFO-consume swap acquisition lots for token_in.

        Returns (cost_basis_consumed, unmatched_amount).

        Returns (None, amount) when no lots exist for this token — signals to the
        caller that realized PnL cannot be computed (no prior acquisition recorded).
        Returns (Decimal, Decimal("0")) on a full FIFO match.
        Returns (Decimal, unmatched_amount) on a partial match (spent more than was
        recorded — e.g. tokens acquired before the accounting system was deployed).

        cost_basis_consumed is the USD cost of the consumed lot quantity.  Returns
        None when any consumed lot had cost_usd=None — missing basis means realized
        PnL cannot be reliably computed for this disposal.  Callers must treat a
        None return as ESTIMATED confidence.
        """
        key = self._key(deployment_id, position_key, token)
        lots = self._lots.get(key)

        # No lots at all for this token — cannot compute realized PnL.
        if lots is None:
            return None, amount

        remaining = amount
        cost_consumed = Decimal("0")
        _has_unknown_basis = False

        for lot in lots:
            if remaining <= 0:
                break
            available = lot.get("remaining", Decimal("0"))
            if available <= 0:
                continue
            consume = min(available, remaining)
            lot["remaining"] -= consume
            remaining -= consume
            lot_cost_usd: Decimal | None = lot.get("cost_usd")
            if lot_cost_usd is not None and lot["amount"] > 0:
                # Pro-rate the lot's cost by the fraction consumed.
                cost_consumed += lot_cost_usd * (consume / lot["amount"])
            else:
                _has_unknown_basis = True

        if _has_unknown_basis:
            return None, remaining

        return cost_consumed, remaining


def canonical_symbol(symbol: Any) -> str:
    """Case-insensitive canonical key for token-symbol matching (ALM-2766).

    Identical to ``inventory_revaluation._canonical`` — a stable, dependency-free
    upper-case of the trimmed string. Defined here (the lower layer) so the
    teardown clamp and the wallet-basis summer key on EXACTLY the same form;
    ``inventory_revaluation`` lives above ``basis`` and cannot be imported from
    here without a cycle.
    """
    return str(symbol or "").strip().upper()


# Maturity grammar for principal-token (PT) symbols, e.g. the ``-25JUN2026`` in
# ``PT-wstETH-25JUN2026``. Duplicated (deliberately, to keep this framework layer
# free of any connector-name coupling — the framework→connector ratchet
# ``scripts/ci/scan_chain_protocol_coupling.py`` forbids naming a protocol here)
# from the connector-side parser; keep the two in sync if the PT symbol grammar
# ever changes:
#   almanak/connectors/pendle/accounting_spec.py:_parse_pt_maturity
_PT_MATURITY_RE = re.compile(r"[-_](\d{1,2})([A-Z]{3})(\d{4})(?:$|[-_])")


def canonical_pt_symbol(symbol: Any) -> str:
    """Cross-surface canonical identity for a token, maturity-INSENSITIVE for PTs.

    Identical to :func:`canonical_symbol` for every non-PT token (so non-Pendle
    inventory is byte-identical), AND for a ``PT-`` symbol that carries no
    parseable maturity suffix. For a ``PT-`` symbol WITH a maturity suffix it
    strips the suffix to the maturity-less identity:

        canonical_pt_symbol("PT-wstETH-25JUN2026") == "PT-WSTETH"
        canonical_pt_symbol("PT-wstETH")           == "PT-WSTETH"

    WHY (VIB-5353 / VIB-5355): the SAME held PT is named in two forms across
    surfaces — the framework/ledger/accounting layer resolves the maturity-
    BEARING symbol (receipt parser → SWAP ledger row → ``PT_BUY.pt_token`` →
    FIFO lot), while the strategy/config layer can only emit the maturity-LESS
    form (``get_config("pt_token", "PT-wstETH")`` → teardown ``from_token`` and
    ``details["pt_token"]``). ``canonical_symbol`` (bare upper/strip) never joins
    them, so the teardown clamp strands a swap-acquired PT (``untracked_token``)
    and the PortfolioValuer dedup misses (counting the PT in BOTH the reprice and
    FIFO-inventory paths → ~2× NAV). The maturity-less form is the ONLY form both
    layers can produce, so it is the cross-surface join key.

    This is a JOIN/DEDUP/tracked-inventory identity ONLY. The FIFO *match* key
    (:meth:`FIFOBasisStore._key`, raw token + position_key) and the valuer
    *pricing* aggregation key (``portfolio_valuer._aggregate_open_pt_lots`` keys
    on :func:`canonical_symbol`, maturity-bearing) are deliberately left on the
    maturity-bearing form, so two distinct maturities of the same underlying stay
    distinct for matching and pricing.
    """
    base = canonical_symbol(symbol)
    if not base.startswith("PT-"):
        return base
    m = _PT_MATURITY_RE.search(base)
    if not m:
        return base
    return base[: m.start()]


def sum_open_wallet_basis_by_token(
    events: list[dict[str, Any]],
    deployment_id: str,
) -> dict[str, Decimal] | None:
    """Tracked wallet inventory per token for ``deployment_id`` (ALM-2766).

    Reconstructs FIFO lots from ``events`` via
    :meth:`FIFOBasisStore.reconstruct_from_events` and sums ``remaining`` across
    ALL wallet-basis lots (:meth:`FIFOBasisStore.iter_open_wallet_basis_lots` —
    SWAP / BORROW / WITHDRAW sources all count) AND held principal-token (PT)
    lots (:meth:`FIFOBasisStore.iter_open_pt_lots` — the open ``PT_BUY`` residual
    after ``PT_SELL`` / ``PT_REDEEM`` matching), keyed by
    :func:`canonical_pt_symbol`. This is the quantity a default teardown is
    allowed to swap back: ``min(this, live_balance)`` never touches commingled
    funds.

    VIB-5353: a PT acquired via a ``SwapIntent`` is booked as a ``PT_BUY`` and
    lives in the PT lot lane (``pendle_pt:`` key, ``remaining_pt`` field), so it
    is invisible to :meth:`iter_open_wallet_basis_lots` (``:swap:`` key +
    ``remaining`` field). Folding the PT lane in here makes a held PT TRACKED
    wallet inventory — it IS ours to swap back on teardown — so the clamp
    classifies the swap-back as ``clamped`` instead of stranding it as
    ``untracked_token``. The PT and swap lanes live in disjoint key namespaces
    and PT symbols (``PT-…``) never collide with ordinary token symbols, so the
    two folds sum into one map without overlap. ``canonical_pt_symbol`` is
    maturity-insensitive for PTs so the maturity-less teardown ``from_token``
    (config) matches the maturity-bearing FIFO lot (ledger); it is identical to
    ``canonical_symbol`` for every non-PT token (no behaviour change there).

    Deployment-scoped: only events whose ``deployment_id`` matches are replayed
    (a shared wallet's sibling-strategy lots are not ours). An empty / missing
    ``deployment_id`` returns the UNMEASURED sentinel ``None`` (Empty ≠ Zero) —
    NOT ``{}`` — so the caller fails closed rather than treating "we can't scope
    this" as "nothing is tracked". A scoped-but-empty event set returns ``{}``
    (measured: this deployment has no tracked wallet inventory).
    """
    if not deployment_id:
        return None
    scoped = [ev for ev in events if isinstance(ev, dict) and ev.get("deployment_id") == deployment_id]
    store = FIFOBasisStore()
    store.reconstruct_from_events(scoped)
    by_token: dict[str, Decimal] = {}
    for _position_key, token, remaining, _cost in store.iter_open_wallet_basis_lots():
        sym = canonical_pt_symbol(token)
        if not sym:
            continue
        by_token[sym] = by_token.get(sym, Decimal("0")) + remaining
    # VIB-5353: fold held-PT inventory into the same tracked map (maturity-less
    # canonical key) so a swap-acquired PT's teardown swap-back is clamped, not
    # stranded as untracked. iter_open_pt_lots yields the open PT_BUY residual.
    for _position_key, pt_token, remaining_pt, _sy_cost, _usd_cost in store.iter_open_pt_lots():
        sym = canonical_pt_symbol(pt_token)
        if not sym:
            continue
        by_token[sym] = by_token.get(sym, Decimal("0")) + remaining_pt
    return by_token


# Per-event-type dispatch table for reconstruct_from_events. Unknown types
# fall through (silently skipped) so that future event types added in
# newer schema versions don't break older runners replaying mixed history.
_ReplayCallable = Callable[["FIFOBasisStore", _ReplayContext, dict[str, int], Any], int]
_REPLAY_DISPATCH: dict[str, _ReplayCallable] = {
    "BORROW": FIFOBasisStore._replay_borrow,
    "WITHDRAW": FIFOBasisStore._replay_withdraw,
    "SUPPLY": FIFOBasisStore._replay_supply,
    "REPAY": FIFOBasisStore._replay_repay,
    "DELEVERAGE": FIFOBasisStore._replay_repay,
    "PT_BUY": FIFOBasisStore._replay_pt_buy,
    "PT_SELL": FIFOBasisStore._replay_pt_sell,
    "PT_REDEEM": FIFOBasisStore._replay_pt_redeem,
    "SWAP": FIFOBasisStore._replay_swap,
    "PREDICTION_OPEN": FIFOBasisStore._replay_prediction,
    "PREDICTION_INCREASE": FIFOBasisStore._replay_prediction,
    "PREDICTION_REDUCE": FIFOBasisStore._replay_prediction,
    "PREDICTION_CLOSE": FIFOBasisStore._replay_prediction,
    "PREDICTION_REDEEM": FIFOBasisStore._replay_prediction,
}
