"""FIFO lot matching for interest and yield attribution.

Used for:
  - REPAY: match against BORROW lots to compute interest_paid
  - PT_REDEEM: match against PT_BUY lots to compute realized_yield

Policy is FIFO by (position_key, token). schema_version tracks the matching
policy so that future changes do not silently invalidate old records.

MATCHING_POLICY_VERSION must be bumped any time the matching algorithm changes.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

MATCHING_POLICY_VERSION = 2


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
        import json as _json
        import logging as _logging

        _log = _logging.getLogger(__name__)

        self._lots.clear()

        _DECIMALS_18 = Decimal(10**18)
        replayed = 0
        _v1_skipped: dict[str, int] = {}  # event_type → count, for aggregated warning at end

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

        for row in events:
            event_type = row.get("event_type", "")
            position_key = row.get("position_key", "")
            deployment_id = row.get("deployment_id", "")
            timestamp_str = row.get("timestamp")
            # ledger_entry_id links a lot back to the on-chain transaction (VIB-3468).
            ledger_entry_id: str | None = row.get("ledger_entry_id") or None

            # Rows missing identity fields cannot be keyed into the lot store.
            if not deployment_id or not position_key:
                continue

            try:
                payload = _json.loads(row.get("payload_json") or "{}")
            except Exception:  # noqa: BLE001
                continue

            try:
                # Normalise UTC offset — Python <3.11 fromisoformat cannot parse trailing "Z"
                ts_norm = timestamp_str.replace("Z", "+00:00") if timestamp_str else None
                ts: datetime | None = datetime.fromisoformat(ts_norm) if ts_norm else None
            except (ValueError, TypeError):
                ts = None

            if event_type == "BORROW":
                raw_amount_token = payload.get("amount_token")
                amount_token = _parse_decimal(raw_amount_token)
                asset = payload.get("asset", "")
                # amount_token key absent → policy v1 event (pre-VIB-3484); count for summary.
                # amount_token present but non-positive → v2 extraction bug; skip silently (debug).
                if raw_amount_token is None:
                    _v1_skipped["BORROW"] = _v1_skipped.get("BORROW", 0) + 1
                    continue
                if amount_token is None or amount_token <= 0:
                    _log.debug(
                        "FIFOBasisStore: BORROW event %s/%s has non-positive amount_token — skipping",
                        deployment_id,
                        position_key,
                    )
                    continue
                if not asset:
                    continue
                self.record_borrow(
                    deployment_id=deployment_id,
                    position_key=position_key,
                    token=asset,
                    principal_amount=amount_token,
                    timestamp=ts,
                    source_ledger_entry_id=ledger_entry_id,
                )
                replayed += 1

            elif event_type in ("REPAY", "DELEVERAGE"):
                # DELEVERAGE is structurally a repay — it reduces an open borrow lot.
                raw_amount_token = payload.get("amount_token")
                amount_token = _parse_decimal(raw_amount_token)
                asset = payload.get("asset", "")
                if raw_amount_token is None:
                    _v1_skipped[event_type] = _v1_skipped.get(event_type, 0) + 1
                    continue
                if amount_token is None or amount_token <= 0:
                    _log.debug(
                        "FIFOBasisStore: %s event %s/%s has non-positive amount_token — skipping",
                        event_type,
                        deployment_id,
                        position_key,
                    )
                    continue
                if not asset:
                    continue
                self.match_repay(
                    deployment_id=deployment_id,
                    position_key=position_key,
                    token=asset,
                    repay_amount=amount_token,
                )
                replayed += 1

            elif event_type == "PT_BUY":
                pt_token = payload.get("pt_token", "")
                if not pt_token:
                    continue
                # PT_BUY stores raw 18-decimal integers from the swap receipt.
                pt_human = _parse_decimal(payload.get("pt_amount"))
                sy_human = _parse_decimal(payload.get("sy_amount"))
                if pt_human is None or sy_human is None:
                    continue
                pt_human = pt_human / _DECIMALS_18
                sy_human = sy_human / _DECIMALS_18
                if pt_human <= 0:
                    continue
                self.record_pt_buy(
                    deployment_id=deployment_id,
                    position_key=position_key,
                    pt_token=pt_token,
                    pt_amount=pt_human,
                    sy_cost=sy_human,
                    timestamp=ts,
                    source_ledger_entry_id=ledger_entry_id,
                )
                replayed += 1

            elif event_type == "PT_SELL":
                # PT_SELL follows the same raw-integer convention as PT_BUY.
                pt_token = payload.get("pt_token", "")
                if not pt_token:
                    continue
                pt_raw = _parse_decimal(payload.get("pt_amount"))
                if pt_raw is None:
                    continue
                pt_human = pt_raw / _DECIMALS_18
                if pt_human <= 0:
                    continue
                sy_raw = _parse_decimal(payload.get("sy_amount"))
                # sy_amount is required for PT_SELL: it's the actual market proceeds.
                # Defaulting to pt_amount (1:1 assumption) would invent cost-basis data.
                if sy_raw is None or sy_raw <= 0:
                    continue
                sy_human = sy_raw / _DECIMALS_18
                self.match_pt_redeem(
                    deployment_id=deployment_id,
                    position_key=position_key,
                    pt_token=pt_token,
                    pt_redeemed=pt_human,
                    sy_received=sy_human,
                )
                replayed += 1

            elif event_type == "PT_REDEEM":
                pt_token = payload.get("pt_token", "")
                if not pt_token:
                    continue
                # PT_REDEEM events are written by build_pendle_pt_redeem_accounting_event()
                # which converts to human-decimal before storing (unlike PT_BUY / PT_SELL).
                # When py_redeemed was missing from the receipt, pt_amount is None and the
                # builder fell back to sy_amount — mirror that fallback here.
                pt_raw = _parse_decimal(payload.get("pt_amount"))
                sy_raw = _parse_decimal(payload.get("sy_amount"))
                if pt_raw is not None:
                    pt_human = pt_raw
                elif sy_raw is not None:
                    pt_human = sy_raw
                else:
                    continue
                if pt_human <= 0:
                    continue
                sy_human = sy_raw if sy_raw is not None else pt_human
                self.match_pt_redeem(
                    deployment_id=deployment_id,
                    position_key=position_key,
                    pt_token=pt_token,
                    pt_redeemed=pt_human,
                    sy_received=sy_human,
                )
                replayed += 1

        if replayed:
            _log.info("FIFOBasisStore: reconstructed %d lot operations from accounting_events", replayed)
        if _v1_skipped:
            total = sum(_v1_skipped.values())
            breakdown = ", ".join(f"{k}={v}" for k, v in sorted(_v1_skipped.items()))
            _log.warning(
                "FIFOBasisStore: skipped %d policy-v1 event(s) during reconstruction (%s) — "
                "amount_token missing (pre-VIB-3484); FIFO store may be incomplete on restart.",
                total,
                breakdown,
            )
        return replayed

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
                "pt_amount": pt_amount,
                "sy_cost": sy_cost,
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
        """FIFO match PT redemption against open PT buy lots."""
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

        realized_yield = sy_received - original_cost
        return MatchResult(
            repaid_principal=original_cost,
            interest_or_yield=realized_yield,
            lot_matches=lot_matches,
            unmatched_amount=max(Decimal("0"), remaining),
            earliest_lot_timestamp=earliest_ts,
        )
