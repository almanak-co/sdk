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

MATCHING_POLICY_VERSION = 1


@dataclass
class MatchResult:
    """Result of a FIFO match operation."""

    repaid_principal: Decimal
    interest_or_yield: Decimal
    matched_lot_ids: list[str]
    unmatched_amount: Decimal
    matching_policy_version: int = MATCHING_POLICY_VERSION
    earliest_lot_timestamp: datetime | None = None


class FIFOBasisStore:
    """In-memory FIFO lot store backed by the accounting_events table.

    In production, lots are reconstructed from BORROW/PT_BUY accounting events
    so the store is always consistent with durable state.
    """

    def __init__(self) -> None:
        self._lots: dict[str, list[dict[str, Any]]] = {}

    def _key(self, deployment_id: str, position_key: str, token: str) -> str:
        return f"{deployment_id}:{position_key}:{token.lower()}"

    def record_borrow(
        self,
        deployment_id: str,
        position_key: str,
        token: str,
        principal_amount: Decimal,
        timestamp: datetime | None = None,
    ) -> str:
        lot_id = str(uuid.uuid4())
        key = self._key(deployment_id, position_key, token)
        if key not in self._lots:
            self._lots[key] = []
        self._lots[key].append(
            {
                "lot_id": lot_id,
                "principal": principal_amount,
                "remaining": principal_amount,
                "timestamp": (timestamp or datetime.now(UTC)).isoformat(),
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
                matched_lot_ids=[],
                unmatched_amount=repay_amount,
            )

        remaining_repay = repay_amount
        principal_consumed = Decimal("0")
        matched_lot_ids: list[str] = []

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
            matched_lot_ids.append(lot["lot_id"])

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
            matched_lot_ids=matched_lot_ids,
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
    ) -> str:
        lot_id = str(uuid.uuid4())
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
        matched_lot_ids: list[str] = []
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
            matched_lot_ids.append(lot["lot_id"])
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
            matched_lot_ids=matched_lot_ids,
            unmatched_amount=max(Decimal("0"), remaining),
            earliest_lot_timestamp=earliest_ts,
        )
