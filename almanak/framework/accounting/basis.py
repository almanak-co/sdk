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
from collections.abc import Callable
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
                "amount_token missing (pre-VIB-3484); FIFO store may be incomplete on restart.",
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
        # Rows missing identity fields cannot be keyed into the lot store.
        if not deployment_id or not position_key:
            return None

        try:
            payload = _json.loads(row.get("payload_json") or "{}")
        except Exception:  # noqa: BLE001
            return None

        timestamp_str = row.get("timestamp")
        try:
            # Normalise UTC offset — Python <3.11 fromisoformat cannot parse trailing "Z"
            ts_norm = timestamp_str.replace("Z", "+00:00") if timestamp_str else None
            ts: datetime | None = datetime.fromisoformat(ts_norm) if ts_norm else None
        except (ValueError, TypeError):
            ts = None

        # VIB-3964: derive the swap-key the BORROW / WITHDRAW credit was minted
        # under. The accounting_events row carries `chain` and `wallet_address`
        # at the top level, so the key is reconstructible without re-encoding it
        # in the payload.
        chain_norm = (row.get("chain") or "").lower().strip()
        wallet_norm = (row.get("wallet_address") or "").lower().strip()
        swap_wallet_key = f"swap:{chain_norm}:{wallet_norm}" if chain_norm and wallet_norm else ""

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
        # PT_BUY stores raw 18-decimal integers from the swap receipt.
        pt_human = _parse_decimal(ctx.payload.get("pt_amount"))
        sy_human = _parse_decimal(ctx.payload.get("sy_amount"))
        if pt_human is None or sy_human is None:
            return 0
        pt_human = pt_human / _DECIMALS_18
        sy_human = sy_human / _DECIMALS_18
        if pt_human <= 0:
            return 0
        self.record_pt_buy(
            deployment_id=ctx.deployment_id,
            position_key=ctx.position_key,
            pt_token=pt_token,
            pt_amount=pt_human,
            sy_cost=sy_human,
            timestamp=ctx.timestamp,
            source_ledger_entry_id=ctx.ledger_entry_id,
        )
        return 1

    def _replay_pt_sell(self, ctx: _ReplayContext, v1_skipped: dict[str, int], log: Any) -> int:
        # PT_SELL follows the same raw-integer convention as PT_BUY.
        pt_token = ctx.payload.get("pt_token", "")
        if not pt_token:
            return 0
        pt_raw = _parse_decimal(ctx.payload.get("pt_amount"))
        if pt_raw is None:
            return 0
        pt_human = pt_raw / _DECIMALS_18
        if pt_human <= 0:
            return 0
        sy_raw = _parse_decimal(ctx.payload.get("sy_amount"))
        # sy_amount is required for PT_SELL: it's the actual market proceeds.
        # Defaulting to pt_amount (1:1 assumption) would invent cost-basis data.
        if sy_raw is None or sy_raw <= 0:
            return 0
        sy_human = sy_raw / _DECIMALS_18
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
        # PT_REDEEM events are written by build_pendle_pt_redeem_accounting_event()
        # which converts to human-decimal before storing (unlike PT_BUY / PT_SELL).
        # When py_redeemed was missing from the receipt, pt_amount is None and the
        # builder fell back to sy_amount — mirror that fallback here.
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
        # Fall back to row position_key for events written before VIB-3473.
        swap_position_key = ctx.payload.get("swap_position_key") or ctx.position_key
        if not swap_position_key:
            return 0

        # 1. Replay disposal of token_in to consume any prior acquisition lots,
        #    keeping the FIFO store consistent with the state before this swap.
        token_in_r = ctx.payload.get("token_in", "")
        amount_in_r = _parse_decimal(ctx.payload.get("amount_in"))
        if token_in_r and amount_in_r is not None and amount_in_r > 0:
            self.match_swap_disposal(
                deployment_id=ctx.deployment_id,
                position_key=swap_position_key,
                token=token_in_r,
                amount=amount_in_r,
            )

        # 2. Replay acquisition lot for token_out so future disposals can match it.
        token_out = ctx.payload.get("token_out", "")
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
