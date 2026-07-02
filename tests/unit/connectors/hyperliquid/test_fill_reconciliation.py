"""Unit tests for Hyperliquid fill-vs-submission reconciliation (VIB-5597).

Covers the two independent signals (orderStatus-by-cloid + the 0x0800 position
read) and their combination, with the Empty ≠ Zero spine: an unconfirmed
submission is UNMEASURED, never assumed filled or flat.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors.hyperliquid.fill_reconciliation import (
    FillOutcome,
    FillStatus,
    build_order_status_request,
    cloid_to_hex,
    confirm_close_from_position,
    confirm_open_from_position,
    parse_order_status_response,
    reconcile_fill,
)
from almanak.connectors.hyperliquid.sdk import Position

_WALLET = "0x1234567890123456789012345678901234567890"


def _pos(szi: int) -> Position:
    return Position(szi=szi, entry_ntl=abs(szi) * 100, isolated_raw_usd=0, leverage=1, is_isolated=False)


# =============================================================================
# Request builder / cloid hex
# =============================================================================


class TestRequestBuilder:
    def test_builds_order_status_body(self):
        body = build_order_status_request(_WALLET, 0x1234)
        assert body["type"] == "orderStatus"
        assert body["user"] == _WALLET
        assert body["oid"] == "0x" + "0" * 28 + "1234"

    def test_cloid_hex_is_32_nibbles(self):
        assert cloid_to_hex(1) == "0x" + "0" * 31 + "1"
        assert len(cloid_to_hex(2**128 - 1)) == 34  # 0x + 32

    @pytest.mark.parametrize("bad", [0, -1, True, "5"])
    def test_rejects_bad_cloid(self, bad):
        with pytest.raises(ValueError):
            build_order_status_request(_WALLET, bad)

    def test_rejects_bad_wallet(self):
        with pytest.raises(ValueError):
            build_order_status_request("not-an-address", 5)


# =============================================================================
# Signal 1 — orderStatus response parsing
# =============================================================================


class TestParseOrderStatus:
    def test_filled(self):
        resp = {"status": "order", "order": {"status": "filled", "order": {"filledSz": "0.5", "avgPx": "100.2"}}}
        out = parse_order_status_response(resp)
        assert out.status is FillStatus.FILLED
        assert out.filled_size == Decimal("0.5")
        assert out.avg_fill_price == Decimal("100.2")

    def test_rejected(self):
        out = parse_order_status_response({"status": "order", "order": {"status": "rejected", "order": {}}})
        assert out.status is FillStatus.REJECTED

    @pytest.mark.parametrize("s", ["canceled", "marginCanceled", "insufficientMarginCanceled", "expired"])
    def test_terminal_cancels_are_rejects(self, s):
        out = parse_order_status_response({"status": "order", "order": {"status": s, "order": {}}})
        assert out.status is FillStatus.REJECTED

    def test_cancel_with_partial_fill_is_partial(self):
        # A reduce-only/margin cancel that still filled some size DID produce a
        # (smaller) position — report partial, never reject.
        resp = {"status": "order", "order": {"status": "marginCanceled", "order": {"filledSz": "0.2"}}}
        out = parse_order_status_response(resp)
        assert out.status is FillStatus.PARTIALLY_FILLED
        assert out.filled_size == Decimal("0.2")

    def test_resting_no_fill(self):
        out = parse_order_status_response({"status": "order", "order": {"status": "open", "order": {}}})
        assert out.status is FillStatus.RESTING

    def test_resting_with_partial_fill_is_partial(self):
        resp = {"status": "order", "order": {"status": "open", "order": {"filledSz": "0.3"}}}
        out = parse_order_status_response(resp)
        assert out.status is FillStatus.PARTIALLY_FILLED

    def test_unknown_oid_is_unmeasured(self):
        out = parse_order_status_response({"status": "unknownOid"})
        assert out.status is FillStatus.UNMEASURED

    @pytest.mark.parametrize("resp", [None, {}, "garbage", {"status": "order"}, {"status": "order", "order": 5}])
    def test_malformed_is_unmeasured_never_a_fill(self, resp):
        out = parse_order_status_response(resp)
        assert out.status is FillStatus.UNMEASURED
        # Empty ≠ Zero: never a fabricated fill size / price.
        assert out.filled_size is None
        assert out.avg_fill_price is None

    def test_unrecognised_inner_status_is_unmeasured(self):
        out = parse_order_status_response({"status": "order", "order": {"status": "someNewState", "order": {}}})
        assert out.status is FillStatus.UNMEASURED


# =============================================================================
# Signal 2 — position read (settlement observer)
# =============================================================================


class TestConfirmOpenFromPosition:
    def test_none_read_is_unmeasured(self):
        assert confirm_open_from_position(None, expected_is_long=True).status is FillStatus.UNMEASURED

    def test_baseline_increase_long_is_filled(self):
        out = confirm_open_from_position(_pos(150), expected_is_long=True, prior_position=_pos(100))
        assert out.status is FillStatus.FILLED
        assert out.filled_size == 50

    def test_baseline_increase_short_is_filled(self):
        out = confirm_open_from_position(_pos(-150), expected_is_long=False, prior_position=_pos(-100))
        assert out.status is FillStatus.FILLED
        assert out.filled_size == 50

    def test_baseline_no_change_is_rejected(self):
        out = confirm_open_from_position(_pos(100), expected_is_long=True, prior_position=_pos(100))
        assert out.status is FillStatus.REJECTED

    def test_baseline_from_flat_to_open_is_filled(self):
        out = confirm_open_from_position(_pos(50), expected_is_long=True, prior_position=_pos(0))
        assert out.status is FillStatus.FILLED

    def test_baseline_wrong_direction_is_unmeasured(self):
        # Position moved AGAINST the expected direction — anomalous, fail-safe.
        out = confirm_open_from_position(_pos(50), expected_is_long=True, prior_position=_pos(100))
        assert out.status is FillStatus.UNMEASURED

    def test_no_baseline_position_present_is_confirmatory_fill(self):
        out = confirm_open_from_position(_pos(100), expected_is_long=True)
        assert out.status is FillStatus.FILLED

    def test_no_baseline_flat_is_unmeasured_not_reject(self):
        # Without a baseline we cannot call a reject — a flat read is unmeasured.
        out = confirm_open_from_position(_pos(0), expected_is_long=True)
        assert out.status is FillStatus.UNMEASURED

    def test_no_baseline_wrong_side_is_unmeasured(self):
        out = confirm_open_from_position(_pos(-100), expected_is_long=True)
        assert out.status is FillStatus.UNMEASURED


class TestConfirmCloseFromPosition:
    def test_none_is_unmeasured_never_closed(self):
        assert confirm_close_from_position(None, was_full_close=True).status is FillStatus.UNMEASURED

    def test_flat_after_full_close_is_filled(self):
        assert confirm_close_from_position(_pos(0), was_full_close=True).status is FillStatus.FILLED

    def test_still_open_after_full_close_is_resting(self):
        assert confirm_close_from_position(_pos(100), was_full_close=True).status is FillStatus.RESTING

    def test_still_open_after_partial_close_is_unmeasured(self):
        assert confirm_close_from_position(_pos(100), was_full_close=False).status is FillStatus.UNMEASURED


# =============================================================================
# Reconciliation — combine
# =============================================================================


class TestReconcile:
    def test_order_status_filled_wins_economics(self):
        os_out = FillOutcome(FillStatus.FILLED, filled_size=Decimal("0.5"), avg_fill_price=Decimal("100"), source="order_status")
        pos_out = FillOutcome(FillStatus.FILLED, filled_size=50, source="position_read")
        out = reconcile_fill(os_out, pos_out)
        assert out.status is FillStatus.FILLED
        # orderStatus economics preferred.
        assert out.avg_fill_price == Decimal("100")
        assert "order_status" in out.source and "position_read" in out.source

    def test_reject_authoritative_when_no_position_fill(self):
        os_out = FillOutcome(FillStatus.REJECTED, source="order_status")
        pos_out = FillOutcome(FillStatus.REJECTED, source="position_read")
        assert reconcile_fill(os_out, pos_out).status is FillStatus.REJECTED

    def test_partial_position_fill_overrides_order_status_reject(self):
        # orderStatus says canceled, but the position read shows a fill happened.
        os_out = FillOutcome(FillStatus.REJECTED, source="order_status")
        pos_out = FillOutcome(FillStatus.FILLED, filled_size=10, source="position_read")
        out = reconcile_fill(os_out, pos_out)
        assert out.status is FillStatus.FILLED

    def test_position_fill_when_order_status_unmeasured(self):
        os_out = FillOutcome(FillStatus.UNMEASURED, source="order_status")
        pos_out = FillOutcome(FillStatus.FILLED, filled_size=50, source="position_read")
        assert reconcile_fill(os_out, pos_out).status is FillStatus.FILLED

    def test_both_unmeasured_is_unmeasured(self):
        out = reconcile_fill(
            FillOutcome(FillStatus.UNMEASURED, source="order_status"),
            FillOutcome(FillStatus.UNMEASURED, source="position_read"),
        )
        assert out.status is FillStatus.UNMEASURED

    def test_both_none_is_unmeasured(self):
        assert reconcile_fill(None, None).status is FillStatus.UNMEASURED

    def test_partial_is_sticky(self):
        os_out = FillOutcome(FillStatus.PARTIALLY_FILLED, filled_size=Decimal("0.2"), source="order_status")
        out = reconcile_fill(os_out, FillOutcome(FillStatus.FILLED, source="position_read"))
        assert out.status is FillStatus.PARTIALLY_FILLED

    def test_resting_only_is_resting_not_unmeasured(self):
        out = reconcile_fill(FillOutcome(FillStatus.RESTING, source="order_status"), None)
        assert out.status is FillStatus.RESTING

    def test_single_signal_position_only(self):
        out = reconcile_fill(None, FillOutcome(FillStatus.FILLED, filled_size=50, source="position_read"))
        assert out.status is FillStatus.FILLED
        assert out.source == "position_read"


class TestFillStatusProperties:
    def test_is_confirmed_fill(self):
        assert FillStatus.FILLED.is_confirmed_fill
        assert FillStatus.PARTIALLY_FILLED.is_confirmed_fill
        assert not FillStatus.REJECTED.is_confirmed_fill
        assert not FillStatus.UNMEASURED.is_confirmed_fill
        assert not FillStatus.RESTING.is_confirmed_fill

    def test_is_confirmed_reject(self):
        assert FillStatus.REJECTED.is_confirmed_reject
        assert not FillStatus.UNMEASURED.is_confirmed_reject
        assert not FillStatus.FILLED.is_confirmed_reject
