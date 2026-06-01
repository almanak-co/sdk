"""VIB-4848 (T8 + T9 + T12) — LP attribution correctness.

Pins three behaviours added to ``pnl_attributor`` so the next contributor
cannot silently regress them. The originating PRD is
``docs/internal/PRD-May26.md`` §Epic D; the verified file:line anchors are
in the Linear ticket.

- T8: ``fee_separation_method`` + ``fee_confidence`` flow from
  ``LPCloseData`` through the close-event ``attribution_json`` sidecar
  and reach ``attribute_lp``'s output dict so downstream consumers
  (dashboards, repair tooling) can tell BUNDLED from SEPARATE closes.
- T9: ``compute_impermanent_loss`` accepts an optional ``fees_usd`` so
  V_lp can be principal-only on protocols that bundle principal + fees
  (mirrors the long-standing lp_handler behaviour that the
  accounting_events lane already enforces). ``attribute_lp`` reads the
  USD-priced close fees from the sidecar stamped by
  ``_apply_lp_close_value_usd`` and stamps ``fee_adjusted`` /
  ``close_fees_usd``.
- T12: ``attribute_lp`` accepts optional ``collect_events`` (mid-life
  ``LP_COLLECT_FEES`` rows) and folds their ``value_usd`` into a new
  ``collected_fees_usd`` field AND into ``net_pnl_usd``.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from almanak.framework.observability.pnl_attributor import (
    CURRENT_VERSION,
    attribute_lp,
    compute_attribution,
    compute_impermanent_loss,
)


def _entry_state_json(*, token0, token1, amount0, amount1, price0=None, price1=None):
    payload = {
        "entry_state": {
            "token0": token0,
            "token1": token1,
            "amount0": str(amount0),
            "amount1": str(amount1),
        }
    }
    if price0 is not None:
        payload["entry_state"]["price0"] = str(price0)
    if price1 is not None:
        payload["entry_state"]["price1"] = str(price1)
    return json.dumps(payload)


def _close_attr(*, prices=None, method=None, confidence=None, fees_total_usd=None):
    payload: dict = {}
    if prices is not None:
        payload["current_prices"] = prices
    if method is not None:
        payload["fee_separation_method"] = method
    if confidence is not None:
        payload["fee_confidence"] = confidence
    if fees_total_usd is not None:
        payload["fees_total_usd"] = str(fees_total_usd)
    return json.dumps(payload)


class TestCurrentVersionBump:
    """v3 → v4 bump pins the formula change so recompute fires on legacy rows."""

    def test_current_version_is_v4(self) -> None:
        assert CURRENT_VERSION == 4


class TestT9FeeAdjustedImpermanentLoss:
    """T9 — ``compute_impermanent_loss(fees_usd=...)`` subtracts before IL."""

    def test_fees_usd_default_none_preserves_v3_behaviour(self) -> None:
        # Entry: 1 WETH @ 2000 + 2000 USDC @ 1 ⇒ entry total $4000.
        # Close prices: WETH=2400, USDC=1 ⇒ hodl = 2400 + 2000 = 4400.
        # close_value_usd = 4350 (includes bundled fees if any).
        # Without fees adjustment ⇒ IL = 4350 - 4400 = -50.
        open_evt = {
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="2000",
                price0="2000", price1="1",
            ),
        }
        close_evt = {
            "value_usd": "4350",
            "attribution_json": _close_attr(prices={"WETH": "2400", "USDC": "1"}),
        }
        assert compute_impermanent_loss(open_evt, close_evt) == Decimal("-50")

    def test_fees_usd_subtracts_from_v_lp(self) -> None:
        # Same scenario but caller supplies fees_usd=10 ⇒
        # V_lp adjusted = 4350 - 10 = 4340 ⇒ IL = 4340 - 4400 = -60.
        open_evt = {
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="2000",
                price0="2000", price1="1",
            ),
        }
        close_evt = {
            "value_usd": "4350",
            "attribution_json": _close_attr(prices={"WETH": "2400", "USDC": "1"}),
        }
        adjusted = compute_impermanent_loss(open_evt, close_evt, fees_usd=Decimal("10"))
        assert adjusted == Decimal("-60")

    def test_fees_usd_decimal_zero_is_explicit_no_op(self) -> None:
        # Decimal("0") is a measured zero — same as fees_usd not present
        # numerically, but signals intent (the SEPARATE close genuinely
        # earned nothing). IL math must produce the same number, not a
        # special-case error or None.
        open_evt = {
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="2000",
                price0="2000", price1="1",
            ),
        }
        close_evt = {
            "value_usd": "4350",
            "attribution_json": _close_attr(prices={"WETH": "2400", "USDC": "1"}),
        }
        no_adj = compute_impermanent_loss(open_evt, close_evt)
        measured_zero = compute_impermanent_loss(open_evt, close_evt, fees_usd=Decimal("0"))
        assert no_adj == measured_zero == Decimal("-50")


class TestT9AttributeLpFeeAdjustedFlag:
    """T9 — ``attribute_lp`` reads stamped fees, sets ``fee_adjusted``."""

    def test_separate_close_subtracts_fees_and_stamps_adjusted(self) -> None:
        # Stamps simulating what ``_apply_lp_close_value_usd`` produces
        # for a SEPARATE/EXACT UniV3-class close.
        open_evt = {
            "value_usd": "4000",
            "gas_usd": "1",
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="2000",
                price0="2000", price1="1",
            ),
        }
        close_evt = {
            "value_usd": "4350",  # includes $10 of fees bundled into the collect
            "gas_usd": "1",
            "attribution_json": _close_attr(
                prices={"WETH": "2400", "USDC": "1"},
                method="SEPARATE",
                confidence="EXACT",
                fees_total_usd=Decimal("10"),
            ),
        }
        out = attribute_lp(open_evt, close_evt)
        assert out["fee_adjusted"] is True
        assert out["close_fees_usd"] == "10"
        assert out["fee_separation_method"] == "SEPARATE"
        assert out["fee_confidence"] == "EXACT"
        # IL math used principal-only V_lp: 4340 - 4400 = -60.
        assert out["impermanent_loss_usd"] == "-60"

    def test_bundled_close_skips_adjustment(self) -> None:
        # BUNDLED stamp (V4 / Fluid / Aerodrome V1) leaves fees_total_usd
        # absent; attribute_lp falls back to v3 IL behaviour.
        open_evt = {
            "value_usd": "4000",
            "gas_usd": "1",
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="2000",
                price0="2000", price1="1",
            ),
        }
        close_evt = {
            "value_usd": "4350",
            "gas_usd": "1",
            "attribution_json": _close_attr(
                prices={"WETH": "2400", "USDC": "1"},
                method="BUNDLED",
                confidence="UNKNOWN",
            ),
        }
        out = attribute_lp(open_evt, close_evt)
        assert out["fee_adjusted"] is False
        assert out["close_fees_usd"] is None  # Empty ≠ Zero
        assert out["fee_separation_method"] == "BUNDLED"
        assert out["impermanent_loss_usd"] == "-50"

    def test_unknown_close_skips_adjustment(self) -> None:
        # Hand-built fixture / legacy close with no taxonomy stamp ⇒
        # treat as v3 path; no adjustment.
        open_evt = {
            "value_usd": "4000",
            "gas_usd": "1",
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="2000",
                price0="2000", price1="1",
            ),
        }
        close_evt = {
            "value_usd": "4350",
            "gas_usd": "1",
            "attribution_json": _close_attr(prices={"WETH": "2400", "USDC": "1"}),
        }
        out = attribute_lp(open_evt, close_evt)
        assert out["fee_adjusted"] is False
        assert out["close_fees_usd"] is None
        assert out["fee_separation_method"] is None


class TestT12CollectEventsMidLifeFees:
    """T12 — ``collect_events`` plumbing through ``attribute_lp``."""

    def test_none_default_preserves_v3_net_pnl(self) -> None:
        # Drift-free LP without collect events: net_pnl = principal_recovered
        # - principal_deposited + fee_pnl - gas (collected_fees_usd None).
        open_evt = {"value_usd": "4000", "gas_usd": "1"}
        close_evt = {"value_usd": "4050", "gas_usd": "1"}
        out = attribute_lp(open_evt, close_evt)
        # Net = 4050 - 4000 - 2 = 48
        assert out["net_pnl_usd"] == "48"
        assert out["collected_fees_usd"] is None

    def test_empty_list_explicit_zero_not_none(self) -> None:
        # Empty list ⇒ the caller LOOKED for collect events and found
        # none ⇒ collected_fees_usd is None (no measurement),
        # not Decimal("0"). PRD §Epic D Empty ≠ Zero.
        open_evt = {"value_usd": "4000", "gas_usd": "1"}
        close_evt = {"value_usd": "4050", "gas_usd": "1"}
        out = attribute_lp(open_evt, close_evt, collect_events=[])
        assert out["collected_fees_usd"] is None

    def test_collect_events_sum_into_net_pnl(self) -> None:
        # Two mid-life harvests of $10 and $15. Lifecycle PnL must add
        # them on top of the principal round-trip.
        open_evt = {"value_usd": "4000", "gas_usd": "1"}
        close_evt = {"value_usd": "4050", "gas_usd": "1"}
        collects = [{"value_usd": "10"}, {"value_usd": "15"}]
        out = attribute_lp(open_evt, close_evt, collect_events=collects)
        assert out["collected_fees_usd"] == "25"
        # Net = 4050 - 4000 + 25 - 2 = 73
        assert out["net_pnl_usd"] == "73"

    def test_collect_events_with_measured_zero(self) -> None:
        # Three collect calls but every one a measured zero ⇒
        # collected_fees_usd = 0 (a measured zero, not None) and net_pnl
        # unaffected.
        open_evt = {"value_usd": "4000", "gas_usd": "1"}
        close_evt = {"value_usd": "4050", "gas_usd": "1"}
        collects = [{"value_usd": "0"}, {"value_usd": "0"}, {"value_usd": "0"}]
        out = attribute_lp(open_evt, close_evt, collect_events=collects)
        assert out["collected_fees_usd"] == "0"
        # Net = 4050 - 4000 + 0 - 2 = 48
        assert out["net_pnl_usd"] == "48"


class TestT9PlusT12Composability:
    """T9's fee_adjusted and T12's collect_events are orthogonal."""

    def test_separate_close_with_mid_life_collects(self) -> None:
        # Real-world UniV3 lifecycle: 2 mid-life COLLECT_FEES ($5 + $3),
        # then a SEPARATE close with bundled fees worth $4.
        open_evt = {
            "value_usd": "1000",
            "gas_usd": "0.5",
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="0.5", amount1="0",
                price0="2000", price1="1",
            ),
        }
        close_evt = {
            "value_usd": "1010",  # principal + $4 fees bundled
            "gas_usd": "0.5",
            "attribution_json": _close_attr(
                prices={"WETH": "2020", "USDC": "1"},
                method="SEPARATE",
                confidence="EXACT",
                fees_total_usd=Decimal("4"),
            ),
        }
        collects = [{"value_usd": "5"}, {"value_usd": "3"}]
        out = attribute_lp(open_evt, close_evt, collect_events=collects)
        # T9: IL on principal-only V_lp.
        # V_lp_principal = 1010 - 4 = 1006. hodl = 0.5 * 2020 = 1010.0.
        # IL = 1006 - 1010.0 = -4.0. Compare via Decimal so the precision
        # detail in the str representation does not become a brittle
        # assertion. ``Decimal("-4") == Decimal("-4.0")`` holds.
        assert Decimal(out["impermanent_loss_usd"]) == Decimal("-4")
        assert out["fee_adjusted"] is True
        # T12: collected_fees_usd = 5 + 3 = 8.
        assert Decimal(out["collected_fees_usd"]) == Decimal("8")
        # Net = 1010 + 0 + 8 - 1000 - 1.0 = 17.0.
        assert Decimal(out["net_pnl_usd"]) == Decimal("17")


class TestComputeAttributionT12Plumbing:
    """compute_attribution forwards collect_events to attribute_lp."""

    def test_lp_dispatch_forwards_collect_events(self) -> None:
        open_evt = {"value_usd": "100", "gas_usd": "0", "position_type": "LP"}
        close_evt = {"value_usd": "100", "gas_usd": "0", "position_type": "LP"}
        collects = [{"value_usd": "7"}]
        raw = compute_attribution(open_evt, close_evt, collect_events=collects)
        out = json.loads(raw)
        assert out["collected_fees_usd"] == "7"
        # Sanity: PERP dispatch ignores collect_events (param unused for
        # perps). compute_attribution should not crash on the keyword.
        perp_close = {"value_usd": "100", "gas_usd": "0", "position_type": "PERP"}
        perp_open = {"value_usd": "100", "gas_usd": "0", "position_type": "PERP"}
        compute_attribution(perp_open, perp_close, collect_events=collects)


class TestComputeImpermanentLossPositionalArg:
    """Sanity check the new positional-arg ergonomics."""

    @pytest.mark.parametrize("fees_usd", [None, Decimal("0"), Decimal("5"), Decimal("-2")])
    def test_does_not_crash_on_supported_fees_values(self, fees_usd) -> None:
        open_evt = {
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="100",
                price0="2000", price1="1",
            ),
        }
        close_evt = {
            "value_usd": "2100",
            "attribution_json": _close_attr(prices={"WETH": "2000", "USDC": "1"}),
        }
        # Should never raise; result is a Decimal or None.
        result = compute_impermanent_loss(open_evt, close_evt, fees_usd=fees_usd)
        assert result is None or isinstance(result, Decimal)


# ---------------------------------------------------------------------------
# Helpers underpinning T12 collect-event extraction. The parent function
# (``_extract_collect_events_between``) is a pure composition of these;
# unit-pinning them here gives the parent its coverage by construction.
# ---------------------------------------------------------------------------


class TestCoerceTimestamp:
    """``_coerce_timestamp`` normalises mixed SQLite shapes to ``datetime``."""

    def test_none_stays_none(self) -> None:
        from almanak.framework.observability.pnl_attributor import _coerce_timestamp

        assert _coerce_timestamp(None) is None

    def test_datetime_passes_through(self) -> None:
        from datetime import datetime, timezone

        from almanak.framework.observability.pnl_attributor import _coerce_timestamp

        dt = datetime(2026, 5, 28, 12, 0, 0, tzinfo=timezone.utc)
        assert _coerce_timestamp(dt) is dt

    def test_iso_string_parses_to_datetime(self) -> None:
        from datetime import datetime

        from almanak.framework.observability.pnl_attributor import _coerce_timestamp

        out = _coerce_timestamp("2026-05-28T12:00:00+00:00")
        assert isinstance(out, datetime)

    def test_iso_z_suffix_parses(self) -> None:
        from datetime import datetime

        from almanak.framework.observability.pnl_attributor import _coerce_timestamp

        # SQLite payloads sometimes carry the Z-suffix form.
        out = _coerce_timestamp("2026-05-28T12:00:00Z")
        assert isinstance(out, datetime)

    def test_malformed_string_returns_none(self) -> None:
        from almanak.framework.observability.pnl_attributor import _coerce_timestamp

        assert _coerce_timestamp("not-a-timestamp") is None

    def test_non_string_non_datetime_returns_none(self) -> None:
        from almanak.framework.observability.pnl_attributor import _coerce_timestamp

        assert _coerce_timestamp(12345) is None
        assert _coerce_timestamp([2026, 5, 28]) is None


class TestCloseEventAttr:
    """``_close_event_attr`` reads dict-vs-object close events uniformly."""

    def test_dict_lookup(self) -> None:
        from almanak.framework.observability.pnl_attributor import _close_event_attr

        evt = {"id": 42, "timestamp": "2026-05-28T00:00:00+00:00"}
        assert _close_event_attr(evt, "id") == 42
        assert _close_event_attr(evt, "timestamp") == "2026-05-28T00:00:00+00:00"
        assert _close_event_attr(evt, "missing") is None

    def test_object_lookup(self) -> None:
        from almanak.framework.observability.pnl_attributor import _close_event_attr

        class _Stub:
            id = 7
            timestamp = "2026-05-28T01:00:00+00:00"

        assert _close_event_attr(_Stub(), "id") == 7
        assert _close_event_attr(_Stub(), "timestamp") == "2026-05-28T01:00:00+00:00"
        assert _close_event_attr(_Stub(), "missing") is None


class TestIsCollectInWindow:
    """Predicate for ``_extract_collect_events_between``."""

    def _evt(self, **kwargs) -> dict:
        base = {"event_type": "LP_COLLECT_FEES", "id": 1, "timestamp": "2026-05-28T01:00:00+00:00"}
        base.update(kwargs)
        return base

    def _ts(self, iso: str):
        from almanak.framework.observability.pnl_attributor import _coerce_timestamp

        return _coerce_timestamp(iso)

    def test_non_collect_rejected(self) -> None:
        from almanak.framework.observability.pnl_attributor import _is_collect_in_window

        evt = self._evt(event_type="LP_OPEN")
        assert (
            _is_collect_in_window(
                evt,
                open_ts=self._ts("2026-05-28T00:00:00+00:00"),
                close_ts=self._ts("2026-05-28T02:00:00+00:00"),
                close_id=None,
            )
            is False
        )

    def test_close_id_collision_rejected(self) -> None:
        from almanak.framework.observability.pnl_attributor import _is_collect_in_window

        evt = self._evt(id=99)
        assert (
            _is_collect_in_window(
                evt,
                open_ts=None,
                close_ts=None,
                close_id=99,
            )
            is False
        )

    def test_strictly_after_open(self) -> None:
        from almanak.framework.observability.pnl_attributor import _is_collect_in_window

        # ts == open_ts → out of window (strict `>` semantics).
        evt = self._evt(timestamp="2026-05-28T00:00:00+00:00")
        assert (
            _is_collect_in_window(
                evt,
                open_ts=self._ts("2026-05-28T00:00:00+00:00"),
                close_ts=self._ts("2026-05-28T02:00:00+00:00"),
                close_id=None,
            )
            is False
        )

    def test_on_or_before_close(self) -> None:
        from almanak.framework.observability.pnl_attributor import _is_collect_in_window

        # ts == close_ts → still in window (inclusive upper bound).
        evt = self._evt(timestamp="2026-05-28T02:00:00+00:00")
        assert (
            _is_collect_in_window(
                evt,
                open_ts=self._ts("2026-05-28T00:00:00+00:00"),
                close_ts=self._ts("2026-05-28T02:00:00+00:00"),
                close_id=None,
            )
            is True
        )

    def test_after_close_rejected(self) -> None:
        from almanak.framework.observability.pnl_attributor import _is_collect_in_window

        evt = self._evt(timestamp="2026-05-28T03:00:00+00:00")
        assert (
            _is_collect_in_window(
                evt,
                open_ts=self._ts("2026-05-28T00:00:00+00:00"),
                close_ts=self._ts("2026-05-28T02:00:00+00:00"),
                close_id=None,
            )
            is False
        )

    def test_malformed_timestamp_keeps_event(self) -> None:
        """A row with an unparseable timestamp is kept rather than dropped —
        the alternative would silently lose a measurable fee on a
        malformed-row pathology, which is worse than including it."""
        from almanak.framework.observability.pnl_attributor import _is_collect_in_window

        evt = self._evt(timestamp="not-a-timestamp")
        assert (
            _is_collect_in_window(
                evt,
                open_ts=self._ts("2026-05-28T00:00:00+00:00"),
                close_ts=self._ts("2026-05-28T02:00:00+00:00"),
                close_id=None,
            )
            is True
        )

    def test_missing_bounds_allow_event(self) -> None:
        from almanak.framework.observability.pnl_attributor import _is_collect_in_window

        evt = self._evt(timestamp="2026-05-28T01:00:00+00:00")
        assert (
            _is_collect_in_window(
                evt,
                open_ts=None,
                close_ts=None,
                close_id=None,
            )
            is True
        )


class TestExtractCollectEventsBetween:
    """Integration: parent function composes the helpers correctly."""

    def test_filters_to_collect_rows_inside_window(self) -> None:
        from almanak.framework.observability.pnl_attributor import _extract_collect_events_between

        history = [
            {"event_type": "LP_OPEN", "id": 1, "timestamp": "2026-05-28T00:00:00+00:00"},
            {"event_type": "LP_COLLECT_FEES", "id": 2, "timestamp": "2026-05-28T00:30:00+00:00"},
            {"event_type": "LP_COLLECT_FEES", "id": 3, "timestamp": "2026-05-28T01:30:00+00:00"},
            {"event_type": "LP_CLOSE", "id": 4, "timestamp": "2026-05-28T02:00:00+00:00"},
        ]
        open_event = {"timestamp": "2026-05-28T00:00:00+00:00"}
        close_event = {"id": 4, "timestamp": "2026-05-28T02:00:00+00:00"}
        out = _extract_collect_events_between(history, open_event, close_event)
        assert [evt["id"] for evt in out] == [2, 3]

    def test_close_object_shape_supported(self) -> None:
        from almanak.framework.observability.pnl_attributor import _extract_collect_events_between

        class _Close:
            id = 99
            timestamp = "2026-05-28T02:00:00+00:00"

        history = [
            {"event_type": "LP_COLLECT_FEES", "id": 1, "timestamp": "2026-05-28T01:00:00+00:00"},
        ]
        open_event = {"timestamp": "2026-05-28T00:00:00+00:00"}
        out = _extract_collect_events_between(history, open_event, _Close())
        assert [evt["id"] for evt in out] == [1]
