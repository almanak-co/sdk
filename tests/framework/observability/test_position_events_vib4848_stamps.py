"""VIB-4848 (T8 / T9) — sidecar stamping helpers in ``position_events``.

Pins the three close-event enrichers added on the strategy-side
position_events lane:

- ``_load_attribution_dict`` — tolerant JSON loader used by every
  T8 / T9 / T12 enricher to read ``event.attribution_json`` into a
  mutable dict (returns ``{}`` on malformed JSON, list payloads, or
  ``None``).
- ``_fees_unmeasured`` — Empty ≠ Zero guard for the
  ``PositionEvent.fees_token0`` / ``fees_token1`` columns (which
  default to ``""`` over the SQLite round-trip).  ``None`` and ``""``
  are unmeasured; an explicit ``"0"`` is measured zero and must
  pass through.
- ``_stamp_lp_close_fees_total_usd`` — T9 sidecar that emits
  ``fees_total_usd`` when (and only when) the fee separation taxonomy
  is SEPARATE and both legs are measurable.  BUNDLED closes (UniV4 /
  Fluid / Aerodrome V1) intentionally leave the field unstamped so the
  attribution lane preserves the unknown signal rather than substitute
  a fabricated zero.

The previous test suite covered the dataclass-side taxonomy
(``test_lpclose_data_defaults.py::TestLPCloseDataFeeSeparationTaxonomy``)
and the attribution-side consumption
(``test_pnl_attributor_vib4848.py::TestT9*``), but did not exercise
the position_events enricher itself.  CI's CRAP gate flagged that gap
(cov=5%) — this file closes it without changing behaviour.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from almanak.framework.observability.position_events import (
    PositionEvent,
    _fees_unmeasured,
    _load_attribution_dict,
    _stamp_lp_close_fees_total_usd,
)


def _close_event(**overrides) -> PositionEvent:
    """Build a minimally-populated CLOSE PositionEvent.

    Mirrors what Phase δ would have written by the time
    ``_stamp_lp_close_fees_total_usd`` runs.  Overrides on top let
    individual tests vary fees_token0 / fees_token1 / attribution_json.
    """
    base = {
        "deployment_id": "strat:vib4848",
        "position_id": "12345",
        "position_type": "LP",
        "event_type": "CLOSE",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "USDC",
        "token1": "WETH",
    }
    base.update(overrides)
    return PositionEvent(**base)


class TestLoadAttributionDict:
    """``_load_attribution_dict`` tolerates the SQLite payload shapes."""

    def test_none_returns_empty_dict(self) -> None:
        evt = _close_event(attribution_json=None)
        assert _load_attribution_dict(evt) == {}

    def test_empty_string_returns_empty_dict(self) -> None:
        evt = _close_event(attribution_json="")
        assert _load_attribution_dict(evt) == {}

    def test_valid_json_dict_roundtrips(self) -> None:
        evt = _close_event(attribution_json=json.dumps({"foo": "bar", "n": 1}))
        out = _load_attribution_dict(evt)
        assert out == {"foo": "bar", "n": 1}

    def test_malformed_json_returns_empty_dict(self) -> None:
        evt = _close_event(attribution_json="{not-json}")
        assert _load_attribution_dict(evt) == {}

    def test_non_dict_json_returns_empty_dict(self) -> None:
        # A list / int / string payload at the top level is not a dict;
        # the loader must defensively coalesce so downstream callers can
        # safely subscript without ``TypeError``.
        for payload in ("[]", "42", "\"hello\""):
            evt = _close_event(attribution_json=payload)
            assert _load_attribution_dict(evt) == {}


class TestFeesUnmeasured:
    """Empty ≠ Zero guard for the fees columns."""

    @pytest.mark.parametrize("raw", [None, ""])
    def test_unmeasured_signals(self, raw) -> None:
        assert _fees_unmeasured(raw) is True

    @pytest.mark.parametrize("raw", ["0", "0.0", "1", "42", "100000"])
    def test_measured_values_pass_through(self, raw) -> None:
        # An explicit ``"0"`` is measured zero — distinct from ``""``.
        assert _fees_unmeasured(raw) is False

    def test_measured_zero_distinct_from_unmeasured(self) -> None:
        # Pinning the Empty ≠ Zero contract explicitly so the next
        # contributor does not collapse the two.
        assert _fees_unmeasured("") != _fees_unmeasured("0")


class TestStampLpCloseFeesTotalUsd:
    """T9 sidecar emits only on SEPARATE/EXACT closes with measurable fees."""

    def _stamp(
        self,
        evt: PositionEvent,
        *,
        decimals0: int = 6,
        decimals1: int = 18,
        price0: Decimal = Decimal("1"),
        price1: Decimal = Decimal("2000"),
    ) -> dict:
        _stamp_lp_close_fees_total_usd(evt, decimals0, decimals1, price0, price1)
        return _load_attribution_dict(evt)

    def test_bundled_method_skips_stamping(self) -> None:
        # BUNDLED closes (UniV4 / Fluid / Aerodrome V1) intentionally
        # leave fees_total_usd unstamped — Empty ≠ Zero.
        evt = _close_event(
            attribution_json=json.dumps({"fee_separation_method": "BUNDLED"}),
            fees_token0="100",
            fees_token1="200",
        )
        out = self._stamp(evt)
        assert "fees_total_usd" not in out

    def test_unknown_method_skips_stamping(self) -> None:
        evt = _close_event(
            attribution_json=json.dumps({"fee_separation_method": "UNKNOWN"}),
            fees_token0="100",
            fees_token1="200",
        )
        out = self._stamp(evt)
        assert "fees_total_usd" not in out

    def test_missing_method_skips_stamping(self) -> None:
        # No fee_separation_method on the sidecar (legacy row) ⇒
        # treated as not-SEPARATE ⇒ skip.
        evt = _close_event(
            attribution_json="{}",
            fees_token0="100",
            fees_token1="200",
        )
        out = self._stamp(evt)
        assert "fees_total_usd" not in out

    def test_separate_with_default_empty_fees_skips(self) -> None:
        # SEPARATE but both fees default to "" (parser didn't populate)
        # ⇒ unmeasured ⇒ skip rather than coerce Decimal("").
        evt = _close_event(
            attribution_json=json.dumps({"fee_separation_method": "SEPARATE"}),
        )
        # Confirm the column defaults are "" not None.
        assert evt.fees_token0 == ""
        assert evt.fees_token1 == ""
        out = self._stamp(evt)
        assert "fees_total_usd" not in out

    def test_separate_with_one_unmeasured_leg_skips(self) -> None:
        evt = _close_event(
            attribution_json=json.dumps({"fee_separation_method": "SEPARATE"}),
            fees_token0="100",
            # fees_token1 stays at default ""
        )
        out = self._stamp(evt)
        assert "fees_total_usd" not in out

    def test_separate_with_measured_fees_emits_stamp(self) -> None:
        evt = _close_event(
            attribution_json=json.dumps({"fee_separation_method": "SEPARATE"}),
            fees_token0=str(5 * 10**6),  # 5 USDC raw
            fees_token1=str(10**16),  # 0.01 WETH raw
        )
        out = self._stamp(evt)
        # 5 * $1 + 0.01 * $2000 = $25.
        assert "fees_total_usd" in out
        assert Decimal(out["fees_total_usd"]) == Decimal("25")

    def test_separate_with_measured_zero_emits_stamp(self) -> None:
        # Measured zero ≠ unmeasured: SEPARATE/EXACT closes that observed
        # zero fees still get a stamp.
        evt = _close_event(
            attribution_json=json.dumps({"fee_separation_method": "SEPARATE"}),
            fees_token0="0",
            fees_token1="0",
        )
        out = self._stamp(evt)
        assert "fees_total_usd" in out
        assert Decimal(out["fees_total_usd"]) == Decimal("0")

    def test_malformed_decimal_input_silently_skips(self) -> None:
        # Parser emitted garbage on fees_token0; the function must
        # tolerate it rather than propagate InvalidOperation up the
        # enrichment pipeline.
        evt = _close_event(
            attribution_json=json.dumps({"fee_separation_method": "SEPARATE"}),
            fees_token0="abc",
            fees_token1="100",
        )
        out = self._stamp(evt)
        # No stamp; the sidecar metadata (method) is preserved unchanged.
        assert "fees_total_usd" not in out
        assert out.get("fee_separation_method") == "SEPARATE"

    def test_existing_sidecar_keys_preserved(self) -> None:
        # The function must not clobber unrelated sidecar keys when
        # stamping (other T8/T12 fields, funding_fee_usd from a perp
        # parallel write, etc.).
        evt = _close_event(
            attribution_json=json.dumps(
                {
                    "fee_separation_method": "SEPARATE",
                    "fee_confidence": "EXACT",
                    "entry_state": {"price0": "1", "price1": "2000"},
                }
            ),
            fees_token0=str(10**6),  # 1 USDC raw
            fees_token1="0",
        )
        out = self._stamp(evt)
        assert out["fee_separation_method"] == "SEPARATE"
        assert out["fee_confidence"] == "EXACT"
        assert out["entry_state"] == {"price0": "1", "price1": "2000"}
        # 1 * $1 + 0 * $2000 = $1.
        assert Decimal(out["fees_total_usd"]) == Decimal("1")
