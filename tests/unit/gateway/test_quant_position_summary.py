"""Branch coverage for ``_QuantPositionSummary`` (VIB-5059 Phase 1-SQL extraction).

The class was extracted verbatim from ``_load_quant_inputs`` to keep the loader
under the complexity budget; these tests pin every parse/dispatch branch so the
shim cannot silently change shape (malformed JSON, non-list payloads, the LP
in_range tri-state, and the LENDING/PERP bad-decimal swallows).
"""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

from almanak.gateway.services.dashboard_service import _QuantPositionSummary


def _snap(positions_json):
    return SimpleNamespace(positions_json=positions_json)


class TestPositionsJsonParsing:
    def test_none_positions_json_yields_empty_defaults(self):
        s = _QuantPositionSummary(_snap(None))
        assert s.lp_positions == []
        assert s.health_factor is None
        assert s.leverage is None

    def test_missing_positions_json_attribute_yields_empty_defaults(self):
        s = _QuantPositionSummary(SimpleNamespace())
        assert s.lp_positions == []
        assert s.health_factor is None
        assert s.leverage is None

    def test_invalid_json_yields_empty_defaults(self):
        s = _QuantPositionSummary(_snap("{not json"))
        assert s.lp_positions == []
        assert s.health_factor is None
        assert s.leverage is None

    def test_non_list_non_envelope_json_yields_empty_defaults(self):
        s = _QuantPositionSummary(_snap(json.dumps({"position_type": "LP"})))
        assert s.lp_positions == []
        assert s.health_factor is None
        assert s.leverage is None

    def test_already_deserialized_list_is_consumed(self):
        s = _QuantPositionSummary(_snap([{"position_type": "LP", "in_range": True}]))
        assert [p.in_range for p in s.lp_positions] == [True]

    def test_envelope_dict_is_unwrapped(self):
        envelope = {"positions": [{"position_type": "LENDING", "health_factor": "1.2"}]}
        s = _QuantPositionSummary(_snap(envelope))
        assert s.health_factor == Decimal("1.2")

    def test_envelope_json_string_is_unwrapped(self):
        envelope = json.dumps({"positions": [{"position_type": "PERP", "leverage": "5"}]})
        s = _QuantPositionSummary(_snap(envelope))
        assert s.leverage == Decimal("5")

    def test_non_dict_items_are_skipped(self):
        s = _QuantPositionSummary(_snap(json.dumps(["LP", 7, None])))
        assert s.lp_positions == []

    def test_unknown_and_missing_position_types_are_ignored(self):
        payload = json.dumps([{"position_type": "TOKEN"}, {"no_type_key": True}])
        s = _QuantPositionSummary(_snap(payload))
        assert s.lp_positions == []
        assert s.health_factor is None
        assert s.leverage is None


class TestLpInRangeTriState:
    def test_in_range_none_stays_none(self):
        s = _QuantPositionSummary(_snap(json.dumps([{"position_type": "LP", "in_range": None}])))
        assert len(s.lp_positions) == 1
        assert s.lp_positions[0].in_range is None

    def test_in_range_true_and_false_become_bools(self):
        payload = json.dumps(
            [
                {"position_type": "LP", "in_range": True},
                {"position_type": "lp", "in_range": 0},
            ]
        )
        s = _QuantPositionSummary(_snap(payload))
        assert [p.in_range for p in s.lp_positions] == [True, False]


class TestLendingHealthFactor:
    def test_numeric_health_factor_parsed_to_decimal(self):
        s = _QuantPositionSummary(_snap(json.dumps([{"position_type": "LENDING", "health_factor": "1.85"}])))
        assert s.health_factor == Decimal("1.85")

    def test_none_health_factor_stays_unmeasured(self):
        s = _QuantPositionSummary(_snap(json.dumps([{"position_type": "LENDING", "health_factor": None}])))
        assert s.health_factor is None

    def test_garbage_health_factor_swallowed_not_raised(self):
        s = _QuantPositionSummary(_snap(json.dumps([{"position_type": "LENDING", "health_factor": "n/a"}])))
        assert s.health_factor is None


class TestPerpLeverage:
    def test_numeric_leverage_parsed_to_decimal(self):
        s = _QuantPositionSummary(_snap(json.dumps([{"position_type": "PERP", "leverage": 3}])))
        assert s.leverage == Decimal("3")

    def test_none_leverage_stays_unmeasured(self):
        s = _QuantPositionSummary(_snap(json.dumps([{"position_type": "PERP", "leverage": None}])))
        assert s.leverage is None

    def test_garbage_leverage_swallowed_not_raised(self):
        s = _QuantPositionSummary(_snap(json.dumps([{"position_type": "PERP", "leverage": "max"}])))
        assert s.leverage is None


def test_mixed_positions_populate_all_fields():
    payload = json.dumps(
        [
            {"position_type": "LP", "in_range": True},
            {"position_type": "LENDING", "health_factor": "2.5"},
            {"position_type": "PERP", "leverage": "4.2"},
            {"position_type": "LP", "in_range": None},
        ]
    )
    s = _QuantPositionSummary(_snap(payload))
    assert [p.in_range for p in s.lp_positions] == [True, None]
    assert s.health_factor == Decimal("2.5")
    assert s.leverage == Decimal("4.2")
