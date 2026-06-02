"""Tests for ``almanak.framework.accounting.reporting.render_json``.

Pure dict-builders — given a section dataclass, produce a JSON-serialisable
dict. Tests cover the four ``*_to_dict`` entry points used by
``cli/strat_pnl.py`` for ``--json`` output.

The contract these tests pin:

* Decimal fields → string (preserves precision; never float-coerced).
* None-able Decimals → None when unset, never empty string.
* Booleans / ints / strings pass through unchanged.
* datetime → ISO-format string.
* Empty section → empty positions/issues lists, scalar fields still present.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.accounting.reporting.data_quality import (
    DataQualityIssue,
    DataQualitySection,
)
from almanak.framework.accounting.reporting.lending_report import (
    LendingPositionSummary,
    LendingSection,
)
from almanak.framework.accounting.reporting.lp_report import (
    LPPositionSummary,
    LPSection,
)
from almanak.framework.accounting.reporting.pendle_report import (
    PendlePositionSummary,
    PendleSection,
)
from almanak.framework.accounting.reporting.render_json import (
    data_quality_to_dict,
    lending_section_to_dict,
    lp_section_to_dict,
    pendle_section_to_dict,
)


def _assert_json_roundtrip(payload: dict) -> None:
    """Assert payload survives a json.dumps → json.loads round-trip unchanged.

    Stronger than just asserting `json.dumps` doesn't raise: confirms every
    value in the dict is JSON-native (str/int/float/bool/None/list/dict) AND
    that the parsed-back object is structurally equal to the original. This
    catches silent lossy serialisations (e.g. a Decimal that gets coerced via
    a custom encoder, or a tuple becoming a list when we expected list).
    """
    encoded = json.dumps(payload)
    assert json.loads(encoded) == payload


# ──────────────────────────────────────────────────────────────────────────────
# lp_section_to_dict
# ──────────────────────────────────────────────────────────────────────────────


def _lp_pos(**overrides) -> LPPositionSummary:
    base: dict = {
        "position_id": "0xabc123",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "WETH",
        "token1": "USDC",
        "entry_value_usd": Decimal("10000.50"),
    }
    base.update(overrides)
    return LPPositionSummary(**base)


class TestLpSectionToDict:
    def test_empty_section(self):
        out = lp_section_to_dict(LPSection())
        assert out == {
            "positions": [],
            "total_net_pnl_usd": None,
            "total_gas_usd": "0",
        }

    def test_single_position_full(self):
        section = LPSection(
            positions=[
                _lp_pos(
                    is_closed=True,
                    exit_value_usd=Decimal("11000.25"),
                    fees_token0=Decimal("0.5"),
                    fees_token1=Decimal("100"),
                    protocol_fees_usd=Decimal("3.21"),
                    total_gas_usd=Decimal("12.34"),
                    il_usd=Decimal("-50"),
                    net_pnl_usd=Decimal("950.75"),
                    in_range=True,
                )
            ]
        )
        out = lp_section_to_dict(section)
        pos = out["positions"][0]
        # Decimals stringified with full precision.
        assert pos["entry_value_usd"] == "10000.50"
        assert pos["exit_value_usd"] == "11000.25"
        assert pos["fees_token0"] == "0.5"
        assert pos["protocol_fees_usd"] == "3.21"
        assert pos["il_usd"] == "-50"
        assert pos["net_pnl_usd"] == "950.75"
        # Booleans/strings pass through.
        assert pos["is_closed"] is True
        assert pos["in_range"] is True
        assert pos["protocol"] == "uniswap_v3"
        assert pos["chain"] == "arbitrum"

    def test_none_decimal_fields_are_none_not_empty_string(self):
        # exit/il/net_pnl/entry default to None if not set on the dataclass.
        out = lp_section_to_dict(LPSection(positions=[_lp_pos(entry_value_usd=None)]))
        pos = out["positions"][0]
        assert pos["entry_value_usd"] is None
        assert pos["exit_value_usd"] is None
        assert pos["il_usd"] is None
        assert pos["net_pnl_usd"] is None
        assert pos["in_range"] is None
        # Non-None defaults remain stringified zero.
        assert pos["fees_token0"] == "0"
        assert pos["fees_token1"] == "0"
        assert pos["protocol_fees_usd"] == "0"
        assert pos["total_gas_usd"] == "0"

    def test_total_net_pnl_aggregated_from_positions(self):
        section = LPSection(
            positions=[
                _lp_pos(net_pnl_usd=Decimal("100")),
                _lp_pos(net_pnl_usd=Decimal("-30")),
                _lp_pos(net_pnl_usd=None),  # contributes nothing
            ]
        )
        out = lp_section_to_dict(section)
        assert out["total_net_pnl_usd"] == "70"

    def test_total_net_pnl_none_when_all_positions_none(self):
        out = lp_section_to_dict(LPSection(positions=[_lp_pos(net_pnl_usd=None)]))
        assert out["total_net_pnl_usd"] is None

    def test_output_is_json_serialisable(self):
        out = lp_section_to_dict(LPSection(positions=[_lp_pos()]))
        # Round-trip through json.dumps/json.loads to confirm no Decimal/datetime
        # leakage AND that re-parsing yields a structurally equal payload.
        _assert_json_roundtrip(out)


# ──────────────────────────────────────────────────────────────────────────────
# lending_section_to_dict
# ──────────────────────────────────────────────────────────────────────────────


def _lend_pos(**overrides) -> LendingPositionSummary:
    base: dict = {
        "position_key": "lending:aave_v3:USDC:0xWALLET",
        "protocol": "aave_v3",
        "chain": "ethereum",
        "asset": "USDC",
        "market_id": "0xMARKET",
        "collateral_usd": Decimal("5000"),
        "debt_usd": Decimal("2000"),
    }
    base.update(overrides)
    return LendingPositionSummary(**base)


class TestLendingSectionToDict:
    def test_empty_section(self):
        out = lending_section_to_dict(LendingSection())
        assert out == {"positions": []}

    def test_single_position(self):
        out = lending_section_to_dict(
            LendingSection(
                positions=[
                    _lend_pos(
                        net_equity_usd=Decimal("3000"),
                        health_factor=Decimal("2.5"),
                        liquidation_threshold=Decimal("0.85"),
                        supply_apr_pct=Decimal("3.5"),
                        borrow_apr_pct=Decimal("8.1"),
                        total_gas_usd=Decimal("12.34"),
                        total_interest_delta_usd=Decimal("0.99"),
                        deleverage_count=2,
                        is_closed=True,
                    )
                ]
            )
        )
        pos = out["positions"][0]
        assert pos["position_key"] == "lending:aave_v3:USDC:0xWALLET"
        assert pos["protocol"] == "aave_v3"
        assert pos["asset"] == "USDC"
        assert pos["collateral_usd"] == "5000"
        assert pos["debt_usd"] == "2000"
        assert pos["net_equity_usd"] == "3000"
        assert pos["health_factor"] == "2.5"
        assert pos["liquidation_threshold"] == "0.85"
        assert pos["supply_apr_pct"] == "3.5"
        assert pos["borrow_apr_pct"] == "8.1"
        # int passes through unchanged.
        assert pos["deleverage_count"] == 2
        assert pos["is_closed"] is True

    def test_interest_fields_signed_net_and_per_side(self):
        # VIB-4974: the lending JSON contract carries the signed net realized
        # interest (debt cost negative, supply yield positive) plus the
        # per-side gross magnitudes. Lock all three keys + their values in so
        # the contract can't silently regress.
        out = lending_section_to_dict(
            LendingSection(
                positions=[
                    _lend_pos(
                        total_interest_delta_usd=Decimal("-0.25"),  # paid 0.75 - earned 0.50
                        total_interest_paid_usd=Decimal("0.75"),
                        total_interest_earned_usd=Decimal("0.50"),
                    )
                ]
            )
        )
        pos = out["positions"][0]
        assert pos["total_interest_delta_usd"] == "-0.25"
        assert pos["total_interest_paid_usd"] == "0.75"
        assert pos["total_interest_earned_usd"] == "0.50"

    def test_none_apr_fields_are_none(self):
        out = lending_section_to_dict(LendingSection(positions=[_lend_pos()]))
        pos = out["positions"][0]
        assert pos["net_equity_usd"] is None
        assert pos["health_factor"] is None
        assert pos["supply_apr_pct"] is None
        assert pos["borrow_apr_pct"] is None
        assert pos["liquidation_threshold"] is None

    def test_output_is_json_serialisable(self):
        out = lending_section_to_dict(LendingSection(positions=[_lend_pos()]))
        _assert_json_roundtrip(out)


# ──────────────────────────────────────────────────────────────────────────────
# pendle_section_to_dict
# ──────────────────────────────────────────────────────────────────────────────


def _pendle_pos(**overrides) -> PendlePositionSummary:
    base: dict = {
        "position_key": "pendle:wstETH",
        "market_id": "0xMARKET",
        "pt_token": "PT-wstETH",
        "protocol": "pendle",
        "chain": "ethereum",
    }
    base.update(overrides)
    return PendlePositionSummary(**base)


class TestPendleSectionToDict:
    def test_empty_section(self):
        out = pendle_section_to_dict(PendleSection())
        assert out == {"positions": []}

    def test_single_position(self):
        ts = datetime(2026, 12, 31, 0, 0, tzinfo=UTC)
        out = pendle_section_to_dict(
            PendleSection(
                positions=[
                    _pendle_pos(
                        pt_amount=Decimal("1.234567"),
                        pt_price=Decimal("0.987654"),
                        implied_apr_pct_at_entry=Decimal("8.5"),
                        implied_apr_pct_latest=Decimal("9.0"),
                        days_to_maturity=42,
                        maturity_timestamp=ts,
                        realized_yield_usd=Decimal("100.50"),
                        is_redeemed=True,
                        event_count=5,
                    )
                ]
            )
        )
        pos = out["positions"][0]
        assert pos["pt_amount"] == "1.234567"
        assert pos["pt_price"] == "0.987654"
        assert pos["implied_apr_pct_at_entry"] == "8.5"
        assert pos["implied_apr_pct_latest"] == "9.0"
        assert pos["days_to_maturity"] == 42
        # datetime → ISO format string.
        assert pos["maturity_timestamp"] == "2026-12-31T00:00:00+00:00"
        assert pos["realized_yield_usd"] == "100.50"
        assert pos["is_redeemed"] is True
        assert pos["event_count"] == 5

    def test_maturity_timestamp_none_renders_none(self):
        out = pendle_section_to_dict(PendleSection(positions=[_pendle_pos()]))
        pos = out["positions"][0]
        assert pos["maturity_timestamp"] is None
        assert pos["pt_amount"] is None
        assert pos["pt_price"] is None
        assert pos["implied_apr_pct_at_entry"] is None
        assert pos["implied_apr_pct_latest"] is None
        assert pos["days_to_maturity"] is None
        # realized_yield default (0) stringified.
        assert pos["realized_yield_usd"] == "0"
        assert pos["is_redeemed"] is False
        assert pos["event_count"] == 0

    def test_output_is_json_serialisable(self):
        ts = datetime(2026, 12, 31, 0, 0, tzinfo=UTC)
        out = pendle_section_to_dict(
            PendleSection(positions=[_pendle_pos(maturity_timestamp=ts)])
        )
        _assert_json_roundtrip(out)


# ──────────────────────────────────────────────────────────────────────────────
# data_quality_to_dict
# ──────────────────────────────────────────────────────────────────────────────


def _issue(**overrides) -> DataQualityIssue:
    base: dict = {
        "event_type": "LP_OPEN",
        "position_key": "lp:uniswap_v3:0xPOOL:0xWALLET",
        "timestamp": "2026-05-04T12:00:00+00:00",
        "reason": "stale oracle",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
    }
    base.update(overrides)
    return DataQualityIssue(**base)


class TestDataQualityToDict:
    def test_empty_section(self):
        out = data_quality_to_dict(DataQualitySection())
        assert out == {"unavailable_count": 0, "parse_errors": 0, "issues": []}

    def test_unavailable_count_matches_issues_length(self):
        out = data_quality_to_dict(DataQualitySection(issues=[_issue(), _issue(), _issue()]))
        assert out["unavailable_count"] == 3
        assert len(out["issues"]) == 3

    def test_parse_errors_pass_through(self):
        out = data_quality_to_dict(DataQualitySection(parse_errors=7))
        assert out["parse_errors"] == 7
        assert out["unavailable_count"] == 0
        assert out["issues"] == []

    def test_issue_fields_pass_through_unchanged(self):
        out = data_quality_to_dict(DataQualitySection(issues=[_issue()]))
        i = out["issues"][0]
        assert i["event_type"] == "LP_OPEN"
        assert i["position_key"] == "lp:uniswap_v3:0xPOOL:0xWALLET"
        assert i["timestamp"] == "2026-05-04T12:00:00+00:00"
        assert i["reason"] == "stale oracle"
        assert i["protocol"] == "uniswap_v3"
        assert i["chain"] == "arbitrum"

    def test_output_is_json_serialisable(self):
        out = data_quality_to_dict(DataQualitySection(issues=[_issue()], parse_errors=2))
        _assert_json_roundtrip(out)
