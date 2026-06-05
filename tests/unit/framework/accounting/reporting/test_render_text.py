"""Tests for ``almanak.framework.accounting.reporting.render_text``.

Pure renderers — given a section dataclass, produce a multi-line text block.
Tests cover empty-section short-circuit, dataclass-field formatting helpers
(``_m``, ``_pct``, ``_hf``), and the four ``render_*_section`` entry points
used by ``cli/strat_pnl.py`` / ``cli/ax_render.py`` / ``cli/status_helpers.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.connectors.pendle.reporting import (
    PendlePositionSummary,
    PendleSection,
)
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
from almanak.framework.accounting.reporting.render_text import (
    _hf,
    _m,
    _pct,
    render_data_quality_section,
    render_lending_section,
    render_lp_section,
    render_pendle_section,
)

# ──────────────────────────────────────────────────────────────────────────────
# Formatter helpers
# ──────────────────────────────────────────────────────────────────────────────


class TestMoneyFormatter:
    def test_none_returns_em_dash(self):
        assert _m(None) == "—"

    def test_positive_value_has_leading_space(self):
        # Positive values get a leading space (not '+'); width=8 default.
        assert _m(Decimal("1234.56")) == " $1,234.56"

    def test_negative_value_has_minus_sign(self):
        assert _m(Decimal("-1234.56")) == "-$1,234.56"

    def test_zero(self):
        assert _m(Decimal("0")) == " $    0.00"

    def test_custom_width_pads_left(self):
        # width=12 → minimum 12-character wide number portion.
        result = _m(Decimal("1.23"), width=12)
        assert result.endswith("$        1.23")  # 12-wide number, right-aligned

    def test_two_decimal_places(self):
        # Always quantises to .2f regardless of input precision.
        assert _m(Decimal("100.5")) == " $  100.50"
        assert _m(Decimal("100.555")) == " $  100.56"  # banker rounding via :.2f


class TestPercentFormatter:
    def test_none_returns_em_dash(self):
        assert _pct(None) == "—"

    def test_default_two_decimals(self):
        # Banker rounding: 12.345 → 12.34 (round-half-to-even on .005 halfway).
        assert _pct(Decimal("12.345")) == "12.34%"

    def test_custom_decimals(self):
        assert _pct(Decimal("12.345"), decimals=1) == "12.3%"

    def test_zero(self):
        assert _pct(Decimal("0")) == "0.00%"

    def test_negative(self):
        assert _pct(Decimal("-5.5"), decimals=1) == "-5.5%"


class TestHealthFactorFormatter:
    def test_none_returns_em_dash(self):
        assert _hf(None) == "—"

    def test_three_decimals(self):
        assert _hf(Decimal("1.23456")) == "1.235"

    def test_below_one(self):
        # HF < 1 means liquidation territory; renderer doesn't mark it specially.
        assert _hf(Decimal("0.987")) == "0.987"


# ──────────────────────────────────────────────────────────────────────────────
# render_lp_section
# ──────────────────────────────────────────────────────────────────────────────


def _lp_pos(**overrides) -> LPPositionSummary:
    base: dict = {
        "position_id": "0xabc123def456789012345678",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "WETH",
        "token1": "USDC",
        "entry_value_usd": Decimal("10000"),
    }
    base.update(overrides)
    return LPPositionSummary(**base)


class TestRenderLpSection:
    def test_empty_section_returns_empty_string(self):
        assert render_lp_section(LPSection()) == ""

    def test_open_position_minimal_fields(self):
        section = LPSection(positions=[_lp_pos()])
        out = render_lp_section(section)
        assert "LP Positions" in out
        assert "WETH/USDC" in out
        assert "[uniswap_v3 / arbitrum]" in out
        assert "[OPEN]" in out
        assert "Entry:" in out
        assert "$10,000.00" in out
        # Closed-only fields absent.
        assert "Exit:" not in out
        # IL absent when None.
        assert "IL:" not in out

    def test_closed_position_shows_exit(self):
        section = LPSection(positions=[_lp_pos(is_closed=True, exit_value_usd=Decimal("11000"))])
        out = render_lp_section(section)
        assert "[CLOSED]" in out
        assert "Exit:" in out
        assert "$11,000.00" in out

    def test_fees_block_only_when_nonzero(self):
        # Both fee fields zero → no Fees line.
        out_zero = render_lp_section(LPSection(positions=[_lp_pos()]))
        assert "Fees:" not in out_zero

        # Either fee field nonzero → Fees line present.
        out_nonzero = render_lp_section(LPSection(positions=[_lp_pos(fees_token0=Decimal("0.5"))]))
        assert "Fees:" in out_nonzero
        assert "0.5000 WETH" in out_nonzero

    def test_protocol_fees_only_when_positive(self):
        out = render_lp_section(LPSection(positions=[_lp_pos(protocol_fees_usd=Decimal("12.34"))]))
        assert "Proto fees:" in out

    def test_il_field_when_set(self):
        out = render_lp_section(LPSection(positions=[_lp_pos(il_usd=Decimal("-25"))]))
        assert "IL:" in out
        # Width=8 default → number portion right-padded to 8 chars.
        assert "-$   25.00" in out

    def test_in_range_yes_no(self):
        out_yes = render_lp_section(LPSection(positions=[_lp_pos(in_range=True)]))
        assert "In range:   yes" in out_yes
        out_no = render_lp_section(LPSection(positions=[_lp_pos(in_range=False)]))
        assert "In range:   no" in out_no

    def test_position_id_truncated_to_12_chars_with_ellipsis(self):
        pos = _lp_pos(position_id="0xABCDEF" * 5)
        out = render_lp_section(LPSection(positions=[pos]))
        assert "0xABCDEF0xAB…" in out

    def test_position_id_used_when_token0_empty(self):
        # _lp_pos enforces non-empty token0; build one with empty.
        pos = LPPositionSummary(
            position_id="0xPOSID000000",
            protocol="x",
            chain="y",
            token0="",
            token1="",
        )
        out = render_lp_section(LPSection(positions=[pos]))
        # First identifier line uses position_id (no token0 fallback path).
        assert "0xPOSID000000" in out

    def test_totals_at_end(self):
        section = LPSection(
            positions=[
                _lp_pos(net_pnl_usd=Decimal("100"), total_gas_usd=Decimal("5")),
                _lp_pos(net_pnl_usd=Decimal("-30"), total_gas_usd=Decimal("3")),
            ]
        )
        out = render_lp_section(section)
        assert "Total gas:" in out
        assert "Total net PnL:" in out
        # Gas totals are rendered with leading minus (cost).
        assert "-$    8.00" in out


# ──────────────────────────────────────────────────────────────────────────────
# render_lending_section
# ──────────────────────────────────────────────────────────────────────────────


def _lend_pos(**overrides) -> LendingPositionSummary:
    base: dict = {
        "position_key": "lending:aave_v3:USDC:0xWALLET",
        "protocol": "aave_v3",
        "chain": "ethereum",
        "asset": "USDC",
        "market_id": "0xMARKETID",
        "collateral_usd": Decimal("5000"),
        "debt_usd": Decimal("2000"),
        "net_equity_usd": Decimal("3000"),
        "health_factor": Decimal("2.5"),
    }
    base.update(overrides)
    return LendingPositionSummary(**base)


class TestRenderLendingSection:
    def test_empty_section_returns_empty_string(self):
        assert render_lending_section(LendingSection()) == ""

    def test_open_position(self):
        out = render_lending_section(LendingSection(positions=[_lend_pos()]))
        assert "Lending Positions" in out
        assert "USDC" in out
        assert "[aave_v3 / ethereum]" in out
        assert "[OPEN]" in out
        # Debt is rendered as negative.
        assert "Debt:" in out
        assert "-$2,000.00" in out
        assert "Health:     2.500" in out

    def test_closed_position(self):
        out = render_lending_section(LendingSection(positions=[_lend_pos(is_closed=True)]))
        assert "[CLOSED]" in out

    def test_debt_none_renders_em_dash(self):
        out = render_lending_section(LendingSection(positions=[_lend_pos(debt_usd=None)]))
        # Debt line still present, but value is em-dash (no negative-of-None).
        assert "Debt:" in out
        assert "—" in out

    def test_optional_apr_fields_only_when_set(self):
        out_with = render_lending_section(
            LendingSection(positions=[_lend_pos(supply_apr_pct=Decimal("3.5"), borrow_apr_pct=Decimal("8.1"))])
        )
        assert "Supply APR: 3.50%" in out_with
        assert "Borrow APR: 8.10%" in out_with

        out_without = render_lending_section(LendingSection(positions=[_lend_pos()]))
        assert "Supply APR" not in out_without
        assert "Borrow APR" not in out_without

    def test_liquidation_threshold_converted_to_percent(self):
        # Stored as fraction (0.85), rendered as 85.0%.
        out = render_lending_section(LendingSection(positions=[_lend_pos(liquidation_threshold=Decimal("0.85"))]))
        assert "Liq. thr.:  85.0%" in out

    def test_interest_block_only_when_nonzero(self):
        out_zero = render_lending_section(LendingSection(positions=[_lend_pos()]))
        assert "Interest paid:" not in out_zero
        assert "Interest earned:" not in out_zero
        # VIB-4974: supply-side yield renders as a positive "Interest earned"
        # line (was the single "Realized interest:" line pre-fix).
        out_earned = render_lending_section(
            LendingSection(
                positions=[
                    _lend_pos(
                        total_interest_earned_usd=Decimal("12.34"),
                        total_interest_delta_usd=Decimal("12.34"),
                    )
                ]
            )
        )
        assert "Interest earned:" in out_earned
        assert "+$12.340000" in out_earned
        assert "Interest paid:" not in out_earned

    def test_debt_side_interest_renders_as_paid_negative(self):
        # VIB-4974: borrow interest paid is a COST — must render negative under
        # an "Interest paid" label, never a +gain.
        out = render_lending_section(
            LendingSection(
                positions=[
                    _lend_pos(
                        total_interest_paid_usd=Decimal("0.000237"),
                        total_interest_delta_usd=Decimal("-0.000237"),
                    )
                ]
            )
        )
        assert "Interest paid:" in out
        assert "-$0.000237" in out
        assert "Interest earned:" not in out

    def test_mixed_position_shows_both_gross_components_not_netted(self):
        # VIB-4974: a same-asset supply+borrow position sharing one key must
        # show BOTH gross legs plus a net line — never collapse the paid
        # borrow cost into a single netted figure.
        out = render_lending_section(
            LendingSection(
                positions=[
                    _lend_pos(
                        total_interest_paid_usd=Decimal("0.30"),
                        total_interest_earned_usd=Decimal("0.50"),
                        total_interest_delta_usd=Decimal("0.20"),
                    )
                ]
            )
        )
        assert "Interest paid:    -$0.300000" in out
        assert "Interest earned:  +$0.500000" in out
        assert "Net interest:     +$0.200000" in out

    def test_deleverage_count_only_when_nonzero(self):
        out_zero = render_lending_section(LendingSection(positions=[_lend_pos()]))
        assert "Deleverages:" not in out_zero
        out_some = render_lending_section(LendingSection(positions=[_lend_pos(deleverage_count=3)]))
        assert "Deleverages: 3" in out_some

    def test_position_key_truncated_to_16_chars(self):
        out = render_lending_section(LendingSection(positions=[_lend_pos(position_key="A" * 50)]))
        # 16 chars + ellipsis.
        assert "A" * 16 + "…" in out


# ──────────────────────────────────────────────────────────────────────────────
# render_pendle_section
# ──────────────────────────────────────────────────────────────────────────────


def _pendle_pos(**overrides) -> PendlePositionSummary:
    base: dict = {
        "position_key": "pendle:wstETH:0xMARKET:0xWALLET",
        "market_id": "0xMARKET00000000",
        "pt_token": "PT-wstETH",
        "protocol": "pendle",
        "chain": "ethereum",
    }
    base.update(overrides)
    return PendlePositionSummary(**base)


class TestRenderPendleSection:
    def test_empty_section_returns_empty_string(self):
        assert render_pendle_section(PendleSection()) == ""

    def test_active_position(self):
        out = render_pendle_section(PendleSection(positions=[_pendle_pos()]))
        assert "Pendle Positions" in out
        assert "PT-wstETH" in out
        assert "[ACTIVE]" in out

    def test_redeemed_position(self):
        out = render_pendle_section(PendleSection(positions=[_pendle_pos(is_redeemed=True)]))
        assert "[REDEEMED]" in out

    def test_maturity_date_when_set(self):
        ts = datetime(2026, 12, 31, 0, 0, tzinfo=UTC)
        out = render_pendle_section(PendleSection(positions=[_pendle_pos(maturity_timestamp=ts)]))
        assert "Maturity:      2026-12-31" in out

    def test_days_to_maturity_when_set(self):
        out = render_pendle_section(PendleSection(positions=[_pendle_pos(days_to_maturity=42)]))
        assert "Days to mat.:  42" in out

    def test_pt_amount_and_price_format(self):
        out = render_pendle_section(
            PendleSection(positions=[_pendle_pos(pt_amount=Decimal("1.234567"), pt_price=Decimal("0.987654"))])
        )
        assert "PT amount:     1.2346" in out
        assert "PT price:      0.9877 (underlying)" in out

    def test_apr_only_emits_latest_when_different_from_entry(self):
        # Same APR → only "APR at entry" line.
        out_same = render_pendle_section(
            PendleSection(
                positions=[_pendle_pos(implied_apr_pct_at_entry=Decimal("8"), implied_apr_pct_latest=Decimal("8"))]
            )
        )
        assert "APR at entry:" in out_same
        assert "APR latest:" not in out_same

        # Different APR → both lines.
        out_diff = render_pendle_section(
            PendleSection(
                positions=[_pendle_pos(implied_apr_pct_at_entry=Decimal("8"), implied_apr_pct_latest=Decimal("9"))]
            )
        )
        assert "APR at entry:" in out_diff
        assert "APR latest:" in out_diff

    def test_realized_yield_only_when_nonzero(self):
        out_zero = render_pendle_section(PendleSection(positions=[_pendle_pos()]))
        assert "Realized yld:" not in out_zero
        out_some = render_pendle_section(PendleSection(positions=[_pendle_pos(realized_yield_usd=Decimal("100"))]))
        assert "Realized yld:" in out_some

    def test_event_count_always_present(self):
        out = render_pendle_section(PendleSection(positions=[_pendle_pos(event_count=5)]))
        assert "Events:        5" in out


# ──────────────────────────────────────────────────────────────────────────────
# render_data_quality_section
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


class TestRenderDataQualitySection:
    def test_empty_section_returns_empty_string(self):
        assert render_data_quality_section(DataQualitySection()) == ""

    def test_section_with_only_parse_errors(self):
        out = render_data_quality_section(DataQualitySection(parse_errors=3))
        assert "Data Quality" in out
        assert "3 event(s) failed to parse" in out

    def test_issue_renders_event_metadata(self):
        out = render_data_quality_section(DataQualitySection(issues=[_issue()]))
        assert "1 record(s) with UNAVAILABLE confidence:" in out
        # Timestamp truncated to 19 chars (drops timezone).
        assert "[2026-05-04T12:00:00]" in out
        assert "LP_OPEN" in out
        assert "[uniswap_v3]" in out
        assert "Reason: stale oracle" in out

    def test_issue_without_timestamp_uses_question_mark(self):
        out = render_data_quality_section(DataQualitySection(issues=[_issue(timestamp="")]))
        assert "[?]" in out

    def test_issue_without_reason_omits_reason_line(self):
        out = render_data_quality_section(DataQualitySection(issues=[_issue(reason="")]))
        assert "Reason:" not in out

    def test_position_key_truncated_to_16_chars(self):
        out = render_data_quality_section(DataQualitySection(issues=[_issue(position_key="K" * 30)]))
        # The renderer uses [:16] without ellipsis for the data-quality position_key.
        assert "K" * 16 in out

    def test_both_issues_and_parse_errors(self):
        out = render_data_quality_section(DataQualitySection(issues=[_issue()], parse_errors=2))
        assert "1 record(s) with UNAVAILABLE confidence" in out
        assert "2 event(s) failed to parse" in out


# ──────────────────────────────────────────────────────────────────────────────
# Cross-cutting: every renderer is empty-section-safe
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "renderer,section",
    [
        (render_lp_section, LPSection()),
        (render_lending_section, LendingSection()),
        (render_pendle_section, PendleSection()),
        (render_data_quality_section, DataQualitySection()),
    ],
)
def test_empty_section_short_circuits_to_empty_string(renderer, section):
    assert renderer(section) == ""
