"""Unit tests for almanak.framework.dashboard.utils helpers."""

from decimal import Decimal

from almanak.framework.dashboard.utils import format_pnl_display, format_usd, pnl_color

# ---------------------------------------------------------------------------
# pnl_color
# ---------------------------------------------------------------------------


class TestPnlColor:
    def test_positive_value_returns_green(self) -> None:
        assert pnl_color(Decimal("1.23")) == "#00c853"

    def test_negative_value_returns_red(self) -> None:
        assert pnl_color(Decimal("-0.01")) == "#f44336"

    def test_zero_returns_grey(self) -> None:
        assert pnl_color(Decimal("0")) == "#9e9e9e"

    def test_zero_stale_returns_grey(self) -> None:
        assert pnl_color(Decimal("0"), is_stale=True) == "#9e9e9e"

    def test_positive_stale_returns_grey(self) -> None:
        # Even a positive value should be grey when stale — it means no data.
        assert pnl_color(Decimal("999"), is_stale=True) == "#9e9e9e"

    def test_negative_stale_returns_grey(self) -> None:
        assert pnl_color(Decimal("-5"), is_stale=True) == "#9e9e9e"


# ---------------------------------------------------------------------------
# format_pnl_display
# ---------------------------------------------------------------------------


class TestFormatPnlDisplay:
    def test_positive_formats_with_plus_sign(self) -> None:
        assert format_pnl_display(Decimal("12.34")) == "+$12.34"

    def test_negative_formats_with_minus_sign(self) -> None:
        assert format_pnl_display(Decimal("-7.50")) == "-$7.50"

    def test_zero_formats_as_plus_zero(self) -> None:
        assert format_pnl_display(Decimal("0")) == "+$0.00"

    def test_stale_returns_dash(self) -> None:
        assert format_pnl_display(Decimal("999"), is_stale=True) == "--"

    def test_stale_zero_returns_dash(self) -> None:
        assert format_pnl_display(Decimal("0"), is_stale=True) == "--"

    def test_stale_negative_returns_dash(self) -> None:
        assert format_pnl_display(Decimal("-1"), is_stale=True) == "--"

    def test_large_value_formatted_with_commas(self) -> None:
        assert format_pnl_display(Decimal("1234567.89")) == "+$1,234,567.89"


# ---------------------------------------------------------------------------
# format_usd — adaptive sub-cent precision (VIB-4980)
# ---------------------------------------------------------------------------


class TestFormatUsd:
    def test_default_two_dp_unchanged_positive(self) -> None:
        assert format_usd(Decimal("1234.5")) == "$1,234.50"

    def test_default_two_dp_unchanged_negative(self) -> None:
        assert format_usd(Decimal("-1234.5")) == "-$1,234.50"

    def test_default_subcent_rounds_to_zero(self) -> None:
        """Default path is unchanged: sub-cent collapses to $0.00."""
        assert format_usd(Decimal("0.0023")) == "$0.00"

    def test_zero_is_always_two_dp(self) -> None:
        assert format_usd(Decimal("0")) == "$0.00"
        assert format_usd(Decimal("0"), precise_small=True) == "$0.00"

    def test_precise_small_subcent_shows_real_value(self) -> None:
        """The VIB-4980 fix: a real $0.0023 fee no longer reads as $0.00."""
        assert format_usd(Decimal("0.0023"), precise_small=True) == "$0.0023"

    def test_precise_small_negative_subcent(self) -> None:
        assert format_usd(Decimal("-0.0023"), precise_small=True) == "-$0.0023"

    def test_precise_small_trims_trailing_zeros(self) -> None:
        assert format_usd(Decimal("0.0050"), precise_small=True) == "$0.005"

    def test_precise_small_at_one_cent_stays_two_dp(self) -> None:
        """>= $0.01 keeps the 2-dp form even with precise_small."""
        assert format_usd(Decimal("0.01"), precise_small=True) == "$0.01"

    def test_precise_small_just_under_one_cent(self) -> None:
        assert format_usd(Decimal("0.009999"), precise_small=True) == "$0.009999"

    def test_precise_small_below_six_dp_uses_scientific(self) -> None:
        """Magnitudes 6 dp cannot express fall back to scientific notation
        rather than rounding a real cost to $0."""
        assert format_usd(Decimal("0.00000034"), precise_small=True) == "$3.40e-7"

    def test_precise_small_large_value_unaffected(self) -> None:
        assert format_usd(Decimal("1234.5"), precise_small=True) == "$1,234.50"
