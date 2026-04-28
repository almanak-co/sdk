"""Unit tests for almanak.framework.dashboard.utils helpers."""

from decimal import Decimal

from almanak.framework.dashboard.utils import format_pnl_display, pnl_color

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
