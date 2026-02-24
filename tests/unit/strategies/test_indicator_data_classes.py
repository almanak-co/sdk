"""Tests for indicator data class numeric operations (VIB-153, VIB-155).

Verifies that RSIData, BollingerBandsData, and ATRData support f-string formatting,
float() conversion, and numeric comparisons.
"""

from decimal import Decimal

import pytest

from almanak.framework.strategies.intent_strategy import ATRData, BollingerBandsData, RSIData


# ---------------------------------------------------------------------------
# RSIData
# ---------------------------------------------------------------------------


class TestRSIDataNumericOps:
    """RSIData supports f-string formatting, float(), and comparisons."""

    @pytest.fixture()
    def rsi(self):
        return RSIData(value=Decimal("65.42"))

    def test_float_conversion(self, rsi):
        assert float(rsi) == pytest.approx(65.42)

    def test_format_two_decimal(self, rsi):
        assert f"{rsi:.2f}" == "65.42"

    def test_format_zero_decimal(self, rsi):
        assert f"{rsi:.0f}" == "65"

    def test_format_no_spec(self, rsi):
        """Empty format spec returns str(value)."""
        assert f"{rsi}" == "65.42"

    def test_gt_int(self, rsi):
        assert rsi > 60
        assert not (rsi > 70)

    def test_lt_int(self, rsi):
        assert rsi < 70
        assert not (rsi < 60)

    def test_ge_float(self, rsi):
        assert rsi >= 65.42
        assert rsi >= 65.0
        assert not (rsi >= 66.0)

    def test_le_decimal(self, rsi):
        assert rsi <= Decimal("65.42")
        assert rsi <= Decimal("70")
        assert not (rsi <= Decimal("60"))

    def test_eq_same_value(self, rsi):
        assert rsi == Decimal("65.42")
        assert rsi == 65.42

    def test_eq_other_rsi(self):
        a = RSIData(value=Decimal("50"))
        b = RSIData(value=Decimal("50"))
        assert a == b

    def test_neq(self, rsi):
        assert rsi != 99

    def test_comparison_returns_not_implemented_for_string(self, rsi):
        with pytest.raises(TypeError):
            rsi > "70"  # noqa: B015

    def test_signal_property(self):
        assert RSIData(value=Decimal("25")).signal == "BUY"
        assert RSIData(value=Decimal("75")).signal == "SELL"
        assert RSIData(value=Decimal("50")).signal == "HOLD"

    def test_hashable(self):
        """RSIData must be hashable for use in sets and as dict keys."""
        rsi = RSIData(value=Decimal("50"))
        assert hash(rsi) == hash(rsi)

    def test_usable_in_set(self):
        a = RSIData(value=Decimal("50"))
        b = RSIData(value=Decimal("50"))
        assert len({a, b}) == 1

    def test_different_values_distinct_in_set(self):
        a = RSIData(value=Decimal("30"))
        b = RSIData(value=Decimal("70"))
        assert len({a, b}) == 2


# ---------------------------------------------------------------------------
# BollingerBandsData
# ---------------------------------------------------------------------------


class TestBollingerBandsDataNumericOps:
    """BollingerBandsData supports f-string formatting and float() via percent_b."""

    @pytest.fixture()
    def bb(self):
        return BollingerBandsData(
            upper_band=Decimal("3500"),
            middle_band=Decimal("3400"),
            lower_band=Decimal("3300"),
            percent_b=Decimal("0.75"),
        )

    def test_float_conversion(self, bb):
        assert float(bb) == pytest.approx(0.75)

    def test_format_two_decimal(self, bb):
        assert f"{bb:.2f}" == "0.75"

    def test_format_percent(self, bb):
        assert f"{bb:.0%}" == "75%"


# ---------------------------------------------------------------------------
# ATRData
# ---------------------------------------------------------------------------


class TestATRDataNumericOps:
    """ATRData supports f-string formatting and float() via value."""

    @pytest.fixture()
    def atr(self):
        return ATRData(value=Decimal("123.45"), value_percent=Decimal("3.6"))

    def test_float_conversion(self, atr):
        assert float(atr) == pytest.approx(123.45)

    def test_format_two_decimal(self, atr):
        assert f"{atr:.2f}" == "123.45"

    def test_format_no_spec(self, atr):
        assert f"{atr}" == "123.45"
