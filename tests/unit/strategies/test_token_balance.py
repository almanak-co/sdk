"""Tests for TokenBalance numeric comparison operators (VIB-128)."""

from decimal import Decimal

from almanak.framework.strategies.intent_strategy import TokenBalance


def _tb(balance: str = "100.5", balance_usd: str = "200.0") -> TokenBalance:
    return TokenBalance(symbol="ETH", balance=Decimal(balance), balance_usd=Decimal(balance_usd))


# -- comparison with Decimal ------------------------------------------------


class TestTokenBalanceDecimalComparison:
    def test_gt_decimal_true(self):
        assert _tb("100") > Decimal("50")

    def test_gt_decimal_false(self):
        assert not (_tb("50") > Decimal("100"))

    def test_lt_decimal_true(self):
        assert _tb("50") < Decimal("100")

    def test_lt_decimal_false(self):
        assert not (_tb("100") < Decimal("50"))

    def test_ge_decimal_equal(self):
        assert _tb("100") >= Decimal("100")

    def test_ge_decimal_greater(self):
        assert _tb("100") >= Decimal("50")

    def test_le_decimal_equal(self):
        assert _tb("100") <= Decimal("100")

    def test_le_decimal_less(self):
        assert _tb("50") <= Decimal("100")

    def test_eq_decimal_true(self):
        assert _tb("100") == Decimal("100")

    def test_eq_decimal_false(self):
        assert not (_tb("100") == Decimal("50"))


# -- comparison with int/float --------------------------------------------


class TestTokenBalanceNumericComparison:
    def test_gt_int(self):
        assert _tb("100") > 50

    def test_lt_float(self):
        assert _tb("50") < 100.5

    def test_ge_int(self):
        assert _tb("100") >= 100

    def test_le_float(self):
        assert _tb("50.5") <= 50.5

    def test_eq_int(self):
        assert _tb("100") == 100


# -- comparison between TokenBalance instances ----------------------------


class TestTokenBalanceCrossComparison:
    def test_gt_tokenbalance(self):
        assert _tb("200") > _tb("100")

    def test_lt_tokenbalance(self):
        assert _tb("50") < _tb("100")

    def test_eq_tokenbalance(self):
        assert _tb("100") == _tb("100")

    def test_different_tokens_same_balance_not_equal(self):
        """Two different tokens with the same balance must NOT be equal."""
        usdc = TokenBalance(symbol="USDC", balance=Decimal("100"), balance_usd=Decimal("100"), address="0xA")
        eth = TokenBalance(symbol="ETH", balance=Decimal("100"), balance_usd=Decimal("300"), address="0xB")
        assert usdc != eth

    def test_different_tokens_same_balance_distinct_in_set(self):
        """Different tokens must not collapse in sets even if balance matches."""
        usdc = TokenBalance(symbol="USDC", balance=Decimal("100"), balance_usd=Decimal("100"), address="0xA")
        eth = TokenBalance(symbol="ETH", balance=Decimal("100"), balance_usd=Decimal("300"), address="0xB")
        assert len({usdc, eth}) == 2


# -- min/max builtins ----------------------------------------------------


class TestTokenBalanceMinMax:
    def test_min_with_decimal(self):
        tb = _tb("100")
        result = min(tb, Decimal("50"))
        assert result == Decimal("50")

    def test_min_tokenbalance_wins(self):
        tb = _tb("30")
        result = min(tb, Decimal("50"))
        assert result is tb

    def test_max_with_decimal(self):
        tb = _tb("100")
        result = max(tb, Decimal("50"))
        assert result is tb

    def test_min_two_tokenbalances(self):
        a, b = _tb("30"), _tb("70")
        assert min(a, b) is a


# -- float / int / format ------------------------------------------------


class TestTokenBalanceConversions:
    def test_float(self):
        assert float(_tb("3.14")) == 3.14

    def test_int(self):
        assert int(_tb("42.9")) == 42

    def test_format_f(self):
        assert f"{_tb('3.14159'):.2f}" == "3.14"

    def test_format_empty(self):
        assert format(_tb("100")) == "100"

    def test_repr(self):
        r = repr(_tb("100", "200"))
        assert "TokenBalance" in r
        assert "ETH" in r


# -- hash -----------------------------------------------------------------


class TestTokenBalanceHash:
    def test_hashable(self):
        tb = _tb("100")
        assert hash(tb) == hash(tb)

    def test_usable_in_set(self):
        s = {_tb("100"), _tb("100")}
        assert len(s) == 1


# -- unsupported types return NotImplemented ----------------------------


class TestTokenBalanceNotImplemented:
    def test_eq_string_returns_false(self):
        # NotImplemented falls through, Python returns False for ==
        assert not (_tb("100") == "not a number")

    def test_gt_string_raises(self):
        import pytest

        with pytest.raises(TypeError):
            _tb("100") > "string"  # type: ignore[operator]
