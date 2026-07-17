"""Contract tests for the typed money/identity primitives (ALM-2943 phase 1).

These pin the two structural rules the types exist to enforce:
one identity per economic token, and no USD<->units mixing without a price.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.money import (
    PriceQuote,
    TokenIdentity,
    TokenUnits,
    UsdAmount,
    ValueKindError,
    as_decimal,
)
from almanak.framework.market.errors import PriceUnavailableError

WETH_BASE = "0x4200000000000000000000000000000000000006"
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def _weth() -> TokenIdentity:
    return TokenIdentity(chain="base", address=WETH_BASE, symbol="WETH")


def _usdc() -> TokenIdentity:
    return TokenIdentity(chain="base", address=USDC_BASE, symbol="USDC")


def _quote(token: TokenIdentity, price: str) -> PriceQuote:
    return PriceQuote(
        token=token, usd_per_unit=Decimal(price), source="test", timestamp=datetime(2024, 1, 1, tzinfo=UTC)
    )


class TestAsDecimal:
    def test_decimal_int_str_accepted(self) -> None:
        assert as_decimal(Decimal("1.5"), what="x") == Decimal("1.5")
        assert as_decimal(7, what="x") == Decimal(7)
        assert as_decimal("2.25", what="x") == Decimal("2.25")

    def test_float_rejected(self) -> None:
        with pytest.raises(ValueKindError, match="floats are rejected"):
            as_decimal(1.5, what="x")

    def test_bool_rejected(self) -> None:
        # bool passes isinstance(int) checks; it must never be money.
        with pytest.raises(ValueKindError, match="bool"):
            as_decimal(True, what="x")


class TestTokenIdentity:
    def test_symbol_and_address_forms_of_same_asset_are_one_key(self) -> None:
        # The 2960 split-brain: a symbol-shaped credit landing beside an
        # address-shaped funding balance. With identity typed, both references
        # ARE the same key regardless of display symbol casing.
        a = TokenIdentity(chain="Base", address=WETH_BASE.upper().replace("0X", "0x"), symbol="weth")
        b = TokenIdentity(chain="base", address=WETH_BASE, symbol="WETH")
        assert a == b
        assert hash(a) == hash(b)
        assert len({a, b}) == 1

    def test_display_symbol_never_splits_an_addressed_identity(self) -> None:
        mislabeled = TokenIdentity(chain="base", address=WETH_BASE, symbol="WETH9")
        assert mislabeled == _weth()

    def test_unresolved_symbol_still_gets_exactly_one_identity(self) -> None:
        a = TokenIdentity(chain="base", address=None, symbol="weth")
        b = TokenIdentity(chain="base", address=None, symbol="WETH")
        assert a == b
        assert a.key == ("base", "WETH")

    def test_unresolved_and_resolved_are_distinct_keys(self) -> None:
        # An unresolved symbol is NOT silently the addressed asset — merging
        # them is the resolver's job (phase-1 PR3), never equality's.
        assert TokenIdentity(chain="base", address=None, symbol="WETH") != _weth()

    def test_same_address_different_chain_is_a_different_asset(self) -> None:
        assert TokenIdentity(chain="optimism", address=WETH_BASE, symbol="WETH") != _weth()

    def test_malformed_address_rejected(self) -> None:
        with pytest.raises(ValueError, match="0x"):
            TokenIdentity(chain="base", address="4200", symbol="WETH")

    def test_empty_fields_rejected(self) -> None:
        with pytest.raises(ValueError):
            TokenIdentity(chain="", address=None, symbol="WETH")
        with pytest.raises(ValueError):
            TokenIdentity(chain="base", address=None, symbol="  ")


class TestPriceQuote:
    def test_zero_price_is_not_a_quote(self) -> None:
        # Empty != Zero: the optimism native-USDC defect (deployment prices a
        # real token at $0) must be representable only as ABSENCE.
        with pytest.raises(ValueError, match="data defect"):
            PriceQuote(token=_weth(), usd_per_unit=Decimal("0"), source="subgraph")

    def test_negative_price_rejected(self) -> None:
        with pytest.raises(ValueError):
            PriceQuote(token=_weth(), usd_per_unit=Decimal("-1"), source="subgraph")

    def test_empty_source_rejected(self) -> None:
        with pytest.raises(ValueError, match="source"):
            PriceQuote(token=_weth(), usd_per_unit=Decimal("2500"), source="")


class TestValueKindSeparation:
    def test_usd_plus_units_is_a_type_error(self) -> None:
        with pytest.raises(TypeError):
            UsdAmount(Decimal("100")) + TokenUnits(token=_weth(), units=Decimal("1"))  # type: ignore[operator]

    def test_units_plus_usd_is_a_type_error(self) -> None:
        with pytest.raises(TypeError):
            TokenUnits(token=_weth(), units=Decimal("1")) + UsdAmount(Decimal("100"))  # type: ignore[operator]

    def test_cross_token_arithmetic_refused(self) -> None:
        with pytest.raises(ValueKindError, match="convert via to_usd"):
            TokenUnits(token=_weth(), units=Decimal("1")) + TokenUnits(token=_usdc(), units=Decimal("5"))

    def test_usd_scalar_math_is_exact(self) -> None:
        total = (UsdAmount(Decimal("0.1")) + UsdAmount(Decimal("0.2"))) * 3
        assert total.value == Decimal("0.9")

    def test_same_token_units_math(self) -> None:
        held = TokenUnits(token=_weth(), units=Decimal("0.5")) + TokenUnits(token=_weth(), units=Decimal("0.25"))
        assert held.units == Decimal("0.75")
        assert held.token == _weth()


class TestConversionOnlyThroughQuotes:
    def test_units_to_usd(self) -> None:
        assert TokenUnits(token=_weth(), units=Decimal("0.5")).to_usd(_quote(_weth(), "2500")) == UsdAmount(
            Decimal("1250.0")
        )

    def test_usd_to_units(self) -> None:
        assert UsdAmount(Decimal("100")).to_units(_quote(_weth(), "2500")) == TokenUnits(
            token=_weth(), units=Decimal("0.04")
        )

    def test_missing_quote_raises_never_guesses(self) -> None:
        # The round-2 P0 class: "price unknown -> treat USD as units" minted
        # value. The typed path has no such branch to take.
        with pytest.raises(PriceUnavailableError):
            TokenUnits(token=_weth(), units=Decimal("1")).to_usd(None)
        with pytest.raises(PriceUnavailableError):
            UsdAmount(Decimal("100")).to_units(None)

    def test_mismatched_quote_token_raises(self) -> None:
        with pytest.raises(PriceUnavailableError, match="cross-token"):
            TokenUnits(token=_weth(), units=Decimal("1")).to_usd(_quote(_usdc(), "1"))

    def test_round_trip_conserves_at_off_par_price(self) -> None:
        # Sell-all conservation at $0.90/$1.10 is the r3_p0 battery probe;
        # this is its type-level kernel: converting through the SAME quote in
        # both directions is exact at any price.
        for price in ("0.90", "1.10"):
            quote = _quote(_usdc(), price)
            units = TokenUnits(token=_usdc(), units=Decimal("121.1"))
            assert units.to_usd(quote).to_units(quote) == units


class TestReviewRound3314:
    def test_non_finite_decimals_rejected(self) -> None:
        for bad in ("NaN", "Infinity", "-Infinity"):
            with pytest.raises(ValueKindError, match="finite"):
                as_decimal(Decimal(bad), what="x")
        with pytest.raises(ValueKindError, match="finite"):
            PriceQuote(token=_weth(), usd_per_unit=Decimal("NaN"), source="test")

    def test_non_hex_address_rejected(self) -> None:
        with pytest.raises(ValueError, match="0x"):
            TokenIdentity(chain="base", address="0x" + "g" * 40, symbol="WETH")
