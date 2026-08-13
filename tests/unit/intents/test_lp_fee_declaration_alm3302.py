"""ALM-3302: LP fee identity and economic-rate units cannot be confused."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.backtesting.pnl.intent_extraction import lp_pool_fee_units
from almanak.framework.intents.lp_fees import lp_fee_declaration_from_intent, pool_fee_tier_units
from almanak.framework.intents.vocabulary import LPOpenIntent


def _intent(**overrides):
    values = {
        "pool": "WETH/USDC/500",
        "amount0": Decimal("1"),
        "amount1": Decimal("1"),
        "range_lower": Decimal("1"),
        "range_upper": Decimal("2"),
        "protocol": "uniswap_v3",
    }
    values.update(overrides)
    return LPOpenIntent(**values)


def test_typed_factory_units_have_one_exact_economic_rate() -> None:
    intent = _intent(fee_tier_units=500)

    declaration = lp_fee_declaration_from_intent(intent)

    assert declaration.fee_tier_units == 500
    assert declaration.economic_rate == Decimal("0.0005")


def test_typed_fee_declaration_survives_serialization_round_trip() -> None:
    intent = _intent(fee_tier_units=500)

    restored = LPOpenIntent.deserialize(intent.serialize())

    assert restored.fee_tier_units == 500
    assert restored.fee_rate is None
    assert lp_fee_declaration_from_intent(restored).economic_rate == Decimal("0.0005")


def test_unused_fee_fields_do_not_change_legacy_wire_shape() -> None:
    serialized = _intent().serialize()

    assert "fee_tier_units" not in serialized
    assert "fee_rate" not in serialized


def test_legacy_fee_tier_is_factory_units_and_migrates_to_typed_field() -> None:
    with pytest.warns(DeprecationWarning, match="fee_tier_units"):
        intent = _intent(protocol_params={"fee_tier": 500})

    assert intent.fee_tier_units == 500
    assert lp_fee_declaration_from_intent(intent).economic_rate == Decimal("0.0005")


@pytest.mark.parametrize(
    "value",
    [Decimal("0.0005"), Decimal("500"), 500.0, "500", "NaN", "Infinity", 0, -1, 1_000_000, True],
)
def test_invalid_or_fractional_factory_units_fail_validation(value) -> None:
    with pytest.raises(ValidationError, match="fee_tier"):
        _intent(fee_tier_units=value)


def test_legacy_aliases_compare_after_raw_unit_parsing() -> None:
    with pytest.warns(DeprecationWarning, match="fee_tier_units"):
        intent = _intent(protocol_params={"fee_tier": "500", "feeTier": 500})

    assert intent.fee_tier_units == 500


def test_dual_conflicting_declarations_fail_validation() -> None:
    with pytest.raises(ValidationError, match="conflicts"):
        _intent(fee_tier_units=500, protocol_params={"fee_tier": 3000})


def test_factory_units_and_fractional_rate_are_mutually_exclusive() -> None:
    with pytest.raises(ValidationError, match="mutually exclusive"):
        _intent(fee_tier_units=500, fee_rate=Decimal("0.0005"))


def test_model_construct_cannot_bypass_runtime_fee_validation() -> None:
    raw = LPOpenIntent.model_construct(
        pool="WETH/USDC/500",
        amount0=Decimal("1"),
        amount1=Decimal("1"),
        range_lower=Decimal("1"),
        range_upper=Decimal("2"),
        protocol="uniswap_v3",
        fee_tier_units=Decimal("0.0005"),
        fee_rate=None,
        protocol_params=None,
    )

    with pytest.raises(ValueError, match="integer in raw factory units"):
        lp_fee_declaration_from_intent(raw)


@pytest.mark.parametrize(
    ("pool", "expected"),
    [
        ("WETH/USDC/500", 500),
        (" WETH / USDC / 500.0 ", 500),
        ("WETH/USDC/5e2", 500),
        ("WETH/USDC/500.5", None),
        ("WETH/USDC/not-a-tier", None),
    ],
)
def test_pool_fee_parser_is_shared_with_backtesting(pool: str, expected: int | None) -> None:
    assert pool_fee_tier_units(pool) == expected
    assert lp_pool_fee_units(pool) == expected
