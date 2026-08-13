"""Validation contract for authenticated immutable pool descriptors."""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.data.pools.descriptor import PoolDescriptor

POOL = "0x0000000000000000000000000000000000000001"
TOKEN0 = "0x0000000000000000000000000000000000000002"
TOKEN1 = "0x0000000000000000000000000000000000000003"
FACTORY = "0x0000000000000000000000000000000000000004"


def _descriptor(**overrides: object) -> PoolDescriptor:
    values: dict[str, object] = {
        "chain": "ethereum",
        "protocol": "uniswap_v3",
        "address": POOL,
        "token0": TOKEN0,
        "token1": TOKEN1,
        "token0_decimals": 18,
        "token1_decimals": 6,
        "fee_tier_units": 500,
        "provenance": "historical:on_chain_archive",
        "factory": FACTORY,
    }
    values.update(overrides)
    return PoolDescriptor(**values)  # type: ignore[arg-type]


def test_descriptor_normalizes_complete_identity_and_provenance() -> None:
    descriptor = _descriptor(
        chain=" Ethereum ",
        protocol=" Uniswap-V3 ",
        address=f" {POOL.upper()} ",
        token0=f" {TOKEN0.upper()} ",
        token1=f" {TOKEN1.upper()} ",
        factory=f" {FACTORY.upper()} ",
        provenance=" historical:test ",
    )

    assert descriptor.key == ("ethereum", "uniswap_v3", POOL)
    assert (descriptor.token0, descriptor.token1) == (TOKEN0, TOKEN1)
    assert descriptor.factory == FACTORY
    assert descriptor.provenance == "historical:test"
    assert descriptor.fee_rate == Decimal("0.0005")


@pytest.mark.parametrize("field", ["chain", "protocol", "provenance"])
def test_descriptor_rejects_blank_required_text(field: str) -> None:
    with pytest.raises(ValueError, match="requires chain, protocol, and provenance"):
        _descriptor(**{field: "   "})


@pytest.mark.parametrize("field", ["address", "token0", "token1"])
def test_descriptor_rejects_malformed_pool_or_token_address(field: str) -> None:
    with pytest.raises(ValueError, match="valid pool address"):
        _descriptor(**{field: "0x1234"})


def test_descriptor_rejects_duplicate_tokens() -> None:
    with pytest.raises(ValueError, match="two distinct token addresses"):
        _descriptor(token1=TOKEN0)


@pytest.mark.parametrize("factory", ["", "0x1234", 123])
def test_descriptor_rejects_invalid_factory(factory: object) -> None:
    with pytest.raises(ValueError, match="factory"):
        _descriptor(factory=factory)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("token0_decimals", -1),
        ("token0_decimals", 37),
        ("token1_decimals", -1),
        ("token1_decimals", 37),
    ],
)
def test_descriptor_rejects_out_of_range_decimals(field: str, value: int) -> None:
    with pytest.raises(ValueError, match="interval"):
        _descriptor(**{field: value})


@pytest.mark.parametrize("fee_tier_units", [0, 1_000_000, Decimal("500"), True])
def test_descriptor_rejects_invalid_fee_units(fee_tier_units: object) -> None:
    with pytest.raises(ValueError, match="fee_tier_units"):
        _descriptor(fee_tier_units=fee_tier_units)
