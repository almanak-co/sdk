"""Unit tests for protocol-neutral pool identity and capability contracts."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, cast

import pytest

from almanak.connectors._base.types import ProtocolName
from almanak.connectors._strategy_base.pool_data import (
    PoolAsset,
    PoolDataFacet,
    PoolDataSource,
    PoolDataSpec,
    PoolMetadata,
    PoolPriceObservation,
    PoolRef,
    PoolReferenceKind,
    PoolStateObservation,
    unsupported_pool_data_spec,
)
from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM


def _unsupported_except(*supported: PoolDataFacet) -> dict[PoolDataFacet, str]:
    return {facet: "not implemented" for facet in PoolDataFacet if facet not in supported}


def test_pool_ref_normalizes_typed_identity() -> None:
    ref = PoolRef(
        chain=ETHEREUM,
        protocol=ProtocolName("Uniswap-V3"),
        kind=PoolReferenceKind.EVM_CONTRACT,
        value="0x" + "AB" * 20,
    )

    assert ref.protocol == "uniswap_v3"
    assert ref.value == "0x" + "ab" * 20
    assert ref.key == ("ethereum", "uniswap_v3", "evm_contract", "0x" + "ab" * 20)


@pytest.mark.parametrize(
    ("kind", "value"),
    [
        (PoolReferenceKind.EVM_CONTRACT, "0x1234"),
        (PoolReferenceKind.EVM_POOL_ID, "0x" + "0" * 40),
        (PoolReferenceKind.SOLANA_ACCOUNT, "contains-0-or-O"),
    ],
)
def test_pool_ref_rejects_identifier_shape_mismatches(kind: PoolReferenceKind, value: str) -> None:
    with pytest.raises(ValueError, match="invalid"):
        PoolRef(chain=ETHEREUM, protocol=ProtocolName("test"), kind=kind, value=value)


def test_pool_data_spec_requires_an_explicit_decision_for_every_facet() -> None:
    with pytest.raises(ValueError, match="classify every facet"):
        PoolDataSpec(
            protocol="partial",
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={},
            unsupported={},
        )


def test_pool_data_spec_binds_each_supported_facet_to_an_executable_lane() -> None:
    spec = PoolDataSpec(
        protocol="twap_only",
        reference_kind=PoolReferenceKind.EVM_CONTRACT,
        bindings={PoolDataFacet.TWAP: PoolDataSource.GATEWAY_TWAP},
        unsupported=_unsupported_except(PoolDataFacet.TWAP),
    )

    assert spec.supported == frozenset({PoolDataFacet.TWAP})
    assert spec.source_for(PoolDataFacet.TWAP) is PoolDataSource.GATEWAY_TWAP


def test_live_pool_data_binding_requires_a_bound_reader() -> None:
    with pytest.raises(ValueError, match="require price_reader"):
        PoolDataSpec(
            protocol="unbound_live",
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={PoolDataFacet.SPOT_PRICE: PoolDataSource.LIVE_PRICE_READER},
            unsupported=_unsupported_except(PoolDataFacet.SPOT_PRICE),
        )


def test_pool_data_spec_normalizes_protocol_identity() -> None:
    spec = PoolDataSpec(
        protocol="Example-Pool",
        aliases=(" Example-Alias ",),
        reference_kind=PoolReferenceKind.EVM_CONTRACT,
        bindings={},
        unsupported=_unsupported_except(),
    )

    assert spec.keys == ("example_pool", "example_alias")


@pytest.mark.parametrize(
    ("protocol", "aliases", "message"),
    [
        ("", (), "non-empty"),
        ("pool", (" ",), "non-empty"),
        ("pool", ("pool",), "unique"),
        ("pool", ("alias", "alias"), "unique"),
    ],
)
def test_pool_data_spec_rejects_invalid_identity(
    protocol: str,
    aliases: tuple[str, ...],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        PoolDataSpec(
            protocol=protocol,
            aliases=aliases,
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={},
            unsupported=_unsupported_except(),
        )


def test_pool_data_spec_requires_typed_reference_kind() -> None:
    with pytest.raises(TypeError, match="reference_kind"):
        PoolDataSpec(
            protocol="pool",
            reference_kind=cast(PoolReferenceKind, "evm_contract"),
            bindings={},
            unsupported=_unsupported_except(),
        )


def test_pool_data_spec_requires_typed_facets_and_sources() -> None:
    invalid_facet = cast(PoolDataFacet, "metadata")
    with pytest.raises(TypeError, match="facets"):
        PoolDataSpec(
            protocol="pool",
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={invalid_facet: PoolDataSource.GATEWAY_POOL_STATE},
            unsupported=_unsupported_except(),
        )

    invalid_source = cast(PoolDataSource, "gateway_pool_state")
    with pytest.raises(TypeError, match="bindings"):
        PoolDataSpec(
            protocol="pool",
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={PoolDataFacet.METADATA: invalid_source},
            unsupported=_unsupported_except(PoolDataFacet.METADATA),
        )


def test_pool_data_spec_rejects_overlapping_or_unexplained_facets() -> None:
    with pytest.raises(ValueError, match="both supported and unsupported"):
        PoolDataSpec(
            protocol="pool",
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={PoolDataFacet.TWAP: PoolDataSource.GATEWAY_TWAP},
            unsupported=_unsupported_except(),
        )

    unsupported = _unsupported_except()
    unsupported[PoolDataFacet.TWAP] = " "
    with pytest.raises(ValueError, match="non-empty reasons"):
        PoolDataSpec(
            protocol="pool",
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={},
            unsupported=unsupported,
        )


def test_pool_data_spec_requires_matching_price_reader_identity_and_spot_binding() -> None:
    mismatched_reader = PoolReaderSpec(protocol="other", factory_addresses={})
    with pytest.raises(ValueError, match="identity must match"):
        PoolDataSpec(
            protocol="pool",
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={PoolDataFacet.SPOT_PRICE: PoolDataSource.LIVE_PRICE_READER},
            unsupported=_unsupported_except(PoolDataFacet.SPOT_PRICE),
            price_reader=mismatched_reader,
        )

    matching_reader = PoolReaderSpec(protocol="pool", factory_addresses={})
    with pytest.raises(ValueError, match="must bind SPOT_PRICE"):
        PoolDataSpec(
            protocol="pool",
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={PoolDataFacet.LIQUIDITY: PoolDataSource.LIVE_PRICE_READER},
            unsupported=_unsupported_except(PoolDataFacet.LIQUIDITY),
            price_reader=matching_reader,
        )


def test_pool_data_spec_rejects_incompatible_facet_source_binding() -> None:
    with pytest.raises(ValueError, match="invalid facet/source bindings"):
        PoolDataSpec(
            protocol="pool",
            reference_kind=PoolReferenceKind.EVM_CONTRACT,
            bindings={PoolDataFacet.TWAP: PoolDataSource.GATEWAY_POOL_STATE},
            unsupported=_unsupported_except(PoolDataFacet.TWAP),
        )


def test_unsupported_spec_is_inventoried_without_claiming_implementation() -> None:
    spec = unsupported_pool_data_spec(
        protocol="future_pool",
        reference_kind=PoolReferenceKind.SOLANA_ACCOUNT,
        reason="adapter pending",
    )

    assert spec.supported == frozenset()
    assert set(spec.unsupported) == set(PoolDataFacet)
    assert {spec.unsupported_reason(facet) for facet in PoolDataFacet} == {"adapter pending"}


def test_generic_state_supports_n_assets_without_v3_fields() -> None:
    ref = PoolRef(
        chain=ETHEREUM,
        protocol=ProtocolName("curve"),
        kind=PoolReferenceKind.EVM_CONTRACT,
        value="0x" + "ab" * 20,
    )
    metadata = PoolMetadata(
        ref=ref,
        assets=(
            PoolAsset(identifier="0x" + "01" * 20, decimals=18, index=0),
            PoolAsset(identifier="0x" + "02" * 20, decimals=6, index=1),
            PoolAsset(identifier="0x" + "03" * 20, decimals=8, index=2),
        ),
        provenance="curve-registry",
        fee_rate=Decimal("0.0004"),
    )

    observation = PoolStateObservation(
        metadata=metadata,
        timestamp=1_700_000_000,
        block_number=18_000_000,
        balances_raw=(10**18, 10**6, None),
        state={"amplification": "200"},
    )

    assert len(observation.balances_raw) == 3
    assert observation.state["amplification"] == "200"


def test_pool_price_observation_accepts_an_oriented_block_anchored_price() -> None:
    ref = PoolRef(
        chain=ETHEREUM,
        protocol=ProtocolName("curve"),
        kind=PoolReferenceKind.EVM_CONTRACT,
        value="0x" + "ab" * 20,
    )

    observation = PoolPriceObservation(
        ref=ref,
        base_asset="0x" + "01" * 20,
        quote_asset="0x" + "02" * 20,
        price=Decimal("1.01"),
        timestamp=1_700_000_000,
        block_number=18_000_000,
        provenance="curve-oracle",
    )

    assert observation.price == Decimal("1.01")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"base_asset": ""}, "requires base and quote"),
        ({"quote_asset": " "}, "requires base and quote"),
        ({"quote_asset": "asset-a"}, "must differ"),
        ({"price": Decimal("0")}, "must be positive"),
        ({"timestamp": 0}, "must be positive"),
        ({"block_number": 0}, "must be positive"),
        ({"provenance": " "}, "provenance is required"),
    ],
)
def test_pool_price_observation_rejects_invalid_measurements(
    overrides: dict[str, Any],
    message: str,
) -> None:
    values: dict[str, Any] = {
        "ref": PoolRef(
            chain=ETHEREUM,
            protocol=ProtocolName("curve"),
            kind=PoolReferenceKind.EVM_CONTRACT,
            value="0x" + "ab" * 20,
        ),
        "base_asset": "asset-a",
        "quote_asset": "asset-b",
        "price": Decimal("1"),
        "timestamp": 1_700_000_000,
        "block_number": 18_000_000,
        "provenance": "test",
    }
    values.update(overrides)

    with pytest.raises(ValueError, match=message):
        PoolPriceObservation(**values)
