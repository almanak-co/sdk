"""Inventory and parity guards for connector-owned pool-data declarations."""

from __future__ import annotations

from collections.abc import Iterable

from almanak.connectors._base.gateway_capabilities import (
    GatewayDexPoolStateCapability,
    GatewayDexTwapCapability,
)
from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import CONNECTOR_REGISTRY, Connector
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
from almanak.connectors._strategy_base.pool_data import PoolDataFacet, PoolDataSource, PoolDataSpec
from almanak.connectors._strategy_pool_data_registry import POOL_DATA_REGISTRY
from almanak.core.intent_types import IntentType
from almanak.framework.data.pools.reader import PoolPriceReader

_POOL_FAMILY_CONNECTORS = frozenset(
    {
        "aerodrome",
        "balancer_v2",
        "camelot",
        "curve",
        "fluid",
        "fluid_dex_lp",
        "meteora",
        "orca",
        "pancakeswap_v3",
        "pendle",
        "raydium",
        "sushiswap_v3",
        "traderjoe_v2",
        "uniswap_v3",
        "uniswap_v4",
    }
)


def _is_pool_bearing(connector: Connector) -> bool:
    intents = connector.strategy_intents or ()
    return (
        connector.kind is ProtocolKind.LP
        or connector.dex_volume is not None
        or IntentType.LP_OPEN in intents
        or IntentType.LP_CLOSE in intents
        or IntentType.LP_COLLECT_FEES in intents
    )


def _load_specs(connector: Connector) -> tuple[PoolDataSpec, ...]:
    assert connector.pool_data is not None
    value = connector.pool_data.load()
    if isinstance(value, PoolDataSpec):
        return (value,)
    assert isinstance(value, tuple)
    assert value
    assert all(isinstance(spec, PoolDataSpec) for spec in value)
    return value


def _gateway_by_key(providers: Iterable[object]) -> dict[str, object]:
    keyed: dict[str, object] = {}
    for provider in providers:
        keyed[provider.dex_name()] = provider  # type: ignore[attr-defined]
        aliases = getattr(provider, "dex_aliases", None)
        if callable(aliases):
            keyed.update({alias: provider for alias in aliases()})
    return keyed


def test_every_pool_bearing_connector_publishes_pool_data_contract() -> None:
    inferred = {connector.name for connector in CONNECTOR_REGISTRY.all() if _is_pool_bearing(connector)}
    assert inferred <= _POOL_FAMILY_CONNECTORS
    published = {connector.name for connector in CONNECTOR_REGISTRY.with_pool_data()}
    assert published == _POOL_FAMILY_CONNECTORS


def test_every_pool_data_manifest_is_registered_and_classifies_every_facet() -> None:
    for connector in CONNECTOR_REGISTRY.with_pool_data():
        specs = _load_specs(connector)
        assert connector.name in {key for spec in specs for key in spec.keys}
        for spec in specs:
            assert POOL_DATA_REGISTRY.require(spec.protocol) is spec
            assert set(spec.bindings) | spec.unsupported.keys() == set(PoolDataFacet)
            assert not (spec.bindings.keys() & spec.unsupported.keys())


def test_every_bound_price_reader_resolves_from_the_connector_spec() -> None:
    for spec in POOL_DATA_REGISTRY.all():
        live_facets = {facet for facet, source in spec.bindings.items() if source is PoolDataSource.LIVE_PRICE_READER}
        if not live_facets:
            assert spec.price_reader is None
            continue
        assert spec.price_reader is not None
        assert PoolDataFacet.SPOT_PRICE in live_facets
        reader_type = spec.price_reader.reader.load()
        assert isinstance(reader_type, type)
        assert issubclass(reader_type, PoolPriceReader)


def test_historical_capability_declarations_have_gateway_implementations() -> None:
    state_by_key = _gateway_by_key(GATEWAY_REGISTRY.capability_providers(GatewayDexPoolStateCapability))
    twap_by_key = _gateway_by_key(GATEWAY_REGISTRY.capability_providers(GatewayDexTwapCapability))

    for spec in POOL_DATA_REGISTRY.all():
        if spec.source_for(PoolDataFacet.HISTORICAL_STATE) is PoolDataSource.GATEWAY_POOL_STATE:
            provider = state_by_key[spec.protocol]
            factory_chains = set(spec.price_reader.factory_addresses) if spec.price_reader is not None else set()
            assert factory_chains <= provider.pool_state_supported_chains()  # type: ignore[attr-defined]
        if spec.source_for(PoolDataFacet.TWAP) is PoolDataSource.GATEWAY_TWAP:
            provider = twap_by_key[spec.protocol]
            factory_chains = set(spec.price_reader.factory_addresses) if spec.price_reader is not None else set()
            assert factory_chains <= provider.twap_supported_chains()  # type: ignore[attr-defined]

    # Keep the connector manifest canonical in both directions: a gateway
    # provider must not expose a historical lane that generated-strategy
    # discovery can never select.
    for protocol in state_by_key:
        assert (
            POOL_DATA_REGISTRY.require(protocol).source_for(PoolDataFacet.HISTORICAL_STATE)
            is PoolDataSource.GATEWAY_POOL_STATE
        )
    for protocol in twap_by_key:
        assert POOL_DATA_REGISTRY.require(protocol).source_for(PoolDataFacet.TWAP) is PoolDataSource.GATEWAY_TWAP
