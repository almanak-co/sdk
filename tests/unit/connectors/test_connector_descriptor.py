"""Tests for connector manifest discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._connector import (
    CONNECTOR_REGISTRY,
    Connector,
    ConnectorRegistry,
)
from almanak.connectors._connector_descriptor import (
    CONNECTOR_DESCRIPTOR_REGISTRY,
    ConnectorDescriptor,
    ConnectorDescriptorRegistry,
)
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserConnector,
)

EXPECTED_CONNECTOR_KINDS = {
    "aave_v3": ProtocolKind.LENDING,
    "across": ProtocolKind.BRIDGE,
    "aerodrome": ProtocolKind.LP,
    "aster_perps": ProtocolKind.PERP,
    "balancer_v2": ProtocolKind.LP,
    "beefy": ProtocolKind.VAULT,
    "benqi": ProtocolKind.LENDING,
    "compound_v3": ProtocolKind.LENDING,
    "curvance": ProtocolKind.LENDING,
    "curve": ProtocolKind.LP,
    "drift": ProtocolKind.PERP,
    "enso": ProtocolKind.SWAP,
    "ethena": ProtocolKind.LENDING,
    "euler_v2": ProtocolKind.LENDING,
    "fluid": ProtocolKind.LP,
    "gimo": ProtocolKind.LENDING,
    "gmx_v2": ProtocolKind.PERP,
    "hyperliquid": ProtocolKind.PERP,
    "joelend": ProtocolKind.LENDING,
    "jupiter": ProtocolKind.SWAP,
    "jupiter_lend": ProtocolKind.LENDING,
    "kamino": ProtocolKind.LENDING,
    "lagoon": ProtocolKind.VAULT,
    "lido": ProtocolKind.LENDING,
    "lifi": ProtocolKind.BRIDGE,
    "meteora": ProtocolKind.LP,
    "morpho_blue": ProtocolKind.LENDING,
    "morpho_vault": ProtocolKind.VAULT,
    "orca": ProtocolKind.LP,
    "pancakeswap_perps": ProtocolKind.PERP,
    "pancakeswap_v3": ProtocolKind.LP,
    "pendle": ProtocolKind.YIELD_TRADING,
    "polymarket": ProtocolKind.PREDICTION_MARKET,
    "raydium": ProtocolKind.LP,
    "silo_v2": ProtocolKind.LENDING,
    "spark": ProtocolKind.LENDING,
    "stargate": ProtocolKind.BRIDGE,
    "sushiswap_v3": ProtocolKind.LP,
    "traderjoe_v2": ProtocolKind.LP,
    "uniswap_v3": ProtocolKind.LP,
    "uniswap_v4": ProtocolKind.LP,
    "yearn": ProtocolKind.VAULT,
}

EXPECTED_ALIASES = {
    "aerodrome": ("aerodrome_slipstream",),
    "kamino": ("kamino_klend",),
    "meteora": ("meteora_dlmm",),
    "morpho_blue": ("morpho",),
    "orca": ("orca_whirlpools",),
    "raydium": ("raydium_clmm",),
    "uniswap_v3": ("agni_finance",),
}

EXPECTED_RECEIPT_PROVIDER_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.receipt_parser_provider",
    "across": "almanak.connectors.across.receipt_parser_provider",
    "aerodrome": "almanak.connectors.aerodrome.receipt_parser_provider",
    "aster_perps": "almanak.connectors.aster_perps.receipt_parser_provider",
    "benqi": "almanak.connectors.benqi.receipt_parser_provider",
    "compound_v3": "almanak.connectors.compound_v3.receipt_parser_provider",
    "curvance": "almanak.connectors.curvance.receipt_parser_provider",
    "curve": "almanak.connectors.curve.receipt_parser_provider",
    "drift": "almanak.connectors.drift.receipt_parser_provider",
    "enso": "almanak.connectors.enso.receipt_parser_provider",
    "ethena": "almanak.connectors.ethena.receipt_parser_provider",
    "euler_v2": "almanak.connectors.euler_v2.receipt_parser_provider",
    "fluid": "almanak.connectors.fluid.receipt_parser_provider",
    "gimo": "almanak.connectors.gimo.receipt_parser_provider",
    "gmx_v2": "almanak.connectors.gmx_v2.receipt_parser_provider",
    "joelend": "almanak.connectors.joelend.receipt_parser_provider",
    "jupiter": "almanak.connectors.jupiter.receipt_parser_provider",
    "jupiter_lend": "almanak.connectors.jupiter_lend.receipt_parser_provider",
    "kamino": "almanak.connectors.kamino.receipt_parser_provider",
    "lagoon": "almanak.connectors.lagoon.receipt_parser_provider",
    "lido": "almanak.connectors.lido.receipt_parser_provider",
    "lifi": "almanak.connectors.lifi.receipt_parser_provider",
    "meteora": "almanak.connectors.meteora.receipt_parser_provider",
    "morpho_blue": "almanak.connectors.morpho_blue.receipt_parser_provider",
    "morpho_vault": "almanak.connectors.morpho_vault.receipt_parser_provider",
    "orca": "almanak.connectors.orca.receipt_parser_provider",
    "pancakeswap_perps": "almanak.connectors.pancakeswap_perps.receipt_parser_provider",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.receipt_parser_provider",
    "pendle": "almanak.connectors.pendle.receipt_parser_provider",
    "polymarket": "almanak.connectors.polymarket.receipt_parser_provider",
    "raydium": "almanak.connectors.raydium.receipt_parser_provider",
    "silo_v2": "almanak.connectors.silo_v2.receipt_parser_provider",
    "spark": "almanak.connectors.spark.receipt_parser_provider",
    "stargate": "almanak.connectors.stargate.receipt_parser_provider",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.receipt_parser_provider",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.receipt_parser_provider",
    "uniswap_v3": "almanak.connectors.uniswap_v3.receipt_parser_provider",
    "uniswap_v4": "almanak.connectors.uniswap_v4.receipt_parser_provider",
}

EXPECTED_GATEWAY_PROVIDER_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.gateway.provider",
    "aerodrome": "almanak.connectors.aerodrome.gateway.provider",
    "agni_finance": "almanak.connectors.uniswap_v3.gateway.agni_provider",
    "aster_perps": "almanak.connectors.aster_perps.gateway.provider",
    "balancer_v2": "almanak.connectors.balancer_v2.gateway.provider",
    "beefy": "almanak.connectors.beefy.gateway.provider",
    "benqi": "almanak.connectors.benqi.gateway.provider",
    "compound_v3": "almanak.connectors.compound_v3.gateway.provider",
    "curve": "almanak.connectors.curve.gateway.provider",
    "enso": "almanak.connectors.enso.gateway.provider",
    "ethena": "almanak.connectors.ethena.gateway.provider",
    "fluid": "almanak.connectors.fluid.gateway.provider",
    "gmx_v2": "almanak.connectors.gmx_v2.gateway.provider",
    "hyperliquid": "almanak.connectors.hyperliquid.gateway.provider",
    "jupiter": "almanak.connectors.jupiter.gateway.provider",
    "lido": "almanak.connectors.lido.gateway.provider",
    "morpho_blue": "almanak.connectors.morpho_blue.gateway.provider",
    "morpho_vault": "almanak.connectors.morpho_vault.gateway.provider",
    "orca": "almanak.connectors.orca.gateway.provider",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.gateway.provider",
    "pendle": "almanak.connectors.pendle.gateway.provider",
    "polymarket": "almanak.connectors.polymarket.gateway.provider",
    "raydium": "almanak.connectors.raydium.gateway.provider",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.gateway.provider",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.gateway.provider",
    "uniswap_v3": "almanak.connectors.uniswap_v3.gateway.provider",
    "uniswap_v4": "almanak.connectors.uniswap_v4.gateway.provider",
    "yearn": "almanak.connectors.yearn.gateway.provider",
}

EXPECTED_GATEWAY_PROVIDER_ORDER = (
    "uniswap_v4",
    "aave_v3",
    "compound_v3",
    "fluid",
    "morpho_vault",
    "pendle",
    "jupiter",
    "beefy",
    "yearn",
    "enso",
    "polymarket",
    "uniswap_v3",
    "aerodrome",
    "gmx_v2",
    "hyperliquid",
    "balancer_v2",
    "lido",
    "ethena",
    "traderjoe_v2",
    "pancakeswap_v3",
    "raydium",
    "orca",
    "benqi",
    "curve",
    "sushiswap_v3",
    "agni_finance",
    "morpho_blue",
    "aster_perps",
)


def test_descriptor_names_alias_connector_interface() -> None:
    """Legacy descriptor names remain aliases for the canonical Connector API."""
    assert ConnectorDescriptor is Connector
    assert ConnectorDescriptorRegistry is ConnectorRegistry
    assert CONNECTOR_DESCRIPTOR_REGISTRY is CONNECTOR_REGISTRY


def test_discovers_migrated_connectors() -> None:
    """Discovery finds connector-owned manifests without central imports."""
    CONNECTOR_REGISTRY.clear()

    connectors = {connector.name: connector for connector in CONNECTOR_REGISTRY.all()}

    assert set(EXPECTED_CONNECTOR_KINDS) <= connectors.keys()
    for name, kind in EXPECTED_CONNECTOR_KINDS.items():
        assert isinstance(connectors[name], Connector)
        assert connectors[name].kind is kind
    for name, aliases in EXPECTED_ALIASES.items():
        assert connectors[name].aliases == aliases
    assert connectors["morpho_vault"].receipt_parser_protocols == ("metamorpho",)
    assert connectors["morpho_vault"].receipt_parser_keys == frozenset({"metamorpho"})


def test_connector_rejects_invalid_gateway_connector_ref() -> None:
    """Invalid singular gateway refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.gateway_connector"):
        Connector(
            name="bad_gateway_ref",
            kind=ProtocolKind.LP,
            gateway_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_receipt_parser_connector_ref() -> None:
    """Invalid singular receipt-parser refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.receipt_parser_connector"):
        Connector(
            name="bad_receipt_ref",
            kind=ProtocolKind.LP,
            receipt_parser_connector=object(),  # type: ignore[arg-type]
        )


def test_receipt_parser_connectors_instantiate_from_descriptors() -> None:
    """Migrated receipt-parser providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    for name, module in EXPECTED_RECEIPT_PROVIDER_MODULES.items():
        connector_manifest = CONNECTOR_REGISTRY.get(name)

        assert connector_manifest is not None
        assert connector_manifest.receipt_parser_connector is not None
        connector = connector_manifest.receipt_parser_connector.instantiate()

        assert isinstance(connector, ReceiptParserConnector)
        assert str(connector.protocol) in connector_manifest.receipt_parser_keys
        assert connector.receipt_parser_keys() == connector_manifest.receipt_parser_keys
        assert type(connector).__module__ == module


def test_gateway_connectors_instantiate_from_descriptors() -> None:
    """Migrated gateway providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[GatewayConnector, str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.all():
        for import_ref in connector_manifest.gateway_connector_refs:
            connector = import_ref.instantiate()
            connectors[str(connector.protocol)] = (connector, import_ref.module)
            orders[str(connector.protocol)] = import_ref.order

    assert set(EXPECTED_GATEWAY_PROVIDER_MODULES) <= connectors.keys()
    for name, module in EXPECTED_GATEWAY_PROVIDER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, GatewayConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module
    assert tuple(name for name, _order in sorted(orders.items(), key=lambda item: item[1] or 0)) == (
        EXPECTED_GATEWAY_PROVIDER_ORDER
    )
    assert CONNECTOR_REGISTRY.get("agni_finance").name == "uniswap_v3"


def test_connector_receipt_parsers_are_not_in_legacy_boot_file() -> None:
    """Connector-backed receipt parsers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_receipt_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_receipt_parser():
        assert connector_manifest.receipt_parser_connector is not None
        import_ref = connector_manifest.receipt_parser_connector
        assert import_ref.module not in source
        assert import_ref.attribute not in source


def test_connector_gateway_connectors_are_not_in_legacy_boot_file() -> None:
    """Connector-backed gateway providers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_gateway_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.all():
        for import_ref in connector_manifest.gateway_connector_refs:
            assert import_ref.module not in source
            assert import_ref.attribute not in source


def test_connector_modules_use_canonical_connector_name() -> None:
    """Connector-local manifests use ``Connector``, not the descriptor alias."""
    repo_root = Path(__file__).resolve().parents[3]

    offenders = []
    for connector_file in sorted((repo_root / "almanak/connectors").glob("*/connector.py")):
        source = connector_file.read_text()
        if "ConnectorDescriptor" in source or "_connector_descriptor" in source:
            offenders.append(connector_file.relative_to(repo_root).as_posix())

    assert not offenders
