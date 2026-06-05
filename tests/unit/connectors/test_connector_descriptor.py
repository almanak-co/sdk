"""Tests for connector manifest discovery."""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

import almanak.connectors._connector_descriptor as connector_descriptor_module
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._connector import (
    CONNECTOR_REGISTRY,
    Connector,
    ConnectorDiscoveryError,
    ConnectorRegistry,
    ImportRef,
)
from almanak.connectors._connector_descriptor import (
    CONNECTOR_DESCRIPTOR_REGISTRY,
    ConnectorDescriptor,
    ConnectorDescriptorRegistry,
)
from almanak.connectors._strategy_base.accounting_report_registry import (
    AccountingReportConnector,
    AccountingReportSectionCapability,
)
from almanak.connectors._strategy_base.accounting_treatment_base import (
    AccountingTreatmentSpec,
)
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadConnector,
)
from almanak.connectors._strategy_base.bridge_base import BridgeAdapter
from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRoleSpec,
)
from almanak.connectors._strategy_base.flash_loan_base import FlashLoanProvider
from almanak.connectors._strategy_base.gas_estimate_registry import (
    GasEstimateConnector,
)
from almanak.connectors._strategy_base.principal_token_market_reader_registry import (
    PrincipalTokenMarketReadConnector,
)
from almanak.connectors._strategy_base.protocol_family_registry import (
    ProtocolFamilySpec,
)
from almanak.connectors._strategy_base.protocol_metadata_registry import (
    ProtocolMetadataConnector,
)
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserConnector,
)
from almanak.connectors._strategy_base.registry import ConnectorRegistry as StrategyConnectorRegistry
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerHookConnector,
)
from almanak.connectors._strategy_base.swap_classification_registry import (
    SwapClassificationSpec,
)
from almanak.connectors._strategy_base.swap_route_inference_registry import (
    SwapRouteInferenceConnector,
)
from almanak.connectors._strategy_base.vault_tool_registry import (
    VaultToolConnector,
)
from almanak.framework.permissions.models import ContractPermission

EXPECTED_CONNECTOR_KINDS = {
    "aave_v3": ProtocolKind.LENDING,
    "across": ProtocolKind.BRIDGE,
    "aerodrome": ProtocolKind.LP,
    "aster_perps": ProtocolKind.PERP,
    "balancer_v2": ProtocolKind.LP,
    "beefy": ProtocolKind.VAULT,
    "benqi": ProtocolKind.LENDING,
    "camelot": ProtocolKind.SWAP,
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

EXPECTED_GAS_ESTIMATE_PROVIDER_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.gas_estimate_provider",
    "across": "almanak.connectors.across.gas_estimate_provider",
    "balancer_v2": "almanak.connectors.balancer_v2.gas_estimate_provider",
    "morpho_vault": "almanak.connectors.morpho_vault.gas_estimate_provider",
    "uniswap_v3": "almanak.connectors.uniswap_v3.gas_estimate_provider",
}

EXPECTED_CONTRACT_ROLE_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.contract_roles",
    "aerodrome": "almanak.connectors.aerodrome.contract_roles",
    "balancer_v2": "almanak.connectors.balancer_v2.contract_roles",
    "camelot": "almanak.connectors.camelot.contract_roles",
    "fluid": "almanak.connectors.fluid.contract_roles",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.contract_roles",
    "spark": "almanak.connectors.spark.contract_roles",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.contract_roles",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.contract_roles",
    "uniswap_v3": "almanak.connectors.uniswap_v3.contract_roles",
    "uniswap_v4": "almanak.connectors.uniswap_v4.contract_roles",
}

EXPECTED_CONTRACT_ROLE_ORDER = (
    "uniswap_v3",
    "uniswap_v4",
    "sushiswap_v3",
    "pancakeswap_v3",
    "aerodrome",
    "traderjoe_v2",
    "camelot",
    "fluid",
    "aave_v3",
    "spark",
    "balancer_v2",
)

EXPECTED_PERMISSION_INFRASTRUCTURE_MODULES = {
    "enso": "almanak.connectors.enso.permission_hints",
}

EXPECTED_BRIDGE_ADAPTER_MODULES = {
    "across": "almanak.connectors.across.adapter",
    "stargate": "almanak.connectors.stargate.adapter",
}

EXPECTED_BRIDGE_ADAPTER_ORDER = ("across", "stargate")

EXPECTED_FLASH_LOAN_MODULES = {
    "aave_v3": (
        "aave",
        "almanak.connectors.aave_v3.flash_loan_provider",
        "almanak.connectors.aave_v3.flash_loan",
        True,
    ),
    "balancer_v2": (
        "balancer",
        "almanak.connectors.balancer_v2.flash_loan_provider",
        "almanak.connectors.balancer_v2.flash_loan",
        True,
    ),
    "morpho_blue": (
        "morpho",
        "almanak.connectors.morpho_blue.flash_loan_provider",
        "almanak.connectors.morpho_blue.flash_loan",
        False,
    ),
}

EXPECTED_FLASH_LOAN_ORDER = ("aave_v3", "balancer_v2", "morpho_blue")

EXPECTED_PROTOCOL_FAMILY_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.protocol_family",
    "aerodrome": "almanak.connectors.aerodrome.protocol_family",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.protocol_family",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.protocol_family",
    "uniswap_v3": "almanak.connectors.uniswap_v3.protocol_family",
}

EXPECTED_SWAP_CLASSIFICATION_MODULES = {
    "camelot": "almanak.connectors.camelot.swap_classification",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.swap_classification",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.swap_classification",
    "uniswap_v3": "almanak.connectors.uniswap_v3.swap_classification",
}

EXPECTED_SWAP_CLASSIFICATION_ORDER = (
    "uniswap_v3",
    "sushiswap_v3",
    "pancakeswap_v3",
    "camelot",
)

EXPECTED_AGENT_READ_PROVIDER_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.agent_read_provider",
    "aerodrome_slipstream": "almanak.connectors.aerodrome.agent_read_provider",
    "agni_finance": "almanak.connectors.uniswap_v3.agent_read_provider",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.agent_read_provider",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.agent_read_provider",
    "uniswap_v3": "almanak.connectors.uniswap_v3.agent_read_provider",
}

EXPECTED_AGENT_READ_PROVIDER_ORDER = (
    "uniswap_v3",
    "agni_finance",
    "aerodrome_slipstream",
    "pancakeswap_v3",
    "sushiswap_v3",
    "aave_v3",
)

EXPECTED_VAULT_TOOL_PROVIDER_MODULES = {
    "lagoon": "almanak.connectors.lagoon.vault_tool_provider",
}

EXPECTED_RUNNER_HOOK_PROVIDER_MODULES = {
    "uniswap_v3": "almanak.connectors.uniswap_v3.runner_hooks",
    "uniswap_v4": "almanak.connectors.uniswap_v4.runner_hooks",
}

EXPECTED_PROTOCOL_METADATA_MODULES = {
    "pendle": "almanak.connectors.pendle.metadata_provider",
}

EXPECTED_PRINCIPAL_TOKEN_MARKET_READER_MODULES = {
    "pendle": "almanak.connectors.pendle.on_chain_reader_provider",
}

EXPECTED_SWAP_ROUTE_INFERENCE_MODULES = {
    "pendle": "almanak.connectors.pendle.swap_route_inference",
}

EXPECTED_ACCOUNTING_TREATMENT_MODULES = {
    "pendle": "almanak.connectors.pendle.accounting_spec",
}

EXPECTED_ACCOUNTING_REPORT_MODULES = {
    "pendle": "almanak.connectors.pendle.reporting",
}


@pytest.fixture(autouse=True)
def _isolate_strategy_connector_registry() -> Iterator[None]:
    """Keep descriptor provider imports from leaking strategy registry state."""
    StrategyConnectorRegistry._clear()
    yield
    StrategyConnectorRegistry._clear()


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


def test_manifest_discovery_does_not_import_connector_package(monkeypatch: pytest.MonkeyPatch) -> None:
    """Manifest discovery avoids connector package side effects."""
    import sys

    import almanak.connectors as connectors_package

    CONNECTOR_REGISTRY.clear()
    sys.modules.pop("almanak.connectors.pancakeswap_perps", None)
    sys.modules.pop("almanak.connectors.pancakeswap_perps.connector", None)
    monkeypatch.delattr(connectors_package, "pancakeswap_perps", raising=False)

    connector_manifest = CONNECTOR_REGISTRY.get("pancakeswap_perps")

    assert connector_manifest is not None
    assert connector_manifest.name == "pancakeswap_perps"
    assert "almanak.connectors.pancakeswap_perps" not in sys.modules


def test_connector_registry_rejects_reentrant_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recursive manifest discovery fails with an actionable registry error."""
    registry = ConnectorRegistry()

    def reenter_discovery() -> tuple[Connector, ...]:
        return registry.all()

    monkeypatch.setattr(registry, "_discover", reenter_discovery)

    with pytest.raises(ConnectorDiscoveryError, match="recursive connector discovery"):
        registry.all()

    monkeypatch.setattr(registry, "_discover", lambda: ())
    assert registry.all() == ()


@pytest.mark.parametrize(
    ("capability", "connector_kwargs"),
    (
        (
            "Gateway connector",
            lambda name: {
                "kind": ProtocolKind.LP,
                "gateway_connector": ImportRef(module=f"tests.fake_{name}", attribute="Gateway", order=7),
            },
        ),
        (
            "Contract-role",
            lambda name: {
                "kind": ProtocolKind.LP,
                "contract_roles": ImportRef(module=f"tests.fake_{name}", attribute="CONTRACT_ROLES", order=7),
            },
        ),
        (
            "Swap-classification",
            lambda name: {
                "kind": ProtocolKind.SWAP,
                "swap_classification": ImportRef(
                    module=f"tests.fake_{name}",
                    attribute="SWAP_CLASSIFICATION",
                    order=7,
                ),
            },
        ),
        (
            "Bridge adapter",
            lambda name: {
                "kind": ProtocolKind.BRIDGE,
                "bridge_adapter": ImportRef(module=f"tests.fake_{name}", attribute="BridgeAdapter", order=7),
            },
        ),
        (
            "Flash-loan provider",
            lambda name: {
                "kind": ProtocolKind.LENDING,
                "flash_loan_provider_name": name,
                "flash_loan_provider": ImportRef(module=f"tests.fake_{name}", attribute="FlashLoanProvider", order=7),
                "flash_loan_builder": ImportRef(module=f"tests.fake_{name}", attribute="build_flash_loan"),
            },
        ),
    ),
)
def test_connector_registry_rejects_duplicate_order_bearing_refs(
    monkeypatch: pytest.MonkeyPatch,
    capability: str,
    connector_kwargs: object,
) -> None:
    """Discovery rejects duplicate explicit order keys before stable-sort tie breaking."""
    registry = ConnectorRegistry()
    kwargs_for_name = connector_kwargs
    assert callable(kwargs_for_name)
    connectors = {name: Connector(name=name, **kwargs_for_name(name)) for name in ("alpha", "beta")}

    monkeypatch.setattr(
        connector_descriptor_module.pkgutil,
        "iter_modules",
        lambda _paths: iter(SimpleNamespace(ispkg=True, name=name) for name in connectors),
    )
    monkeypatch.setattr(registry, "_load_connector", lambda name: connectors[name])

    with pytest.raises(
        ConnectorDiscoveryError,
        match=rf"{capability} order 7 is claimed by both 'alpha' and 'beta'",
    ):
        registry.all()


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


def test_connector_rejects_invalid_gas_estimate_connector_ref() -> None:
    """Invalid gas-estimate refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.gas_estimate_connector"):
        Connector(
            name="bad_gas_ref",
            kind=ProtocolKind.LP,
            gas_estimate_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_agent_read_connector_ref() -> None:
    """Invalid agent-read refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.agent_read_connector"):
        Connector(
            name="bad_agent_read_ref",
            kind=ProtocolKind.LP,
            agent_read_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_vault_tool_connector_ref() -> None:
    """Invalid vault-tool refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.vault_tool_connector"):
        Connector(
            name="bad_vault_tool_ref",
            kind=ProtocolKind.VAULT,
            vault_tool_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_runner_hook_connector_ref() -> None:
    """Invalid runner-hook refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.runner_hook_connector"):
        Connector(
            name="bad_runner_hook_ref",
            kind=ProtocolKind.LP,
            runner_hook_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_protocol_metadata_ref() -> None:
    """Invalid protocol-metadata refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.protocol_metadata"):
        Connector(
            name="bad_protocol_metadata_ref",
            kind=ProtocolKind.YIELD_TRADING,
            protocol_metadata=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_principal_token_market_reader_ref() -> None:
    """Invalid principal-token reader refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.principal_token_market_reader"):
        Connector(
            name="bad_principal_token_market_reader_ref",
            kind=ProtocolKind.YIELD_TRADING,
            principal_token_market_reader=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_swap_route_inference_ref() -> None:
    """Invalid swap-route inference refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.swap_route_inference"):
        Connector(
            name="bad_swap_route_inference_ref",
            kind=ProtocolKind.YIELD_TRADING,
            swap_route_inference=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_accounting_treatment_ref() -> None:
    """Invalid accounting-treatment refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.accounting_treatment"):
        Connector(
            name="bad_accounting_treatment_ref",
            kind=ProtocolKind.YIELD_TRADING,
            accounting_treatment=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_accounting_report_ref() -> None:
    """Invalid accounting-report refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.accounting_report"):
        Connector(
            name="bad_accounting_report_ref",
            kind=ProtocolKind.YIELD_TRADING,
            accounting_report=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_protocol_family_ref() -> None:
    """Invalid protocol-family refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.protocol_family"):
        Connector(
            name="bad_protocol_family_ref",
            kind=ProtocolKind.LP,
            protocol_family=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_swap_classification_ref() -> None:
    """Invalid swap-classification refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.swap_classification"):
        Connector(
            name="bad_swap_classification_ref",
            kind=ProtocolKind.SWAP,
            swap_classification=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_contract_roles_ref() -> None:
    """Invalid contract-role refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.contract_roles"):
        Connector(
            name="bad_contract_roles_ref",
            kind=ProtocolKind.LP,
            contract_roles=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_permission_infrastructure_ref() -> None:
    """Invalid infrastructure-permission refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.permission_infrastructure"):
        Connector(
            name="bad_permission_infrastructure",
            kind=ProtocolKind.SWAP,
            permission_infrastructure=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_bridge_adapter_ref() -> None:
    """Invalid bridge-adapter refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.bridge_adapter"):
        Connector(
            name="bad_bridge_adapter_ref",
            kind=ProtocolKind.BRIDGE,
            bridge_adapter=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_flash_loan_provider_ref() -> None:
    """Invalid flash-loan provider refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.flash_loan_provider"):
        Connector(
            name="bad_flash_loan_provider_ref",
            kind=ProtocolKind.LENDING,
            flash_loan_provider_name="bad",
            flash_loan_provider=object(),  # type: ignore[arg-type]
            flash_loan_builder=ImportRef(module="tests.fake_flash_loan", attribute="build_fake_flash_loan"),
        )


def test_connector_rejects_invalid_flash_loan_builder_ref() -> None:
    """Invalid flash-loan builder refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.flash_loan_builder"):
        Connector(
            name="bad_flash_loan_builder_ref",
            kind=ProtocolKind.LENDING,
            flash_loan_provider_name="bad",
            flash_loan_provider=ImportRef(module="tests.fake_flash_loan_provider", attribute="FakeProvider"),
            flash_loan_builder=object(),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("kwargs", "match"),
    (
        (
            {
                "flash_loan_provider": ImportRef(
                    module="tests.fake_flash_loan_provider",
                    attribute="FakeProvider",
                ),
                "flash_loan_builder": ImportRef(
                    module="tests.fake_flash_loan",
                    attribute="build_fake_flash_loan",
                ),
            },
            "Connector.flash_loan_provider_name",
        ),
        (
            {
                "flash_loan_synthetic_discovery": True,
            },
            "Connector.flash_loan_provider_name",
        ),
        (
            {
                "flash_loan_provider_name": "fake",
                "flash_loan_builder": ImportRef(
                    module="tests.fake_flash_loan",
                    attribute="build_fake_flash_loan",
                ),
            },
            "Connector.flash_loan_provider is required",
        ),
        (
            {
                "flash_loan_provider_name": "fake",
                "flash_loan_provider": ImportRef(
                    module="tests.fake_flash_loan_provider",
                    attribute="FakeProvider",
                ),
            },
            "Connector.flash_loan_builder is required",
        ),
    ),
)
def test_connector_rejects_partial_flash_loan_metadata(kwargs: dict[str, object], match: str) -> None:
    """Flash-loan manifest fields must be published as a complete set."""
    with pytest.raises(ValueError, match=match):
        Connector(
            name="partial_flash_loan",
            kind=ProtocolKind.LENDING,
            **kwargs,
        )


def test_connector_rejects_invalid_flash_loan_synthetic_discovery_flag() -> None:
    """Flash-loan synthetic-discovery opt-in must be boolean."""
    with pytest.raises(ValueError, match="Connector.flash_loan_synthetic_discovery"):
        Connector(
            name="bad_flash_loan_synthetic_discovery",
            kind=ProtocolKind.LENDING,
            flash_loan_synthetic_discovery="yes",  # type: ignore[arg-type]
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


def test_gas_estimate_connectors_instantiate_from_descriptors() -> None:
    """Migrated gas-estimate providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    for name, module in EXPECTED_GAS_ESTIMATE_PROVIDER_MODULES.items():
        connector_manifest = CONNECTOR_REGISTRY.get(name)

        assert connector_manifest is not None
        assert connector_manifest.gas_estimate_connector is not None
        connector = connector_manifest.gas_estimate_connector.instantiate()

        assert isinstance(connector, GasEstimateConnector)
        assert type(connector).__module__ == module


def test_contract_role_specs_load_from_descriptors() -> None:
    """Migrated contract-role specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[tuple[ContractRoleSpec, ...], str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_contract_roles():
        assert connector_manifest.contract_roles is not None
        specs = connector_manifest.contract_roles.load()
        assert isinstance(specs, tuple)
        assert specs
        assert all(isinstance(spec, ContractRoleSpec) for spec in specs)
        connectors[connector_manifest.name] = (specs, connector_manifest.contract_roles.module)
        orders[connector_manifest.name] = connector_manifest.contract_roles.order

    assert set(EXPECTED_CONTRACT_ROLE_MODULES) == connectors.keys()
    for name, module in EXPECTED_CONTRACT_ROLE_MODULES.items():
        _specs, actual_module = connectors[name]

        assert actual_module == module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_CONTRACT_ROLE_ORDER
    )


def test_permission_infrastructure_builders_load_from_descriptors() -> None:
    """Migrated infrastructure-permission hooks are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, str] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_permission_infrastructure():
        assert connector_manifest.permission_infrastructure is not None
        builder = connector_manifest.permission_infrastructure.load()
        assert callable(builder)
        permissions = builder("arbitrum")
        assert isinstance(permissions, list)
        assert permissions
        assert all(isinstance(permission, ContractPermission) for permission in permissions)
        connectors[connector_manifest.name] = connector_manifest.permission_infrastructure.module

    assert EXPECTED_PERMISSION_INFRASTRUCTURE_MODULES == connectors


def test_bridge_adapters_load_from_descriptors() -> None:
    """Migrated bridge adapters are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    adapters: dict[str, tuple[type[BridgeAdapter], str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_bridge_adapter():
        assert connector_manifest.bridge_adapter is not None
        adapter_cls = connector_manifest.bridge_adapter.load()
        assert isinstance(adapter_cls, type)
        assert issubclass(adapter_cls, BridgeAdapter)
        adapters[connector_manifest.name] = (adapter_cls, connector_manifest.bridge_adapter.module)
        orders[connector_manifest.name] = connector_manifest.bridge_adapter.order

    assert set(EXPECTED_BRIDGE_ADAPTER_MODULES) == adapters.keys()
    for name, module in EXPECTED_BRIDGE_ADAPTER_MODULES.items():
        adapter_cls, actual_module = adapters[name]

        assert actual_module == module
        assert adapter_cls.__module__ == module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_BRIDGE_ADAPTER_ORDER
    )


def test_flash_loan_providers_load_from_descriptors() -> None:
    """Migrated flash-loan providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    providers: dict[str, tuple[str, type[FlashLoanProvider], str, str, bool]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_flash_loan():
        assert connector_manifest.flash_loan_provider_name is not None
        assert connector_manifest.flash_loan_provider is not None
        assert connector_manifest.flash_loan_builder is not None

        provider_cls = connector_manifest.flash_loan_provider.load()
        builder = connector_manifest.flash_loan_builder.load()

        assert isinstance(provider_cls, type)
        assert issubclass(provider_cls, FlashLoanProvider)
        assert callable(builder)

        provider = provider_cls()
        assert provider.name == connector_manifest.flash_loan_provider_name
        providers[connector_manifest.name] = (
            connector_manifest.flash_loan_provider_name,
            provider_cls,
            connector_manifest.flash_loan_provider.module,
            connector_manifest.flash_loan_builder.module,
            connector_manifest.flash_loan_synthetic_discovery,
        )
        orders[connector_manifest.name] = connector_manifest.flash_loan_provider.order

    assert set(EXPECTED_FLASH_LOAN_MODULES) == providers.keys()
    for name, (
        expected_provider_name,
        expected_provider_module,
        expected_builder_module,
        expected_synthetic,
    ) in EXPECTED_FLASH_LOAN_MODULES.items():
        provider_name, provider_cls, actual_provider_module, actual_builder_module, actual_synthetic = providers[name]

        assert provider_name == expected_provider_name
        assert actual_provider_module == expected_provider_module
        assert actual_builder_module == expected_builder_module
        assert actual_synthetic is expected_synthetic
        assert provider_cls.__module__ == expected_provider_module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_FLASH_LOAN_ORDER
    )


def test_protocol_family_specs_load_from_descriptors() -> None:
    """Migrated protocol-family specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    for name, module in EXPECTED_PROTOCOL_FAMILY_MODULES.items():
        connector_manifest = CONNECTOR_REGISTRY.get(name)

        assert connector_manifest is not None
        assert connector_manifest.protocol_family is not None
        spec = connector_manifest.protocol_family.load()

        assert isinstance(spec, ProtocolFamilySpec)
        assert spec.families
        assert connector_manifest.protocol_family.module == module


def test_swap_classification_specs_load_from_descriptors() -> None:
    """Migrated swap-classification specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[tuple[SwapClassificationSpec, ...], str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_swap_classification():
        assert connector_manifest.swap_classification is not None
        specs = connector_manifest.swap_classification.load()
        assert isinstance(specs, tuple)
        assert specs
        assert all(isinstance(spec, SwapClassificationSpec) for spec in specs)
        connectors[connector_manifest.name] = (specs, connector_manifest.swap_classification.module)
        orders[connector_manifest.name] = connector_manifest.swap_classification.order

    assert set(EXPECTED_SWAP_CLASSIFICATION_MODULES) == connectors.keys()
    for name, module in EXPECTED_SWAP_CLASSIFICATION_MODULES.items():
        _specs, actual_module = connectors[name]

        assert actual_module == module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_SWAP_CLASSIFICATION_ORDER
    )


def test_agent_read_connectors_instantiate_from_descriptors() -> None:
    """Migrated agent-read providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[AgentReadConnector, str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.all():
        for import_ref in connector_manifest.agent_read_connector_refs:
            connector = import_ref.instantiate()
            connectors[str(connector.protocol)] = (connector, import_ref.module)
            orders[str(connector.protocol)] = import_ref.order

    assert set(EXPECTED_AGENT_READ_PROVIDER_MODULES) == connectors.keys()
    for name, module in EXPECTED_AGENT_READ_PROVIDER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, AgentReadConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_AGENT_READ_PROVIDER_ORDER
    )


def test_vault_tool_connectors_instantiate_from_descriptors() -> None:
    """Migrated vault-tool providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[VaultToolConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.all():
        for import_ref in connector_manifest.vault_tool_connector_refs:
            connector = import_ref.instantiate()
            connectors[str(connector.protocol)] = (connector, import_ref.module)

    assert set(EXPECTED_VAULT_TOOL_PROVIDER_MODULES) == connectors.keys()
    for name, module in EXPECTED_VAULT_TOOL_PROVIDER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, VaultToolConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_runner_hook_connectors_instantiate_from_descriptors() -> None:
    """Migrated runner-hook providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[RunnerHookConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_runner_hooks():
        assert connector_manifest.runner_hook_connector is not None
        connector = connector_manifest.runner_hook_connector.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.runner_hook_connector.module)

    assert set(EXPECTED_RUNNER_HOOK_PROVIDER_MODULES) == connectors.keys()
    for name, module in EXPECTED_RUNNER_HOOK_PROVIDER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, RunnerHookConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_protocol_metadata_connectors_instantiate_from_descriptors() -> None:
    """Migrated protocol-metadata providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[ProtocolMetadataConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_protocol_metadata():
        assert connector_manifest.protocol_metadata is not None
        connector = connector_manifest.protocol_metadata.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.protocol_metadata.module)

    assert set(EXPECTED_PROTOCOL_METADATA_MODULES) == connectors.keys()
    for name, module in EXPECTED_PROTOCOL_METADATA_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, ProtocolMetadataConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_principal_token_market_readers_instantiate_from_descriptors() -> None:
    """Migrated principal-token market readers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[PrincipalTokenMarketReadConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_principal_token_market_reader():
        assert connector_manifest.principal_token_market_reader is not None
        connector = connector_manifest.principal_token_market_reader.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.principal_token_market_reader.module)

    assert set(EXPECTED_PRINCIPAL_TOKEN_MARKET_READER_MODULES) == connectors.keys()
    for name, module in EXPECTED_PRINCIPAL_TOKEN_MARKET_READER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, PrincipalTokenMarketReadConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_swap_route_inference_connectors_instantiate_from_descriptors() -> None:
    """Migrated swap-route inference providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[SwapRouteInferenceConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_swap_route_inference():
        assert connector_manifest.swap_route_inference is not None
        connector = connector_manifest.swap_route_inference.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.swap_route_inference.module)

    assert set(EXPECTED_SWAP_ROUTE_INFERENCE_MODULES) == connectors.keys()
    for name, module in EXPECTED_SWAP_ROUTE_INFERENCE_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, SwapRouteInferenceConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_accounting_treatment_specs_load_from_descriptors() -> None:
    """Migrated accounting-treatment specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    specs: dict[str, tuple[AccountingTreatmentSpec, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_treatment():
        assert connector_manifest.accounting_treatment is not None
        spec = connector_manifest.accounting_treatment.load()
        specs[connector_manifest.name] = (spec, connector_manifest.accounting_treatment.module)

    assert set(EXPECTED_ACCOUNTING_TREATMENT_MODULES) == specs.keys()
    for name, module in EXPECTED_ACCOUNTING_TREATMENT_MODULES.items():
        spec, actual_module = specs[name]

        assert isinstance(spec, AccountingTreatmentSpec)
        assert actual_module == module


def test_accounting_report_connectors_instantiate_from_descriptors() -> None:
    """Migrated accounting-report providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[AccountingReportConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_report():
        assert connector_manifest.accounting_report is not None
        connector = connector_manifest.accounting_report.instantiate()
        connectors[connector.key] = (connector, connector_manifest.accounting_report.module)

    assert set(EXPECTED_ACCOUNTING_REPORT_MODULES) == connectors.keys()
    for name, module in EXPECTED_ACCOUNTING_REPORT_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, AccountingReportConnector)
        assert connector.key == name
        assert connector.strategy_class == name
        assert isinstance(connector, AccountingReportSectionCapability)
        assert connector.section_key == name
        assert actual_module == module
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


def test_connector_gas_estimates_are_not_in_legacy_boot_file() -> None:
    """Connector-backed gas-estimate providers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_gas_estimate_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_gas_estimate():
        assert connector_manifest.gas_estimate_connector is not None
        import_ref = connector_manifest.gas_estimate_connector
        assert import_ref.module not in source
        assert import_ref.attribute not in source


def test_connector_contract_roles_are_not_in_legacy_boot_file() -> None:
    """Connector-backed contract-role specs must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_contract_role_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_contract_roles():
        assert connector_manifest.contract_roles is not None
        import_ref = connector_manifest.contract_roles
        assert import_ref.module not in source


def test_connector_permission_infrastructure_is_not_hardcoded_in_framework() -> None:
    """Connector-backed infrastructure-permission hooks must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/permissions/generator.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_permission_infrastructure():
        assert connector_manifest.permission_infrastructure is not None
        import_ref = connector_manifest.permission_infrastructure
        assert import_ref.module not in source
        assert import_ref.attribute not in source
        assert f"almanak.connectors.{connector_manifest.name}." not in source


def test_connector_bridge_adapters_are_not_in_legacy_boot_file() -> None:
    """Connector-backed bridge adapters must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_bridge_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_bridge_adapter():
        assert connector_manifest.bridge_adapter is not None
        import_ref = connector_manifest.bridge_adapter
        assert import_ref.module not in source
        assert import_ref.attribute not in source


def test_connector_flash_loans_are_not_in_legacy_boot_file() -> None:
    """Connector-backed flash-loan providers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_flash_loan_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_flash_loan():
        assert connector_manifest.flash_loan_provider is not None
        assert connector_manifest.flash_loan_builder is not None
        assert connector_manifest.flash_loan_provider.module not in source
        assert connector_manifest.flash_loan_provider.attribute not in source
        assert connector_manifest.flash_loan_builder.module not in source
        assert connector_manifest.flash_loan_builder.attribute not in source


def test_connector_protocol_families_are_not_in_legacy_boot_file() -> None:
    """Connector-backed protocol-family specs must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_protocol_family_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_protocol_family():
        assert connector_manifest.protocol_family is not None
        import_ref = connector_manifest.protocol_family
        assert import_ref.module not in source


def test_connector_swap_classifications_are_not_in_legacy_boot_file() -> None:
    """Connector-backed swap-classification specs must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_swap_classification_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_swap_classification():
        assert connector_manifest.swap_classification is not None
        import_ref = connector_manifest.swap_classification
        assert import_ref.module not in source


def test_connector_agent_tools_are_not_in_legacy_boot_file() -> None:
    """Connector-backed agent-tool providers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_agent_tool_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_agent_read():
        for import_ref in connector_manifest.agent_read_connector_refs:
            assert import_ref.module not in source
            assert import_ref.attribute not in source
    for connector_manifest in CONNECTOR_REGISTRY.with_vault_tool():
        for import_ref in connector_manifest.vault_tool_connector_refs:
            assert import_ref.module not in source
            assert import_ref.attribute not in source


def test_connector_vault_lifecycle_is_not_hardcoded_in_framework() -> None:
    """Framework vault lifecycle paths must not import concrete vault connector modules."""
    repo_root = Path(__file__).resolve().parents[3]
    source = "\n".join(
        (repo_root / path).read_text()
        for path in (
            "almanak/framework/vault/lifecycle.py",
            "almanak/framework/cli/run_helpers.py",
            "almanak/framework/cli/run.py",
        )
    )

    assert re.search(r"\bget_vault_tool_capability\s*\(", source) is not None
    assert re.search(r"\balmanak\.connectors\.(?!_)[a-zA-Z0-9_]+", source) is None
    assert (
        re.search(
            r"^\s*from\s+almanak\.connectors(?:\.[A-Za-z0-9_]+)?\s+import\s+(?!_)[A-Za-z0-9_]+",
            source,
            re.MULTILINE,
        )
        is None
    )
    assert "almanak.connectors.lagoon" not in source
    assert "LagoonVaultSDK" not in source
    assert "LagoonVaultAdapter" not in source
    assert "LagoonVaultDeployer" not in source
    assert "VaultDeployParams" not in source
    assert "LagoonReceiptParser" not in source


def test_connector_runner_hooks_are_not_hardcoded_in_framework_runner() -> None:
    """Framework runner paths must not import concrete connector hook modules."""
    repo_root = Path(__file__).resolve().parents[3]
    source = "\n".join(
        (repo_root / path).read_text()
        for path in (
            "almanak/framework/runner/strategy_runner.py",
            "almanak/framework/runner/inner_runner.py",
            "almanak/framework/runner/teardown_commit.py",
        )
    )

    assert "almanak.connectors.uniswap_v3.slot0_fallback" not in source
    assert "almanak.connectors.uniswap_v3.receipt_parser" not in source
    assert "UniswapV3ReceiptParser" not in source
    assert "almanak.connectors.uniswap_v4.gateway_pool_key_client" not in source
    assert "make_sync_pool_key_lookup" not in source
    assert "_build_v4_pool_key_lookup" not in source


def test_connector_protocol_metadata_is_not_hardcoded_in_framework_data() -> None:
    """Framework data paths must not import concrete connector metadata modules."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/data/tokens/resolver.py").read_text()

    assert "almanak.connectors.pendle.sdk" not in source


def test_framework_pendle_data_package_is_removed() -> None:
    """Pendle data implementation must stay connector-owned."""
    repo_root = Path(__file__).resolve().parents[3]

    assert not (repo_root / "almanak/framework/data/pendle").exists()


def test_connector_principal_token_reader_is_not_hardcoded_in_framework_data() -> None:
    """Framework data and valuation paths must not import concrete Pendle reader modules."""
    repo_root = Path(__file__).resolve().parents[3]
    source = "\n".join(
        (repo_root / path).read_text()
        for path in (
            "almanak/framework/data/position_health.py",
            "almanak/framework/valuation/pendle_valuer.py",
        )
    )

    assert "almanak.framework.data.pendle.on_chain_reader" not in source
    assert "almanak.connectors.pendle.on_chain_reader" not in source
    assert "PendleOnChainReader" not in source


def test_connector_swap_route_inference_is_not_hardcoded_in_framework_compiler() -> None:
    """Framework compiler asks route inference providers instead of naming concrete heuristics."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/intents/compiler.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_swap_route_inference():
        assert connector_manifest.swap_route_inference is not None
        assert connector_manifest.swap_route_inference.module not in source
        assert connector_manifest.swap_route_inference.attribute not in source
    assert "_has_pendle_token_prefix" not in source
    assert 'get_connector_compiler("pendle")' not in source


def test_connector_accounting_treatment_is_not_hardcoded_in_framework_registry() -> None:
    """Framework accounting paths discover connector treatments from manifests."""
    repo_root = Path(__file__).resolve().parents[3]
    source = "\n".join(
        (repo_root / path).read_text()
        for path in (
            "almanak/connectors/_strategy_base/accounting_treatment_registry.py",
            "almanak/connectors/_strategy_accounting_treatment_registry.py",
            "almanak/framework/accounting/processor.py",
            "almanak/framework/runner/strategy_runner.py",
        )
    )

    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_treatment():
        assert connector_manifest.accounting_treatment is not None
        assert connector_manifest.accounting_treatment.module not in source
    assert '"pendle": ("almanak.connectors.pendle.accounting_spec"' not in source


def test_connector_accounting_report_is_not_hardcoded_in_reporting_loader() -> None:
    """Framework reporting loader asks connector report providers for connector events."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/accounting/reporting/loader.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_report():
        assert connector_manifest.accounting_report is not None
        assert connector_manifest.accounting_report.module not in source
        assert connector_manifest.accounting_report.attribute not in source
    assert "PendleAccountingEvent" not in source
    assert "PendleEventType" not in source


def test_connector_accounting_sections_are_not_hardcoded_in_strat_pnl() -> None:
    """The strat-pnl CLI discovers connector report sections through providers."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/cli/strat_pnl.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_report():
        assert connector_manifest.accounting_report is not None
        assert connector_manifest.accounting_report.module not in source
        assert connector_manifest.accounting_report.attribute not in source

    assert "build_pendle_report" not in source
    assert "render_pendle_section" not in source
    assert "pendle_section_to_dict" not in source
    assert "acct_data.pendle_events" not in source


def test_connector_accounting_sections_are_owned_by_connector_modules() -> None:
    """Framework Pendle reporting compatibility wrappers do not own connector section logic."""
    repo_root = Path(__file__).resolve().parents[3]

    reporting_init_source = (repo_root / "almanak/framework/accounting/reporting/__init__.py").read_text()
    pendle_report_source = (repo_root / "almanak/framework/accounting/reporting/pendle_report.py").read_text()
    render_text_source = (repo_root / "almanak/framework/accounting/reporting/render_text.py").read_text()
    render_json_source = (repo_root / "almanak/framework/accounting/reporting/render_json.py").read_text()
    connector_source = (repo_root / "almanak/connectors/pendle/reporting.py").read_text()

    assert "from .pendle_report import" not in reporting_init_source
    assert "from .pendle_report import" not in render_text_source
    assert "from .pendle_report import" not in render_json_source
    assert "PendleAccountingEvent" not in pendle_report_source
    assert "PendleEventType" not in pendle_report_source
    assert "class PendlePositionSummary" not in pendle_report_source
    assert "class PendleSection" not in pendle_report_source

    assert "PendleAccountingEvent" in connector_source
    assert "PendleEventType" in connector_source
    assert "class PendlePositionSummary" in connector_source
    assert "class PendleSection" in connector_source


def test_connector_modules_use_canonical_connector_name() -> None:
    """Connector-local manifests use ``Connector``, not the descriptor alias."""
    repo_root = Path(__file__).resolve().parents[3]

    offenders = []
    for connector_file in sorted((repo_root / "almanak/connectors").glob("*/connector.py")):
        source = connector_file.read_text()
        if "ConnectorDescriptor" in source or "_connector_descriptor" in source:
            offenders.append(connector_file.relative_to(repo_root).as_posix())

    assert not offenders
