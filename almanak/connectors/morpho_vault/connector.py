"""Morpho vault connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors._strategy_base.vault_representatives import VaultRepresentativeSpec

CONNECTOR = Connector(
    name="morpho_vault",
    kind=ProtocolKind.VAULT,
    gateway_connector=ImportRef(
        module="almanak.connectors.morpho_vault.gateway.provider",
        attribute="MorphoVaultGatewayConnector",
        order=5,
    ),
    gas_estimate_connector=ImportRef(
        module="almanak.connectors.morpho_vault.gas_estimate_provider",
        attribute="MetaMorphoGasEstimateConnector",
    ),
    receipt_parser_protocols=("metamorpho",),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.morpho_vault.receipt_parser_provider",
        attribute="MetaMorphoReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.morpho_vault.compiler",
        attribute="MorphoVaultCompiler",
    ),
    compiler_protocols=("morpho_vault", "metamorpho"),
    capabilities=CapabilitiesSpec(
        keys=("metamorpho",),
        module="almanak.connectors.morpho_vault.capabilities",
    ),
    vault_representatives=(
        VaultRepresentativeSpec(
            protocol="metamorpho",
            module="almanak.connectors.morpho_vault.addresses",
            attribute="METAMORPHO_VAULTS",
        ),
    ),
    strategy_intents=("VAULT_DEPOSIT", "VAULT_REDEEM"),
    strategy_chains=("ethereum", "base"),
)

__all__ = ["CONNECTOR"]
