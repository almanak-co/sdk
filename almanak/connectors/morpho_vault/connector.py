"""Morpho vault connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

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
    strategy_intents=("VAULT_DEPOSIT", "VAULT_REDEEM"),
    strategy_chains=("ethereum", "base"),
)

__all__ = ["CONNECTOR"]
