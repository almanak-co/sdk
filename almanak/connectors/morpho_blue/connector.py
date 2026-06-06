"""Morpho Blue connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="morpho_blue",
    kind=ProtocolKind.LENDING,
    aliases=("morpho",),
    address_tables=(
        AddressTableSpec(
            protocol="morpho_blue",
            module="almanak.connectors.morpho_blue.addresses",
            attribute="MORPHO_BLUE",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.morpho_blue.gateway.provider",
        attribute="MorphoBlueGatewayConnector",
        order=27,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.morpho_blue.receipt_parser_provider",
        attribute="MorphoBlueReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.morpho_blue.contract_monitoring",
        attribute="MORPHO_BLUE_CONTRACT_MONITORING_SPECS",
    ),
    flash_loan_provider_name="morpho",
    flash_loan_provider=ImportRef(
        module="almanak.connectors.morpho_blue.flash_loan_provider",
        attribute="MorphoFlashLoanProvider",
        order=3,
    ),
    flash_loan_builder=ImportRef(
        module="almanak.connectors.morpho_blue.flash_loan",
        attribute="build_morpho_flash_loan",
    ),
)

__all__ = ["CONNECTOR"]
