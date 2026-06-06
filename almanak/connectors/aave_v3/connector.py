"""Aave V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="aave_v3",
    kind=ProtocolKind.LENDING,
    address_tables=(
        AddressTableSpec(
            protocol="aave_v3",
            module="almanak.connectors.aave_v3.addresses",
            attribute="AAVE_V3",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.aave_v3.gateway.provider",
        attribute="AaveV3GatewayConnector",
        order=2,
    ),
    gas_estimate_connector=ImportRef(
        module="almanak.connectors.aave_v3.gas_estimate_provider",
        attribute="AaveV3GasEstimateConnector",
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.aave_v3.agent_read_provider",
        attribute="AaveV3AgentReadConnector",
        order=6,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.aave_v3.receipt_parser_provider",
        attribute="AaveV3ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.aave_v3.contract_monitoring",
        attribute="AAVE_V3_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.aave_v3.contract_roles",
        attribute="CONTRACT_ROLES",
        order=9,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.aave_v3.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    flash_loan_provider_name="aave",
    flash_loan_provider=ImportRef(
        module="almanak.connectors.aave_v3.flash_loan_provider",
        attribute="AaveFlashLoanProvider",
        order=1,
    ),
    flash_loan_builder=ImportRef(
        module="almanak.connectors.aave_v3.flash_loan",
        attribute="build_aave_flash_loan",
    ),
    flash_loan_synthetic_discovery=True,
)

__all__ = ["CONNECTOR"]
