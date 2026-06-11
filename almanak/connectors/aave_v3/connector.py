"""Aave V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    FeeModelDecl,
    ImportRef,
    LendingReadDecl,
    MetadataAmountEncoding,
    StrategyMatrixEntry,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec

CONNECTOR = Connector(
    name="aave_v3",
    kind=ProtocolKind.LENDING,
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.aave_v3.fee_model", attribute="AaveV3FeeModel"),
        description="Aave V3 lending protocol fee model",
        aliases=("aave", "aave_v2"),
    ),
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
    compiler=ImportRef(
        module="almanak.connectors.aave_v3.compiler",
        attribute="AaveV3Compiler",
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
    capabilities=CapabilitiesSpec(
        keys=("aave_v3",),
        module="almanak.connectors.aave_v3.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("aave_v3",),
        module="almanak.connectors.aave_v3.supported_chains",
    ),
    primitive=ImportRef(
        module="almanak.connectors.aave_v3.primitive",
        attribute="PRIMITIVE",
    ),
    # Aave-family reads (VIB-4929): whole-wallet account state; 'aave' alias is lending-scoped.
    lending_read=LendingReadDecl(
        spec=ImportRef(module="almanak.connectors.aave_v3.lending_read", attribute="LENDING_READ_SPEC"),
        account_state=ImportRef(module="almanak.connectors.aave_v3.lending_read", attribute="ACCOUNT_STATE_READ_SPEC"),
        aliases=("aave", "aavev3"),
    ),
    # Aave-family compilers ship lending metadata amounts wei-encoded (VIB-3747).
    metadata_amount_encoding=MetadataAmountEncoding(lending="wei"),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW", "FLASH_LOAN"),
    strategy_chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "mantle", "xlayer"),
    # Matrix output stays lending-only for now; Aave flash-loan support exists
    # but historically has not rendered as its own support-matrix row.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="aave_v3",
            category="lending",
            chains=frozenset(
                (
                    "ethereum",
                    "arbitrum",
                    "optimism",
                    "polygon",
                    "base",
                    "avalanche",
                    "bsc",
                    "linea",
                    "plasma",
                    "sonic",
                    "mantle",
                    "xlayer",
                )
            ),
        ),
    ),
)

__all__ = ["CONNECTOR"]
