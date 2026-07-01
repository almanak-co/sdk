"""GMX V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    FeeModelDecl,
    FundingHistoryDecl,
    ImportRef,
    PerpsReadDecl,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec
from almanak.connectors.gmx_v2.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK

CONNECTOR = Connector(
    name="gmx_v2",
    kind=ProtocolKind.PERP,
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.gmx_v2.fee_model", attribute="GMXFeeModel"),
        name="gmx",
        description="GMX V2 perpetuals protocol fee model",
        aliases=("gmx_v2",),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="perp", aliases=("gmx",)),
    address_tables=(
        AddressTableSpec(
            protocol="gmx_v2",
            module="almanak.connectors.gmx_v2.addresses",
            attribute="GMX_V2",
        ),
        AddressTableSpec(
            protocol="gmx_v2_markets",
            module="almanak.connectors.gmx_v2.addresses",
            attribute="GMX_V2_MARKETS",
        ),
        AddressTableSpec(
            protocol="gmx_v2_tokens",
            module="almanak.connectors.gmx_v2.addresses",
            attribute="GMX_V2_TOKENS",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.gmx_v2.gateway.provider",
        attribute="GmxV2GatewayConnector",
        order=14,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.gmx_v2.receipt_parser_provider",
        attribute="GmxV2ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.gmx_v2.contract_monitoring",
        attribute="GMX_V2_CONTRACT_MONITORING_SPECS",
    ),
    compiler=ImportRef(
        module="almanak.connectors.gmx_v2.compiler",
        attribute="GMXV2Compiler",
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.gmx_v2.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    capabilities=CapabilitiesSpec(
        keys=("gmx_v2",),
        module="almanak.connectors.gmx_v2.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("gmx_v2",),
        module="almanak.connectors.gmx_v2.supported_chains",
    ),
    primitive=ImportRef(
        module="almanak.connectors.gmx_v2.primitive",
        attribute="PRIMITIVE",
    ),
    perps_read=PerpsReadDecl(
        spec=ImportRef(module="almanak.connectors.gmx_v2.perps_read", attribute="PERPS_READ_SPEC"),
        aliases=("gmx",),
    ),
    funding_history=FundingHistoryDecl(
        venue="gmx_v2",
        chains=("arbitrum", "avalanche"),
        aliases=("gmx",),
        markets=(
            "ETH-USD",
            "BTC-USD",
            "ARB-USD",
            "LINK-USD",
            "SOL-USD",
            "DOGE-USD",
            "UNI-USD",
            "AVAX-USD",
        ),
        backtest_provider=ImportRef(
            module="almanak.framework.backtesting.pnl.providers.perp.gmx_funding",
            attribute="GMXFundingProvider",
        ),
    ),
    backtest_risk=_BACKTEST_RISK,
    strategy_intents=("PERP_OPEN", "PERP_CLOSE"),
    strategy_chains=("arbitrum", "avalanche"),
)

__all__ = ["CONNECTOR"]
