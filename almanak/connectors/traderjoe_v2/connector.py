"""Trader Joe V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    DexVolumeDecl,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import SupportedChainsSpec

CONNECTOR = Connector(
    name="traderjoe_v2",
    kind=ProtocolKind.LP,
    dex_volume=DexVolumeDecl(
        chains=("avalanche",),
        amm_family="liquidity_book",
        aliases=("joe_v2",),
        chain_default=("avalanche",),
        liquidity_subgraph_ids={
            "avalanche": "6KD9JYCg2qa3TxNK3tLdhj5zuZTABoLLNcnUZXKG9vuH",
        },
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(
        strategy_type="lp", aliases=("traderjoe",), lp_economic_family="bin"
    ),
    address_tables=(
        AddressTableSpec(
            protocol="traderjoe_v2",
            module="almanak.connectors.traderjoe_v2.addresses",
            attribute="TRADERJOE_V2",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.traderjoe_v2.gateway.provider",
        attribute="TraderJoeV2GatewayConnector",
        order=19,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.traderjoe_v2.receipt_parser_provider",
        attribute="TraderJoeV2ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.traderjoe_v2.contract_monitoring",
        attribute="TRADERJOE_V2_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.traderjoe_v2.contract_roles",
        attribute="CONTRACT_ROLES",
        order=6,
    ),
    compiler=ImportRef(
        module="almanak.connectors.traderjoe_v2.compiler",
        attribute="TraderJoeV2Compiler",
    ),
    teardown_post_condition=ImportRef(
        module="almanak.connectors.traderjoe_v2.teardown_post_condition",
        attribute="traderjoe_v2_post_condition",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("traderjoe_v2",),
        module="almanak.connectors.traderjoe_v2.supported_chains",
    ),
    primitive=ImportRef(
        module="almanak.connectors.traderjoe_v2.primitive",
        attribute="PRIMITIVE",
    ),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
    strategy_chains=("avalanche", "arbitrum", "bsc", "ethereum"),
)

__all__ = ["CONNECTOR"]
