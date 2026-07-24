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
    runner_hook_connector=ImportRef(
        module="almanak.connectors.gmx_v2.runner_hooks",
        attribute="GmxV2RunnerHookConnector",
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
    # VIB-5116: on-chain closure verify (open positions + pending OrderVault
    # orders) and residual discovery of pending unfilled orders that hold
    # collateral but are not yet positions.
    teardown_post_condition=ImportRef(
        module="almanak.connectors.gmx_v2.teardown_post_condition",
        attribute="gmx_v2_teardown_post_condition",
    ),
    teardown_residual_discovery=ImportRef(
        module="almanak.connectors.gmx_v2.teardown_residual_discovery",
        attribute="gmx_v2_teardown_residual_discovery",
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
            module="almanak.connectors.gmx_v2.backtest_funding",
            attribute="GMXFundingProvider",
        ),
    ),
    backtest_risk=_BACKTEST_RISK,
    # PERP_CANCEL_ORDER (VIB-5568) is INTENTIONALLY NOT here. strategy_intents is the
    # STRATEGY-AUTHORING universe (what a strategy's decide() may return, driving the
    # intent-coverage gate + SKILL surface). A cancel is a framework TEARDOWN-RECOVERY
    # verb — never authored by a strategy — so it lives on GMXV2Compiler.intents (the
    # compilation universe, which routes it) but not here. Do not "fix" this by adding it.
    strategy_intents=("PERP_OPEN", "PERP_CLOSE"),
    strategy_chains=("arbitrum", "avalanche"),
)

__all__ = ["CONNECTOR"]
