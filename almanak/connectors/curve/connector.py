"""Curve connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    DexVolumeDecl,
    FeeModelDecl,
    ImportRef,
    MetadataAmountEncoding,
)

CONNECTOR = Connector(
    name="curve",
    kind=ProtocolKind.LP,
    dex_volume=DexVolumeDecl(
        chains=("ethereum", "optimism"),
        amm_family="stableswap",
        aliases=("crv",),
        volume_data_source="curve_messari_subgraph",
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.curve.fee_model", attribute="CurveFeeModel"),
        description="Curve Finance DEX fee model with dynamic fee calculation",
        aliases=("curve_fi", "crv"),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lp"),
    gateway_connector=ImportRef(
        module="almanak.connectors.curve.gateway.provider",
        attribute="CurveGatewayConnector",
        order=24,
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.curve.swap_quote_provider",
        attribute="CurveSwapQuoteConnector",
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.curve.receipt_parser_provider",
        attribute="CurveReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.curve.compiler",
        attribute="CurveCompiler",
    ),
    # Curve's SWAP compiler ships amount_in as a human-readable Decimal (VIB-3747).
    metadata_amount_encoding=MetadataAmountEncoding(swap="human"),
    # Curve LP positions are fungible ERC20 LP tokens: LPCloseIntent.position_id
    # is overloaded as the burn AMOUNT, never an NFT discriminator (VIB-4968).
    fungible_lp=True,
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE"),
    strategy_chains=("ethereum", "arbitrum", "optimism", "polygon", "base"),
)

__all__ = ["CONNECTOR"]
