"""Aerodrome connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    DexVolumeDecl,
    FeeModelDecl,
    FungibleLpCloseDecl,
    ImportRef,
    MetadataAmountEncoding,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.intent_types import IntentType

CONNECTOR = Connector(
    name="aerodrome",
    kind=ProtocolKind.LP,
    # DefiLlama's yields catalog tracks Aerodrome classic (Solidly vAMM/sAMM)
    # under project "aerodrome-v1"; "aerodrome-v2" exists in no DefiLlama
    # namespace (yields projects are aerodrome-v1 / aerodrome-slipstream, and
    # api.llama.fi/protocol/aerodrome-v2 is Protocol-not-found).
    external_ids={"defillama": "aerodrome-v1"},
    # VIB-6162. Classic Solidly pools are fungible: the POOL CONTRACT IS THE LP TOKEN,
    # so a close that withdraws `balanceOf(wallet)` burns any LP the wallet already
    # held -- a user's own position, or another deployment's -- and books the proceeds
    # as this strategy's PnL. Clamping bounds the close to this deployment's own
    # outstanding liquidity.
    #
    # `raw`/18: Aerodrome writes BASE UNITS into position_events.liquidity where Curve
    # writes token units. Same column, different units per connector -- verified on
    # captured rows, and not inferable from the JSON shape, which is why it is declared
    # here rather than guessed by the framework.
    #
    # Clamped even though `fungible_lp` is False. The two flags are independent: that
    # one auto-registers the framework-default teardown post-condition, whose closure
    # rule is `balanceOf <= 10 wei` and would report a CORRECT clamped close as FAILED.
    # Aerodrome has no such post-condition registered (VIB-6487), so the clamp is safe
    # here and is NOT yet safe on Curve (VIB-6489).
    fungible_lp_close=FungibleLpCloseDecl(
        units="raw",
        decimals=18,
        clamp=True,
        identity=ImportRef(
            module="almanak.connectors._strategy_base.fungible_lp_identity",
            attribute="canonical_pool_key",
        ),
    ),
    dex_volume=DexVolumeDecl(
        chains=("base",),
        amm_family="solidly_v2",
        chain_default=("base",),
        # Slipstream (CL) volume rides the same connector; the canonical slug
        # callers pass is "aerodrome_slipstream", so the DEX-volume caller path
        # (DexVolumeRegistry → MultiDEXVolumeProvider) resolves it here instead
        # of falling through to the LOW-confidence unknown-protocol fallback.
        aliases=("aerodrome_slipstream",),
        liquidity_subgraph_ids={
            "base": "GENunSHWLBXm59mBSgPzQ8metBEp9YDfdqwFr91Av1UM",
        },
        # The declared subgraph is a uniswap-v3 fork exposing poolDayDatas;
        # the solidly pairDayDatas query hard-errors against it (ALM-2930).
        liquidity_query_family="v3_concentrated",
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.aerodrome.fee_model", attribute="AerodromeFeeModel"),
        description="Aerodrome DEX fee model with stable/volatile pool distinction",
        aliases=("aero", "velodrome"),
    ),
    # Velodrome (the Optimism original Aerodrome forked) has no connector
    # package; this folder owns its backtest detection key, mirroring the
    # fee-model alias above.
    backtest_strategy_type=BacktestStrategyTypeDecl(
        strategy_type="lp",
        aliases=("velodrome",),
        # Plain aerodrome/velodrome pools are solidly-style fungible shares;
        # the slipstream product is concentrated liquidity.
        lp_economic_family="fungible",
        lp_economic_family_overrides={"aerodrome_slipstream": "concentrated", "velodrome_slipstream": "concentrated"},
    ),
    aliases=("aerodrome_slipstream",),
    address_tables=(
        AddressTableSpec(
            protocol="aerodrome",
            module="almanak.connectors.aerodrome.addresses",
            attribute="AERODROME",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.aerodrome.gateway.provider",
        attribute="AerodromeGatewayConnector",
        order=13,
    ),
    pool_reader=ImportRef(
        module="almanak.connectors.aerodrome.pool_reader",
        attribute="POOL_READER_SPEC",
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.aerodrome.agent_read_provider",
        attribute="AerodromeSlipstreamAgentReadConnector",
        order=3,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.aerodrome.receipt_parser_provider",
        attribute="AerodromeReceiptParserConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.aerodrome.swap_quote_provider",
        attribute="AerodromeSwapQuoteConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.aerodrome.contract_monitoring",
        attribute="AERODROME_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.aerodrome.contract_roles",
        attribute="CONTRACT_ROLES",
        order=5,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.aerodrome.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    compiler=ImportRef(
        module="almanak.connectors.aerodrome.compiler",
        attribute="AerodromeCompiler",
    ),
    primitive=ImportRef(
        module="almanak.connectors.aerodrome.primitive",
        attribute="PRIMITIVE",
    ),
    # Aerodrome's SWAP compiler ships amount_in as a human-readable Decimal (VIB-3747).
    metadata_amount_encoding=MetadataAmountEncoding(swap="human"),
    strategy_intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE),
    supported_chains=SupportedChainsSpec(
        chains=(BASE, OPTIMISM),
        # Slipstream (concentrated liquidity) is deployed on Base only. Optimism
        # is Velodrome V2 — the Solidly-fork Classic pools — and `addresses.py`
        # carries no `cl_*` entries for it: no cl_factory, no cl_nft
        # (NonfungiblePositionManager), no cl_quoter. Without the override the
        # alias inherits the connector union and publishes an optimism row that
        # cannot compile an LP_OPEN. Optimism's concentrated venue is Velodrome
        # Slipstream, which has its own row.
        protocol_overrides={"aerodrome_slipstream": (BASE,)},
    ),
)

__all__ = ["CONNECTOR"]
