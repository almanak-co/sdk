"""Uniswap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    DexVolumeDecl,
    FeeModelDecl,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec

_V3_ABI_FAMILIES = (AbiFamily.V3_FACTORY, AbiFamily.V3_NPM)

_VOLUME_SUBGRAPH_URLS = {
    "ethereum": "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    "arbitrum": "https://gateway.thegraph.com/api/subgraphs/id/FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    "base": "https://gateway.thegraph.com/api/subgraphs/id/96eJ9Go8gFjySRGnndG7EYxThaiwVDV8BYPp1TMDcoYh",
    "optimism": "https://gateway.thegraph.com/api/subgraphs/id/Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj",
    "polygon": "https://gateway.thegraph.com/api/subgraphs/id/3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
}

CONNECTOR = Connector(
    name="uniswap_v3",
    kind=ProtocolKind.LP,
    external_ids={"defillama": "uniswap-v3"},
    dex_volume=DexVolumeDecl(
        chains=("ethereum", "arbitrum", "base", "optimism", "polygon"),
        amm_family="v3_concentrated",
        aliases=("uni_v3",),
        generic_default=True,
        twap_reference_pools=ImportRef(
            module="almanak.connectors.uniswap_v3.backtest_pools",
            attribute="TWAP_REFERENCE_POOLS",
        ),
        # Decentralised TheGraph gateway endpoints for SubgraphVolumeProvider
        # (plan 024). IDs are byte-identical to _UNISWAP_V3_VOLUME_SUBGRAPH_IDS
        # in almanak/connectors/uniswap_v3/gateway/provider.py — ID-parity test
        # in tests/unit/connectors/uniswap_v3/test_subgraph_url_parity.py pins this.
        volume_subgraph_urls=_VOLUME_SUBGRAPH_URLS,
        # Free hosted-service fallback endpoints (no API key required — 4 chains;
        # base is not available on the hosted service).
        hosted_volume_subgraph_urls={
            "ethereum": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
            "arbitrum": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-arbitrum-one",
            "optimism": "https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis",
            "polygon": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
        },
        liquidity_subgraph_ids={chain: url.rsplit("/", 1)[-1] for chain, url in _VOLUME_SUBGRAPH_URLS.items()},
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.uniswap_v3.fee_model", attribute="UniswapV3FeeModel"),
        description="Uniswap V3 DEX fee model with tier-based fees",
        aliases=("uniswap", "uni_v3"),
    ),
    # Also a swap venue, but backtests as "lp". This folder owns the Uniswap
    # family's detection keys: the bare "uniswap" (fee-model precedent above)
    # and "uniswap_v2", which has no connector package (aave_v3 lending_read
    # claims "aave_v2" the same way).
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lp", aliases=("uniswap", "uniswap_v2")),
    aliases=("agni_finance",),
    address_tables=(
        AddressTableSpec(
            protocol="uniswap_v3",
            module="almanak.connectors.uniswap_v3.addresses",
            attribute="UNISWAP_V3",
            abi_families=_V3_ABI_FAMILIES,
            abi_family_order=1,
        ),
        AddressTableSpec(
            protocol="agni_finance",
            module="almanak.connectors.uniswap_v3.addresses",
            attribute="AGNI_FINANCE",
            abi_families=_V3_ABI_FAMILIES,
            abi_family_order=2,
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.gateway.provider",
        attribute="UniswapV3GatewayConnector",
        order=12,
    ),
    gateway_connectors=(
        ImportRef(
            module="almanak.connectors.uniswap_v3.gateway.agni_provider",
            attribute="AgniFinanceGatewayConnector",
            order=26,
        ),
    ),
    gas_estimate_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.gas_estimate_provider",
        attribute="UniswapV3GasEstimateConnector",
    ),
    pool_reader=ImportRef(
        module="almanak.connectors.uniswap_v3.pool_reader",
        attribute="POOL_READER_SPEC",
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.agent_read_provider",
        attribute="UniswapV3AgentReadConnector",
        order=1,
    ),
    agent_read_connectors=(
        ImportRef(
            module="almanak.connectors.uniswap_v3.agent_read_provider",
            attribute="AgniFinanceAgentReadConnector",
            order=2,
        ),
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.receipt_parser_provider",
        attribute="UniswapV3ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.uniswap_v3.contract_monitoring",
        attribute="UNISWAP_V3_CONTRACT_MONITORING_SPECS",
    ),
    runner_hook_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.runner_hooks",
        attribute="UniswapV3RunnerHookConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.swap_quote_provider",
        attribute="UniswapV3SwapQuoteConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.uniswap_v3.contract_roles",
        attribute="CONTRACT_ROLES",
        order=1,
    ),
    swap_classification=ImportRef(
        module="almanak.connectors.uniswap_v3.swap_classification",
        attribute="SWAP_CLASSIFICATION",
        order=1,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.uniswap_v3.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    compiler=ImportRef(
        module="almanak.connectors.uniswap_v3.compiler",
        attribute="UniswapV3Compiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("uniswap_v3",),
        module="almanak.connectors.uniswap_v3.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("uniswap_v3", "agni_finance"),
        module="almanak.connectors.uniswap_v3.supported_chains",
    ),
    primitive=ImportRef(
        module="almanak.connectors.uniswap_v3.primitive",
        attribute="PRIMITIVE",
    ),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
    strategy_chains=(
        "ethereum",
        "arbitrum",
        "optimism",
        "polygon",
        "base",
        "avalanche",
        "bsc",
        "monad",
        "robinhood",
    ),
)

__all__ = ["CONNECTOR"]
