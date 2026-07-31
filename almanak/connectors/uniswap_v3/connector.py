"""Uniswap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    DexVolumeDecl,
    FeeModelDecl,
    ImportRef,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.avalanche import DESCRIPTOR as AVALANCHE
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.mantle import DESCRIPTOR as MANTLE
from almanak.core.chains.monad import DESCRIPTOR as MONAD
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
from almanak.core.chains.robinhood import DESCRIPTOR as ROBINHOOD
from almanak.core.chains.xlayer import DESCRIPTOR as XLAYER
from almanak.core.chains.zerog import DESCRIPTOR as ZEROG
from almanak.core.intent_types import IntentType

_V3_ABI_FAMILIES = (AbiFamily.V3_FACTORY, AbiFamily.V3_NPM)

#: Chains with a proven 4-layer LP lifecycle (LP_OPEN / LP_CLOSE /
#: LP_COLLECT_FEES) as well as SWAP.
_LP_CHAINS = (
    ETHEREUM,
    ARBITRUM,
    OPTIMISM,
    POLYGON,
    BASE,
    AVALANCHE,
    BSC,
    MONAD,
    ROBINHOOD,
)

#: SWAP-proven, LP-unproven. Each is a distinct deployment with a full address
#: table (factory / position_manager / quoter_v2 / swap_router / swap_router_02),
#: a receipt-parser NPM entry, and a maintained 4-layer SWAP suite:
#:   mantle -> tests/intents/mantle/test_uniswap_swap.py
#:   xlayer -> tests/intents/xlayer/test_uniswap_v3_swap.py
#:   zerog  -> tests/intents/zerog/test_jaine_swap.py  (JAINE DEX)
#: None has an LP suite, so their LP intents are withheld below. Dropping the
#: chains outright would strip proven SWAP coverage; publishing their LP intents
#: would re-create the "published but unproven" claim VIB-6231 exists to kill —
#: `check_intent_coverage --enforce` fails on all three LP verbs for them.
#: `linea` is in neither tuple: it has no suite for any verb, so it stays
#: unpublished until one exists.
_SWAP_ONLY_CHAINS = (MANTLE, XLAYER, ZEROG)

_VOLUME_SUBGRAPH_URLS = {
    "ethereum": "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    "arbitrum": "https://gateway.thegraph.com/api/subgraphs/id/FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    "base": "https://gateway.thegraph.com/api/subgraphs/id/96eJ9Go8gFjySRGnndG7EYxThaiwVDV8BYPp1TMDcoYh",
    # The V3-native optimism deployment (Cghf4LfV...) has sick indexers;
    # the healthy current deployment is Messari-standard — the gateway
    # provider and liquidity_query_family_overrides branch on it.
    "optimism": "https://gateway.thegraph.com/api/subgraphs/id/EgnS9YE1avupkvCNj9fHnJxppfEmNNywYJtghqiu2pd9",
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
        liquidity_subgraph_ids={chain: url.rsplit("/", 1)[-1] for chain, url in _VOLUME_SUBGRAPH_URLS.items()},
        liquidity_query_family_overrides={"optimism": "messari_standard"},
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
    backtest_strategy_type=BacktestStrategyTypeDecl(
        strategy_type="lp",
        aliases=("uniswap", "uniswap_v2"),
        lp_economic_family="concentrated",
        # uniswap_v2 pool shares are fungible — no range semantics; agni is a
        # v3 fork sharing this connector's address tables (strategies name it
        # by both the manifest key and the bare "agni").
        lp_economic_family_overrides={
            "uniswap_v2": "fungible",
            "agni_finance": "concentrated",
            "agni": "concentrated",
        },
    ),
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
    primitive=ImportRef(
        module="almanak.connectors.uniswap_v3.primitive",
        attribute="PRIMITIVE",
    ),
    strategy_intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
    supported_chains=SupportedChainsSpec(
        # SWAP spans both tuples; the LP lifecycle is restricted to _LP_CHAINS
        # by the overrides below. Composing rather than re-listing keeps the two
        # sets from drifting apart.
        chains=_LP_CHAINS + _SWAP_ONLY_CHAINS,
        intent_overrides={
            IntentType.LP_OPEN: _LP_CHAINS,
            IntentType.LP_CLOSE: _LP_CHAINS,
            IntentType.LP_COLLECT_FEES: _LP_CHAINS,
        },
        # Agni Finance is a Uniswap V3 fork on Mantle sharing this connector,
        # with its OWN address table (AGNI_FINANCE) distinct from the canonical
        # UNISWAP_V3["mantle"] deployment. The override pins the alias to
        # mantle alone rather than letting it inherit the union above.
        #
        # A protocol override REPLACES the chain set for that alias, so it is
        # deliberately not intersected with the LP overrides: mantle sits in
        # _SWAP_ONLY_CHAINS because the CANONICAL uniswap_v3 mantle deployment
        # has no proven LP suite, which says nothing about Agni's. Agni ships
        # its own NonfungiblePositionManager (AGNI_FINANCE["mantle"]) and its
        # LP lifecycle is proven on-chain by tests/intents/mantle/test_agni_lp.py
        # (LP_OPEN / LP_CLOSE). Intersecting here would delete proven coverage.
        protocol_overrides={"agni_finance": (MANTLE,)},
    ),
)

__all__ = ["CONNECTOR"]
