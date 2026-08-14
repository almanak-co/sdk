"""Uniswap V4 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    ImportRef,
    StrategyMatrixEntry,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.avalanche import DESCRIPTOR as AVALANCHE
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
from almanak.core.intent_types import IntentType

CONNECTOR = Connector(
    name="uniswap_v4",
    kind=ProtocolKind.LP,
    address_tables=(
        AddressTableSpec(
            protocol="uniswap_v4",
            module="almanak.connectors.uniswap_v4.addresses",
            attribute="UNISWAP_V4",
            # VIB-6109: teardown LP-discovery recovery walks the V4 PositionManager
            # family by verifying deployment-owned candidate token ids on-chain
            # (V4 is NOT ERC721Enumerable, so the V3 wallet-scan cannot enumerate
            # it). Declared here so ``discovery.py`` resolves V4 PMs via the
            # registry — never a hard-coded framework-side address.
            abi_families=(AbiFamily.V4_PM,),
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.gateway.provider",
        attribute="UniswapV4GatewayConnector",
        order=1,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.receipt_parser_provider",
        attribute="UniswapV4ReceiptParserConnector",
    ),
    # The V4 receipt parser resolves ModifyLiquidity.pool_id -> canonical
    # PoolKey via the gateway; the enricher threads this kwarg only to
    # parsers that declare it (VIB-4477 T08).
    receipt_parser_kwargs=("pool_key_lookup",),
    contract_monitoring=ImportRef(
        module="almanak.connectors.uniswap_v4.contract_monitoring",
        attribute="UNISWAP_V4_CONTRACT_MONITORING_SPECS",
    ),
    runner_hook_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.runner_hooks",
        attribute="UniswapV4RunnerHookConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.swap_quote_provider",
        attribute="UniswapV4SwapQuoteConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.uniswap_v4.contract_roles",
        attribute="CONTRACT_ROLES",
        order=2,
    ),
    compiler=ImportRef(
        module="almanak.connectors.uniswap_v4.compiler",
        attribute="UniswapV4Compiler",
    ),
    # StateView-backed pool-data spec: V4 pool state lives in the PoolManager
    # singleton and is keyed by bytes32 PoolId, never a per-pool contract.
    pool_data=ImportRef(
        module="almanak.connectors.uniswap_v4.pool_reader",
        attribute="POOL_DATA_SPEC",
    ),
    # VIB-5634: TD-14 on-chain closure verifier. Registered under the connector's
    # slugs by the framework manifest loader; the LP_V4 primitive-label alias
    # (registry-derived positions carry ``protocol='lp_v4'``) is added generically
    # framework-side. Without this, a closed V4 LP had no post-condition and was
    # mis-reported UNVERIFIED / FAILED instead of CHAIN_VERIFIED.
    teardown_post_condition=ImportRef(
        module="almanak.connectors.uniswap_v4.teardown_post_condition",
        attribute="uniswap_v4_post_condition",
    ),
    primitive=ImportRef(
        module="almanak.connectors.uniswap_v4.primitive",
        attribute="PRIMITIVE",
    ),
    # VIB-4583: declares membership in the ``UNIV4_LP_GROUPING`` family so the
    # migration backfill / runner registry dispatch resolve V4 LP grouping
    # through ``PROTOCOL_FAMILY_REGISTRY`` without naming this connector.
    protocol_family=ImportRef(
        module="almanak.connectors.uniswap_v4.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    strategy_intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
    # Backtests as "lp"; V4 positions are tick-ranged concentrated liquidity
    # (hooks change fee routing, not range economics). Historical-data lanes
    # for V4 are a separate, still-open support question.
    backtest_strategy_type=BacktestStrategyTypeDecl(
        strategy_type="lp",
        # Family-only: this venue declares SWAP in strategy_intents, so
        # swap-only strategies on it are legal — joining protocol-name
        # detection (which runs before intent detection) would reroute them
        # to the LP adapter.
        detection=False,
        lp_economic_family="concentrated",
    ),
    # VIB-4421: extended from ("ethereum", "arbitrum", "base") to the full
    # deployed set so the registry's (connector, intent, chain) universe matches
    # ``UNISWAP_V4`` in addresses.py and the 28 on-chain intent tests (7 chains x
    # 4 intents). ChainRegistry canonical names are used (``bsc``, not the
    # ``bnb`` alias).
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM, BASE, OPTIMISM, POLYGON, AVALANCHE, BSC)),
    # Matrix output covers deployed V4 chains for both swap and LP rows.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="uniswap_v4",
            category="swap",
            intents=(IntentType.SWAP,),
        ),
        StrategyMatrixEntry(
            matrix_name="uniswap_v4",
            category="lp",
            intents=(IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
        ),
    ),
)

__all__ = ["CONNECTOR"]
