"""Connector-owned pool reader spec for Uniswap V3."""

from __future__ import annotations

from almanak.connectors._connector import ImportRef
from almanak.connectors._strategy_base.pool_data import (
    PoolDataFacet,
    PoolDataSource,
    PoolDataSpec,
    PoolReferenceKind,
)
from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors._strategy_base.v3_pool_abi import V3_GET_POOL_SELECTOR

from .addresses import AGNI_FINANCE, UNISWAP_V3

_KNOWN_POOLS: dict[str, dict[tuple[str, str, int], str]] = {
    "ethereum": {
        (
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            500,
        ): "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        (
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            3000,
        ): "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",
    },
    "arbitrum": {
        # Native-USDC fee-500 pool, verified on-chain 2026-07-16 (ALM-2947);
        # 0xC31E54c7 is the bridged USDC.e pool and was wrong under this key.
        (
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            500,
        ): "0xC6962004f452bE9203591991D15f6b388e09E8D0",
        (
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            3000,
        ): "0xc473e2aEE3441BF9240Be85eb122aBB059A3B57c",
    },
    "base": {
        (
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            500,
        ): "0xd0b53D9277642d899DF5C87A3966A349A798F224",
    },
}

POOL_READER_SPEC = PoolReaderSpec(
    protocol="uniswap_v3",
    factory_addresses={
        chain: deployment["factory"] for chain, deployment in UNISWAP_V3.items() if "factory" in deployment
    },
    reader=ImportRef(
        module="almanak.framework.data.pools.reader",
        attribute="UniswapV3PoolPriceReader",
    ),
    known_pools=_KNOWN_POOLS,
    get_pool_selector=V3_GET_POOL_SELECTOR,
)

_V3_BINDINGS = {
    PoolDataFacet.METADATA: PoolDataSource.GATEWAY_POOL_STATE,
    PoolDataFacet.BALANCES: PoolDataSource.GATEWAY_POOL_STATE,
    PoolDataFacet.SPOT_PRICE: PoolDataSource.LIVE_PRICE_READER,
    PoolDataFacet.LIQUIDITY: PoolDataSource.LIVE_PRICE_READER,
    PoolDataFacet.HISTORICAL_STATE: PoolDataSource.GATEWAY_POOL_STATE,
    PoolDataFacet.TWAP: PoolDataSource.GATEWAY_TWAP,
    PoolDataFacet.TICK_LIQUIDITY: PoolDataSource.LIVE_PRICE_READER,
}

POOL_DATA_SPEC = PoolDataSpec(
    protocol="uniswap_v3",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    bindings=_V3_BINDINGS,
    unsupported={},
    price_reader=POOL_READER_SPEC,
)

AGNI_POOL_READER_SPEC = PoolReaderSpec(
    protocol="agni_finance",
    factory_addresses={chain: addresses["factory"] for chain, addresses in AGNI_FINANCE.items()},
    reader=ImportRef(
        module="almanak.framework.data.pools.reader",
        attribute="UniswapV3PoolPriceReader",
    ),
    get_pool_selector=V3_GET_POOL_SELECTOR,
)

AGNI_POOL_DATA_SPEC = PoolDataSpec(
    protocol="agni_finance",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    bindings=_V3_BINDINGS,
    unsupported={},
    price_reader=AGNI_POOL_READER_SPEC,
)

POOL_DATA_SPECS = (POOL_DATA_SPEC, AGNI_POOL_DATA_SPEC)

__all__ = [
    "AGNI_POOL_DATA_SPEC",
    "AGNI_POOL_READER_SPEC",
    "POOL_DATA_SPEC",
    "POOL_DATA_SPECS",
    "POOL_READER_SPEC",
]
