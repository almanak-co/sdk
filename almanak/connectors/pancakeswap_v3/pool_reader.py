"""Connector-owned pool reader spec for PancakeSwap V3."""

from __future__ import annotations

from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors._strategy_base.v3_pool_abi import V3_GET_POOL_SELECTOR

from .addresses import PANCAKESWAP_V3

_FACTORY_CHAINS = ("ethereum", "arbitrum", "base", "bsc")

_KNOWN_POOLS: dict[str, dict[tuple[str, str, int], str]] = {
    "arbitrum": {
        (
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            500,
        ): "0xd9E2A1A61B6e61b275ceC326465D417E52c1A621",
    },
    "ethereum": {
        (
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            500,
        ): "0x1ac1A8FEaAEa1900C4166dEeed0C11cC10669D36",
    },
}

POOL_READER_SPEC = PoolReaderSpec(
    protocol="pancakeswap_v3",
    factory_addresses={chain: PANCAKESWAP_V3[chain]["factory"] for chain in _FACTORY_CHAINS},
    known_pools=_KNOWN_POOLS,
    get_pool_selector=V3_GET_POOL_SELECTOR,
)

__all__ = ["POOL_READER_SPEC"]
