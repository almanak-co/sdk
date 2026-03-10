"""Pendle data layer for pricing, market data, and on-chain reads.

Provides:
- PendleAPIClient: REST API wrapper for Pendle v3 API
- PendleMarketResolver: Dynamic market discovery (replaces hardcoded dicts)
- PendleOnChainReader: RouterStatic fallback for on-chain reads
- Data models: PendleMarketData, PendleSwapQuote, PendleAsset
"""

from .api_client import PendleAPIClient
from .models import PendleAsset, PendleMarketData, PendleSwapQuote
from .on_chain_reader import PendleOnChainReader
from .resolver import PendleMarketResolver

__all__ = [
    "PendleAPIClient",
    "PendleAsset",
    "PendleMarketData",
    "PendleMarketResolver",
    "PendleOnChainReader",
    "PendleSwapQuote",
]
