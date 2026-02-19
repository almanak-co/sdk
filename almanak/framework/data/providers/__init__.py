"""Data Providers - External API adapters for market data.

Providers:
    - DefiLlamaProvider: TVL, yield, and historical price data from DeFi Llama
"""

from almanak.framework.data.providers.defillama_provider import DefiLlamaProvider

__all__ = [
    "DefiLlamaProvider",
]
