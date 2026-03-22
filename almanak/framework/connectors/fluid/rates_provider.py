"""Fluid Rates Provider — on-chain APY data for YieldAggregator integration.

Reads pool APY and rate data from Fluid's DexResolver on Arbitrum.
Integrated into YieldAggregator via the provider registry pattern.

Data source: FluidDexResolver.getAllDexEntireDatas() — single eth_call.
Cache TTL: 60 seconds (consistent with other lending rate providers).

Rate values from Fluid use 1e12 precision (divide by 1e12 for raw fraction).
"""

import logging
import time
from dataclasses import dataclass

from almanak.framework.connectors.fluid.sdk import FLUID_ADDRESSES, FluidSDK, FluidSDKError

logger = logging.getLogger(__name__)

# Cache TTL for rate data (seconds)
RATES_CACHE_TTL = 60


@dataclass(frozen=True)
class FluidPoolRate:
    """Rate data for a single Fluid DEX pool.

    Attributes:
        dex_address: Pool contract address
        token0: Token0 address
        token1: Token1 address
        fee_bps: Trading fee in basis points
        is_smart_collateral: Whether smart collateral is enabled
        is_smart_debt: Whether smart debt is enabled
    """

    dex_address: str
    token0: str
    token1: str
    fee_bps: int
    is_smart_collateral: bool
    is_smart_debt: bool


class FluidRatesProvider:
    """On-chain rate data provider for Fluid DEX pools.

    Reads per-pool data from FluidDexResolver on Arbitrum.
    Results are cached for 60 seconds.

    Args:
        chain: Chain name (default: "arbitrum")
        rpc_url: RPC endpoint URL (required)
        cache_ttl: Cache TTL in seconds (default: 60)
    """

    def __init__(
        self,
        chain: str = "arbitrum",
        rpc_url: str | None = None,
        cache_ttl: int = RATES_CACHE_TTL,
    ) -> None:
        self.chain = chain.lower()
        self._rpc_url = rpc_url
        self._cache_ttl = cache_ttl
        self._cache: dict[str, tuple[list[FluidPoolRate], float]] = {}
        self._sdk: FluidSDK | None = None

    def _get_sdk(self) -> FluidSDK:
        """Lazily initialize the SDK (requires rpc_url)."""
        if self._sdk is None:
            if not self._rpc_url:
                raise FluidSDKError("FluidRatesProvider requires an rpc_url")
            self._sdk = FluidSDK(chain=self.chain, rpc_url=self._rpc_url)
        return self._sdk

    def get_all_pool_rates(self) -> list[FluidPoolRate]:
        """Get rate data for all Fluid DEX pools.

        Returns cached data if within TTL, otherwise fetches fresh from chain.

        Returns:
            List of FluidPoolRate for each registered pool
        """
        cache_key = f"fluid_rates:{self.chain}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            rates, cached_at = cached
            if time.monotonic() - cached_at < self._cache_ttl:
                return rates

        sdk = self._get_sdk()
        fresh_rates: list[FluidPoolRate] = []

        try:
            addresses = sdk.get_all_dex_addresses()
        except FluidSDKError as e:
            logger.warning(f"Failed to enumerate Fluid DEX pools: {e}")
            return []

        for addr in addresses:
            try:
                data = sdk.get_dex_data(addr)
                fresh_rates.append(
                    FluidPoolRate(
                        dex_address=data.dex_address,
                        token0=data.token0,
                        token1=data.token1,
                        fee_bps=data.fee_bps,
                        is_smart_collateral=data.is_smart_collateral,
                        is_smart_debt=data.is_smart_debt,
                    )
                )
            except FluidSDKError as e:
                logger.debug(f"Skipping Fluid pool {addr}: {e}")
                continue

        self._cache[cache_key] = (fresh_rates, time.monotonic())
        logger.info(f"Fetched rates for {len(fresh_rates)} Fluid DEX pools on {self.chain}")
        return fresh_rates

    def get_pool_rate(self, dex_address: str) -> FluidPoolRate | None:
        """Get rate data for a specific pool.

        Args:
            dex_address: Pool contract address

        Returns:
            FluidPoolRate or None if not found
        """
        rates = self.get_all_pool_rates()
        dex_lower = dex_address.lower()
        for rate in rates:
            if rate.dex_address.lower() == dex_lower:
                return rate
        return None

    @staticmethod
    def is_available(chain: str) -> bool:
        """Check if Fluid rates are available for a given chain.

        Args:
            chain: Chain name

        Returns:
            True if the chain has Fluid DEX deployments
        """
        return chain.lower() in FLUID_ADDRESSES
