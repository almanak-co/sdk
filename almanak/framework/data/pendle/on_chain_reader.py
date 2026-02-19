"""On-chain reader for Pendle RouterStatic contract.

Provides fallback pricing when the Pendle REST API is unavailable.
Reads PT-to-asset rate, implied APY, and market expiry directly from
the RouterStatic contract via RPC calls.
"""

import logging
import threading
import time
from decimal import Decimal
from typing import Any

from web3 import Web3

logger = logging.getLogger(__name__)

# RouterStatic addresses per chain
ROUTER_STATIC_ADDRESSES: dict[str, str] = {
    "ethereum": "0x263833d47eA3fA4a30f269323aba6a107f9eB14C",
    "arbitrum": "0xAdB09F65bd90d19e3148DB7B340e4B65d6063a90",
}

# Minimal ABI for RouterStatic read methods
ROUTER_STATIC_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "market", "type": "address"}],
        "name": "getPtToAssetRate",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "market", "type": "address"}],
        "name": "getImpliedApy",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "market", "type": "address"}],
        "name": "readTokens",
        "outputs": [
            {"internalType": "address", "name": "sy", "type": "address"},
            {"internalType": "address", "name": "pt", "type": "address"},
            {"internalType": "address", "name": "yt", "type": "address"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

# Market expiry ABI (on the market contract itself)
MARKET_EXPIRY_ABI = [
    {
        "inputs": [],
        "name": "expiry",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]


class PendleOnChainError(Exception):
    """Raised when an on-chain read fails."""


class PendleOnChainReader:
    """Reads Pendle market data directly from on-chain contracts.

    Used as a fallback when the Pendle REST API is unavailable.
    All reads are via the RouterStatic contract which provides
    view-only aggregation functions.

    Example:
        reader = PendleOnChainReader(rpc_url="https://eth.llamarpc.com", chain="ethereum")
        rate = reader.get_pt_to_asset_rate("0x...")
        print(f"PT/Asset rate: {rate}")
    """

    def __init__(
        self,
        rpc_url: str,
        chain: str = "ethereum",
        cache_ttl_seconds: float = 30.0,
    ):
        """Initialize the on-chain reader.

        Args:
            rpc_url: RPC endpoint URL
            chain: Target chain (ethereum, arbitrum)
            cache_ttl_seconds: Cache TTL for on-chain reads
        """
        if chain not in ROUTER_STATIC_ADDRESSES:
            raise ValueError(
                f"Unsupported chain for Pendle on-chain reads: {chain}. Supported: {list(ROUTER_STATIC_ADDRESSES.keys())}"
            )

        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        self.chain = chain
        self.cache_ttl = cache_ttl_seconds

        self.router_static_address = ROUTER_STATIC_ADDRESSES[chain]
        self.router_static = self.web3.eth.contract(
            address=self.web3.to_checksum_address(self.router_static_address),
            abi=ROUTER_STATIC_ABI,
        )

        # Simple TTL cache
        self._cache: dict[str, tuple[Any, float]] = {}
        self._cache_lock = threading.Lock()

        logger.info(f"PendleOnChainReader initialized: chain={chain}, router_static={self.router_static_address}")

    def get_pt_to_asset_rate(self, market_address: str) -> Decimal:
        """Get the PT-to-underlying-asset exchange rate.

        This is the key pricing function: it returns how much underlying
        asset 1 PT is worth. Before maturity, this is typically < 1.0
        (PT trades at a discount). At maturity, it converges to 1.0.

        Args:
            market_address: Market contract address

        Returns:
            Exchange rate as Decimal (in 1e18 scale, normalized to human-readable)

        Raises:
            PendleOnChainError: If the RPC call fails
        """
        cache_key = f"pt_rate:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            raw_rate = self.router_static.functions.getPtToAssetRate(
                self.web3.to_checksum_address(market_address)
            ).call()
            # Rate is in 1e18 scale
            rate = Decimal(str(raw_rate)) / Decimal("1000000000000000000")
            self._set_cached(cache_key, rate)
            return rate
        except Exception as e:
            logger.warning(f"Failed to read PT-to-asset rate for {market_address}: {e}")
            raise PendleOnChainError(f"getPtToAssetRate failed: {e}") from e

    def get_implied_apy(self, market_address: str) -> Decimal:
        """Get the implied APY for a market.

        Args:
            market_address: Market contract address

        Returns:
            Implied APY as Decimal (e.g., 0.05 = 5%)

        Raises:
            PendleOnChainError: If the RPC call fails
        """
        cache_key = f"implied_apy:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            raw_apy = self.router_static.functions.getImpliedApy(self.web3.to_checksum_address(market_address)).call()
            # APY is in 1e18 scale
            apy = Decimal(str(raw_apy)) / Decimal("1000000000000000000")
            self._set_cached(cache_key, apy)
            return apy
        except Exception as e:
            logger.warning(f"Failed to read implied APY for {market_address}: {e}")
            raise PendleOnChainError(f"getImpliedApy failed: {e}") from e

    def is_market_expired(self, market_address: str) -> bool:
        """Check if a market has expired.

        Args:
            market_address: Market contract address

        Returns:
            True if the market has expired

        Raises:
            PendleOnChainError: If the RPC call fails
        """
        cache_key = f"expiry:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            market_contract = self.web3.eth.contract(
                address=self.web3.to_checksum_address(market_address),
                abi=MARKET_EXPIRY_ABI,
            )
            expiry = market_contract.functions.expiry().call()
            current_time = int(time.time())
            is_expired = current_time >= expiry
            self._set_cached(cache_key, is_expired)
            return is_expired
        except Exception as e:
            logger.warning(f"Failed to read market expiry for {market_address}: {e}")
            raise PendleOnChainError(f"expiry() failed: {e}") from e

    def get_market_tokens(self, market_address: str) -> dict[str, str]:
        """Get SY, PT, and YT addresses for a market.

        Args:
            market_address: Market contract address

        Returns:
            Dict with keys "sy", "pt", "yt" mapping to addresses

        Raises:
            PendleOnChainError: If the RPC call fails
        """
        cache_key = f"tokens:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            sy, pt, yt = self.router_static.functions.readTokens(self.web3.to_checksum_address(market_address)).call()
            result = {
                "sy": sy.lower(),
                "pt": pt.lower(),
                "yt": yt.lower(),
            }
            self._set_cached(cache_key, result)
            return result
        except Exception as e:
            logger.warning(f"Failed to read market tokens for {market_address}: {e}")
            raise PendleOnChainError(f"readTokens failed: {e}") from e

    def estimate_pt_output(self, market_address: str, amount_in: int) -> int:
        """Estimate PT output for a given input amount using the on-chain rate.

        Args:
            market_address: Market contract address
            amount_in: Input amount in wei

        Returns:
            Estimated PT output in wei
        """
        rate = self.get_pt_to_asset_rate(market_address)
        if rate <= 0:
            raise PendleOnChainError(f"Invalid PT rate for {market_address}: {rate}")
        # amount_out = amount_in / rate (since rate is asset-per-PT)
        return int(Decimal(str(amount_in)) / rate)

    # =========================================================================
    # Cache
    # =========================================================================

    def _get_cached(self, key: str) -> Any | None:
        with self._cache_lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.monotonic() > expiry:
                del self._cache[key]
                return None
            return value

    def _set_cached(self, key: str, value: Any) -> None:
        with self._cache_lock:
            self._cache[key] = (value, time.monotonic() + self.cache_ttl)

    def clear_cache(self) -> None:
        with self._cache_lock:
            self._cache.clear()
