"""On-chain reader for Pendle RouterStatic contract.

Provides fallback pricing when the Pendle REST API is unavailable.
Reads PT-to-asset rate, implied APY, and market expiry directly from
the RouterStatic contract via RPC calls.

Supports two modes:
- **Gateway mode** (preferred): Routes reads through the gateway's RpcService
  using raw eth_call, respecting the gateway-only architecture.
- **Direct mode** (legacy): Uses web3.py with a direct RPC URL. Only for
  local development or when no gateway is available.
"""

import json
import logging
import threading
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

# RouterStatic addresses per chain
ROUTER_STATIC_ADDRESSES: dict[str, str] = {
    "ethereum": "0x263833d47eA3fA4a30f269323aba6a107f9eB14C",
    "arbitrum": "0xAdB09F65bd90d19e3148DB7B340e4B65d6063a90",
}

# Function selectors (keccak256 of canonical signatures, first 4 bytes)
GET_PT_TO_ASSET_RATE_SELECTOR = "0xf2344deb"  # getPtToAssetRate(address)
GET_IMPLIED_APY_SELECTOR = "0xfc0e022c"  # getImpliedApy(address)
READ_TOKENS_SELECTOR = "0x61d725ab"  # readTokens(address)
EXPIRY_SELECTOR = "0xe184c9be"  # expiry()

# Minimal ABI for RouterStatic read methods (only used in direct/web3 mode)
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

# Market expiry ABI (on the market contract itself, only used in direct mode)
MARKET_EXPIRY_ABI = [
    {
        "inputs": [],
        "name": "expiry",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

SCALE_1E18 = Decimal("1000000000000000000")


def _encode_address(addr: str) -> str:
    """ABI-encode an address as a 32-byte hex string (no 0x prefix)."""
    return addr.lower().removeprefix("0x").zfill(64)


def _decode_uint256(hex_str: str) -> int:
    """Decode a hex string to a uint256."""
    raw = hex_str.removeprefix("0x")
    return int(raw, 16) if raw else 0


def _decode_address(hex_str: str) -> str:
    """Decode a 32-byte hex slot to a checksummed address."""
    raw = hex_str.removeprefix("0x")
    # Address is in the last 40 chars of a 64-char slot
    return "0x" + raw[-40:]


class PendleOnChainError(Exception):
    """Raised when an on-chain read fails."""


class PendleOnChainReader:
    """Reads Pendle market data directly from on-chain contracts.

    Used as a fallback when the Pendle REST API is unavailable.
    All reads are via the RouterStatic contract which provides
    view-only aggregation functions.

    Supports two initialization modes:

    Gateway mode (preferred for production):
        reader = PendleOnChainReader(gateway_client=client, chain="ethereum")

    Direct mode (legacy, local development):
        reader = PendleOnChainReader(rpc_url="https://...", chain="ethereum")
    """

    def __init__(
        self,
        rpc_url: str | None = None,
        chain: str = "ethereum",
        cache_ttl_seconds: float = 30.0,
        gateway_client: "GatewayClient | None" = None,
    ):
        """Initialize the on-chain reader.

        Args:
            rpc_url: RPC endpoint URL (for direct/web3 mode).
            chain: Target chain (ethereum, arbitrum).
            cache_ttl_seconds: Cache TTL for on-chain reads.
            gateway_client: Gateway client (for gateway mode). Preferred over rpc_url.

        Raises:
            ValueError: If chain is unsupported or neither rpc_url nor gateway_client provided.
        """
        if chain not in ROUTER_STATIC_ADDRESSES:
            raise ValueError(
                f"Unsupported chain for Pendle on-chain reads: {chain}. Supported: {list(ROUTER_STATIC_ADDRESSES.keys())}"
            )

        self.chain = chain
        self.cache_ttl = cache_ttl_seconds
        self.router_static_address = ROUTER_STATIC_ADDRESSES[chain]
        self._gateway_client = gateway_client

        # Simple TTL cache
        self._cache: dict[str, tuple[Any, float]] = {}
        self._cache_lock = threading.Lock()

        if gateway_client is not None:
            # Gateway mode: no web3 dependency needed
            self.web3 = None
            self.router_static = None
            logger.info("PendleOnChainReader initialized (gateway mode): chain=%s", chain)
        elif rpc_url is not None:
            # Direct/web3 mode (legacy)
            from web3 import Web3

            self.web3 = Web3(Web3.HTTPProvider(rpc_url))
            self.router_static = self.web3.eth.contract(
                address=self.web3.to_checksum_address(self.router_static_address),
                abi=ROUTER_STATIC_ABI,
            )
            logger.info("PendleOnChainReader initialized (direct mode): chain=%s", chain)
        else:
            raise ValueError("Either rpc_url or gateway_client must be provided")

    def _gateway_eth_call(self, to: str, data: str, request_id: str) -> str:
        """Make an eth_call through the gateway's RPC service.

        Args:
            to: Contract address.
            data: Encoded calldata (hex string with 0x prefix).
            request_id: Request identifier for logging.

        Returns:
            Hex result string from the call.

        Raises:
            PendleOnChainError: If the RPC call fails.
        """
        from almanak.gateway.proto import gateway_pb2

        assert self._gateway_client is not None  # guaranteed by callers

        params = json.dumps([{"to": to, "data": data}, "latest"])
        try:
            resp = self._gateway_client.rpc.Call(
                gateway_pb2.RpcRequest(
                    chain=self.chain,
                    method="eth_call",
                    params=params,
                    id=request_id,
                ),
                timeout=30.0,
            )
        except Exception as e:
            raise PendleOnChainError(f"Gateway RPC call failed ({request_id}): {e}") from e

        if not resp.success:
            raise PendleOnChainError(f"Gateway RPC call error ({request_id}): {resp.error}")

        result = json.loads(resp.result)
        if result is None or result == "0x":
            raise PendleOnChainError(f"Empty result from gateway RPC ({request_id})")
        return result

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
            if self._gateway_client is not None:
                calldata = GET_PT_TO_ASSET_RATE_SELECTOR + _encode_address(market_address)
                result = self._gateway_eth_call(self.router_static_address, calldata, "pendle_pt_rate")
                raw_rate = _decode_uint256(result)
            else:
                assert self.web3 is not None and self.router_static is not None
                raw_rate = self.router_static.functions.getPtToAssetRate(
                    self.web3.to_checksum_address(market_address)
                ).call()
            rate = Decimal(str(raw_rate)) / SCALE_1E18
            self._set_cached(cache_key, rate)
            return rate
        except PendleOnChainError:
            raise
        except Exception as e:
            logger.warning("Failed to read PT-to-asset rate for %s: %s", market_address, e)
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
            if self._gateway_client is not None:
                calldata = GET_IMPLIED_APY_SELECTOR + _encode_address(market_address)
                result = self._gateway_eth_call(self.router_static_address, calldata, "pendle_implied_apy")
                raw_apy = _decode_uint256(result)
            else:
                assert self.web3 is not None and self.router_static is not None
                raw_apy = self.router_static.functions.getImpliedApy(
                    self.web3.to_checksum_address(market_address)
                ).call()
            apy = Decimal(str(raw_apy)) / SCALE_1E18
            self._set_cached(cache_key, apy)
            return apy
        except PendleOnChainError:
            raise
        except Exception as e:
            logger.warning("Failed to read implied APY for %s: %s", market_address, e)
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
            if self._gateway_client is not None:
                result = self._gateway_eth_call(market_address, EXPIRY_SELECTOR, "pendle_expiry")
                expiry = _decode_uint256(result)
            else:
                assert self.web3 is not None and self.router_static is not None
                market_contract = self.web3.eth.contract(
                    address=self.web3.to_checksum_address(market_address),
                    abi=MARKET_EXPIRY_ABI,
                )
                expiry = market_contract.functions.expiry().call()
            current_time = int(time.time())
            is_expired = current_time >= expiry
            self._set_cached(cache_key, is_expired)
            return is_expired
        except PendleOnChainError:
            raise
        except Exception as e:
            logger.warning("Failed to read market expiry for %s: %s", market_address, e)
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
            if self._gateway_client is not None:
                calldata = READ_TOKENS_SELECTOR + _encode_address(market_address)
                raw = self._gateway_eth_call(self.router_static_address, calldata, "pendle_read_tokens")
                hex_data = raw.removeprefix("0x")
                if len(hex_data) < 192:
                    raise PendleOnChainError(f"readTokens returned unexpected data length: {len(hex_data)}")
                result = {
                    "sy": _decode_address(hex_data[0:64]),
                    "pt": _decode_address(hex_data[64:128]),
                    "yt": _decode_address(hex_data[128:192]),
                }
            else:
                assert self.web3 is not None and self.router_static is not None
                sy, pt, yt = self.router_static.functions.readTokens(
                    self.web3.to_checksum_address(market_address)
                ).call()
                result = {
                    "sy": sy.lower(),
                    "pt": pt.lower(),
                    "yt": yt.lower(),
                }
            self._set_cached(cache_key, result)
            return result
        except PendleOnChainError:
            raise
        except Exception as e:
            logger.warning("Failed to read market tokens for %s: %s", market_address, e)
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
