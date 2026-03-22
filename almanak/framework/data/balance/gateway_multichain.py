"""Multi-Chain Gateway Balance Provider.

Provides balance queries across multiple chains via the gateway's
MarketService, replacing direct Web3 calls in multi-chain strategies.

Includes retry with exponential backoff and cached fallback for resilience
against RPC rate limiting (VIB-1712).

Example:
    from almanak.framework.data.balance.gateway_multichain import MultiChainGatewayBalanceProvider

    provider = MultiChainGatewayBalanceProvider(
        client=gateway_client,
        wallet_address="0x1234...",
        chains=["arbitrum", "base"],
    )
    balance = provider("USDC", "base")
"""

import logging
import time
from decimal import Decimal

from almanak.framework.data.balance.gateway_provider import _BACKOFF_BASE_SECONDS, _MAX_RETRIES, _is_retryable
from almanak.framework.gateway_client import GatewayClient
from almanak.framework.strategies.intent_strategy import TokenBalance
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


class MultiChainGatewayBalanceProvider:
    """Balance provider using gateway for multi-chain balance queries.

    Implements the MultiChainBalanceProvider callable interface:
        Callable[[str, str], TokenBalance]

    All balance queries are routed through the gateway's MarketService,
    so the CLI process never makes direct RPC calls.

    Resilience features (VIB-1712):
    - Retries up to 3 times with exponential backoff on transient RPC errors.
    - Caches successful reads with a configurable TTL (default 30s).
    - On total failure, returns last-known cached balance. If no cache exists,
      returns a zero balance (preserving the original error-swallowing contract).
    """

    def __init__(
        self,
        client: GatewayClient,
        wallet_address: str,
        chains: list[str],
        cache_ttl: float = 30.0,
    ) -> None:
        """Initialize the multi-chain gateway balance provider.

        Args:
            client: Connected GatewayClient instance
            wallet_address: Wallet address to query balances for
            chains: List of supported chain names
            cache_ttl: How long (seconds) cached balances remain valid. Default 30s.
        """
        self._client = client
        self._wallet_address = wallet_address
        self._chains = [c.lower() for c in chains]
        self._cache_ttl = cache_ttl
        # Cache: (token, chain) -> (TokenBalance, monotonic_timestamp)
        self._cache: dict[tuple[str, str], tuple[TokenBalance, float]] = {}

        logger.info(f"MultiChainGatewayBalanceProvider initialized for chains: {self._chains}")

    def get_balance(self, token: str, chain: str) -> TokenBalance:
        """Get balance for a token on a specific chain via gateway.

        Retries up to 3 times with exponential backoff on transient errors.
        Falls back to cached value on total failure.

        Args:
            token: Token symbol (e.g., "USDC", "WETH")
            chain: Chain name (e.g., "arbitrum", "base")

        Returns:
            TokenBalance with balance and USD value
        """
        chain_lower = chain.lower()
        if chain_lower not in self._chains:
            logger.warning(f"Chain '{chain}' not in configured chains: {self._chains}")
            return TokenBalance(symbol=token, balance=Decimal("0"), balance_usd=Decimal("0"))

        cache_key = (token, chain_lower)
        last_error: Exception | None = None

        for attempt in range(_MAX_RETRIES):
            try:
                response = self._client.market.GetBalance(
                    gateway_pb2.BalanceRequest(
                        token=token,
                        chain=chain_lower,
                        wallet_address=self._wallet_address,
                    ),
                    timeout=15.0,
                )

                balance = Decimal(response.balance) if response.balance else Decimal("0")
                balance_usd = Decimal(response.balance_usd) if response.balance_usd else Decimal("0")

                result = TokenBalance(
                    symbol=token,
                    balance=balance,
                    balance_usd=balance_usd,
                )

                # Cache successful result
                self._cache[cache_key] = (result, time.monotonic())

                return result

            except Exception as e:
                last_error = e
                error_msg = str(e)

                if not _is_retryable(error_msg):
                    break

                if attempt < _MAX_RETRIES - 1:
                    backoff = _BACKOFF_BASE_SECONDS * (2**attempt)
                    logger.warning(
                        "Balance request for %s on %s failed (attempt %d/%d), retrying in %.1fs: %s",
                        token,
                        chain_lower,
                        attempt + 1,
                        _MAX_RETRIES,
                        backoff,
                        error_msg,
                    )
                    time.sleep(backoff)

        # All retries exhausted -- try cached fallback (within TTL)
        error_msg = str(last_error) if last_error else "unknown error"
        cached_result = self._get_cached(cache_key)
        if cached_result is not None:
            # NOTE: TokenBalance has no `stale` field, so callers cannot
            # distinguish this from a fresh read.  Log at ERROR so operators
            # are alerted.  A future PR should add a staleness indicator.
            logger.error(
                "Returning STALE cached balance for %s on %s after %d failed attempts: %s",
                token,
                chain_lower,
                _MAX_RETRIES,
                error_msg,
            )
            return cached_result

        logger.error(f"Failed to get {token} balance on {chain} via gateway (no cache): {error_msg}")
        return TokenBalance(symbol=token, balance=Decimal("0"), balance_usd=Decimal("0"))

    def _get_cached(self, cache_key: tuple[str, str]) -> TokenBalance | None:
        """Return a cached balance if it exists and is within TTL, else None."""
        cached = self._cache.get(cache_key)
        if cached is None:
            return None
        result, cached_at = cached
        if (time.monotonic() - cached_at) > self._cache_ttl:
            return None
        return result

    def __call__(self, token: str, chain: str) -> TokenBalance:
        """Callable interface matching MultiChainBalanceProvider type."""
        return self.get_balance(token, chain)

    @property
    def chains(self) -> list[str]:
        """Get list of configured chains."""
        return self._chains
