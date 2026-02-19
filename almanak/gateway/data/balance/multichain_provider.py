"""Multi-Chain Balance Provider for cross-chain balance queries.

This module provides a balance provider that can query balances across
multiple blockchain networks, wrapping per-chain Web3BalanceProviders.

Example:
    from almanak.gateway.data.balance import MultiChainWeb3BalanceProvider

    provider = MultiChainWeb3BalanceProvider(
        rpc_urls={
            "arbitrum": "https://arb1.arbitrum.io/rpc",
            "base": "https://mainnet.base.org",
        },
        wallet_address="0x1234...",
    )

    # Query USDC balance on Base
    result = provider.get_balance("USDC", "base")
    print(f"USDC on Base: {result.balance}")
"""

import asyncio
import logging
from decimal import Decimal

from almanak.framework.strategies.intent_strategy import TokenBalance

from .web3_provider import Web3BalanceProvider

logger = logging.getLogger(__name__)


class MultiChainWeb3BalanceProvider:
    """Multi-chain balance provider wrapping per-chain Web3BalanceProviders.

    This class provides a unified interface for querying token balances
    across multiple blockchain networks.

    Attributes:
        rpc_urls: Mapping of chain names to RPC URLs
        wallet_address: The wallet address to query balances for
        providers: Per-chain Web3BalanceProvider instances (lazily created)
    """

    def __init__(
        self,
        rpc_urls: dict[str, str],
        wallet_address: str,
        cache_ttl: int = 5,
    ) -> None:
        """Initialize multi-chain balance provider.

        Args:
            rpc_urls: Mapping of chain names to RPC URLs
            wallet_address: Wallet address for balance queries
            cache_ttl: Cache TTL in seconds (default 5)
        """
        self._rpc_urls = {k.lower(): v for k, v in rpc_urls.items()}
        self._wallet_address = wallet_address
        self._cache_ttl = cache_ttl
        self._providers: dict[str, Web3BalanceProvider] = {}

        logger.info(f"MultiChainWeb3BalanceProvider initialized for chains: {list(self._rpc_urls.keys())}")

    def _get_provider(self, chain: str) -> Web3BalanceProvider:
        """Get or create a Web3BalanceProvider for a chain.

        Args:
            chain: Chain name (e.g., "arbitrum", "base")

        Returns:
            Web3BalanceProvider for the chain

        Raises:
            ValueError: If chain is not configured
        """
        chain = chain.lower()
        if chain not in self._rpc_urls:
            raise ValueError(f"Chain '{chain}' not configured. Available: {list(self._rpc_urls.keys())}")

        if chain not in self._providers:
            self._providers[chain] = Web3BalanceProvider(
                rpc_url=self._rpc_urls[chain],
                wallet_address=self._wallet_address,
                chain=chain,
                cache_ttl=self._cache_ttl,
            )

        return self._providers[chain]

    def get_balance(self, token: str, chain: str) -> TokenBalance:
        """Get token balance on a specific chain.

        This method is synchronous for compatibility with the
        MultiChainBalanceProvider callable interface.

        Args:
            token: Token symbol (e.g., "USDC", "WETH")
            chain: Chain name (e.g., "arbitrum", "base")

        Returns:
            TokenBalance with balance and USD value
        """
        provider = self._get_provider(chain)

        try:
            # Run async method synchronously
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in an async context, create a new thread
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, provider.get_balance(token))
                    result = future.result(timeout=10)
            else:
                result = asyncio.run(provider.get_balance(token))

            return TokenBalance(
                symbol=token,
                balance=result.balance,
                balance_usd=Decimal("0"),  # USD value calculated separately via price oracle
            )

        except Exception as e:
            logger.warning(f"Failed to get {token} balance on {chain}: {e}")
            # Return zero balance on error
            return TokenBalance(
                symbol=token,
                balance=Decimal("0"),
                balance_usd=Decimal("0"),
            )

    def __call__(self, token: str, chain: str) -> TokenBalance:
        """Callable interface for MultiChainBalanceProvider type.

        Args:
            token: Token symbol
            chain: Chain name

        Returns:
            TokenBalance
        """
        return self.get_balance(token, chain)

    def invalidate_cache(self, chain: str | None = None) -> None:
        """Invalidate balance cache.

        Args:
            chain: Specific chain to invalidate, or None for all chains
        """
        if chain:
            if chain.lower() in self._providers:
                self._providers[chain.lower()].invalidate_cache()
        else:
            for provider in self._providers.values():
                provider.invalidate_cache()

    @property
    def chains(self) -> list[str]:
        """Get list of configured chains."""
        return list(self._rpc_urls.keys())
