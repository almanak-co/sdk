"""Polymarket SDK Orchestrator.

Unified SDK that orchestrates CLOB API and CTF on-chain operations for
seamless prediction market trading.

This SDK provides a single entry point for all Polymarket operations:
- Market data fetching
- Order management
- Position tracking
- On-chain token operations

Example:
    from almanak.framework.connectors.polymarket import PolymarketSDK, PolymarketConfig
    from web3 import Web3

    # Initialize SDK
    config = PolymarketConfig.from_env()
    web3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
    sdk = PolymarketSDK(config, web3)

    # Fetch market by slug
    market = sdk.get_market_by_slug("will-bitcoin-exceed-100k-2025")

    # Get YES/NO prices
    yes_price, no_price = sdk.get_yes_no_prices(market.id)

    # Ensure all approvals are set up
    approval_txs = sdk.ensure_allowances()
    for tx in approval_txs:
        # Sign and submit tx...
        pass
"""

from decimal import Decimal
from typing import Any

import structlog

from .clob_client import ClobClient
from .ctf_sdk import CtfSDK, TransactionData
from .exceptions import PolymarketMarketNotFoundError
from .models import (
    ApiCredentials,
    GammaMarket,
    MarketFilters,
    PolymarketConfig,
)

logger = structlog.get_logger(__name__)


class PolymarketSDK:
    """Unified SDK for Polymarket prediction market operations.

    Orchestrates both CLOB API client (off-chain order management) and
    CTF SDK (on-chain token operations) to provide a seamless trading
    experience.

    Key features:
    - Lazy credential creation: API credentials are created on first
      authenticated request
    - Unified market lookups: Find markets by ID, slug, or condition ID
    - Automatic approval checking: Ensure all token approvals are set up
    - Convenience methods: Common operations wrapped in simple methods

    Attributes:
        config: Polymarket configuration
        clob: CLOB API client for order management
        ctf: CTF SDK for on-chain operations
        web3: Web3 instance for on-chain queries (optional)

    Thread Safety:
        This class is NOT thread-safe. Use separate instances per thread.

    Example:
        >>> config = PolymarketConfig.from_env()
        >>> web3 = Web3(Web3.HTTPProvider(rpc_url))
        >>> sdk = PolymarketSDK(config, web3)
        >>>
        >>> # Get market data
        >>> market = sdk.get_market_by_slug("btc-100k")
        >>> print(f"YES: {market.yes_price}, NO: {market.no_price}")
        >>>
        >>> # Check and set up approvals
        >>> approval_txs = sdk.ensure_allowances()
        >>> if approval_txs:
        ...     print(f"Need {len(approval_txs)} approval(s)")
    """

    def __init__(
        self,
        config: PolymarketConfig,
        web3: Any | None = None,
    ) -> None:
        """Initialize Polymarket SDK.

        Args:
            config: Polymarket configuration with wallet and keys
            web3: Optional Web3 instance for on-chain operations.
                  Required for allowance checking and CTF operations.
        """
        self.config = config
        self.web3 = web3
        self._credentials: ApiCredentials | None = config.api_credentials

        # Initialize CLOB client
        self.clob = ClobClient(config)

        # Initialize CTF SDK
        self.ctf = CtfSDK()

        logger.info(
            "PolymarketSDK initialized",
            wallet=config.wallet_address,
            has_credentials=self._credentials is not None,
            has_web3=web3 is not None,
        )

    def close(self) -> None:
        """Close SDK and release resources."""
        self.clob.close()

    def __enter__(self) -> "PolymarketSDK":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # =========================================================================
    # Credential Management
    # =========================================================================

    @property
    def credentials(self) -> ApiCredentials | None:
        """Get current API credentials."""
        return self._credentials

    def get_or_create_credentials(self) -> ApiCredentials:
        """Get existing credentials or create new ones lazily.

        This method is called automatically when making authenticated
        requests. It first attempts to derive existing credentials,
        and if that fails, creates new ones.

        Returns:
            ApiCredentials with api_key, secret, and passphrase

        Raises:
            PolymarketAuthenticationError: If credential creation fails
        """
        if self._credentials is not None:
            return self._credentials

        logger.info("Lazily creating API credentials")
        self._credentials = self.clob.get_or_create_credentials()
        return self._credentials

    # =========================================================================
    # Market Lookup Convenience Methods
    # =========================================================================

    def get_market_by_slug(self, slug: str) -> GammaMarket:
        """Get market by URL slug.

        Convenience method that searches for a market by its URL slug
        and raises an error if not found.

        Args:
            slug: Market URL slug (e.g., "will-bitcoin-exceed-100k-2025")

        Returns:
            GammaMarket object

        Raises:
            PolymarketMarketNotFoundError: If market not found

        Example:
            >>> market = sdk.get_market_by_slug("btc-100k")
            >>> print(market.question)
        """
        markets = self.clob.get_markets(MarketFilters(slug=slug, limit=1))
        if not markets:
            raise PolymarketMarketNotFoundError(f"Market not found with slug: {slug}")
        return markets[0]

    def get_market_by_condition_id(self, condition_id: str) -> GammaMarket:
        """Get market by CTF condition ID.

        Args:
            condition_id: CTF condition ID (0x...)

        Returns:
            GammaMarket object

        Raises:
            PolymarketMarketNotFoundError: If market not found

        Example:
            >>> market = sdk.get_market_by_condition_id("0x9915bea...")
        """
        markets = self.clob.get_markets(MarketFilters(condition_ids=[condition_id], limit=1))
        if not markets:
            raise PolymarketMarketNotFoundError(f"Market not found with condition_id: {condition_id}")
        return markets[0]

    def get_market_by_token_id(self, token_id: str) -> GammaMarket:
        """Get market by CLOB token ID.

        Args:
            token_id: CLOB token ID (YES or NO token)

        Returns:
            GammaMarket object

        Raises:
            PolymarketMarketNotFoundError: If market not found

        Example:
            >>> market = sdk.get_market_by_token_id("19045189...")
        """
        markets = self.clob.get_markets(MarketFilters(clob_token_ids=[token_id], limit=1))
        if not markets:
            raise PolymarketMarketNotFoundError(f"Market not found with token_id: {token_id}")
        return markets[0]

    # =========================================================================
    # Price Convenience Methods
    # =========================================================================

    def get_yes_no_prices(self, market_id: str) -> tuple[Decimal, Decimal]:
        """Get YES and NO prices for a market.

        Fetches the market and returns the current prices for both
        YES and NO outcomes.

        Args:
            market_id: Market ID

        Returns:
            Tuple of (yes_price, no_price)

        Example:
            >>> yes_price, no_price = sdk.get_yes_no_prices("12345")
            >>> print(f"YES: ${yes_price}, NO: ${no_price}")
        """
        market = self.clob.get_market(market_id)
        return market.yes_price, market.no_price

    def get_prices_by_slug(self, slug: str) -> tuple[Decimal, Decimal]:
        """Get YES and NO prices for a market by slug.

        Args:
            slug: Market URL slug

        Returns:
            Tuple of (yes_price, no_price)

        Example:
            >>> yes_price, no_price = sdk.get_prices_by_slug("btc-100k")
        """
        market = self.get_market_by_slug(slug)
        return market.yes_price, market.no_price

    # =========================================================================
    # Allowance Convenience Methods
    # =========================================================================

    def ensure_allowances(self) -> list[TransactionData]:
        """Build transactions to ensure all necessary approvals.

        Checks current allowance status and returns a list of transactions
        needed to set up all required approvals for trading on Polymarket.

        This includes:
        - USDC approval for CTF Exchange
        - USDC approval for Neg Risk Exchange
        - ERC-1155 approval for CTF Exchange
        - ERC-1155 approval for Neg Risk Adapter

        Returns:
            List of TransactionData for any needed approvals.
            Empty list if all approvals are already in place.

        Raises:
            ValueError: If web3 instance is not configured

        Example:
            >>> txs = sdk.ensure_allowances()
            >>> if txs:
            ...     print(f"Need {len(txs)} approval(s)")
            ...     for tx in txs:
            ...         # Sign and submit transaction
            ...         pass
        """
        if self.web3 is None:
            raise ValueError("Web3 instance required for allowance checking. Initialize SDK with web3 parameter.")

        return self.ctf.ensure_allowances(self.config.wallet_address, self.web3)

    def check_allowances(self) -> "AllowanceStatus":
        """Check all relevant token allowances.

        Queries USDC allowances and ERC-1155 operator approvals needed
        for trading on Polymarket.

        Returns:
            AllowanceStatus with all allowance information

        Raises:
            ValueError: If web3 instance is not configured

        Example:
            >>> status = sdk.check_allowances()
            >>> print(f"USDC approved: {status.usdc_approved_ctf_exchange}")
            >>> print(f"Fully approved: {status.fully_approved}")
        """
        if self.web3 is None:
            raise ValueError("Web3 instance required for allowance checking. Initialize SDK with web3 parameter.")

        return self.ctf.check_allowances(self.config.wallet_address, self.web3)

    # =========================================================================
    # Balance Methods
    # =========================================================================

    def get_usdc_balance(self) -> int:
        """Get USDC balance for configured wallet.

        Returns:
            USDC balance in base units (6 decimals)

        Raises:
            ValueError: If web3 instance is not configured

        Example:
            >>> balance = sdk.get_usdc_balance()
            >>> print(f"USDC balance: {balance / 1e6}")
        """
        if self.web3 is None:
            raise ValueError("Web3 instance required for balance checking. Initialize SDK with web3 parameter.")

        return self.ctf.get_usdc_balance(self.config.wallet_address, self.web3)

    def get_position_balance(self, token_id: int) -> int:
        """Get ERC-1155 position token balance.

        Args:
            token_id: Conditional token ID (position ID)

        Returns:
            Token balance in base units

        Raises:
            ValueError: If web3 instance is not configured

        Example:
            >>> balance = sdk.get_position_balance(token_id)
            >>> print(f"Position: {balance / 1e6} shares")
        """
        if self.web3 is None:
            raise ValueError("Web3 instance required for balance checking. Initialize SDK with web3 parameter.")

        return self.ctf.get_token_balance(self.config.wallet_address, token_id, self.web3)


# Re-export AllowanceStatus for type hints
from .ctf_sdk import AllowanceStatus as AllowanceStatus  # noqa: E402, F811

__all__ = ["PolymarketSDK"]
