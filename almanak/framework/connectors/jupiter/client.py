"""Jupiter DEX Aggregator HTTP Client.

This module provides the JupiterClient class for interacting with the
Jupiter Swap API v1. Jupiter is the primary DEX aggregator on Solana, routing
across Raydium, Orca, Meteora, and other Solana AMMs.

An API key is required (free keys available at https://portal.jup.ag).
Set via JUPITER_API_KEY env var or pass to JupiterConfig.

Example:
    from almanak.framework.connectors.jupiter import JupiterClient, JupiterConfig

    config = JupiterConfig(
        wallet_address="your-solana-wallet-pubkey",
        api_key="your-jupiter-api-key",
    )
    client = JupiterClient(config)

    # Get a swap quote
    quote = client.get_quote(
        input_mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        output_mint="So11111111111111111111111111111111111111112",     # WSOL
        amount=1000000000,  # 1000 USDC (6 decimals)
        slippage_bps=50,
    )

    # Get a swap transaction
    swap_tx = client.get_swap_transaction(quote, user_public_key="your-pubkey")
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import JupiterAPIError, JupiterConfigError, JupiterPriceImpactError
from .models import JupiterQuote, JupiterSwapTransaction

logger = logging.getLogger(__name__)


@dataclass
class JupiterConfig:
    """Configuration for Jupiter client.

    Attributes:
        wallet_address: Solana wallet public key (Base58)
        api_key: Jupiter API key (from https://portal.jup.ag). Falls back to JUPITER_API_KEY env var.
        base_url: Jupiter API base URL
        timeout: Request timeout in seconds
        max_accounts: Max intermediate accounts for routing (None = no limit)
    """

    wallet_address: str
    api_key: str | None = field(default=None, repr=False)
    base_url: str = ""
    timeout: int = 30
    max_accounts: int | None = None

    # lite-api.jup.ag is the free tier (no key required, rate-limited)
    # api.jup.ag requires an API key (free keys at https://portal.jup.ag)
    _DEFAULT_FREE_URL: str = field(default="https://lite-api.jup.ag", init=False, repr=False)
    _DEFAULT_PAID_URL: str = field(default="https://api.jup.ag", init=False, repr=False)

    def __post_init__(self) -> None:
        """Validate configuration."""
        if not self.wallet_address:
            raise JupiterConfigError(
                "wallet_address is required",
                parameter="wallet_address",
            )
        # Fall back to env var for API key
        if not self.api_key:
            self.api_key = os.environ.get("JUPITER_API_KEY")
        # Auto-select base URL based on API key availability
        if not self.base_url:
            self.base_url = self._DEFAULT_PAID_URL if self.api_key else self._DEFAULT_FREE_URL


class JupiterClient:
    """Client for interacting with the Jupiter Swap API v1.

    This client provides methods for:
    - Getting swap quotes across Solana DEXs
    - Building swap transactions (serialized VersionedTransactions)

    An API key is required (free keys at https://portal.jup.ag).

    Example:
        config = JupiterConfig(
            wallet_address="your-solana-wallet-pubkey",
            api_key="your-jupiter-api-key",
        )
        client = JupiterClient(config)

        quote = client.get_quote(
            input_mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            output_mint="So11111111111111111111111111111111111111112",
            amount=1000000000,
            slippage_bps=50,
        )
    """

    def __init__(self, config: JupiterConfig) -> None:
        """Initialize the Jupiter client.

        Args:
            config: Jupiter client configuration
        """
        self.config = config
        self._setup_session()
        logger.info(f"JupiterClient initialized for wallet={config.wallet_address[:8]}...")

    def _setup_session(self) -> None:
        """Set up requests session with retry logic."""
        self.session = requests.Session()
        headers = {"Accept": "application/json"}
        if self.config.api_key:
            headers["x-api-key"] = self.config.api_key
        self.session.headers.update(headers)

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: Any | None = None,
    ) -> Any:
        """Make a request to the Jupiter API.

        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            params: Query parameters
            json_data: JSON body for POST requests

        Returns:
            Parsed JSON response

        Raises:
            JupiterAPIError: If the API request fails
        """
        url = f"{self.config.base_url}{endpoint}"

        logger.debug(f"Jupiter API Request: {method} {endpoint}")

        try:
            response = self.session.request(
                method,
                url,
                params=params,
                json=json_data,
                timeout=self.config.timeout,
            )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            error_data = None
            try:
                error_data = response.json()
            except Exception:
                pass

            raise JupiterAPIError(
                message=f"API request failed: {e}",
                status_code=response.status_code,
                endpoint=endpoint,
                error_data=error_data,
            ) from e

        except requests.exceptions.RequestException as e:
            raise JupiterAPIError(
                message=f"Request failed: {e}",
                status_code=0,
                endpoint=endpoint,
            ) from e

    def get_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        slippage_bps: int = 50,
        max_price_impact_pct: float | None = None,
    ) -> JupiterQuote:
        """Get a swap quote from Jupiter.

        Args:
            input_mint: Input token mint address (Base58)
            output_mint: Output token mint address (Base58)
            amount: Input amount in smallest units (e.g., lamports for SOL)
            slippage_bps: Slippage tolerance in basis points (default 50 = 0.5%)
            max_price_impact_pct: Maximum allowed price impact percentage

        Returns:
            JupiterQuote with route and expected output

        Raises:
            JupiterAPIError: If the API request fails
            JupiterPriceImpactError: If price impact exceeds threshold
        """
        params: dict[str, Any] = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": slippage_bps,
        }

        if self.config.max_accounts is not None:
            params["maxAccounts"] = self.config.max_accounts

        response = self._make_request("GET", "/swap/v1/quote", params=params)
        quote = JupiterQuote.from_api_response(response)

        # Validate price impact if threshold provided
        if max_price_impact_pct is not None:
            actual_impact = quote.get_price_impact_float()
            if actual_impact > max_price_impact_pct:
                raise JupiterPriceImpactError(
                    f"Price impact {actual_impact:.4f}% exceeds threshold {max_price_impact_pct:.4f}%",
                    price_impact_pct=actual_impact,
                    threshold_pct=max_price_impact_pct,
                )

        logger.info(
            f"Jupiter quote: {input_mint[:8]}... -> {output_mint[:8]}..., "
            f"in={quote.in_amount}, out={quote.out_amount}, "
            f"impact={quote.price_impact_pct}%"
        )

        return quote

    # Valid priority fee levels for Jupiter API
    VALID_PRIORITY_FEE_LEVELS = ("low", "medium", "high", "veryHigh")
    DEFAULT_PRIORITY_FEE_LEVEL = "veryHigh"
    DEFAULT_MAX_LAMPORTS = 1_000_000

    def get_swap_transaction(
        self,
        quote: JupiterQuote,
        user_public_key: str | None = None,
        priority_fee_level: str | None = None,
        priority_fee_max_lamports: int | None = None,
    ) -> JupiterSwapTransaction:
        """Get a serialized swap transaction from Jupiter.

        The returned transaction is a base64-encoded VersionedTransaction
        ready for signing and submission.

        Args:
            quote: Quote from get_quote()
            user_public_key: Solana wallet public key (defaults to config wallet)
            priority_fee_level: Priority fee level ("low", "medium", "high", "veryHigh").
                Defaults to "veryHigh".
            priority_fee_max_lamports: Maximum priority fee in lamports.
                Defaults to 1_000_000 (0.001 SOL).

        Returns:
            JupiterSwapTransaction with base64-encoded transaction

        Raises:
            JupiterAPIError: If the API request fails
        """
        pubkey = user_public_key or self.config.wallet_address

        fee_level = priority_fee_level or self.DEFAULT_PRIORITY_FEE_LEVEL
        if fee_level not in self.VALID_PRIORITY_FEE_LEVELS:
            raise ValueError(
                f"Invalid priority_fee_level '{fee_level}'. Must be one of: {', '.join(self.VALID_PRIORITY_FEE_LEVELS)}"
            )
        max_lamports = priority_fee_max_lamports if priority_fee_max_lamports is not None else self.DEFAULT_MAX_LAMPORTS

        payload = {
            "quoteResponse": quote.raw_response,
            "userPublicKey": pubkey,
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "dynamicSlippage": True,
            "prioritizationFeeLamports": {
                "priorityLevelWithMaxLamports": {
                    "maxLamports": max_lamports,
                    "priorityLevel": fee_level,
                }
            },
        }

        response = self._make_request("POST", "/swap/v1/swap", json_data=payload)
        swap_tx = JupiterSwapTransaction.from_api_response(response, quote=quote)

        logger.info(
            f"Jupiter swap transaction received: "
            f"block_height={swap_tx.last_valid_block_height}, "
            f"priority_fee={swap_tx.priority_fee_lamports} lamports"
        )

        return swap_tx

    @property
    def wallet_address(self) -> str:
        """Get the configured wallet address."""
        return self.config.wallet_address
