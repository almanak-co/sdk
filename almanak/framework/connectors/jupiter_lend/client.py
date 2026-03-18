"""Jupiter Lend HTTP Client.

This module provides the JupiterLendClient class for interacting with the
Jupiter Lend REST API. Jupiter Lend is the #2 Solana money market (~$1.65B TVL),
featuring isolated vaults, rehypothecation, and aggressive LTV ratios.

No authentication is required.

Example:
    from almanak.framework.connectors.jupiter_lend import JupiterLendClient, JupiterLendConfig

    config = JupiterLendConfig(wallet_address="your-solana-wallet-pubkey")
    client = JupiterLendClient(config)

    # Get available vaults
    vaults = client.get_vaults()

    # Build a deposit transaction
    tx = client.deposit(vault=vaults[0].address, amount="100.0")
"""

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import JupiterLendAPIError, JupiterLendConfigError
from .models import JupiterLendTransactionResponse, JupiterLendVault

logger = logging.getLogger(__name__)

# U64::MAX for "withdraw all" / "repay all"
U64_MAX = "18446744073709551615"


@dataclass
class JupiterLendConfig:
    """Configuration for Jupiter Lend client.

    Attributes:
        wallet_address: Solana wallet public key (Base58)
        base_url: Jupiter Lend API base URL
        timeout: Request timeout in seconds
    """

    wallet_address: str
    base_url: str = "https://api.jup.ag/lend"
    timeout: int = 30

    def __post_init__(self) -> None:
        """Validate configuration."""
        if not self.wallet_address:
            raise JupiterLendConfigError(
                "wallet_address is required",
                parameter="wallet_address",
            )


class JupiterLendClient:
    """Client for interacting with the Jupiter Lend REST API.

    This client provides methods for:
    - Listing lending vaults and their metrics
    - Building deposit, borrow, repay, and withdraw transactions

    All transaction endpoints return base64-encoded unsigned VersionedTransactions.
    No authentication is required.

    Example:
        config = JupiterLendConfig(wallet_address="your-solana-wallet-pubkey")
        client = JupiterLendClient(config)

        vaults = client.get_vaults()
        tx = client.deposit(vault=vaults[0].address, amount="100.0")
    """

    def __init__(self, config: JupiterLendConfig) -> None:
        """Initialize the Jupiter Lend client.

        Args:
            config: Jupiter Lend client configuration
        """
        self.config = config
        self._setup_session()
        self._vault_cache: list[JupiterLendVault] = []
        self._vault_cache_ts: float = 0.0
        self._vault_cache_ttl: float = 60.0  # seconds
        logger.info(f"JupiterLendClient initialized for wallet={config.wallet_address[:8]}...")

    def _setup_session(self) -> None:
        """Set up requests session with retry logic."""
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

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
        """Make a request to the Jupiter Lend API.

        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            params: Query parameters
            json_data: JSON body for POST requests

        Returns:
            Parsed JSON response

        Raises:
            JupiterLendAPIError: If the API request fails
        """
        url = f"{self.config.base_url}{endpoint}"

        logger.debug(f"Jupiter Lend API Request: {method} {endpoint}")

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
            error_code = None
            try:
                error_data = response.json()
                if isinstance(error_data, dict):
                    error_code = error_data.get("code")
            except ValueError:
                logger.debug("Failed to parse Jupiter Lend error response JSON", exc_info=True)

            raise JupiterLendAPIError(
                message=f"API request failed: {e}",
                status_code=response.status_code,
                endpoint=endpoint,
                error_code=error_code,
                error_data=error_data,
            ) from e

        except requests.exceptions.RequestException as e:
            raise JupiterLendAPIError(
                message=f"Request failed: {e}",
                status_code=0,
                endpoint=endpoint,
            ) from e

    # =========================================================================
    # Vault queries
    # =========================================================================

    def get_vaults(self, force_refresh: bool = False) -> list[JupiterLendVault]:
        """List all available lending vaults.

        Results are cached for 60 seconds to avoid redundant API calls
        during multi-intent compilation within a single strategy cycle.

        Args:
            force_refresh: If True, bypass cache and fetch fresh data.

        Returns:
            List of JupiterLendVault objects

        Raises:
            JupiterLendAPIError: If the API request fails
        """
        now = time.monotonic()
        if not force_refresh and self._vault_cache and (now - self._vault_cache_ts) < self._vault_cache_ttl:
            return self._vault_cache

        response = self._make_request("GET", "/v1/vaults")
        if isinstance(response, list):
            vaults = [JupiterLendVault.from_api_response(v) for v in response]
        else:
            # Handle wrapped response
            vaults_data = response.get("vaults", response.get("data", []))
            vaults = [JupiterLendVault.from_api_response(v) for v in vaults_data]

        self._vault_cache = vaults
        self._vault_cache_ts = now
        return vaults

    def find_vault_by_token(self, token_symbol: str) -> JupiterLendVault | None:
        """Find a vault by token symbol.

        Args:
            token_symbol: Token symbol (e.g., "USDC", "SOL")

        Returns:
            JupiterLendVault if found, None otherwise
        """
        vaults = self.get_vaults()
        token_upper = token_symbol.upper()
        for vault in vaults:
            if vault.token_symbol.upper() == token_upper:
                return vault
        return None

    # =========================================================================
    # Transaction builders
    # =========================================================================

    def deposit(
        self,
        vault: str,
        amount: str,
        wallet: str | None = None,
    ) -> JupiterLendTransactionResponse:
        """Build a deposit (supply) transaction.

        Args:
            vault: Vault address to deposit into
            amount: Amount in token units (e.g., "100.5" for 100.5 USDC)
            wallet: Wallet address (defaults to config wallet)

        Returns:
            JupiterLendTransactionResponse with base64-encoded unsigned transaction

        Raises:
            JupiterLendAPIError: If the API request fails
        """
        payload = {
            "wallet": wallet or self.config.wallet_address,
            "vault": vault,
            "amount": amount,
        }

        response = self._make_request("POST", "/v1/deposit", json_data=payload)
        logger.info(f"Jupiter Lend deposit tx built: vault={vault[:8]}..., amount={amount}")
        return JupiterLendTransactionResponse.from_api_response(response, action="deposit")

    def borrow(
        self,
        vault: str,
        amount: str,
        wallet: str | None = None,
    ) -> JupiterLendTransactionResponse:
        """Build a borrow transaction.

        Args:
            vault: Vault address to borrow from
            amount: Amount in token units (e.g., "50.0" for 50 USDC)
            wallet: Wallet address (defaults to config wallet)

        Returns:
            JupiterLendTransactionResponse with base64-encoded unsigned transaction

        Raises:
            JupiterLendAPIError: If the API request fails
        """
        payload = {
            "wallet": wallet or self.config.wallet_address,
            "vault": vault,
            "amount": amount,
        }

        response = self._make_request("POST", "/v1/borrow", json_data=payload)
        logger.info(f"Jupiter Lend borrow tx built: vault={vault[:8]}..., amount={amount}")
        return JupiterLendTransactionResponse.from_api_response(response, action="borrow")

    def repay(
        self,
        vault: str,
        amount: str,
        wallet: str | None = None,
    ) -> JupiterLendTransactionResponse:
        """Build a repay transaction.

        Args:
            vault: Vault address to repay into
            amount: Amount in token units (e.g., "50.0" for 50 USDC)
            wallet: Wallet address (defaults to config wallet)

        Returns:
            JupiterLendTransactionResponse with base64-encoded unsigned transaction

        Raises:
            JupiterLendAPIError: If the API request fails
        """
        payload = {
            "wallet": wallet or self.config.wallet_address,
            "vault": vault,
            "amount": amount,
        }

        response = self._make_request("POST", "/v1/repay", json_data=payload)
        logger.info(f"Jupiter Lend repay tx built: vault={vault[:8]}..., amount={amount}")
        return JupiterLendTransactionResponse.from_api_response(response, action="repay")

    def withdraw(
        self,
        vault: str,
        amount: str,
        wallet: str | None = None,
    ) -> JupiterLendTransactionResponse:
        """Build a withdraw transaction.

        Use amount=U64_MAX ("18446744073709551615") to withdraw all.

        Args:
            vault: Vault address to withdraw from
            amount: Amount in token units, or U64_MAX for withdraw-all
            wallet: Wallet address (defaults to config wallet)

        Returns:
            JupiterLendTransactionResponse with base64-encoded unsigned transaction

        Raises:
            JupiterLendAPIError: If the API request fails
        """
        payload = {
            "wallet": wallet or self.config.wallet_address,
            "vault": vault,
            "amount": amount,
        }

        response = self._make_request("POST", "/v1/withdraw", json_data=payload)
        logger.info(f"Jupiter Lend withdraw tx built: vault={vault[:8]}..., amount={amount}")
        return JupiterLendTransactionResponse.from_api_response(response, action="withdraw")

    @property
    def wallet_address(self) -> str:
        """Get the configured wallet address."""
        return self.config.wallet_address
