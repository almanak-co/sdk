"""LiFi API Client.

This module provides the LiFiClient class for interacting with the LiFi API.
LiFi is a cross-chain liquidity meta-aggregator that routes through bridges
(Across, Stargate, Hop, etc.) and DEXs (1inch, 0x, Paraswap, etc.) to find
optimal cross-chain and same-chain swap routes.

Example:
    from almanak.framework.connectors.lifi import LiFiClient, LiFiConfig

    config = LiFiConfig(chain_id=42161, wallet_address="0x...")
    client = LiFiClient(config)

    # Get a cross-chain quote
    quote = client.get_quote(
        from_chain_id=42161,  # Arbitrum
        to_chain_id=8453,     # Base
        from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC
        to_token="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC
        from_amount="1000000000",  # 1000 USDC
        from_address="0x...",
    )
"""

import logging
import os
from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import LiFiAPIError, LiFiConfigError, LiFiRouteNotFoundError
from .models import LiFiOrderStrategy, LiFiStatusResponse, LiFiStep

logger = logging.getLogger(__name__)


# Chain name to chain ID mapping
CHAIN_MAPPING: dict[str, int] = {
    "ethereum": 1,
    "optimism": 10,
    "bsc": 56,
    "gnosis": 100,
    "polygon": 137,
    "base": 8453,
    "arbitrum": 42161,
    "avalanche": 43114,
    "sonic": 146,
    "linea": 59144,
}

# Reverse mapping
CHAIN_ID_TO_NAME: dict[int, str] = {v: k for k, v in CHAIN_MAPPING.items()}

# LiFi Diamond proxy address (same on most EVM chains)
LIFI_DIAMOND_ADDRESS = "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE"

# Native token address used by LiFi API
NATIVE_TOKEN_ADDRESS = "0x0000000000000000000000000000000000000000"

# Default API base URL
DEFAULT_BASE_URL = "https://li.quest/v1"

# Default integrator name
DEFAULT_INTEGRATOR = "almanak"


@dataclass
class LiFiConfig:
    """Configuration for LiFi client.

    Attributes:
        chain_id: Default source chain ID
        wallet_address: Default wallet address for transactions
        api_key: LiFi API key (optional, from LIFI_API_KEY env var)
        base_url: LiFi API base URL
        integrator: Integrator name for LiFi API
        timeout: Request timeout in seconds
        order: Default route ordering strategy
    """

    chain_id: int
    wallet_address: str
    api_key: str | None = None
    base_url: str = DEFAULT_BASE_URL
    integrator: str = DEFAULT_INTEGRATOR
    timeout: int = 30
    order: LiFiOrderStrategy = LiFiOrderStrategy.RECOMMENDED

    def __post_init__(self) -> None:
        """Resolve API key from env if not provided."""
        if self.api_key is None:
            self.api_key = os.environ.get("LIFI_API_KEY")
            # API key is optional for LiFi (just gives higher rate limits)

        if not self.wallet_address:
            raise LiFiConfigError(
                "wallet_address is required",
                parameter="wallet_address",
            )


class LiFiClient:
    """Client for interacting with the LiFi API.

    This client provides methods for:
    - Getting cross-chain and same-chain swap quotes
    - Checking cross-chain transfer status
    - Querying supported tokens and chains

    Example:
        config = LiFiConfig(chain_id=42161, wallet_address="0x...")
        client = LiFiClient(config)

        quote = client.get_quote(
            from_chain_id=42161,
            to_chain_id=8453,
            from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            to_token="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            from_amount="1000000000",
            from_address="0x...",
        )
    """

    def __init__(self, config: LiFiConfig) -> None:
        """Initialize the LiFi client.

        Args:
            config: LiFi client configuration
        """
        self.config = config
        self._setup_session()

        logger.info(f"LiFiClient initialized for chain_id={config.chain_id}, integrator={config.integrator}")

    def _setup_session(self) -> None:
        """Set up requests session with retry logic."""
        self.session = requests.Session()

        headers: dict[str, str] = {
            "Accept": "application/json",
        }

        # API key is optional for LiFi
        if self.config.api_key:
            headers["x-lifi-api-key"] = self.config.api_key

        self.session.headers.update(headers)

        # Configure retry strategy
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
        """Make a request to the LiFi API.

        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path (e.g., "/quote")
            params: Query parameters
            json_data: JSON body for POST requests

        Returns:
            Parsed JSON response

        Raises:
            LiFiAPIError: If the API request fails
        """
        url = f"{self.config.base_url}{endpoint}"

        logger.debug(f"LiFi API Request: {method} {endpoint}")

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

            raise LiFiAPIError(
                message=f"API request failed: {e}",
                status_code=response.status_code,
                endpoint=endpoint,
                error_data=error_data,
            ) from e

        except requests.exceptions.RequestException as e:
            raise LiFiAPIError(
                message=f"Request failed: {e}",
                status_code=0,
                endpoint=endpoint,
            ) from e

    def get_quote(
        self,
        from_chain_id: int,
        to_chain_id: int,
        from_token: str,
        to_token: str,
        from_amount: str,
        from_address: str,
        to_address: str | None = None,
        slippage: float = 0.005,
        order: LiFiOrderStrategy | None = None,
        allow_bridges: list[str] | None = None,
        deny_bridges: list[str] | None = None,
    ) -> LiFiStep:
        """Get a quote with executable transaction data.

        This calls the LiFi /quote endpoint which returns a single best
        route with transaction data ready to execute.

        Args:
            from_chain_id: Source chain ID
            to_chain_id: Destination chain ID
            from_token: Source token address
            to_token: Destination token address
            from_amount: Amount in smallest unit (wei for ETH, etc.)
            from_address: Sender wallet address
            to_address: Receiver address (defaults to from_address)
            slippage: Slippage tolerance as decimal (0.005 = 0.5%)
            order: Route ordering strategy (FASTEST, CHEAPEST, etc.)
            allow_bridges: Only use these bridges
            deny_bridges: Exclude these bridges

        Returns:
            LiFiStep with quote details and transaction data

        Raises:
            LiFiAPIError: If the API request fails
            LiFiRouteNotFoundError: If no route is found
        """
        params: dict[str, Any] = {
            "fromChain": from_chain_id,
            "toChain": to_chain_id,
            "fromToken": from_token,
            "toToken": to_token,
            "fromAmount": from_amount,
            "fromAddress": from_address,
            "toAddress": to_address or from_address,
            "slippage": slippage,
            "order": (order or self.config.order).value,
            "integrator": self.config.integrator,
        }

        if allow_bridges:
            params["allowBridges"] = ",".join(allow_bridges)
        if deny_bridges:
            params["denyBridges"] = ",".join(deny_bridges)

        try:
            response = self._make_request("GET", "/quote", params=params)
        except LiFiAPIError as e:
            # LiFi returns 404 when no route is found
            if e.status_code == 404:
                raise LiFiRouteNotFoundError(
                    f"No route found: {from_chain_id}:{from_token} -> {to_chain_id}:{to_token}, amount={from_amount}"
                ) from e
            raise

        step = LiFiStep.from_api_response(response)

        if not step.transaction_request or not step.transaction_request.data:
            raise LiFiRouteNotFoundError(
                f"Quote returned but no transaction data: {from_chain_id}:{from_token} -> {to_chain_id}:{to_token}"
            )

        logger.info(
            f"LiFi quote: {from_token[:10]}...@{from_chain_id} -> {to_token[:10]}...@{to_chain_id}, "
            f"tool={step.tool}, type={step.type}, "
            f"to_amount={step.get_to_amount()}, "
            f"duration={step.estimate.execution_duration if step.estimate else 'N/A'}s"
        )

        return step

    def get_status(
        self,
        tx_hash: str,
        from_chain: int,
        to_chain: int,
        bridge: str | None = None,
    ) -> LiFiStatusResponse:
        """Check the status of a cross-chain transfer.

        Args:
            tx_hash: Source chain transaction hash
            from_chain: Source chain ID
            to_chain: Destination chain ID
            bridge: Bridge name (optional, improves lookup speed)

        Returns:
            LiFiStatusResponse with transfer status
        """
        params: dict[str, Any] = {
            "txHash": tx_hash,
            "fromChain": from_chain,
            "toChain": to_chain,
        }

        if bridge:
            params["bridge"] = bridge

        response = self._make_request("GET", "/status", params=params)
        status = LiFiStatusResponse.from_api_response(response)

        logger.debug(f"LiFi status: tx={tx_hash[:10]}..., status={status.status}, substatus={status.substatus}")

        return status

    def get_tokens(self, chain_id: int | None = None) -> dict[str, Any]:
        """Get supported tokens.

        Args:
            chain_id: Filter by chain ID (optional)

        Returns:
            Dict of tokens by chain
        """
        params: dict[str, Any] = {}
        if chain_id is not None:
            params["chains"] = str(chain_id)

        return self._make_request("GET", "/tokens", params=params)

    def get_chains(self) -> list[dict[str, Any]]:
        """Get supported chains.

        Returns:
            List of chain information dicts
        """
        response = self._make_request("GET", "/chains")
        return response.get("chains", response) if isinstance(response, dict) else response

    @staticmethod
    def resolve_chain_id(chain: str | int) -> int:
        """Resolve a chain name or ID to a chain ID.

        Args:
            chain: Chain name (e.g., "arbitrum") or chain ID (e.g., 42161)

        Returns:
            Chain ID as integer

        Raises:
            LiFiConfigError: If chain name is not recognized
        """
        if isinstance(chain, int):
            return chain
        chain_lower = chain.lower()
        if chain_lower not in CHAIN_MAPPING:
            raise LiFiConfigError(
                f"Unsupported chain: {chain}. Supported: {', '.join(CHAIN_MAPPING.keys())}",
                parameter="chain",
            )
        return CHAIN_MAPPING[chain_lower]
