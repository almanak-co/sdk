"""Enso Finance SDK Client.

This module provides the EnsoClient class for interacting with the Enso Finance API.
Enso is a routing and composable transaction protocol that aggregates liquidity
across multiple DEXs and lending protocols.

Transport (VIB-2986, Phase 5):
    When ``EnsoConfig.gateway_client`` is provided the client routes every API
    call through the gateway's ``EnsoService`` gRPC stubs, keeping the Enso API
    key inside the gateway process. When no gateway_client is provided the
    client falls back to direct HTTPS to ``api.enso.finance``. The direct path
    is only reachable from trusted contexts (gateway-internal execution and
    local paper-trading), not from deployed strategy containers — production
    strategy containers have no outbound network except the gateway channel,
    so the fallback is moot there.

Example:
    from almanak.framework.connectors.enso import EnsoClient, EnsoConfig

    config = EnsoConfig(
        api_key="your-api-key",
        chain="arbitrum",
        wallet_address="0x...",
    )
    client = EnsoClient(config)

    # Get a swap route
    route = client.get_route(
        token_in="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC
        token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
        amount_in=1000000000,  # 1000 USDC (6 decimals)
        slippage_bps=50,
    )
"""

import logging
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import (
    EnsoAPIError,
    EnsoConfigError,
    EnsoValidationError,
    PriceImpactExceedsThresholdError,
    check_known_router_revert,
)
from .models import (
    BundleAction,
    Quote,
    RouteParams,
    RouteTransaction,
    RoutingStrategy,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# Chain ID mapping
CHAIN_MAPPING: dict[str, int] = {
    "ethereum": 1,
    "optimism": 10,
    "bsc": 56,
    "gnosis": 100,
    "polygon": 137,
    "zksync": 324,
    "base": 8453,
    "arbitrum": 42161,
    "avalanche": 43114,
    "sonic": 146,
    "linea": 59144,
    "berachain": 80094,
    "sepolia": 11155111,
}

# Reverse mapping
CHAIN_ID_TO_NAME: dict[int, str] = {v: k for k, v in CHAIN_MAPPING.items()}

# Enso Router addresses per chain
ROUTER_ADDRESSES: dict[int, str] = {
    1: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Ethereum
    10: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Optimism
    56: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # BSC
    100: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Gnosis
    137: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Polygon
    324: "0x1BD8CefD703CF6b8fF886AD2E32653C32bc62b5C",  # zkSync
    8453: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Base
    42161: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Arbitrum
    43114: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Avalanche
    146: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Sonic
    59144: "0xA146d46823f3F594B785200102Be5385CAfCE9B5",  # Linea
    80094: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Berachain
    11155111: "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",  # Sepolia
}

# Enso Delegate addresses per chain (for borrow operations)
DELEGATE_ADDRESSES: dict[int, str] = {
    1: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
    10: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
    56: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
    100: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
    137: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
    324: "0x4c3Db0fFf66f98d84429Bf60E7622e206Fc4947c",
    8453: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
    42161: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
    43114: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
    146: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
    59144: "0xEe41aB55411a957c43C469F74867fa4671F9f017",
    80094: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",  # Berachain
    11155111: "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
}


@dataclass
class EnsoConfig:
    """Configuration for Enso client.

    Attributes:
        chain: Chain name (e.g., "arbitrum", "ethereum") or chain ID
        wallet_address: Default wallet address for transactions
        api_key: Enso API key (or set ENSO_API_KEY env var). Not required when
            ``gateway_client`` is provided — the gateway holds the credential.
        base_url: Enso API base URL (used by the direct HTTP fallback)
        routing_strategy: Default routing strategy
        timeout: Request timeout in seconds
        gateway_client: Optional GatewayClient. When provided the client routes
            every API call through the gateway's EnsoService gRPC stubs and
            never constructs a requests.Session. See module docstring.
    """

    chain: str
    wallet_address: str
    api_key: str | None = None
    base_url: str = "https://api.enso.finance"
    routing_strategy: RoutingStrategy = RoutingStrategy.ROUTER
    timeout: int = 30
    gateway_client: "GatewayClient | None" = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Validate configuration and resolve API key."""
        # API key lives in the gateway when gateway_client is provided. Only the
        # direct HTTP fallback needs a local credential.
        if self.gateway_client is None and self.api_key is None:
            self.api_key = os.environ.get("ENSO_API_KEY")
            if not self.api_key:
                raise EnsoConfigError(
                    "API key is required. Set ENSO_API_KEY env var, pass api_key, "
                    "or pass gateway_client to route through the gateway.",
                    parameter="api_key",
                )

        # Validate chain
        if isinstance(self.chain, str):
            chain_lower = self.chain.lower()
            if chain_lower not in CHAIN_MAPPING:
                raise EnsoConfigError(
                    f"Unsupported chain: {self.chain}. Supported: {', '.join(CHAIN_MAPPING.keys())}",
                    parameter="chain",
                )

    @property
    def chain_id(self) -> int:
        """Get numeric chain ID."""
        if isinstance(self.chain, int):
            return self.chain
        return CHAIN_MAPPING[self.chain.lower()]

    @property
    def chain_name(self) -> str:
        """Get canonical chain name (e.g., "arbitrum").

        The gateway's ``EnsoService`` expects chain names, not numeric IDs, so
        gateway-path callers must resolve numeric chain values back to names
        before constructing protobuf requests.
        """
        if isinstance(self.chain, int):
            try:
                return CHAIN_ID_TO_NAME[self.chain]
            except KeyError as e:
                raise EnsoConfigError(
                    f"Unknown chain ID: {self.chain}. Known: {sorted(CHAIN_ID_TO_NAME.keys())}",
                    parameter="chain",
                ) from e
        return self.chain.lower()


class EnsoClient:
    """Client for interacting with the Enso Finance API.

    This client provides methods for:
    - Getting swap routes across multiple DEXs
    - Getting quotes for swaps
    - Building bundle transactions for complex DeFi operations
    - Approving tokens for the Enso router

    Example:
        config = EnsoConfig(
            chain="arbitrum",
            wallet_address="0x...",
        )
        client = EnsoClient(config)

        # Get a simple swap route
        route = client.get_route(
            token_in="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            amount_in=1000000000,
            slippage_bps=50,
        )
    """

    def __init__(self, config: EnsoConfig) -> None:
        """Initialize the Enso client.

        Args:
            config: Enso client configuration. When ``config.gateway_client`` is
                set, no HTTP session is created — every API call uses the
                gateway's EnsoService gRPC stubs instead.
        """
        self.config = config
        self.session: requests.Session | None = None
        if config.gateway_client is None:
            self._setup_session()

        transport = "gateway" if config.gateway_client is not None else "direct-http"
        logger.info(
            f"EnsoClient initialized for chain={config.chain} (chain_id={config.chain_id}, transport={transport})"
        )

    @property
    def _via_gateway(self) -> bool:
        """True when API calls should be routed through the gateway."""
        return self.config.gateway_client is not None

    def _setup_session(self) -> None:
        """Set up requests session with retry logic.

        Only used by the direct HTTP fallback. See the module docstring for
        the gateway vs direct transport policy.
        """
        self.session = requests.Session()  # vib-2986-exempt: gateway-internal fallback
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.config.api_key}",
                "Accept": "application/json",
            }
        )

        # Configure retry strategy
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
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
        """Make a request to the Enso API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Query parameters
            json_data: JSON body for POST requests

        Returns:
            Parsed JSON response

        Raises:
            EnsoAPIError: If the API request fails
        """
        url = f"{self.config.base_url}{endpoint}"

        logger.debug(f"API Request: {method} {endpoint}")
        logger.debug(f"Params: {params}")

        assert self.session is not None, "Direct HTTP request on gateway-only client"
        try:
            if json_data is not None:
                response = self.session.request(  # vib-2986-exempt: gateway-internal fallback
                    method,
                    url,
                    params=params,
                    json=json_data,
                    timeout=self.config.timeout,
                )
            else:
                response = self.session.request(  # vib-2986-exempt: gateway-internal fallback
                    method,
                    url,
                    params=params,
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

            raise EnsoAPIError(
                message=f"API request failed: {str(e)}",
                status_code=response.status_code,
                endpoint=endpoint,
                error_data=error_data,
            ) from e

        except requests.exceptions.RequestException as e:
            raise EnsoAPIError(
                message=f"Request failed: {str(e)}",
                status_code=0,
                endpoint=endpoint,
            ) from e

    def get_route(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        slippage_bps: int = 50,
        from_address: str | None = None,
        receiver: str | None = None,
        routing_strategy: RoutingStrategy | None = None,
        max_price_impact_bps: int | None = None,
        destination_chain_id: int | None = None,
        refund_receiver: str | None = None,
    ) -> RouteTransaction:
        """Get the best swap route from one token to another.

        Supports both same-chain swaps and cross-chain swaps via Enso's
        bridge aggregation (Stargate, LayerZero).

        Args:
            token_in: Input token address
            token_out: Output token address
            amount_in: Input amount in wei (as integer)
            slippage_bps: Slippage tolerance in basis points (default 50 = 0.5%)
            from_address: Address executing the swap (defaults to config wallet)
            receiver: Address to receive output (defaults to from_address)
            routing_strategy: Routing strategy to use
            max_price_impact_bps: Maximum allowed price impact in basis points
            destination_chain_id: Target chain ID for cross-chain swaps (None for same-chain)
            refund_receiver: Address to receive refunds if cross-chain fails (defaults to from_address)

        Returns:
            RouteTransaction with transaction data and route details

        Raises:
            EnsoAPIError: If the API request fails
            PriceImpactExceedsThresholdError: If price impact exceeds threshold

        Example:
            # Same-chain swap on Arbitrum
            route = client.get_route(
                token_in="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC
                token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
                amount_in=1000000000,
            )

            # Cross-chain swap: Base -> Arbitrum
            route = client.get_route(
                token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC on Base
                token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH on Arbitrum
                amount_in=1000000000,
                destination_chain_id=42161,  # Arbitrum
            )
        """
        from_addr = from_address or self.config.wallet_address
        strategy = routing_strategy or self.config.routing_strategy
        is_cross_chain = destination_chain_id is not None and destination_chain_id != self.config.chain_id

        # Normalize routing params once so gateway and direct paths agree:
        # - treat destination_chain_id == source as same-chain
        # - inherit from_addr into receiver/refund_receiver on cross-chain
        effective_destination_chain_id = destination_chain_id if is_cross_chain else None
        effective_receiver = receiver
        effective_refund_receiver = refund_receiver
        if is_cross_chain:
            effective_receiver = receiver or from_addr
            effective_refund_receiver = refund_receiver or from_addr

        if self._via_gateway:
            route_tx = self._get_route_via_gateway(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                slippage_bps=slippage_bps,
                from_addr=from_addr,
                receiver=effective_receiver,
                strategy=strategy,
                destination_chain_id=effective_destination_chain_id,
                refund_receiver=effective_refund_receiver,
            )
        else:
            params = {
                "fromAddress": from_addr,
                "tokenIn": [token_in],
                "tokenOut": [token_out],
                "amountIn": [str(amount_in)],
                "chainId": self.config.chain_id,
                "slippage": str(slippage_bps),
                "routingStrategy": strategy.value,
                "disableRFQs": "false",
            }

            if receiver:
                params["receiver"] = receiver

            # Cross-chain parameters
            if is_cross_chain:
                params["destinationChainId"] = destination_chain_id
                # receiver and refundReceiver are required for cross-chain operations
                params["receiver"] = receiver or from_addr
                params["refundReceiver"] = refund_receiver or from_addr
                logger.info(
                    f"Cross-chain route: {self.config.chain} (chainId={self.config.chain_id}) -> "
                    f"chainId={destination_chain_id}"
                )

            logger.debug(f"Route API params: {params}")

            response = self._make_request("GET", "/api/v1/shortcuts/route", params=params)
            response["chainId"] = self.config.chain_id
            if is_cross_chain:
                response["destinationChainId"] = destination_chain_id

            route_tx = RouteTransaction.from_api_response(response)

        # Validate price impact if threshold provided
        if max_price_impact_bps is not None and route_tx.price_impact is not None:
            if route_tx.price_impact > max_price_impact_bps:
                raise PriceImpactExceedsThresholdError(
                    f"Price impact {route_tx.price_impact}bp exceeds threshold {max_price_impact_bps}bp",
                    price_impact_bps=route_tx.price_impact,
                    threshold_bps=max_price_impact_bps,
                )

        if is_cross_chain:
            logger.info(
                f"Cross-chain route found: {token_in[:10]}... -> {token_out[:10]}..., "
                f"amount_out={route_tx.get_amount_out_wei()}, "
                f"price_impact={route_tx.price_impact}bp, "
                f"bridge_fee={route_tx.bridge_fee}, "
                f"estimated_time={route_tx.estimated_time}s"
            )
        else:
            logger.info(
                f"Route found: {token_in[:10]}... -> {token_out[:10]}..., "
                f"amount_out={route_tx.get_amount_out_wei()}, "
                f"price_impact={route_tx.price_impact}bp"
            )

        return route_tx

    def get_route_from_params(self, params: RouteParams) -> RouteTransaction:
        """Get route using RouteParams object.

        Args:
            params: Route parameters

        Returns:
            RouteTransaction with transaction data
        """
        return self.get_route(
            token_in=params.token_in,
            token_out=params.token_out,
            amount_in=params.amount_in,
            slippage_bps=params.slippage_bps,
            from_address=params.from_address,
            receiver=params.receiver,
            routing_strategy=params.routing_strategy,
            max_price_impact_bps=params.max_price_impact_bps,
            destination_chain_id=params.destination_chain_id,
            refund_receiver=params.refund_receiver,
        )

    def get_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        from_address: str | None = None,
        routing_strategy: RoutingStrategy | None = None,
        destination_chain_id: int | None = None,
    ) -> Quote:
        """Get a quote for a swap without building the transaction.

        Supports both same-chain and cross-chain quotes.

        Args:
            token_in: Input token address
            token_out: Output token address
            amount_in: Input amount in wei
            from_address: Address executing the swap
            routing_strategy: Routing strategy to use
            destination_chain_id: Target chain ID for cross-chain quotes (None for same-chain)

        Returns:
            Quote with expected output amount
        """
        from_addr = from_address or self.config.wallet_address
        strategy = routing_strategy or self.config.routing_strategy
        is_cross_chain = destination_chain_id is not None and destination_chain_id != self.config.chain_id

        if self._via_gateway:
            return self._get_quote_via_gateway(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                from_addr=from_addr,
                strategy=strategy,
                destination_chain_id=destination_chain_id if is_cross_chain else None,
            )

        params = {
            "fromAddress": from_addr,
            "tokenIn": [token_in],
            "tokenOut": [token_out],
            "amountIn": [str(amount_in)],
            "chainId": self.config.chain_id,
            "routingStrategy": strategy.value,
        }

        # Cross-chain parameters
        if is_cross_chain:
            params["destinationChainId"] = destination_chain_id

        response = self._make_request("GET", "/api/v1/shortcuts/quote", params=params)
        response["chainId"] = self.config.chain_id
        if is_cross_chain:
            response["destinationChainId"] = destination_chain_id

        return Quote.from_api_response(response)

    def get_bundle(
        self,
        bundle_actions: list[BundleAction],
        from_address: str | None = None,
        routing_strategy: RoutingStrategy | None = None,
        skip_quote: bool = False,
    ) -> dict[str, Any]:
        """Get a bundle transaction for multiple DeFi actions.

        Bundles allow composing multiple operations (deposits, borrows, swaps)
        into a single transaction.

        Args:
            bundle_actions: List of actions to bundle
            from_address: Address executing the bundle
            routing_strategy: Routing strategy to use
            skip_quote: Skip quote generation (for operations that don't need it)

        Returns:
            Bundle response with transaction data
        """
        from_addr = from_address or self.config.wallet_address
        strategy = routing_strategy or self.config.routing_strategy

        if self._via_gateway:
            return self._get_bundle_via_gateway(
                bundle_actions=bundle_actions,
                from_addr=from_addr,
                strategy=strategy,
                skip_quote=skip_quote,
            )

        params: dict[str, Any] = {
            "chainId": self.config.chain_id,
            "fromAddress": from_addr,
            "routingStrategy": strategy.value,
        }

        if skip_quote:
            params["skipQuote"] = True

        # Convert BundleAction objects to API format
        actions_data = [action.to_api_format() for action in bundle_actions]

        logger.debug(f"Bundle params: {params}")
        logger.debug(f"Bundle actions: {actions_data}")

        # Make POST request with actions as JSON body
        url = f"{self.config.base_url}/api/v1/shortcuts/bundle"
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        response = requests.post(  # vib-2986-exempt: gateway-internal fallback
            url,
            headers=headers,
            params=params,
            json=actions_data,
            timeout=self.config.timeout,
        )

        if response.status_code != 200:
            logger.error(f"Bundle API error: {response.status_code}")
            logger.error(f"Response: {response.text}")

            error_data = None
            try:
                error_data = response.json()
            except Exception:
                pass

            raise EnsoAPIError(
                message=f"Bundle request failed: {response.text}",
                status_code=response.status_code,
                endpoint="/api/v1/shortcuts/bundle",
                error_data=error_data,
            )

        return response.json()

    def get_approval(
        self,
        token_address: str,
        amount: int | None = None,
        from_address: str | None = None,
        routing_strategy: RoutingStrategy | None = None,
    ) -> dict[str, Any]:
        """Get approval transaction data for a token.

        Args:
            token_address: Token to approve
            amount: Amount to approve (defaults to unlimited)
            from_address: Address granting approval
            routing_strategy: Routing strategy (determines spender)

        Returns:
            Approval transaction data
        """
        from_addr = from_address or self.config.wallet_address
        strategy = routing_strategy or self.config.routing_strategy

        # Use max uint256 for unlimited approval
        approve_amount = amount if amount is not None else (2**256 - 1)

        if self._via_gateway:
            return self._get_approval_via_gateway(
                token_address=token_address,
                approve_amount=approve_amount,
                from_addr=from_addr,
                strategy=strategy,
            )

        params = {
            "chainId": self.config.chain_id,
            "fromAddress": from_addr,
            "tokenAddress": token_address,
            "amount": str(approve_amount),
            "routingStrategy": strategy.value,
        }

        response = self._make_request("GET", "/api/v1/wallet/approve", params=params)
        return response

    def get_router_address(
        self,
        routing_strategy: RoutingStrategy | None = None,
    ) -> str:
        """Get the Enso router address for the current chain.

        Args:
            routing_strategy: Routing strategy (router or delegate)

        Returns:
            Router contract address
        """
        strategy = routing_strategy or self.config.routing_strategy
        chain_id = self.config.chain_id

        if strategy == RoutingStrategy.DELEGATE:
            if chain_id not in DELEGATE_ADDRESSES:
                raise EnsoValidationError(
                    f"Delegate address not available for chain {chain_id}",
                    field="chain_id",
                    value=chain_id,
                )
            return DELEGATE_ADDRESSES[chain_id]
        else:
            if chain_id not in ROUTER_ADDRESSES:
                raise EnsoValidationError(
                    f"Router address not available for chain {chain_id}",
                    field="chain_id",
                    value=chain_id,
                )
            return ROUTER_ADDRESSES[chain_id]

    @property
    def chain_id(self) -> int:
        """Get the chain ID."""
        return self.config.chain_id

    @property
    def wallet_address(self) -> str:
        """Get the configured wallet address."""
        return self.config.wallet_address

    @staticmethod
    def resolve_chain_id(chain: str | int) -> int:
        """Resolve a chain name or ID to a chain ID.

        Args:
            chain: Chain name (e.g., "arbitrum") or chain ID (e.g., 42161)

        Returns:
            Chain ID as integer

        Raises:
            EnsoValidationError: If chain name is not supported
        """
        if isinstance(chain, int):
            return chain
        chain_lower = chain.lower()
        if chain_lower not in CHAIN_MAPPING:
            raise EnsoValidationError(
                f"Unsupported chain: {chain}. Supported: {', '.join(CHAIN_MAPPING.keys())}",
                field="chain",
                value=chain,
            )
        return CHAIN_MAPPING[chain_lower]

    def get_cross_chain_route(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        destination_chain: str | int,
        slippage_bps: int = 50,
        receiver: str | None = None,
        max_price_impact_bps: int | None = None,
    ) -> RouteTransaction:
        """Convenience method for cross-chain swaps.

        This is a simplified interface for cross-chain operations that handles
        chain ID resolution and sets sensible defaults.

        Args:
            token_in: Input token address (on source chain)
            token_out: Output token address (on destination chain)
            amount_in: Input amount in wei
            destination_chain: Destination chain name (e.g., "arbitrum") or chain ID
            slippage_bps: Slippage tolerance in basis points (default 50 = 0.5%)
            receiver: Address to receive output (defaults to wallet address)
            max_price_impact_bps: Maximum allowed price impact in basis points

        Returns:
            RouteTransaction with cross-chain transaction data

        Example:
            # Bridge USDC from Base to Arbitrum (swap to WETH on arrival)
            client = EnsoClient(EnsoConfig(chain="base", wallet_address="0x..."))
            route = client.get_cross_chain_route(
                token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC on Base
                token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH on Arbitrum
                amount_in=1000 * 10**6,  # 1000 USDC
                destination_chain="arbitrum",
            )
        """
        destination_chain_id = self.resolve_chain_id(destination_chain)

        return self.get_route(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage_bps=slippage_bps,
            receiver=receiver,
            max_price_impact_bps=max_price_impact_bps,
            destination_chain_id=destination_chain_id,
            refund_receiver=self.config.wallet_address,
        )

    # =========================================================================
    # Gateway transport (VIB-2986, Phase 5)
    #
    # These helpers invoke the gateway's EnsoService gRPC. They run when
    # ``config.gateway_client`` is set, which keeps the Enso API key inside
    # the gateway process (production strategy containers have no outbound
    # HTTPS and cannot reach api.enso.finance directly).
    # =========================================================================

    @staticmethod
    def _call_enso_stub(method, request, *, timeout: float, endpoint: str, method_name: str):
        """Invoke a gateway Enso stub, translating transport errors to ``EnsoAPIError``.

        Direct-HTTP mode always raises ``EnsoAPIError`` on failure. Mirror that
        contract for gateway transport so callers can rely on a single
        exception type regardless of the route.
        """
        try:
            return method(request, timeout=timeout)
        except EnsoAPIError:
            raise
        except Exception as e:
            raise EnsoAPIError(
                message=f"Gateway Enso {method_name} transport error: {e}",
                status_code=0,
                endpoint=endpoint,
            ) from e

    def _get_route_via_gateway(
        self,
        *,
        token_in: str,
        token_out: str,
        amount_in: int,
        slippage_bps: int,
        from_addr: str,
        receiver: str | None,
        strategy: RoutingStrategy,
        destination_chain_id: int | None,
        refund_receiver: str | None,
    ) -> RouteTransaction:
        """Fetch a swap route via the gateway's EnsoService.GetRoute."""
        from almanak.gateway.proto import gateway_pb2

        assert self.config.gateway_client is not None
        request = gateway_pb2.EnsoRouteRequest(
            chain=self.config.chain_name,
            token_in=token_in,
            token_out=token_out,
            amount_in=str(amount_in),
            from_address=from_addr,
            slippage_bps=slippage_bps,
            routing_strategy=strategy.value,
            destination_chain_id=destination_chain_id or 0,
            receiver=receiver or "",
            refund_receiver=refund_receiver or "",
        )
        response = self._call_enso_stub(
            self.config.gateway_client.enso.GetRoute,
            request,
            timeout=float(self.config.timeout),
            endpoint="/api/v1/shortcuts/route",
            method_name="GetRoute",
        )
        if not response.success:
            # VIB-3828: when the Enso route simulation reverts with a known
            # router custom-error selector, raise the typed
            # ``EnsoRouterRevertError`` so the state machine classifies as
            # ``COMPILATION_PERMANENT`` instead of falling through to the
            # generic ``REVERT`` retry class. Unknown selectors and
            # selector-free errors fall through to the existing
            # ``EnsoAPIError`` path unchanged.
            check_known_router_revert(
                response.error,
                chain=self.config.chain_name,
                route_summary=f"{token_in} -> {token_out}",
            )
            raise EnsoAPIError(
                message=f"Gateway Enso GetRoute failed: {response.error}",
                status_code=0,
                endpoint="/api/v1/shortcuts/route",
            )
        gas_str = response.gas or response.gas_estimate
        return RouteTransaction.from_api_response(
            {
                "tx": {"to": response.to, "data": response.data, "value": response.value},
                "gas": str(gas_str) if gas_str else "0",
                "amountOut": response.amount_out,
                "priceImpact": response.price_impact,
                "chainId": self.config.chain_id,
                "destinationChainId": destination_chain_id,
                "bridgeFee": response.bridge_fee if response.is_cross_chain else None,
                "estimatedTime": response.estimated_time if response.is_cross_chain else None,
            }
        )

    def _get_quote_via_gateway(
        self,
        *,
        token_in: str,
        token_out: str,
        amount_in: int,
        from_addr: str,
        strategy: RoutingStrategy,
        destination_chain_id: int | None,
    ) -> Quote:
        """Fetch a quote via the gateway's EnsoService.GetQuote."""
        from almanak.gateway.proto import gateway_pb2

        assert self.config.gateway_client is not None
        request = gateway_pb2.EnsoQuoteRequest(
            chain=self.config.chain_name,
            token_in=token_in,
            token_out=token_out,
            amount_in=str(amount_in),
            from_address=from_addr,
            routing_strategy=strategy.value,
            destination_chain_id=destination_chain_id or 0,
        )
        response = self._call_enso_stub(
            self.config.gateway_client.enso.GetQuote,
            request,
            timeout=float(self.config.timeout),
            endpoint="/api/v1/shortcuts/quote",
            method_name="GetQuote",
        )
        if not response.success:
            raise EnsoAPIError(
                message=f"Gateway Enso GetQuote failed: {response.error}",
                status_code=0,
                endpoint="/api/v1/shortcuts/quote",
            )
        return Quote.from_api_response(
            {
                "amountOut": response.amount_out,
                "gas": str(response.gas_estimate) if response.gas_estimate else None,
                "priceImpact": response.price_impact,
                "chainId": self.config.chain_id,
            }
        )

    def _get_approval_via_gateway(
        self,
        *,
        token_address: str,
        approve_amount: int,
        from_addr: str,
        strategy: RoutingStrategy,
    ) -> dict[str, Any]:
        """Fetch approval tx data via the gateway's EnsoService.GetApproval."""
        from almanak.gateway.proto import gateway_pb2

        assert self.config.gateway_client is not None
        request = gateway_pb2.EnsoApprovalRequest(
            chain=self.config.chain_name,
            from_address=from_addr,
            token_address=token_address,
            amount=str(approve_amount),
            routing_strategy=strategy.value,
        )
        response = self._call_enso_stub(
            self.config.gateway_client.enso.GetApproval,
            request,
            timeout=float(self.config.timeout),
            endpoint="/api/v1/wallet/approve",
            method_name="GetApproval",
        )
        if not response.success:
            raise EnsoAPIError(
                message=f"Gateway Enso GetApproval failed: {response.error}",
                status_code=0,
                endpoint="/api/v1/wallet/approve",
            )
        return {
            "tx": {"to": response.to, "data": response.data},
            "to": response.to,
            "data": response.data,
            "gas": response.gas,
        }

    def _get_bundle_via_gateway(
        self,
        *,
        bundle_actions: list[BundleAction],
        from_addr: str,
        strategy: RoutingStrategy,
        skip_quote: bool,
    ) -> dict[str, Any]:
        """Fetch a bundle tx via the gateway's EnsoService.GetBundle."""
        import json

        from almanak.gateway.proto import gateway_pb2

        assert self.config.gateway_client is not None
        proto_actions = []
        for action in bundle_actions:
            api_payload = action.to_api_format()
            # Gateway proto uses map<string,string>; JSON-encode every arg
            # value so the gateway can round-trip back to native types
            # (bools/numbers/lists/dicts) before forwarding to the Enso API.
            # Encoding strings yields quoted JSON strings so decode is
            # unambiguous on the gateway side.
            str_args = {k: json.dumps(v) for k, v in (api_payload.get("args") or {}).items()}
            proto_actions.append(
                gateway_pb2.EnsoBundleAction(
                    protocol=str(api_payload.get("protocol", "")),
                    action=str(api_payload.get("action", "")),
                    args=str_args,
                )
            )
        request = gateway_pb2.EnsoBundleRequest(
            chain=self.config.chain_name,
            from_address=from_addr,
            actions=proto_actions,
            routing_strategy=strategy.value,
            skip_quote=skip_quote,
        )
        response = self._call_enso_stub(
            self.config.gateway_client.enso.GetBundle,
            request,
            timeout=float(self.config.timeout),
            endpoint="/api/v1/shortcuts/bundle",
            method_name="GetBundle",
        )
        if not response.success:
            raise EnsoAPIError(
                message=f"Gateway Enso GetBundle failed: {response.error}",
                status_code=0,
                endpoint="/api/v1/shortcuts/bundle",
            )
        # Bundle responses are consumed as raw dicts by callers; re-inflate
        # the JSON the gateway already validated so downstream parsing stays
        # identical between transports.
        if response.bundle_json:
            try:
                return json.loads(response.bundle_json)
            except json.JSONDecodeError:
                logger.warning("Gateway returned malformed bundle_json; using tx fields")
        return {
            "tx": {
                "to": response.to,
                "data": response.data,
                "value": response.value,
                "gas": response.gas,
            },
        }
