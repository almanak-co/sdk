"""Across Bridge Adapter Implementation.

This module provides the AcrossBridgeAdapter class for interacting with the
Across Protocol bridge. Across uses relayers to provide fast cross-chain
transfers with optimistic verification.

Across Protocol:
- Relayers front capital on destination chain for instant finality
- UMA oracle provides dispute resolution for incorrect fills
- Spoke pools on each chain handle deposits and fills
- API provides quotes with dynamic fees based on liquidity

Supported Operations:
- get_quote(): Get fee/time estimates from Across API
- build_deposit_tx(): Build deposit transaction for spoke pool
- check_status(): Poll for fill status on destination chain
- estimate_completion_time(): Get typical completion times

Example:
    adapter = AcrossBridgeAdapter()

    quote = adapter.get_quote(
        token="USDC",
        amount=Decimal("1000"),
        from_chain="arbitrum",
        to_chain="optimism",
    )

    tx = adapter.build_deposit_tx(quote, recipient="0x...")
"""

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import requests
from eth_abi import encode
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from almanak.framework.data.tokens.exceptions import TokenResolutionError

from ..base import (
    BridgeAdapter,
    BridgeError,
    BridgeQuote,
    BridgeQuoteError,
    BridgeRoute,
    BridgeStatus,
    BridgeStatusEnum,
    BridgeStatusError,
    BridgeTransactionError,
)

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)

# Chain ID to chain name mapping for TokenResolver
_CHAIN_ID_TO_NAME: dict[int, str] = {
    1: "ethereum",
    10: "optimism",
    42161: "arbitrum",
    8453: "base",
    137: "polygon",
    43114: "avalanche",
    56: "bsc",
    59144: "linea",
    324: "zksync",
}


# =============================================================================
# Exceptions
# =============================================================================


class AcrossError(BridgeError):
    """Base exception for Across-related errors."""

    pass


class AcrossQuoteError(AcrossError, BridgeQuoteError):
    """Error when retrieving an Across quote."""

    pass


class AcrossTransactionError(AcrossError, BridgeTransactionError):
    """Error when building or submitting an Across transaction."""

    pass


class AcrossStatusError(AcrossError, BridgeStatusError):
    """Error when checking Across transfer status."""

    pass


# =============================================================================
# Constants
# =============================================================================


# Chain ID mapping for Across-supported chains
ACROSS_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "optimism": 10,
    "polygon": 137,
    "arbitrum": 42161,
    "base": 8453,
    "linea": 59144,
    "zksync": 324,
}

# Reverse mapping
ACROSS_CHAIN_ID_TO_NAME: dict[int, str] = {v: k for k, v in ACROSS_CHAIN_IDS.items()}

# Across SpokePool contract addresses per chain
# These are the V3 SpokePool addresses
ACROSS_SPOKE_POOL_ADDRESSES: dict[int, str] = {
    1: "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5",  # Ethereum
    10: "0x6f26Bf09B1C792e3228e5467807a900A503c0281",  # Optimism
    137: "0x9295ee1d8C5b022Be115A2AD3c30C72E34e7F096",  # Polygon
    42161: "0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A",  # Arbitrum
    8453: "0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64",  # Base
    59144: "0x7E63A5f1a8F0B4d0934B2f2327DAED3F6bb2ee75",  # Linea
    324: "0xE0B015E54d54fc84a6cB9B666099c46adE9335FF",  # zkSync
}

# Supported tokens on Across
ACROSS_SUPPORTED_TOKENS: list[str] = ["ETH", "WETH", "USDC", "USDT", "WBTC", "DAI"]

# Estimated completion times in seconds per route (from_chain -> to_chain)
# These are approximate and depend on relayer competition and liquidity
ACROSS_COMPLETION_TIMES: dict[str, dict[str, int]] = {
    "ethereum": {
        "arbitrum": 180,  # ~3 minutes
        "optimism": 180,
        "polygon": 180,
        "base": 180,
    },
    "arbitrum": {
        "ethereum": 240,  # ~4 minutes (L2 -> L1 slightly slower)
        "optimism": 120,  # ~2 minutes (L2 -> L2 fast)
        "polygon": 120,
        "base": 120,
    },
    "optimism": {
        "ethereum": 240,
        "arbitrum": 120,
        "polygon": 120,
        "base": 120,
    },
    "polygon": {
        "ethereum": 240,
        "arbitrum": 120,
        "optimism": 120,
        "base": 120,
    },
    "base": {
        "ethereum": 240,
        "arbitrum": 120,
        "optimism": 120,
        "polygon": 120,
    },
}

# Default timeout for bridge operations (30 minutes)
DEFAULT_TIMEOUT_SECONDS = 1800


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class AcrossConfig:
    """Configuration for Across bridge adapter.

    Attributes:
        api_base_url: Across API base URL
        timeout_seconds: Timeout for bridge operations (default 30 min)
        request_timeout: HTTP request timeout in seconds
        max_retries: Maximum number of retry attempts for API calls
    """

    api_base_url: str = "https://app.across.to/api"
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    request_timeout: int = 30
    max_retries: int = 3

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.timeout_seconds <= 0:
            raise AcrossError("timeout_seconds must be positive")
        if self.request_timeout <= 0:
            raise AcrossError("request_timeout must be positive")


# =============================================================================
# Adapter Implementation
# =============================================================================


class AcrossBridgeAdapter(BridgeAdapter):
    """Across Protocol bridge adapter implementation.

    Provides integration with the Across bridge for fast cross-chain transfers
    using relayers and optimistic verification.

    Features:
    - Fast finality via relayer network
    - Competitive fees through relayer competition
    - Support for ETH, USDC, WBTC and other major tokens
    - Multi-chain support (Ethereum, Arbitrum, Optimism, Base, Polygon)

    Example:
        adapter = AcrossBridgeAdapter()

        # Get quote
        quote = adapter.get_quote(
            token="USDC",
            amount=Decimal("1000"),
            from_chain="arbitrum",
            to_chain="optimism",
        )

        # Build transaction
        tx = adapter.build_deposit_tx(quote, "0xRecipient...")

        # Check status after deposit
        status = adapter.check_status(deposit_tx_hash)
    """

    def __init__(self, config: AcrossConfig | None = None, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize Across bridge adapter.

        Args:
            config: Optional configuration. Uses defaults if not provided.
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self._config = config or AcrossConfig()
        self._session: requests.Session | None = None
        self._routes: list[BridgeRoute] | None = None

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

    # =========================================================================
    # Abstract Property Implementations
    # =========================================================================

    @property
    def name(self) -> str:
        """Get bridge adapter name."""
        return "Across"

    @property
    def supported_tokens(self) -> list[str]:
        """Get list of supported tokens."""
        return ACROSS_SUPPORTED_TOKENS.copy()

    @property
    def supported_routes(self) -> list[BridgeRoute]:
        """Get list of supported bridge routes."""
        if self._routes is None:
            self._routes = self._build_routes()
        return self._routes

    # =========================================================================
    # Abstract Method Implementations
    # =========================================================================

    def get_quote(
        self,
        token: str,
        amount: Decimal,
        from_chain: str,
        to_chain: str,
        max_slippage: Decimal = Decimal("0.005"),
    ) -> BridgeQuote:
        """Get a quote for bridging tokens via Across.

        Calls the Across API to get current fee and time estimates
        for the specified transfer.

        Args:
            token: Token symbol (e.g., "USDC", "ETH", "WBTC")
            amount: Amount to bridge in token units
            from_chain: Source chain (e.g., "arbitrum", "optimism")
            to_chain: Destination chain
            max_slippage: Maximum slippage tolerance (default 0.5%)

        Returns:
            BridgeQuote with fee, timing, and route information

        Raises:
            AcrossQuoteError: If quote cannot be retrieved
        """
        # Validate transfer parameters
        is_valid, error_msg = self.validate_transfer(token, amount, from_chain, to_chain)
        if not is_valid:
            raise AcrossQuoteError(error_msg or "Invalid transfer parameters")

        # Get chain IDs
        from_chain_id = ACROSS_CHAIN_IDS.get(from_chain.lower())
        to_chain_id = ACROSS_CHAIN_IDS.get(to_chain.lower())

        if from_chain_id is None or to_chain_id is None:
            raise AcrossQuoteError(f"Unsupported chain: {from_chain if from_chain_id is None else to_chain}")

        # Get token address
        token_address = self._get_token_address(token, from_chain_id)
        if token_address is None:
            raise AcrossQuoteError(f"Token {token} not supported on {from_chain}")

        # Convert amount to wei/smallest unit
        decimals = self._get_token_decimals(token, from_chain_id)
        amount_wei = int(amount * Decimal(10**decimals))

        try:
            # Call Across suggested-fees API
            response = self._call_api(
                "suggested-fees",
                params={
                    "token": token_address,
                    "inputToken": token_address,
                    "outputToken": self._get_token_address(token, to_chain_id) or token_address,
                    "originChainId": from_chain_id,
                    "destinationChainId": to_chain_id,
                    "amount": str(amount_wei),
                    "skipAmountLimit": "false",
                },
            )

            # Parse response
            total_relay_fee = int(response.get("totalRelayFee", {}).get("total", "0"))
            lp_fee = int(response.get("lpFee", {}).get("total", "0"))
            relayer_gas_fee = int(response.get("relayerGasFee", {}).get("total", "0"))
            relayer_capital_fee = int(response.get("relayerCapitalFee", {}).get("total", "0"))

            # Calculate output amount
            output_amount_wei = amount_wei - total_relay_fee
            output_amount = Decimal(output_amount_wei) / Decimal(10**decimals)
            fee_amount = Decimal(total_relay_fee) / Decimal(10**decimals)

            # Get estimated time
            estimated_time = self.estimate_completion_time(from_chain, to_chain)

            # Build quote
            quote = BridgeQuote(
                bridge_name=self.name,
                token=token,
                input_amount=amount,
                output_amount=output_amount,
                from_chain=from_chain.lower(),
                to_chain=to_chain.lower(),
                fee_amount=fee_amount,
                gas_fee_amount=Decimal(relayer_gas_fee) / Decimal(10**decimals),
                relayer_fee_amount=Decimal(relayer_capital_fee + lp_fee) / Decimal(10**decimals),
                estimated_time_seconds=estimated_time,
                slippage_tolerance=max_slippage,
                route_data={
                    "token_address": token_address,
                    "from_chain_id": from_chain_id,
                    "to_chain_id": to_chain_id,
                    "amount_wei": str(amount_wei),
                    "output_amount_wei": str(output_amount_wei),
                    "spoke_pool_address": ACROSS_SPOKE_POOL_ADDRESSES.get(from_chain_id),
                    "timestamp": response.get("timestamp"),
                    "fill_deadline": response.get("fillDeadline"),
                    "exclusivity_deadline": response.get("exclusivityDeadline"),
                    "exclusive_relayer": response.get("exclusiveRelayer"),
                },
                quote_id=response.get("quoteTimestamp"),
            )

            logger.info(
                f"Across quote: {amount} {token} {from_chain} -> {to_chain}, "
                f"fee: {fee_amount} {token} ({quote.fee_percentage:.2f}%)"
            )

            return quote

        except requests.RequestException as e:
            raise AcrossQuoteError(f"API request failed: {e}") from e
        except (KeyError, ValueError) as e:
            raise AcrossQuoteError(f"Failed to parse quote response: {e}") from e

    def build_deposit_tx(
        self,
        quote: BridgeQuote,
        recipient: str,
    ) -> dict[str, Any]:
        """Build the deposit transaction for an Across bridge transfer.

        Creates the transaction data to call depositV3() on the SpokePool contract.

        Args:
            quote: BridgeQuote from get_quote()
            recipient: Address to receive tokens on destination chain

        Returns:
            Transaction data dict with 'to', 'value', 'data' fields

        Raises:
            AcrossTransactionError: If transaction cannot be built
        """
        if quote.is_expired:
            raise AcrossTransactionError("Quote has expired")

        if not quote.route_data:
            raise AcrossTransactionError("Quote missing route data")

        try:
            # Extract route data
            spoke_pool = quote.route_data.get("spoke_pool_address")
            if not spoke_pool:
                raise AcrossTransactionError("Missing spoke pool address in quote")

            token_address: str = quote.route_data.get("token_address", "")
            quote.route_data.get("from_chain_id", 0)
            to_chain_id: int = quote.route_data.get("to_chain_id", 0)
            amount_wei = int(quote.route_data.get("amount_wei", "0"))
            output_amount_wei = int(quote.route_data.get("output_amount_wei", "0"))

            if not token_address:
                raise AcrossTransactionError("Missing token address in quote")

            # Calculate fill deadline (quote expiry + buffer)
            fill_deadline = quote.route_data.get("fill_deadline")
            if not fill_deadline:
                # Default: 4 hours from now
                fill_deadline = int(time.time()) + (4 * 60 * 60)

            # Exclusivity settings
            exclusivity_deadline = quote.route_data.get("exclusivity_deadline", 0)
            exclusive_relayer = quote.route_data.get("exclusive_relayer")
            if not exclusive_relayer or exclusive_relayer == "0x":
                exclusive_relayer = "0x0000000000000000000000000000000000000000"

            # Get output token address (same token on destination)
            output_token: str = self._get_token_address(quote.token, to_chain_id) or token_address

            # Build depositV3 calldata
            # Function signature: depositV3(
            #   address depositor,
            #   address recipient,
            #   address inputToken,
            #   address outputToken,
            #   uint256 inputAmount,
            #   uint256 outputAmount,
            #   uint256 destinationChainId,
            #   address exclusiveRelayer,
            #   uint32 quoteTimestamp,
            #   uint32 fillDeadline,
            #   uint32 exclusivityDeadline,
            #   bytes message
            # )
            function_selector = bytes.fromhex("7b939232")  # depositV3 selector

            # For quoteTimestamp, use current timestamp if not in route_data
            quote_timestamp = quote.route_data.get("timestamp")
            if not quote_timestamp:
                quote_timestamp = int(time.time())

            # Encode parameters
            encoded_params = encode(
                [
                    "address",  # depositor (will be msg.sender)
                    "address",  # recipient
                    "address",  # inputToken
                    "address",  # outputToken
                    "uint256",  # inputAmount
                    "uint256",  # outputAmount
                    "uint256",  # destinationChainId
                    "address",  # exclusiveRelayer
                    "uint32",  # quoteTimestamp
                    "uint32",  # fillDeadline
                    "uint32",  # exclusivityDeadline
                    "bytes",  # message (empty for simple transfers)
                ],
                [
                    bytes.fromhex(recipient[2:]) if recipient.startswith("0x") else bytes.fromhex(recipient),
                    bytes.fromhex(recipient[2:]) if recipient.startswith("0x") else bytes.fromhex(recipient),
                    bytes.fromhex(token_address[2:])
                    if token_address.startswith("0x")
                    else bytes.fromhex(token_address),
                    bytes.fromhex(output_token[2:]) if output_token.startswith("0x") else bytes.fromhex(output_token),
                    amount_wei,
                    output_amount_wei,
                    to_chain_id,
                    bytes.fromhex(exclusive_relayer[2:])
                    if exclusive_relayer.startswith("0x")
                    else bytes.fromhex(exclusive_relayer),
                    quote_timestamp,
                    fill_deadline,
                    exclusivity_deadline,
                    b"",  # Empty message for simple transfers
                ],
            )

            calldata = function_selector + encoded_params

            # Determine ETH value (if bridging native ETH)
            value = 0
            if quote.token.upper() in ("ETH", "WETH"):
                # For ETH bridges, the contract wraps ETH automatically
                value = amount_wei

            tx_data = {
                "to": spoke_pool,
                "value": value,
                "data": "0x" + calldata.hex(),
            }

            logger.info(
                f"Built Across deposit tx: {quote.input_amount} {quote.token} "
                f"{quote.from_chain} -> {quote.to_chain}, recipient: {recipient}"
            )

            return tx_data

        except Exception as e:
            raise AcrossTransactionError(f"Failed to build deposit transaction: {e}") from e

    def check_status(
        self,
        bridge_deposit_id: str,
    ) -> BridgeStatus:
        """Check the status of an Across bridge transfer.

        Polls the Across API to check if a deposit has been filled
        on the destination chain.

        Args:
            bridge_deposit_id: Source chain deposit transaction hash

        Returns:
            BridgeStatus with current transfer status

        Raises:
            AcrossStatusError: If status cannot be retrieved
        """
        if not bridge_deposit_id:
            raise AcrossStatusError("bridge_deposit_id is required")

        # Normalize tx hash
        tx_hash = bridge_deposit_id
        if not tx_hash.startswith("0x"):
            tx_hash = "0x" + tx_hash

        try:
            # Call Across deposit/status API
            response = self._call_api(
                "deposit/status",
                params={"depositTxHash": tx_hash},
            )

            # Parse response
            status_str = response.get("status", "").lower()

            # Map Across status to our BridgeStatusEnum
            status_map = {
                "pending": BridgeStatusEnum.PENDING,
                "filled": BridgeStatusEnum.FILLED,
                "expired": BridgeStatusEnum.EXPIRED,
                "slow_fill_requested": BridgeStatusEnum.IN_FLIGHT,
            }
            status = status_map.get(status_str, BridgeStatusEnum.PENDING)

            # Check if fully completed (filled + confirmations)
            if status == BridgeStatusEnum.FILLED:
                fill_tx = response.get("fillTxHash")
                if fill_tx:
                    status = BridgeStatusEnum.COMPLETED

            # Build status object
            deposit_info = response.get("deposit", {})

            # Parse amounts
            input_amount = Decimal("0")
            output_amount = None
            token = deposit_info.get("inputToken", "")

            if "inputAmount" in deposit_info:
                # Get token decimals (default to 18 for ETH-like)
                decimals = 18
                if "USDC" in token.upper() or "USDT" in token.upper():
                    decimals = 6
                input_amount = Decimal(deposit_info["inputAmount"]) / Decimal(10**decimals)

            if "outputAmount" in deposit_info:
                decimals = 18
                if "USDC" in token.upper() or "USDT" in token.upper():
                    decimals = 6
                output_amount = Decimal(deposit_info["outputAmount"]) / Decimal(10**decimals)

            # Get chain names
            from_chain_id = deposit_info.get("originChainId", 0)
            to_chain_id = deposit_info.get("destinationChainId", 0)
            from_chain = ACROSS_CHAIN_ID_TO_NAME.get(from_chain_id, str(from_chain_id))
            to_chain = ACROSS_CHAIN_ID_TO_NAME.get(to_chain_id, str(to_chain_id))

            # Parse timestamps
            deposited_at = None
            filled_at = None
            if "depositTime" in response:
                deposited_at = datetime.fromtimestamp(response["depositTime"], tz=UTC)
            if "fillTime" in response:
                filled_at = datetime.fromtimestamp(response["fillTime"], tz=UTC)

            bridge_status = BridgeStatus(
                bridge_name=self.name,
                bridge_deposit_id=tx_hash,
                status=status,
                from_chain=from_chain,
                to_chain=to_chain,
                token=token,
                input_amount=input_amount,
                output_amount=output_amount,
                source_tx_hash=tx_hash,
                destination_tx_hash=response.get("fillTxHash"),
                deposited_at=deposited_at,
                filled_at=filled_at,
                completed_at=filled_at if status == BridgeStatusEnum.COMPLETED else None,
                relay_id=response.get("relayHash"),
            )

            logger.debug(f"Across status for {tx_hash}: {status.value}")

            return bridge_status

        except requests.RequestException as e:
            raise AcrossStatusError(f"API request failed: {e}") from e
        except (KeyError, ValueError) as e:
            raise AcrossStatusError(f"Failed to parse status response: {e}") from e

    def estimate_completion_time(
        self,
        from_chain: str,
        to_chain: str,
    ) -> int:
        """Estimate completion time for a route.

        Returns typical completion time based on historical data
        and relayer network activity.

        Args:
            from_chain: Source chain identifier
            to_chain: Destination chain identifier

        Returns:
            Estimated completion time in seconds

        Raises:
            AcrossError: If route is not supported
        """
        from_chain_lower = from_chain.lower()
        to_chain_lower = to_chain.lower()

        # Check if route is supported
        if not self.supports_route(from_chain, to_chain):
            raise AcrossError(f"Route {from_chain} -> {to_chain} not supported")

        # Get estimated time from constants
        chain_times = ACROSS_COMPLETION_TIMES.get(from_chain_lower, {})
        estimated_time = chain_times.get(to_chain_lower)

        if estimated_time is not None:
            return estimated_time

        # Default estimate if not in constants
        # L2 -> L2: ~2 minutes, L2 -> L1: ~4 minutes, L1 -> L2: ~3 minutes
        if from_chain_lower == "ethereum":
            return 180  # 3 minutes
        elif to_chain_lower == "ethereum":
            return 240  # 4 minutes
        else:
            return 120  # 2 minutes

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _get_session(self) -> requests.Session:
        """Get or create HTTP session with retry logic."""
        if self._session is None:
            self._session = requests.Session()

            # Configure retries
            retry_strategy = Retry(
                total=self._config.max_retries,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("https://", adapter)
            self._session.mount("http://", adapter)

        return self._session

    def _call_api(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        method: str = "GET",
    ) -> dict[str, Any]:
        """Make an API call to Across.

        Args:
            endpoint: API endpoint (e.g., "suggested-fees")
            params: Query parameters
            method: HTTP method

        Returns:
            JSON response as dict

        Raises:
            requests.RequestException: On network/API errors
        """
        url = f"{self._config.api_base_url}/{endpoint}"
        session = self._get_session()

        logger.debug(f"Across API call: {method} {url} params={params}")

        if method.upper() == "GET":
            response = session.get(
                url,
                params=params,
                timeout=self._config.request_timeout,
            )
        else:
            response = session.post(
                url,
                json=params,
                timeout=self._config.request_timeout,
            )

        response.raise_for_status()
        return response.json()

    def _build_routes(self) -> list[BridgeRoute]:
        """Build list of supported routes."""
        routes = []

        # Generate all valid chain pairs
        chains = list(ACROSS_CHAIN_IDS.keys())
        for from_chain in chains:
            for to_chain in chains:
                if from_chain == to_chain:
                    continue

                # Use the supported tokens list - all tokens are supported on all chains
                common_tokens = ACROSS_SUPPORTED_TOKENS.copy()

                if not common_tokens:
                    continue

                # Add ETH if WETH is supported
                if "WETH" in common_tokens and "ETH" not in common_tokens:
                    common_tokens.append("ETH")

                estimated_time = ACROSS_COMPLETION_TIMES.get(from_chain, {}).get(to_chain, 180)

                routes.append(
                    BridgeRoute(
                        from_chain=from_chain,
                        to_chain=to_chain,
                        tokens=common_tokens,
                        estimated_time_seconds=estimated_time,
                        is_active=True,
                    )
                )

        return routes

    def _get_token_address(self, token: str, chain_id: int) -> str:
        """Get token address for a chain using TokenResolver.

        Args:
            token: Token symbol
            chain_id: Chain ID

        Returns:
            Token address

        Raises:
            TokenResolutionError: If the token cannot be resolved
        """
        token_upper = token.upper()

        # ETH uses WETH address for deposits
        if token_upper == "ETH":
            token_upper = "WETH"

        chain_name = _CHAIN_ID_TO_NAME.get(chain_id)
        if chain_name is None:
            raise TokenResolutionError(
                token=token_upper,
                chain=str(chain_id),
                reason=f"[AcrossBridgeAdapter] Unknown chain ID: {chain_id}",
            )
        try:
            resolved = self._token_resolver.resolve(token_upper, chain_name)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token_upper,
                chain=str(chain_name),
                reason=f"[AcrossBridgeAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_token_decimals(self, token: str, chain_id: int | None = None) -> int:
        """Get token decimals using TokenResolver.

        Args:
            token: Token symbol
            chain_id: Optional chain ID for resolver lookup

        Returns:
            Number of decimals

        Raises:
            TokenResolutionError: If decimals cannot be determined
        """
        if chain_id is None:
            raise TokenResolutionError(
                token=token,
                chain="unknown",
                reason="[AcrossBridgeAdapter] chain_id is required for decimals lookup",
            )
        chain_name = _CHAIN_ID_TO_NAME.get(chain_id)
        if chain_name is None:
            raise TokenResolutionError(
                token=token,
                chain=str(chain_id),
                reason=f"[AcrossBridgeAdapter] Unknown chain ID: {chain_id}",
            )
        try:
            resolved = self._token_resolver.resolve(token, chain_name)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(chain_name),
                reason=f"[AcrossBridgeAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "AcrossBridgeAdapter",
    "AcrossConfig",
    "AcrossError",
    "AcrossQuoteError",
    "AcrossTransactionError",
    "AcrossStatusError",
    "ACROSS_CHAIN_IDS",
    "ACROSS_SPOKE_POOL_ADDRESSES",
    "ACROSS_SUPPORTED_TOKENS",
]
