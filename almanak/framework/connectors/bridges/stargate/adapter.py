"""Stargate Bridge Adapter Implementation.

This module provides the StargateBridgeAdapter class for interacting with the
Stargate protocol built on LayerZero messaging for cross-chain transfers.

Stargate Protocol:
- Unified liquidity pools across chains for native asset transfers
- LayerZero messaging layer for cross-chain communication
- Instant guaranteed finality on destination chain
- Delta algorithm for rebalancing across chains

Supported Operations:
- get_quote(): Get fee/time estimates from Stargate API
- build_bridge_tx(): Build source chain transaction for transfer
- check_status(): Poll LayerZero scan API for message delivery
- estimate_completion_time(): Get typical completion times

Example:
    adapter = StargateBridgeAdapter()

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
from datetime import datetime
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
}


# =============================================================================
# Exceptions
# =============================================================================


class StargateError(BridgeError):
    """Base exception for Stargate-related errors."""

    pass


class StargateQuoteError(StargateError, BridgeQuoteError):
    """Error when retrieving a Stargate quote."""

    pass


class StargateTransactionError(StargateError, BridgeTransactionError):
    """Error when building or submitting a Stargate transaction."""

    pass


class StargateStatusError(StargateError, BridgeStatusError):
    """Error when checking Stargate transfer status."""

    pass


# =============================================================================
# Constants
# =============================================================================


# LayerZero chain IDs (endpoint IDs) for Stargate V2
# These differ from EVM chain IDs
STARGATE_CHAIN_IDS: dict[str, int] = {
    "ethereum": 30101,
    "arbitrum": 30110,
    "optimism": 30111,
    "polygon": 30109,
    "base": 30184,
    "avalanche": 30106,
    "bsc": 30102,
}

# EVM chain IDs for transaction building
EVM_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "polygon": 137,
    "base": 8453,
    "avalanche": 43114,
    "bsc": 56,
}

# Reverse mapping for LayerZero chain IDs
STARGATE_CHAIN_ID_TO_NAME: dict[int, str] = {v: k for k, v in STARGATE_CHAIN_IDS.items()}

# Stargate V2 Router (StargatePoolNative for ETH, StargatePool for tokens)
# These are the OFT/Pool addresses for Stargate V2
STARGATE_ROUTER_ADDRESSES: dict[int, dict[str, str]] = {
    1: {  # Ethereum
        "USDC": "0xc026395860Db2d07ee33e05fE50ed7bD583189C7",
        "USDT": "0x933597a323Eb81cAe705C5bC29985172fd5A3973",
        "ETH": "0x77b2043768d28E9C9aB44E1aBfC95944bcE57931",
    },
    42161: {  # Arbitrum
        "USDC": "0xe8CDF27AcD73a434D661C84887215F7598e7d0d3",
        "USDT": "0xcE8CcA271Ebc0533920C83d39F417ED6A0abB7D0",
        "ETH": "0xA45B5130f36CDcA45667738e2a258AB09f4A5f7F",
    },
    10: {  # Optimism
        "USDC": "0xcE8CcA271Ebc0533920C83d39F417ED6A0abB7D0",
        "USDT": "0x19cFCE47eD54a88614648DC3f19A5980097007dD",
        "ETH": "0xe8CDF27AcD73a434D661C84887215F7598e7d0d3",
    },
    137: {  # Polygon
        "USDC": "0x9Aa02D4Fae7F58b8E8f34c66E756cC734DAc7fe4",
        "USDT": "0xd47b03ee6d86Cf251ee7860FB2ACf9f91B9fD4d7",
    },
    8453: {  # Base
        "USDC": "0x27a16dc786820B16E5c9028b75B99F6f604b5d26",
        "ETH": "0xdc181Bd607330aeeBEF6ea62e03e5e1Fb4B6F7C7",
    },
    43114: {  # Avalanche
        "USDC": "0x5634c4a5FEd09819E3c46D86A965Dd9447d86e47",
        "USDT": "0x12dC9256Acc9895B076f6638D628382881e62CeE",
    },
    56: {  # BSC
        "USDT": "0x138EB30f73BC423c6455C53df6D89CB01d9eBc63",
    },
}

# Pool IDs for Stargate V1 (legacy reference)
STARGATE_POOL_IDS: dict[str, int] = {
    "USDC": 1,
    "USDT": 2,
    "ETH": 13,
    "SGETH": 13,
    "FRAX": 7,
    "USDD": 11,
    "MAI": 16,
}

# Supported tokens
STARGATE_SUPPORTED_TOKENS: list[str] = ["USDC", "USDT", "ETH"]

# Estimated completion times in seconds per route
# Stargate/LayerZero is generally fast due to instant finality
STARGATE_COMPLETION_TIMES: dict[str, dict[str, int]] = {
    "ethereum": {
        "arbitrum": 90,
        "optimism": 90,
        "polygon": 120,
        "base": 90,
        "avalanche": 120,
        "bsc": 120,
    },
    "arbitrum": {
        "ethereum": 120,
        "optimism": 60,
        "polygon": 90,
        "base": 60,
        "avalanche": 90,
        "bsc": 90,
    },
    "optimism": {
        "ethereum": 120,
        "arbitrum": 60,
        "polygon": 90,
        "base": 60,
        "avalanche": 90,
        "bsc": 90,
    },
    "polygon": {
        "ethereum": 120,
        "arbitrum": 90,
        "optimism": 90,
        "base": 90,
        "avalanche": 90,
        "bsc": 90,
    },
    "base": {
        "ethereum": 120,
        "arbitrum": 60,
        "optimism": 60,
        "polygon": 90,
        "avalanche": 90,
        "bsc": 90,
    },
    "avalanche": {
        "ethereum": 120,
        "arbitrum": 90,
        "optimism": 90,
        "polygon": 90,
        "base": 90,
        "bsc": 90,
    },
    "bsc": {
        "ethereum": 120,
        "arbitrum": 90,
        "optimism": 90,
        "polygon": 90,
        "base": 90,
        "avalanche": 90,
    },
}

# Default timeout for bridge operations (30 minutes)
DEFAULT_TIMEOUT_SECONDS = 1800


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class StargateConfig:
    """Configuration for Stargate bridge adapter.

    Attributes:
        api_base_url: Stargate API base URL
        layerzero_scan_url: LayerZero scan API URL for message tracking
        timeout_seconds: Timeout for bridge operations (default 30 min)
        request_timeout: HTTP request timeout in seconds
        max_retries: Maximum number of retry attempts for API calls
    """

    api_base_url: str = "https://api.stargate.finance/v1"
    layerzero_scan_url: str = "https://api.layerzeroscan.com"
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    request_timeout: int = 30
    max_retries: int = 3

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.timeout_seconds <= 0:
            raise StargateError("timeout_seconds must be positive")
        if self.request_timeout <= 0:
            raise StargateError("request_timeout must be positive")


# =============================================================================
# Adapter Implementation
# =============================================================================


class StargateBridgeAdapter(BridgeAdapter):
    """Stargate Protocol bridge adapter implementation.

    Provides integration with the Stargate bridge for cross-chain transfers
    using LayerZero messaging infrastructure.

    Features:
    - Unified liquidity pools for efficient capital utilization
    - Instant guaranteed finality via LayerZero messaging
    - Native asset transfers without wrapped tokens
    - Support for USDC, USDT, ETH across major chains

    Example:
        adapter = StargateBridgeAdapter()

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

    def __init__(self, config: StargateConfig | None = None, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize Stargate bridge adapter.

        Args:
            config: Optional configuration. Uses defaults if not provided.
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self._config = config or StargateConfig()
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
        return "Stargate"

    @property
    def supported_tokens(self) -> list[str]:
        """Get list of supported tokens."""
        return STARGATE_SUPPORTED_TOKENS.copy()

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
        """Get a quote for bridging tokens via Stargate.

        Calculates fees including LayerZero messaging fees and
        protocol fees for the specified transfer.

        Args:
            token: Token symbol (e.g., "USDC", "USDT", "ETH")
            amount: Amount to bridge in token units
            from_chain: Source chain (e.g., "arbitrum", "optimism")
            to_chain: Destination chain
            max_slippage: Maximum slippage tolerance (default 0.5%)

        Returns:
            BridgeQuote with fee, timing, and route information

        Raises:
            StargateQuoteError: If quote cannot be retrieved
        """
        # Validate transfer parameters
        is_valid, error_msg = self.validate_transfer(token, amount, from_chain, to_chain)
        if not is_valid:
            raise StargateQuoteError(error_msg or "Invalid transfer parameters")

        # Get chain IDs
        from_lz_chain_id = STARGATE_CHAIN_IDS.get(from_chain.lower())
        to_lz_chain_id = STARGATE_CHAIN_IDS.get(to_chain.lower())
        from_evm_chain_id = EVM_CHAIN_IDS.get(from_chain.lower())
        to_evm_chain_id = EVM_CHAIN_IDS.get(to_chain.lower())

        if from_lz_chain_id is None or to_lz_chain_id is None:
            raise StargateQuoteError(f"Unsupported chain: {from_chain if from_lz_chain_id is None else to_chain}")

        if from_evm_chain_id is None or to_evm_chain_id is None:
            raise StargateQuoteError(f"Unsupported EVM chain: {from_chain if from_evm_chain_id is None else to_chain}")

        # Get pool/router address
        pool_address = self._get_pool_address(token, from_evm_chain_id)
        if pool_address is None:
            raise StargateQuoteError(f"Token {token} not supported on {from_chain}")

        # Get token decimals and convert amount
        decimals = self._get_token_decimals(token, from_evm_chain_id)
        amount_wei = int(amount * Decimal(10**decimals))

        try:
            # Estimate fees using the Stargate API or calculate locally
            # Stargate V2 uses OFT send with LayerZero fees
            lz_fee = self._estimate_layerzero_fee(
                from_chain.lower(),
                to_chain.lower(),
                token,
                amount_wei,
            )

            # Protocol fee (typically 0.06% for Stargate)
            protocol_fee_rate = Decimal("0.0006")
            protocol_fee_wei = int(amount_wei * protocol_fee_rate)
            protocol_fee = Decimal(protocol_fee_wei) / Decimal(10**decimals)

            # Total fee
            total_fee = protocol_fee + lz_fee

            # Output amount after fees
            output_amount = amount - protocol_fee

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
                fee_amount=total_fee,
                gas_fee_amount=lz_fee,
                relayer_fee_amount=protocol_fee,
                estimated_time_seconds=estimated_time,
                slippage_tolerance=max_slippage,
                route_data={
                    "pool_address": pool_address,
                    "from_lz_chain_id": from_lz_chain_id,
                    "to_lz_chain_id": to_lz_chain_id,
                    "from_evm_chain_id": from_evm_chain_id,
                    "to_evm_chain_id": to_evm_chain_id,
                    "amount_wei": str(amount_wei),
                    "min_amount_wei": str(int(amount_wei * (1 - max_slippage))),
                    "lz_fee_wei": str(int(lz_fee * Decimal(10**18))),
                    "token": token,
                },
                quote_id=f"sg_{int(time.time())}_{from_chain}_{to_chain}",
            )

            logger.info(
                f"Stargate quote: {amount} {token} {from_chain} -> {to_chain}, "
                f"fee: {total_fee} ({quote.fee_percentage:.3f}%)"
            )

            return quote

        except requests.RequestException as e:
            raise StargateQuoteError(f"API request failed: {e}") from e
        except (KeyError, ValueError) as e:
            raise StargateQuoteError(f"Failed to calculate quote: {e}") from e

    def build_deposit_tx(
        self,
        quote: BridgeQuote,
        recipient: str,
    ) -> dict[str, Any]:
        """Build the deposit transaction for a Stargate bridge transfer.

        Creates the transaction data to call send() on the Stargate OFT/Pool contract.

        Args:
            quote: BridgeQuote from get_quote()
            recipient: Address to receive tokens on destination chain

        Returns:
            Transaction data dict with 'to', 'value', 'data' fields

        Raises:
            StargateTransactionError: If transaction cannot be built
        """
        if quote.is_expired:
            raise StargateTransactionError("Quote has expired")

        if not quote.route_data:
            raise StargateTransactionError("Quote missing route data")

        try:
            # Extract route data
            pool_address = quote.route_data.get("pool_address")
            if not pool_address:
                raise StargateTransactionError("Missing pool address in quote")

            to_lz_chain_id: int = quote.route_data.get("to_lz_chain_id", 0)
            amount_wei = int(quote.route_data.get("amount_wei", "0"))
            min_amount_wei = int(quote.route_data.get("min_amount_wei", "0"))
            lz_fee_wei = int(quote.route_data.get("lz_fee_wei", "0"))
            token: str = quote.route_data.get("token", "")

            if not to_lz_chain_id:
                raise StargateTransactionError("Missing destination chain ID in quote")

            # Normalize recipient address
            if not recipient.startswith("0x"):
                recipient = "0x" + recipient
            recipient_bytes = bytes.fromhex(recipient[2:].zfill(64))

            # Build Stargate V2 OFT send() calldata
            # Function signature: send(SendParam calldata _sendParam, MessagingFee calldata _fee, address _refundAddress) payable
            # SendParam struct: (uint32 dstEid, bytes32 to, uint256 amountLD, uint256 minAmountLD, bytes extraOptions, bytes composeMsg, bytes oftCmd)
            # MessagingFee struct: (uint256 nativeFee, uint256 lzTokenFee)

            # Function selector for send()
            function_selector = bytes.fromhex("c7c7f5b3")  # send(SendParam,MessagingFee,address)

            # Build SendParam tuple
            # extraOptions: empty for basic transfer
            # composeMsg: empty for basic transfer
            # oftCmd: empty for basic transfer
            extra_options = b""
            compose_msg = b""
            oft_cmd = b""

            # Encode parameters using ABI encoding
            # The send function takes complex nested structs, encode manually
            encoded_params = encode(
                [
                    "(uint32,bytes32,uint256,uint256,bytes,bytes,bytes)",  # SendParam
                    "(uint256,uint256)",  # MessagingFee
                    "address",  # refundAddress
                ],
                [
                    (
                        to_lz_chain_id,  # dstEid
                        recipient_bytes,  # to (bytes32)
                        amount_wei,  # amountLD
                        min_amount_wei,  # minAmountLD
                        extra_options,  # extraOptions
                        compose_msg,  # composeMsg
                        oft_cmd,  # oftCmd
                    ),
                    (
                        lz_fee_wei,  # nativeFee
                        0,  # lzTokenFee (0 for native fee payment)
                    ),
                    bytes.fromhex(recipient[2:]),  # refundAddress
                ],
            )

            calldata = function_selector + encoded_params

            # Determine ETH value
            # For ETH bridges, send the amount + LZ fee
            # For token bridges, only send LZ fee
            if token.upper() == "ETH":
                value = amount_wei + lz_fee_wei
            else:
                value = lz_fee_wei

            tx_data = {
                "to": pool_address,
                "value": value,
                "data": "0x" + calldata.hex(),
            }

            logger.info(
                f"Built Stargate deposit tx: {quote.input_amount} {quote.token} "
                f"{quote.from_chain} -> {quote.to_chain}, recipient: {recipient}"
            )

            return tx_data

        except Exception as e:
            raise StargateTransactionError(f"Failed to build deposit transaction: {e}") from e

    def check_status(
        self,
        bridge_deposit_id: str,
    ) -> BridgeStatus:
        """Check the status of a Stargate bridge transfer.

        Polls the LayerZero scan API to check if the cross-chain message
        has been delivered on the destination chain.

        Args:
            bridge_deposit_id: Source chain deposit transaction hash

        Returns:
            BridgeStatus with current transfer status

        Raises:
            StargateStatusError: If status cannot be retrieved
        """
        if not bridge_deposit_id:
            raise StargateStatusError("bridge_deposit_id is required")

        # Normalize tx hash
        tx_hash = bridge_deposit_id
        if not tx_hash.startswith("0x"):
            tx_hash = "0x" + tx_hash

        try:
            # Call LayerZero scan API to get message status
            response = self._call_layerzero_api(
                f"v1/messages/tx/{tx_hash}",
            )

            # Parse response
            messages = response.get("messages", [])
            if not messages:
                # Message not yet indexed
                return BridgeStatus(
                    bridge_name=self.name,
                    bridge_deposit_id=tx_hash,
                    status=BridgeStatusEnum.PENDING,
                    from_chain="",
                    to_chain="",
                    token="",
                    input_amount=Decimal("0"),
                    source_tx_hash=tx_hash,
                )

            # Get first message (typically only one per tx)
            message = messages[0]

            # Map LayerZero status to our BridgeStatusEnum
            lz_status = message.get("status", "").upper()
            status_map = {
                "INFLIGHT": BridgeStatusEnum.IN_FLIGHT,
                "DELIVERED": BridgeStatusEnum.COMPLETED,
                "FAILED": BridgeStatusEnum.FAILED,
                "BLOCKED": BridgeStatusEnum.FAILED,
                "CONFIRMING": BridgeStatusEnum.DEPOSITED,
            }
            status = status_map.get(lz_status, BridgeStatusEnum.PENDING)

            # Get chain names
            src_chain_id = message.get("srcChainId", 0)
            dst_chain_id = message.get("dstChainId", 0)
            from_chain = STARGATE_CHAIN_ID_TO_NAME.get(src_chain_id, str(src_chain_id))
            to_chain = STARGATE_CHAIN_ID_TO_NAME.get(dst_chain_id, str(dst_chain_id))

            # Parse timestamps
            deposited_at = None
            completed_at = None
            created_str = message.get("created")
            if created_str:
                try:
                    deposited_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                except ValueError:
                    pass

            updated_str = message.get("updated")
            if updated_str and status == BridgeStatusEnum.COMPLETED:
                try:
                    completed_at = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                except ValueError:
                    pass

            bridge_status = BridgeStatus(
                bridge_name=self.name,
                bridge_deposit_id=tx_hash,
                status=status,
                from_chain=from_chain,
                to_chain=to_chain,
                token="",  # LayerZero API doesn't provide token info
                input_amount=Decimal("0"),  # Amount not provided by API
                source_tx_hash=message.get("srcTxHash", tx_hash),
                destination_tx_hash=message.get("dstTxHash"),
                deposited_at=deposited_at,
                completed_at=completed_at,
                relay_id=message.get("guid"),
            )

            logger.debug(f"Stargate status for {tx_hash}: {status.value}")

            return bridge_status

        except requests.RequestException as e:
            raise StargateStatusError(f"API request failed: {e}") from e
        except (KeyError, ValueError) as e:
            raise StargateStatusError(f"Failed to parse status response: {e}") from e

    def estimate_completion_time(
        self,
        from_chain: str,
        to_chain: str,
    ) -> int:
        """Estimate completion time for a route.

        Returns typical completion time based on LayerZero messaging
        and chain finality requirements.

        Args:
            from_chain: Source chain identifier
            to_chain: Destination chain identifier

        Returns:
            Estimated completion time in seconds

        Raises:
            StargateError: If route is not supported
        """
        from_chain_lower = from_chain.lower()
        to_chain_lower = to_chain.lower()

        # Check if route is supported
        if not self.supports_route(from_chain, to_chain):
            raise StargateError(f"Route {from_chain} -> {to_chain} not supported")

        # Get estimated time from constants
        chain_times = STARGATE_COMPLETION_TIMES.get(from_chain_lower, {})
        estimated_time = chain_times.get(to_chain_lower)

        if estimated_time is not None:
            return estimated_time

        # Default estimate based on chain types
        # L2 -> L2: ~60-90 seconds, L2 -> L1: ~120 seconds, L1 -> L2: ~90 seconds
        if from_chain_lower == "ethereum":
            return 90
        elif to_chain_lower == "ethereum":
            return 120
        else:
            return 60

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

    def _call_layerzero_api(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make an API call to LayerZero scan.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            JSON response as dict

        Raises:
            requests.RequestException: On network/API errors
        """
        url = f"{self._config.layerzero_scan_url}/{endpoint}"
        session = self._get_session()

        logger.debug(f"LayerZero API call: GET {url} params={params}")

        response = session.get(
            url,
            params=params,
            timeout=self._config.request_timeout,
        )

        response.raise_for_status()
        return response.json()

    def _build_routes(self) -> list[BridgeRoute]:
        """Build list of supported routes."""
        routes = []

        # Generate all valid chain pairs based on token support
        chains = list(STARGATE_CHAIN_IDS.keys())
        for from_chain in chains:
            for to_chain in chains:
                if from_chain == to_chain:
                    continue

                from_evm_id = EVM_CHAIN_IDS.get(from_chain)
                to_evm_id = EVM_CHAIN_IDS.get(to_chain)

                if from_evm_id is None or to_evm_id is None:
                    continue

                # Get tokens supported on both chains
                from_tokens = set(STARGATE_ROUTER_ADDRESSES.get(from_evm_id, {}).keys())
                to_tokens = set(STARGATE_ROUTER_ADDRESSES.get(to_evm_id, {}).keys())
                common_tokens = list(from_tokens & to_tokens)

                if not common_tokens:
                    continue

                estimated_time = STARGATE_COMPLETION_TIMES.get(from_chain, {}).get(to_chain, 90)

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

    def _get_pool_address(self, token: str, chain_id: int) -> str | None:
        """Get Stargate pool/OFT address for a token on a chain.

        Args:
            token: Token symbol
            chain_id: EVM chain ID

        Returns:
            Pool address or None if not found
        """
        token_upper = token.upper()
        chain_pools = STARGATE_ROUTER_ADDRESSES.get(chain_id, {})
        return chain_pools.get(token_upper)

    def _get_token_address(self, token: str, chain_id: int) -> str:
        """Get token address for a chain using TokenResolver.

        Args:
            token: Token symbol
            chain_id: EVM chain ID

        Returns:
            Token address

        Raises:
            TokenResolutionError: If the token cannot be resolved
        """
        token_upper = token.upper()

        # Special case for ETH - return zero address
        if token_upper == "ETH":
            return "0x0000000000000000000000000000000000000000"

        chain_name = _CHAIN_ID_TO_NAME.get(chain_id)
        if chain_name is None:
            raise TokenResolutionError(
                token=token_upper,
                chain=str(chain_id),
                reason=f"[StargateBridgeAdapter] Unknown chain ID: {chain_id}",
            )
        try:
            resolved = self._token_resolver.resolve(token_upper, chain_name)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token_upper,
                chain=str(chain_name),
                reason=f"[StargateBridgeAdapter] Cannot resolve token: {e.reason}",
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
                reason="[StargateBridgeAdapter] chain_id is required for decimals lookup",
            )
        chain_name = _CHAIN_ID_TO_NAME.get(chain_id)
        if chain_name is None:
            raise TokenResolutionError(
                token=token,
                chain=str(chain_id),
                reason=f"[StargateBridgeAdapter] Unknown chain ID: {chain_id}",
            )
        try:
            resolved = self._token_resolver.resolve(token, chain_name)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(chain_name),
                reason=f"[StargateBridgeAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _estimate_layerzero_fee(
        self,
        from_chain: str,
        to_chain: str,
        token: str,
        amount_wei: int,
    ) -> Decimal:
        """Estimate LayerZero messaging fee.

        This is an approximate calculation. In production, the actual fee
        would be queried from the LayerZero endpoint contract.

        Args:
            from_chain: Source chain name
            to_chain: Destination chain name
            token: Token symbol
            amount_wei: Amount in wei

        Returns:
            Estimated fee in ETH
        """
        # Base fee varies by destination chain
        # These are approximate values in ETH
        base_fees: dict[str, Decimal] = {
            "ethereum": Decimal("0.001"),
            "arbitrum": Decimal("0.0003"),
            "optimism": Decimal("0.0003"),
            "polygon": Decimal("0.0005"),
            "base": Decimal("0.0003"),
            "avalanche": Decimal("0.0005"),
            "bsc": Decimal("0.0003"),
        }

        # Get base fee for destination
        base_fee = base_fees.get(to_chain, Decimal("0.0005"))

        # Add small amount for larger transfers (gas cost scales slightly)
        if amount_wei > 10**24:  # > ~1M in smallest unit
            base_fee *= Decimal("1.2")

        return base_fee


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "StargateBridgeAdapter",
    "StargateConfig",
    "StargateError",
    "StargateQuoteError",
    "StargateTransactionError",
    "StargateStatusError",
    "STARGATE_CHAIN_IDS",
    "STARGATE_ROUTER_ADDRESSES",
    "STARGATE_POOL_IDS",
    "STARGATE_SUPPORTED_TOKENS",
]
