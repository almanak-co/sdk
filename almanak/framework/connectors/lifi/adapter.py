"""LiFi Protocol Adapter.

This module provides the LiFiAdapter class for converting Intents to
executable transactions using the LiFi cross-chain aggregation protocol.

LiFi aggregates bridges (Across, Stargate, Hop, etc.) and DEXs (1inch,
0x, Paraswap, etc.) to find optimal routes for cross-chain and same-chain
swaps.

IMPORTANT: LiFi route data becomes stale quickly. The adapter marks
swap transactions as "deferred" and provides a `get_fresh_transaction()`
method to fetch fresh route data immediately before execution.

Approval flow: Standard ERC-20 approve (NO Permit2 needed). The approval
address comes from the quote response `estimate.approval_address`.

Example:
    from almanak.framework.connectors.lifi import LiFiAdapter, LiFiConfig

    config = LiFiConfig(chain_id=42161, wallet_address="0x...")
    adapter = LiFiAdapter(config)

    intent = SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount=Decimal("1000"),
    )
    bundle = adapter.compile_swap_intent(intent)
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ...data.tokens.exceptions import TokenResolutionError
from ...intents.vocabulary import IntentType, SwapIntent
from ...models.reproduction_bundle import ActionBundle
from .client import LiFiClient, LiFiConfig

if TYPE_CHECKING:
    from ...data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


def _parse_hex_int(value: Any) -> int:
    """Parse a value that may be hex-encoded (e.g. '0x0') or decimal."""
    if value is None:
        return 0
    s = str(value).strip()
    if not s:
        return 0
    return int(s, 16) if s.lower().startswith("0x") else int(s)


# ERC20 approve function selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"

# Max uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1

# Default gas estimates
LIFI_GAS_ESTIMATES: dict[str, int] = {
    "approve": 50000,
    "swap": 200000,
    "bridge": 300000,
}


@dataclass
class TransactionData:
    """Transaction data for execution.

    Attributes:
        to: Target contract address
        value: Native token value to send (in wei)
        data: Encoded calldata
        gas_estimate: Estimated gas
        description: Human-readable description
        tx_type: Type of transaction (approve, swap, bridge)
    """

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str
    tx_type: str = "swap"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "to": self.to,
            "value": str(self.value),
            "data": self.data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
            "tx_type": self.tx_type,
        }


class LiFiAdapter:
    """Adapter for LiFi protocol integration with the Intent system.

    This adapter converts high-level SwapIntents into executable transactions
    using the LiFi cross-chain aggregation protocol. Supports both same-chain
    swaps and cross-chain bridge+swap operations.

    Features:
    - Cross-chain swap aggregation (bridge + DEX routing)
    - Same-chain DEX aggregation
    - Standard ERC-20 approval (no Permit2)
    - Deferred transaction pattern (fresh routes at execution)

    Example:
        config = LiFiConfig(chain_id=42161, wallet_address="0x...")
        adapter = LiFiAdapter(config)

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
        )
        bundle = adapter.compile_swap_intent(intent)
    """

    def __init__(
        self,
        config: LiFiConfig,
        price_provider: dict[str, Decimal] | None = None,
        allow_placeholder_prices: bool = False,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the LiFi adapter.

        Args:
            config: LiFi client configuration
            price_provider: Price oracle dict (token symbol -> USD price). Required for
                production use to calculate accurate amounts from USD.
            allow_placeholder_prices: If False (default), raises ValueError when no
                price_provider is given. Set to True ONLY for unit tests.
            token_resolver: Optional TokenResolver instance for unified token resolution.
                If None, uses the default singleton from get_token_resolver().

        Raises:
            ValueError: If no price_provider is provided and allow_placeholder_prices is False.
        """
        self._using_placeholders = price_provider is None
        if self._using_placeholders and not allow_placeholder_prices:
            raise ValueError(
                "LiFiAdapter requires price_provider for production use. "
                "Pass a dict mapping token symbols to USD prices "
                "or set allow_placeholder_prices=True for testing only."
            )

        self.config = config
        self.client = LiFiClient(config)
        self.chain_id = config.chain_id
        self.wallet_address = config.wallet_address

        # Initialize token resolver
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from ...data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Price provider
        if self._using_placeholders:
            logger.warning(
                "LiFiAdapter using PLACEHOLDER PRICES. "
                "Slippage calculations will be INCORRECT. "
                "This is only acceptable for unit tests."
            )
            self._price_provider = self._get_placeholder_prices()
        else:
            self._price_provider = price_provider or {}

        logger.info(f"LiFiAdapter initialized for chain_id={self.chain_id}, wallet={self.wallet_address[:10]}...")

    def resolve_token_address(self, token: str, chain: str | None = None) -> str:
        """Resolve token symbol or address to address using TokenResolver.

        Args:
            token: Token symbol (e.g., "USDC") or address
            chain: Chain name override (defaults to config chain)

        Returns:
            Token address

        Raises:
            TokenResolutionError: If the token cannot be resolved
        """
        if token.startswith("0x") and len(token) == 42:
            return token

        chain_name = chain or self._get_chain_name()
        try:
            resolved = self._token_resolver.resolve(token, chain_name)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=chain_name,
                reason=f"[LiFiAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def get_token_decimals(self, token: str, chain: str | None = None) -> int:
        """Get token decimals using TokenResolver.

        Args:
            token: Token symbol or address
            chain: Chain name override (defaults to config chain)

        Returns:
            Token decimals

        Raises:
            TokenResolutionError: If decimals cannot be determined
        """
        chain_name = chain or self._get_chain_name()
        try:
            resolved = self._token_resolver.resolve(token, chain_name)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=chain_name,
                reason=f"[LiFiAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def compile_swap_intent(
        self,
        intent: SwapIntent,
        price_oracle: dict[str, Decimal] | None = None,
        destination_chain_id: int | None = None,
    ) -> ActionBundle:
        """Compile a SwapIntent to an ActionBundle using LiFi.

        Supports both same-chain swaps and cross-chain swaps.

        Args:
            intent: The SwapIntent to compile
            price_oracle: Optional price oracle for USD conversions
            destination_chain_id: Destination chain for cross-chain swaps.
                If None, uses same chain as source.

        Returns:
            ActionBundle containing transactions for execution
        """
        try:
            if price_oracle is None:
                price_oracle = self._price_provider

            # Resolve token addresses
            token_in_address = self.resolve_token_address(intent.from_token)
            token_out_address = self.resolve_token_address(
                intent.to_token,
                chain=self._chain_id_to_name(destination_chain_id) if destination_chain_id else None,
            )

            # Determine the swap amount in wei
            amount_in_wei = self._resolve_amount(intent, price_oracle)
            if amount_in_wei is None:
                return self._error_bundle(intent, "Could not resolve swap amount")

            # Determine chains
            from_chain_id = self.chain_id
            to_chain_id = destination_chain_id or self.chain_id
            is_cross_chain = from_chain_id != to_chain_id

            # Convert slippage from decimal to float for LiFi (0.005 = 0.5%)
            slippage = float(intent.max_slippage)

            # Get quote from LiFi
            quote = self.client.get_quote(
                from_chain_id=from_chain_id,
                to_chain_id=to_chain_id,
                from_token=token_in_address,
                to_token=token_out_address,
                from_amount=str(amount_in_wei),
                from_address=self.wallet_address,
                slippage=slippage,
            )

            # Build transactions list
            transactions: list[TransactionData] = []

            # Build approval transaction if needed.
            # NOTE: The approval spender comes from the current quote. If the route
            # changes when get_fresh_transaction() re-quotes, a different spender may
            # be returned. The deferred refresh logic in deferred_refresh.py will
            # update the approval tx spender to match the fresh quote.
            approval_address = quote.estimate.approval_address if quote.estimate else ""
            if approval_address and not self._is_native_token(token_in_address):
                approve_tx = self._build_approve_transaction(
                    token_address=token_in_address,
                    spender=approval_address,
                    amount=amount_in_wei,
                )
                if approve_tx:
                    transactions.append(approve_tx)

            # Build swap/bridge transaction (deferred - needs fresh data at execution)
            tx_request = quote.transaction_request
            tx_type = "bridge_deferred" if is_cross_chain else "swap_deferred"
            description_action = "Bridge" if is_cross_chain else "Swap"

            swap_tx = TransactionData(
                to=tx_request.to if tx_request else "",
                value=_parse_hex_int(tx_request.value) if tx_request else 0,
                data=tx_request.data if tx_request else "",
                gas_estimate=self._get_gas_estimate(quote),
                description=f"{description_action} {intent.from_token} -> {intent.to_token} via LiFi ({quote.tool})",
                tx_type=tx_type,
            )
            transactions.append(swap_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Compiled {'cross-chain ' if is_cross_chain else ''}swap intent: "
                f"{intent.from_token} -> {intent.to_token}, "
                f"amount_in={amount_in_wei}, to_amount={quote.get_to_amount()}, "
                f"tool={quote.tool}, type={quote.type}"
            )

            return ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "intent_id": intent.intent_id,
                    "from_token": intent.from_token,
                    "to_token": intent.to_token,
                    "token_in_address": token_in_address,
                    "token_out_address": token_out_address,
                    "amount_in": str(amount_in_wei),
                    "amount_out": str(quote.get_to_amount()),
                    "amount_out_min": str(quote.get_to_amount_min()),
                    "slippage": slippage,
                    "from_chain_id": from_chain_id,
                    "to_chain_id": to_chain_id,
                    "is_cross_chain": is_cross_chain,
                    "protocol": "lifi",
                    "tool": quote.tool,
                    "step_type": quote.type,
                    "gas_estimate": total_gas,
                    "execution_duration": quote.estimate.execution_duration if quote.estimate else 0,
                    # Include parameters for fetching fresh route at execution time
                    "deferred_swap": True,
                    "route_params": {
                        "from_chain_id": from_chain_id,
                        "to_chain_id": to_chain_id,
                        "from_token": token_in_address,
                        "to_token": token_out_address,
                        "from_amount": str(amount_in_wei),
                        "from_address": self.wallet_address,
                        "slippage": slippage,
                    },
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile swap intent via LiFi: {e}")
            return self._error_bundle(intent, str(e))

    def get_fresh_transaction(
        self,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch fresh transaction data immediately before execution.

        IMPORTANT: LiFi route data becomes stale quickly. This method
        should be called immediately before executing a transaction to
        get fresh route data from the LiFi API.

        Args:
            metadata: The metadata from a compiled ActionBundle containing route_params

        Returns:
            Fresh transaction data dict with keys:
                - to: Target contract address
                - value: Native token value
                - data: Fresh route calldata
                - gas_estimate: Estimated gas
                - amount_out: Expected output amount

        Raises:
            ValueError: If metadata doesn't contain route_params
        """
        route_params = metadata.get("route_params")
        if not route_params:
            raise ValueError(
                "metadata must contain 'route_params' for deferred transaction. "
                "Ensure the bundle was compiled with compile_swap_intent()."
            )

        logger.info(
            f"Fetching fresh LiFi route: "
            f"{route_params['from_token'][:10]}...@{route_params['from_chain_id']} -> "
            f"{route_params['to_token'][:10]}...@{route_params['to_chain_id']}"
        )

        # Fetch fresh quote from LiFi API
        quote = self.client.get_quote(
            from_chain_id=route_params["from_chain_id"],
            to_chain_id=route_params["to_chain_id"],
            from_token=route_params["from_token"],
            to_token=route_params["to_token"],
            from_amount=route_params["from_amount"],
            from_address=route_params["from_address"],
            slippage=route_params["slippage"],
        )

        tx_request = quote.transaction_request
        gas_estimate = self._get_gas_estimate(quote)

        logger.info(
            f"Fresh LiFi route received: to_amount={quote.get_to_amount()}, tool={quote.tool}, gas={gas_estimate}"
        )

        return {
            "to": tx_request.to if tx_request else "",
            "value": _parse_hex_int(tx_request.value) if tx_request else 0,
            "data": tx_request.data if tx_request else "",
            "gas_estimate": gas_estimate,
            "amount_out": quote.get_to_amount(),
            "amount_out_min": quote.get_to_amount_min(),
            "tool": quote.tool,
            "description": (
                f"{'Bridge' if metadata.get('is_cross_chain') else 'Swap'} "
                f"{metadata.get('from_token', '?')} -> {metadata.get('to_token', '?')} via LiFi"
            ),
            "tx_type": "bridge" if metadata.get("is_cross_chain") else "swap",
            "approval_address": quote.estimate.approval_address if quote.estimate else "",
        }

    def _resolve_amount(
        self,
        intent: SwapIntent,
        price_oracle: dict[str, Decimal],
    ) -> int | None:
        """Resolve swap amount from intent to wei.

        Args:
            intent: The swap intent
            price_oracle: Price oracle for USD conversions

        Returns:
            Amount in wei, or None if cannot resolve
        """
        if intent.amount is not None:
            if intent.amount == "all":
                return None  # Must be resolved before compilation
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            decimals = self.get_token_decimals(intent.from_token)
            return int(amount_decimal * Decimal(10**decimals))
        elif intent.amount_usd is not None:
            from_price = price_oracle.get(intent.from_token.upper(), Decimal("1"))
            if from_price == 0:
                from_price = Decimal("1")
            token_amount = intent.amount_usd / from_price
            decimals = self.get_token_decimals(intent.from_token)
            return int(token_amount * Decimal(10**decimals))
        return None

    def _build_approve_transaction(
        self,
        token_address: str,
        spender: str,
        amount: int,
    ) -> TransactionData | None:
        """Build an ERC-20 approve transaction.

        Args:
            token_address: Token to approve
            spender: Address to approve (from LiFi quote)
            amount: Amount to approve

        Returns:
            TransactionData for approve, or None if not needed
        """
        if self._is_native_token(token_address):
            return None

        # Build approve calldata: approve(address spender, uint256 amount)
        calldata = (
            ERC20_APPROVE_SELECTOR + self._pad_address(spender) + self._pad_uint256(MAX_UINT256)  # Use max approval
        )

        return TransactionData(
            to=token_address,
            value=0,
            data=calldata,
            gas_estimate=LIFI_GAS_ESTIMATES["approve"],
            description=f"Approve {token_address[:10]}... for LiFi",
            tx_type="approve",
        )

    def _get_gas_estimate(self, quote: Any) -> int:
        """Extract gas estimate from a LiFi quote.

        Args:
            quote: LiFiStep with estimate

        Returns:
            Gas estimate as integer
        """
        if quote.estimate:
            total_gas = quote.estimate.total_gas_estimate
            if total_gas > 0:
                return total_gas

        # Fall back to transaction request gas limit
        if quote.transaction_request and quote.transaction_request.gas_limit:
            try:
                return _parse_hex_int(quote.transaction_request.gas_limit)
            except (ValueError, TypeError):
                pass

        # Default estimates
        if quote.is_cross_chain:
            return LIFI_GAS_ESTIMATES["bridge"]
        return LIFI_GAS_ESTIMATES["swap"]

    def _get_chain_name(self) -> str:
        """Get chain name from chain ID."""
        from .client import CHAIN_ID_TO_NAME

        return CHAIN_ID_TO_NAME.get(self.chain_id, str(self.chain_id))

    @staticmethod
    def _chain_id_to_name(chain_id: int | None) -> str | None:
        """Convert chain ID to name."""
        if chain_id is None:
            return None
        from .client import CHAIN_ID_TO_NAME

        return CHAIN_ID_TO_NAME.get(chain_id)

    @staticmethod
    def _is_native_token(address: str) -> bool:
        """Check if an address represents a native token."""
        return address.lower() in (
            "0x0000000000000000000000000000000000000000",
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        )

    def _error_bundle(self, intent: SwapIntent, error: str) -> ActionBundle:
        """Create an error ActionBundle."""
        return ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[],
            metadata={
                "error": error,
                "intent_id": intent.intent_id,
            },
        )

    @staticmethod
    def _get_placeholder_prices() -> dict[str, Decimal]:
        """Get placeholder price data for testing only."""
        return {
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDC": Decimal("1"),
            "USDC.e": Decimal("1"),
            "USDT": Decimal("1"),
            "DAI": Decimal("1"),
            "WBTC": Decimal("45000"),
            "ARB": Decimal("1.20"),
        }

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return addr.lower().replace("0x", "").zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)
