"""Enso Protocol Adapter.

This module provides the EnsoAdapter class for converting Intents to
executable transactions using the Enso Finance routing protocol.

Enso aggregates liquidity across multiple DEXs to find optimal swap routes,
and also supports lending protocol operations (Aave, Morpho) via bundle API.

IMPORTANT: Enso route data becomes stale within seconds. The adapter marks
swap transactions as "deferred" and provides a `get_fresh_swap_transaction()`
method to fetch fresh route data immediately before execution.

NOTE on Permit2: Enso's router internally uses Uniswap Permit2 on some chains
but not others. Our adapter only performs standard ERC-20 approve() to the Enso
Router address. On chains where Enso's router requires Permit2 internally, the
router handles the Permit2 flow itself after receiving the standard approval.
This is an Enso-internal detail and may vary across chains.

Example:
    from almanak.framework.connectors.enso import EnsoAdapter, EnsoConfig

    config = EnsoConfig(
        chain="arbitrum",
        wallet_address="0x...",
    )
    adapter = EnsoAdapter(config)

    # Compile a SwapIntent (returns approval + deferred swap)
    intent = SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount_usd=Decimal("1000"),
    )
    bundle = adapter.compile_swap_intent(intent)

    # Execute approval first...

    # Then fetch fresh swap transaction immediately before execution
    fresh_swap_tx = adapter.get_fresh_swap_transaction(bundle.metadata)
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from eth_abi import decode, encode

from ...data.tokens.exceptions import TokenResolutionError
from ...intents.vocabulary import IntentType, SwapIntent
from ...models.reproduction_bundle import ActionBundle
from .client import EnsoClient, EnsoConfig

if TYPE_CHECKING:
    from ...data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# Function selectors for Enso Router
ENSO_FUNCTION_SELECTORS = {
    "routeSingle": "0xb94c3609",
    "safeRouteSingle": "0x21025a06",
    "routeMulti": "0xf52e33f5",
    "safeRouteMulti": "0xf35cae90",
}


# ERC20 approve selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"

# Max uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1

# Default gas estimates
ENSO_GAS_ESTIMATES: dict[str, int] = {
    "approve": 50000,
    "swap": 200000,
    "swap_multi_hop": 350000,
}


@dataclass
class TransactionData:
    """Transaction data for execution.

    Attributes:
        to: Target contract address
        value: Native token value to send
        data: Encoded calldata
        gas_estimate: Estimated gas
        description: Human-readable description
        tx_type: Type of transaction (approve, swap)
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


class EnsoAdapter:
    """Adapter for Enso protocol integration with the Intent system.

    This adapter converts high-level SwapIntents into executable transactions
    using the Enso Finance routing protocol.

    Features:
    - Multi-DEX routing for optimal swap prices
    - Automatic slippage protection via safeRouteSingle
    - ERC-20 approval handling

    Example:
        config = EnsoConfig(chain="arbitrum", wallet_address="0x...")
        adapter = EnsoAdapter(config)

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
        )
        bundle = adapter.compile_swap_intent(intent)
    """

    def __init__(
        self,
        config: EnsoConfig,
        use_safe_route_single: bool = True,
        price_provider: dict[str, Decimal] | None = None,
        allow_placeholder_prices: bool = False,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the Enso adapter.

        Args:
            config: Enso client configuration
            use_safe_route_single: Whether to transform routes to use safeRouteSingle
                for slippage protection (default True)
            price_provider: Price oracle dict (token symbol -> USD price). Required for
                production use to calculate accurate slippage amounts.
            allow_placeholder_prices: If False (default), raises ValueError when no
                price_provider is given. Set to True ONLY for unit tests.
            token_resolver: Optional TokenResolver instance for unified token resolution.
                If None, uses the default singleton from get_token_resolver().

        Raises:
            ValueError: If no price_provider is provided and allow_placeholder_prices is False.
        """
        # Validate price_provider requirement
        self._using_placeholders = price_provider is None
        if self._using_placeholders and not allow_placeholder_prices:
            raise ValueError(
                "EnsoAdapter requires price_provider for production use. "
                "Pass a dict mapping token symbols to USD prices "
                "(e.g., {'ETH': Decimal('3400'), 'USDC': Decimal('1')}) "
                "or set allow_placeholder_prices=True for testing only. "
                "Using placeholder prices will cause incorrect slippage calculations."
            )

        self.config = config
        self.client = EnsoClient(config)
        self.chain = config.chain
        self.wallet_address = config.wallet_address
        self.use_safe_route_single = use_safe_route_single

        # Initialize token resolver
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from ...data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Price provider - use provided or fall back to placeholders (only if allowed)
        if self._using_placeholders:
            logger.warning(
                "EnsoAdapter using PLACEHOLDER PRICES. "
                "Slippage calculations will be INCORRECT. "
                "This is only acceptable for unit tests."
            )
            self._price_provider = self._get_placeholder_prices()
        else:
            self._price_provider = price_provider or {}

        logger.info(
            f"EnsoAdapter initialized for chain={self.chain}, "
            f"wallet={self.wallet_address[:10]}..., "
            f"using_placeholders={self._using_placeholders}"
        )

    def resolve_token_address(self, token: str) -> str:
        """Resolve token symbol or address to address using TokenResolver.

        Args:
            token: Token symbol (e.g., "USDC") or address

        Returns:
            Token address

        Raises:
            TokenResolutionError: If the token cannot be resolved
        """
        if token.startswith("0x") and len(token) == 42:
            return token
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[EnsoAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def get_token_decimals(self, token: str) -> int:
        """Get token decimals using TokenResolver.

        Args:
            token: Token symbol or address

        Returns:
            Token decimals

        Raises:
            TokenResolutionError: If decimals cannot be determined
        """
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[EnsoAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def compile_swap_intent(
        self,
        intent: SwapIntent,
        price_oracle: dict[str, Decimal] | None = None,
    ) -> ActionBundle:
        """Compile a SwapIntent to an ActionBundle using Enso.

        Args:
            intent: The SwapIntent to compile
            price_oracle: Optional price oracle for USD conversions

        Returns:
            ActionBundle containing transactions for execution
        """
        try:
            # Use default price oracle if not provided
            if price_oracle is None:
                price_oracle = self._get_default_price_oracle()

            # Resolve token addresses
            token_in_address = self.resolve_token_address(intent.from_token)
            token_out_address = self.resolve_token_address(intent.to_token)

            if not token_in_address:
                return self._error_bundle(intent, f"Unknown input token: {intent.from_token}")
            if not token_out_address:
                return self._error_bundle(intent, f"Unknown output token: {intent.to_token}")

            # Determine the swap amount in wei
            if intent.amount is not None:
                # Check for chained amount - must be resolved before compilation
                if intent.amount == "all":
                    return self._error_bundle(
                        intent,
                        "amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts.",
                    )
                # Direct token amount specified - type is validated above to be Decimal
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                decimals = self.get_token_decimals(intent.from_token)
                amount_in_wei = int(amount_decimal * Decimal(10**decimals))
            elif intent.amount_usd is not None:
                # Convert USD to token amount
                from_price = price_oracle.get(intent.from_token.upper())
                if not from_price:
                    return self._error_bundle(
                        intent,
                        f"Price unavailable for '{intent.from_token}' -- cannot convert amount_usd "
                        "to token amount. Ensure the price oracle includes this token.",
                    )
                token_amount = intent.amount_usd / from_price
                decimals = self.get_token_decimals(intent.from_token)
                amount_in_wei = int(token_amount * Decimal(10**decimals))
            else:
                return self._error_bundle(intent, "Either amount or amount_usd must be specified")

            # Convert slippage from decimal to basis points
            slippage_bps = int(intent.max_slippage * 10000)

            # Get route from Enso
            route_tx = self.client.get_route(
                token_in=token_in_address,
                token_out=token_out_address,
                amount_in=amount_in_wei,
                slippage_bps=slippage_bps,
            )

            # Transform to safeRouteSingle if enabled
            tx_data = route_tx.tx.data
            if self.use_safe_route_single and tx_data.startswith(ENSO_FUNCTION_SELECTORS["routeSingle"]):
                tx_data = self._transform_to_safe_route_single(
                    original_data=tx_data,
                    token_out_address=token_out_address,
                    receiver=self.wallet_address,
                    amount_out=route_tx.get_amount_out_wei(),
                    slippage_bps=slippage_bps,
                )

            # Build transactions list
            transactions: list[TransactionData] = []

            # Build approval transaction
            approve_tx = self._build_approve_transaction(
                token_address=token_in_address,
                spender=self.client.get_router_address(),
                amount=amount_in_wei,
            )
            if approve_tx:
                transactions.append(approve_tx)

            # IMPORTANT: Enso route data becomes stale within seconds.
            # Mark swap transaction as "deferred" - fresh route data must be
            # fetched immediately before execution using get_fresh_swap_transaction().
            # The initial route is included for quote/gas estimation only.
            swap_tx = TransactionData(
                to=route_tx.tx.to,
                value=int(route_tx.tx.value),
                data=tx_data,
                gas_estimate=int(route_tx.gas) if route_tx.gas else ENSO_GAS_ESTIMATES["swap"],
                description=f"Swap {intent.from_token} -> {intent.to_token} via Enso",
                tx_type="swap_deferred",  # Mark as deferred - needs fresh data at execution
            )
            transactions.append(swap_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Compiled swap intent: {intent.from_token} -> {intent.to_token}, "
                f"amount_in={amount_in_wei}, amount_out={route_tx.get_amount_out_wei()}, "
                f"price_impact={route_tx.price_impact}bp"
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
                    "amount_out": str(route_tx.get_amount_out_wei()),
                    "slippage_bps": slippage_bps,
                    "price_impact_bps": route_tx.price_impact,
                    "chain": self.chain,
                    "protocol": "enso",
                    "router": self.client.get_router_address(),
                    "gas_estimate": total_gas,
                    # Include parameters for fetching fresh route at execution time
                    "deferred_swap": True,
                    "route_params": {
                        "token_in": token_in_address,
                        "token_out": token_out_address,
                        "amount_in": amount_in_wei,
                        "slippage_bps": slippage_bps,
                    },
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile swap intent: {e}")
            return self._error_bundle(intent, str(e))

    def _build_approve_transaction(
        self,
        token_address: str,
        spender: str,
        amount: int,
    ) -> TransactionData | None:
        """Build an ERC-20 approve transaction.

        Args:
            token_address: Token to approve
            spender: Address to approve (router)
            amount: Amount to approve

        Returns:
            TransactionData for approve, or None if not needed
        """
        # Skip approval for native token
        if token_address.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee":
            return None

        # Build approve calldata: approve(address spender, uint256 amount)
        calldata = (
            ERC20_APPROVE_SELECTOR + self._pad_address(spender) + self._pad_uint256(MAX_UINT256)  # Use max approval
        )

        token_symbol = self._get_token_symbol(token_address)

        return TransactionData(
            to=token_address,
            value=0,
            data=calldata,
            gas_estimate=ENSO_GAS_ESTIMATES["approve"],
            description=f"Approve {token_symbol} for Enso Router",
            tx_type="approve",
        )

    def _transform_to_safe_route_single(
        self,
        original_data: str,
        token_out_address: str,
        receiver: str,
        amount_out: int,
        slippage_bps: int,
    ) -> str:
        """Transform routeSingle call data to safeRouteSingle.

        safeRouteSingle adds on-chain slippage protection by specifying
        a minimum output amount.

        Args:
            original_data: Original routeSingle calldata
            token_out_address: Output token address
            receiver: Address to receive tokens
            amount_out: Expected output amount
            slippage_bps: Slippage in basis points

        Returns:
            Transformed calldata for safeRouteSingle
        """
        try:
            # Decode routeSingle((uint8,bytes),bytes)
            # Remove 0x and function selector (first 4 bytes = 8 hex chars)
            calldata = original_data[10:]  # Remove "0xb94c3609"
            token_in, inner_data = decode(["(uint8,bytes)", "bytes"], bytes.fromhex(calldata))

            # Calculate minimum amount out with slippage
            min_amount_out = amount_out * (10000 - slippage_bps) // 10000
            if min_amount_out <= 0:
                logger.warning("Calculated min_amount_out is 0, using 1")
                min_amount_out = 1

            # Construct tokenOut (ERC20 type = 1)
            token_out_data = encode(["address", "uint256"], [token_out_address, min_amount_out])
            token_out = (1, token_out_data)  # (TokenType.ERC20, encoded_data)

            # Encode safeRouteSingle((uint8,bytes),(uint8,bytes),address,bytes)
            encoded_params = encode(
                ["(uint8,bytes)", "(uint8,bytes)", "address", "bytes"],
                [token_in, token_out, receiver, inner_data],
            )

            # Add safeRouteSingle selector
            return ENSO_FUNCTION_SELECTORS["safeRouteSingle"] + encoded_params.hex()

        except Exception as e:
            logger.error(f"Error transforming to safeRouteSingle: {e}")
            # Return original data if transformation fails
            return original_data

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

    def _get_placeholder_prices(self) -> dict[str, Decimal]:
        """Get placeholder price data for testing only.

        WARNING: These prices are HARDCODED and OUTDATED.
        DO NOT USE IN PRODUCTION - they will cause:
        - Incorrect slippage calculations
        - Swap reverts (amountOutMinimum too high)

        Real prices as of 2026-01: ETH ~$3400, BTC ~$105,000
        These placeholders show ETH at $2000, BTC at $45,000 - 40-60% wrong!
        """
        logger.warning(
            "PLACEHOLDER PRICES being used - NOT SAFE FOR PRODUCTION. "
            "ETH=$2000 (real ~$3400), BTC=$45000 (real ~$105000)"
        )
        return {
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDC": Decimal("1"),
            "USDC.e": Decimal("1"),
            "USDT": Decimal("1"),
            "DAI": Decimal("1"),
            "WBTC": Decimal("45000"),
            "ARB": Decimal("1.20"),
            "OP": Decimal("2.50"),
            "MATIC": Decimal("0.80"),
            "WMATIC": Decimal("0.80"),
        }

    def _get_default_price_oracle(self) -> dict[str, Decimal]:
        """Get price oracle data (uses instance price provider).

        Deprecated: This method exists for backward compatibility.
        The adapter now uses self._price_provider initialized in __init__.
        """
        return self._price_provider

    def _get_token_symbol(self, address: str) -> str:
        """Get token symbol from address using TokenResolver."""
        if not address.startswith("0x"):
            return address
        try:
            resolved = self._token_resolver.resolve(address, self.chain)
            return resolved.symbol
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=address,
                chain=str(self.chain),
                reason=f"[EnsoAdapter] Cannot resolve symbol: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return addr.lower().replace("0x", "").zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    def get_fresh_swap_transaction(
        self,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch fresh swap transaction data immediately before execution.

        IMPORTANT: Enso route data becomes stale within seconds. This method
        should be called immediately before executing a swap transaction to
        get fresh route data from the Enso API.

        Args:
            metadata: The metadata from a compiled ActionBundle containing route_params

        Returns:
            Fresh transaction data dict with keys:
                - to: Router address
                - value: Native token value (usually 0)
                - data: Fresh route calldata
                - gas_estimate: Raw gas estimate (orchestrator applies buffer)
                - amount_out: Expected output amount (for logging/verification)

        Raises:
            ValueError: If metadata doesn't contain route_params
            Exception: If route fetching fails

        Example:
            # After executing approval, fetch fresh swap data
            fresh_tx = adapter.get_fresh_swap_transaction(bundle.metadata)

            # Build and sign transaction with fresh data
            unsigned_tx = UnsignedTransaction(
                to=fresh_tx["to"],
                data=fresh_tx["data"],
                value=fresh_tx["value"],
                gas_limit=fresh_tx["gas_estimate"],
                ...
            )
        """
        route_params = metadata.get("route_params")
        if not route_params:
            raise ValueError(
                "metadata must contain 'route_params' for deferred swap. "
                "Ensure the bundle was compiled with compile_swap_intent()."
            )

        token_in = route_params["token_in"]
        token_out = route_params["token_out"]
        amount_in = route_params["amount_in"]
        slippage_bps = route_params["slippage_bps"]

        logger.info(
            f"Fetching fresh Enso route: {token_in[:10]}... -> {token_out[:10]}..., "
            f"amount={amount_in}, slippage={slippage_bps}bp"
        )

        # Fetch fresh route from Enso API
        route_tx = self.client.get_route(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage_bps=slippage_bps,
        )

        # Use the raw routeSingle data - don't transform to safeRouteSingle
        # as it can cause issues with certain routes
        tx_data = route_tx.tx.data

        gas_estimate = int(route_tx.gas) if route_tx.gas else ENSO_GAS_ESTIMATES["swap"]

        logger.info(
            f"Fresh route received: amount_out={route_tx.get_amount_out_wei()}, "
            f"gas={gas_estimate}, price_impact={route_tx.price_impact}bp"
        )

        return {
            "to": route_tx.tx.to,
            "value": int(route_tx.tx.value),
            "data": tx_data,
            "gas_estimate": gas_estimate,
            "amount_out": route_tx.get_amount_out_wei(),
            "price_impact_bps": route_tx.price_impact,
            "description": f"Swap {metadata.get('from_token', '?')} -> {metadata.get('to_token', '?')} via Enso",
            "tx_type": "swap",
        }
