"""Aerodrome Finance Protocol Adapter.

This module provides the AerodromeAdapter class for executing swaps and managing
liquidity positions on Aerodrome Finance (Base chain).

Aerodrome Architecture:
- Router: Main entry point for swaps and liquidity operations
- Factory: Creates and manages pools
- Pool: Individual AMM pools (volatile and stable)

Key Concepts:
- Volatile pools: x*y=k formula, 0.3% fee
- Stable pools: x^3*y + y^3*x formula, 0.05% fee
- Fungible LP tokens (not NFTs)

Example:
    config = AerodromeConfig(
        chain="base",
        wallet_address="0x...",
    )
    adapter = AerodromeAdapter(config)

    # Execute a swap
    result = adapter.swap_exact_input(
        token_in="USDC",
        token_out="WETH",
        amount_in=Decimal("1000"),
        stable=False,
        slippage_bps=50,
    )
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from ...data.tokens.exceptions import TokenResolutionError
from ...intents.vocabulary import IntentType, SwapIntent
from ...models.reproduction_bundle import ActionBundle
from .sdk import (
    AERODROME_ADDRESSES,
    AERODROME_GAS_ESTIMATES,
    MAX_UINT256,
    AerodromeSDK,
)

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Function selectors for Aerodrome Router (Solidly fork)
# Note: Aerodrome uses different signatures than UniswapV2 due to the `stable` parameter
# addLiquidity(address,address,bool,uint256,uint256,uint256,uint256,address,uint256)
ADD_LIQUIDITY_SELECTOR = "0x5a47ddc3"
# removeLiquidity(address,address,bool,uint256,uint256,uint256,address,uint256)
REMOVE_LIQUIDITY_SELECTOR = "0x0dede6c4"
# swapExactTokensForTokens(uint256,uint256,(address,address,bool,address)[],address,uint256)
SWAP_EXACT_TOKENS_SELECTOR = "0xcac88ea9"

# Slipstream CL SwapRouter: exactInputSingle((address,address,int24,address,uint256,uint256,uint256,uint160))
CL_EXACT_INPUT_SINGLE_SELECTOR = "0xa026383e"

# ERC20 approve selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"

# Default deadline (5 minutes)
DEFAULT_DEADLINE_SECONDS = 300


# =============================================================================
# Enums
# =============================================================================


class SwapType(Enum):
    """Type of swap operation."""

    EXACT_INPUT = "EXACT_INPUT"
    EXACT_OUTPUT = "EXACT_OUTPUT"


class PoolType(Enum):
    """Pool type for Aerodrome."""

    VOLATILE = "VOLATILE"  # x*y=k, 0.3% fee
    STABLE = "STABLE"  # x^3*y + y^3*x, 0.05% fee


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class AerodromeConfig:
    """Configuration for AerodromeAdapter.

    Attributes:
        chain: Target blockchain (currently only "base")
        wallet_address: Address executing transactions
        default_slippage_bps: Default slippage tolerance in basis points (default 50 = 0.5%)
        deadline_seconds: Transaction deadline in seconds (default 300 = 5 minutes)
        price_provider: Price oracle dict (token symbol -> USD price). Required for
            production use to calculate accurate slippage amounts.
        allow_placeholder_prices: If False (default), raises ValueError when no
            price_provider is given. Set to True ONLY for unit tests.
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50
    deadline_seconds: int = DEFAULT_DEADLINE_SECONDS
    price_provider: dict[str, Decimal] | None = None
    allow_placeholder_prices: bool = False
    rpc_url: str | None = None  # Optional RPC override for SDK on-chain queries

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.chain not in AERODROME_ADDRESSES:
            raise ValueError(f"Unsupported chain: {self.chain}. Supported: {list(AERODROME_ADDRESSES.keys())}")

        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError("Slippage must be between 0 and 10000 basis points")

        # Validate price_provider requirement
        if self.price_provider is None and not self.allow_placeholder_prices:
            raise ValueError(
                "AerodromeConfig requires price_provider for production use. "
                "Pass a dict mapping token symbols to USD prices "
                "(e.g., {'ETH': Decimal('3400'), 'USDC': Decimal('1')}) "
                "or set allow_placeholder_prices=True for testing only. "
                "Using placeholder prices will cause incorrect slippage calculations."
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "default_slippage_bps": self.default_slippage_bps,
            "deadline_seconds": self.deadline_seconds,
        }


@dataclass
class SwapQuote:
    """Quote for a swap operation.

    Attributes:
        token_in: Input token address
        token_out: Output token address
        amount_in: Input amount in wei
        amount_out: Output amount in wei
        stable: Pool type (True=stable, False=volatile)
        gas_estimate: Estimated gas for the swap
        price_impact_bps: Price impact in basis points
        effective_price: Effective price of the swap
        quoted_at: Timestamp when quote was fetched
    """

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    stable: bool
    gas_estimate: int = AERODROME_GAS_ESTIMATES["swap"]
    price_impact_bps: int = 0
    effective_price: Decimal = Decimal("0")
    quoted_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "stable": self.stable,
            "gas_estimate": self.gas_estimate,
            "price_impact_bps": self.price_impact_bps,
            "effective_price": str(self.effective_price),
            "quoted_at": self.quoted_at.isoformat(),
        }


@dataclass
class SwapResult:
    """Result of a swap operation.

    Attributes:
        success: Whether the swap was built successfully
        transactions: List of transactions to execute
        quote: Quote used for the swap
        amount_in: Actual input amount
        amount_out_minimum: Minimum output amount (with slippage)
        error: Error message if failed
        gas_estimate: Total gas estimate for all transactions
    """

    success: bool
    transactions: list["TransactionData"] = field(default_factory=list)
    quote: SwapQuote | None = None
    amount_in: int = 0
    amount_out_minimum: int = 0
    error: str | None = None
    gas_estimate: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "quote": self.quote.to_dict() if self.quote else None,
            "amount_in": str(self.amount_in),
            "amount_out_minimum": str(self.amount_out_minimum),
            "error": self.error,
            "gas_estimate": self.gas_estimate,
        }


@dataclass
class LiquidityResult:
    """Result of a liquidity operation.

    Attributes:
        success: Whether operation was built successfully
        transactions: List of transactions to execute
        token_a: First token address
        token_b: Second token address
        amount_a: Amount of token A
        amount_b: Amount of token B
        liquidity: LP tokens (minted or burned)
        stable: Pool type
        error: Error message if failed
        gas_estimate: Total gas estimate
    """

    success: bool
    transactions: list["TransactionData"] = field(default_factory=list)
    token_a: str = ""
    token_b: str = ""
    amount_a: int = 0
    amount_b: int = 0
    liquidity: int = 0
    stable: bool = False
    error: str | None = None
    gas_estimate: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "token_a": self.token_a,
            "token_b": self.token_b,
            "amount_a": str(self.amount_a),
            "amount_b": str(self.amount_b),
            "liquidity": str(self.liquidity),
            "stable": self.stable,
            "error": self.error,
            "gas_estimate": self.gas_estimate,
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
        tx_type: Type of transaction (approve, swap, add_liquidity, remove_liquidity)
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


# =============================================================================
# Aerodrome Adapter
# =============================================================================


class AerodromeAdapter:
    """Adapter for Aerodrome Finance DEX protocol.

    This adapter provides methods for:
    - Executing token swaps (exact input)
    - Adding and removing liquidity
    - Building swap and LP transactions
    - Handling ERC-20 approvals
    - Managing slippage protection

    Example:
        config = AerodromeConfig(
            chain="base",
            wallet_address="0x...",
        )
        adapter = AerodromeAdapter(config)

        # Execute a volatile pool swap
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            stable=False,
            slippage_bps=50,
        )

        # Execute a stable pool swap
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="USDbC",
            amount_in=Decimal("1000"),
            stable=True,
            slippage_bps=10,
        )
    """

    def __init__(self, config: AerodromeConfig, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize the adapter.

        Args:
            config: Aerodrome adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Initialize SDK (includes optional RPC override for pool lookups/quotes)
        self.sdk = AerodromeSDK(chain=self.chain, rpc_url=config.rpc_url)
        self._web3: Any = None

        # Load contract addresses
        self.addresses = AERODROME_ADDRESSES[self.chain]

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Price provider - use provided or fall back to placeholders (only if allowed)
        self._using_placeholders = config.price_provider is None
        if self._using_placeholders:
            logger.warning(
                "AerodromeAdapter using PLACEHOLDER PRICES. "
                "Slippage calculations will be INCORRECT. "
                "This is only acceptable for unit tests."
            )
            self._price_provider = self._get_placeholder_prices()
        else:
            self._price_provider = config.price_provider if config.price_provider is not None else {}

        # Allowance cache (token -> amount approved)
        self._allowance_cache: dict[str, int] = {}

        logger.info(
            f"AerodromeAdapter initialized for chain={self.chain}, "
            f"wallet={self.wallet_address[:10]}..., "
            f"using_placeholders={self._using_placeholders}"
        )

    # =========================================================================
    # Swap Operations
    # =========================================================================

    def swap_exact_input(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        stable: bool = False,
        slippage_bps: int | None = None,
        recipient: str | None = None,
        tick_spacing: int = 100,
        use_classic: bool = False,
    ) -> SwapResult:
        """Build a swap transaction with exact input amount.

        By default, routes through the Slipstream CL (concentrated liquidity) pool.
        Use ``use_classic=True`` to opt into the Classic (v1) volatile/stable router.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Amount of input token (in token units, not wei)
            stable: Pool type for Classic routing (True=stable, False=volatile)
            slippage_bps: Slippage tolerance in basis points (default from config)
            recipient: Address to receive output tokens (default: wallet_address)
            tick_spacing: Slipstream CL tick spacing (default 100)
            use_classic: If True, route through Classic router instead of CL

        Returns:
            SwapResult with transaction data
        """
        try:
            # Use defaults from config if not specified
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Resolve token addresses
            token_in_address = self._resolve_token(token_in)
            token_out_address = self._resolve_token(token_out)

            if token_in_address is None:
                return SwapResult(
                    success=False,
                    error=f"Unknown input token: {token_in}",
                )
            if token_out_address is None:
                return SwapResult(
                    success=False,
                    error=f"Unknown output token: {token_out}",
                )

            # Get token decimals
            token_in_decimals = self._get_token_decimals(token_in)

            # Convert amount to wei
            amount_in_wei = int(amount_in * Decimal(10**token_in_decimals))

            # Check if we need native token handling
            is_native_input = self._is_native_token(token_in)
            actual_token_in = token_in_address

            if is_native_input:
                # Use WETH for the swap - resolve WETH address
                weth_address = self._resolve_token("WETH")
                actual_token_in = weth_address if weth_address else token_in_address

            # Determine routing: CL (default) vs Classic (opt-in)
            routing = "classic" if use_classic else "cl"

            # Get quote (estimate output)
            # For CL, skip on-chain Classic router quoting; use oracle-based estimation
            if routing == "cl":
                quote = self._get_quote_exact_input(
                    token_in_address, token_out_address, amount_in_wei, stable, skip_onchain=True
                )
            else:
                quote = self._get_quote_exact_input(token_in_address, token_out_address, amount_in_wei, stable)

            amount_out_minimum = int(quote.amount_out * (10000 - slippage_bps) // 10000)

            # Build transactions
            transactions: list[TransactionData] = []

            # Determine spender (CL router vs Classic router)
            spender = self.addresses["cl_router"] if routing == "cl" else self.addresses["router"]

            # Build approve transaction if needed (skip for native token)
            if not is_native_input:
                approve_tx = self._build_approve_tx(
                    actual_token_in,
                    spender,
                    amount_in_wei,
                )
                if approve_tx is not None:
                    transactions.append(approve_tx)

            # Build swap transaction
            if routing == "cl":
                swap_tx = self._build_swap_exact_input_cl_tx(
                    token_in=actual_token_in,
                    token_out=token_out_address,
                    tick_spacing=tick_spacing,
                    recipient=recipient,
                    amount_in=amount_in_wei,
                    amount_out_minimum=amount_out_minimum,
                    value=amount_in_wei if is_native_input else 0,
                )
            else:
                swap_tx = self._build_swap_exact_input_tx(
                    token_in=actual_token_in,
                    token_out=token_out_address,
                    stable=stable,
                    recipient=recipient,
                    amount_in=amount_in_wei,
                    amount_out_minimum=amount_out_minimum,
                    value=amount_in_wei if is_native_input else 0,
                )
            transactions.append(swap_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built Aerodrome {routing} swap: {token_in} -> {token_out}, "
                f"amount_in={amount_in}, slippage={slippage_bps}bps, "
                f"transactions={len(transactions)}"
            )

            return SwapResult(
                success=True,
                transactions=transactions,
                quote=quote,
                amount_in=amount_in_wei,
                amount_out_minimum=amount_out_minimum,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build swap: {e}")
            return SwapResult(
                success=False,
                error=str(e),
            )

    # =========================================================================
    # Liquidity Operations
    # =========================================================================

    def add_liquidity(
        self,
        token_a: str,
        token_b: str,
        amount_a: Decimal,
        amount_b: Decimal,
        stable: bool = False,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build an add liquidity transaction.

        Args:
            token_a: First token symbol or address
            token_b: Second token symbol or address
            amount_a: Amount of token A (in token units)
            amount_b: Amount of token B (in token units)
            stable: Pool type
            slippage_bps: Slippage tolerance in basis points
            recipient: Address to receive LP tokens

        Returns:
            LiquidityResult with transaction data
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Resolve token addresses
            token_a_address = self._resolve_token(token_a)
            token_b_address = self._resolve_token(token_b)

            if token_a_address is None:
                return LiquidityResult(success=False, error=f"Unknown token: {token_a}")
            if token_b_address is None:
                return LiquidityResult(success=False, error=f"Unknown token: {token_b}")

            # Get token decimals
            token_a_decimals = self._get_token_decimals(token_a)
            token_b_decimals = self._get_token_decimals(token_b)

            # Convert amounts to wei
            amount_a_wei = int(amount_a * Decimal(10**token_a_decimals))
            amount_b_wei = int(amount_b * Decimal(10**token_b_decimals))

            # For LP operations, set minimums to 0.
            # Aerodrome (Solidly-based AMM) adjusts amounts to match the pool's current
            # ratio during addLiquidity. Any excess tokens are refunded to the user.
            # Setting tight minimums causes InsufficientAmountB() reverts when the
            # actual amounts added don't match user-specified amounts.
            _ = slippage_bps  # Acknowledge but don't use for LP
            amount_a_min = 0
            amount_b_min = 0

            logger.debug(
                f"Aerodrome add liquidity: amount_a_min={amount_a_min}, "
                f"amount_b_min={amount_b_min} (set to 0 for LP operations)"
            )

            transactions: list[TransactionData] = []

            # Build approve transactions for both tokens
            approve_a = self._build_approve_tx(token_a_address, self.addresses["router"], amount_a_wei)
            if approve_a:
                transactions.append(approve_a)

            approve_b = self._build_approve_tx(token_b_address, self.addresses["router"], amount_b_wei)
            if approve_b:
                transactions.append(approve_b)

            # Build add liquidity transaction
            add_liq_tx = self._build_add_liquidity_tx(
                token_a=token_a_address,
                token_b=token_b_address,
                stable=stable,
                amount_a_desired=amount_a_wei,
                amount_b_desired=amount_b_wei,
                amount_a_min=amount_a_min,
                amount_b_min=amount_b_min,
                recipient=recipient,
            )
            transactions.append(add_liq_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(f"Built add liquidity: {token_a}/{token_b} stable={stable}, transactions={len(transactions)}")

            return LiquidityResult(
                success=True,
                transactions=transactions,
                token_a=token_a_address,
                token_b=token_b_address,
                amount_a=amount_a_wei,
                amount_b=amount_b_wei,
                stable=stable,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build add liquidity: {e}")
            return LiquidityResult(success=False, error=str(e))

    def remove_liquidity(
        self,
        token_a: str,
        token_b: str,
        liquidity: Decimal,
        stable: bool = False,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a remove liquidity transaction.

        Args:
            token_a: First token symbol or address
            token_b: Second token symbol or address
            liquidity: LP token amount to burn (in LP token units)
            stable: Pool type
            slippage_bps: Slippage tolerance in basis points
            recipient: Address to receive tokens

        Returns:
            LiquidityResult with transaction data
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Resolve token addresses
            token_a_address = self._resolve_token(token_a)
            token_b_address = self._resolve_token(token_b)

            if token_a_address is None:
                return LiquidityResult(success=False, error=f"Unknown token: {token_a}")
            if token_b_address is None:
                return LiquidityResult(success=False, error=f"Unknown token: {token_b}")

            # LP tokens have 18 decimals
            liquidity_wei = int(liquidity * Decimal(10**18))

            # For minimums, we need to estimate. Set to 0 with slippage protection
            # In production, you'd query pool reserves to estimate
            amount_a_min = 0
            amount_b_min = 0

            transactions: list[TransactionData] = []

            # Get pool address for LP token approval
            # The pool contract IS the LP token (ERC-20)
            pool_address = self.sdk.get_pool_address(token_a_address, token_b_address, stable)
            if pool_address:
                # Approve router to spend LP tokens
                approve_tx = self._build_approve_tx(
                    token_address=pool_address,
                    spender=self.addresses["router"],
                    amount=liquidity_wei,
                    token_label=f"LP({token_a}/{token_b})",
                )
                if approve_tx:
                    transactions.append(approve_tx)
            else:
                logger.warning(
                    f"Could not find pool address for {token_a}/{token_b} stable={stable}, "
                    "skipping LP approval - transaction may fail"
                )

            # Build remove liquidity transaction
            remove_liq_tx = self._build_remove_liquidity_tx(
                token_a=token_a_address,
                token_b=token_b_address,
                stable=stable,
                liquidity=liquidity_wei,
                amount_a_min=amount_a_min,
                amount_b_min=amount_b_min,
                recipient=recipient,
            )
            transactions.append(remove_liq_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built remove liquidity: {token_a}/{token_b} stable={stable}, "
                f"liquidity={liquidity}, transactions={len(transactions)}"
            )

            return LiquidityResult(
                success=True,
                transactions=transactions,
                token_a=token_a_address,
                token_b=token_b_address,
                liquidity=liquidity_wei,
                stable=stable,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build remove liquidity: {e}")
            return LiquidityResult(success=False, error=str(e))

    # =========================================================================
    # Intent Compilation
    # =========================================================================

    def compile_swap_intent(
        self,
        intent: SwapIntent,
        stable: bool = False,
        price_oracle: dict[str, Decimal] | None = None,
    ) -> ActionBundle:
        """Compile a SwapIntent to an ActionBundle.

        Args:
            intent: The SwapIntent to compile
            stable: Pool type (True=stable, False=volatile)
            price_oracle: Optional price oracle for USD conversions

        Returns:
            ActionBundle containing transactions for execution
        """
        # Use default price oracle if not provided
        if price_oracle is None:
            price_oracle = self._get_default_price_oracle()

        # Determine the swap amount
        if intent.amount is not None:
            # Check for chained amount
            if intent.amount == "all":
                raise ValueError(
                    "amount='all' must be resolved before compilation. "
                    "Use Intent.set_resolved_amount() to resolve chained amounts."
                )
            amount_in: Decimal = intent.amount  # type: ignore[assignment]
        elif intent.amount_usd is not None:
            # Convert USD to token amount
            from_price = price_oracle.get(intent.from_token.upper())
            if not from_price:
                raise ValueError(
                    f"Price unavailable for '{intent.from_token}' -- cannot convert amount_usd "
                    "to token amount. Ensure the price oracle includes this token."
                )
            amount_in = intent.amount_usd / from_price
        else:
            raise ValueError("Either amount or amount_usd must be specified")

        # Convert slippage from decimal to basis points
        slippage_bps = int(intent.max_slippage * 10000)

        # Build the swap
        result = self.swap_exact_input(
            token_in=intent.from_token,
            token_out=intent.to_token,
            amount_in=amount_in,
            stable=stable,
            slippage_bps=slippage_bps,
        )

        if not result.success:
            return ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[],
                metadata={
                    "error": result.error,
                    "intent_id": intent.intent_id,
                    "protocol": "aerodrome",
                },
            )

        return ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx.to_dict() for tx in result.transactions],
            metadata={
                "intent_id": intent.intent_id,
                "from_token": intent.from_token,
                "to_token": intent.to_token,
                "amount_in": str(result.amount_in),
                "amount_out_minimum": str(result.amount_out_minimum),
                "slippage_bps": slippage_bps,
                "chain": self.chain,
                "protocol": "aerodrome",
                "stable": stable,
                "router": self.addresses["router"],
                "gas_estimate": result.gas_estimate,
            },
        )

    # =========================================================================
    # Transaction Building
    # =========================================================================

    def _build_swap_exact_input_tx(
        self,
        token_in: str,
        token_out: str,
        stable: bool,
        recipient: str,
        amount_in: int,
        amount_out_minimum: int,
        value: int = 0,
    ) -> TransactionData:
        """Build swapExactTokensForTokens transaction.

        Aerodrome swapExactTokensForTokens signature:
        function swapExactTokensForTokens(
            uint256 amountIn,
            uint256 amountOutMin,
            Route[] calldata routes,
            address to,
            uint256 deadline
        )

        Route struct: { address from, address to, bool stable, address factory }
        """
        deadline = int(datetime.now(UTC).timestamp()) + self.config.deadline_seconds

        # Encode Route struct
        route_data = self._encode_route(token_in, token_out, stable)

        # Encode full calldata
        calldata = (
            SWAP_EXACT_TOKENS_SELECTOR
            + self._pad_uint256(amount_in)
            + self._pad_uint256(amount_out_minimum)
            + self._pad_uint256(160)  # offset to routes array (5 * 32 bytes)
            + self._pad_address(recipient)
            + self._pad_uint256(deadline)
            + self._pad_uint256(1)  # routes array length
            + route_data  # single route
        )

        # Format amounts for description
        token_in_symbol = self._get_token_symbol(token_in)
        token_out_symbol = self._get_token_symbol(token_out)
        token_in_decimals = self._get_token_decimals(token_in_symbol)
        token_out_decimals = self._get_token_decimals(token_out_symbol)

        amount_in_formatted = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
        amount_out_formatted = Decimal(str(amount_out_minimum)) / Decimal(10**token_out_decimals)

        pool_type = "stable" if stable else "volatile"

        return TransactionData(
            to=self.addresses["router"],
            value=value,
            data=calldata,
            gas_estimate=AERODROME_GAS_ESTIMATES["swap"],
            description=(
                f"Aerodrome {pool_type} swap {amount_in_formatted:.6f} {token_in_symbol} -> "
                f"{token_out_symbol} (min: {amount_out_formatted:.6f})"
            ),
            tx_type="swap",
        )

    def _build_swap_exact_input_cl_tx(
        self,
        token_in: str,
        token_out: str,
        tick_spacing: int,
        recipient: str,
        amount_in: int,
        amount_out_minimum: int,
        value: int = 0,
    ) -> TransactionData:
        """Build Slipstream CL exactInputSingle swap transaction.

        Aerodrome Slipstream exactInputSingle signature:
        function exactInputSingle(ExactInputSingleParams calldata params)

        ExactInputSingleParams struct:
            address tokenIn, address tokenOut, int24 tickSpacing,
            address recipient, uint256 deadline, uint256 amountIn,
            uint256 amountOutMinimum, uint160 sqrtPriceLimitX96
        """
        deadline = int(datetime.now(UTC).timestamp()) + self.config.deadline_seconds

        calldata = (
            CL_EXACT_INPUT_SINGLE_SELECTOR
            + self._pad_address(token_in)
            + self._pad_address(token_out)
            + self._pad_int24(tick_spacing)
            + self._pad_address(recipient)
            + self._pad_uint256(deadline)
            + self._pad_uint256(amount_in)
            + self._pad_uint256(amount_out_minimum)
            + self._pad_uint256(0)  # sqrtPriceLimitX96 = 0 (no limit)
        )

        # Format amounts for description
        token_in_symbol = self._get_token_symbol(token_in)
        token_out_symbol = self._get_token_symbol(token_out)
        token_in_decimals = self._get_token_decimals(token_in_symbol)
        token_out_decimals = self._get_token_decimals(token_out_symbol)

        amount_in_formatted = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
        amount_out_formatted = Decimal(str(amount_out_minimum)) / Decimal(10**token_out_decimals)

        return TransactionData(
            to=self.addresses["cl_router"],
            value=value,
            data=calldata,
            gas_estimate=AERODROME_GAS_ESTIMATES["swap"],
            description=(
                f"Aerodrome CL swap {amount_in_formatted:.6f} {token_in_symbol} -> "
                f"{token_out_symbol} (min: {amount_out_formatted:.6f}, tickSpacing={tick_spacing})"
            ),
            tx_type="swap",
        )

    def _build_add_liquidity_tx(
        self,
        token_a: str,
        token_b: str,
        stable: bool,
        amount_a_desired: int,
        amount_b_desired: int,
        amount_a_min: int,
        amount_b_min: int,
        recipient: str,
    ) -> TransactionData:
        """Build addLiquidity transaction."""
        deadline = int(datetime.now(UTC).timestamp()) + self.config.deadline_seconds

        calldata = (
            ADD_LIQUIDITY_SELECTOR
            + self._pad_address(token_a)
            + self._pad_address(token_b)
            + self._pad_bool(stable)
            + self._pad_uint256(amount_a_desired)
            + self._pad_uint256(amount_b_desired)
            + self._pad_uint256(amount_a_min)
            + self._pad_uint256(amount_b_min)
            + self._pad_address(recipient)
            + self._pad_uint256(deadline)
        )

        token_a_symbol = self._get_token_symbol(token_a)
        token_b_symbol = self._get_token_symbol(token_b)
        pool_type = "stable" if stable else "volatile"

        return TransactionData(
            to=self.addresses["router"],
            value=0,
            data=calldata,
            gas_estimate=AERODROME_GAS_ESTIMATES["add_liquidity"],
            description=f"Aerodrome add {pool_type} liquidity {token_a_symbol}/{token_b_symbol}",
            tx_type="add_liquidity",
        )

    def _build_remove_liquidity_tx(
        self,
        token_a: str,
        token_b: str,
        stable: bool,
        liquidity: int,
        amount_a_min: int,
        amount_b_min: int,
        recipient: str,
    ) -> TransactionData:
        """Build removeLiquidity transaction."""
        deadline = int(datetime.now(UTC).timestamp()) + self.config.deadline_seconds

        calldata = (
            REMOVE_LIQUIDITY_SELECTOR
            + self._pad_address(token_a)
            + self._pad_address(token_b)
            + self._pad_bool(stable)
            + self._pad_uint256(liquidity)
            + self._pad_uint256(amount_a_min)
            + self._pad_uint256(amount_b_min)
            + self._pad_address(recipient)
            + self._pad_uint256(deadline)
        )

        token_a_symbol = self._get_token_symbol(token_a)
        token_b_symbol = self._get_token_symbol(token_b)
        pool_type = "stable" if stable else "volatile"

        return TransactionData(
            to=self.addresses["router"],
            value=0,
            data=calldata,
            gas_estimate=AERODROME_GAS_ESTIMATES["remove_liquidity"],
            description=f"Aerodrome remove {pool_type} liquidity {token_a_symbol}/{token_b_symbol}",
            tx_type="remove_liquidity",
        )

    def _build_approve_tx(
        self,
        token_address: str,
        spender: str,
        amount: int,
        token_label: str | None = None,
    ) -> TransactionData | None:
        """Build an ERC-20 approve transaction if needed."""
        # Check cache for existing allowance
        cache_key = f"{token_address}:{spender}"
        cached = self._allowance_cache.get(cache_key, 0)
        if cached >= amount:
            logger.debug(f"Sufficient allowance exists for {token_address}")
            return None

        # Build approve calldata
        calldata = ERC20_APPROVE_SELECTOR + self._pad_address(spender) + self._pad_uint256(MAX_UINT256)

        # Update cache
        self._allowance_cache[cache_key] = MAX_UINT256

        token_symbol = token_label or self._get_token_symbol(token_address)

        return TransactionData(
            to=token_address,
            value=0,
            data=calldata,
            gas_estimate=AERODROME_GAS_ESTIMATES["approve"],
            description=f"Approve {token_symbol} for Aerodrome Router",
            tx_type="approve",
        )

    # =========================================================================
    # Quote Functions
    # =========================================================================

    def _get_quote_exact_input(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        stable: bool,
        skip_onchain: bool = False,
    ) -> SwapQuote:
        """Get quote for exact input swap.

        In production, this would call the pool contract.
        Prefer on-chain quoting via router.getAmountsOut when rpc_url is available.
        Falls back to a price-oracle estimate when on-chain quoting is unavailable.
        """
        token_in_symbol = self._get_token_symbol(token_in)
        token_out_symbol = self._get_token_symbol(token_out)

        token_in_decimals = self._get_token_decimals(token_in_symbol)
        token_out_decimals = self._get_token_decimals(token_out_symbol)

        amount_out = None if skip_onchain else self._try_get_amount_out_onchain(token_in, token_out, amount_in, stable)
        if amount_out is not None:
            amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
            amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**token_out_decimals)
            effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal("0")

            return SwapQuote(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                amount_out=amount_out,
                stable=stable,
                effective_price=effective_price,
                gas_estimate=AERODROME_GAS_ESTIMATES["swap"],
            )

        prices = self._get_default_price_oracle()
        price_in = prices.get(token_in_symbol, Decimal("1"))
        price_out = prices.get(token_out_symbol, Decimal("1")) or Decimal("1")

        # Calculate expected output
        amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
        usd_value = amount_in_decimal * price_in

        # Apply fee (0.05% for stable, 0.3% for volatile)
        fee_percent = Decimal("0.0005") if stable else Decimal("0.003")
        usd_after_fee = usd_value * (Decimal("1") - fee_percent)

        amount_out_decimal = usd_after_fee / price_out
        amount_out = int(amount_out_decimal * Decimal(10**token_out_decimals))

        effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal("0")

        return SwapQuote(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            stable=stable,
            effective_price=effective_price,
            gas_estimate=AERODROME_GAS_ESTIMATES["swap"],
        )

    def _try_get_amount_out_onchain(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        stable: bool,
    ) -> int | None:
        """Best-effort on-chain quote for amount out via router.getAmountsOut().

        Returns None when rpc_url is not configured or when the quote cannot be fetched.
        """
        if not self.config.rpc_url:
            return None

        try:
            from web3 import Web3

            if self._web3 is None:
                self._web3 = Web3(Web3.HTTPProvider(self.config.rpc_url, request_kwargs={"timeout": 15}))

            from .sdk import SwapRoute

            routes = [SwapRoute(from_token=token_in, to_token=token_out, stable=stable)]
            amounts = self.sdk.get_amounts_out(amount_in, routes, self._web3)
            if not amounts:
                return None

            return int(amounts[-1])
        except Exception as e:
            logger.debug("Aerodrome on-chain quote failed; falling back to price oracle: %s", e)
            return None

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _resolve_token(self, token: str) -> str:
        """Resolve token symbol or address to address using TokenResolver."""
        if token.startswith("0x") and len(token) == 42:
            return token
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[AerodromeAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

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
                reason=f"[AerodromeAdapter] Cannot resolve symbol: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_token_decimals(self, symbol: str) -> int:
        """Get token decimals from symbol using TokenResolver."""
        try:
            resolved = self._token_resolver.resolve(symbol, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=symbol,
                chain=str(self.chain),
                reason=f"[AerodromeAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _is_native_token(self, token: str) -> bool:
        """Check if token is the native token (ETH)."""
        if token.upper() == "ETH":
            return True
        native_placeholder = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE".lower()
        if token.lower() == native_placeholder:
            return True
        return False

    def _get_placeholder_prices(self) -> dict[str, Decimal]:
        """Get placeholder price data for testing only.

        WARNING: These prices are HARDCODED and OUTDATED.
        DO NOT USE IN PRODUCTION - they will cause:
        - Incorrect slippage calculations
        - Swap reverts (amountOutMinimum too high)

        Real prices as of 2026-01: ETH ~$3400
        These placeholders show ETH at $2000 - 40% wrong!
        """
        logger.warning("PLACEHOLDER PRICES being used - NOT SAFE FOR PRODUCTION. ETH=$2000 (real ~$3400)")
        return {
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDC": Decimal("1"),
            "USDbC": Decimal("1"),
            "DAI": Decimal("1"),
            "AERO": Decimal("1.50"),
            "cbETH": Decimal("2100"),
            "rETH": Decimal("2200"),
        }

    def _get_default_price_oracle(self) -> dict[str, Decimal]:
        """Get price oracle data (uses instance price provider).

        Deprecated: This method exists for backward compatibility.
        The adapter now uses self._price_provider initialized in __init__.
        """
        return self._price_provider

    def _encode_route(self, token_in: str, token_out: str, stable: bool) -> str:
        """Encode a Route struct for Aerodrome."""
        return (
            self._pad_address(token_in)
            + self._pad_address(token_out)
            + self._pad_bool(stable)
            + self._pad_address(self.addresses["factory"])
        )

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return addr.lower().replace("0x", "").zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_int24(value: int) -> str:
        """Pad int24 to 32 bytes (two's complement for negative values)."""
        if value < 0:
            value = (1 << 256) + value
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_bool(value: bool) -> str:
        """Pad bool to 32 bytes."""
        return "0" * 63 + ("1" if value else "0")

    # =========================================================================
    # State Management
    # =========================================================================

    def set_allowance(self, token: str, spender: str, amount: int) -> None:
        """Set cached allowance (for testing)."""
        cache_key = f"{token}:{spender}"
        self._allowance_cache[cache_key] = amount

    def clear_allowance_cache(self) -> None:
        """Clear the allowance cache."""
        self._allowance_cache.clear()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "AerodromeAdapter",
    "AerodromeConfig",
    "SwapQuote",
    "SwapResult",
    "SwapType",
    "PoolType",
    "LiquidityResult",
    "TransactionData",
    "AERODROME_ADDRESSES",
    "AERODROME_GAS_ESTIMATES",
]
