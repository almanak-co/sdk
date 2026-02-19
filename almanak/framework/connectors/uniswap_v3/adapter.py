"""Uniswap V3 Protocol Adapter.

This module provides the UniswapV3Adapter class for executing token swaps
on Uniswap V3 across multiple chains.

Uniswap V3 Architecture:
- SwapRouter: Main entry point for swap execution
- Factory: Creates and manages pools
- NonfungiblePositionManager: Manages LP positions
- QuoterV2: Get swap quotes without executing

Key Concepts:
- Pool: Token pair with specific fee tier (0.01%, 0.05%, 0.3%, 1%)
- Swap: Exchange one token for another
- exactInputSingle: Specify exact input amount, receive variable output
- exactOutputSingle: Specify exact output amount, pay variable input
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.core.contracts import UNISWAP_V3 as UNISWAP_V3_ADDRESSES
from almanak.framework.data.tokens.exceptions import TokenResolutionError

from ...intents.vocabulary import IntentType, SwapIntent
from ...models.reproduction_bundle import ActionBundle

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================


# Fee tiers in basis points
FEE_TIERS: dict[int, str] = {
    100: "0.01%",
    500: "0.05%",
    3000: "0.3%",
    10000: "1%",
}

# Default fee tier
DEFAULT_FEE_TIER = 3000  # 0.3%

# Gas estimates for Uniswap V3 operations
UNISWAP_V3_GAS_ESTIMATES: dict[str, int] = {
    "approve": 46000,
    "swap_exact_input": 150000,
    "swap_exact_output": 170000,
    "swap_with_unwrap": 200000,
    "multicall": 250000,
}

# Function selectors for SwapRouter02 / IV3SwapRouter (7-param struct, no deadline)
# See: https://github.com/Uniswap/swap-router-contracts/blob/main/contracts/interfaces/IV3SwapRouter.sol
EXACT_INPUT_SINGLE_SELECTOR = "0x04e45aaf"
EXACT_OUTPUT_SINGLE_SELECTOR = "0x5023b4df"
MULTICALL_SELECTOR = "0xac9650d8"

# ERC20 approve selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"

# Max uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1

# Default deadline (100 days in seconds)
DEFAULT_DEADLINE_SECONDS = 8640000


# =============================================================================
# Enums
# =============================================================================


class SwapType(Enum):
    """Type of swap operation."""

    EXACT_INPUT = "EXACT_INPUT"
    EXACT_OUTPUT = "EXACT_OUTPUT"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class UniswapV3Config:
    """Configuration for UniswapV3Adapter.

    Attributes:
        chain: Target blockchain (ethereum, arbitrum, optimism, polygon, base)
        wallet_address: Address executing transactions
        default_slippage_bps: Default slippage tolerance in basis points (default 50 = 0.5%)
        default_fee_tier: Default fee tier for pools (default 3000 = 0.3%)
        deadline_seconds: Transaction deadline in seconds (default 300 = 5 minutes)
        price_provider: Price oracle dict (token symbol -> USD price). Required for
            production use to calculate accurate slippage amounts.
        allow_placeholder_prices: If False (default), raises ValueError when no
            price_provider is given. Set to True ONLY for unit tests.
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50
    default_fee_tier: int = DEFAULT_FEE_TIER
    deadline_seconds: int = 300
    price_provider: dict[str, Decimal] | None = None
    allow_placeholder_prices: bool = False

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.chain not in UNISWAP_V3_ADDRESSES:
            raise ValueError(f"Unsupported chain: {self.chain}. Supported: {list(UNISWAP_V3_ADDRESSES.keys())}")

        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError("Slippage must be between 0 and 10000 basis points")

        if self.default_fee_tier not in FEE_TIERS:
            raise ValueError(f"Invalid fee tier: {self.default_fee_tier}. Valid tiers: {list(FEE_TIERS.keys())}")

        # Validate price_provider requirement
        if self.price_provider is None and not self.allow_placeholder_prices:
            raise ValueError(
                "UniswapV3Config requires price_provider for production use. "
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
            "default_fee_tier": self.default_fee_tier,
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
        fee_tier: Fee tier of the pool
        sqrt_price_x96_after: Price after swap (sqrt format)
        gas_estimate: Estimated gas for the swap
        price_impact_bps: Price impact in basis points
        effective_price: Effective price of the swap
        quoted_at: Timestamp when quote was fetched
    """

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    fee_tier: int
    sqrt_price_x96_after: int = 0
    gas_estimate: int = UNISWAP_V3_GAS_ESTIMATES["swap_exact_input"]
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
            "fee_tier": self.fee_tier,
            "sqrt_price_x96_after": str(self.sqrt_price_x96_after),
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


# =============================================================================
# Uniswap V3 Adapter
# =============================================================================


class UniswapV3Adapter:
    """Adapter for Uniswap V3 DEX protocol.

    This adapter provides methods for:
    - Executing token swaps (exact input and exact output)
    - Building swap transactions
    - Handling ERC-20 approvals
    - Managing slippage protection

    Example:
        config = UniswapV3Config(
            chain="arbitrum",
            wallet_address="0x...",
        )
        adapter = UniswapV3Adapter(config)

        # Execute a swap
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),  # 1000 USDC
            slippage_bps=50,
        )

        # Compile a SwapIntent to ActionBundle
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
        )
        bundle = adapter.compile_swap_intent(intent)
    """

    def __init__(self, config: UniswapV3Config, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize the adapter.

        Args:
            config: Uniswap V3 adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Load contract addresses
        self.addresses = UNISWAP_V3_ADDRESSES[self.chain]

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
                "UniswapV3Adapter using PLACEHOLDER PRICES. "
                "Slippage calculations will be INCORRECT. "
                "This is only acceptable for unit tests."
            )
            self._price_provider = self._get_placeholder_prices()
        else:
            self._price_provider = config.price_provider if config.price_provider is not None else {}

        # Allowance cache (token -> amount approved)
        self._allowance_cache: dict[str, int] = {}

        logger.info(
            f"UniswapV3Adapter initialized for chain={self.chain}, "
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
        slippage_bps: int | None = None,
        fee_tier: int | None = None,
        recipient: str | None = None,
    ) -> SwapResult:
        """Build a swap transaction with exact input amount.

        This is the most common swap type where you specify exactly how much
        you want to spend and accept variable output.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Amount of input token (in token units, not wei)
            slippage_bps: Slippage tolerance in basis points (default from config)
            fee_tier: Pool fee tier (default from config)
            recipient: Address to receive output tokens (default: wallet_address)

        Returns:
            SwapResult with transaction data
        """
        try:
            # Use defaults from config if not specified
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            fee_tier = fee_tier or self.config.default_fee_tier
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

            # Calculate minimum output with slippage
            # In production, this would query the QuoterV2 contract
            # For now, use a simple estimate
            quote = self._get_quote_exact_input(token_in_address, token_out_address, amount_in_wei, fee_tier)

            amount_out_minimum = int(quote.amount_out * (10000 - slippage_bps) // 10000)

            # Build transactions
            transactions: list[TransactionData] = []

            # Check if we need native token handling
            is_native_input = self._is_native_token(token_in)
            actual_token_in = token_in_address

            if is_native_input:
                # Use WETH for the swap - resolve WETH address
                weth_address = self._resolve_token("WETH")
                actual_token_in = weth_address if weth_address else token_in_address

            # Build approve transaction if needed (skip for native token)
            if not is_native_input:
                approve_tx = self._build_approve_tx(
                    actual_token_in,
                    self.addresses["swap_router"],
                    amount_in_wei,
                )
                if approve_tx is not None:
                    transactions.append(approve_tx)

            # Build swap transaction
            swap_tx = self._build_exact_input_single_tx(
                token_in=actual_token_in,
                token_out=token_out_address,
                fee=fee_tier,
                recipient=recipient,
                amount_in=amount_in_wei,
                amount_out_minimum=amount_out_minimum,
                value=amount_in_wei if is_native_input else 0,
            )
            transactions.append(swap_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built swap: {token_in} -> {token_out}, "
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

    def swap_exact_output(
        self,
        token_in: str,
        token_out: str,
        amount_out: Decimal,
        slippage_bps: int | None = None,
        fee_tier: int | None = None,
        recipient: str | None = None,
    ) -> SwapResult:
        """Build a swap transaction with exact output amount.

        This swap type specifies exactly how much you want to receive
        and accepts variable input.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_out: Amount of output token (in token units, not wei)
            slippage_bps: Slippage tolerance in basis points (default from config)
            fee_tier: Pool fee tier (default from config)
            recipient: Address to receive output tokens (default: wallet_address)

        Returns:
            SwapResult with transaction data
        """
        try:
            # Use defaults from config if not specified
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            fee_tier = fee_tier or self.config.default_fee_tier
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
            token_out_decimals = self._get_token_decimals(token_out)

            # Convert amount to wei
            amount_out_wei = int(amount_out * Decimal(10**token_out_decimals))

            # Calculate maximum input with slippage
            quote = self._get_quote_exact_output(token_in_address, token_out_address, amount_out_wei, fee_tier)

            amount_in_maximum = int(quote.amount_in * (10000 + slippage_bps) // 10000)

            # Build transactions
            transactions: list[TransactionData] = []

            # Check if we need native token handling
            is_native_input = self._is_native_token(token_in)
            actual_token_in = token_in_address

            if is_native_input:
                # Use WETH for the swap - resolve WETH address
                weth_address = self._resolve_token("WETH")
                actual_token_in = weth_address if weth_address else token_in_address

            # Build approve transaction if needed
            if not is_native_input:
                approve_tx = self._build_approve_tx(
                    actual_token_in,
                    self.addresses["swap_router"],
                    amount_in_maximum,
                )
                if approve_tx is not None:
                    transactions.append(approve_tx)

            # Build swap transaction
            swap_tx = self._build_exact_output_single_tx(
                token_in=actual_token_in,
                token_out=token_out_address,
                fee=fee_tier,
                recipient=recipient,
                amount_out=amount_out_wei,
                amount_in_maximum=amount_in_maximum,
                value=amount_in_maximum if is_native_input else 0,
            )
            transactions.append(swap_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built exact output swap: {token_in} -> {token_out}, "
                f"amount_out={amount_out}, slippage={slippage_bps}bps, "
                f"transactions={len(transactions)}"
            )

            return SwapResult(
                success=True,
                transactions=transactions,
                quote=quote,
                amount_in=amount_in_maximum,
                amount_out_minimum=amount_out_wei,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build exact output swap: {e}")
            return SwapResult(
                success=False,
                error=str(e),
            )

    # =========================================================================
    # Intent Compilation
    # =========================================================================

    def compile_swap_intent(
        self,
        intent: SwapIntent,
        price_oracle: dict[str, Decimal] | None = None,
    ) -> ActionBundle:
        """Compile a SwapIntent to an ActionBundle.

        This method integrates with the intent system to convert high-level
        swap intents into executable transaction bundles.

        Args:
            intent: The SwapIntent to compile
            price_oracle: Optional price oracle for USD conversions

        Returns:
            ActionBundle containing transactions for execution
        """
        # Use default price oracle if not provided
        if price_oracle is None:
            price_oracle = self._get_default_price_oracle()

        # Determine the swap amount
        if intent.amount is not None:
            # Check for chained amount - must be resolved before compilation
            if intent.amount == "all":
                raise ValueError(
                    "amount='all' must be resolved before compilation. "
                    "Use Intent.set_resolved_amount() to resolve chained amounts."
                )
            # Direct token amount specified - type is validated above to be Decimal
            amount_in: Decimal = intent.amount  # type: ignore[assignment]
        elif intent.amount_usd is not None:
            # Convert USD to token amount
            from_price = price_oracle.get(intent.from_token.upper(), Decimal("1"))
            if from_price == 0:
                from_price = Decimal("1")
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
            slippage_bps=slippage_bps,
        )

        if not result.success:
            # Return empty bundle with error metadata
            return ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[],
                metadata={
                    "error": result.error,
                    "intent_id": intent.intent_id,
                },
            )

        # Build ActionBundle
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
                "router": self.addresses["swap_router"],
                "gas_estimate": result.gas_estimate,
            },
        )

    # =========================================================================
    # Transaction Building
    # =========================================================================

    def _build_exact_input_single_tx(
        self,
        token_in: str,
        token_out: str,
        fee: int,
        recipient: str,
        amount_in: int,
        amount_out_minimum: int,
        value: int = 0,
    ) -> TransactionData:
        """Build exactInputSingle swap transaction.

        SwapRouter02 ExactInputSingleParams struct (7 params, no deadline):
        - address tokenIn
        - address tokenOut
        - uint24 fee
        - address recipient
        - uint256 amountIn
        - uint256 amountOutMinimum
        - uint160 sqrtPriceLimitX96

        Note: SwapRouter02 doesn't have deadline in the struct.
        Deadline is handled via multicall wrapper if needed.
        """
        # Encode parameters for SwapRouter02 (no deadline in struct)
        calldata = (
            EXACT_INPUT_SINGLE_SELECTOR
            + self._pad_address(token_in)
            + self._pad_address(token_out)
            + self._pad_uint24(fee)
            + self._pad_address(recipient)
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
            to=self.addresses["swap_router"],
            value=value,
            data=calldata,
            gas_estimate=UNISWAP_V3_GAS_ESTIMATES["swap_exact_input"],
            description=(
                f"Swap {amount_in_formatted:.6f} {token_in_symbol} -> "
                f"{token_out_symbol} (min: {amount_out_formatted:.6f})"
            ),
            tx_type="swap",
        )

    def _build_exact_output_single_tx(
        self,
        token_in: str,
        token_out: str,
        fee: int,
        recipient: str,
        amount_out: int,
        amount_in_maximum: int,
        value: int = 0,
    ) -> TransactionData:
        """Build exactOutputSingle swap transaction.

        SwapRouter02 ExactOutputSingleParams struct (7 params, no deadline):
        - address tokenIn
        - address tokenOut
        - uint24 fee
        - address recipient
        - uint256 amountOut
        - uint256 amountInMaximum
        - uint160 sqrtPriceLimitX96

        Note: SwapRouter02 doesn't have deadline in the struct.
        Deadline is handled via multicall wrapper if needed.
        """
        # Encode parameters for SwapRouter02 (no deadline in struct)
        calldata = (
            EXACT_OUTPUT_SINGLE_SELECTOR
            + self._pad_address(token_in)
            + self._pad_address(token_out)
            + self._pad_uint24(fee)
            + self._pad_address(recipient)
            + self._pad_uint256(amount_out)
            + self._pad_uint256(amount_in_maximum)
            + self._pad_uint256(0)  # sqrtPriceLimitX96 = 0 (no limit)
        )

        # Format amounts for description
        token_in_symbol = self._get_token_symbol(token_in)
        token_out_symbol = self._get_token_symbol(token_out)
        token_in_decimals = self._get_token_decimals(token_in_symbol)
        token_out_decimals = self._get_token_decimals(token_out_symbol)

        amount_in_formatted = Decimal(str(amount_in_maximum)) / Decimal(10**token_in_decimals)
        amount_out_formatted = Decimal(str(amount_out)) / Decimal(10**token_out_decimals)

        return TransactionData(
            to=self.addresses["swap_router"],
            value=value,
            data=calldata,
            gas_estimate=UNISWAP_V3_GAS_ESTIMATES["swap_exact_output"],
            description=(
                f"Swap {token_in_symbol} (max: {amount_in_formatted:.6f}) -> "
                f"{amount_out_formatted:.6f} {token_out_symbol}"
            ),
            tx_type="swap",
        )

    def _build_approve_tx(
        self,
        token_address: str,
        spender: str,
        amount: int,
    ) -> TransactionData | None:
        """Build an ERC-20 approve transaction if needed.

        Args:
            token_address: Token to approve
            spender: Address to approve (router)
            amount: Amount to approve

        Returns:
            TransactionData for approve, or None if sufficient allowance exists
        """
        # Check cache for existing allowance
        cache_key = f"{token_address}:{spender}"
        cached = self._allowance_cache.get(cache_key, 0)
        if cached >= amount:
            logger.debug(f"Sufficient allowance exists for {token_address}")
            return None

        # Build approve calldata: approve(address spender, uint256 amount)
        calldata = (
            ERC20_APPROVE_SELECTOR + self._pad_address(spender) + self._pad_uint256(MAX_UINT256)  # Use max approval
        )

        # Update cache
        self._allowance_cache[cache_key] = MAX_UINT256

        token_symbol = self._get_token_symbol(token_address)

        return TransactionData(
            to=token_address,
            value=0,
            data=calldata,
            gas_estimate=UNISWAP_V3_GAS_ESTIMATES["approve"],
            description=f"Approve {token_symbol} for Uniswap V3 Router",
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
        fee_tier: int,
    ) -> SwapQuote:
        """Get quote for exact input swap.

        In production, this would call the QuoterV2 contract.
        For now, returns an estimate based on price oracle.
        """
        # Get token info for price calculation
        token_in_symbol = self._get_token_symbol(token_in)
        token_out_symbol = self._get_token_symbol(token_out)

        prices = self._get_default_price_oracle()
        price_in = prices.get(token_in_symbol, Decimal("1"))
        price_out = prices.get(token_out_symbol, Decimal("1"))

        if price_out == 0:
            price_out = Decimal("1")

        # Calculate expected output
        token_in_decimals = self._get_token_decimals(token_in_symbol)
        token_out_decimals = self._get_token_decimals(token_out_symbol)

        amount_in_decimal = Decimal(str(amount_in)) / Decimal(10**token_in_decimals)
        usd_value = amount_in_decimal * price_in

        # Apply fee (0.3% for default tier)
        fee_percent = Decimal(str(fee_tier)) / Decimal("1000000")
        usd_after_fee = usd_value * (Decimal("1") - fee_percent)

        amount_out_decimal = usd_after_fee / price_out
        amount_out = int(amount_out_decimal * Decimal(10**token_out_decimals))

        # Calculate effective price
        effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal("0")

        return SwapQuote(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            fee_tier=fee_tier,
            effective_price=effective_price,
            gas_estimate=UNISWAP_V3_GAS_ESTIMATES["swap_exact_input"],
        )

    def _get_quote_exact_output(
        self,
        token_in: str,
        token_out: str,
        amount_out: int,
        fee_tier: int,
    ) -> SwapQuote:
        """Get quote for exact output swap.

        In production, this would call the QuoterV2 contract.
        For now, returns an estimate based on price oracle.
        """
        # Get token info for price calculation
        token_in_symbol = self._get_token_symbol(token_in)
        token_out_symbol = self._get_token_symbol(token_out)

        prices = self._get_default_price_oracle()
        price_in = prices.get(token_in_symbol, Decimal("1"))
        price_out = prices.get(token_out_symbol, Decimal("1"))

        if price_in == 0:
            price_in = Decimal("1")

        # Calculate required input
        token_in_decimals = self._get_token_decimals(token_in_symbol)
        token_out_decimals = self._get_token_decimals(token_out_symbol)

        amount_out_decimal = Decimal(str(amount_out)) / Decimal(10**token_out_decimals)
        usd_value = amount_out_decimal * price_out

        # Add fee (0.3% for default tier)
        fee_percent = Decimal(str(fee_tier)) / Decimal("1000000")
        usd_before_fee = usd_value / (Decimal("1") - fee_percent)

        amount_in_decimal = usd_before_fee / price_in
        amount_in = int(amount_in_decimal * Decimal(10**token_in_decimals))

        # Calculate effective price
        effective_price = amount_out_decimal / amount_in_decimal if amount_in_decimal > 0 else Decimal("0")

        return SwapQuote(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            fee_tier=fee_tier,
            effective_price=effective_price,
            gas_estimate=UNISWAP_V3_GAS_ESTIMATES["swap_exact_output"],
        )

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
                reason=f"[UniswapV3Adapter] Cannot resolve token: {e.reason}",
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
                reason=f"[UniswapV3Adapter] Cannot resolve symbol: {e.reason}",
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
                reason=f"[UniswapV3Adapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _is_native_token(self, token: str) -> bool:
        """Check if token is the native token (ETH, MATIC, AVAX, BNB, MNT, etc.)."""
        native_tokens = {"ETH", "MATIC", "AVAX", "BNB", "MNT"}
        if token.upper() in native_tokens:
            return True
        # Check native placeholder address
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
            "WETH.e": Decimal("2000"),
            "USDC": Decimal("1"),
            "USDC.e": Decimal("1"),
            "USDT": Decimal("1"),
            "DAI": Decimal("1"),
            "DAI.e": Decimal("1"),
            "WBTC": Decimal("45000"),
            "WBTC.e": Decimal("45000"),
            "BTCB": Decimal("45000"),
            "ARB": Decimal("1.20"),
            "OP": Decimal("2.50"),
            "MATIC": Decimal("0.80"),
            "WMATIC": Decimal("0.80"),
            "AVAX": Decimal("35"),
            "WAVAX": Decimal("35"),
            "BNB": Decimal("300"),
            "WBNB": Decimal("300"),
            "MNT": Decimal("1"),
            "WMNT": Decimal("1"),
        }

    def _get_default_price_oracle(self) -> dict[str, Decimal]:
        """Get price oracle data (uses instance price provider).

        Deprecated: This method exists for backward compatibility.
        The adapter now uses self._price_provider initialized in __init__.
        """
        return self._price_provider

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return addr.lower().replace("0x", "").zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_uint24(value: int) -> str:
        """Pad uint24 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    # =========================================================================
    # State Management (for testing/simulation)
    # =========================================================================

    def set_allowance(self, token: str, spender: str, amount: int) -> None:
        """Set cached allowance (for testing).

        Args:
            token: Token address
            spender: Spender address
            amount: Allowance amount
        """
        cache_key = f"{token}:{spender}"
        self._allowance_cache[cache_key] = amount

    def clear_allowance_cache(self) -> None:
        """Clear the allowance cache."""
        self._allowance_cache.clear()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "UniswapV3Adapter",
    "UniswapV3Config",
    "SwapQuote",
    "SwapResult",
    "SwapType",
    "TransactionData",
    "UNISWAP_V3_ADDRESSES",
    "UNISWAP_V3_GAS_ESTIMATES",
    "FEE_TIERS",
    "DEFAULT_FEE_TIER",
]
