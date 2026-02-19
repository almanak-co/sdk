"""SushiSwap V3 Protocol Adapter.

This module provides the SushiSwapV3Adapter class for executing token swaps
and LP operations on SushiSwap V3 across multiple chains.

SushiSwap V3 Architecture (fork of Uniswap V3):
- SwapRouter: Main entry point for swap execution
- Factory: Creates and manages pools
- NonfungiblePositionManager: Manages LP positions as NFTs
- QuoterV2: Get swap quotes without executing

Key Concepts:
- Pool: Token pair with specific fee tier (0.01%, 0.05%, 0.3%, 1%)
- Swap: Exchange one token for another
- exactInputSingle: Specify exact input amount, receive variable output
- exactOutputSingle: Specify exact output amount, pay variable input
- LP Position: Concentrated liquidity within a tick range

Supported Chains:
- Arbitrum (primary)
- Ethereum
- Base
- Polygon
- Avalanche
- BSC
- Optimism
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

from .sdk import (
    FACTORY_ADDRESSES,
    FEE_TIERS,
    POSITION_MANAGER_ADDRESSES,
    QUOTER_ADDRESSES,
    ROUTER_ADDRESSES,
    MintParams,
    SushiSwapV3SDK,
    sort_tokens,
)

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# SushiSwap V3 contract addresses per chain (derived from SDK imports)
SUSHISWAP_V3_ADDRESSES: dict[str, dict[str, str]] = {
    chain: {
        "swap_router": ROUTER_ADDRESSES[chain],
        "factory": FACTORY_ADDRESSES[chain],
        "position_manager": POSITION_MANAGER_ADDRESSES[chain],
        "quoter_v2": QUOTER_ADDRESSES[chain],
    }
    for chain in FACTORY_ADDRESSES
}

# Default fee tier
DEFAULT_FEE_TIER = 3000  # 0.3%

# Gas estimates for SushiSwap V3 operations
SUSHISWAP_V3_GAS_ESTIMATES: dict[str, int] = {
    "approve": 46000,
    "swap_exact_input": 150000,
    "swap_exact_output": 170000,
    "swap_with_unwrap": 200000,
    "multicall": 250000,
    "mint": 500000,
    "increase_liquidity": 350000,
    "decrease_liquidity": 250000,
    "collect": 150000,
}

# Function selectors for SwapRouter
EXACT_INPUT_SINGLE_SELECTOR = "0x414bf389"
EXACT_OUTPUT_SINGLE_SELECTOR = "0xdb3e2198"
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
class SushiSwapV3Config:
    """Configuration for SushiSwapV3Adapter.

    Attributes:
        chain: Target blockchain (ethereum, arbitrum, base, polygon, avalanche, bsc, optimism)
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
        if self.chain not in SUSHISWAP_V3_ADDRESSES:
            raise ValueError(f"Unsupported chain: {self.chain}. Supported: {list(SUSHISWAP_V3_ADDRESSES.keys())}")

        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError("Slippage must be between 0 and 10000 basis points")

        if self.default_fee_tier not in FEE_TIERS:
            raise ValueError(f"Invalid fee tier: {self.default_fee_tier}. Valid tiers: {FEE_TIERS}")

        # Validate price_provider requirement
        if self.price_provider is None and not self.allow_placeholder_prices:
            raise ValueError(
                "SushiSwapV3Config requires price_provider for production use. "
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
    gas_estimate: int = SUSHISWAP_V3_GAS_ESTIMATES["swap_exact_input"]
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
        tx_type: Type of transaction (approve, swap, mint, etc.)
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


@dataclass
class LPResult:
    """Result of an LP operation.

    Attributes:
        success: Whether the operation was built successfully
        transactions: List of transactions to execute
        error: Error message if failed
        gas_estimate: Total gas estimate for all transactions
        position_info: Additional info about the LP position
    """

    success: bool
    transactions: list[TransactionData] = field(default_factory=list)
    error: str | None = None
    gas_estimate: int = 0
    position_info: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "error": self.error,
            "gas_estimate": self.gas_estimate,
            "position_info": self.position_info,
        }


# =============================================================================
# SushiSwap V3 Adapter
# =============================================================================


class SushiSwapV3Adapter:
    """Adapter for SushiSwap V3 DEX protocol.

    This adapter provides methods for:
    - Executing token swaps (exact input and exact output)
    - Building swap transactions
    - Managing LP positions (mint, increase, decrease, collect)
    - Handling ERC-20 approvals
    - Managing slippage protection

    Example:
        config = SushiSwapV3Config(
            chain="arbitrum",
            wallet_address="0x...",
            price_provider={"ETH": Decimal("3400"), "USDC": Decimal("1")},
        )
        adapter = SushiSwapV3Adapter(config)

        # Execute a swap
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),  # 1000 USDC
            slippage_bps=50,
        )

        # Open LP position
        lp_result = adapter.open_lp_position(
            token0="USDC",
            token1="WETH",
            amount0=Decimal("1000"),
            amount1=Decimal("0.5"),
            fee_tier=3000,
            tick_lower=-887220,
            tick_upper=887220,
        )
    """

    def __init__(self, config: SushiSwapV3Config, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize the adapter.

        Args:
            config: SushiSwap V3 adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Load contract addresses
        self.addresses = SUSHISWAP_V3_ADDRESSES[self.chain]

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Initialize SDK
        self.sdk = SushiSwapV3SDK(chain=self.chain)

        # Price provider - use provided or fall back to placeholders (only if allowed)
        self._using_placeholders = config.price_provider is None
        if self._using_placeholders:
            logger.warning(
                "SushiSwapV3Adapter using PLACEHOLDER PRICES. "
                "Slippage calculations will be INCORRECT. "
                "This is only acceptable for unit tests."
            )
            self._price_provider = self._get_placeholder_prices()
        else:
            self._price_provider = config.price_provider or {}

        # Allowance cache (token -> amount approved)
        self._allowance_cache: dict[str, int] = {}

        logger.info(
            f"SushiSwapV3Adapter initialized for chain={self.chain}, "
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
            # Use explicit None checks to preserve valid zero values
            if slippage_bps is None:
                slippage_bps = self.config.default_slippage_bps
            if fee_tier is None:
                fee_tier = self.config.default_fee_tier
            if recipient is None:
                recipient = self.wallet_address

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
            quote = self._get_quote_exact_input(token_in_address, token_out_address, amount_in_wei, fee_tier)

            amount_out_minimum = int(quote.amount_out * (10000 - slippage_bps) // 10000)

            # Build transactions
            transactions: list[TransactionData] = []

            # Check if we need native token handling
            is_native_input = self._is_native_token(token_in)
            actual_token_in = token_in_address

            if is_native_input:
                # Resolve native token to its wrapped counterpart (WETH, WMATIC, WAVAX, etc.)
                wrapped = self._token_resolver.resolve_for_swap(token_in, self.chain)
                actual_token_in = wrapped.address

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
            # Use explicit None checks to preserve valid zero values
            if slippage_bps is None:
                slippage_bps = self.config.default_slippage_bps
            if fee_tier is None:
                fee_tier = self.config.default_fee_tier
            if recipient is None:
                recipient = self.wallet_address

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
                # Resolve native token to its wrapped counterpart (WETH, WMATIC, WAVAX, etc.)
                wrapped = self._token_resolver.resolve_for_swap(token_in, self.chain)
                actual_token_in = wrapped.address

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
    # LP Operations
    # =========================================================================

    def open_lp_position(
        self,
        token0: str,
        token1: str,
        amount0: Decimal,
        amount1: Decimal,
        fee_tier: int | None = None,
        tick_lower: int = -887220,
        tick_upper: int = 887220,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LPResult:
        """Build transactions to open a new LP position.

        Args:
            token0: First token symbol or address
            token1: Second token symbol or address
            amount0: Amount of token0 to provide
            amount1: Amount of token1 to provide
            fee_tier: Pool fee tier (default from config)
            tick_lower: Lower tick bound (default: full range)
            tick_upper: Upper tick bound (default: full range)
            slippage_bps: Slippage tolerance in basis points (default from config)
            recipient: Address to receive the NFT (default: wallet_address)

        Returns:
            LPResult with transaction data
        """
        try:
            # Use explicit None checks to preserve valid zero values
            if fee_tier is None:
                fee_tier = self.config.default_fee_tier
            if slippage_bps is None:
                slippage_bps = self.config.default_slippage_bps
            if recipient is None:
                recipient = self.wallet_address

            # Resolve token addresses
            token0_address = self._resolve_token(token0)
            token1_address = self._resolve_token(token1)

            if token0_address is None:
                return LPResult(success=False, error=f"Unknown token: {token0}")
            if token1_address is None:
                return LPResult(success=False, error=f"Unknown token: {token1}")

            # Sort tokens for the pool
            sorted_token0, sorted_token1 = sort_tokens(token0_address, token1_address)
            sorted_amount0, sorted_amount1 = amount0, amount1
            tokens_swapped = sorted_token0.lower() != token0_address.lower()
            if tokens_swapped:
                sorted_amount0, sorted_amount1 = amount1, amount0

            # Get token decimals - must match sorted token order
            if tokens_swapped:
                sorted_token0_decimals = self._get_token_decimals(token1)
                sorted_token1_decimals = self._get_token_decimals(token0)
            else:
                sorted_token0_decimals = self._get_token_decimals(token0)
                sorted_token1_decimals = self._get_token_decimals(token1)

            # Convert amounts to wei
            amount0_wei = int(sorted_amount0 * Decimal(10**sorted_token0_decimals))
            amount1_wei = int(sorted_amount1 * Decimal(10**sorted_token1_decimals))

            # Calculate minimum amounts with slippage
            amount0_min = int(amount0_wei * (10000 - slippage_bps) // 10000)
            amount1_min = int(amount1_wei * (10000 - slippage_bps) // 10000)

            # Build transactions
            transactions: list[TransactionData] = []

            # Approve token0
            approve0_tx = self._build_approve_tx(
                sorted_token0,
                self.addresses["position_manager"],
                amount0_wei,
            )
            if approve0_tx is not None:
                transactions.append(approve0_tx)

            # Approve token1
            approve1_tx = self._build_approve_tx(
                sorted_token1,
                self.addresses["position_manager"],
                amount1_wei,
            )
            if approve1_tx is not None:
                transactions.append(approve1_tx)

            # Build mint transaction
            deadline = int(datetime.now(UTC).timestamp()) + self.config.deadline_seconds
            mint_params = MintParams(
                token0=sorted_token0,
                token1=sorted_token1,
                fee=fee_tier,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                amount0_desired=amount0_wei,
                amount1_desired=amount1_wei,
                amount0_min=amount0_min,
                amount1_min=amount1_min,
                recipient=recipient,
                deadline=deadline,
            )

            mint_tx_data = self.sdk.build_mint_tx(mint_params)
            transactions.append(
                TransactionData(
                    to=mint_tx_data.to,
                    value=mint_tx_data.value,
                    data=mint_tx_data.data,
                    gas_estimate=mint_tx_data.gas_estimate,
                    description=mint_tx_data.description,
                    tx_type="mint",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built LP open: {token0}/{token1}, fee_tier={fee_tier}, "
                f"ticks=[{tick_lower}, {tick_upper}], transactions={len(transactions)}"
            )

            return LPResult(
                success=True,
                transactions=transactions,
                gas_estimate=total_gas,
                position_info={
                    "token0": sorted_token0,
                    "token1": sorted_token1,
                    "fee_tier": fee_tier,
                    "tick_lower": tick_lower,
                    "tick_upper": tick_upper,
                    "amount0": str(amount0_wei),
                    "amount1": str(amount1_wei),
                },
            )

        except Exception as e:
            logger.exception(f"Failed to build LP open: {e}")
            return LPResult(success=False, error=str(e))

    def close_lp_position(
        self,
        token_id: int,
        liquidity: int,
        amount0_min: int = 0,
        amount1_min: int = 0,
        recipient: str | None = None,
    ) -> LPResult:
        """Build transactions to close an LP position.

        Args:
            token_id: NFT token ID of the position
            liquidity: Amount of liquidity to remove (use position's full liquidity to close)
            amount0_min: Minimum amount of token0 to receive
            amount1_min: Minimum amount of token1 to receive
            recipient: Address to receive tokens (default: wallet_address)

        Returns:
            LPResult with transaction data
        """
        try:
            recipient = recipient or self.wallet_address
            deadline = int(datetime.now(UTC).timestamp()) + self.config.deadline_seconds

            transactions: list[TransactionData] = []

            # Build decrease liquidity transaction
            decrease_tx = self.sdk.build_decrease_liquidity_tx(
                token_id=token_id,
                liquidity=liquidity,
                amount0_min=amount0_min,
                amount1_min=amount1_min,
                deadline=deadline,
            )
            transactions.append(
                TransactionData(
                    to=decrease_tx.to,
                    value=decrease_tx.value,
                    data=decrease_tx.data,
                    gas_estimate=decrease_tx.gas_estimate,
                    description=decrease_tx.description,
                    tx_type="decrease_liquidity",
                )
            )

            # Build collect transaction
            collect_tx = self.sdk.build_collect_tx(
                token_id=token_id,
                recipient=recipient,
            )
            transactions.append(
                TransactionData(
                    to=collect_tx.to,
                    value=collect_tx.value,
                    data=collect_tx.data,
                    gas_estimate=collect_tx.gas_estimate,
                    description=collect_tx.description,
                    tx_type="collect",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(f"Built LP close: position #{token_id}, liquidity={liquidity}")

            return LPResult(
                success=True,
                transactions=transactions,
                gas_estimate=total_gas,
                position_info={
                    "token_id": token_id,
                    "liquidity_removed": str(liquidity),
                },
            )

        except Exception as e:
            logger.exception(f"Failed to build LP close: {e}")
            return LPResult(success=False, error=str(e))

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
        """Build exactInputSingle swap transaction."""
        deadline = int(datetime.now(UTC).timestamp()) + self.config.deadline_seconds

        # Encode parameters
        calldata = (
            EXACT_INPUT_SINGLE_SELECTOR
            + self._pad_address(token_in)
            + self._pad_address(token_out)
            + self._pad_uint24(fee)
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
            to=self.addresses["swap_router"],
            value=value,
            data=calldata,
            gas_estimate=SUSHISWAP_V3_GAS_ESTIMATES["swap_exact_input"],
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
        """Build exactOutputSingle swap transaction."""
        deadline = int(datetime.now(UTC).timestamp()) + self.config.deadline_seconds

        # Encode parameters
        calldata = (
            EXACT_OUTPUT_SINGLE_SELECTOR
            + self._pad_address(token_in)
            + self._pad_address(token_out)
            + self._pad_uint24(fee)
            + self._pad_address(recipient)
            + self._pad_uint256(deadline)
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
            gas_estimate=SUSHISWAP_V3_GAS_ESTIMATES["swap_exact_output"],
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
            gas_estimate=SUSHISWAP_V3_GAS_ESTIMATES["approve"],
            description=f"Approve {token_symbol} for SushiSwap V3",
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
        """Get quote for exact input swap."""
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
            gas_estimate=SUSHISWAP_V3_GAS_ESTIMATES["swap_exact_input"],
        )

    def _get_quote_exact_output(
        self,
        token_in: str,
        token_out: str,
        amount_out: int,
        fee_tier: int,
    ) -> SwapQuote:
        """Get quote for exact output swap."""
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
            gas_estimate=SUSHISWAP_V3_GAS_ESTIMATES["swap_exact_output"],
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
                reason=f"[SushiSwapV3Adapter] Cannot resolve token: {e.reason}",
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
                reason=f"[SushiSwapV3Adapter] Cannot resolve symbol: {e.reason}",
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
                reason=f"[SushiSwapV3Adapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _is_native_token(self, token: str) -> bool:
        """Check if token is the native token (ETH, MATIC, AVAX, BNB, etc.)."""
        native_tokens = {"ETH", "MATIC", "AVAX", "BNB"}
        if token.upper() in native_tokens:
            return True
        # Check native placeholder address
        native_placeholder = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE".lower()
        if token.lower() == native_placeholder:
            return True
        return False

    def _get_placeholder_prices(self) -> dict[str, Decimal]:
        """Get placeholder price data for testing only."""
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
            "ARB": Decimal("1.20"),
            "OP": Decimal("2.50"),
            "MATIC": Decimal("0.80"),
            "WMATIC": Decimal("0.80"),
            "AVAX": Decimal("35"),
            "WAVAX": Decimal("35"),
            "BNB": Decimal("300"),
            "WBNB": Decimal("300"),
            "SUSHI": Decimal("1"),
        }

    def _get_default_price_oracle(self) -> dict[str, Decimal]:
        """Get price oracle data (uses instance price provider)."""
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
    "SushiSwapV3Adapter",
    "SushiSwapV3Config",
    "SwapQuote",
    "SwapResult",
    "SwapType",
    "TransactionData",
    "LPResult",
    "SUSHISWAP_V3_ADDRESSES",
    "SUSHISWAP_V3_GAS_ESTIMATES",
    "DEFAULT_FEE_TIER",
]
