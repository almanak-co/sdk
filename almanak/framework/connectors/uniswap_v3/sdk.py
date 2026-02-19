"""Uniswap V3 SDK Core Functions.

This module provides essential SDK functions for Uniswap V3 operations:
- Pool address computation
- Quote fetching
- Swap transaction building
- Tick math utilities for price calculations

Ported and improved from src-v0/transaction_builder/protocols/uniswap_v3/uniswap_v3_sdk.py.

Example:
    from almanak.framework.connectors.uniswap_v3.sdk import UniswapV3SDK

    sdk = UniswapV3SDK(chain="arbitrum", rpc_url="https://arb1.arbitrum.io/rpc")

    # Get pool address
    pool = sdk.get_pool_address(token0_address, token1_address, fee_tier=3000)

    # Get quote for swap
    quote = await sdk.get_quote(
        token_in=token0_address,
        token_out=token1_address,
        amount_in=10**18,
        fee_tier=3000,
    )

    # Build swap transaction
    tx_data = sdk.build_swap_tx(
        quote=quote,
        recipient="0x...",
        slippage_bps=50,
        deadline=int(time.time()) + 300,
    )
"""

import hashlib
import logging
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.core.contracts import UNISWAP_V3

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Uniswap V3 constants
Q96 = 2**96
Q128 = 2**128
MAX_UINT128 = 2**128 - 1
MAX_UINT256 = 2**256 - 1

# Tick bounds
MIN_TICK = -887272
MAX_TICK = 887272

# Tick spacing per fee tier
TICK_SPACING: dict[int, int] = {
    100: 1,  # 0.01%
    500: 10,  # 0.05%
    3000: 60,  # 0.3%
    10000: 200,  # 1%
}

# Supported fee tiers
FEE_TIERS: list[int] = [100, 500, 3000, 10000]

# Pool init code hash for CREATE2 address computation
# Uniswap V3 Pool init code hash (same across all chains)
POOL_INIT_CODE_HASH = "0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54"

# Factory addresses per chain
FACTORY_ADDRESSES: dict[str, str] = {chain: addrs["factory"] for chain, addrs in UNISWAP_V3.items()}

# SwapRouter02 addresses per chain
# Note: Using SwapRouter02 which has 7-param struct (no deadline in struct)
# See: https://github.com/Uniswap/swap-router-contracts/blob/main/contracts/interfaces/IV3SwapRouter.sol
ROUTER_ADDRESSES: dict[str, str] = {chain: addrs["swap_router"] for chain, addrs in UNISWAP_V3.items()}

# QuoterV2 addresses per chain
_missing_quoter = [chain for chain, addrs in UNISWAP_V3.items() if "quoter_v2" not in addrs]
if _missing_quoter:
    raise ValueError(f"UNISWAP_V3 registry missing quoter_v2 for chains: {_missing_quoter}")

QUOTER_ADDRESSES: dict[str, str] = {chain: addrs["quoter_v2"] for chain, addrs in UNISWAP_V3.items()}

# Function selectors for SwapRouter02 / IV3SwapRouter (7-param struct, no deadline)
# See: https://github.com/Uniswap/swap-router-contracts/blob/main/contracts/interfaces/IV3SwapRouter.sol
EXACT_INPUT_SINGLE_SELECTOR = "0x04e45aaf"
EXACT_OUTPUT_SINGLE_SELECTOR = "0x5023b4df"
QUOTE_EXACT_INPUT_SINGLE_SELECTOR = "0xc6a5026a"
QUOTE_EXACT_OUTPUT_SINGLE_SELECTOR = "0xbd21704a"


# =============================================================================
# Exceptions
# =============================================================================


class UniswapV3SDKError(Exception):
    """Base exception for Uniswap V3 SDK errors."""

    pass


class InvalidFeeError(UniswapV3SDKError):
    """Invalid fee tier provided."""

    def __init__(self, fee: int) -> None:
        self.fee = fee
        super().__init__(f"Invalid fee tier: {fee}. Valid tiers: {FEE_TIERS}")


class InvalidTickError(UniswapV3SDKError):
    """Invalid tick value provided."""

    def __init__(self, tick: int, reason: str) -> None:
        self.tick = tick
        self.reason = reason
        super().__init__(f"Invalid tick {tick}: {reason}")


class PoolNotFoundError(UniswapV3SDKError):
    """Pool does not exist."""

    def __init__(self, token0: str, token1: str, fee: int) -> None:
        self.token0 = token0
        self.token1 = token1
        self.fee = fee
        super().__init__(f"Pool not found for {token0}/{token1} with fee {fee}")


class QuoteError(UniswapV3SDKError):
    """Error fetching quote."""

    def __init__(self, message: str, token_in: str, token_out: str) -> None:
        self.token_in = token_in
        self.token_out = token_out
        super().__init__(f"Quote error for {token_in}/{token_out}: {message}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PoolInfo:
    """Information about a Uniswap V3 pool.

    Attributes:
        address: Computed pool address
        token0: First token address (sorted)
        token1: Second token address (sorted)
        fee: Fee tier in hundredths of a bip
        tick_spacing: Tick spacing for this fee tier
    """

    address: str
    token0: str
    token1: str
    fee: int
    tick_spacing: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "address": self.address,
            "token0": self.token0,
            "token1": self.token1,
            "fee": self.fee,
            "tick_spacing": self.tick_spacing,
        }


@dataclass
class PoolState:
    """Current state of a Uniswap V3 pool.

    Attributes:
        sqrt_price_x96: Current sqrt price (Q64.96 format)
        tick: Current tick
        liquidity: Current in-range liquidity
        fee_growth_global_0: Fee growth for token0
        fee_growth_global_1: Fee growth for token1
    """

    sqrt_price_x96: int
    tick: int
    liquidity: int
    fee_growth_global_0: int = 0
    fee_growth_global_1: int = 0

    @property
    def price(self) -> Decimal:
        """Get current price (token1 per token0)."""
        return sqrt_price_x96_to_price(self.sqrt_price_x96)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sqrt_price_x96": str(self.sqrt_price_x96),
            "tick": self.tick,
            "liquidity": str(self.liquidity),
            "fee_growth_global_0": str(self.fee_growth_global_0),
            "fee_growth_global_1": str(self.fee_growth_global_1),
            "price": str(self.price),
        }


@dataclass
class SwapQuote:
    """Quote for a swap operation.

    Attributes:
        token_in: Input token address
        token_out: Output token address
        amount_in: Input amount in wei
        amount_out: Expected output amount in wei
        fee: Fee tier
        sqrt_price_x96_after: Price after swap
        initialized_ticks_crossed: Number of initialized ticks crossed
        gas_estimate: Estimated gas for the swap
        quoted_at: Timestamp when quote was fetched
    """

    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    fee: int
    sqrt_price_x96_after: int = 0
    initialized_ticks_crossed: int = 0
    gas_estimate: int = 150000
    quoted_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def effective_price(self) -> Decimal:
        """Calculate effective price of the swap."""
        if self.amount_in == 0:
            return Decimal("0")
        return Decimal(str(self.amount_out)) / Decimal(str(self.amount_in))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "fee": self.fee,
            "sqrt_price_x96_after": str(self.sqrt_price_x96_after),
            "initialized_ticks_crossed": self.initialized_ticks_crossed,
            "gas_estimate": self.gas_estimate,
            "effective_price": str(self.effective_price),
            "quoted_at": self.quoted_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SwapQuote":
        """Create from dictionary."""
        return cls(
            token_in=data["token_in"],
            token_out=data["token_out"],
            amount_in=int(data["amount_in"]),
            amount_out=int(data["amount_out"]),
            fee=data["fee"],
            sqrt_price_x96_after=int(data.get("sqrt_price_x96_after", 0)),
            initialized_ticks_crossed=data.get("initialized_ticks_crossed", 0),
            gas_estimate=data.get("gas_estimate", 150000),
            quoted_at=datetime.fromisoformat(data["quoted_at"]) if "quoted_at" in data else datetime.now(UTC),
        )


@dataclass
class SwapTransaction:
    """Transaction data for a swap.

    Attributes:
        to: Router address
        value: ETH value to send (for native token swaps)
        data: Encoded calldata
        gas_estimate: Estimated gas
        description: Human-readable description
    """

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "to": self.to,
            "value": str(self.value),
            "data": self.data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
        }


# =============================================================================
# Tick Math Utilities
# =============================================================================


def tick_to_sqrt_price_x96(tick: int) -> int:
    """Convert a tick to sqrt price in Q64.96 format.

    Uses the formula: sqrt(1.0001^tick) * 2^96

    Args:
        tick: Tick value

    Returns:
        Sqrt price in Q64.96 format

    Raises:
        InvalidTickError: If tick is out of bounds
    """
    if tick < MIN_TICK or tick > MAX_TICK:
        raise InvalidTickError(tick, f"Must be between {MIN_TICK} and {MAX_TICK}")

    # Calculate sqrt(1.0001^tick)
    # Using math.pow for precision at moderate tick values
    sqrt_ratio = math.pow(1.0001, tick / 2)
    return int(sqrt_ratio * Q96)


def sqrt_price_x96_to_tick(sqrt_price_x96: int) -> int:
    """Convert sqrt price in Q64.96 format to tick.

    Args:
        sqrt_price_x96: Sqrt price in Q64.96 format

    Returns:
        Tick value

    Raises:
        ValueError: If sqrt_price_x96 is invalid
    """
    if sqrt_price_x96 <= 0:
        raise ValueError("sqrt_price_x96 must be positive")

    # Convert back: tick = 2 * log_1.0001(sqrt_price_x96 / 2^96)
    ratio = sqrt_price_x96 / Q96
    if ratio <= 0:
        raise ValueError("Invalid sqrt price ratio")

    tick = math.floor(math.log(ratio, math.sqrt(1.0001)))
    return max(MIN_TICK, min(MAX_TICK, tick))


def tick_to_price(tick: int, decimals0: int = 18, decimals1: int = 18) -> Decimal:
    """Convert a tick to a human-readable price.

    Args:
        tick: Tick value
        decimals0: Decimals of token0
        decimals1: Decimals of token1

    Returns:
        Price of token0 in terms of token1 (adjusted for decimals)
    """
    raw_price = Decimal(str(1.0001**tick))
    decimal_adjustment = Decimal(10 ** (decimals0 - decimals1))
    return raw_price * decimal_adjustment


def price_to_tick(
    price: Decimal | float,
    decimals0: int = 18,
    decimals1: int = 18,
) -> int:
    """Convert a price to the nearest tick.

    Args:
        price: Price of token0 in terms of token1
        decimals0: Decimals of token0
        decimals1: Decimals of token1

    Returns:
        Tick value (may not be on a valid tick spacing boundary)
    """
    if price <= 0:
        return MIN_TICK

    # Adjust for decimals
    decimal_adjustment = 10 ** (decimals0 - decimals1)
    adjusted_price = float(price) / decimal_adjustment

    if adjusted_price <= 0:
        return MIN_TICK

    tick = math.floor(math.log(adjusted_price, 1.0001))
    return max(MIN_TICK, min(MAX_TICK, tick))


def sqrt_price_x96_to_price(sqrt_price_x96: int) -> Decimal:
    """Convert sqrt price X96 to a decimal price.

    Args:
        sqrt_price_x96: Sqrt price in Q64.96 format

    Returns:
        Price as Decimal
    """
    if sqrt_price_x96 <= 0:
        return Decimal("0")
    ratio = Decimal(str(sqrt_price_x96)) / Decimal(str(Q96))
    return ratio * ratio


def price_to_sqrt_price_x96(price: Decimal | float) -> int:
    """Convert a decimal price to sqrt price X96 format.

    Args:
        price: Price as decimal

    Returns:
        Sqrt price in Q64.96 format
    """
    if price <= 0:
        return 0
    sqrt_price = math.sqrt(float(price))
    return int(sqrt_price * Q96)


def get_nearest_tick(tick: int, fee: int) -> int:
    """Get the nearest valid tick for a given fee tier.

    Args:
        tick: Raw tick value
        fee: Fee tier

    Returns:
        Nearest valid tick

    Raises:
        InvalidFeeError: If fee is not a valid tier
    """
    if fee not in TICK_SPACING:
        raise InvalidFeeError(fee)

    tick_spacing = TICK_SPACING[fee]
    rounded = round(tick / tick_spacing) * tick_spacing

    # Clamp to valid range
    min_tick = get_min_tick(fee)
    max_tick = get_max_tick(fee)

    return max(min_tick, min(max_tick, rounded))


def get_min_tick(fee: int) -> int:
    """Get the minimum valid tick for a fee tier.

    Args:
        fee: Fee tier

    Returns:
        Minimum valid tick

    Raises:
        InvalidFeeError: If fee is not a valid tier
    """
    if fee not in TICK_SPACING:
        raise InvalidFeeError(fee)

    tick_spacing = TICK_SPACING[fee]
    # Round towards zero (ceiling for negative)
    return -(-MIN_TICK // tick_spacing) * tick_spacing


def get_max_tick(fee: int) -> int:
    """Get the maximum valid tick for a fee tier.

    Args:
        fee: Fee tier

    Returns:
        Maximum valid tick

    Raises:
        InvalidFeeError: If fee is not a valid tier
    """
    if fee not in TICK_SPACING:
        raise InvalidFeeError(fee)

    tick_spacing = TICK_SPACING[fee]
    return (MAX_TICK // tick_spacing) * tick_spacing


# =============================================================================
# Pool Address Computation
# =============================================================================


def compute_pool_address(
    factory: str,
    token0: str,
    token1: str,
    fee: int,
    init_code_hash: str = POOL_INIT_CODE_HASH,
) -> str:
    """Compute the CREATE2 address for a Uniswap V3 pool.

    This deterministically computes the pool address without any RPC calls.

    Args:
        factory: Factory contract address
        token0: First token address
        token1: Second token address
        fee: Fee tier

    Returns:
        Pool address

    Raises:
        InvalidFeeError: If fee is not a valid tier

    Example:
        >>> pool_addr = compute_pool_address(
        ...     factory="0x1F98431c8aD98523631AE4a59f267346ea31F984",
        ...     token0="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
        ...     token1="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC
        ...     fee=3000,
        ... )
    """
    if fee not in FEE_TIERS:
        raise InvalidFeeError(fee)

    # Sort tokens (Uniswap V3 always orders token0 < token1)
    token0_lower = token0.lower()
    token1_lower = token1.lower()
    if token0_lower > token1_lower:
        token0_lower, token1_lower = token1_lower, token0_lower

    # Normalize addresses (remove 0x prefix)
    token0_bytes = bytes.fromhex(token0_lower.replace("0x", ""))
    token1_bytes = bytes.fromhex(token1_lower.replace("0x", ""))
    factory_bytes = bytes.fromhex(factory.lower().replace("0x", ""))
    init_code_bytes = bytes.fromhex(init_code_hash.replace("0x", ""))

    # Compute salt: keccak256(abi.encode(token0, token1, fee))
    # ABI encoding pads each value to 32 bytes
    token0_padded = token0_bytes.rjust(32, b"\x00")
    token1_padded = token1_bytes.rjust(32, b"\x00")
    fee_padded = fee.to_bytes(32, byteorder="big")

    salt_input = token0_padded + token1_padded + fee_padded
    salt = hashlib.sha3_256(salt_input).digest()

    # CREATE2 address: keccak256(0xff ++ factory ++ salt ++ init_code_hash)
    create2_input = b"\xff" + factory_bytes + salt + init_code_bytes
    address_bytes = hashlib.sha3_256(create2_input).digest()

    # Take last 20 bytes as address
    pool_address = "0x" + address_bytes[-20:].hex()

    return pool_address


def sort_tokens(token0: str, token1: str) -> tuple[str, str]:
    """Sort two token addresses in ascending order.

    Uniswap V3 pools always order tokens such that token0 < token1.

    Args:
        token0: First token address
        token1: Second token address

    Returns:
        Tuple of (token0, token1) sorted in ascending order
    """
    if token0.lower() < token1.lower():
        return token0, token1
    return token1, token0


# =============================================================================
# Uniswap V3 SDK Class
# =============================================================================


class UniswapV3SDK:
    """SDK for Uniswap V3 operations.

    This class provides methods for:
    - Computing pool addresses
    - Fetching quotes (requires RPC)
    - Building swap transactions
    - Tick math utilities

    Example:
        sdk = UniswapV3SDK(chain="arbitrum", rpc_url="https://arb1.arbitrum.io/rpc")

        # Compute pool address (no RPC needed)
        pool_info = sdk.get_pool_address(
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC
            fee_tier=3000,
        )

        # Get quote (requires RPC)
        quote = await sdk.get_quote(
            token_in=weth_address,
            token_out=usdc_address,
            amount_in=10**18,  # 1 WETH
            fee_tier=3000,
        )
    """

    def __init__(
        self,
        chain: str,
        rpc_url: str | None = None,
        web3: Any | None = None,
    ) -> None:
        """Initialize the SDK.

        Args:
            chain: Target blockchain (ethereum, arbitrum, optimism, polygon, base)
            rpc_url: RPC URL for on-chain queries (optional)
            web3: Existing Web3 instance (optional)

        Raises:
            ValueError: If chain is not supported
        """
        if chain not in FACTORY_ADDRESSES:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {list(FACTORY_ADDRESSES.keys())}")

        self.chain = chain
        self.rpc_url = rpc_url
        self._web3 = web3

        # Contract addresses for this chain
        self.factory_address = FACTORY_ADDRESSES[chain]
        self.router_address = ROUTER_ADDRESSES[chain]
        self.quoter_address = QUOTER_ADDRESSES[chain]

        logger.info(f"UniswapV3SDK initialized for chain={chain}")

    # =========================================================================
    # Pool Address Computation
    # =========================================================================

    def get_pool_address(
        self,
        token0: str,
        token1: str,
        fee_tier: int,
    ) -> PoolInfo:
        """Get the pool address for a token pair.

        This computes the CREATE2 address deterministically without RPC calls.

        Args:
            token0: First token address
            token1: Second token address
            fee_tier: Fee tier (100, 500, 3000, 10000)

        Returns:
            PoolInfo with computed address and token ordering

        Raises:
            InvalidFeeError: If fee_tier is not valid

        Example:
            >>> pool = sdk.get_pool_address(weth, usdc, fee_tier=3000)
            >>> print(f"Pool address: {pool.address}")
        """
        if fee_tier not in FEE_TIERS:
            raise InvalidFeeError(fee_tier)

        # Sort tokens
        sorted_token0, sorted_token1 = sort_tokens(token0, token1)

        # Compute address
        address = compute_pool_address(
            factory=self.factory_address,
            token0=sorted_token0,
            token1=sorted_token1,
            fee=fee_tier,
        )

        return PoolInfo(
            address=address,
            token0=sorted_token0,
            token1=sorted_token1,
            fee=fee_tier,
            tick_spacing=TICK_SPACING[fee_tier],
        )

    # =========================================================================
    # Quote Functions
    # =========================================================================

    async def get_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        fee_tier: int,
    ) -> SwapQuote:
        """Get a quote for a swap.

        This fetches the expected output amount for a given input amount
        by calling the QuoterV2 contract.

        Args:
            token_in: Input token address
            token_out: Output token address
            amount_in: Input amount in wei
            fee_tier: Fee tier

        Returns:
            SwapQuote with expected output

        Raises:
            QuoteError: If quote fails
            InvalidFeeError: If fee_tier is not valid

        Note:
            Requires RPC connection. Use get_quote_local for offline estimation.
        """
        if fee_tier not in FEE_TIERS:
            raise InvalidFeeError(fee_tier)

        if self._web3 is None and self.rpc_url is None:
            # Fall back to local estimation
            return self.get_quote_local(token_in, token_out, amount_in, fee_tier)

        try:
            web3 = await self._get_web3()

            # Build quoteExactInputSingle call
            # QuoterV2.quoteExactInputSingle params:
            # (address tokenIn, address tokenOut, uint256 amountIn, uint24 fee, uint160 sqrtPriceLimitX96)
            calldata = self._encode_quote_exact_input_single(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                fee=fee_tier,
            )

            # Call quoter contract
            result = await web3.eth.call(
                {
                    "to": self.quoter_address,
                    "data": calldata,
                }
            )

            # Decode response: (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
            amount_out, sqrt_price_after, ticks_crossed, gas_estimate = self._decode_quote_response(result)

            return SwapQuote(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                amount_out=amount_out,
                fee=fee_tier,
                sqrt_price_x96_after=sqrt_price_after,
                initialized_ticks_crossed=ticks_crossed,
                gas_estimate=gas_estimate,
            )

        except Exception as e:
            logger.warning(f"RPC quote failed, using local estimate: {e}")
            return self.get_quote_local(token_in, token_out, amount_in, fee_tier)

    def get_quote_local(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        fee_tier: int,
        price_ratio: Decimal | None = None,
    ) -> SwapQuote:
        """Get an estimated quote without RPC calls.

        This provides an approximation based on fee tier only.
        For accurate quotes, use get_quote() with RPC.

        Args:
            token_in: Input token address
            token_out: Output token address
            amount_in: Input amount in wei
            fee_tier: Fee tier
            price_ratio: Optional token_out/token_in price ratio

        Returns:
            SwapQuote with estimated output
        """
        if fee_tier not in FEE_TIERS:
            raise InvalidFeeError(fee_tier)

        # Calculate fee deduction
        fee_decimal = Decimal(str(fee_tier)) / Decimal("1000000")
        amount_after_fee = Decimal(str(amount_in)) * (Decimal("1") - fee_decimal)

        # Apply price ratio if provided, otherwise assume 1:1
        if price_ratio is not None and price_ratio > 0:
            amount_out = int(amount_after_fee * price_ratio)
        else:
            amount_out = int(amount_after_fee)

        return SwapQuote(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            fee=fee_tier,
            gas_estimate=150000,  # Default estimate
        )

    # =========================================================================
    # Transaction Building
    # =========================================================================

    def build_swap_tx(
        self,
        quote: SwapQuote,
        recipient: str,
        slippage_bps: int,
        deadline: int,
        value: int = 0,
    ) -> SwapTransaction:
        """Build a swap transaction from a quote.

        Args:
            quote: Quote from get_quote()
            recipient: Address to receive output tokens
            slippage_bps: Slippage tolerance in basis points
            deadline: Unix timestamp deadline
            value: ETH value to send (for native token swaps)

        Returns:
            SwapTransaction with encoded calldata

        Example:
            >>> quote = await sdk.get_quote(weth, usdc, 10**18, 3000)
            >>> tx = sdk.build_swap_tx(
            ...     quote=quote,
            ...     recipient="0x...",
            ...     slippage_bps=50,
            ...     deadline=int(time.time()) + 300,
            ... )
        """
        # Calculate minimum output with slippage
        amount_out_minimum = int(quote.amount_out * (10000 - slippage_bps) // 10000)

        calldata = self._encode_exact_input_single(
            token_in=quote.token_in,
            token_out=quote.token_out,
            fee=quote.fee,
            recipient=recipient,
            deadline=deadline,
            amount_in=quote.amount_in,
            amount_out_minimum=amount_out_minimum,
        )

        description = (
            f"Swap {quote.amount_in} wei -> min {amount_out_minimum} wei (fee={quote.fee}, slippage={slippage_bps}bps)"
        )

        return SwapTransaction(
            to=self.router_address,
            value=value,
            data=calldata,
            gas_estimate=quote.gas_estimate,
            description=description,
        )

    def build_exact_output_swap_tx(
        self,
        token_in: str,
        token_out: str,
        fee: int,
        recipient: str,
        deadline: int,
        amount_out: int,
        amount_in_maximum: int,
        value: int = 0,
    ) -> SwapTransaction:
        """Build an exact output swap transaction.

        For swaps where you specify the exact output amount.

        Args:
            token_in: Input token address
            token_out: Output token address
            fee: Fee tier
            recipient: Address to receive output tokens
            deadline: Unix timestamp deadline
            amount_out: Exact output amount desired
            amount_in_maximum: Maximum input amount (with slippage)
            value: ETH value to send (for native token swaps)

        Returns:
            SwapTransaction with encoded calldata
        """
        calldata = self._encode_exact_output_single(
            token_in=token_in,
            token_out=token_out,
            fee=fee,
            recipient=recipient,
            deadline=deadline,
            amount_out=amount_out,
            amount_in_maximum=amount_in_maximum,
        )

        description = f"Swap max {amount_in_maximum} wei -> exact {amount_out} wei (fee={fee})"

        return SwapTransaction(
            to=self.router_address,
            value=value,
            data=calldata,
            gas_estimate=170000,  # Exact output typically costs more
            description=description,
        )

    # =========================================================================
    # Encoding Helpers
    # =========================================================================

    def _encode_exact_input_single(
        self,
        token_in: str,
        token_out: str,
        fee: int,
        recipient: str,
        deadline: int,
        amount_in: int,
        amount_out_minimum: int,
    ) -> str:
        """Encode exactInputSingle calldata.

        SwapRouter02 ExactInputSingleParams struct (7 params, no deadline):
        - address tokenIn
        - address tokenOut
        - uint24 fee
        - address recipient
        - uint256 amountIn
        - uint256 amountOutMinimum
        - uint160 sqrtPriceLimitX96

        Note: SwapRouter02 on Base/Arbitrum doesn't have deadline in the struct.
        Deadline is handled via multicall wrapper if needed.
        The deadline parameter is kept for API compatibility but not encoded.
        """
        return (
            EXACT_INPUT_SINGLE_SELECTOR
            + self._pad_address(token_in)
            + self._pad_address(token_out)
            + self._pad_uint(fee)
            + self._pad_address(recipient)
            + self._pad_uint(amount_in)
            + self._pad_uint(amount_out_minimum)
            + self._pad_uint(0)  # sqrtPriceLimitX96 = 0 (no limit)
        )

    def _encode_exact_output_single(
        self,
        token_in: str,
        token_out: str,
        fee: int,
        recipient: str,
        deadline: int,
        amount_out: int,
        amount_in_maximum: int,
    ) -> str:
        """Encode exactOutputSingle calldata.

        SwapRouter02 ExactOutputSingleParams struct (7 params, no deadline):
        - address tokenIn
        - address tokenOut
        - uint24 fee
        - address recipient
        - uint256 amountOut
        - uint256 amountInMaximum
        - uint160 sqrtPriceLimitX96

        Note: SwapRouter02 on Base/Arbitrum doesn't have deadline in the struct.
        Deadline is handled via multicall wrapper if needed.
        The deadline parameter is kept for API compatibility but not encoded.
        """
        return (
            EXACT_OUTPUT_SINGLE_SELECTOR
            + self._pad_address(token_in)
            + self._pad_address(token_out)
            + self._pad_uint(fee)
            + self._pad_address(recipient)
            + self._pad_uint(amount_out)
            + self._pad_uint(amount_in_maximum)
            + self._pad_uint(0)  # sqrtPriceLimitX96 = 0 (no limit)
        )

    def _encode_quote_exact_input_single(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        fee: int,
    ) -> str:
        """Encode quoteExactInputSingle calldata for QuoterV2.

        QuoteExactInputSingleParams struct:
        - address tokenIn
        - address tokenOut
        - uint256 amountIn
        - uint24 fee
        - uint160 sqrtPriceLimitX96
        """
        return (
            QUOTE_EXACT_INPUT_SINGLE_SELECTOR
            + self._pad_address(token_in)
            + self._pad_address(token_out)
            + self._pad_uint(amount_in)
            + self._pad_uint(fee)
            + self._pad_uint(0)  # sqrtPriceLimitX96 = 0
        )

    def _decode_quote_response(self, data: bytes) -> tuple[int, int, int, int]:
        """Decode QuoterV2 response.

        Returns: (amountOut, sqrtPriceX96After, initializedTicksCrossed, gasEstimate)
        """
        # Each value is 32 bytes
        if len(data) < 128:
            raise QuoteError("Invalid quote response length", "", "")

        amount_out = int.from_bytes(data[0:32], byteorder="big")
        sqrt_price_after = int.from_bytes(data[32:64], byteorder="big")
        ticks_crossed = int.from_bytes(data[64:96], byteorder="big")
        gas_estimate = int.from_bytes(data[96:128], byteorder="big")

        return amount_out, sqrt_price_after, ticks_crossed, gas_estimate

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return addr.lower().replace("0x", "").zfill(64)

    @staticmethod
    def _pad_uint(value: int) -> str:
        """Pad uint to 32 bytes."""
        return hex(value)[2:].zfill(64)

    # =========================================================================
    # Web3 Helper
    # =========================================================================

    async def _get_web3(self) -> Any:
        """Get or create Web3 instance."""
        if self._web3 is not None:
            return self._web3

        if self.rpc_url is None:
            raise UniswapV3SDKError("No RPC URL or Web3 instance provided")

        # Import here to avoid requiring web3 for offline operations
        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3
        except ImportError as e:
            raise UniswapV3SDKError("web3 package required for RPC operations") from e

        self._web3 = AsyncWeb3(AsyncHTTPProvider(self.rpc_url))
        return self._web3


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # SDK Class
    "UniswapV3SDK",
    # Data Classes
    "PoolInfo",
    "PoolState",
    "SwapQuote",
    "SwapTransaction",
    # Exceptions
    "UniswapV3SDKError",
    "InvalidFeeError",
    "InvalidTickError",
    "PoolNotFoundError",
    "QuoteError",
    # Tick Math Functions
    "tick_to_sqrt_price_x96",
    "sqrt_price_x96_to_tick",
    "tick_to_price",
    "price_to_tick",
    "sqrt_price_x96_to_price",
    "price_to_sqrt_price_x96",
    "get_nearest_tick",
    "get_min_tick",
    "get_max_tick",
    # Pool Functions
    "compute_pool_address",
    "sort_tokens",
    # Constants
    "Q96",
    "Q128",
    "MIN_TICK",
    "MAX_TICK",
    "TICK_SPACING",
    "FEE_TIERS",
    "FACTORY_ADDRESSES",
    "ROUTER_ADDRESSES",
    "QUOTER_ADDRESSES",
]
