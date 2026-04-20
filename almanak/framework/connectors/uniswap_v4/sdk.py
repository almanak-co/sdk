"""Uniswap V4 SDK — pool key computation, quote interface, swap encoding.

Uniswap V4 uses a singleton PoolManager contract that manages all pools.
Pool keys include (currency0, currency1, fee, tickSpacing, hooks).
Native ETH is supported directly (address(0) for currency).

Example:
    from almanak.framework.connectors.uniswap_v4.sdk import UniswapV4SDK

    sdk = UniswapV4SDK(chain="arbitrum")
    pool_key = sdk.compute_pool_key(token0, token1, fee=3000)
"""

import logging
import math
from dataclasses import dataclass
from decimal import Decimal

from almanak.core.contracts import UNISWAP_V4

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

Q96 = 2**96
Q128 = 2**128

# Tick bounds (same as V3)
MIN_TICK = -887272
MAX_TICK = 887272

# sqrtPriceX96 bounds for swap price limits (from PoolManager)
# zeroForOne swaps require sqrtPriceLimitX96 >= MIN_SQRT_PRICE + 1
# !zeroForOne swaps require sqrtPriceLimitX96 <= MAX_SQRT_PRICE - 1
MIN_SQRT_PRICE = 4295128739
MAX_SQRT_PRICE = 1461446703485210103287273052203988822378723970342

# Default tick spacing per fee tier in V4
# V4 allows custom tick spacing, but these are common defaults
TICK_SPACING: dict[int, int] = {
    100: 1,  # 0.01%
    500: 10,  # 0.05%
    3000: 60,  # 0.3%
    10000: 200,  # 1%
}

FEE_TIERS: list[int] = [100, 500, 3000, 10000]

# Zero address represents native ETH in V4
NATIVE_CURRENCY = "0x0000000000000000000000000000000000000000"

# V4SwapRouter function selectors
# swap(PoolKey,IPoolManager.SwapParams,uint256,uint256,bytes)
SWAP_SELECTOR = "0xf3cd914c"

# Gas estimates
UNISWAP_V4_GAS_ESTIMATES = {
    "approve": 50_000,
    "swap": 200_000,
    "swap_with_hooks": 350_000,
}

# PoolManager addresses per chain
POOL_MANAGER_ADDRESSES: dict[str, str] = {chain: addrs["pool_manager"] for chain, addrs in UNISWAP_V4.items()}

ROUTER_ADDRESSES: dict[str, str] = {chain: addrs["v4_swap_router"] for chain, addrs in UNISWAP_V4.items()}

QUOTER_ADDRESSES: dict[str, str] = {chain: addrs["quoter"] for chain, addrs in UNISWAP_V4.items()}


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class PoolKey:
    """Uniswap V4 pool key — uniquely identifies a pool.

    In V4, pools are identified by (currency0, currency1, fee, tickSpacing, hooks).
    currency0 must be numerically less than currency1 (sorted order).
    """

    currency0: str
    currency1: str
    fee: int
    tick_spacing: int
    hooks: str = NATIVE_CURRENCY  # Default: no hooks

    def __post_init__(self) -> None:
        self.currency0 = self.currency0.lower()
        self.currency1 = self.currency1.lower()
        self.hooks = self.hooks.lower()
        # Ensure sorted order
        if int(self.currency0, 16) > int(self.currency1, 16):
            self.currency0, self.currency1 = self.currency1, self.currency0


@dataclass
class SwapQuote:
    """Quote data for a V4 swap."""

    amount_in: int
    amount_out: int
    fee_tier: int
    token_in: str
    token_out: str
    sqrt_price_x96_after: int | None = None
    effective_price: Decimal | None = None
    gas_estimate: int = UNISWAP_V4_GAS_ESTIMATES["swap"]


@dataclass
class SwapTransaction:
    """Encoded swap transaction data."""

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str


# =============================================================================
# UniswapV4SDK
# =============================================================================


class UniswapV4SDK:
    """Uniswap V4 SDK for pool operations and swap encoding.

    Args:
        chain: Chain name (e.g. "arbitrum", "ethereum").
        rpc_url: Optional RPC URL for on-chain queries.
    """

    def __init__(self, chain: str, rpc_url: str | None = None) -> None:
        self.chain = chain.lower()
        self.rpc_url = rpc_url

        if self.chain not in UNISWAP_V4:
            raise ValueError(
                f"Uniswap V4 not supported on chain '{self.chain}'. Supported: {', '.join(UNISWAP_V4.keys())}"
            )

        self.addresses = UNISWAP_V4[self.chain]
        self.pool_manager = self.addresses["pool_manager"]
        self.router = self.addresses["v4_swap_router"]
        self.quoter = self.addresses["quoter"]

    def compute_pool_key(
        self,
        token0: str,
        token1: str,
        fee: int = 3000,
        tick_spacing: int | None = None,
        hooks: str = NATIVE_CURRENCY,
    ) -> PoolKey:
        """Compute a V4 pool key for a token pair.

        Args:
            token0: First token address.
            token1: Second token address.
            fee: Fee tier in hundredths of a bip (e.g., 3000 = 0.3%).
            tick_spacing: Custom tick spacing. Defaults to standard for fee tier.
            hooks: Hooks contract address. Default: no hooks (zero address).

        Returns:
            PoolKey with sorted currency addresses.
        """
        if tick_spacing is None:
            tick_spacing = TICK_SPACING.get(fee, 60)

        return PoolKey(
            currency0=token0,
            currency1=token1,
            fee=fee,
            tick_spacing=tick_spacing,
            hooks=hooks,
        )

    def get_quote_local(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        fee_tier: int = 3000,
        token_in_decimals: int = 18,
        token_out_decimals: int = 18,
        price_ratio: Decimal | None = None,
    ) -> SwapQuote:
        """Compute an offline swap quote estimate based on fee tier.

        This is a best-effort estimate without on-chain data. For accurate
        quotes, use the V4 Quoter contract via RPC.

        Args:
            token_in: Input token address.
            token_out: Output token address.
            amount_in: Input amount in smallest units.
            fee_tier: Fee tier (e.g. 3000 = 0.3%).
            token_in_decimals: Decimals for input token.
            token_out_decimals: Decimals for output token.
            price_ratio: Optional price ratio (token_in/token_out).

        Returns:
            SwapQuote with estimated output.
        """
        fee_fraction = Decimal(fee_tier) / Decimal(1_000_000)

        if price_ratio is not None:
            decimal_adjusted_amount = Decimal(amount_in) / Decimal(10**token_in_decimals)
            output_amount = decimal_adjusted_amount * price_ratio * (1 - fee_fraction)
            amount_out = int(output_amount * Decimal(10**token_out_decimals))
        else:
            # Same-decimal estimate (e.g., stablecoin pairs)
            amount_out = int(Decimal(amount_in) * (1 - fee_fraction))

        effective_price = None
        if amount_in > 0 and amount_out > 0:
            effective_price = (Decimal(amount_out) / Decimal(10**token_out_decimals)) / (
                Decimal(amount_in) / Decimal(10**token_in_decimals)
            )

        return SwapQuote(
            amount_in=amount_in,
            amount_out=amount_out,
            fee_tier=fee_tier,
            token_in=token_in,
            token_out=token_out,
            effective_price=effective_price,
        )

    def build_approve_tx(
        self,
        token_address: str,
        spender: str,
        amount: int,
    ) -> SwapTransaction:
        """Build an ERC-20 approve transaction.

        Args:
            token_address: Token contract address.
            spender: Address to approve (typically the router).
            amount: Amount to approve.

        Returns:
            SwapTransaction with encoded approve calldata.
        """
        # ERC-20 approve(address,uint256) selector: 0x095ea7b3
        spender_padded = _pad_address(spender)
        amount_padded = _pad_uint(amount)
        data = f"0x095ea7b3{spender_padded}{amount_padded}"

        return SwapTransaction(
            to=token_address,
            value=0,
            data=data,
            gas_estimate=UNISWAP_V4_GAS_ESTIMATES["approve"],
            description=f"Approve {spender[:10]}... to spend {amount} tokens",
        )

    def build_swap_tx(
        self,
        quote: SwapQuote,
        recipient: str,
        slippage_bps: int = 50,
        deadline: int = 0,
    ) -> SwapTransaction:
        """Build a V4 swap transaction via the V4SwapRouter.

        Encodes an exactInputSingle swap through the V4 swap router.

        Args:
            quote: Swap quote with amounts.
            recipient: Address to receive output tokens.
            slippage_bps: Slippage tolerance in basis points.
            deadline: Transaction deadline (0 = no deadline).

        Returns:
            SwapTransaction with encoded calldata.
        """
        amount_out_minimum = int(quote.amount_out * (10000 - slippage_bps) / 10000)
        is_native_in = quote.token_in.lower() == NATIVE_CURRENCY

        # Encode exactInputSingle parameters
        # The V4SwapRouter uses a different encoding than V3:
        # exactInputSingle(ExactInputSingleParams)
        # struct ExactInputSingleParams {
        #   PoolKey poolKey;
        #   bool zeroForOne;
        #   uint128 amountIn;
        #   uint128 amountOutMinimum;
        #   uint160 sqrtPriceLimitX96;
        #   bytes hookData;
        # }

        # Determine swap direction
        pool_key = self.compute_pool_key(quote.token_in, quote.token_out, quote.fee_tier)
        zero_for_one = quote.token_in.lower() == pool_key.currency0

        # Encode PoolKey struct
        pool_key_encoded = (
            _pad_address(pool_key.currency0)
            + _pad_address(pool_key.currency1)
            + _pad_uint24(pool_key.fee)
            + _pad_int24(pool_key.tick_spacing)
            + _pad_address(pool_key.hooks)
        )

        # Encode swap params
        zero_for_one_encoded = _pad_bool(zero_for_one)
        amount_in_encoded = _pad_uint(quote.amount_in)
        amount_out_min_encoded = _pad_uint(amount_out_minimum)

        # sqrtPriceLimitX96: must be within bounds per PoolManager requirements
        # zeroForOne: MIN_SQRT_PRICE + 1, !zeroForOne: MAX_SQRT_PRICE - 1
        sqrt_price_limit_value = (MIN_SQRT_PRICE + 1) if zero_for_one else (MAX_SQRT_PRICE - 1)
        sqrt_price_limit = _pad_uint(sqrt_price_limit_value)

        # hookData: empty bytes (offset + length + no data)
        hook_data_offset = _pad_uint(7 * 32)  # offset to hookData
        hook_data_length = _pad_uint(0)  # empty bytes

        calldata = (
            SWAP_SELECTOR
            + pool_key_encoded
            + zero_for_one_encoded
            + amount_in_encoded
            + amount_out_min_encoded
            + sqrt_price_limit
            + hook_data_offset
            + hook_data_length
        )

        return SwapTransaction(
            to=self.router,
            value=quote.amount_in if is_native_in else 0,
            data=calldata,
            gas_estimate=UNISWAP_V4_GAS_ESTIMATES["swap"],
            description=(f"Uniswap V4 swap {quote.token_in[:10]}... -> {quote.token_out[:10]}..."),
        )

    @staticmethod
    def tick_to_price(tick: int, decimals0: int = 18, decimals1: int = 18) -> Decimal:
        """Convert tick to human-readable price.

        Uses Decimal arithmetic to avoid float overflow at extreme ticks.
        """
        raw_price = Decimal("1.0001") ** tick
        decimal_adjustment = Decimal(10 ** (decimals0 - decimals1))
        return raw_price * decimal_adjustment

    @staticmethod
    def price_to_tick(price: Decimal, decimals0: int = 18, decimals1: int = 18) -> int:
        """Convert human-readable price to tick.

        Uses math.log for the inverse computation. Safe for typical price ranges.
        """
        decimal_adjustment = Decimal(10 ** (decimals0 - decimals1))
        adjusted_price = price / decimal_adjustment
        if adjusted_price <= 0:
            raise ValueError("Price must be positive")
        return int(math.log(float(adjusted_price)) / math.log(1.0001))


# =============================================================================
# ABI Encoding Helpers
# =============================================================================


def _pad_address(addr: str) -> str:
    """Pad an address to 32 bytes."""
    clean = addr.lower().replace("0x", "")
    return clean.zfill(64)


def _pad_uint(value: int) -> str:
    """Pad a uint256 to 32 bytes."""
    return hex(value)[2:].zfill(64)


def _pad_uint24(value: int) -> str:
    """Pad a uint24 to 32 bytes."""
    return hex(value)[2:].zfill(64)


def _pad_int24(value: int) -> str:
    """Pad an int24 to 32 bytes (two's complement for negative)."""
    if value < 0:
        value = (1 << 256) + value
    return hex(value)[2:].zfill(64)


def _pad_bool(value: bool) -> str:
    """Pad a bool to 32 bytes."""
    return "0" * 63 + ("1" if value else "0")


__all__ = [
    "FEE_TIERS",
    "NATIVE_CURRENCY",
    "POOL_MANAGER_ADDRESSES",
    "PoolKey",
    "QUOTER_ADDRESSES",
    "ROUTER_ADDRESSES",
    "SwapQuote",
    "SwapTransaction",
    "TICK_SPACING",
    "UNISWAP_V4_GAS_ESTIMATES",
    "UniswapV4SDK",
]
