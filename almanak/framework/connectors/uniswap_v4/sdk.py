"""Uniswap V4 SDK — pool key computation, quote interface, swap encoding.

Uniswap V4 uses a singleton PoolManager contract that manages all pools.
Pool keys include (currency0, currency1, fee, tickSpacing, hooks).
Native ETH is supported directly (address(0) for currency).

Swaps are routed through the canonical UniversalRouter which uses Permit2
for token transfers. The flow is:
  1. ERC-20 approve input token to Permit2
  2. Permit2.approve(universalRouter, token, amount, expiration)
  3. UniversalRouter.execute([V4_SWAP_EXACT_IN_SINGLE], [params], deadline)

Example:
    from almanak.framework.connectors.uniswap_v4.sdk import UniswapV4SDK

    sdk = UniswapV4SDK(chain="arbitrum")
    pool_key = sdk.compute_pool_key(token0, token1, fee=3000)
"""

import logging
import math
import time
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

# Canonical Permit2 address (CREATE2, same on all EVM chains)
PERMIT2_ADDRESS = "0x000000000022D473030F116dDEE9F6B43aC78BA3"

# --- Function selectors ---

# V4SwapRouter function selector (standalone router, NOT the UniversalRouter).
# swap(PoolKey,IPoolManager.SwapParams,uint256,uint256,bytes)
# NOTE: The v4_swap_router address in contracts.py needs on-chain verification.
# The UniversalRouter is the canonical Uniswap-deployed swap entry point for V4.
SWAP_SELECTOR = "0xf3cd914c"

# PositionManager function selectors (canonical V4 periphery)
# keccak256("modifyLiquidities(bytes,uint256)")[:4]
MODIFY_LIQUIDITIES_SELECTOR = "0xdd46508f"
# keccak256("modifyLiquiditiesWithoutUnlock(bytes,bytes[])")[:4]
MODIFY_LIQUIDITIES_WITHOUT_UNLOCK_SELECTOR = "0x4afe393c"

# UniversalRouter.execute(bytes commands, bytes[] inputs, uint256 deadline)
UNIVERSAL_ROUTER_EXECUTE_SELECTOR = "0x3593564c"

# Permit2.approve(address token, address spender, uint160 amount, uint48 expiration)
PERMIT2_APPROVE_SELECTOR = "0x87517c45"

# V4 command bytes for UniversalRouter
V4_SWAP_EXACT_IN_SINGLE = 0x06
V4_SWAP_EXACT_IN = 0x07
V4_SWAP_EXACT_OUT_SINGLE = 0x08
V4_SWAP_EXACT_OUT = 0x09

# Gas estimates
UNISWAP_V4_GAS_ESTIMATES = {
    "approve": 50_000,
    "permit2_approve": 55_000,
    "swap": 250_000,  # Higher than V3 due to PoolManager unlock callback overhead
    "swap_with_hooks": 400_000,
}

# PoolManager addresses per chain
POOL_MANAGER_ADDRESSES: dict[str, str] = {chain: addrs["pool_manager"] for chain, addrs in UNISWAP_V4.items()}

# UniversalRouter addresses (canonical V4 swap entry point)
ROUTER_ADDRESSES: dict[str, str] = {chain: addrs["universal_router"] for chain, addrs in UNISWAP_V4.items()}

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

    Routes swaps through the canonical UniversalRouter with Permit2 flow.

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
        self.router = self.addresses["universal_router"]
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
            spender: Address to approve (Permit2 for V4 flow).
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

    def build_permit2_approve_tx(
        self,
        token_address: str,
        spender: str,
        amount: int,
        expiration: int = 0,
    ) -> SwapTransaction:
        """Build a Permit2.approve transaction to grant the UniversalRouter allowance.

        Args:
            token_address: Token address to approve.
            spender: Address to grant allowance to (UniversalRouter).
            amount: Amount to approve (uint160 max = 2^160-1).
            expiration: Expiration timestamp (0 = default 30 days from now).

        Returns:
            SwapTransaction targeting the Permit2 contract.
        """
        if expiration == 0:
            expiration = int(time.time()) + 30 * 86400  # 30 days

        # Permit2.approve(address token, address spender, uint160 amount, uint48 expiration)
        # Clamp amount to uint160 max
        uint160_max = (1 << 160) - 1
        amount = min(amount, uint160_max)

        data = (
            PERMIT2_APPROVE_SELECTOR
            + _pad_address(token_address)
            + _pad_address(spender)
            + _pad_uint(amount)
            + _pad_uint(expiration)
        )

        return SwapTransaction(
            to=PERMIT2_ADDRESS,
            value=0,
            data=data,
            gas_estimate=UNISWAP_V4_GAS_ESTIMATES["permit2_approve"],
            description=f"Permit2 approve {spender[:10]}... for {token_address[:10]}...",
        )

    def build_swap_tx(
        self,
        quote: SwapQuote,
        recipient: str,
        slippage_bps: int = 50,
        deadline: int = 0,
    ) -> SwapTransaction:
        """Build a V4 swap transaction via the UniversalRouter.

        Encodes UniversalRouter.execute() with a V4_SWAP_EXACT_IN_SINGLE command.

        Args:
            quote: Swap quote with amounts.
            recipient: Address to receive output tokens.
            slippage_bps: Slippage tolerance in basis points.
            deadline: Transaction deadline (0 = 30 minutes from now).

        Returns:
            SwapTransaction with encoded calldata.
        """
        amount_out_minimum = quote.amount_out * (10000 - slippage_bps) // 10000
        is_native_in = quote.token_in.lower() == NATIVE_CURRENCY

        if deadline == 0:
            deadline = int(time.time()) + 1800  # 30 minutes

        # Encode the ExactInputSingleParams struct for V4_SWAP_EXACT_IN_SINGLE
        params_encoded = self._encode_exact_input_single_params(
            quote=quote,
            amount_out_minimum=amount_out_minimum,
        )

        # Encode UniversalRouter.execute(bytes commands, bytes[] inputs, uint256 deadline)
        calldata = _encode_execute(
            commands=bytes([V4_SWAP_EXACT_IN_SINGLE]),
            inputs=[params_encoded],
            deadline=deadline,
        )

        return SwapTransaction(
            to=self.router,
            value=quote.amount_in if is_native_in else 0,
            data=calldata,
            gas_estimate=UNISWAP_V4_GAS_ESTIMATES["swap"],
            description=(f"Uniswap V4 swap {quote.token_in[:10]}... -> {quote.token_out[:10]}..."),
        )

    def _encode_exact_input_single_params(
        self,
        quote: SwapQuote,
        amount_out_minimum: int,
    ) -> str:
        """Encode ExactInputSingleParams struct for V4_SWAP_EXACT_IN_SINGLE.

        Struct layout (solidity):
            struct ExactInputSingleParams {
                PoolKey poolKey;        // (currency0, currency1, fee, tickSpacing, hooks)
                bool zeroForOne;
                uint128 amountIn;
                uint128 amountOutMinimum;
                uint160 sqrtPriceLimitX96;
                bytes hookData;         // dynamic
            }

        Returns:
            Hex string (no 0x prefix) of ABI-encoded params.
        """
        pool_key = self.compute_pool_key(quote.token_in, quote.token_out, quote.fee_tier)
        zero_for_one = quote.token_in.lower() == pool_key.currency0

        sqrt_price_limit = (MIN_SQRT_PRICE + 1) if zero_for_one else (MAX_SQRT_PRICE - 1)

        # Head: 9 static fields + 1 offset for hookData = 10 words
        # hookData offset from start of struct: 10 * 32 = 320 = 0x140
        head = (
            _pad_address(pool_key.currency0)
            + _pad_address(pool_key.currency1)
            + _pad_uint24(pool_key.fee)
            + _pad_int24(pool_key.tick_spacing)
            + _pad_address(pool_key.hooks)
            + _pad_bool(zero_for_one)
            + _pad_uint(quote.amount_in)
            + _pad_uint(amount_out_minimum)
            + _pad_uint(sqrt_price_limit)
            + _pad_uint(0x140)  # offset to hookData
        )

        # Tail: hookData = empty bytes
        tail = _pad_uint(0)  # hookData length = 0

        return head + tail

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


def _encode_execute(commands: bytes, inputs: list[str], deadline: int) -> str:
    """Encode UniversalRouter.execute(bytes commands, bytes[] inputs, uint256 deadline).

    Args:
        commands: Command bytes (each byte is a command ID).
        inputs: List of hex-encoded input data (no 0x prefix) for each command.
        deadline: Transaction deadline timestamp.

    Returns:
        Full calldata hex string with 0x prefix.
    """
    # Head: 3 slots (offset_commands, offset_inputs, deadline)
    # commands starts at 3 * 32 = 96 = 0x60

    # Commands section: length (32 bytes) + data (padded to 32 bytes)
    commands_hex = commands.hex()
    commands_padded = commands_hex.ljust(64, "0")  # right-pad to 32 bytes
    commands_section = _pad_uint(len(commands)) + commands_padded

    # Offset to inputs = 0x60 + len(commands_section in bytes)
    # commands_section is 2 words = 64 bytes
    offset_inputs = 0x60 + 64  # = 0xa0

    # Inputs section: array length + offsets + elements
    num_inputs = len(inputs)
    # After array length, there are num_inputs offset words
    # First element data starts at num_inputs * 32 bytes after offsets start
    offsets_area_size = num_inputs * 32
    element_data = ""
    offsets = []
    current_offset = offsets_area_size

    for inp in inputs:
        offsets.append(current_offset)
        # Each element: length (32 bytes) + data (padded to 32-byte boundary)
        byte_len = len(inp) // 2
        padded_data = inp
        # Pad data to 32-byte boundary
        if len(padded_data) % 64 != 0:
            padded_data = padded_data + "0" * (64 - len(padded_data) % 64)
        element_data += _pad_uint(byte_len) + padded_data
        current_offset += 32 + len(padded_data) // 2  # length word + data bytes

    inputs_section = _pad_uint(num_inputs)
    for off in offsets:
        inputs_section += _pad_uint(off)
    inputs_section += element_data

    # Assemble
    head = _pad_uint(0x60) + _pad_uint(offset_inputs) + _pad_uint(deadline)

    return "0x" + UNIVERSAL_ROUTER_EXECUTE_SELECTOR[2:] + head + commands_section + inputs_section


__all__ = [
    "FEE_TIERS",
    "MODIFY_LIQUIDITIES_SELECTOR",
    "MODIFY_LIQUIDITIES_WITHOUT_UNLOCK_SELECTOR",
    "NATIVE_CURRENCY",
    "PERMIT2_ADDRESS",
    "PERMIT2_APPROVE_SELECTOR",
    "POOL_MANAGER_ADDRESSES",
    "PoolKey",
    "QUOTER_ADDRESSES",
    "ROUTER_ADDRESSES",
    "SwapQuote",
    "SwapTransaction",
    "TICK_SPACING",
    "UNISWAP_V4_GAS_ESTIMATES",
    "UNIVERSAL_ROUTER_EXECUTE_SELECTOR",
    "UniswapV4SDK",
    "V4_SWAP_EXACT_IN",
    "V4_SWAP_EXACT_IN_SINGLE",
    "V4_SWAP_EXACT_OUT",
    "V4_SWAP_EXACT_OUT_SINGLE",
]
