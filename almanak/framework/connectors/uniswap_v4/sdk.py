"""Uniswap V4 SDK — pool key computation, quote interface, swap & LP encoding.

Uniswap V4 uses a singleton PoolManager contract that manages all pools.
Pool keys include (currency0, currency1, fee, tickSpacing, hooks).
Native ETH is supported directly (address(0) for currency).

Swaps are routed through the canonical UniversalRouter which uses Permit2
for token transfers. LP operations use the PositionManager with
flash accounting (modifyLiquidities + Actions-encoded bytes).

Swap flow:
  1. ERC-20 approve input token to Permit2
  2. Permit2.approve(universalRouter, token, amount, expiration)
  3. UniversalRouter.execute([V4_SWAP_EXACT_IN_SINGLE], [params], deadline)

LP flow (mint):
  1. ERC-20 approve token0 + token1 to Permit2
  2. Permit2.approve(positionManager, token0/token1, amount, expiration)
  3. PositionManager.modifyLiquidities([MINT_POSITION, SETTLE_PAIR], deadline)

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

# --- PositionManager Action bytes ---
# V4 PositionManager uses modifyLiquidities(bytes unlockData, uint256 deadline)
# where unlockData = abi.encode(bytes actions, bytes[] params)
PM_INCREASE_LIQUIDITY = 0x00
PM_DECREASE_LIQUIDITY = 0x01
PM_MINT_POSITION = 0x02
PM_BURN_POSITION = 0x03
PM_CLOSE_CURRENCY = 0x04
PM_CLEAR_OR_TAKE = 0x05
PM_SWEEP = 0x06
PM_SETTLE = 0x09
PM_SETTLE_ALL = 0x0A
PM_SETTLE_PAIR = 0x0B
PM_TAKE = 0x0C
PM_TAKE_ALL = 0x0D
PM_TAKE_PAIR = 0x0E
PM_TAKE_PORTION = 0x0F

# Gas estimates
UNISWAP_V4_GAS_ESTIMATES = {
    "approve": 50_000,
    "permit2_approve": 55_000,
    "swap": 250_000,  # Higher than V3 due to PoolManager unlock callback overhead
    "swap_with_hooks": 400_000,
    "lp_mint": 450_000,  # Mint new LP position via PositionManager
    "lp_decrease": 300_000,  # Decrease liquidity
    "lp_burn": 200_000,  # Burn empty position NFT
    "lp_collect_fees": 250_000,  # Collect fees only (decrease with 0 liquidity + take)
}

# PoolManager addresses per chain
POOL_MANAGER_ADDRESSES: dict[str, str] = {chain: addrs["pool_manager"] for chain, addrs in UNISWAP_V4.items()}

# UniversalRouter addresses (canonical V4 swap entry point)
ROUTER_ADDRESSES: dict[str, str] = {chain: addrs["universal_router"] for chain, addrs in UNISWAP_V4.items()}

QUOTER_ADDRESSES: dict[str, str] = {chain: addrs["quoter"] for chain, addrs in UNISWAP_V4.items()}

# PositionManager addresses per chain
POSITION_MANAGER_ADDRESSES: dict[str, str] = {chain: addrs["position_manager"] for chain, addrs in UNISWAP_V4.items()}


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
    """Encoded swap or LP transaction data."""

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str


@dataclass
class LPMintParams:
    """Parameters for minting a new V4 LP position."""

    pool_key: PoolKey
    tick_lower: int
    tick_upper: int
    liquidity: int
    amount0_max: int
    amount1_max: int
    owner: str
    hook_data: bytes = b""


@dataclass
class LPDecreaseParams:
    """Parameters for decreasing liquidity from a V4 LP position."""

    token_id: int
    liquidity: int
    amount0_min: int = 0
    amount1_min: int = 0
    hook_data: bytes = b""


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

    # =========================================================================
    # LP Methods — PositionManager encoding
    # =========================================================================

    def build_mint_position_tx(
        self,
        params: LPMintParams,
        deadline: int = 0,
    ) -> SwapTransaction:
        """Build a PositionManager.modifyLiquidities TX to mint a new LP position.

        Encodes actions [MINT_POSITION, SETTLE_PAIR] to:
        1. Create the position NFT with the specified liquidity
        2. Settle (pay) both currencies via Permit2

        Args:
            params: LPMintParams with pool key, tick range, liquidity, etc.
            deadline: TX deadline (0 = 30 minutes from now).

        Returns:
            SwapTransaction targeting PositionManager.
        """
        if deadline == 0:
            deadline = int(time.time()) + 1800

        position_manager = self.addresses["position_manager"]

        # Encode MINT_POSITION params
        mint_params = self._encode_mint_position_params(params)

        # Encode SETTLE_PAIR params: abi.encode(currency0, currency1)
        settle_params = _pad_address(params.pool_key.currency0) + _pad_address(params.pool_key.currency1)

        # Build modifyLiquidities calldata
        actions = bytes([PM_MINT_POSITION, PM_SETTLE_PAIR])
        calldata = _encode_modify_liquidities(actions, [mint_params, settle_params], deadline)

        # Native ETH: when one currency is address(0), send native value
        native_value = 0
        if params.pool_key.currency0 == NATIVE_CURRENCY:
            native_value = params.amount0_max
        elif params.pool_key.currency1 == NATIVE_CURRENCY:
            native_value = params.amount1_max

        return SwapTransaction(
            to=position_manager,
            value=native_value,
            data=calldata,
            gas_estimate=UNISWAP_V4_GAS_ESTIMATES["lp_mint"],
            description="Uniswap V4 mint LP position",
        )

    def build_decrease_liquidity_tx(
        self,
        params: LPDecreaseParams,
        currency0: str,
        currency1: str,
        recipient: str,
        deadline: int = 0,
        burn: bool = True,
    ) -> SwapTransaction:
        """Build a PositionManager.modifyLiquidities TX to decrease/close an LP position.

        Encodes actions [DECREASE_LIQUIDITY, TAKE_PAIR] and optionally [BURN_POSITION].

        Args:
            params: LPDecreaseParams with token ID, liquidity, minimums.
            currency0: Token0 address (sorted).
            currency1: Token1 address (sorted).
            recipient: Address to receive withdrawn tokens.
            deadline: TX deadline (0 = 30 minutes from now).
            burn: Whether to burn the NFT after withdrawal.

        Returns:
            SwapTransaction targeting PositionManager.
        """
        if deadline == 0:
            deadline = int(time.time()) + 1800

        position_manager = self.addresses["position_manager"]

        # Encode DECREASE_LIQUIDITY params
        decrease_params = self._encode_decrease_liquidity_params(params)

        # Encode TAKE_PAIR params: abi.encode(currency0, currency1, address recipient)
        take_params = _pad_address(currency0) + _pad_address(currency1) + _pad_address(recipient)

        actions_list = [PM_DECREASE_LIQUIDITY, PM_TAKE_PAIR]
        params_list = [decrease_params, take_params]

        if burn:
            # Encode BURN_POSITION params: abi.encode(uint256 tokenId, address owner, bytes hookData)
            burn_params = self._encode_burn_position_params(params.token_id, recipient, params.hook_data)
            actions_list.append(PM_BURN_POSITION)
            params_list.append(burn_params)

        actions = bytes(actions_list)
        calldata = _encode_modify_liquidities(actions, params_list, deadline)

        return SwapTransaction(
            to=position_manager,
            value=0,
            data=calldata,
            gas_estimate=UNISWAP_V4_GAS_ESTIMATES["lp_decrease"],
            description=f"Uniswap V4 {'close' if burn else 'decrease'} LP position #{params.token_id}",
        )

    def build_collect_fees_tx(
        self,
        token_id: int,
        currency0: str,
        currency1: str,
        recipient: str,
        hook_data: bytes = b"",
        deadline: int = 0,
    ) -> SwapTransaction:
        """Build a PositionManager.modifyLiquidities TX to collect fees only.

        Decreases liquidity by 0 (triggers fee accrual update) then takes pair.

        Args:
            token_id: Position NFT token ID.
            currency0: Token0 address (sorted).
            currency1: Token1 address (sorted).
            recipient: Address to receive fees.
            hook_data: Optional hook data for hooked pools.
            deadline: TX deadline (0 = 30 minutes from now).

        Returns:
            SwapTransaction targeting PositionManager.
        """
        if deadline == 0:
            deadline = int(time.time()) + 1800

        position_manager = self.addresses["position_manager"]

        # Decrease by 0 to trigger fee update
        decrease_params = self._encode_decrease_liquidity_params(
            LPDecreaseParams(token_id=token_id, liquidity=0, hook_data=hook_data)
        )

        # Take the accrued fees
        take_params = _pad_address(currency0) + _pad_address(currency1) + _pad_address(recipient)

        actions = bytes([PM_DECREASE_LIQUIDITY, PM_TAKE_PAIR])
        calldata = _encode_modify_liquidities(actions, [decrease_params, take_params], deadline)

        return SwapTransaction(
            to=position_manager,
            value=0,
            data=calldata,
            gas_estimate=UNISWAP_V4_GAS_ESTIMATES["lp_collect_fees"],
            description=f"Uniswap V4 collect fees for position #{token_id}",
        )

    # =========================================================================
    # LP Encoding Helpers
    # =========================================================================

    def _encode_mint_position_params(self, params: LPMintParams) -> str:
        """Encode MINT_POSITION action params.

        Layout:
            PoolKey(currency0, currency1, fee, tickSpacing, hooks), // 5 fields
            int24 tickLower,
            int24 tickUpper,
            uint256 liquidity,
            uint128 amount0Max,
            uint128 amount1Max,
            address owner,
            bytes hookData  // dynamic

        Returns:
            Hex string (no 0x prefix).
        """
        pk = params.pool_key
        # 5 PoolKey fields + 6 more static + 1 offset for hookData = 12 words
        # hookData offset = 12 * 32 = 384 = 0x180
        hook_data_offset = 12 * 32

        head = (
            _pad_address(pk.currency0)
            + _pad_address(pk.currency1)
            + _pad_uint24(pk.fee)
            + _pad_int24(pk.tick_spacing)
            + _pad_address(pk.hooks)
            + _pad_int24(params.tick_lower)
            + _pad_int24(params.tick_upper)
            + _pad_uint(params.liquidity)
            + _pad_uint(params.amount0_max)
            + _pad_uint(params.amount1_max)
            + _pad_address(params.owner)
            + _pad_uint(hook_data_offset)
        )

        # hookData tail
        tail = _encode_bytes(params.hook_data)

        return head + tail

    def _encode_decrease_liquidity_params(self, params: LPDecreaseParams) -> str:
        """Encode DECREASE_LIQUIDITY action params.

        Layout:
            uint256 tokenId,
            uint256 liquidity,
            uint128 amount0Min,
            uint128 amount1Min,
            bytes hookData  // dynamic

        Returns:
            Hex string (no 0x prefix).
        """
        # 4 static fields + 1 offset = 5 words
        hook_data_offset = 5 * 32

        head = (
            _pad_uint(params.token_id)
            + _pad_uint(params.liquidity)
            + _pad_uint(params.amount0_min)
            + _pad_uint(params.amount1_min)
            + _pad_uint(hook_data_offset)
        )

        tail = _encode_bytes(params.hook_data)

        return head + tail

    @staticmethod
    def _encode_burn_position_params(token_id: int, owner: str, hook_data: bytes = b"") -> str:
        """Encode BURN_POSITION action params.

        Layout:
            uint256 tokenId,
            address owner,
            bytes hookData  // dynamic
        """
        hook_data_offset = 3 * 32

        head = _pad_uint(token_id) + _pad_address(owner) + _pad_uint(hook_data_offset)
        tail = _encode_bytes(hook_data)

        return head + tail

    @staticmethod
    def compute_liquidity_from_amounts(
        sqrt_price_x96: int,
        tick_lower: int,
        tick_upper: int,
        amount0: int,
        amount1: int,
    ) -> int:
        """Compute liquidity from token amounts and price range.

        Uses the same math as Uniswap V3/V4:
        - If current price is below range: liquidity from amount0 only
        - If current price is above range: liquidity from amount1 only
        - If current price is in range: min(liquidity from amount0, liquidity from amount1)

        Args:
            sqrt_price_x96: Current pool sqrtPriceX96 (or estimate).
            tick_lower: Lower tick boundary.
            tick_upper: Upper tick boundary.
            amount0: Desired amount of token0 (in smallest units).
            amount1: Desired amount of token1 (in smallest units).

        Returns:
            Estimated liquidity value.
        """
        if tick_lower == tick_upper:
            raise ValueError(
                f"tick_lower ({tick_lower}) must not equal tick_upper ({tick_upper}). "
                "This would create a zero-width range causing division by zero."
            )

        sqrt_ratio_a = _tick_to_sqrt_ratio_x96(tick_lower)
        sqrt_ratio_b = _tick_to_sqrt_ratio_x96(tick_upper)

        if sqrt_ratio_a > sqrt_ratio_b:
            sqrt_ratio_a, sqrt_ratio_b = sqrt_ratio_b, sqrt_ratio_a

        if sqrt_price_x96 <= sqrt_ratio_a:
            # Current price below range — all token0
            if amount0 == 0:
                return 0
            return _get_liquidity_for_amount0(sqrt_ratio_a, sqrt_ratio_b, amount0)
        elif sqrt_price_x96 >= sqrt_ratio_b:
            # Current price above range — all token1
            if amount1 == 0:
                return 0
            return _get_liquidity_for_amount1(sqrt_ratio_a, sqrt_ratio_b, amount1)
        else:
            # Current price in range — use min
            liq0 = _get_liquidity_for_amount0(sqrt_price_x96, sqrt_ratio_b, amount0) if amount0 > 0 else 0
            liq1 = _get_liquidity_for_amount1(sqrt_ratio_a, sqrt_price_x96, amount1) if amount1 > 0 else 0
            if liq0 == 0:
                return liq1
            if liq1 == 0:
                return liq0
            return min(liq0, liq1)

    @staticmethod
    def estimate_sqrt_price_x96(price: Decimal, decimals0: int = 18, decimals1: int = 18) -> int:
        """Estimate sqrtPriceX96 from a human-readable price (token1 per token0).

        Args:
            price: Price of token0 in terms of token1.
            decimals0: Decimals of token0.
            decimals1: Decimals of token1.

        Returns:
            Estimated sqrtPriceX96 value.
        """
        import decimal

        if price <= 0:
            raise ValueError("Price must be positive")
        with decimal.localcontext() as ctx:
            ctx.prec = 78  # Enough precision for full uint256 range
            decimal_adjustment = Decimal(10 ** (decimals1 - decimals0))
            adjusted = price * decimal_adjustment
            sqrt_price = adjusted.sqrt() * Decimal(2**96)
            return int(sqrt_price)

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


def _encode_bytes(data: bytes) -> str:
    """Encode a dynamic `bytes` value (length + padded data)."""
    length = len(data)
    hex_data = data.hex() if data else ""
    # Pad to 32-byte boundary
    if len(hex_data) % 64 != 0:
        hex_data = hex_data + "0" * (64 - len(hex_data) % 64)
    if not hex_data:
        hex_data = ""
    return _pad_uint(length) + hex_data


def _encode_modify_liquidities(actions: bytes, params: list[str], deadline: int) -> str:
    """Encode PositionManager.modifyLiquidities(bytes unlockData, uint256 deadline).

    The unlockData is abi.encode(bytes actions, bytes[] params).

    Args:
        actions: Packed action bytes (each byte is an action type).
        params: List of hex-encoded param blobs (no 0x prefix) for each action.
        deadline: Transaction deadline timestamp.

    Returns:
        Full calldata hex string with 0x prefix.
    """
    # unlockData = abi.encode(bytes actions, bytes[] params)
    # This is two dynamic types, so:
    # [offset_actions, offset_params, actions_data, params_data]

    # Actions section: at offset 0x40 (2 words for the two offsets)
    actions_hex = actions.hex()
    actions_padded = actions_hex
    if len(actions_padded) % 64 != 0:
        actions_padded = actions_padded + "0" * (64 - len(actions_padded) % 64)
    actions_section = _pad_uint(len(actions)) + actions_padded

    # Params section: bytes[] array
    # Array layout: [length, offset0, offset1, ..., element0, element1, ...]
    num_params = len(params)
    offsets_area = num_params * 32  # offset words
    element_data = ""
    offsets = []
    current_offset = offsets_area

    for p in params:
        offsets.append(current_offset)
        byte_len = len(p) // 2
        padded = p
        if len(padded) % 64 != 0:
            padded = padded + "0" * (64 - len(padded) % 64)
        element_data += _pad_uint(byte_len) + padded
        current_offset += 32 + len(padded) // 2  # length word + data

    params_section = _pad_uint(num_params)
    for off in offsets:
        params_section += _pad_uint(off)
    params_section += element_data

    # Offset to actions: 0x40 (starts after 2 offset words)
    offset_actions = 0x40
    # Offset to params: 0x40 + len(actions_section) in bytes
    offset_params = offset_actions + len(actions_section) // 2

    unlock_data_hex = _pad_uint(offset_actions) + _pad_uint(offset_params) + actions_section + params_section

    # Now encode the outer call: modifyLiquidities(bytes unlockData, uint256 deadline)
    # unlockData is dynamic bytes, so: [offset_unlockData, deadline, unlockData_section]
    unlock_data_bytes_len = len(unlock_data_hex) // 2
    unlock_data_padded = unlock_data_hex
    if len(unlock_data_padded) % 64 != 0:
        unlock_data_padded = unlock_data_padded + "0" * (64 - len(unlock_data_padded) % 64)

    outer_head = _pad_uint(0x40) + _pad_uint(deadline)
    outer_data = _pad_uint(unlock_data_bytes_len) + unlock_data_padded

    return "0x" + MODIFY_LIQUIDITIES_SELECTOR[2:] + outer_head + outer_data


def _tick_to_sqrt_ratio_x96(tick: int) -> int:
    """Convert a tick to sqrtRatioX96 using Decimal arithmetic for precision."""
    import decimal

    with decimal.localcontext() as ctx:
        ctx.prec = 78  # Enough precision for full uint256 range
        sqrt_price = Decimal("1.0001") ** (Decimal(tick) / 2) * Decimal(Q96)
        return int(sqrt_price)


def _get_liquidity_for_amount0(sqrt_ratio_a: int, sqrt_ratio_b: int, amount0: int) -> int:
    """Compute liquidity from amount0 given two sqrt ratios."""
    if sqrt_ratio_a > sqrt_ratio_b:
        sqrt_ratio_a, sqrt_ratio_b = sqrt_ratio_b, sqrt_ratio_a
    intermediate = sqrt_ratio_a * sqrt_ratio_b // Q96
    return amount0 * intermediate // (sqrt_ratio_b - sqrt_ratio_a)


def _get_liquidity_for_amount1(sqrt_ratio_a: int, sqrt_ratio_b: int, amount1: int) -> int:
    """Compute liquidity from amount1 given two sqrt ratios."""
    if sqrt_ratio_a > sqrt_ratio_b:
        sqrt_ratio_a, sqrt_ratio_b = sqrt_ratio_b, sqrt_ratio_a
    return amount1 * Q96 // (sqrt_ratio_b - sqrt_ratio_a)


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
    "LPDecreaseParams",
    "LPMintParams",
    "MODIFY_LIQUIDITIES_SELECTOR",
    "MODIFY_LIQUIDITIES_WITHOUT_UNLOCK_SELECTOR",
    "NATIVE_CURRENCY",
    "PERMIT2_ADDRESS",
    "PERMIT2_APPROVE_SELECTOR",
    "PM_BURN_POSITION",
    "PM_DECREASE_LIQUIDITY",
    "PM_INCREASE_LIQUIDITY",
    "PM_MINT_POSITION",
    "PM_SETTLE_PAIR",
    "PM_TAKE_PAIR",
    "POOL_MANAGER_ADDRESSES",
    "POSITION_MANAGER_ADDRESSES",
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
