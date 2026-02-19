"""Pool liquidity querying for slippage models.

This module provides functions to query actual on-chain liquidity from DEX pools
(Uniswap V3, PancakeSwap V3, etc.) for use in liquidity-aware slippage models.

The liquidity data helps slippage models make more accurate predictions by
using real pool liquidity instead of estimates.

Key Features:
    - Query Uniswap V3 pool liquidity via `liquidity()` function
    - Support for multiple chains and pool types
    - Fallback to estimated liquidity when on-chain query fails
    - Both async and sync query methods
    - USD liquidity estimation from raw liquidity values

Example:
    from almanak.framework.backtesting.pnl.fee_models.liquidity import (
        query_pool_liquidity,
        PoolLiquidityResult,
    )

    # Query actual liquidity from a pool
    result = await query_pool_liquidity(
        pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        web3=web3,
    )
    print(f"Pool liquidity: {result.liquidity}")
    print(f"Estimated TVL: ${result.liquidity_usd:,.0f}")
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Fallback ETH price used when no current_price_usd is provided for TVL estimation.
# This is a rough reference; callers should always pass current_price_usd for accuracy.
_ETH_FALLBACK_PRICE = Decimal("3000")
_eth_fallback_warned = False

# Function selectors for Uniswap V3 Pool
# liquidity() returns: uint128
LIQUIDITY_SELECTOR = "0x1a686502"

# slot0() returns: sqrtPriceX96, tick, observationIndex, observationCardinality, ...
SLOT0_SELECTOR = "0x3850c7bd"

# token0() returns: address
TOKEN0_SELECTOR = "0x0dfe1681"

# token1() returns: address
TOKEN1_SELECTOR = "0xd21220a7"

# fee() returns: uint24
FEE_SELECTOR = "0xddca3f43"


# Default liquidity estimates per pool type/chain (in USD)
# Used as fallback when on-chain query fails
DEFAULT_LIQUIDITY_USD: dict[str, Decimal] = {
    # By fee tier
    "stable_001": Decimal("10000000"),  # $10M for 0.01% stablecoin pools
    "stable_005": Decimal("5000000"),  # $5M for 0.05% stable pools
    "blue_chip_005": Decimal("5000000"),  # $5M for 0.05% blue chip pools
    "blue_chip_030": Decimal("2000000"),  # $2M for 0.3% blue chip pools
    "volatile_030": Decimal("1000000"),  # $1M for 0.3% volatile pools
    "exotic_100": Decimal("100000"),  # $100k for 1% exotic pools
    # Default fallback
    "default": Decimal("1000000"),  # $1M default
}


# Known pool addresses for common pairs (chain -> token_pair -> pool_address)
KNOWN_POOLS: dict[str, dict[str, str]] = {
    "ethereum": {
        "WETH/USDC": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # 0.05%
        "WETH/USDT": "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",  # 0.3%
        "WBTC/WETH": "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",  # 0.3%
        "USDC/USDT": "0x3416cF6C708Da44DB2624D63ea0AAef7113527C6",  # 0.01%
    },
    "arbitrum": {
        "WETH/USDC": "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",  # 0.05%
        "ARB/USDC": "0xc473e2aEE3441BF9240Be85eb122aBB059A3B57c",  # 0.05%
        "WBTC/WETH": "0x2f5e87C9312fa29aed5c179E456625D79015299c",  # 0.05%
        "GMX/WETH": "0x80A9ae39310abf666A87C743d6ebBD0E8C42158E",  # 0.3%
    },
    "base": {
        "WETH/USDC": "0xd0b53D9277642d899DF5C87A3966A349A798F224",  # 0.05%
        "CBETH/WETH": "0x10648BA41B8565907Cfa1496765fA4D95390aa0d",  # 0.05%
    },
    "optimism": {
        "WETH/USDC": "0x85149247691df622eaF1a8Bd0CaFd40BC45154a9",  # 0.05%
        "OP/USDC": "0x1C3140aB59d6cAf9fa7459C6f83D4B52ba881d36",  # 0.3%
    },
    "polygon": {
        "WETH/USDC": "0x45dDa9cb7c25131DF268515131f647d726f50608",  # 0.05%
        "MATIC/USDC": "0xA374094527e1673A86dE625aa59517c5dE346d32",  # 0.05%
    },
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PoolLiquidityResult:
    """Result of a pool liquidity query.

    Attributes:
        pool_address: Address of the queried pool
        liquidity: Raw liquidity value from the pool (uint128)
        liquidity_usd: Estimated USD value of the liquidity
        sqrt_price_x96: Current sqrtPriceX96 from slot0 (if available)
        tick: Current tick from slot0 (if available)
        fee_tier: Pool fee tier in bps (if available)
        is_estimated: True if liquidity_usd is estimated, False if from on-chain
        source: Source of the data ("on-chain", "estimated", "default")
    """

    pool_address: str
    liquidity: int
    liquidity_usd: Decimal
    sqrt_price_x96: int | None = None
    tick: int | None = None
    fee_tier: int | None = None
    is_estimated: bool = False
    source: str = "on-chain"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "pool_address": self.pool_address,
            "liquidity": str(self.liquidity),
            "liquidity_usd": str(self.liquidity_usd),
            "sqrt_price_x96": str(self.sqrt_price_x96) if self.sqrt_price_x96 else None,
            "tick": self.tick,
            "fee_tier": self.fee_tier,
            "is_estimated": self.is_estimated,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PoolLiquidityResult":
        """Deserialize from dictionary."""
        return cls(
            pool_address=data["pool_address"],
            liquidity=int(data["liquidity"]),
            liquidity_usd=Decimal(data["liquidity_usd"]),
            sqrt_price_x96=int(data["sqrt_price_x96"]) if data.get("sqrt_price_x96") else None,
            tick=data.get("tick"),
            fee_tier=data.get("fee_tier"),
            is_estimated=data.get("is_estimated", False),
            source=data.get("source", "on-chain"),
        )


# =============================================================================
# Pool Liquidity Query Functions
# =============================================================================


async def query_pool_liquidity(
    pool_address: str,
    web3: Any,
    current_price_usd: Decimal | None = None,
) -> PoolLiquidityResult:
    """Query the liquidity of a Uniswap V3 (or compatible) pool.

    This function queries the pool's liquidity() function to get the current
    active liquidity, and optionally queries slot0() for price data.

    Args:
        pool_address: Address of the pool contract
        web3: Web3 instance connected to the target chain
        current_price_usd: Current token price in USD for liquidity estimation
            If not provided, uses a rough estimate based on liquidity value

    Returns:
        PoolLiquidityResult with liquidity data

    Example:
        from web3 import Web3

        web3 = Web3(Web3.HTTPProvider("https://arb1.arbitrum.io/rpc"))
        result = await query_pool_liquidity(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            web3=web3,
            current_price_usd=Decimal("3500"),  # ETH price
        )
        print(f"Liquidity: {result.liquidity}")
        print(f"TVL estimate: ${result.liquidity_usd:,.0f}")
    """
    pool_checksum = web3.to_checksum_address(pool_address)

    try:
        # Query liquidity
        liquidity = await _query_liquidity(web3, pool_checksum)
        if liquidity == 0:
            logger.debug(f"Pool {pool_address} has zero liquidity")

        # Query slot0 for current price data
        slot0_data = await _query_slot0(web3, pool_checksum)

        # Query fee tier
        fee_tier = await _query_fee(web3, pool_checksum)

        # Estimate USD liquidity
        liquidity_usd = _estimate_liquidity_usd(
            liquidity=liquidity,
            sqrt_price_x96=slot0_data.get("sqrt_price_x96") if slot0_data else None,
            current_price_usd=current_price_usd,
            fee_tier=fee_tier,
        )

        return PoolLiquidityResult(
            pool_address=pool_address,
            liquidity=liquidity,
            liquidity_usd=liquidity_usd,
            sqrt_price_x96=slot0_data.get("sqrt_price_x96") if slot0_data else None,
            tick=slot0_data.get("tick") if slot0_data else None,
            fee_tier=fee_tier,
            is_estimated=False,
            source="on-chain",
        )

    except Exception as e:
        logger.warning(f"Failed to query pool liquidity for {pool_address}: {e}")
        return _get_default_liquidity_result(pool_address)


def query_pool_liquidity_sync(
    pool_address: str,
    web3: Any,
    current_price_usd: Decimal | None = None,
) -> PoolLiquidityResult:
    """Synchronous version of query_pool_liquidity.

    For use in non-async contexts. See query_pool_liquidity for full docs.

    Args:
        pool_address: Address of the pool contract
        web3: Web3 instance connected to the target chain
        current_price_usd: Current token price in USD for liquidity estimation

    Returns:
        PoolLiquidityResult with liquidity data
    """
    pool_checksum = web3.to_checksum_address(pool_address)

    try:
        # Query liquidity
        liquidity = _query_liquidity_sync(web3, pool_checksum)
        if liquidity == 0:
            logger.debug(f"Pool {pool_address} has zero liquidity")

        # Query slot0 for current price data
        slot0_data = _query_slot0_sync(web3, pool_checksum)

        # Query fee tier
        fee_tier = _query_fee_sync(web3, pool_checksum)

        # Estimate USD liquidity
        liquidity_usd = _estimate_liquidity_usd(
            liquidity=liquidity,
            sqrt_price_x96=slot0_data.get("sqrt_price_x96") if slot0_data else None,
            current_price_usd=current_price_usd,
            fee_tier=fee_tier,
        )

        return PoolLiquidityResult(
            pool_address=pool_address,
            liquidity=liquidity,
            liquidity_usd=liquidity_usd,
            sqrt_price_x96=slot0_data.get("sqrt_price_x96") if slot0_data else None,
            tick=slot0_data.get("tick") if slot0_data else None,
            fee_tier=fee_tier,
            is_estimated=False,
            source="on-chain",
        )

    except Exception as e:
        logger.warning(f"Failed to query pool liquidity for {pool_address}: {e}")
        return _get_default_liquidity_result(pool_address)


def get_pool_address(
    token0: str,
    token1: str,
    chain: str = "ethereum",
) -> str | None:
    """Get a known pool address for a token pair.

    Args:
        token0: First token symbol (e.g., "WETH")
        token1: Second token symbol (e.g., "USDC")
        chain: Chain identifier

    Returns:
        Pool address if found, None otherwise
    """
    chain_pools = KNOWN_POOLS.get(chain.lower(), {})

    # Try both orderings
    pair = f"{token0.upper()}/{token1.upper()}"
    pair_reverse = f"{token1.upper()}/{token0.upper()}"

    return chain_pools.get(pair) or chain_pools.get(pair_reverse)


def estimate_liquidity_for_trade(
    trade_amount_usd: Decimal,
    pool_liquidity_usd: Decimal,
    fee_tier_bps: int = 3000,
) -> Decimal:
    """Estimate the impact of a trade on pool liquidity.

    This helps determine whether a trade size is appropriate for
    the available liquidity.

    Args:
        trade_amount_usd: Trade size in USD
        pool_liquidity_usd: Pool TVL in USD
        fee_tier_bps: Pool fee tier in basis points

    Returns:
        Estimated slippage factor (0.01 = 1% slippage)
    """
    if pool_liquidity_usd <= 0:
        return Decimal("0.05")  # 5% max slippage for unknown liquidity

    # Basic model: slippage ~ trade_size / liquidity
    # Adjusted by fee tier (higher fees = pools typically have less liquidity depth)
    fee_multiplier = Decimal(str(fee_tier_bps)) / Decimal("3000")  # Normalize to 0.3%
    base_slippage = trade_amount_usd / pool_liquidity_usd * fee_multiplier

    # Apply sqrt scaling for concentrated liquidity
    # In V3, slippage scales roughly with sqrt(amount/liquidity)
    import math

    scaled_slippage = Decimal(str(math.sqrt(float(base_slippage))))

    # Cap at 5%
    return min(scaled_slippage, Decimal("0.05"))


# =============================================================================
# Internal Helper Functions
# =============================================================================


def _pad_address(addr: str) -> str:
    """Pad address to 32 bytes for ABI encoding."""
    return addr.lower().replace("0x", "").zfill(64)


async def _query_liquidity(web3: Any, pool_address: str) -> int:
    """Query pool liquidity via liquidity() function."""
    try:
        result = await web3.eth.call({"to": pool_address, "data": LIQUIDITY_SELECTOR})
        return int.from_bytes(result, byteorder="big")
    except Exception as e:
        logger.debug(f"Failed to query liquidity: {e}")
        return 0


def _query_liquidity_sync(web3: Any, pool_address: str) -> int:
    """Synchronous version of _query_liquidity."""
    try:
        result = web3.eth.call({"to": pool_address, "data": LIQUIDITY_SELECTOR})
        return int.from_bytes(result, byteorder="big")
    except Exception as e:
        logger.debug(f"Failed to query liquidity: {e}")
        return 0


async def _query_slot0(web3: Any, pool_address: str) -> dict[str, Any] | None:
    """Query pool slot0 for current price data.

    slot0() returns:
    - sqrtPriceX96 (uint160)
    - tick (int24)
    - observationIndex (uint16)
    - observationCardinality (uint16)
    - observationCardinalityNext (uint16)
    - feeProtocol (uint8)
    - unlocked (bool)
    """
    try:
        result = await web3.eth.call({"to": pool_address, "data": SLOT0_SELECTOR})
        if len(result) < 64:
            return None

        sqrt_price_x96 = int.from_bytes(result[0:32], byteorder="big")
        tick_raw = int.from_bytes(result[32:64], byteorder="big")

        # Handle int24 sign extension
        if tick_raw >= 2**23:
            tick = tick_raw - 2**24
        else:
            tick = tick_raw

        return {
            "sqrt_price_x96": sqrt_price_x96,
            "tick": tick,
        }
    except Exception as e:
        logger.debug(f"Failed to query slot0: {e}")
        return None


def _query_slot0_sync(web3: Any, pool_address: str) -> dict[str, Any] | None:
    """Synchronous version of _query_slot0."""
    try:
        result = web3.eth.call({"to": pool_address, "data": SLOT0_SELECTOR})
        if len(result) < 64:
            return None

        sqrt_price_x96 = int.from_bytes(result[0:32], byteorder="big")
        tick_raw = int.from_bytes(result[32:64], byteorder="big")

        # Handle int24 sign extension
        if tick_raw >= 2**23:
            tick = tick_raw - 2**24
        else:
            tick = tick_raw

        return {
            "sqrt_price_x96": sqrt_price_x96,
            "tick": tick,
        }
    except Exception as e:
        logger.debug(f"Failed to query slot0: {e}")
        return None


async def _query_fee(web3: Any, pool_address: str) -> int | None:
    """Query pool fee tier."""
    try:
        result = await web3.eth.call({"to": pool_address, "data": FEE_SELECTOR})
        return int.from_bytes(result, byteorder="big")
    except Exception as e:
        logger.debug(f"Failed to query fee: {e}")
        return None


def _query_fee_sync(web3: Any, pool_address: str) -> int | None:
    """Synchronous version of _query_fee."""
    try:
        result = web3.eth.call({"to": pool_address, "data": FEE_SELECTOR})
        return int.from_bytes(result, byteorder="big")
    except Exception as e:
        logger.debug(f"Failed to query fee: {e}")
        return None


def _estimate_liquidity_usd(
    liquidity: int,
    sqrt_price_x96: int | None = None,
    current_price_usd: Decimal | None = None,
    fee_tier: int | None = None,
    strict_mode: bool = False,
) -> Decimal:
    """Estimate USD liquidity from raw liquidity value.

    Uniswap V3 liquidity is a complex value representing the geometric mean
    of token amounts. This function provides a rough USD estimate.

    The formula is approximate:
        TVL_USD ~ liquidity * sqrt(price) / 2^48

    This is a simplification that works reasonably well for most pools.

    Args:
        liquidity: Raw liquidity value (uint128)
        sqrt_price_x96: Current sqrtPriceX96 from slot0
        current_price_usd: Known price of the quote token in USD
        fee_tier: Pool fee tier for heuristic adjustment
        strict_mode: If True, raise instead of using fallback ETH price

    Returns:
        Estimated TVL in USD
    """
    if liquidity == 0:
        return Decimal("0")

    # If we have sqrt_price_x96, use it for a better estimate
    if sqrt_price_x96 and sqrt_price_x96 > 0:
        # sqrtPriceX96 = sqrt(price) * 2^96
        # price = (sqrtPriceX96 / 2^96)^2
        sqrt_price = sqrt_price_x96 / (2**96)

        # Rough TVL estimate: liquidity * sqrt(price) * price_usd / 10^15
        # This is highly simplified but gives a reasonable order of magnitude
        if current_price_usd:
            tvl = Decimal(str(liquidity * sqrt_price)) * current_price_usd / Decimal("1e15")
        else:
            # No USD price provided -- use a rough ETH reference price.
            # Callers should pass current_price_usd for accurate results.
            if strict_mode:
                raise ValueError(
                    "No current_price_usd provided for TVL estimate and strict_mode=True. "
                    "Pass current_price_usd for accurate results."
                )
            global _eth_fallback_warned  # noqa: PLW0603
            if not _eth_fallback_warned:
                logger.warning(
                    "No current_price_usd provided for TVL estimate; falling back to $%s ETH reference price. "
                    "This warning is logged once per session.",
                    _ETH_FALLBACK_PRICE,
                )
                _eth_fallback_warned = True
            tvl = Decimal(str(liquidity * sqrt_price)) * _ETH_FALLBACK_PRICE / Decimal("1e15")

        # Apply fee tier multiplier (lower fee = typically more liquidity)
        if fee_tier:
            if fee_tier == 100:  # 0.01%
                tvl = tvl * Decimal("1.5")
            elif fee_tier == 500:  # 0.05%
                tvl = tvl * Decimal("1.2")
            elif fee_tier == 10000:  # 1%
                tvl = tvl * Decimal("0.5")

        return max(tvl, Decimal("1000"))  # Minimum $1k

    # Fallback: very rough estimate based on liquidity magnitude
    # This is a heuristic based on typical pool sizes
    liquidity_log = len(str(liquidity))  # Order of magnitude

    if liquidity_log >= 25:
        return Decimal("50000000")  # $50M+
    elif liquidity_log >= 22:
        return Decimal("10000000")  # $10M
    elif liquidity_log >= 19:
        return Decimal("1000000")  # $1M
    elif liquidity_log >= 16:
        return Decimal("100000")  # $100k
    else:
        return Decimal("10000")  # $10k


def _get_default_liquidity_result(pool_address: str) -> PoolLiquidityResult:
    """Get default liquidity result when on-chain query fails."""
    return PoolLiquidityResult(
        pool_address=pool_address,
        liquidity=0,
        liquidity_usd=DEFAULT_LIQUIDITY_USD["default"],
        is_estimated=True,
        source="default",
    )


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Main query functions
    "query_pool_liquidity",
    "query_pool_liquidity_sync",
    # Helper functions
    "get_pool_address",
    "estimate_liquidity_for_trade",
    # Data classes
    "PoolLiquidityResult",
    # Constants
    "DEFAULT_LIQUIDITY_USD",
    "KNOWN_POOLS",
    "LIQUIDITY_SELECTOR",
]
