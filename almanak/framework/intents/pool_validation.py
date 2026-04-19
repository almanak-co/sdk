"""Best-effort pool existence validation for DEX protocols.

Validates that a liquidity pool exists on-chain before attempting to compile
a swap or LP intent. Returns structured results instead of raising exceptions,
so callers can decide whether to fail fast or proceed with a warning.

Usage in compiler:
    from almanak.framework.intents.pool_validation import validate_v3_pool, PoolValidationResult

    result = validate_v3_pool("base", "uniswap_v3", token_a, token_b, 3000, rpc_url)
    if result.exists is False:
        return CompilationResult(status=FAILED, error=result.error)

Usage in tests:
    from tests.intents.pool_helpers import fail_if_v3_pool_missing

    fail_if_v3_pool_missing(web3, "base", "uniswap_v3", token0, token1, 3000)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

from almanak.core.contracts import AERODROME, AGNI_FINANCE, PANCAKESWAP_V3, SUSHISWAP_V3, TRADERJOE_V2, UNISWAP_V3
from almanak.framework.data.pools.reader import GET_POOL_SELECTOR

logger = logging.getLogger(__name__)

ZERO_ADDRESS = "0x" + "0" * 40

# Maps protocol name -> contracts.py registry (factory addresses fetched from there)
_V3_PROTOCOL_REGISTRY: dict[str, dict[str, dict[str, str]]] = {
    "uniswap_v3": UNISWAP_V3,
    "sushiswap_v3": SUSHISWAP_V3,
    "pancakeswap_v3": PANCAKESWAP_V3,
    "agni_finance": AGNI_FINANCE,
}

# Aerodrome Classic getPool(address,address,bool) selector
# See `almanak/framework/connectors/aerodrome/abis/pool_factory.json`
_AERODROME_GET_POOL_SELECTOR = "0x79bc57d5"


class PoolValidationReason(str, Enum):
    """Typed reasons for a PoolValidationResult outcome.

    The compiler uses this enum to decide whether to fail compilation
    (fail-closed) or warn-and-proceed (fail-open, impossible-to-verify cases).
    """

    # Positive outcome — pool confirmed on-chain.
    CONFIRMED = "CONFIRMED"

    # Negative outcomes — callers MUST fail closed.
    NOT_FOUND = "NOT_FOUND"  # Factory returned zero address / pool is absent
    RPC_FAILED = "RPC_FAILED"  # RPC call was attempted but errored / bad response

    # Impossible-to-verify outcomes — callers may warn and proceed.
    RPC_UNAVAILABLE = "RPC_UNAVAILABLE"  # No RPC URL configured
    FACTORY_MISSING = "FACTORY_MISSING"  # No factory entry for chain in registry
    PROTOCOL_UNKNOWN = "PROTOCOL_UNKNOWN"  # Protocol not recognised by validator
    NOT_CONFIGURED = "NOT_CONFIGURED"  # Other misconfiguration (e.g. unexpected response shape)


@dataclass
class PoolValidationResult:
    """Result of a pool existence check.

    Attributes:
        exists: True if pool exists, False if confirmed missing, None if unknown.
        reason: Typed outcome category used by callers to decide fail-closed vs warn-and-proceed.
        pool_address: Pool address if found, None otherwise.
        warning: Set when validation could not be performed (exists=None).
        error: Set when validation confirmed the pool is absent/broken (exists=False).
    """

    exists: bool | None
    reason: PoolValidationReason
    pool_address: str | None = None
    warning: str | None = None
    error: str | None = None


def _eth_call(rpc_url: str, to: str, data: str, timeout: float = 5.0) -> bytes | None:
    """Perform a raw eth_call via JSON-RPC. Returns None on any failure."""
    import requests

    try:
        resp = requests.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": to, "data": data}, "latest"],
                "id": 1,
            },
            timeout=timeout,
        )
        result = resp.json().get("result")
        if not result or result == "0x":
            return None
        return bytes.fromhex(result[2:])
    except Exception:
        return None


def _decode_address(data: bytes) -> str:
    """Decode a single address return value (rightmost 20 bytes of 32-byte word)."""
    if len(data) < 32:
        return ZERO_ADDRESS
    return "0x" + data[12:32].hex()


def _encode_get_pool_v3(token_a: str, token_b: str, fee: int) -> str:
    """Encode getPool(address,address,uint24) calldata for V3 factories."""
    a = token_a.lower().replace("0x", "").zfill(64)
    b = token_b.lower().replace("0x", "").zfill(64)
    f = hex(fee)[2:].zfill(64)
    return GET_POOL_SELECTOR + a + b + f


def _encode_get_pool_aerodrome(token_a: str, token_b: str, stable: bool) -> str:
    """Encode getPool(address,address,bool) calldata for Aerodrome factory."""
    a = token_a.lower().replace("0x", "").zfill(64)
    b = token_b.lower().replace("0x", "").zfill(64)
    s = "1".zfill(64) if stable else "0".zfill(64)
    return _AERODROME_GET_POOL_SELECTOR + a + b + s


def validate_v3_pool(
    chain: str,
    protocol: str,
    token_a: str,
    token_b: str,
    fee_tier: int,
    rpc_url: str | None,
) -> PoolValidationResult:
    """Validate that a V3-style pool exists on-chain.

    Works for Uniswap V3, SushiSwap V3, and PancakeSwap V3.

    Args:
        chain: Chain name (e.g. "arbitrum", "base").
        protocol: Protocol name ("uniswap_v3", "sushiswap_v3", "pancakeswap_v3").
        token_a: Token A address (checksummed or lowercase).
        token_b: Token B address (checksummed or lowercase).
        fee_tier: Fee tier in basis points (e.g. 500, 3000).
        rpc_url: RPC URL for on-chain query. If None, returns unknown.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify {protocol} pool existence on {chain}",
        )

    protocol_contracts = _V3_PROTOCOL_REGISTRY.get(protocol)
    if protocol_contracts is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.PROTOCOL_UNKNOWN,
            warning=f"Unknown protocol '{protocol}' — cannot verify pool existence",
        )

    chain_contracts = protocol_contracts.get(chain.lower())
    if chain_contracts is None or "factory" not in chain_contracts:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning=f"No {protocol} factory address for chain '{chain}' — cannot verify pool existence",
        )
    factory = chain_contracts["factory"]

    calldata = _encode_get_pool_v3(token_a, token_b, fee_tier)
    raw = _eth_call(rpc_url, factory, calldata)

    if raw is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to {protocol} factory failed on {chain} — cannot verify pool existence",
        )

    pool_address = _decode_address(raw)

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No {protocol} pool found for "
                f"{token_a[:10]}.../{token_b[:10]}... with fee tier {fee_tier} on {chain}. "
                f"The pool may not exist or may use a different fee tier."
            ),
        )

    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address)


def validate_aerodrome_pool(
    chain: str,
    token_a: str,
    token_b: str,
    stable: bool,
    rpc_url: str | None,
) -> PoolValidationResult:
    """Validate that an Aerodrome Classic pool exists on-chain.

    Args:
        chain: Chain name (should be "base").
        token_a: Token A address.
        token_b: Token B address.
        stable: True for stable pool, False for volatile.
        rpc_url: RPC URL for on-chain query. If None, returns unknown.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify Aerodrome pool existence on {chain}",
        )

    chain_contracts = AERODROME.get(chain.lower())
    if chain_contracts is None or "factory" not in chain_contracts:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning=f"No Aerodrome factory address for chain '{chain}' — cannot verify pool existence",
        )
    factory = chain_contracts["factory"]

    calldata = _encode_get_pool_aerodrome(token_a, token_b, stable)
    raw = _eth_call(rpc_url, factory, calldata)

    if raw is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to Aerodrome factory failed on {chain} — cannot verify pool existence",
        )

    pool_address = _decode_address(raw)
    pool_type = "stable" if stable else "volatile"

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No Aerodrome {pool_type} pool found for "
                f"{token_a[:10]}.../{token_b[:10]}... on {chain}. "
                f"The pool may not exist or may use a different pool type "
                f"(try {'volatile' if stable else 'stable'})."
            ),
        )

    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address)


# Aerodrome Slipstream CL getPool(address,address,int24) selector
_AERODROME_CL_GET_POOL_SELECTOR = "0x28af8d0b"


def _encode_get_pool_aerodrome_cl(token_a: str, token_b: str, tick_spacing: int) -> str:
    """Encode getPool(address,address,int24) calldata for Aerodrome CL factory."""
    a = token_a.lower().replace("0x", "").zfill(64)
    b = token_b.lower().replace("0x", "").zfill(64)
    # tick_spacing is always positive, safe to encode as uint
    ts = hex(tick_spacing)[2:].zfill(64)
    return _AERODROME_CL_GET_POOL_SELECTOR + a + b + ts


def validate_aerodrome_cl_pool(
    chain: str,
    token_a: str,
    token_b: str,
    tick_spacing: int,
    rpc_url: str | None,
) -> PoolValidationResult:
    """Validate that an Aerodrome Slipstream (CL) pool exists on-chain.

    Args:
        chain: Chain name (should be "base").
        token_a: Token A address.
        token_b: Token B address.
        tick_spacing: CL pool tick spacing (e.g. 100).
        rpc_url: RPC URL for on-chain query. If None, returns unknown.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify Aerodrome CL pool existence on {chain}",
        )

    chain_contracts = AERODROME.get(chain.lower())
    if chain_contracts is None or "cl_factory" not in chain_contracts:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning=f"No Aerodrome CL factory address for chain '{chain}' — cannot verify pool existence",
        )
    cl_factory = chain_contracts["cl_factory"]

    calldata = _encode_get_pool_aerodrome_cl(token_a, token_b, tick_spacing)
    raw = _eth_call(rpc_url, cl_factory, calldata)

    if raw is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to Aerodrome CL factory failed on {chain} — cannot verify pool existence",
        )

    pool_address = _decode_address(raw)

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No Aerodrome CL pool found for "
                f"{token_a[:10]}.../{token_b[:10]}... with tick spacing {tick_spacing} on {chain}. "
                f"The pool may not exist or may use a different tick spacing."
            ),
        )

    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address)


_SLOT0_SELECTOR = "0x3850c7bd"

# TraderJoe V2 LBPair getReserves() selector
_TRADERJOE_GET_RESERVES_SELECTOR = "0x0902f1ac"


def fetch_v3_pool_sqrt_price_x96(pool_address: str, rpc_url: str) -> tuple[int, int] | None:
    """Fetch sqrtPriceX96 and current tick from a Uniswap V3-compatible pool's slot0().

    Calls slot0() on the pool contract and returns the first two return values:
    sqrtPriceX96 (uint160) and tick (int24). Both are used by the compiler to
    recompute LP deposit amounts and avoid "Price slippage check" reverts.

    The tick is used for exact integer branch selection (below/in/above range),
    avoiding float precision issues at tick boundaries.

    Args:
        pool_address: Pool contract address.
        rpc_url: RPC URL for on-chain query.

    Returns:
        (sqrtPriceX96, current_tick) as (int, int), or None on any failure.
    """
    raw = _eth_call(rpc_url, pool_address, _SLOT0_SELECTOR)
    if raw is None or len(raw) < 64:
        return None
    sqrt_price_x96 = int.from_bytes(raw[:32], "big")
    # Sanity check: sqrtPriceX96 must be within Uniswap V3 valid range
    if sqrt_price_x96 < 4295128739 or sqrt_price_x96 > 1461446703485210103287273052203988822378723970342:
        return None
    # ABI-decode int24 tick (sign-extended to int256 in ABI encoding)
    tick_raw = int.from_bytes(raw[32:64], "big")
    if tick_raw >= 2**255:
        tick_raw -= 2**256
    # Validate tick is within Uniswap V3 bounds
    if tick_raw < -887272 or tick_raw > 887272:
        return None
    return sqrt_price_x96, tick_raw


def validate_traderjoe_pool(
    chain: str,
    token_x: str,
    token_y: str,
    bin_step: int,
    rpc_url: str | None,
    *,
    allow_empty_reserves: bool = False,
) -> PoolValidationResult:
    """Validate that a TraderJoe V2 LBPair pool exists on-chain.

    Uses the factory's getLBPairInformation(address,address,uint256) method.

    Args:
        chain: Chain name (e.g. "avalanche").
        token_x: Token X address.
        token_y: Token Y address.
        bin_step: Bin step of the pair (e.g. 20).
        rpc_url: RPC URL for on-chain query. If None, returns unknown.
        allow_empty_reserves: If True, skip the zero-liquidity check. Set True
            for LP_OPEN flows where seeding an empty pool is valid.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify TraderJoe V2 pool existence on {chain}",
        )

    chain_contracts = TRADERJOE_V2.get(chain.lower()) or {}
    factory = chain_contracts.get("factory")
    if factory is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning=f"No TraderJoe V2 factory address for chain '{chain}' — cannot verify pool existence",
        )

    # getLBPairInformation(address,address,uint256) selector
    # See `almanak/framework/connectors/traderjoe_v2/abis/LBFactory.json`
    selector = "0x704037bd"
    x = token_x.lower().replace("0x", "").zfill(64)
    y = token_y.lower().replace("0x", "").zfill(64)
    bs = hex(bin_step)[2:].zfill(64)
    calldata = selector + x + y + bs

    raw = _eth_call(rpc_url, factory, calldata)

    if raw is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to TraderJoe V2 factory failed on {chain} — cannot verify pool existence",
        )

    # getLBPairInformation returns (uint16 binStep, address LBPair, bool createdByOwner, bool ignoredForRouting)
    # LBPair address is in the second 32-byte word (offset 32-64)
    if len(raw) < 64:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.NOT_CONFIGURED,
            warning=f"Unexpected response from TraderJoe V2 factory on {chain}",
        )

    pool_address = _decode_address(raw[32:64])

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No TraderJoe V2 pool found for "
                f"{token_x[:10]}.../{token_y[:10]}... with bin step {bin_step} on {chain}. "
                f"The pool may not exist or may use a different bin step."
            ),
        )

    # Pool address exists in factory — verify it has actual liquidity.
    # Skip for LP_OPEN where seeding an empty pool is valid.
    # getReserves() selector: 0x0902f1ac
    if not allow_empty_reserves:
        reserves_raw = _eth_call(rpc_url, pool_address, _TRADERJOE_GET_RESERVES_SELECTOR)
        if reserves_raw is not None and len(reserves_raw) >= 64:
            reserve_x = int.from_bytes(reserves_raw[0:32], "big")
            reserve_y = int.from_bytes(reserves_raw[32:64], "big")
            if reserve_x == 0 and reserve_y == 0:
                return PoolValidationResult(
                    exists=False,
                    reason=PoolValidationReason.NOT_FOUND,
                    error=(
                        f"TraderJoe V2 pool exists for "
                        f"{token_x[:10]}.../{token_y[:10]}... with bin step {bin_step} on {chain}, "
                        f"but has zero liquidity. The swap would revert on-chain."
                    ),
                )

    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address)
