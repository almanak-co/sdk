"""Shared Uniswap V3-family gateway TWAP-observation pipeline (gateway-side foundation).

Single-observation TWAP fetch via the V3 pool ``observe(secondsAgos)`` ABI,
plus the pure ``observe`` codec and the pool token/decimals reads it needs. The
on-chain ABI (``observe`` / ``token0`` / ``token1`` / ``decimals``) is identical
across Uniswap V3 and its forks (PancakeSwap V3, SushiSwap V3, Agni), so this
pipeline lives in the gateway-side connector foundation rather than in the
Uniswap V3 connector -- the forks call :func:`fetch_v3_twap_observation` from
here instead of importing each other's gateway providers.

Gateway-side: the gateway-service imports (``DexTwapPoint`` /
``RateHistoryUnavailable``) are deferred inside the functions so importing this
module stays cheap and free of import cycles.

Migrated from ``framework/backtesting/pnl/providers/twap.py`` and the Uniswap V3
gateway provider (the ``_query_observe`` decode block).
"""

from __future__ import annotations

import math
import time
from decimal import Decimal
from typing import Any

# observe(uint32[] secondsAgos) -> (int56[] tickCumulatives, uint160[] secondsPerLiquidityX128s)
_OBSERVE_SELECTOR = "883bdbfd"

# token0() / token1() — used for decimal-aware tick→price conversion.
_TOKEN0_SELECTOR = "0dfe1681"
_TOKEN1_SELECTOR = "d21220a7"

# ERC20 decimals() selector.
_DECIMALS_SELECTOR = "313ce567"


# =============================================================================
# ``observe()`` codec helpers — pure functions, no I/O
# =============================================================================


def _encode_observe_call(seconds_agos: list[int]) -> str:
    """ABI-encode ``observe(uint32[] secondsAgos)`` calldata.

    Returns a 0x-prefixed hex string suitable for ``eth_call`` /
    ``web3.eth.call``.
    """
    offset = 32  # 0x20: dynamic data offset (points to array start)
    length = len(seconds_agos)

    calldata = f"0x{_OBSERVE_SELECTOR}"
    calldata += offset.to_bytes(32, byteorder="big").hex()
    calldata += length.to_bytes(32, byteorder="big").hex()
    for sec in seconds_agos:
        calldata += sec.to_bytes(32, byteorder="big").hex()
    return calldata


def _decode_observe_response(result: bytes) -> tuple[list[int], list[int]]:
    """Decode ``observe`` return data into ``(tickCumulatives, secondsPerLiquidityX128s)``.

    The pool's ``observe`` returns two parallel ``uint`` arrays; we
    only consume ``tickCumulatives`` to compute TWAP, but
    ``secondsPerLiquidity`` is returned alongside for future callers
    that may want it (liquidity-weighted price impact, etc.).
    """
    if len(result) < 128:
        raise ValueError(f"observe() response too short: {len(result)} bytes")

    offset_ticks = int.from_bytes(result[0:32], byteorder="big")
    offset_liquidity = int.from_bytes(result[32:64], byteorder="big")

    # tickCumulatives array.
    tick_array_start = offset_ticks
    tick_array_len = int.from_bytes(result[tick_array_start : tick_array_start + 32], byteorder="big")
    tick_cumulatives: list[int] = []
    for i in range(tick_array_len):
        element_start = tick_array_start + 32 + (i * 32)
        # int56 stored signed in the low 7 bytes; read as int256 with
        # sign extension. Empirically, V3 pools return values that fit
        # comfortably in int56 but the codec is int256 on the wire.
        raw_value = int.from_bytes(
            result[element_start : element_start + 32],
            byteorder="big",
            signed=True,
        )
        tick_cumulatives.append(raw_value)

    # secondsPerLiquidityCumulativeX128s array.
    liq_array_start = offset_liquidity
    liq_array_len = int.from_bytes(result[liq_array_start : liq_array_start + 32], byteorder="big")
    liquidity_cumulatives: list[int] = []
    for i in range(liq_array_len):
        element_start = liq_array_start + 32 + (i * 32)
        raw_value = int.from_bytes(result[element_start : element_start + 32], byteorder="big")
        liquidity_cumulatives.append(raw_value)

    return tick_cumulatives, liquidity_cumulatives


def _tick_to_price(
    tick: int,
    token0_decimals: int = 18,
    token1_decimals: int = 6,
) -> Decimal:
    """Convert a Uniswap V3 tick to token1/token0 price in human units.

    Tick formula: ``price = 1.0001^tick * 10^(token0_dec - token1_dec)``.
    The decimal adjustment converts the raw on-chain ratio to
    human-readable price (e.g. ``$3000`` for WETH/USDC instead of
    ``3e-15``).
    """
    base_price = Decimal(str(math.pow(1.0001, tick)))
    decimal_adjustment = Decimal(10 ** (token0_decimals - token1_decimals))
    return base_price * decimal_adjustment


async def _fetch_pool_tokens_and_decimals(
    web3: Any,
    pool_address: str,
    block_identifier: int | str,
) -> tuple[str, str, int, int]:
    """Read ``(token0_addr, token1_addr, token0_decimals, token1_decimals)``.

    Four ``eth_call`` round-trips (token0, token1, t0.decimals(), t1.decimals()).
    Cheap enough for the prototype Step 2; Step 3 introduces a per-pool
    decimals cache in the servicer to amortise across repeated calls.

    The token0/token1 ADDRESSES are returned alongside the decimals (lowercased)
    so the LWAP caller can filter a multi-pool set down to the requested pair
    without a second set of reads (VIB-4924 B2 follow-on).
    """
    t0_data = await web3.eth.call(
        {"to": pool_address, "data": f"0x{_TOKEN0_SELECTOR}"},
        block_identifier=block_identifier,
    )
    t1_data = await web3.eth.call(
        {"to": pool_address, "data": f"0x{_TOKEN1_SELECTOR}"},
        block_identifier=block_identifier,
    )

    # Each token() return is a single 32-byte word: address right-padded.
    t0_address = web3.to_checksum_address("0x" + t0_data[-20:].hex())
    t1_address = web3.to_checksum_address("0x" + t1_data[-20:].hex())

    t0_decimals_data = await web3.eth.call(
        {"to": t0_address, "data": f"0x{_DECIMALS_SELECTOR}"},
        block_identifier=block_identifier,
    )
    t1_decimals_data = await web3.eth.call(
        {"to": t1_address, "data": f"0x{_DECIMALS_SELECTOR}"},
        block_identifier=block_identifier,
    )

    # An empty return from ``decimals()`` (token address isn't a
    # contract, or the contract doesn't implement the ERC20 ABI) would
    # silently decode to ``0``, throwing the tick→price math off by
    # ``10^(t0_dec - t1_dec)`` of magnitude. Raise loudly so the caller
    # surfaces a typed ``RateHistoryUnavailable`` rather than emitting a
    # wildly wrong price. Gemini PR-review feedback (PR #2474).
    if not t0_decimals_data or not t1_decimals_data:
        raise ValueError(
            f"decimals() returned empty data for pool {pool_address!r} (token0={t0_address}, token1={t1_address})"
        )
    t0_decimals = int.from_bytes(t0_decimals_data, byteorder="big")
    t1_decimals = int.from_bytes(t1_decimals_data, byteorder="big")
    # ERC-20 ``decimals()`` is a ``uint8`` on-chain (0..255). A malicious
    # or non-ERC20 contract can return a much larger value, which would
    # trigger pathological ``10 ** (t0_dec - t1_dec)`` exponentiation in
    # the tick->price math. Bound to the on-chain type and raise loudly.
    # CodeRabbit PR-review feedback (PR #2474).
    if not (0 <= t0_decimals <= 255 and 0 <= t1_decimals <= 255):
        raise ValueError(
            f"Invalid ERC20 decimals for pool {pool_address!r}: token0={t0_decimals}, token1={t1_decimals}"
        )
    return t0_address.lower(), t1_address.lower(), t0_decimals, t1_decimals


async def _fetch_pool_token_decimals(
    web3: Any,
    pool_address: str,
    block_identifier: int | str,
) -> tuple[int, int]:
    """Read ``(token0_decimals, token1_decimals)`` for a pool (TWAP path)."""
    _t0, _t1, t0_decimals, t1_decimals = await _fetch_pool_tokens_and_decimals(web3, pool_address, block_identifier)
    return t0_decimals, t1_decimals


async def _twap_resolve_web3_and_pool(
    servicer: Any,
    chain: str,
    pool_address: str,
    *,
    protocol: str,
) -> tuple[Any, str]:
    """Return ``(web3, pool_checksum)`` for a TWAP call.

    Raises ``RateHistoryUnavailable`` when the chain has no RPC URL or
    the pool address fails the checksum decode.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    try:
        web3 = await servicer._get_web3(chain)
    except ValueError as exc:
        raise RateHistoryUnavailable(protocol, f"No RPC URL configured for chain {chain!r}: {exc}") from exc

    try:
        pool_checksum = web3.to_checksum_address(pool_address)
    except ValueError as exc:
        raise RateHistoryUnavailable(protocol, f"Invalid pool address {pool_address!r}: {exc}") from exc
    return web3, pool_checksum


async def _twap_call_observe(
    web3: Any,
    *,
    pool_checksum: str,
    seconds_agos: list[int],
    block_identifier: int | str,
    protocol: str,
    pool_address: str,
) -> tuple[list[int], list[int]]:
    """Encode + execute ``observe(secondsAgos)`` and decode the tick cumulatives.

    Failures are normalised to ``RateHistoryUnavailable`` with ``protocol``
    distinguishing call sites.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    calldata = _encode_observe_call(seconds_agos)
    try:
        result = await web3.eth.call(
            {"to": pool_checksum, "data": calldata},
            block_identifier=block_identifier,
        )
        # Decode inside the try so a malformed ``observe()`` payload
        # (raw ``ValueError`` from ``_decode_observe_response``) surfaces
        # as a typed ``RateHistoryUnavailable`` rather than leaking as a
        # gRPC INTERNAL error. CodeRabbit PR-review feedback (PR #2474).
        tick_cumulatives, liquidity_cumulatives = _decode_observe_response(result)
    except Exception as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"observe() request/decode failed on pool {pool_address!r}: {exc}",
        ) from exc

    if len(tick_cumulatives) < 2:
        raise RateHistoryUnavailable(
            protocol,
            f"observe() returned {len(tick_cumulatives)} tick(s); need >= 2",
        )
    return tick_cumulatives, liquidity_cumulatives


async def _twap_resolve_pool_decimals(
    web3: Any,
    pool_checksum: str,
    block_identifier: int | str,
    *,
    protocol: str,
    pool_address: str,
) -> tuple[int, int]:
    """Read pool decimals, wrapping failures as ``RateHistoryUnavailable``."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    try:
        return await _fetch_pool_token_decimals(web3, pool_checksum, block_identifier)
    except Exception as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"Failed to read token decimals for pool {pool_address!r}: {exc}",
        ) from exc


async def fetch_v3_twap_observation(
    servicer: Any,
    *,
    chain: str,
    pool_address: str,
    secs_ago_start: int,
    secs_ago_end: int,
    as_of_block: int | None,
    protocol: str,
) -> Any:
    """Shared single-observation TWAP fetch for Uniswap V3 + forks.

    ``protocol`` ("uniswap_v3" / "pancakeswap_v3" / "sushiswap_v3") is
    used only for error-message attribution — the on-chain ABI is
    identical across V3 forks.
    """
    from almanak.gateway.services.rate_history_service import (
        DexTwapPoint,
        RateHistoryUnavailable,
    )

    web3, pool_checksum = await _twap_resolve_web3_and_pool(servicer, chain, pool_address, protocol=protocol)

    seconds_elapsed = secs_ago_start - secs_ago_end
    if seconds_elapsed <= 0:
        raise RateHistoryUnavailable(
            protocol,
            f"non-positive window (start={secs_ago_start}, end={secs_ago_end})",
        )

    block_identifier: int | str = as_of_block if as_of_block is not None else "latest"
    tick_cumulatives, _liquidity = await _twap_call_observe(
        web3,
        pool_checksum=pool_checksum,
        seconds_agos=[secs_ago_start, secs_ago_end],
        block_identifier=block_identifier,
        protocol=protocol,
        pool_address=pool_address,
    )

    tick_diff = tick_cumulatives[1] - tick_cumulatives[0]
    tick_twap = tick_diff // seconds_elapsed

    t0_decimals, t1_decimals = await _twap_resolve_pool_decimals(
        web3,
        pool_checksum,
        block_identifier,
        protocol=protocol,
        pool_address=pool_address,
    )
    price = _tick_to_price(tick_twap, t0_decimals, t1_decimals)

    return DexTwapPoint(
        timestamp=int(time.time()),
        price=price,
        tick_observation_count=len(tick_cumulatives),
    )
