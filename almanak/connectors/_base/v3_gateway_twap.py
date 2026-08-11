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

import asyncio
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


def _read_word(result: bytes, start: int, *, signed: bool = False) -> int:
    """Read a 32-byte big-endian word at ``start``, raising on a short payload.

    ``int.from_bytes(b"", ...)`` silently returns ``0``, so without an explicit
    bounds check a truncated ``observe()`` return would decode array lengths /
    elements as zero instead of failing. Raise ``ValueError`` so the caller
    (``_twap_call_observe``) normalises it to a typed ``RateHistoryUnavailable``
    rather than fabricating a price from a malformed payload. Defense-in-depth
    on top of the caller's try/except (Gemini PR-review feedback, PR #2856).
    """
    end = start + 32
    if end > len(result):
        raise ValueError(f"observe() response truncated: need bytes [{start}:{end}), have {len(result)}")
    return int.from_bytes(result[start:end], byteorder="big", signed=signed)


def _decode_observe_response(result: bytes) -> tuple[list[int], list[int]]:
    """Decode ``observe`` return data into ``(tickCumulatives, secondsPerLiquidityX128s)``.

    The pool's ``observe`` returns two parallel ``uint`` arrays; we
    only consume ``tickCumulatives`` to compute TWAP, but
    ``secondsPerLiquidity`` is returned alongside for future callers
    that may want it (liquidity-weighted price impact, etc.).

    Every slice goes through :func:`_read_word`, which bounds-checks the
    payload before each 32-byte read — a truncated return (array offsets /
    lengths that point past the data) raises ``ValueError`` instead of
    silently decoding to zero.
    """
    if len(result) < 128:
        raise ValueError(f"observe() response too short: {len(result)} bytes")

    offset_ticks = _read_word(result, 0)
    offset_liquidity = _read_word(result, 32)

    # tickCumulatives array.
    tick_array_len = _read_word(result, offset_ticks)
    tick_cumulatives: list[int] = []
    for i in range(tick_array_len):
        element_start = offset_ticks + 32 + (i * 32)
        # int56 stored signed in the low 7 bytes; read as int256 with
        # sign extension. Empirically, V3 pools return values that fit
        # comfortably in int56 but the codec is int256 on the wire.
        tick_cumulatives.append(_read_word(result, element_start, signed=True))

    # secondsPerLiquidityCumulativeX128s array.
    liq_array_len = _read_word(result, offset_liquidity)
    liquidity_cumulatives: list[int] = []
    for i in range(liq_array_len):
        element_start = offset_liquidity + 32 + (i * 32)
        liquidity_cumulatives.append(_read_word(result, element_start))

    return tick_cumulatives, liquidity_cumulatives


def _twap_tick_from_cumulatives(tick_diff: int, seconds_elapsed: int) -> int:
    """Average tick over the window, matching on-chain Uniswap V3 oracle semantics.

    The on-chain oracle computes ``tickCumulativeDelta / secondsElapsed`` in
    Solidity, where integer division **truncates toward zero**. Python's ``//``
    **floors toward negative infinity**, so for a negative ``tick_diff`` not
    divisible by ``seconds_elapsed`` the two disagree by one tick (~1bp of
    price). Reproduce the Solidity truncation exactly with pure integer
    arithmetic.

    ``int(tick_diff / seconds_elapsed)`` would also truncate toward zero but
    routes through float division, losing precision on large ``int56``
    ``tick_cumulatives`` — so we stay in integer space via ``divmod``.

    ``seconds_elapsed`` is guaranteed ``> 0`` by the caller's window
    validation, so ``divmod`` yields a non-negative remainder and ``q`` is the
    floor; nudging it up by one for a negative, non-divisible numerator
    recovers truncation-toward-zero.
    """
    q, r = divmod(tick_diff, seconds_elapsed)
    return q + 1 if (r != 0 and tick_diff < 0) else q


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

    # An empty / truncated token0()/token1() return (pool address isn't a
    # contract, or the node returned partial data) would slice ``[-20:]`` over
    # fewer than 20 bytes and surface as a confusing checksum-address
    # ValueError. Validate the 32-byte word length first so the failure names
    # the real cause. Gemini PR-review feedback (PR #2856).
    if len(t0_data) < 32 or len(t1_data) < 32:
        raise ValueError(
            f"token0()/token1() returned truncated data for pool {pool_address!r} "
            f"(token0={len(t0_data)} bytes, token1={len(t1_data)} bytes)"
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
    tick_twap = _twap_tick_from_cumulatives(tick_diff, seconds_elapsed)

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


def _block_field(block: Any, name: str) -> int:
    """Read an integer block field from Web3 mapping/attribute responses."""
    value = block.get(name) if hasattr(block, "get") else getattr(block, name, None)
    if value is None:
        raise ValueError(f"block response omitted {name!r}")
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    return int(value)


async def _historical_block_sample(web3: Any, block_identifier: int | str, *, protocol: str) -> tuple[int, int]:
    """Return ``(block_number, block_timestamp)`` with typed failure attribution."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    try:
        block = await web3.eth.get_block(block_identifier)
        return _block_field(block, "number"), _block_field(block, "timestamp")
    except Exception as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"failed to resolve archive block {block_identifier!r}: {exc}",
        ) from exc


async def _block_at_or_before(
    web3: Any,
    target_timestamp: int,
    *,
    lower: tuple[int, int],
    upper: tuple[int, int],
    protocol: str,
) -> tuple[int, int]:
    """Resolve the latest block whose timestamp is no later than ``target``.

    Block timestamps are monotonic but block time is not constant. Interpolation
    normally lands within a few blocks over long backtest windows; after eight
    interpolation attempts the resolver switches to bisection so pathological
    timestamp distributions still make logarithmic, bounded progress.
    """
    lower_number, lower_timestamp = lower
    upper_number, upper_timestamp = upper
    if target_timestamp < lower_timestamp:
        raise ValueError(f"target timestamp {target_timestamp} precedes earliest block timestamp {lower_timestamp}")
    if target_timestamp >= upper_timestamp:
        return upper

    attempts = 0
    while upper_number - lower_number > 1:
        attempts += 1
        if attempts > 8 or upper_timestamp <= lower_timestamp:
            candidate_number = (lower_number + upper_number) // 2
        else:
            numerator = (target_timestamp - lower_timestamp) * (upper_number - lower_number)
            candidate_number = lower_number + numerator // (upper_timestamp - lower_timestamp)
            candidate_number = max(lower_number + 1, min(upper_number - 1, candidate_number))
        candidate = await _historical_block_sample(web3, candidate_number, protocol=protocol)
        if candidate[1] <= target_timestamp:
            lower_number, lower_timestamp = candidate
        else:
            upper_number, upper_timestamp = candidate
    return lower_number, lower_timestamp


async def _historical_blocks_for_grid(
    web3: Any,
    *,
    start_ts: int,
    end_ts: int,
    interval_secs: int,
    protocol: str,
) -> list[tuple[int, int]]:
    """Resolve the at-or-before archive block for every requested sample."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    if start_ts > end_ts:
        raise RateHistoryUnavailable(protocol, f"start_ts must be <= end_ts (got {start_ts} > {end_ts})")
    if interval_secs <= 0:
        raise RateHistoryUnavailable(protocol, f"interval_secs must be > 0 (got {interval_secs})")

    genesis = await _historical_block_sample(web3, 0, protocol=protocol)
    latest = await _historical_block_sample(web3, "latest", protocol=protocol)
    if start_ts < genesis[1]:
        raise RateHistoryUnavailable(
            protocol,
            f"requested TWAP series starts before chain genesis ({start_ts} < {genesis[1]})",
        )
    if end_ts > latest[1]:
        raise RateHistoryUnavailable(
            protocol,
            f"requested TWAP series ends after the measured chain head ({end_ts} > {latest[1]})",
        )

    samples: list[tuple[int, int]] = []
    lower = genesis
    for target in range(start_ts, end_ts + 1, interval_secs):
        lower = await _block_at_or_before(
            web3,
            target,
            lower=lower,
            upper=latest,
            protocol=protocol,
        )
        samples.append(lower)
    return samples


async def fetch_v3_twap_series(
    servicer: Any,
    *,
    chain: str,
    pool_address: str,
    start_ts: int,
    end_ts: int,
    interval_secs: int,
    window_secs: int,
    protocol: str,
) -> list[Any]:
    """Read an exact-pool historical TWAP series from archive state.

    Each requested sample resolves to the latest block at or before its UTC
    timestamp and calls that pool's native ``observe([window_secs, 0])`` at
    the resolved block. The emitted timestamp is the selected block timestamp,
    not the requested grid boundary. Pool token identities and decimals are
    immutable V3 metadata, so they are measured once per series request and
    reused across samples. A duplicate selected block is observed once and
    re-expanded to its requested grid points; prices are never derived from
    token/USD ratios.
    """
    from almanak.gateway.services.rate_history_service import (
        DexTwapPoint,
        RateHistoryUnavailable,
    )

    if window_secs <= 0:
        raise RateHistoryUnavailable(protocol, f"window_secs must be > 0 (got {window_secs})")

    web3, pool_checksum = await _twap_resolve_web3_and_pool(
        servicer,
        chain,
        pool_address,
        protocol=protocol,
    )
    block_samples = await _historical_blocks_for_grid(
        web3,
        start_ts=start_ts,
        end_ts=end_ts,
        interval_secs=interval_secs,
        protocol=protocol,
    )
    # Multiple grid points may legitimately resolve to the same block on a
    # halted/low-cadence chain. Read it once; downstream staleness checks use
    # the actual block timestamp and will refuse an over-old observation.
    unique_blocks = list(dict.fromkeys(block_samples))
    if not unique_blocks:
        return []

    t0_decimals, t1_decimals = await _twap_resolve_pool_decimals(
        web3,
        pool_checksum,
        unique_blocks[-1][0],
        protocol=protocol,
        pool_address=pool_address,
    )

    async def observe(block_sample: tuple[int, int]) -> DexTwapPoint:
        block_number, block_timestamp = block_sample
        tick_cumulatives, _liquidity = await _twap_call_observe(
            web3,
            pool_checksum=pool_checksum,
            seconds_agos=[window_secs, 0],
            block_identifier=block_number,
            protocol=protocol,
            pool_address=pool_address,
        )
        tick_diff = tick_cumulatives[1] - tick_cumulatives[0]
        tick_twap = _twap_tick_from_cumulatives(tick_diff, window_secs)
        return DexTwapPoint(
            timestamp=block_timestamp,
            price=_tick_to_price(tick_twap, t0_decimals, t1_decimals),
            tick_observation_count=len(tick_cumulatives),
            block_number=block_number,
        )

    point_by_block: dict[tuple[int, int], DexTwapPoint] = {}
    # Bound archive-RPC concurrency. A one-year hourly run still performs one
    # real observe() per sample, but does not serialize thousands of round trips
    # or create an unbounded task fan-out.
    for offset in range(0, len(unique_blocks), 32):
        chunk = unique_blocks[offset : offset + 32]
        observed = await asyncio.gather(*(observe(sample) for sample in chunk))
        point_by_block.update(zip(chunk, observed, strict=True))

    # Preserve the requested grid cardinality. Multiple targets can resolve to
    # the same archive block when a chain stalls; re-expand the cached sample
    # instead of silently returning fewer observations than the caller asked
    # for. The duplicated point retains the real block timestamp.
    return [point_by_block[sample] for sample in block_samples]
