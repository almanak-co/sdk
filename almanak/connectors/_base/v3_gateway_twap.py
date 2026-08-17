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
import re
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

# observe(uint32[] secondsAgos) -> (int56[] tickCumulatives, uint160[] secondsPerLiquidityX128s)
_OBSERVE_SELECTOR = "883bdbfd"

# token0() / token1() — used for decimal-aware tick→price conversion.
_TOKEN0_SELECTOR = "0dfe1681"
_TOKEN1_SELECTOR = "d21220a7"

# ERC20 decimals() selector.
_DECIMALS_SELECTOR = "313ce567"
_SLOT0_SELECTOR = "3850c7bd"
_LIQUIDITY_SELECTOR = "1a686502"
_FEE_SELECTOR = "ddca3f43"
_BALANCE_OF_SELECTOR = "70a08231"


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
    sample_block: Callable[[int | str], Awaitable[tuple[int, int]]] | None = None,
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
        candidate = await (
            sample_block(candidate_number)
            if sample_block is not None
            else _historical_block_sample(web3, candidate_number, protocol=protocol)
        )
        if candidate[1] <= target_timestamp:
            lower_number, lower_timestamp = candidate
        else:
            upper_number, upper_timestamp = candidate
    return lower_number, lower_timestamp


class _HistoricalBlockResolver:
    """Request-scoped timestamp resolver with cached, tightly anchored searches.

    Historical series arrive in pages of hundreds of ordered timestamps.  A
    genesis/head search for every point is both needlessly expensive and, when
    performed serially, slower than the gateway RPC deadline on fast chains.
    Resolve the two page edges first, then recursively resolve midpoints inside
    their already-measured neighbours.  Midpoints at each tree depth are safe
    to resolve concurrently because their bounds are immutable and disjoint.
    """

    _CONCURRENCY = 32

    def __init__(self, web3: Any, *, protocol: str) -> None:
        self._web3 = web3
        self._protocol = protocol
        self._samples: dict[int | str, tuple[int, int]] = {}
        self._bounds: tuple[tuple[int, int], tuple[int, int]] | None = None

    async def sample(self, block_identifier: int | str) -> tuple[int, int]:
        cached = self._samples.get(block_identifier)
        if cached is not None:
            return cached
        measured = await _historical_block_sample(
            self._web3,
            block_identifier,
            protocol=self._protocol,
        )
        self._samples[block_identifier] = measured
        self._samples[measured[0]] = measured
        return measured

    async def bounds(self) -> tuple[tuple[int, int], tuple[int, int]]:
        if self._bounds is None:
            genesis, latest = await asyncio.gather(self.sample(0), self.sample("latest"))
            self._bounds = (genesis, latest)
        return self._bounds

    async def at_or_before(
        self,
        target_timestamp: int,
        *,
        lower: tuple[int, int],
        upper: tuple[int, int],
    ) -> tuple[int, int]:
        return await _block_at_or_before(
            self._web3,
            target_timestamp,
            lower=lower,
            upper=upper,
            protocol=self._protocol,
            sample_block=self.sample,
        )

    async def validate_window(
        self,
        *,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
    ) -> tuple[tuple[int, int], tuple[int, int]]:
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        if start_ts > end_ts:
            raise RateHistoryUnavailable(
                self._protocol,
                f"start_ts must be <= end_ts (got {start_ts} > {end_ts})",
            )
        if interval_secs <= 0:
            raise RateHistoryUnavailable(
                self._protocol,
                f"interval_secs must be > 0 (got {interval_secs})",
            )
        genesis, latest = await self.bounds()
        if start_ts < genesis[1]:
            raise RateHistoryUnavailable(
                self._protocol,
                f"requested historical series starts before chain genesis ({start_ts} < {genesis[1]})",
            )
        if end_ts > latest[1]:
            raise RateHistoryUnavailable(
                self._protocol,
                f"requested historical series ends after the measured chain head ({end_ts} > {latest[1]})",
            )
        return genesis, latest

    async def grid(
        self,
        *,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
        first_sample: tuple[int, int] | None = None,
    ) -> list[tuple[int, int]]:
        genesis, latest = await self.validate_window(
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
        )
        targets = list(range(start_ts, end_ts + 1, interval_secs))
        if not targets:
            return []

        resolved: dict[int, tuple[int, int]] = {}
        resolved[0] = first_sample or await self.at_or_before(
            targets[0],
            lower=genesis,
            upper=latest,
        )
        if len(targets) == 1:
            return [resolved[0]]
        resolved[len(targets) - 1] = await self.at_or_before(
            targets[-1],
            lower=resolved[0],
            upper=latest,
        )

        intervals = [(0, len(targets) - 1)]
        semaphore = asyncio.Semaphore(self._CONCURRENCY)
        while intervals:
            jobs: list[tuple[int, int, int]] = []
            next_intervals: list[tuple[int, int]] = []
            for lower_index, upper_index in intervals:
                if upper_index - lower_index <= 1:
                    continue
                lower_sample = resolved[lower_index]
                upper_sample = resolved[upper_index]
                if lower_sample == upper_sample:
                    for index in range(lower_index + 1, upper_index):
                        resolved[index] = lower_sample
                    continue
                midpoint = (lower_index + upper_index) // 2
                jobs.append((lower_index, midpoint, upper_index))

            async def resolve_midpoint(job: tuple[int, int, int]) -> tuple[int, tuple[int, int]]:
                lower_index, midpoint, upper_index = job
                async with semaphore:
                    sample = await self.at_or_before(
                        targets[midpoint],
                        lower=resolved[lower_index],
                        upper=resolved[upper_index],
                    )
                return midpoint, sample

            if jobs:
                measured = await asyncio.gather(*(resolve_midpoint(job) for job in jobs))
                for midpoint, sample in measured:
                    resolved[midpoint] = sample
                for lower_index, midpoint, upper_index in jobs:
                    next_intervals.extend(((lower_index, midpoint), (midpoint, upper_index)))
            intervals = next_intervals
        return [resolved[index] for index in range(len(targets))]


async def _historical_blocks_for_grid(
    web3: Any,
    *,
    start_ts: int,
    end_ts: int,
    interval_secs: int,
    protocol: str,
) -> list[tuple[int, int]]:
    """Resolve a complete archive grid with bounded, tightly anchored work."""
    return await _HistoricalBlockResolver(web3, protocol=protocol).grid(
        start_ts=start_ts,
        end_ts=end_ts,
        interval_secs=interval_secs,
    )


def _format_block_sample(sample: tuple[int, int]) -> str:
    block_number, timestamp = sample
    observed_at = datetime.fromtimestamp(timestamp, UTC).isoformat().replace("+00:00", "Z")
    return f"block {block_number} ({observed_at})"


def _has_contract_code(code: Any) -> bool:
    if isinstance(code, str):
        return bool(code.removeprefix("0x").strip("0"))
    return bool(bytes(code))


async def _pool_has_code(
    web3: Any,
    *,
    pool_checksum: str,
    block_number: int,
    protocol: str,
) -> bool:
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    try:
        return _has_contract_code(await web3.eth.get_code(pool_checksum, block_identifier=block_number))
    except Exception as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"failed to read deployment code for pool {pool_checksum!r} at block {block_number}: {exc}",
        ) from exc


async def _first_pool_code_block(
    resolver: _HistoricalBlockResolver,
    web3: Any,
    *,
    pool_checksum: str,
    absent_block: int,
    latest: tuple[int, int],
    protocol: str,
) -> tuple[int, int] | None:
    """Find the first block with pool bytecode after a measured absence."""
    if not await _pool_has_code(
        web3,
        pool_checksum=pool_checksum,
        block_number=latest[0],
        protocol=protocol,
    ):
        return None
    lower_number = absent_block
    upper_number = latest[0]
    while upper_number - lower_number > 1:
        candidate = (lower_number + upper_number) // 2
        if await _pool_has_code(
            web3,
            pool_checksum=pool_checksum,
            block_number=candidate,
            protocol=protocol,
        ):
            upper_number = candidate
        else:
            lower_number = candidate
    return await resolver.sample(upper_number)


async def _require_pool_at_series_start(
    resolver: _HistoricalBlockResolver,
    web3: Any,
    *,
    pool_checksum: str,
    start_sample: tuple[int, int],
    latest: tuple[int, int],
    protocol: str,
) -> None:
    """Fail before resolving a page when its first sample predates the pool."""
    if await _pool_has_code(
        web3,
        pool_checksum=pool_checksum,
        block_number=start_sample[0],
        protocol=protocol,
    ):
        return
    first_deployed = await _first_pool_code_block(
        resolver,
        web3,
        pool_checksum=pool_checksum,
        absent_block=start_sample[0],
        latest=latest,
        protocol=protocol,
    )
    if first_deployed is None:
        boundary = f"no contract code exists at measured chain head {_format_block_sample(latest)}"
    else:
        boundary = f"earliest pool deployment is {_format_block_sample(first_deployed)}"
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    raise RateHistoryUnavailable(
        protocol,
        f"pool {pool_checksum.lower()} is not deployed at requested start "
        f"{_format_block_sample(start_sample)}; {boundary}",
    )


def _is_twap_history_boundary(exc: BaseException) -> bool:
    """Return whether an observe failure denotes insufficient oracle history."""
    current: BaseException | None = exc
    while current is not None:
        message = str(current).upper()
        if (
            re.search(r"\bOLD\b", message)
            or "INSUFFICIENT OBSERVATION" in message
            or "OBSERVATION CARDINALITY" in message
        ):
            return True
        current = current.__cause__
    return False


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
    resolver = _HistoricalBlockResolver(web3, protocol=protocol)
    genesis, latest = await resolver.validate_window(
        start_ts=start_ts,
        end_ts=end_ts,
        interval_secs=interval_secs,
    )
    start_sample = await resolver.at_or_before(start_ts, lower=genesis, upper=latest)
    await _require_pool_at_series_start(
        resolver,
        web3,
        pool_checksum=pool_checksum,
        start_sample=start_sample,
        latest=latest,
        protocol=protocol,
    )
    try:
        start_cumulatives, _start_liquidity = await _twap_call_observe(
            web3,
            pool_checksum=pool_checksum,
            seconds_agos=[window_secs, 0],
            block_identifier=start_sample[0],
            protocol=protocol,
            pool_address=pool_address,
        )
    except RateHistoryUnavailable as exc:
        if not _is_twap_history_boundary(exc):
            raise
        raise RateHistoryUnavailable(
            protocol,
            f"native {window_secs}-second TWAP for pool {pool_checksum.lower()} is unavailable at requested start "
            f"{_format_block_sample(start_sample)}; the pool's native oracle does not contain the requested "
            "history at that verified block — choose a later start",
        ) from exc
    block_samples = await resolver.grid(
        start_ts=start_ts,
        end_ts=end_ts,
        interval_secs=interval_secs,
        first_sample=start_sample,
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
        if block_sample == start_sample:
            tick_cumulatives = start_cumulatives
        else:
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


async def fetch_v3_pool_state_series(
    servicer: Any,
    *,
    chain: str,
    pool_address: str,
    start_ts: int,
    end_ts: int,
    interval_secs: int,
    protocol: str,
    factory_address: str,
    factory_get_pool_selector: str = "0x1698ee82",
    factory_pool_key_selector: str = "0xddca3f43",
    factory_pool_key_signed: bool = False,
) -> list[Any]:
    """Read and factory-authenticate exact V3-compatible state.

    Canonical V3 factories key pools by ``fee()``. Some compatible families,
    notably Aerodrome Slipstream, key them by signed ``tickSpacing()`` and use
    a different factory selector. The connector supplies those two ABI facts;
    the archive state reader remains shared.
    """
    from almanak.gateway.services.rate_history_service import (
        DexPoolStatePoint,
        RateHistoryUnavailable,
    )

    normalized_factory = factory_address.strip().lower() if isinstance(factory_address, str) else ""
    if (
        len(normalized_factory) != 42
        or not normalized_factory.startswith("0x")
        or any(char not in "0123456789abcdef" for char in normalized_factory[2:])
    ):
        raise RateHistoryUnavailable(
            protocol,
            f"invalid or missing factory address for {protocol} on {chain!r}: {factory_address!r}",
        )

    web3, pool_checksum = await _twap_resolve_web3_and_pool(
        servicer,
        chain,
        pool_address,
        protocol=protocol,
    )
    resolver = _HistoricalBlockResolver(web3, protocol=protocol)
    genesis, latest = await resolver.validate_window(
        start_ts=start_ts,
        end_ts=end_ts,
        interval_secs=interval_secs,
    )
    start_sample = await resolver.at_or_before(start_ts, lower=genesis, upper=latest)
    await _require_pool_at_series_start(
        resolver,
        web3,
        pool_checksum=pool_checksum,
        start_sample=start_sample,
        latest=latest,
        protocol=protocol,
    )
    block_samples = await resolver.grid(
        start_ts=start_ts,
        end_ts=end_ts,
        interval_secs=interval_secs,
        first_sample=start_sample,
    )
    unique_blocks = list(dict.fromkeys(block_samples))
    if not unique_blocks:
        return []

    try:
        token0, token1, token0_decimals, token1_decimals = await _fetch_pool_tokens_and_decimals(
            web3,
            pool_checksum,
            unique_blocks[-1][0],
        )
        fee_raw = await web3.eth.call(
            {"to": pool_checksum, "data": f"0x{_FEE_SELECTOR}"},
            block_identifier=unique_blocks[-1][0],
        )
        if len(fee_raw) < 32:
            raise ValueError(f"fee() returned {len(fee_raw)} bytes; need 32")
        fee_tier = int.from_bytes(fee_raw[-32:], byteorder="big")

        normalized_key_selector = factory_pool_key_selector.removeprefix("0x").lower()
        if normalized_key_selector == _FEE_SELECTOR:
            pool_key_raw = fee_raw
        else:
            pool_key_raw = await web3.eth.call(
                {"to": pool_checksum, "data": f"0x{normalized_key_selector}"},
                block_identifier=unique_blocks[-1][0],
            )
            if len(pool_key_raw) < 32:
                raise ValueError(
                    f"pool key selector {factory_pool_key_selector} returned {len(pool_key_raw)} bytes; need 32"
                )
        factory_pool_key = int.from_bytes(
            pool_key_raw[-32:],
            byteorder="big",
            signed=factory_pool_key_signed,
        )
        from almanak.connectors._base.v3_pool_abi import encode_get_pool

        factory_checksum = web3.to_checksum_address(normalized_factory)
        canonical_raw = await web3.eth.call(
            {
                "to": factory_checksum,
                "data": encode_get_pool(factory_get_pool_selector, token0, token1, factory_pool_key),
            },
            block_identifier=unique_blocks[-1][0],
        )
        if len(canonical_raw) < 32:
            raise ValueError(f"factory getPool returned {len(canonical_raw)} bytes; need 32")
        canonical_pool = "0x" + bytes(canonical_raw)[-20:].hex()
        if canonical_pool.lower() != pool_checksum.lower():
            raise ValueError(f"registered factory returned {canonical_pool}, not requested pool {pool_address.lower()}")
    except Exception as exc:
        raise RateHistoryUnavailable(
            protocol,
            f"failed to read immutable pool identity for {pool_address!r}: {exc}",
        ) from exc

    balance0_call = "0x" + _BALANCE_OF_SELECTOR + pool_checksum.lower().removeprefix("0x").zfill(64)
    balance1_call = balance0_call

    async def read_state(block_sample: tuple[int, int]) -> DexPoolStatePoint:
        block_number, block_timestamp = block_sample
        try:
            slot0_raw, liquidity_raw, reserve0_raw, reserve1_raw = await asyncio.gather(
                web3.eth.call(
                    {"to": pool_checksum, "data": f"0x{_SLOT0_SELECTOR}"},
                    block_identifier=block_number,
                ),
                web3.eth.call(
                    {"to": pool_checksum, "data": f"0x{_LIQUIDITY_SELECTOR}"},
                    block_identifier=block_number,
                ),
                web3.eth.call(
                    {"to": web3.to_checksum_address(token0), "data": balance0_call},
                    block_identifier=block_number,
                ),
                web3.eth.call(
                    {"to": web3.to_checksum_address(token1), "data": balance1_call},
                    block_identifier=block_number,
                ),
            )
            if len(slot0_raw) < 64 or min(len(liquidity_raw), len(reserve0_raw), len(reserve1_raw)) < 32:
                raise ValueError("slot0/liquidity/balanceOf returned truncated data")
            sqrt_price_x96 = int.from_bytes(slot0_raw[0:32], byteorder="big")
            tick = int.from_bytes(slot0_raw[32:64], byteorder="big", signed=True)
            return DexPoolStatePoint(
                timestamp=block_timestamp,
                block_number=block_number,
                sqrt_price_x96=sqrt_price_x96,
                tick=tick,
                liquidity=int.from_bytes(liquidity_raw[-32:], byteorder="big"),
                token0=token0,
                token1=token1,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                fee_tier=fee_tier,
                reserve0_raw=int.from_bytes(reserve0_raw[-32:], byteorder="big"),
                reserve1_raw=int.from_bytes(reserve1_raw[-32:], byteorder="big"),
            )
        except Exception as exc:
            raise RateHistoryUnavailable(
                protocol,
                f"pool-state archive read failed for {pool_address!r} at block {block_number}: {exc}",
            ) from exc

    by_block: dict[tuple[int, int], DexPoolStatePoint] = {}
    for offset in range(0, len(unique_blocks), 32):
        chunk = unique_blocks[offset : offset + 32]
        points = await asyncio.gather(*(read_state(sample) for sample in chunk))
        by_block.update(zip(chunk, points, strict=True))
    return [by_block[sample] for sample in block_samples]
