"""Unit tests for ``MarketService.LookupV4PoolKey`` (VIB-4472 / T03).

Covers the three surfaces introduced by the ticket:

1. ``V4PoolKeyCache`` -- decode invariants, currency0 < currency1 enforcement,
   normalize / register / lookup hit-path.
2. ``MarketServiceServicer.LookupV4PoolKey`` -- input validation
   (INVALID_ARGUMENT), NOT_FOUND signalling, success-path PoolKey shape.
3. ``lookup_v4_pool_key`` framework client -- NOT_FOUND mapping to a typed
   exception and currency-order defence-in-depth.

Network access is mocked at the cache layer; no real RPC is made.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.connectors.uniswap_v4.gateway_pool_key_client import (
    V4PoolKeyNotFound,
    _coerce_pool_id_bytes,
    lookup_v4_pool_key,
    make_sync_pool_key_lookup,
)
from almanak.connectors.uniswap_v4.sdk import PoolKey as FrameworkPoolKey
from almanak.connectors.uniswap_v4.gateway.pool_key_cache import (
    INITIALIZE_EVENT_TOPIC,
    NO_HOOKS,
    CachedPoolKey,
    V4PoolKeyCache,
    _decode_initialize_log,
    _normalize_pool_id,
)
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer


# ----------------------------------------------------------------------------
# Test fixtures
# ----------------------------------------------------------------------------

# Real Base V4 example: USDC / WETH 0.05% pool (no hooks). Values are
# constructed from canonical addresses; sort order honoured.
_USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
_WETH_BASE = "0x4200000000000000000000000000000000000006"
# Sorted: WETH (0x42...) < USDC (0x83...)
_C0 = _WETH_BASE
_C1 = _USDC_BASE
_FEE = 500
_TICK_SPACING = 10
_HOOKS = NO_HOOKS
# Synthetic but valid 32-byte pool id (real keccak of the encoded key is
# not required for unit-level cache-shape tests; the gateway only asserts
# the shape, not the hash).
_POOL_ID_HEX = "ab" * 32
_POOL_ID_BYTES = bytes.fromhex(_POOL_ID_HEX)


class _AwaitableInt:
    """Awaitable that resolves to an int — emulates web3.py's
    ``AsyncWeb3.eth.block_number`` property which is a coroutine."""

    def __init__(self, value: int) -> None:
        self._value = value

    def __await__(self):
        async def _coro() -> int:
            return self._value

        return _coro().__await__()


def _make_initialize_log(
    *,
    pool_id_hex: str = _POOL_ID_HEX,
    c0: str = _C0,
    c1: str = _C1,
    fee: int = _FEE,
    tick_spacing: int = _TICK_SPACING,
    hooks: str = _HOOKS,
    sqrt_price_x96: int = 0,
    tick: int = 0,
) -> dict:
    """Build a PoolManager.Initialize log dict matching web3.py's get_logs shape."""
    # topics: [event_sig, pool_id, c0, c1] (c0/c1 left-padded to 32 bytes)
    topics = [
        INITIALIZE_EVENT_TOPIC,
        "0x" + pool_id_hex,
        "0x" + c0[2:].rjust(64, "0"),
        "0x" + c1[2:].rjust(64, "0"),
    ]
    # data: fee (uint24 in 32 bytes) + tickSpacing (int24 in 32 bytes,
    # two's complement on 256 bits) + hooks (address in 32 bytes) +
    # sqrtPriceX96 (uint160 in 32 bytes) + tick (int24 in 32 bytes).
    def _i256(x: int) -> str:
        if x < 0:
            x += 1 << 256
        return f"{x:064x}"

    data = (
        _i256(fee)
        + _i256(tick_spacing)
        + hooks[2:].rjust(64, "0")
        + _i256(sqrt_price_x96)
        + _i256(tick)
    )
    return {"topics": topics, "data": "0x" + data}


# ----------------------------------------------------------------------------
# CachedPoolKey invariants
# ----------------------------------------------------------------------------


class TestCachedPoolKeyInvariants:
    def test_constructs_with_sorted_currencies(self) -> None:
        pk = CachedPoolKey(
            currency0=_C0,
            currency1=_C1,
            fee=_FEE,
            tick_spacing=_TICK_SPACING,
            hooks=_HOOKS,
        )
        assert int(pk.currency0, 16) < int(pk.currency1, 16)

    def test_rejects_unsorted_currencies(self) -> None:
        with pytest.raises(ValueError, match="currency0 must be < currency1"):
            CachedPoolKey(
                currency0=_USDC_BASE,  # 0x83... > WETH
                currency1=_WETH_BASE,
                fee=_FEE,
                tick_spacing=_TICK_SPACING,
                hooks=_HOOKS,
            )

    def test_rejects_equal_currencies(self) -> None:
        with pytest.raises(ValueError, match="currency0 must be < currency1"):
            CachedPoolKey(
                currency0=_C0,
                currency1=_C0,
                fee=_FEE,
                tick_spacing=_TICK_SPACING,
                hooks=_HOOKS,
            )

    @pytest.mark.parametrize("fee", [-1, 1 << 24, 1 << 32])
    def test_rejects_fee_out_of_uint24(self, fee: int) -> None:
        with pytest.raises(ValueError, match="fee out of uint24"):
            CachedPoolKey(
                currency0=_C0,
                currency1=_C1,
                fee=fee,
                tick_spacing=_TICK_SPACING,
                hooks=_HOOKS,
            )

    @pytest.mark.parametrize("ts", [-(1 << 23) - 1, 1 << 23, 1 << 30])
    def test_rejects_tick_spacing_out_of_int24(self, ts: int) -> None:
        with pytest.raises(ValueError, match="tick_spacing out of int24"):
            CachedPoolKey(
                currency0=_C0,
                currency1=_C1,
                fee=_FEE,
                tick_spacing=ts,
                hooks=_HOOKS,
            )


# ----------------------------------------------------------------------------
# Pool-id normalisation
# ----------------------------------------------------------------------------


class TestNormalizePoolId:
    def test_bytes_passthrough(self) -> None:
        assert _normalize_pool_id(_POOL_ID_BYTES) == "0x" + _POOL_ID_HEX

    def test_hex_with_prefix(self) -> None:
        assert _normalize_pool_id("0x" + _POOL_ID_HEX.upper()) == "0x" + _POOL_ID_HEX

    def test_hex_without_prefix(self) -> None:
        assert _normalize_pool_id(_POOL_ID_HEX.upper()) == "0x" + _POOL_ID_HEX

    def test_rejects_short_bytes(self) -> None:
        with pytest.raises(ValueError):
            _normalize_pool_id(b"\x00" * 31)

    def test_rejects_long_bytes(self) -> None:
        with pytest.raises(ValueError):
            _normalize_pool_id(b"\x00" * 33)

    def test_rejects_short_hex(self) -> None:
        with pytest.raises(ValueError):
            _normalize_pool_id("0xab")

    def test_rejects_non_hex(self) -> None:
        with pytest.raises(ValueError):
            _normalize_pool_id("z" * 64)

    def test_rejects_wrong_type(self) -> None:
        with pytest.raises(TypeError):
            _normalize_pool_id(12345)  # type: ignore[arg-type]


# ----------------------------------------------------------------------------
# Initialize-log decoder
# ----------------------------------------------------------------------------


class TestDecodeInitializeLog:
    def test_happy_path(self) -> None:
        log = _make_initialize_log()
        result = _decode_initialize_log(log)
        assert result is not None
        pid, key = result
        assert pid == "0x" + _POOL_ID_HEX
        assert key.currency0 == _C0
        assert key.currency1 == _C1
        assert key.fee == _FEE
        assert key.tick_spacing == _TICK_SPACING
        assert key.hooks == _HOOKS

    def test_negative_tick_spacing_decoded(self) -> None:
        log = _make_initialize_log(tick_spacing=-60)
        result = _decode_initialize_log(log)
        assert result is not None
        _, key = result
        assert key.tick_spacing == -60

    def test_wrong_topic0_returns_none(self) -> None:
        log = _make_initialize_log()
        log["topics"][0] = "0x" + "00" * 32
        assert _decode_initialize_log(log) is None

    def test_too_few_topics_returns_none(self) -> None:
        log = _make_initialize_log()
        log["topics"] = log["topics"][:2]
        assert _decode_initialize_log(log) is None

    def test_truncated_data_returns_none(self) -> None:
        log = _make_initialize_log()
        log["data"] = log["data"][:50]  # well under 5*64 hex
        assert _decode_initialize_log(log) is None

    def test_unsorted_pair_in_log_returns_none(self) -> None:
        """If a malformed log carries c0>c1 the decoder rejects it via
        CachedPoolKey.__post_init__ rather than poisoning the cache."""
        log = _make_initialize_log(c0=_USDC_BASE, c1=_WETH_BASE)
        assert _decode_initialize_log(log) is None


# ----------------------------------------------------------------------------
# V4PoolKeyCache register / lookup
# ----------------------------------------------------------------------------


class TestV4PoolKeyCache:
    def test_register_and_known_count(self) -> None:
        cache = V4PoolKeyCache()
        key = CachedPoolKey(
            currency0=_C0,
            currency1=_C1,
            fee=_FEE,
            tick_spacing=_TICK_SPACING,
            hooks=_HOOKS,
        )
        cache.register("base", _POOL_ID_BYTES, key)
        assert cache.known_pool_count("base") == 1
        assert cache.known_pool_count("BASE") == 1  # case-insensitive
        assert cache.known_pool_count("ethereum") == 0

    @pytest.mark.asyncio
    async def test_lookup_hit_bypasses_network(self) -> None:
        cache = V4PoolKeyCache()
        key = CachedPoolKey(
            currency0=_C0,
            currency1=_C1,
            fee=_FEE,
            tick_spacing=_TICK_SPACING,
            hooks=_HOOKS,
        )
        cache.register("base", _POOL_ID_BYTES, key)
        # Patch _refresh_chain to fail loudly if a hit accidentally triggers
        # a network call.
        with patch.object(
            cache, "_refresh_chain", side_effect=AssertionError("hit must not refresh")
        ):
            result = await cache.lookup("base", _POOL_ID_BYTES)
        assert result == key

    @pytest.mark.asyncio
    async def test_lookup_miss_triggers_refresh_then_returns_none(self) -> None:
        cache = V4PoolKeyCache()
        refresh = AsyncMock()
        with patch.object(cache, "_refresh_chain", refresh):
            result = await cache.lookup("base", _POOL_ID_BYTES)
        assert result is None
        refresh.assert_awaited_once_with("base", target_pool_id="0x" + _POOL_ID_HEX)

    @pytest.mark.asyncio
    async def test_lookup_miss_then_populated_in_refresh(self) -> None:
        cache = V4PoolKeyCache()
        key = CachedPoolKey(
            currency0=_C0,
            currency1=_C1,
            fee=_FEE,
            tick_spacing=_TICK_SPACING,
            hooks=_HOOKS,
        )

        async def fake_refresh(chain: str, *, target_pool_id: str | None = None) -> None:
            cache.register(chain, _POOL_ID_BYTES, key)

        with patch.object(cache, "_refresh_chain", fake_refresh):
            result = await cache.lookup("base", _POOL_ID_BYTES)
        assert result == key

    @pytest.mark.asyncio
    async def test_lookup_invalid_pool_id_returns_none(self) -> None:
        cache = V4PoolKeyCache()
        # 31-byte input is malformed: do NOT call refresh, just None.
        with patch.object(cache, "_refresh_chain", side_effect=AssertionError("no refresh")):
            assert await cache.lookup("base", b"\x00" * 31) is None

    @pytest.mark.asyncio
    async def test_populate_from_logs_ingests_initialize(self) -> None:
        cache = V4PoolKeyCache()
        w3 = MagicMock()
        w3.eth.get_logs = AsyncMock(return_value=[_make_initialize_log()])
        added = await cache.populate_from_logs(
            chain="base",
            w3=w3,
            pool_manager="0x498581fF718922c3f8e6A244956aF099B2652b2b",
            from_block=0,
            to_block=100,
        )
        assert added == 1
        result = cache._index["base"].get("0x" + _POOL_ID_HEX)
        assert result is not None
        assert result.fee == _FEE

    @pytest.mark.asyncio
    async def test_populate_from_logs_dedupes(self) -> None:
        cache = V4PoolKeyCache()
        w3 = MagicMock()
        # Same log twice in the same batch.
        w3.eth.get_logs = AsyncMock(return_value=[_make_initialize_log(), _make_initialize_log()])
        added = await cache.populate_from_logs(
            chain="base",
            w3=w3,
            pool_manager="0x498581fF718922c3f8e6A244956aF099B2652b2b",
            from_block=0,
            to_block=100,
        )
        assert added == 1

    @pytest.mark.asyncio
    async def test_populate_from_logs_bisects_on_response_size_error(self) -> None:
        """VIB-4478 — Alchemy and similar providers cap eth_getLogs response
        size at fewer than 50k blocks of Initialize logs on busy chains. The
        cache must self-bisect the request window rather than dropping the
        scan (which corrupts LP_CLOSE attribution downstream).

        Pre-fix: the single-window request failed with the provider's
        ``Log response size exceeded`` error and ``populate_from_logs``
        returned ``None``; LP_CLOSE receipts then dropped with
        ``pool_key_lookup_error`` and produced ``amount0=None, amount1=None``
        accounting events. Caught by the lp_v4 fixture's first Anvil-Base
        E2E run.
        """
        cache = V4PoolKeyCache()
        # Simulate: first call (full 50k window) raises; halves succeed.
        call_log: list[tuple[int, int]] = []

        async def fake_get_logs(params: dict) -> list[dict]:
            lo, hi = params["fromBlock"], params["toBlock"]
            call_log.append((lo, hi))
            span = hi - lo + 1
            # Provider rejects anything larger than 25k blocks.
            if span > 25_000:
                raise RuntimeError(
                    "Log response size exceeded. ... should work: ["
                    f"{hex(lo)}, {hex(lo + 25_000)}]"
                )
            # Only the second half carries the target log; verifies that
            # bisection preserves the full range.
            if lo >= 25_000:
                return [_make_initialize_log()]
            return []

        w3 = MagicMock()
        w3.eth.get_logs = AsyncMock(side_effect=fake_get_logs)
        added = await cache.populate_from_logs(
            chain="base",
            w3=w3,
            pool_manager="0x498581fF718922c3f8e6A244956aF099B2652b2b",
            from_block=0,
            to_block=49_999,  # 50k blocks
        )
        # The pool from the second half must be ingested.
        assert added == 1, f"expected 1 added; got {added}; calls={call_log}"
        # First call must be the full window (proves we tried unbisected first).
        assert call_log[0] == (0, 49_999)
        # Subsequent calls must be smaller and cover the full range.
        smaller = [c for c in call_log[1:] if c[1] - c[0] + 1 <= 25_000]
        assert smaller, f"no bisection observed; calls={call_log}"
        covered = sorted(smaller)
        assert covered[0][0] == 0 and covered[-1][1] == 49_999, (
            f"bisection lost coverage: {covered}"
        )

    @pytest.mark.asyncio
    async def test_populate_from_logs_returns_none_at_min_chunk(self) -> None:
        """Bisection stops at the min chunk size; persistent failure at that
        granularity is a real transport error and must NOT be silently
        treated as 'empty range'. Returning None preserves the
        ``_last_scanned_block`` watermark so the next refresh retries.
        """
        cache = V4PoolKeyCache()
        w3 = MagicMock()
        # Every call fails — bisection eventually reaches min_chunk_blocks
        # and gives up.
        w3.eth.get_logs = AsyncMock(side_effect=RuntimeError("upstream-down"))
        added = await cache.populate_from_logs(
            chain="base",
            w3=w3,
            pool_manager="0x498581fF718922c3f8e6A244956aF099B2652b2b",
            from_block=0,
            to_block=500,  # below default min_chunk_blocks=1000, so first failure -> None
        )
        assert added is None

    @pytest.mark.asyncio
    async def test_refresh_chain_walks_backward_when_target_missing(self) -> None:
        """VIB-4426 — pools initialized > DEFAULT_BACKFILL_BLOCKS ago must be
        recoverable via the historical-expansion pass. Pre-fix: a pool whose
        Initialize log lived only in the second historical window was permanently
        unresolvable because ``_refresh_chain`` only ever scanned ``[head-50k, head]``."""
        cache = V4PoolKeyCache(historical_window=10, max_historical_blocks=100, backfill_blocks=10)
        # Forward tail (blocks 90..100) carries an unrelated pool only;
        # historical window (80..89) carries the lookup target.
        other_pool = "0x" + "cd" * 32
        target_pool = "0x" + _POOL_ID_HEX

        forward_log = _make_initialize_log(pool_id_hex="cd" * 32)
        target_log = _make_initialize_log(pool_id_hex=_POOL_ID_HEX)

        ranges_called: list[tuple[int, int]] = []

        async def fake_get_logs(params: dict) -> list[dict]:
            ranges_called.append((params["fromBlock"], params["toBlock"]))
            # Forward tail returns the unrelated pool, historical returns target.
            if params["fromBlock"] >= 90:
                return [forward_log]
            return [target_log]

        w3 = MagicMock()
        w3.eth.get_logs = AsyncMock(side_effect=fake_get_logs)
        w3.eth.block_number = AsyncMock(return_value=100)
        # mypy: block_number is an async property; AsyncMock works either way here.
        type(w3.eth).block_number = 100  # awaitable handled below via __aiter__-ish stub

        with (
            patch.object(cache, "_get_or_create_web3", return_value=w3),
            patch("almanak.connectors.uniswap_v4.gateway.pool_key_cache.UNISWAP_V4", {"base": {"pool_manager": "0x498581fF718922c3f8e6A244956aF099B2652b2b"}}),
        ):
            # web3.py exposes block_number as an awaitable property; emulate that.
            w3.eth.block_number = _AwaitableInt(100)
            result = await cache.lookup("base", _POOL_ID_BYTES)

        assert result is not None, "historical expansion must recover the older pool"
        assert other_pool in cache._index["base"]
        assert target_pool in cache._index["base"]
        # First call: forward tail starts at ``max(0, head - backfill_blocks)`` = 90.
        # Second call: historical expansion below the now-recorded earliest=90.
        assert any(r[0] == 90 and r[1] == 100 for r in ranges_called), ranges_called
        assert any(r[0] == 80 and r[1] == 89 for r in ranges_called), ranges_called

    @pytest.mark.asyncio
    async def test_refresh_chain_stops_at_historical_floor(self) -> None:
        """Historical expansion must not exceed ``max_historical_blocks`` —
        prevents a rogue lookup from DoS-ing the upstream archive node."""
        cache = V4PoolKeyCache(
            historical_window=10, max_historical_blocks=20, backfill_blocks=10
        )
        # Seed earliest watermark already at the floor; expansion must be a no-op.
        cache._last_scanned_block["base"] = 100
        cache._earliest_scanned_block["base"] = 80  # head - max_historical = 80

        w3 = MagicMock()
        w3.eth.block_number = _AwaitableInt(100)
        w3.eth.get_logs = AsyncMock(return_value=[])

        with (
            patch.object(cache, "_get_or_create_web3", return_value=w3),
            patch(
                "almanak.connectors.uniswap_v4.gateway.pool_key_cache.UNISWAP_V4",
                {"base": {"pool_manager": "0x498581fF718922c3f8e6A244956aF099B2652b2b"}},
            ),
        ):
            result = await cache.lookup("base", _POOL_ID_BYTES)

        assert result is None
        # get_logs is never called because the forward tail is empty (last+1 > head)
        # AND the historical floor blocks the backward pass.
        assert w3.eth.get_logs.await_count == 0

    @pytest.mark.asyncio
    async def test_refresh_chain_reuses_web3_client_across_calls(self) -> None:
        """VIB-4426 — AsyncWeb3 instantiated once per chain, reused across
        every refresh. Pre-fix the SSL handshake and connection pool were
        rebuilt on every lookup miss."""
        cache = V4PoolKeyCache(historical_window=10, max_historical_blocks=20, backfill_blocks=10)

        w3 = MagicMock()
        w3.eth.block_number = _AwaitableInt(100)
        w3.eth.get_logs = AsyncMock(return_value=[])

        call_count = 0

        def fake_resolver(chain: str, network: str) -> str:
            nonlocal call_count
            call_count += 1
            return "https://example.invalid/rpc"

        cache._rpc_url_resolver = fake_resolver  # type: ignore[method-assign]

        with (
            patch(
                "almanak.connectors.uniswap_v4.gateway.pool_key_cache.AsyncWeb3",
                return_value=w3,
            ) as web3_ctor,
            patch(
                "almanak.connectors.uniswap_v4.gateway.pool_key_cache.UNISWAP_V4",
                {"base": {"pool_manager": "0x498581fF718922c3f8e6A244956aF099B2652b2b"}},
            ),
        ):
            await cache.lookup("base", _POOL_ID_BYTES)
            await cache.lookup("base", _POOL_ID_BYTES)
            await cache.lookup("base", _POOL_ID_BYTES)

        # Three lookups, but AsyncWeb3 was constructed exactly once.
        assert web3_ctor.call_count == 1, (
            f"AsyncWeb3 reinstantiated on every lookup; expected 1, got {web3_ctor.call_count}"
        )

    @pytest.mark.asyncio
    async def test_populate_swallows_rpc_error_returns_none(self) -> None:
        """RPC failure returns None (sentinel for 'do not advance watermark')
        so the caller in ensure_chain_synced doesn't silently skip the failed
        range. Empty success still returns 0 — those two cases MUST stay
        distinguishable."""
        cache = V4PoolKeyCache()
        w3 = MagicMock()
        w3.eth.get_logs = AsyncMock(side_effect=RuntimeError("rpc down"))
        added = await cache.populate_from_logs(
            chain="base",
            w3=w3,
            pool_manager="0x498581fF718922c3f8e6A244956aF099B2652b2b",
            from_block=0,
            to_block=100,
        )
        assert added is None
        assert cache.known_pool_count("base") == 0


# ----------------------------------------------------------------------------
# MarketServiceServicer.LookupV4PoolKey
# ----------------------------------------------------------------------------


def _make_servicer() -> MarketServiceServicer:
    settings = MagicMock()
    settings.network = "mainnet"
    settings.chains = ["base"]
    settings.coingecko_api_key = ""
    return MarketServiceServicer(settings)


def _make_context() -> MagicMock:
    ctx = MagicMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


class TestLookupV4PoolKeyHandler:
    @pytest.mark.asyncio
    async def test_rejects_short_pool_id(self) -> None:
        servicer = _make_servicer()
        ctx = _make_context()
        request = gateway_pb2.LookupV4PoolKeyRequest(pool_id=b"\x00" * 31, chain="base")
        resp = await servicer.LookupV4PoolKey(request, ctx)
        assert resp.pool_key.currency0 == ""  # empty body
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_rejects_empty_pool_id(self) -> None:
        servicer = _make_servicer()
        ctx = _make_context()
        request = gateway_pb2.LookupV4PoolKeyRequest(pool_id=b"", chain="base")
        resp = await servicer.LookupV4PoolKey(request, ctx)
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert resp.pool_key.currency0 == ""

    @pytest.mark.asyncio
    async def test_rejects_missing_chain(self) -> None:
        servicer = _make_servicer()
        ctx = _make_context()
        request = gateway_pb2.LookupV4PoolKeyRequest(pool_id=_POOL_ID_BYTES, chain="")
        resp = await servicer.LookupV4PoolKey(request, ctx)
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert resp.pool_key.currency0 == ""

    @pytest.mark.asyncio
    async def test_rejects_invalid_chain(self) -> None:
        servicer = _make_servicer()
        ctx = _make_context()
        request = gateway_pb2.LookupV4PoolKeyRequest(
            pool_id=_POOL_ID_BYTES, chain="not-a-chain"
        )
        resp = await servicer.LookupV4PoolKey(request, ctx)
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert resp.pool_key.currency0 == ""

    @pytest.mark.asyncio
    async def test_cache_hit_returns_pool_key(self) -> None:
        servicer = _make_servicer()
        ctx = _make_context()
        # Pre-seed the cache; bypass the lazy constructor.
        cache = V4PoolKeyCache()
        cache.register(
            "base",
            _POOL_ID_BYTES,
            CachedPoolKey(
                currency0=_C0,
                currency1=_C1,
                fee=_FEE,
                tick_spacing=_TICK_SPACING,
                hooks=_HOOKS,
            ),
        )
        servicer._pool_key_cache = cache

        request = gateway_pb2.LookupV4PoolKeyRequest(
            pool_id=_POOL_ID_BYTES, chain="base"
        )
        resp = await servicer.LookupV4PoolKey(request, ctx)

        ctx.set_code.assert_not_called()
        assert resp.chain == "base"
        assert resp.pool_key.currency0 == _C0
        assert resp.pool_key.currency1 == _C1
        assert resp.pool_key.fee == _FEE
        assert resp.pool_key.tick_spacing == _TICK_SPACING
        assert resp.pool_key.hooks == _HOOKS

    @pytest.mark.asyncio
    async def test_cache_miss_returns_not_found(self) -> None:
        servicer = _make_servicer()
        ctx = _make_context()
        cache = V4PoolKeyCache()
        # No registration; lookup() will call _refresh_chain — stub it.
        with patch.object(cache, "_refresh_chain", AsyncMock()):
            servicer._pool_key_cache = cache
            request = gateway_pb2.LookupV4PoolKeyRequest(
                pool_id=_POOL_ID_BYTES, chain="base"
            )
            resp = await servicer.LookupV4PoolKey(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)
        assert resp.pool_key.currency0 == ""  # empty body

    @pytest.mark.asyncio
    async def test_failed_precondition_when_no_rpc_url_configured(self) -> None:
        """VIB-4426 P1 #2 — refresh-time config failure (no RPC URL) must
        surface as FAILED_PRECONDITION, not NOT_FOUND. Pre-fix the operator
        chasing a missing-pool counter could not tell whether the gateway
        was misconfigured or the pool genuinely didn't exist on-chain."""
        from almanak.connectors._base.gateway_capabilities import PoolKeyCacheError

        servicer = _make_servicer()
        ctx = _make_context()
        cache = MagicMock()
        cache.lookup = AsyncMock(
            side_effect=PoolKeyCacheError(
                "no RPC URL configured for chain=base", code="failed_precondition"
            )
        )
        servicer._pool_key_cache = cache

        request = gateway_pb2.LookupV4PoolKeyRequest(pool_id=_POOL_ID_BYTES, chain="base")
        resp = await servicer.LookupV4PoolKey(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.FAILED_PRECONDITION)
        assert resp.pool_key.currency0 == ""
        details = ctx.set_details.call_args.args[0]
        assert "not configured" in details.lower(), details

    @pytest.mark.asyncio
    async def test_unavailable_when_upstream_rpc_fails(self) -> None:
        """VIB-4426 P1 #2 — refresh-time upstream RPC failure
        (eth_blockNumber / eth_getLogs) must surface as UNAVAILABLE so
        operators see the right signal for a transient/upstream issue."""
        from almanak.connectors._base.gateway_capabilities import PoolKeyCacheError

        servicer = _make_servicer()
        ctx = _make_context()
        cache = MagicMock()
        cache.lookup = AsyncMock(
            side_effect=PoolKeyCacheError(
                "eth_blockNumber failed for chain=base: rpc down",
                code="unavailable",
            )
        )
        servicer._pool_key_cache = cache

        request = gateway_pb2.LookupV4PoolKeyRequest(pool_id=_POOL_ID_BYTES, chain="base")
        resp = await servicer.LookupV4PoolKey(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.UNAVAILABLE)
        assert resp.pool_key.currency0 == ""
        details = ctx.set_details.call_args.args[0]
        assert "temporarily unavailable" in details.lower(), details
        # Should NOT echo raw upstream exception detail across the boundary.
        assert "rpc down" not in details, details

    @pytest.mark.asyncio
    async def test_internal_error_path(self) -> None:
        servicer = _make_servicer()
        ctx = _make_context()
        cache = MagicMock()
        # Exception text below contains a path/URL-like fragment to prove the
        # gateway does NOT echo it back to the client.
        cache.lookup = AsyncMock(
            side_effect=RuntimeError(
                "kaboom: ssl handshake to https://archive.internal/rpc/secret-token failed"
            )
        )
        servicer._pool_key_cache = cache

        request = gateway_pb2.LookupV4PoolKeyRequest(
            pool_id=_POOL_ID_BYTES, chain="base"
        )
        resp = await servicer.LookupV4PoolKey(request, ctx)
        ctx.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        assert resp.pool_key.currency0 == ""
        # VIB-4426 — gRPC details MUST NOT echo the raw backend exception
        # text. Server-side log gets the full diagnostic; the client gets
        # a generic message.
        details_arg = ctx.set_details.call_args.args[0]
        assert "kaboom" not in details_arg, details_arg
        assert "archive.internal" not in details_arg, details_arg
        assert "secret-token" not in details_arg, details_arg
        assert "gateway logs" in details_arg.lower(), details_arg


# ----------------------------------------------------------------------------
# Framework client (lookup_v4_pool_key)
# ----------------------------------------------------------------------------


class _StubRpcError(grpc.RpcError):
    """Bare-bones gRPC error stand-in carrying a status code."""

    def __init__(self, code: grpc.StatusCode, details: str = "") -> None:
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:  # type: ignore[override]
        return self._code

    def details(self) -> str:  # type: ignore[override]
        return self._details


class TestFrameworkClient:
    def test_coerce_pool_id_bytes(self) -> None:
        assert _coerce_pool_id_bytes(_POOL_ID_BYTES) == _POOL_ID_BYTES
        assert _coerce_pool_id_bytes("0x" + _POOL_ID_HEX) == _POOL_ID_BYTES
        assert _coerce_pool_id_bytes(_POOL_ID_HEX.upper()) == _POOL_ID_BYTES

    def test_coerce_pool_id_rejects_short(self) -> None:
        with pytest.raises(ValueError):
            _coerce_pool_id_bytes("0xab")

    def test_coerce_pool_id_rejects_type(self) -> None:
        with pytest.raises(TypeError):
            _coerce_pool_id_bytes(42)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_decodes_response(self) -> None:
        client = MagicMock()
        client.market.LookupV4PoolKey = MagicMock(
            return_value=gateway_pb2.LookupV4PoolKeyResponse(
                pool_key=gateway_pb2.PoolKey(
                    currency0=_C0,
                    currency1=_C1,
                    fee=_FEE,
                    tick_spacing=_TICK_SPACING,
                    hooks=_HOOKS,
                ),
                chain="base",
            )
        )

        pk = await lookup_v4_pool_key(client, pool_id=_POOL_ID_BYTES, chain="base")
        assert pk.currency0 == _C0
        assert pk.currency1 == _C1
        assert pk.fee == _FEE
        assert pk.tick_spacing == _TICK_SPACING
        assert pk.hooks == _HOOKS

    @pytest.mark.asyncio
    async def test_not_found_raises_typed(self) -> None:
        client = MagicMock()
        client.market.LookupV4PoolKey = MagicMock(
            side_effect=_StubRpcError(grpc.StatusCode.NOT_FOUND, "missing")
        )
        with pytest.raises(V4PoolKeyNotFound) as exc_info:
            await lookup_v4_pool_key(client, pool_id=_POOL_ID_BYTES, chain="base")
        assert exc_info.value.chain == "base"
        assert exc_info.value.pool_id_hex == "0x" + _POOL_ID_HEX

    @pytest.mark.asyncio
    async def test_other_rpc_error_propagates(self) -> None:
        client = MagicMock()
        client.market.LookupV4PoolKey = MagicMock(
            side_effect=_StubRpcError(grpc.StatusCode.UNAVAILABLE, "gateway down")
        )
        with pytest.raises(grpc.RpcError):
            await lookup_v4_pool_key(client, pool_id=_POOL_ID_BYTES, chain="base")

    @pytest.mark.asyncio
    async def test_rejects_unsorted_response(self) -> None:
        """If the gateway buggily returns currency0 > currency1 we refuse it."""
        client = MagicMock()
        client.market.LookupV4PoolKey = MagicMock(
            return_value=gateway_pb2.LookupV4PoolKeyResponse(
                pool_key=gateway_pb2.PoolKey(
                    currency0=_USDC_BASE,  # > WETH
                    currency1=_WETH_BASE,
                    fee=_FEE,
                    tick_spacing=_TICK_SPACING,
                    hooks=_HOOKS,
                ),
                chain="base",
            )
        )
        with pytest.raises(ValueError, match="unsorted PoolKey"):
            await lookup_v4_pool_key(client, pool_id=_POOL_ID_BYTES, chain="base")


# ============================================================================
# VIB-4477 (T08): sync-from-async bridge for the ResultEnricher pipeline
# ============================================================================


class TestMakeSyncPoolKeyLookup:
    """The sync bridge wraps the async ``lookup_v4_pool_key`` so the V4
    receipt parser can call it from the sync ResultEnricher pipeline.
    """

    def test_returns_pool_key_on_success(self) -> None:
        client = MagicMock()
        client.market.LookupV4PoolKey = MagicMock(
            return_value=gateway_pb2.LookupV4PoolKeyResponse(
                pool_key=gateway_pb2.PoolKey(
                    currency0=_C0,
                    currency1=_C1,
                    fee=_FEE,
                    tick_spacing=_TICK_SPACING,
                    hooks=_HOOKS,
                ),
                chain="base",
            )
        )
        lookup = make_sync_pool_key_lookup(client)
        pk = lookup("0x" + _POOL_ID_HEX, "base")
        assert pk is not None
        assert pk.currency0 == _C0
        assert pk.currency1 == _C1
        assert pk.fee == _FEE

    def test_not_found_returns_none(self) -> None:
        """The parser's contract: V4PoolKeyNotFound -> None so the parser's
        outer except treats it as a benign 'pool_key_not_found' drop rather
        than a structured error."""
        client = MagicMock()
        client.market.LookupV4PoolKey = MagicMock(
            side_effect=_StubRpcError(grpc.StatusCode.NOT_FOUND, "missing")
        )
        lookup = make_sync_pool_key_lookup(client)
        assert lookup("0x" + _POOL_ID_HEX, "base") is None

    def test_other_rpc_error_propagates(self) -> None:
        """Non-NOT_FOUND failures must propagate so the parser logs them as
        ``pool_key_lookup_error`` (structured warning + None return)."""
        client = MagicMock()
        client.market.LookupV4PoolKey = MagicMock(
            side_effect=_StubRpcError(grpc.StatusCode.UNAVAILABLE, "gateway down")
        )
        lookup = make_sync_pool_key_lookup(client)
        with pytest.raises(grpc.RpcError):
            lookup("0x" + _POOL_ID_HEX, "base")

    def test_called_from_running_loop_succeeds_via_worker_thread(self) -> None:
        """VIB-4426 P1 #1 — the bridge MUST work from inside a running event
        loop. Pre-fix, ``asyncio.run`` raised ``RuntimeError("cannot be
        called from a running event loop")`` because the production runners
        (``strategy_runner._single_chain_handle_success``,
        ``teardown_commit.commit_teardown_intent``, ``inner_runner.execute_intent``)
        all invoke ``ResultEnricher.enrich`` from ``async def`` coroutines.
        The parser swallowed that as ``pool_key_lookup_error`` and V4
        LP_CLOSE accounting silently dropped on the live path.

        Post-fix: the bridge dispatches to a worker thread whose own scope
        has no running loop, so ``asyncio.run`` succeeds there and the
        calling coroutine blocks until the worker returns.
        """
        import asyncio

        client = MagicMock()
        client.market.LookupV4PoolKey = MagicMock(
            return_value=gateway_pb2.LookupV4PoolKeyResponse(
                pool_key=gateway_pb2.PoolKey(
                    currency0=_C0,
                    currency1=_C1,
                    fee=_FEE,
                    tick_spacing=_TICK_SPACING,
                    hooks=_HOOKS,
                ),
                chain="base",
            )
        )
        lookup = make_sync_pool_key_lookup(client)

        async def _runner() -> FrameworkPoolKey | None:
            # If the bridge raises RuntimeError on running-loop calls, the
            # parser swallows it as pool_key_lookup_error and V4 LP_CLOSE
            # accounting silently drops in production. Asserting None
            # (NOT_FOUND mapping) is OK; asserting RuntimeError is NOT OK.
            return lookup("0x" + _POOL_ID_HEX, "base")

        result = asyncio.run(_runner())
        assert result is not None, (
            "running-loop path must succeed via worker-thread fallback; "
            "pre-fix this returned None because the bridge raised"
        )
        assert result.currency0 == _C0
        assert result.currency1 == _C1

    def test_called_from_running_loop_maps_not_found(self) -> None:
        """The worker-thread fallback must preserve NOT_FOUND → None
        mapping so the parser sees the same return-shape as the sync path."""
        import asyncio

        client = MagicMock()
        client.market.LookupV4PoolKey = MagicMock(
            side_effect=_StubRpcError(grpc.StatusCode.NOT_FOUND, "missing")
        )
        lookup = make_sync_pool_key_lookup(client)

        async def _runner() -> FrameworkPoolKey | None:
            return lookup("0x" + _POOL_ID_HEX, "base")

        assert asyncio.run(_runner()) is None
