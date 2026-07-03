"""VIB-5024 — gateway ``QueryV4PositionState`` RPC.

The gateway reads live Uniswap V4 LP position state on-chain via three eth_calls:

1. ``PositionManager.getPositionLiquidity(tokenId)`` → liquidity (uint128).
2. ``PositionManager.getPoolAndPositionInfo(tokenId)`` → ``(PoolKey, info)``
   where ``info`` packs tickLower (bits 8-31) and tickUpper (bits 32-55).
3. ``StateView.getSlot0(poolId)`` → ``(sqrtPriceX96, tick, ...)`` where
   ``poolId = keccak256(abi.encode(PoolKey))`` (bytes32) — the deployed StateView
   takes the PoolId, not a PoolKey tuple (the tuple selector reverts on-chain;
   VIB-5024, validated against live Base positions).

These tests pin: clean decode of liquidity / signed ticks / slot0, the
keccak PoolId derivation for the slot0 call (selector + argument), the
PositionInfo↔PoolKey poolId integrity guard, Empty≠Zero handling (a partial /
errored read is ``success=False`` so the valuer falls back to ESTIMATED — never
a wrong HIGH), and input validation.
"""

from unittest.mock import patch

import pytest
from eth_utils import keccak

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rpc_service import (
    _GET_SLOT0_SELECTOR,
    RpcServiceServicer,
    _as_int24,
    _decode_v4_pool_and_position_info,
)

# A valid V4 PositionManager + StateView on Base (used only for address validation).
_PM = "0x7C5f5A4bBd8fD63184577525326123B519429bDc"
_STATE_VIEW = "0xA3c0c9b65baD0b08107Aa264b0f3dB444b867A71"
_WETH = "0x4200000000000000000000000000000000000006"
_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def _word(value: int) -> str:
    """ABI-encode an int/uint as one 64-hex word (two's complement for negatives)."""
    return format(value & ((1 << 256) - 1), "064x")


def _liquidity_payload(liquidity: int) -> str:
    return "0x" + _word(liquidity)


# The 5-word PoolKey ABI head shared by the synthetic fixtures, plus its
# canonical PoolId (keccak of the ABI-encoded struct == keccak of the 5 words)
# and the 200-bit truncation v4-periphery packs into PositionInfo. Deriving the
# packed poolId from the real keccak keeps fixtures self-consistent with the
# servicer's PositionInfo↔PoolKey integrity guard, exactly as on-chain data is.
_POOL_KEY_WORDS = _word(int(_WETH, 16)) + _word(int(_USDC, 16)) + _word(3000) + _word(60) + _word(0)
_POOL_ID_FULL = keccak(bytes.fromhex(_POOL_KEY_WORDS))
_POOL_ID_TOP200 = int.from_bytes(_POOL_ID_FULL, "big") >> 56


def _pool_and_position_info_payload(
    tick_lower: int, tick_upper: int, pool_id_top: int = _POOL_ID_TOP200
) -> str:
    """Synthetic getPoolAndPositionInfo return: 5 PoolKey words + packed info word.

    ``pool_id_top`` defaults to the real keccak truncation so the fixture passes
    the integrity guard; override it to force a mismatch (guard-rejection test).
    """
    tl = tick_lower & ((1 << 24) - 1)
    tu = tick_upper & ((1 << 24) - 1)
    info = (pool_id_top << 56) | (tu << 32) | (tl << 8)
    return "0x" + _POOL_KEY_WORDS + _word(info)


def _slot0_payload(sqrt_price_x96: int, tick: int) -> str:
    """Synthetic StateView.getSlot0 return: sqrtPriceX96, tick, protocolFee, lpFee."""
    return "0x" + _word(sqrt_price_x96) + _word(tick) + _word(0) + _word(3000)


def _position_info_payload(liquidity: int, fg0_last: int = 0, fg1_last: int = 0) -> str:
    """Synthetic StateView.getPositionInfo return: (liquidity, fg0Last, fg1Last).

    ``liquidity`` must equal the getPositionLiquidity value or the servicer's
    self-verifying guard fails the read (positionId-convention check).
    """
    return "0x" + _word(liquidity) + _word(fg0_last) + _word(fg1_last)


def _fee_growth_inside_payload(fg0: int = 0, fg1: int = 0) -> str:
    """Synthetic StateView.getFeeGrowthInside return: (feeGrowthInside0X128, feeGrowthInside1X128)."""
    return "0x" + _word(fg0) + _word(fg1)


# Convenience: the 5 successful eth_call returns in servicer order
# (liquidity, pool+position info, slot0, position fee snapshot, feeGrowthInside).
def _ok_reads(*, liquidity, tick_lower, tick_upper, sqrt_price_x96, tick, fg0_last=0, fg1_last=0, fg0=0, fg1=0):
    return [
        (_liquidity_payload(liquidity), None),
        (_pool_and_position_info_payload(tick_lower, tick_upper), None),
        (_slot0_payload(sqrt_price_x96, tick), None),
        (_position_info_payload(liquidity, fg0_last, fg1_last), None),
        (_fee_growth_inside_payload(fg0, fg1), None),
    ]


def _request(token_id: int = 2350913, block: str = "") -> gateway_pb2.V4PositionStateRequest:
    return gateway_pb2.V4PositionStateRequest(
        chain="base",
        position_manager=_PM,
        state_view=_STATE_VIEW,
        token_id=token_id,
        block=block,
    )


@pytest.fixture
def rpc_service():
    return RpcServiceServicer(GatewaySettings())


@pytest.fixture
def mock_context():
    from unittest.mock import MagicMock

    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


# ---------------------------------------------------------------------------
# Decode unit checks
# ---------------------------------------------------------------------------


class TestPositionInfoDecode:
    def test_packed_info_decodes_signed_ticks(self):
        payload = _pool_and_position_info_payload(tick_lower=-100, tick_upper=200)
        words, tl, tu, pool_id = _decode_v4_pool_and_position_info(payload)
        assert tl == -100
        assert tu == 200
        assert len(words) == 5 * 64
        assert pool_id.startswith("0x")

    def test_as_int24_two_complement(self):
        assert _as_int24(0) == 0
        assert _as_int24(200) == 200
        assert _as_int24((-100) & ((1 << 24) - 1)) == -100
        assert _as_int24((1 << 23)) == -(1 << 23)  # most-negative int24

    def test_pool_key_words_decoded_verbatim(self):
        """Decode returns the on-chain PoolKey words verbatim (later keccak'd to PoolId)."""
        payload = _pool_and_position_info_payload(tick_lower=-100, tick_upper=200)
        words, *_ = _decode_v4_pool_and_position_info(payload)
        assert words == _POOL_KEY_WORDS

    def test_truncated_payload_raises(self):
        with pytest.raises(ValueError):
            _decode_v4_pool_and_position_info("0x" + "00" * 32)  # only one word


# ---------------------------------------------------------------------------
# RPC behaviour
# ---------------------------------------------------------------------------


class TestQueryV4PositionState:
    @pytest.mark.asyncio
    async def test_full_read_success(self, rpc_service, mock_context):
        reads = _ok_reads(
            liquidity=123456789012345,
            tick_lower=-887220,
            tick_upper=887220,
            sqrt_price_x96=79228162514264337593543950336,
            tick=-50,
        )

        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(rpc_service, "_make_rpc_call", side_effect=reads),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.success is True
        assert resp.liquidity == "123456789012345"
        assert resp.tick_lower == -887220
        assert resp.tick_upper == 887220
        assert resp.current_tick == -50
        assert resp.sqrt_price_x96 == "79228162514264337593543950336"
        assert resp.pool_id.startswith("0x")
        # Fees measured (V3 parity): both owed fields populated, here measured-zero.
        assert resp.tokens_owed0 == "0"
        assert resp.tokens_owed1 == "0"

    @pytest.mark.asyncio
    async def test_measured_zero_liquidity_is_success(self, rpc_service, mock_context):
        """Empty≠Zero: a measured-zero liquidity is a valid (closed-but-owned) read."""
        # posinfo liquidity must match getPositionLiquidity (0) for the guard.
        reads = _ok_reads(
            liquidity=0,
            tick_lower=-100,
            tick_upper=100,
            sqrt_price_x96=79228162514264337593543950336,
            tick=0,
        )

        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(rpc_service, "_make_rpc_call", side_effect=reads),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.success is True
        assert resp.liquidity == "0"  # measured zero, NOT empty
        assert resp.sqrt_price_x96 == "79228162514264337593543950336"
        assert resp.tokens_owed0 == "0"  # measured zero (liquidity 0 ⇒ no fees)
        assert resp.tokens_owed1 == "0"

    @pytest.mark.asyncio
    async def test_uncollected_fees_included(self, rpc_service, mock_context):
        """Fees: owed = liquidity·(fgInside − fgLast)/2^128 per token (V3 parity)."""
        liquidity = 10**18
        q128 = 1 << 128
        # Choose growth deltas that yield clean owed amounts.
        fg0_last, fg0 = 0, 5 * q128 // (10**6)  # owed0 ≈ liquidity*5/1e6
        fg1_last, fg1 = 7 * q128, 7 * q128 + 3 * q128 // (10**3)  # nonzero last, owed1 ≈ liquidity*3/1e3
        reads = _ok_reads(
            liquidity=liquidity,
            tick_lower=-100,
            tick_upper=100,
            sqrt_price_x96=79228162514264337593543950336,
            tick=0,
            fg0_last=fg0_last,
            fg1_last=fg1_last,
            fg0=fg0,
            fg1=fg1,
        )

        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(rpc_service, "_make_rpc_call", side_effect=reads),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.success is True
        assert int(resp.tokens_owed0) == liquidity * ((fg0 - fg0_last) % (1 << 256)) // q128
        assert int(resp.tokens_owed1) == liquidity * ((fg1 - fg1_last) % (1 << 256)) // q128
        assert int(resp.tokens_owed0) > 0
        assert int(resp.tokens_owed1) > 0

    @pytest.mark.asyncio
    async def test_position_liquidity_mismatch_is_failure(self, rpc_service, mock_context):
        """getPositionInfo liquidity must equal getPositionLiquidity (positionId guard)."""
        liq = _liquidity_payload(123456789012345)
        info = _pool_and_position_info_payload(tick_lower=-100, tick_upper=100)
        slot0 = _slot0_payload(sqrt_price_x96=79228162514264337593543950336, tick=0)
        # posinfo reports a DIFFERENT liquidity → guard must fail closed.
        posinfo = _position_info_payload(liquidity=999, fg0_last=0, fg1_last=0)
        fgi = _fee_growth_inside_payload()

        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[(liq, None), (info, None), (slot0, None), (posinfo, None), (fgi, None)],
            ),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.success is False
        assert "liquidity mismatch" in resp.error.lower()
        assert resp.tokens_owed0 == ""  # unmeasured — never a fabricated fee

    @pytest.mark.asyncio
    async def test_uninitialized_pool_is_failure_not_high(self, rpc_service, mock_context):
        """sqrtPriceX96 == 0 → pool not initialized → success=False (never wrong-HIGH)."""
        liq = _liquidity_payload(5)
        info = _pool_and_position_info_payload(tick_lower=-100, tick_upper=100)
        slot0 = _slot0_payload(sqrt_price_x96=0, tick=0)

        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[(liq, None), (info, None), (slot0, None)],
            ),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.success is False
        assert resp.liquidity == ""  # Empty (unmeasured) — never collapse to "0"
        assert "not initialized" in resp.error.lower()

    @pytest.mark.asyncio
    async def test_liquidity_read_error_short_circuits(self, rpc_service, mock_context):
        """A failed first read is a failure — no value at HIGH from a partial read."""
        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[(None, {"message": "execution reverted"})],
            ),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.success is False
        assert "reverted" in resp.error.lower()

    @pytest.mark.asyncio
    async def test_slot0_read_error_is_failure(self, rpc_service, mock_context):
        liq = _liquidity_payload(5)
        info = _pool_and_position_info_payload(tick_lower=-100, tick_upper=100)

        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[(liq, None), (info, None), (None, {"message": "slot0 boom"})],
            ),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.success is False
        assert "slot0 boom" in resp.error.lower()

    @pytest.mark.asyncio
    async def test_slot0_call_uses_bytes32_poolid_selector(self, rpc_service, mock_context):
        """Regression (VIB-5024): slot0 calldata MUST be getSlot0(bytes32) + keccak PoolId.

        The deployed V4 StateView reverts ("no data") on the PoolKey-tuple selector
        (0xe924c4df); this pins the corrected selector + bytes32 argument so the HIGH
        path can't silently regress to the always-reverting (inert) form that the
        original implementation shipped with.
        """
        captured: list[tuple[str, list]] = []
        payloads = _ok_reads(
            liquidity=5,
            tick_lower=-100,
            tick_upper=100,
            sqrt_price_x96=79228162514264337593543950336,
            tick=0,
        )

        async def _capture(rpc_url, method, params, label):
            captured.append((label, params))
            return payloads[len(captured) - 1]

        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(rpc_service, "_make_rpc_call", side_effect=_capture),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.success is True
        assert _GET_SLOT0_SELECTOR == "c815641c"  # getSlot0(bytes32), NOT the tuple selector
        slot0_label, slot0_params = captured[2]
        assert slot0_label == "v4_slot0"
        slot0_calldata = slot0_params[0]["data"]
        assert slot0_calldata == "0x" + _GET_SLOT0_SELECTOR + _POOL_ID_FULL.hex()
        # bytes32 form is 0x + 4-byte selector + one 32-byte word; the reverting
        # tuple form would carry five words (320 hex). Guard against that shape.
        assert len(slot0_calldata) == 2 + 8 + 64
        assert resp.pool_id == "0x" + _POOL_ID_FULL.hex()

    @pytest.mark.asyncio
    async def test_poolid_integrity_mismatch_is_failure(self, rpc_service, mock_context):
        """PositionInfo's packed poolId must prefix keccak(PoolKey); mismatch → fail closed.

        The guard short-circuits before the slot0 read (only two RPCs are wired),
        so an inconsistent PoolKey/info decode never values the wrong pool at HIGH.
        """
        liq = _liquidity_payload(5)
        info = _pool_and_position_info_payload(tick_lower=-100, tick_upper=100, pool_id_top=0xDEAD)

        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(
                rpc_service, "_make_rpc_call", side_effect=[(liq, None), (info, None)]
            ),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.success is False
        assert "poolid mismatch" in resp.error.lower()

    @pytest.mark.asyncio
    async def test_rejects_invalid_position_manager(self, rpc_service, mock_context):
        req = gateway_pb2.V4PositionStateRequest(
            chain="base", position_manager="not-an-address", state_view=_STATE_VIEW, token_id=1
        )
        resp = await rpc_service.QueryV4PositionState(req, mock_context)
        assert resp.success is False
        mock_context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_rejects_unconfigured_chain(self, mock_context):
        service = RpcServiceServicer(GatewaySettings(chains=["zerog"]))
        resp = await service.QueryV4PositionState(_request(), mock_context)
        assert resp.success is False


class TestBlockPinning:
    """VIB-5148 (Layer-2 follow-up to VIB-5140): ``request.block`` must reach
    ALL FIVE underlying eth_calls (getPositionLiquidity, getPoolAndPositionInfo,
    getSlot0, getPositionInfo, getFeeGrowthInside) — not just the first two.

    Before this fix, a caller-supplied ``block`` on ``V4PositionStateRequest``
    had no effect at all (the field didn't exist); every read silently used
    "latest". A post-tx V4 read pinned to a stale read-replica one block
    behind the writer could then return PRE-tx state. This test proves the
    field is threaded end-to-end: an unpinned request still reads "latest"
    (backward-compatible), and a pinned request reaches every eth_call with
    the exact block tag — never "latest".
    """

    @pytest.mark.asyncio
    async def test_omitted_block_reads_latest_on_every_call(self, rpc_service, mock_context):
        """Legacy behaviour preserved: an unpinned request reads "latest" everywhere."""
        payloads = _ok_reads(
            liquidity=5,
            tick_lower=-100,
            tick_upper=100,
            sqrt_price_x96=79228162514264337593543950336,
            tick=0,
        )
        captured: list[tuple[str, str]] = []

        async def _capture(rpc_url, method, params, label):
            captured.append((label, params[1]))
            return payloads[len(captured) - 1]

        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(rpc_service, "_make_rpc_call", side_effect=_capture),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(block=""), mock_context)

        assert resp.success is True
        assert len(captured) == 5
        assert [tag for _label, tag in captured] == ["latest"] * 5

    @pytest.mark.asyncio
    async def test_pinned_block_reaches_every_eth_call(self, rpc_service, mock_context):
        """A pinned request must NOT fall back to "latest" on any of the 5 reads.

        This is the false-negative regression this ticket fixes: before the fix
        a stale read-replica trailing the writer by a block could answer an
        unpinned "latest" call with PRE-tx state. Pinning to the exact receipt
        block (here a decimal string, mirroring ``_block_param``'s contract)
        closes that race for every eth_call the servicer issues.
        """
        payloads = _ok_reads(
            liquidity=5,
            tick_lower=-100,
            tick_upper=100,
            sqrt_price_x96=79228162514264337593543950336,
            tick=0,
        )
        captured: list[tuple[str, str]] = []

        async def _capture(rpc_url, method, params, label):
            captured.append((label, params[1]))
            return payloads[len(captured) - 1]

        pinned_block = "12345678"
        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(rpc_service, "_make_rpc_call", side_effect=_capture),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(block=pinned_block), mock_context)

        assert resp.success is True
        assert len(captured) == 5
        labels = [label for label, _tag in captured]
        assert labels == ["v4_position", "v4_position", "v4_slot0", "v4_posinfo", "v4_feegrowth"]
        for _label, tag in captured:
            assert tag == pinned_block
            assert tag != "latest"


class TestClosedPositionEmptyReturn:
    """VIB-5634: a burned/closed V4 position returns empty "0x" — a MEASURED
    closure (``closed=True``), distinct from an RPC error or a truncated payload
    (an honest read fault → ``success=False, closed=False``). Empty ≠ Zero: an
    all-zero WORD is a measured-zero liquidity, still a normal successful read.
    """

    @pytest.mark.asyncio
    async def test_empty_liquidity_return_is_closed(self, rpc_service, mock_context):
        # getPositionLiquidity returns empty 0x (mapping deleted / burned NFT).
        # The eth_call executed with NO return data (error slot is None).
        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[("0x", None), (_pool_and_position_info_payload(-100, 100), None)],
            ),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.closed is True
        assert resp.success is False  # no live HIGH state to value
        # We stopped after the two PositionManager reads — never decoded / hit slot0.
        assert resp.liquidity == ""

    @pytest.mark.asyncio
    async def test_empty_pool_info_return_is_closed(self, rpc_service, mock_context):
        # getPositionLiquidity may still return 0, but getPoolAndPositionInfo is
        # empty → the position is gone.
        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[(_liquidity_payload(0), None), ("0x", None)],
            ),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.closed is True
        assert resp.success is False

    @pytest.mark.asyncio
    async def test_rpc_error_is_not_closed(self, rpc_service, mock_context):
        # A genuine RPC-level error (error slot set) must NOT be mistaken for a
        # closure — it is an honest read fault → success=False, closed=False.
        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[(None, {"message": "execution reverted"})],
            ),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.closed is False
        assert resp.success is False
        assert "reverted" in resp.error

    @pytest.mark.asyncio
    async def test_truncated_liquidity_is_fault_not_closed(self, rpc_service, mock_context):
        # A truncated-but-NONEMPTY payload is a malformed read (a FAULT), NOT a
        # closure: it must fall through to the length-checked decoders and stay
        # success=False, closed=False (→ UNVERIFIED downstream, never CLOSED).
        with (
            patch.object(rpc_service, "_get_rpc_url", return_value="http://test"),
            patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[("0x" + "00" * 8, None), (_pool_and_position_info_payload(-100, 100), None)],
            ),
        ):
            resp = await rpc_service.QueryV4PositionState(_request(), mock_context)

        assert resp.closed is False
        assert resp.success is False
        assert "decode" in resp.error.lower() or "V4 position info" in resp.error
