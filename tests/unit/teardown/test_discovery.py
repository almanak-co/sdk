"""Tests for on-chain LP position discovery (Bug 2 of 0G DogFooding report).

When a gateway is restarted, the strategy's in-memory position tracking is
lost and ``strategy.get_open_positions()`` returns empty — even if live
NFT positions still sit on-chain. The ``--discover`` flag on
``strat teardown execute`` scans the NonfungiblePositionManager directly
via the gateway's RpcService so those orphans remain closable.

These tests mock the gateway rpc client and assert:
 * balanceOf → tokenOfOwnerByIndex → positions call chain
 * tick range / fee / liquidity decoding (including negative ticks)
 * zero-liquidity filtering (and ``include_empty`` override)
 * unknown chains return empty without raising
 * adapter shape for TeardownPositionSummary
"""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.discovery import (
    DiscoveredPosition,
    DiscoveryIncomplete,
    PositionReadFailure,
    _decode_int24,
    _read_position,
    discover_lp_positions,
    to_teardown_summary,
)
from almanak.framework.teardown.models import PositionType

# ---------------------------------------------------------------------------
# Fake gateway RPC harness
# ---------------------------------------------------------------------------


def _rpc_response(result: str | None = None, success: bool = True, error: str = "") -> SimpleNamespace:
    """Build a gateway RpcResponse-like object (json-encoded result)."""
    return SimpleNamespace(
        success=success,
        result=json.dumps(result) if result is not None else "",
        error=error,
    )


def _encode_positions(
    token0: str,
    token1: str,
    fee: int,
    tick_lower: int,
    tick_upper: int,
    liquidity: int,
) -> str:
    """Encode the 12-word ABI return of NPM.positions(tokenId).

    Negative signed integers are sign-extended to the full 256 bits per the
    Solidity ABI (e.g. ``-1000`` arrives as ``0xffff...fc18``), so the
    test harness must match that — decoding ``i24`` from a right-aligned
    24-bit value would hide the bug the decoder is meant to handle.
    """

    def u(v: int) -> str:
        return hex(v)[2:].zfill(64)

    def addr(a: str) -> str:
        return a.replace("0x", "").zfill(64)

    def i24(v: int) -> str:
        # ABI sign-extends to 256 bits. Convert to unsigned then zero-fill.
        unsigned = v + 2**256 if v < 0 else v
        return hex(unsigned)[2:].zfill(64)

    nonce = u(0)
    operator = addr("0x" + "00" * 20)
    t0 = addr(token0)
    t1 = addr(token1)
    fee_w = u(fee)
    tl = i24(tick_lower)
    tu = i24(tick_upper)
    liq = u(liquidity)
    fg0 = u(0)
    fg1 = u(0)
    ow0 = u(0)
    ow1 = u(0)
    return "0x" + nonce + operator + t0 + t1 + fee_w + tl + tu + liq + fg0 + fg1 + ow0 + ow1


class _FakeRpc:
    """Stub for gateway_client.rpc that dispatches on calldata prefix."""

    def __init__(
        self,
        balance: int,
        token_ids: list[int],
        positions: dict[int, str],  # tokenId -> encoded positions() result
    ):
        self.balance = balance
        self.token_ids = token_ids
        self.positions = positions
        self.calls: list[tuple[str, str]] = []

    def Call(self, request, timeout=15.0):  # noqa: ARG002
        data = request.params
        parsed = json.loads(data)
        calldata = parsed[0]["data"]
        self.calls.append((request.chain, calldata))
        if calldata.startswith("0x70a08231"):  # balanceOf
            return _rpc_response(hex(self.balance))
        if calldata.startswith("0x2f745c59"):  # tokenOfOwnerByIndex
            index = int(calldata[-64:], 16)
            if index < len(self.token_ids):
                return _rpc_response(hex(self.token_ids[index]))
            return _rpc_response("0x0")
        if calldata.startswith("0x99fbab88"):  # positions
            token_id = int(calldata[-64:], 16)
            encoded = self.positions.get(token_id)
            if encoded is None:
                return _rpc_response("0x")
            return _rpc_response(encoded)
        return _rpc_response(success=False, error="unexpected calldata")


def _fake_client(rpc: _FakeRpc) -> MagicMock:
    client = MagicMock()
    client.rpc = rpc
    return client


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestDecodeInt24:
    """ABI-layout tests for the int24 decoder.

    Negative signed integers are sign-extended to the full 256 bits by
    the Solidity ABI. An earlier version of the decoder read the whole
    256-bit integer and subtracted 2**24, producing garbage for every
    negative tick (Codex P3). These tests lock in correct handling for
    both positive and negative ticks, including the Uniswap V3 range
    limits ±887272.
    """

    def test_positive_tick(self):
        assert _decode_int24(hex(100)[2:].zfill(64)) == 100

    def test_negative_tick_sign_extended(self):
        # -1000 ABI-encoded is 0xffffff...fc18 (sign-extended to 256 bits)
        encoded = hex((-1000) + 2**256)[2:].zfill(64)
        assert _decode_int24(encoded) == -1000

    def test_min_tick_sign_extended(self):
        # -887272 is the Uniswap V3 min tick — this is the common case
        encoded = hex((-887272) + 2**256)[2:].zfill(64)
        assert _decode_int24(encoded) == -887272

    def test_max_tick(self):
        assert _decode_int24(hex(887272)[2:].zfill(64)) == 887272

    def test_zero(self):
        assert _decode_int24("0" * 64) == 0


class TestDiscoverLpPositions:
    """Mirrors the DogFooding reproduction: scan zerog NPM for wallet 0x54...."""

    WALLET = "0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF"

    def test_unknown_chain_returns_empty(self):
        rpc = _FakeRpc(balance=0, token_ids=[], positions={})
        client = _fake_client(rpc)
        result = asyncio.run(
            discover_lp_positions(client=client, chain="does_not_exist", wallet=self.WALLET)
        )
        assert result == []
        # Should not have called the RPC for an unsupported chain
        assert rpc.calls == []

    def test_no_positions_returns_empty(self):
        rpc = _FakeRpc(balance=0, token_ids=[], positions={})
        client = _fake_client(rpc)
        result = asyncio.run(
            discover_lp_positions(client=client, chain="zerog", wallet=self.WALLET)
        )
        assert result == []

    def test_discovers_live_position(self):
        """Finds #2359 on zerog JAINE DEX with the exact data from DogFooding."""
        encoded = _encode_positions(
            token0="0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",  # WBTC on 0G
            token1="0x1Cd0690fF9a693f5EF2dD976660a8dAFc81A109c",  # W0G
            fee=10000,
            tick_lower=343_800,
            tick_upper=349_800,
            liquidity=700_417_431_525,
        )
        rpc = _FakeRpc(balance=1, token_ids=[2359], positions={2359: encoded})
        client = _fake_client(rpc)

        result = asyncio.run(
            discover_lp_positions(client=client, chain="zerog", wallet=self.WALLET)
        )
        assert len(result) == 1
        p = result[0]
        assert isinstance(p, DiscoveredPosition)
        assert p.token_id == 2359
        assert p.chain == "zerog"
        assert p.protocol == "uniswap_v3"
        assert p.token0.lower() == "0x0555e30da8f98308edb960aa94c0db47230d2b9c"
        assert p.token1.lower() == "0x1cd0690ff9a693f5ef2dd976660a8dafc81a109c"
        assert p.fee == 10000
        assert p.tick_lower == 343_800
        assert p.tick_upper == 349_800
        assert p.liquidity == 700_417_431_525
        assert p.npm_address.lower() == "0x8f67a30ed186e3e1f6504c6de3239ef43a2e0d72"

    def test_filters_zero_liquidity_by_default(self):
        """Withdrawn-but-not-burned NFTs (liquidity=0) are filtered out."""
        encoded = _encode_positions(
            token0="0xaaa0000000000000000000000000000000000000",
            token1="0xbbb0000000000000000000000000000000000000",
            fee=3000,
            tick_lower=-1000,
            tick_upper=1000,
            liquidity=0,
        )
        rpc = _FakeRpc(balance=1, token_ids=[42], positions={42: encoded})
        client = _fake_client(rpc)

        result = asyncio.run(
            discover_lp_positions(client=client, chain="zerog", wallet=self.WALLET)
        )
        assert result == []

    def test_include_empty_surfaces_burned_positions(self):
        encoded = _encode_positions(
            token0="0xaaa0000000000000000000000000000000000000",
            token1="0xbbb0000000000000000000000000000000000000",
            fee=3000,
            tick_lower=-1000,
            tick_upper=1000,
            liquidity=0,
        )
        rpc = _FakeRpc(balance=1, token_ids=[42], positions={42: encoded})
        client = _fake_client(rpc)

        result = asyncio.run(
            discover_lp_positions(
                client=client,
                chain="zerog",
                wallet=self.WALLET,
                include_zero_liquidity=True,
            )
        )
        assert len(result) == 1
        assert result[0].liquidity == 0

    def test_multiple_positions(self):
        """Enumerates balanceOf → tokenOfOwnerByIndex for each NFT."""
        encoded_a = _encode_positions("0xaa" + "0" * 38, "0xbb" + "0" * 38, 3000, 0, 100, 111)
        encoded_b = _encode_positions("0xcc" + "0" * 38, "0xdd" + "0" * 38, 500, -50, 50, 222)
        rpc = _FakeRpc(
            balance=2,
            token_ids=[2359, 2361],
            positions={2359: encoded_a, 2361: encoded_b},
        )
        client = _fake_client(rpc)

        result = asyncio.run(
            discover_lp_positions(client=client, chain="zerog", wallet=self.WALLET)
        )
        assert [p.token_id for p in result] == [2359, 2361]
        assert [p.liquidity for p in result] == [111, 222]


class _FakeRpcMultiNpm:
    """FakeRpc that dispatches on (chain, npm address) so a single client can
    serve multiple NPMs simultaneously — used for the multi-protocol scan
    tests (DogFooding review finding #3)."""

    def __init__(self, per_npm: dict[str, dict]):
        """per_npm maps lower-cased NPM address → {balance, token_ids, positions}."""
        self.per_npm = {k.lower(): v for k, v in per_npm.items()}
        self.calls: list[tuple[str, str, str]] = []

    def Call(self, request, timeout=15.0):  # noqa: ARG002
        parsed = json.loads(request.params)
        to_addr = parsed[0]["to"].lower()
        calldata = parsed[0]["data"]
        self.calls.append((request.chain, to_addr, calldata))
        entry = self.per_npm.get(to_addr)
        if entry is None:
            return _rpc_response("0x0")  # unknown NPM → zero balance
        if calldata.startswith("0x70a08231"):
            return _rpc_response(hex(entry["balance"]))
        if calldata.startswith("0x2f745c59"):
            index = int(calldata[-64:], 16)
            if index < len(entry["token_ids"]):
                return _rpc_response(hex(entry["token_ids"][index]))
            return _rpc_response("0x0")
        if calldata.startswith("0x99fbab88"):
            token_id = int(calldata[-64:], 16)
            encoded = entry["positions"].get(token_id)
            if encoded is None:
                return _rpc_response("0x")
            return _rpc_response(encoded)
        return _rpc_response(success=False, error="unexpected calldata")


class TestMultiProtocolScan:
    """Discovery walks every V3-fork NPM registered for the chain, not just
    Uniswap V3 (DogFooding review finding #3). A wallet with positions on
    both Uniswap V3 and SushiSwap V3 on Base must surface both, each tagged
    with the correct protocol slug so the close intent uses the right
    compiler path.
    """

    WALLET = "0xaaaa000000000000000000000000000000000000"

    def test_base_scans_both_uniswap_and_sushiswap_npms(self):
        """On Base, Uniswap V3 + SushiSwap V3 NPMs are both enumerated."""
        from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3
        from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3

        univ3_npm = UNISWAP_V3["base"]["position_manager"].lower()
        sushi_npm = SUSHISWAP_V3["base"]["position_manager"].lower()

        uni_encoded = _encode_positions(
            "0xaa" + "0" * 38, "0xbb" + "0" * 38, 3000, 0, 100, 111
        )
        sushi_encoded = _encode_positions(
            "0xcc" + "0" * 38, "0xdd" + "0" * 38, 500, -50, 50, 222
        )

        rpc = _FakeRpcMultiNpm(
            {
                univ3_npm: {
                    "balance": 1,
                    "token_ids": [1000],
                    "positions": {1000: uni_encoded},
                },
                sushi_npm: {
                    "balance": 1,
                    "token_ids": [2000],
                    "positions": {2000: sushi_encoded},
                },
            }
        )
        client = MagicMock()
        client.rpc = rpc

        result = asyncio.run(discover_lp_positions(client=client, chain="base", wallet=self.WALLET))

        protocols = sorted(p.protocol for p in result)
        assert protocols == ["sushiswap_v3", "uniswap_v3"]
        assert {p.token_id for p in result} == {1000, 2000}


class TestDiscoveryIncomplete:
    """When balanceOf reports N positions but a positions() call fails after
    retries, raising DiscoveryIncomplete prevents the operator from running
    a partial teardown that leaves orphans behind (review finding #7)."""

    WALLET = "0xaaaa000000000000000000000000000000000000"

    def test_raises_when_position_read_fails_after_retries(self):
        """Wallet owns 2 positions but positions(2001) always returns '0x'."""
        from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3

        npm = UNISWAP_V3["zerog"]["position_manager"].lower()
        encoded = _encode_positions("0xaa" + "0" * 38, "0xbb" + "0" * 38, 3000, 0, 100, 111)

        rpc = _FakeRpcMultiNpm(
            {
                npm: {
                    "balance": 2,
                    "token_ids": [2000, 2001],
                    "positions": {2000: encoded},  # 2001 is missing
                },
            }
        )
        client = MagicMock()
        client.rpc = rpc

        with pytest.raises(DiscoveryIncomplete) as exc:
            asyncio.run(discover_lp_positions(client=client, chain="zerog", wallet=self.WALLET))

        assert exc.value.chain == "zerog"
        assert exc.value.missing == [1]  # index 1 = tokenId 2001

    def test_non_strict_returns_partial(self):
        """strict=False surfaces what we could read and logs a warning."""
        from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3

        npm = UNISWAP_V3["zerog"]["position_manager"].lower()
        encoded = _encode_positions("0xaa" + "0" * 38, "0xbb" + "0" * 38, 3000, 0, 100, 111)

        rpc = _FakeRpcMultiNpm(
            {
                npm: {
                    "balance": 2,
                    "token_ids": [2000, 2001],
                    "positions": {2000: encoded},
                },
            }
        )
        client = MagicMock()
        client.rpc = rpc

        result = asyncio.run(
            discover_lp_positions(
                client=client, chain="zerog", wallet=self.WALLET, strict=False
            )
        )
        assert [p.token_id for p in result] == [2000]


class TestBalanceOfFailurePropagation:
    """_balance_of must return None on RPC failure (not 0) so the caller can
    distinguish 'NPM has no positions' from 'we couldn't read the NPM' —
    silently treating the latter as the former re-introduces the exact
    Bug 2 failure mode (CodeRabbit critical, PR #1522)."""

    WALLET = "0xaaaa000000000000000000000000000000000000"

    def test_unreadable_balance_of_raises_in_strict_mode(self):
        """A failing balanceOf → DiscoveryIncomplete rather than empty list."""
        from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3

        npm = UNISWAP_V3["zerog"]["position_manager"].lower()

        # Rpc stub that returns a failed response for balanceOf
        class _FailingBalance(_FakeRpcMultiNpm):
            def Call(self, request, timeout=15.0):  # noqa: ARG002
                parsed = json.loads(request.params)
                calldata = parsed[0]["data"]
                if calldata.startswith("0x70a08231"):
                    return _rpc_response(success=False, error="rpc down")
                return super().Call(request, timeout)

        rpc = _FailingBalance({npm: {"balance": 0, "token_ids": [], "positions": {}}})
        client = MagicMock()
        client.rpc = rpc

        # At least one of the 4 registered NPMs for zerog (UNISWAP_V3) will
        # fail balanceOf; strict mode must raise.
        with pytest.raises(DiscoveryIncomplete) as exc:
            asyncio.run(
                discover_lp_positions(client=client, chain="zerog", wallet=self.WALLET)
            )
        # missing=[] signals "we don't even know how many positions exist"
        assert exc.value.missing == []

    def test_unreadable_balance_of_skips_npm_in_non_strict_mode(self):
        """strict=False warns and continues to the next NPM instead of raising."""
        from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3

        npm = UNISWAP_V3["zerog"]["position_manager"].lower()

        class _FailingBalance(_FakeRpcMultiNpm):
            def Call(self, request, timeout=15.0):  # noqa: ARG002
                parsed = json.loads(request.params)
                calldata = parsed[0]["data"]
                if calldata.startswith("0x70a08231"):
                    return _rpc_response(success=False, error="rpc down")
                return super().Call(request, timeout)

        rpc = _FailingBalance({npm: {"balance": 0, "token_ids": [], "positions": {}}})
        client = MagicMock()
        client.rpc = rpc

        # Should not raise; returns empty list
        result = asyncio.run(
            discover_lp_positions(
                client=client, chain="zerog", wallet=self.WALLET, strict=False
            )
        )
        assert result == []


class TestMaxPositionsCap:
    """When balanceOf exceeds _MAX_POSITIONS_PER_NPM, strict mode must raise
    DiscoveryIncomplete rather than silently truncate (CodeRabbit major,
    PR #1522)."""

    WALLET = "0xaaaa000000000000000000000000000000000000"

    def test_strict_mode_raises_when_cap_hit(self):
        from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3
        from almanak.framework.teardown.discovery import _MAX_POSITIONS_PER_NPM

        npm = UNISWAP_V3["zerog"]["position_manager"].lower()
        encoded = _encode_positions("0xaa" + "0" * 38, "0xbb" + "0" * 38, 3000, 0, 100, 111)

        # Pretend the wallet owns _MAX_POSITIONS_PER_NPM + 1 NFTs
        rpc = _FakeRpcMultiNpm(
            {
                npm: {
                    "balance": _MAX_POSITIONS_PER_NPM + 1,
                    "token_ids": list(range(_MAX_POSITIONS_PER_NPM + 1)),
                    "positions": {tid: encoded for tid in range(_MAX_POSITIONS_PER_NPM + 1)},
                },
            }
        )
        client = MagicMock()
        client.rpc = rpc

        with pytest.raises(DiscoveryIncomplete) as exc:
            asyncio.run(
                discover_lp_positions(client=client, chain="zerog", wallet=self.WALLET)
            )
        # Truncated indices are reported as missing
        assert exc.value.missing == [_MAX_POSITIONS_PER_NPM]


class _ErrorRpc:
    """Rpc stub whose positions(tokenId) fails a configurable way."""

    def __init__(self, *, error: str = "", raises: Exception | None = None, result: str | None = None):
        self._error = error
        self._raises = raises
        self._result = result
        self.calls = 0

    def Call(self, request, timeout=15.0):  # noqa: ARG002
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        if self._result is not None:
            return _rpc_response(self._result)
        return _rpc_response(success=False, error=self._error)


class TestTypedPositionRead:
    """VIB-5631 — ``_read_position`` is tri-state: a node-MEASURED burned-NFT
    revert (``PositionReadFailure.REVERTED``) is distinguished from an
    UNMEASURED transport/gateway/decode fault (``PositionReadFailure.FAULT``),
    mirroring the VIB-5631 semantics of the gateway's typed
    ``QueryPositionLiquidity`` read (Empty != Zero)."""

    NPM = "0x8F67A30Ed186e3E1f6504c6dE3239Ef43A2e0d72"

    def _read(self, rpc) -> DiscoveredPosition | PositionReadFailure:
        client = MagicMock()
        client.rpc = rpc
        return asyncio.run(_read_position(client, "zerog", self.NPM, 42))

    def test_burned_nft_revert_is_measured_reverted(self):
        """The canonical NPM burn signal, as RpcService surfaces it: a
        JSON-encoded JSON-RPC error whose message carries 'Invalid token ID'."""
        error = json.dumps({"code": 3, "message": "execution reverted: Invalid token ID"})
        assert self._read(_ErrorRpc(error=error)) is PositionReadFailure.REVERTED

    def test_position_not_found_revert_is_measured_reverted(self):
        """The second marker the gateway client folds to a measured 0
        (query_position_tokens_owed) — same fold here so lanes agree."""
        error = json.dumps({"code": -32000, "message": "position not found"})
        assert self._read(_ErrorRpc(error=error)) is PositionReadFailure.REVERTED

    def test_plain_string_revert_marker_is_measured_reverted(self):
        """A non-JSON error payload still matches on raw text."""
        assert self._read(_ErrorRpc(error="execution reverted: Invalid token ID")) is (
            PositionReadFailure.REVERTED
        )

    def test_transport_error_is_fault_not_reverted(self):
        error = json.dumps({"code": -32603, "message": "HTTP 502: bad gateway"})
        assert self._read(_ErrorRpc(error=error)) is PositionReadFailure.FAULT

    def test_unrecognized_revert_text_is_fault_fail_safe(self):
        """An unknown revert reason is NEVER promoted to a burned-position
        measurement — fail-safe to unmeasured."""
        error = json.dumps({"code": 3, "message": "execution reverted: some custom reason"})
        assert self._read(_ErrorRpc(error=error)) is PositionReadFailure.FAULT

    def test_grpc_raise_is_fault(self):
        assert self._read(_ErrorRpc(raises=RuntimeError("channel down"))) is PositionReadFailure.FAULT

    def test_empty_returndata_is_fault(self):
        assert self._read(_ErrorRpc(result="0x")) is PositionReadFailure.FAULT

    def test_short_returndata_is_fault(self):
        assert self._read(_ErrorRpc(result="0x" + "ab" * 32)) is PositionReadFailure.FAULT

    def test_success_still_parses_position(self):
        encoded = _encode_positions("0xaa" + "0" * 38, "0xbb" + "0" * 38, 3000, -100, 100, 777)
        result = self._read(_ErrorRpc(result=encoded))
        assert isinstance(result, DiscoveredPosition)
        assert result.token_id == 42
        assert result.liquidity == 777


class TestWalkOutcomesUnchangedByTypedRead:
    """Call-site choice: the wallet-scan walk folds BOTH typed failures
    back to skip/missing — a mid-scan burn (revert) means the index
    enumeration is stale, so strict mode still raises DiscoveryIncomplete
    (loud re-run) exactly as before the typed read existed."""

    WALLET = "0xaaaa000000000000000000000000000000000000"

    def _rpc_with_reverting_position(self):
        from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3

        npm = UNISWAP_V3["zerog"]["position_manager"].lower()
        encoded = _encode_positions("0xaa" + "0" * 38, "0xbb" + "0" * 38, 3000, 0, 100, 111)

        class _RevertingPosition(_FakeRpcMultiNpm):
            def Call(self, request, timeout=15.0):  # noqa: ARG002
                parsed = json.loads(request.params)
                calldata = parsed[0]["data"]
                if calldata.startswith("0x99fbab88"):
                    token_id = int(calldata[-64:], 16)
                    if token_id == 2001:  # burned mid-scan
                        return _rpc_response(
                            success=False,
                            error=json.dumps(
                                {"code": 3, "message": "execution reverted: Invalid token ID"}
                            ),
                        )
                return super().Call(request, timeout)

        return _RevertingPosition(
            {npm: {"balance": 2, "token_ids": [2000, 2001], "positions": {2000: encoded}}}
        )

    def test_mid_scan_revert_still_raises_discovery_incomplete_in_strict(self):
        client = MagicMock()
        client.rpc = self._rpc_with_reverting_position()
        with pytest.raises(DiscoveryIncomplete) as exc:
            asyncio.run(discover_lp_positions(client=client, chain="zerog", wallet=self.WALLET))
        assert exc.value.missing == [1]  # index 1 = the reverting tokenId 2001

    def test_mid_scan_revert_non_strict_returns_partial(self):
        client = MagicMock()
        client.rpc = self._rpc_with_reverting_position()
        result = asyncio.run(
            discover_lp_positions(client=client, chain="zerog", wallet=self.WALLET, strict=False)
        )
        assert [p.token_id for p in result] == [2000]


class TestToTeardownSummary:
    def test_wraps_discovered_positions(self):
        p = DiscoveredPosition(
            token_id=2359,
            npm_address="0x8F67A30Ed186e3E1f6504c6dE3239Ef43A2e0d72",
            chain="zerog",
            protocol="uniswap_v3",
            token0="0xtoken0",
            token1="0xtoken1",
            fee=10000,
            tick_lower=343_800,
            tick_upper=349_800,
            liquidity=700_417_431_525,
        )
        summary = to_teardown_summary(deployment_id="exp12", chain="zerog", positions=[p])

        assert summary.deployment_id == "exp12"
        assert len(summary.positions) == 1
        info = summary.positions[0]
        assert info.position_type == PositionType.LP
        assert info.position_id == "2359"
        assert info.chain == "zerog"
        assert info.protocol == "uniswap_v3"
        assert info.details["discovered_on_chain"] is True
        assert info.details["liquidity"] == "700417431525"
        assert info.details["tick_lower"] == 343_800
        # value_usd_unknown sentinel lets SafetyGuard / CLI detect that the 0
        # value_usd doesn't mean a legitimate empty position — CodeRabbit
        # review of PR #1522 flagged that safety caps collapse to the most
        # permissive tier when total_value_usd is 0.
        assert info.details["value_usd_unknown"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
