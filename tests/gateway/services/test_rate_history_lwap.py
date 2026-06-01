"""GetDexLwap servicer tests (VIB-4948 / L3 of ALM-2770).

Mirrors the GetDexTwap skeleton tests: validator-first INVALID_ARGUMENT
behaviour + a full-dispatch test that mocks the servicer's per-chain
``_get_web3`` so the connector's slot0/liquidity/decimals read path and the
liquidity-weighting math run end-to-end without network.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import grpc
import pytest

from almanak.connectors.uniswap_v3.gateway.provider import _sqrt_price_x96_to_price
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer

# Stable Uniswap-V3 ABI selectors (no 0x prefix).
_SLOT0 = "3850c7bd"
_LIQUIDITY = "1a686502"
_TOKEN0 = "0dfe1681"
_TOKEN1 = "d21220a7"
_DECIMALS = "313ce567"

# Base canonical addresses.
_WETH = "0x4200000000000000000000000000000000000006"
_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


class _MockContext:
    def __init__(self):
        self.code = None
        self.details = None

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details


@pytest.fixture
def servicer() -> RateHistoryServiceServicer:
    return RateHistoryServiceServicer(GatewaySettings())


def _uint_word(n: int) -> bytes:
    return int(n).to_bytes(32, byteorder="big")


def _addr_word(addr: str) -> bytes:
    return bytes(12) + bytes.fromhex(addr[2:])


def _make_fake_web3(*, sqrt_price_x96: int, liquidity: int, t0_dec: int = 18, t1_dec: int = 6):
    """Fake AsyncWeb3 whose eth.call dispatches on the calldata selector."""

    async def _call(tx, block_identifier=None):
        to = tx["to"].lower()
        sel = tx["data"][2:10]
        if sel == _SLOT0:
            return _uint_word(sqrt_price_x96) + _uint_word(0)  # sqrtPriceX96 + tick word
        if sel == _LIQUIDITY:
            return _uint_word(liquidity)
        if sel == _TOKEN0:
            return _addr_word(_WETH)
        if sel == _TOKEN1:
            return _addr_word(_USDC)
        if sel == _DECIMALS:
            if to == _WETH.lower():
                return _uint_word(t0_dec)
            if to == _USDC.lower():
                return _uint_word(t1_dec)
        raise AssertionError(f"unexpected eth.call sel={sel} to={to}")

    return SimpleNamespace(
        eth=SimpleNamespace(call=_call),
        to_checksum_address=lambda a: a,
    )


# --------------------------------------------------------------------------- #
# Registration + validators
# --------------------------------------------------------------------------- #


def test_uniswap_v3_registered_as_lwap_provider(servicer):
    assert "uniswap_v3" in servicer._lwap_providers


def test_lwap_rejects_empty_dex(servicer):
    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(dex="", chain="base", pool_addresses=["0xpool"])
    response = asyncio.run(servicer.GetDexLwap(request, ctx))
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert response.success is False


def test_lwap_rejects_empty_chain(servicer):
    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(dex="uniswap_v3", chain="", pool_addresses=["0xpool"])
    response = asyncio.run(servicer.GetDexLwap(request, ctx))
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert response.success is False


def test_lwap_rejects_empty_pool_addresses(servicer):
    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(dex="uniswap_v3", chain="base", pool_addresses=[])
    response = asyncio.run(servicer.GetDexLwap(request, ctx))
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert response.success is False


def test_lwap_rejects_unknown_dex(servicer):
    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(dex="not_a_dex", chain="base", pool_addresses=["0xpool"])
    response = asyncio.run(servicer.GetDexLwap(request, ctx))
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert response.success is False
    assert "unsupported dex" in response.error


def test_lwap_rejects_unsupported_chain(servicer):
    ctx = _MockContext()
    # solana is not in uniswap_v3.lwap_supported_chains().
    request = gateway_pb2.GetDexLwapRequest(dex="uniswap_v3", chain="solana", pool_addresses=["0xpool"])
    response = asyncio.run(servicer.GetDexLwap(request, ctx))
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert response.success is False
    assert "does not support LWAP" in response.error


# --------------------------------------------------------------------------- #
# Full dispatch (mocked _get_web3)
# --------------------------------------------------------------------------- #


def test_lwap_single_pool_returns_weighted_price(servicer):
    sqrt = 3961408125713216879677197516800  # ~ WETH/USDC spot
    fake_web3 = _make_fake_web3(sqrt_price_x96=sqrt, liquidity=10**18)
    servicer._get_web3 = AsyncMock(return_value=fake_web3)

    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(dex="uniswap_v3", chain="base", pool_addresses=["0xpool500"])
    response = asyncio.run(servicer.GetDexLwap(request, ctx))

    assert response.success is True
    assert response.source == "gateway_rpc"
    assert response.point.pool_count == 1
    expected = _sqrt_price_x96_to_price(sqrt, 18, 6)
    assert Decimal(response.point.price) == expected


def test_lwap_weights_by_liquidity_across_pools(servicer):
    # Two pools at different prices; the deeper pool dominates the weighted avg.
    sqrt_a = 3961408125713216879677197516800
    sqrt_b = 3900000000000000000000000000000
    price_a = _sqrt_price_x96_to_price(sqrt_a, 18, 6)
    price_b = _sqrt_price_x96_to_price(sqrt_b, 18, 6)
    liq_a, liq_b = 9 * 10**18, 1 * 10**18

    async def _get_web3(_chain):
        # Distinguish pools by the pool address in the slot0/liquidity call.
        return _FakeMultiPoolWeb3(
            {
                "0xpoola": (sqrt_a, liq_a),
                "0xpoolb": (sqrt_b, liq_b),
            }
        )

    servicer._get_web3 = _get_web3

    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(
        dex="uniswap_v3", chain="base", pool_addresses=["0xpoola", "0xpoolb"]
    )
    response = asyncio.run(servicer.GetDexLwap(request, ctx))

    assert response.success is True
    assert response.point.pool_count == 2
    expected = (price_a * Decimal(liq_a) + price_b * Decimal(liq_b)) / Decimal(liq_a + liq_b)
    assert Decimal(response.point.price) == expected


def test_lwap_no_readable_pools_returns_success_false(servicer):
    async def _get_web3(_chain):
        return _FakeMultiPoolWeb3({})  # every pool reads empty → skipped

    servicer._get_web3 = _get_web3

    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(dex="uniswap_v3", chain="base", pool_addresses=["0xdead"])
    response = asyncio.run(servicer.GetDexLwap(request, ctx))
    assert response.success is False
    assert "no readable" in response.error.lower()


def test_lwap_dedupes_repeated_pool_addresses(servicer):
    # CodeRabbit: a duplicated pool address must not be read+weighted twice
    # (caller-biased weighting). The servicer dedupes case-insensitively.
    sqrt = 3961408125713216879677197516800
    fake_web3 = _make_fake_web3(sqrt_price_x96=sqrt, liquidity=10**18)
    servicer._get_web3 = AsyncMock(return_value=fake_web3)

    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(
        dex="uniswap_v3", chain="base", pool_addresses=["0xPool", "0xpool", "0xPOOL"]
    )
    response = asyncio.run(servicer.GetDexLwap(request, ctx))
    assert response.success is True
    assert response.point.pool_count == 1  # three spellings of one pool → one


def test_lwap_all_zero_liquidity_returns_success_false(servicer):
    # Scenario D (VIB-4924 I1): the pool is READABLE (valid slot0) but has zero
    # in-range liquidity. Equal-weighting its spot price would fabricate an
    # EXECUTION_GRADE price out of economically unbacked data — must fail closed.
    sqrt = 3961408125713216879677197516800
    async def _get_web3(_chain):
        return _FakeMultiPoolWeb3({"0xpoolzero": (sqrt, 0)})

    servicer._get_web3 = _get_web3

    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(dex="uniswap_v3", chain="base", pool_addresses=["0xpoolzero"])
    response = asyncio.run(servicer.GetDexLwap(request, ctx))
    assert response.success is False
    assert "liquidity" in response.error.lower()


_USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"


class _FakePairPoolWeb3:
    """eth.call keyed by pool address, with per-pool token0/token1 pairs.

    Lets a test mix a genuine WETH/USDC pool with a foreign-pair (WETH/USDT)
    pool to exercise the gateway's pair filter (VIB-4924 B2 follow-on).
    """

    def __init__(self, pools):
        # pools: {addr_lower: (sqrt, liq, token0_lower, token1_lower)}
        self._pools = {k.lower(): v for k, v in pools.items()}
        self.eth = SimpleNamespace(call=self._call)
        self._dec = {_WETH.lower(): 18, _USDC.lower(): 6, _USDT.lower(): 6}

    def to_checksum_address(self, a):
        return a

    async def _call(self, tx, block_identifier=None):
        to = tx["to"].lower()
        sel = tx["data"][2:10]
        if to in self._pools:
            sqrt, liq, t0, t1 = self._pools[to]
            if sel == _SLOT0:
                return _uint_word(sqrt) + _uint_word(0)
            if sel == _LIQUIDITY:
                return _uint_word(liq)
            if sel == _TOKEN0:
                return _addr_word(t0)
            if sel == _TOKEN1:
                return _addr_word(t1)
        if sel == _DECIMALS and to in self._dec:
            return _uint_word(self._dec[to])
        raise AssertionError(f"unexpected eth.call sel={sel} to={to}")


def test_lwap_pair_filter_drops_foreign_pair_pool(servicer):
    # One genuine WETH/USDC pool (token0=USDC) and one WETH/USDT pool. With
    # base/quote = WETH/USDC supplied, the WETH/USDT pool MUST be dropped so it
    # cannot dominate Σ(price·liq) (the live Ethereum bug: a stale "USDC/WETH"
    # known-pools entry actually pointed at a WETH/USDT pool).
    sqrt = 3961408125713216879677197516800
    pools = {
        # genuine WETH/USDC, token0=USDC, token1=WETH
        "0xgood": (sqrt, 10**18, _USDC.lower(), _WETH.lower()),
        # foreign WETH/USDT, token0=WETH, token1=USDT — must be filtered out
        "0xbad": (sqrt, 50 * 10**18, _WETH.lower(), _USDT),
    }

    async def _get_web3(_chain):
        return _FakePairPoolWeb3(pools)

    servicer._get_web3 = _get_web3

    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(
        dex="uniswap_v3",
        chain="ethereum",
        pool_addresses=["0xgood", "0xbad"],
        base_token=_WETH,
        quote_token=_USDC,
    )
    response = asyncio.run(servicer.GetDexLwap(request, ctx))

    assert response.success is True
    # Only the genuine WETH/USDC pool survives the pair filter.
    assert response.point.pool_count == 1
    expected = _sqrt_price_x96_to_price(sqrt, 6, 18)  # token0=USDC(6), token1=WETH(18)
    assert Decimal(response.point.price) == expected


def test_lwap_pair_filter_all_foreign_returns_success_false(servicer):
    sqrt = 3961408125713216879677197516800
    pools = {"0xbad": (sqrt, 10**18, _WETH.lower(), _USDT)}

    async def _get_web3(_chain):
        return _FakePairPoolWeb3(pools)

    servicer._get_web3 = _get_web3

    ctx = _MockContext()
    request = gateway_pb2.GetDexLwapRequest(
        dex="uniswap_v3",
        chain="ethereum",
        pool_addresses=["0xbad"],
        base_token=_WETH,
        quote_token=_USDC,
    )
    response = asyncio.run(servicer.GetDexLwap(request, ctx))
    assert response.success is False
    assert "pair" in response.error.lower()


class _FakeMultiPoolWeb3:
    """eth.call keyed by pool address so multiple pools return distinct data."""

    def __init__(self, pools: dict[str, tuple[int, int]]):
        # pools: {pool_address_lower: (sqrt_price_x96, liquidity)}
        self._pools = {k.lower(): v for k, v in pools.items()}
        self.eth = SimpleNamespace(call=self._call)

    def to_checksum_address(self, a):
        return a

    async def _call(self, tx, block_identifier=None):
        to = tx["to"].lower()
        sel = tx["data"][2:10]
        if to in self._pools:
            sqrt, liq = self._pools[to]
            if sel == _SLOT0:
                return _uint_word(sqrt) + _uint_word(0)
            if sel == _LIQUIDITY:
                return _uint_word(liq)
            if sel == _TOKEN0:
                return _addr_word(_WETH)
            if sel == _TOKEN1:
                return _addr_word(_USDC)
        # Unknown pool (or token decimals on the canonical token addresses).
        if sel == _DECIMALS:
            if to == _WETH.lower():
                return _uint_word(18)
            if to == _USDC.lower():
                return _uint_word(6)
        if sel in (_SLOT0, _LIQUIDITY, _TOKEN0, _TOKEN1):
            return b""  # unreadable pool → skipped
        raise AssertionError(f"unexpected eth.call sel={sel} to={to}")
