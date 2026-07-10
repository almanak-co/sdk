"""Tests for CurveLpPositionReader and the PortfolioValuer Curve LP path (VIB-5420).

Covers:
- plain 3pool (DAI/USDC/USDT) valued as lp_balance * virtual_price * $1
- a USD-pegged metapool-shape pool (crvUSD/USDC) valued the same way
- non-USD-numeraire pool (steth) fails closed (Empty != Zero -> UNAVAILABLE)
- Empty != Zero: unmeasured balance / virtual_price -> None, measured zero -> 0
- the live get_virtual_price() / virtual_price() selector fallback
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.curve_lp_position_reader import (
    CurveLpPosition,
    CurveLpPositionReader,
    _resolve_curve_pool_meta,
    _resolve_curve_pool_meta_dynamic,
)
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

# 3pool (ethereum) addresses
POOL_3POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
LP_3POOL = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
WALLET = "0x1234567890123456789012345678901234567890"

# Base 4pool — USDC / USDbC / axlUSDC / crvUSD. StableSwap NG: LP == pool address.
# Plain USD-stable pool that was falsely excluded before audit P0-3 (USDbC and
# axlUSDC missing from the allowlist).
POOL_4POOL_BASE = "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f"
LP_4POOL_BASE = "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f"

GET_VIRTUAL_PRICE = "0xbb7b8b80"
VIRTUAL_PRICE_ALIAS = "0x0c46b72a"
BALANCE_OF = "0x70a08231"
BALANCES_UINT256 = "0x4903b0d1"
BALANCES_INT128 = "0x065a80d8"
COINS_UINT256 = "0xc6610657"  # coins(uint256) — verified on real fork (VIB-5539)
COINS_INT128 = "0x23746eb8"  # coins(int128)
TOTAL_SUPPLY = "0x18160ddd"
DECIMALS = "0x313ce567"


def _addr_word(address: str) -> int:
    """Encode an address as the uint256 the stub returns from a ``coins(i)`` read.

    The reader decodes the eth_call word with ``read_uint256_call`` and re-renders
    it as a 20-byte address; the stub builds the word from this int via
    ``_hex_word`` (right-aligned in 32 bytes), exactly as a real ``coins(i)``
    return is laid out.
    """
    return int(address, 16)


# steth (ethereum) — ETH / stETH crypto-family pool
POOL_STETH = "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"
LP_STETH = "0x06325440D014e39736583c165C2963BA99fAf14E"
ETH_NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
STETH_ADDR = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"

# frax_3crv (ethereum) — USD metapool [FRAX, 3CRV] over the 3pool base
POOL_FRAX3CRV = "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B"
LP_FRAX3CRV = "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B"
FRAX_ADDR = "0x853d955aCEf822Db058eb8505911ED77F175b99e"
DAI_ADDR = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
USDC_ADDR = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT_ADDR = "0xdAC17F958D2ee523a2206206994597C13D831ec7"


def _hex_word(value: int) -> str:
    return "0x" + hex(value)[2:].zfill(64)


class _FullCalldataRpcStub:
    """Routes eth_call by (to, FULL calldata) so multi-arg getters disambiguate.

    The base ``_StubRpcStub`` keys only on the 4-byte selector, which collapses
    ``balances(0)`` and ``balances(1)`` to one key. This variant keys on the
    entire calldata (selector + args), so per-index reserve reads route to
    distinct replies. ``replies`` maps ``(to_lower, data_lower)`` -> int | None;
    a missing key returns 0 (success), a ``None`` value simulates a failed read.
    """

    def __init__(self, replies: dict[tuple[str, str], int | None]) -> None:
        self._replies = {(t.lower(), d.lower()): v for (t, d), v in replies.items()}

    def Call(self, request: Any, timeout: float = 10.0) -> _StubResponse:  # noqa: N802, ARG002
        params = json.loads(request.params)
        call = params[0]
        key = (call["to"].lower(), call["data"].lower())
        reply = self._replies.get(key, 0)
        if reply is None:
            return _StubResponse(False, "")
        return _StubResponse(True, json.dumps(_hex_word(reply)))


class _FullCalldataGatewayClient:
    def __init__(self, replies: dict[tuple[str, str], int | None]) -> None:
        self._rpc_stub = _FullCalldataRpcStub(replies)

        class _Cfg:
            timeout = 10

        self.config = _Cfg()


def _balances_call(selector: str, index: int) -> str:
    return selector + hex(index)[2:].zfill(64)


class _StubResponse:
    def __init__(self, success: bool, result: str) -> None:
        self.success = success
        self.result = result
        self.error = "" if success else "stub failure"


class _StubRpcStub:
    """Routes eth_call by (to, selector) to a configured raw uint256 reply.

    ``replies`` maps ``(to_lower, selector_prefix)`` -> int | None. A ``None``
    reply simulates a failed read (response.success = False).
    """

    def __init__(self, replies: dict[tuple[str, str], int | None]) -> None:
        self._replies = replies

    def Call(self, request: Any, timeout: float = 10.0) -> _StubResponse:  # noqa: N802, ARG002
        params = json.loads(request.params)
        call = params[0]
        to = call["to"].lower()
        data = call["data"]
        selector = data[:10]
        reply = self._replies.get((to, selector), 0)
        if reply is None:
            return _StubResponse(False, "")
        return _StubResponse(True, json.dumps(_hex_word(reply)))


class _StubGatewayClient:
    def __init__(self, replies: dict[tuple[str, str], int | None]) -> None:
        self._rpc_stub = _StubRpcStub(replies)

        class _Cfg:
            timeout = 10

        self.config = _Cfg()


def _make_replies(*, lp_balance_wei: int | None, virtual_price_wei: int | None) -> dict[tuple[str, str], int | None]:
    return {
        (LP_3POOL.lower(), BALANCE_OF): lp_balance_wei,
        (POOL_3POOL.lower(), GET_VIRTUAL_PRICE): virtual_price_wei,
        (POOL_3POOL.lower(), VIRTUAL_PRICE_ALIAS): None,
    }


# ---------------------------------------------------------------------------
# Pool-metadata resolver
# ---------------------------------------------------------------------------


def test_resolve_pool_meta_by_name() -> None:
    meta = _resolve_curve_pool_meta("ethereum", pool="3pool", lp_token="")
    assert meta is not None
    assert meta["address"].lower() == POOL_3POOL.lower()
    assert meta["coins"] == ["DAI", "USDC", "USDT"]


def test_resolve_pool_meta_by_lp_token_address() -> None:
    meta = _resolve_curve_pool_meta("ethereum", pool="", lp_token=LP_3POOL)
    assert meta is not None
    assert meta["address"].lower() == POOL_3POOL.lower()


def test_resolve_pool_meta_unknown_returns_none() -> None:
    assert _resolve_curve_pool_meta("ethereum", pool="not_a_pool", lp_token="") is None


def test_resolve_pool_meta_stale_lp_token_falls_back_to_pool_address() -> None:
    # A stale/unknown lp_token address must NOT mask a resolvable pool address
    # (Gemini robustness fix): both candidate addresses are tried sequentially.
    meta = _resolve_curve_pool_meta(
        "ethereum",
        pool=POOL_3POOL,  # valid pool address
        lp_token="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",  # unknown address
    )
    assert meta is not None
    assert meta["address"].lower() == POOL_3POOL.lower()


# ---------------------------------------------------------------------------
# CurveLpPositionReader
# ---------------------------------------------------------------------------


def test_supports_only_curve() -> None:
    reader = CurveLpPositionReader(None)
    assert reader.supports("curve") is True
    assert reader.supports("CURVE") is True
    assert reader.supports("uniswap_v3") is False
    assert reader.supports("") is False


def test_read_position_no_gateway_returns_none() -> None:
    reader = CurveLpPositionReader(None)
    assert (
        reader.read_position(
            protocol="curve",
            chain="ethereum",
            pool="3pool",
            lp_token=LP_3POOL,
            wallet_address=WALLET,
        )
        is None
    )


def test_read_position_3pool_live_virtual_price() -> None:
    # 10 LP tokens, virtual_price 1.0196 -> value ~ 10.196
    reader = CurveLpPositionReader(
        _StubGatewayClient(_make_replies(lp_balance_wei=10 * 10**18, virtual_price_wei=1_019_566_780_337_011_070))
    )
    pos = reader.read_position(
        protocol="curve",
        chain="ethereum",
        pool="3pool",
        lp_token=LP_3POOL,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert pos.is_active
    assert pos.lp_balance_wei == 10 * 10**18
    assert pos.virtual_price == Decimal("1019566780337011070") / Decimal(10**18)
    assert pos.coins == ["DAI", "USDC", "USDT"]


def test_read_position_measured_zero_balance() -> None:
    reader = CurveLpPositionReader(_StubGatewayClient(_make_replies(lp_balance_wei=0, virtual_price_wei=10**18)))
    pos = reader.read_position(
        protocol="curve",
        chain="ethereum",
        pool="3pool",
        lp_token=LP_3POOL,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert not pos.is_active
    assert pos.lp_balance_wei == 0


def test_read_position_unmeasured_balance_returns_none() -> None:
    # balanceOf read fails -> None (Empty != Zero), never a fabricated 0.
    reader = CurveLpPositionReader(_StubGatewayClient(_make_replies(lp_balance_wei=None, virtual_price_wei=10**18)))
    assert (
        reader.read_position(
            protocol="curve",
            chain="ethereum",
            pool="3pool",
            lp_token=LP_3POOL,
            wallet_address=WALLET,
        )
        is None
    )


def test_read_position_unmeasured_virtual_price_returns_none() -> None:
    reader = CurveLpPositionReader(_StubGatewayClient(_make_replies(lp_balance_wei=10**18, virtual_price_wei=None)))
    assert (
        reader.read_position(
            protocol="curve",
            chain="ethereum",
            pool="3pool",
            lp_token=LP_3POOL,
            wallet_address=WALLET,
        )
        is None
    )


def test_read_position_virtual_price_alias_fallback() -> None:
    # Primary get_virtual_price() unreadable; alias virtual_price() succeeds.
    replies = {
        (LP_3POOL.lower(), BALANCE_OF): 5 * 10**18,
        (POOL_3POOL.lower(), GET_VIRTUAL_PRICE): None,
        (POOL_3POOL.lower(), VIRTUAL_PRICE_ALIAS): 10**18,  # 1.0
    }
    reader = CurveLpPositionReader(_StubGatewayClient(replies))
    pos = reader.read_position(
        protocol="curve",
        chain="ethereum",
        pool="3pool",
        lp_token=LP_3POOL,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert pos.virtual_price == Decimal("1")


def test_read_position_base_4pool_usdbc_axlusdc_values() -> None:
    # Audit P0-3 regression: Base 4pool (USDC/USDbC/axlUSDC/crvUSD) is a PLAIN
    # USD-stable pool. USDbC and axlUSDC are 1:1 USDC wrappers, so it must now
    # value at lp_balance * virtual_price * $1 — previously fail-closed because
    # the two wrapped-USDC symbols were missing from the allowlist.
    reader = CurveLpPositionReader(
        _StubGatewayClient(
            {
                (LP_4POOL_BASE.lower(), BALANCE_OF): 10 * 10**18,
                (POOL_4POOL_BASE.lower(), GET_VIRTUAL_PRICE): 1_019_566_780_337_011_070,
                (POOL_4POOL_BASE.lower(), VIRTUAL_PRICE_ALIAS): None,
            }
        )
    )
    pos = reader.read_position(
        protocol="curve",
        chain="base",
        pool="4pool",
        lp_token=LP_4POOL_BASE,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert pos.is_active
    assert pos.lp_balance_wei == 10 * 10**18
    assert pos.virtual_price == Decimal("1019566780337011070") / Decimal(10**18)
    assert pos.coins == ["USDC", "USDbC", "axlUSDC", "crvUSD"]


def test_usdbc_and_axlusdc_in_usd_stable_allowlist() -> None:
    # Peg justification (audit P0-3): both are 1:1 USDC wrappers. The allowlist
    # check upper-cases coin symbols, so the canonical-cased entries must match
    # the registry's mixed-case "USDbC" / "axlUSDC".
    from almanak.framework.valuation.curve_lp_position_reader import _USD_STABLE_SYMBOLS

    assert "USDbC".upper() in _USD_STABLE_SYMBOLS
    assert "axlUSDC".upper() in _USD_STABLE_SYMBOLS


# ── VIB-5428 — crypto / non-USD pool spot-reserves reads ──────────────────────


def _steth_crypto_replies(
    *,
    lp_balance_wei: int | None = 2 * 10**18,
    total_supply_wei: int | None = 200 * 10**18,
    reserve_eth_wei: int | None = 100 * 10**18,
    reserve_steth_wei: int | None = 110 * 10**18,
    steth_decimals: int | None = 18,
    coin0_addr: str | None = ETH_NATIVE,
    coin1_addr: str | None = STETH_ADDR,
) -> dict[tuple[str, str], int | None]:
    # Real steth resolves on balances(uint256) AND coins(uint256); the int128
    # overloads revert (None) on this pool — matches the real-fork report. ETH
    # (coin 0) is the native sentinel, so its decimals are NOT read (18 by
    # definition) and coins(0) returns the native-ETH placeholder. The coins(i)
    # replies feed the VIB-5539 on-chain coin-order validation; ``coin0_addr`` /
    # ``coin1_addr`` let a test transpose or drop a coin read to exercise the
    # fail-closed path. (The coins(int128) selector fallback is covered separately
    # by test_crypto_coins_int128_selector_fallback below.)
    return {
        (LP_STETH, BALANCE_OF + WALLET.lower().removeprefix("0x").zfill(64)): lp_balance_wei,
        (LP_STETH, TOTAL_SUPPLY): total_supply_wei,
        (POOL_STETH, _balances_call(COINS_UINT256, 0)): None if coin0_addr is None else _addr_word(coin0_addr),
        (POOL_STETH, _balances_call(COINS_INT128, 0)): None,
        (POOL_STETH, _balances_call(COINS_UINT256, 1)): None if coin1_addr is None else _addr_word(coin1_addr),
        (POOL_STETH, _balances_call(COINS_INT128, 1)): None,
        (POOL_STETH, _balances_call(BALANCES_UINT256, 0)): reserve_eth_wei,
        (POOL_STETH, _balances_call(BALANCES_INT128, 0)): None,
        (POOL_STETH, _balances_call(BALANCES_UINT256, 1)): reserve_steth_wei,
        (POOL_STETH, _balances_call(BALANCES_INT128, 1)): None,
        (STETH_ADDR, DECIMALS): steth_decimals,
    }


def test_read_position_steth_crypto_family_reads_reserves() -> None:
    # steth (ETH/stETH) is now a CRYPTO-family pool (VIB-5428): the reader reads
    # spot reserves + supply + decimals via the gateway seam and returns a
    # crypto-family position (was fail-closed "out of scope" before).
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_steth_crypto_replies()))
    pos = reader.read_position(
        protocol="curve",
        chain="ethereum",
        pool="steth",
        lp_token=LP_STETH,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert pos.family == "crypto"
    assert pos.lp_balance_wei == 2 * 10**18
    assert pos.total_supply_wei == 200 * 10**18
    assert pos.reserves_wei == [100 * 10**18, 110 * 10**18]  # balances(uint256) resolved
    assert pos.coin_decimals == [18, 18]  # native ETH → 18 without a call
    assert pos.coins == ["ETH", "stETH"]


def _int128_fallback_replies() -> dict[tuple[str, str], int | None]:
    # SYNTHETIC selector scenario (NOT real steth — see
    # test_read_position_steth_crypto_family_reads_reserves, where real steth
    # resolves on balances(uint256)). Models a hypothetical pre-NG pool that
    # exposes ONLY balances(int128): balances(uint256) reverts (None), so the
    # reader's selector-probe must fall back to the int128 overload. Reuses the
    # steth registry fixture purely for its pool/coin wiring.
    return {
        (LP_STETH, BALANCE_OF + WALLET.lower().removeprefix("0x").zfill(64)): 2 * 10**18,
        (LP_STETH, TOTAL_SUPPLY): 200 * 10**18,
        # coins resolve on uint256 (the int128-balances overload is the variable
        # under test here, not the coins selector).
        (POOL_STETH, _balances_call(COINS_UINT256, 0)): _addr_word(ETH_NATIVE),
        (POOL_STETH, _balances_call(COINS_UINT256, 1)): _addr_word(STETH_ADDR),
        (POOL_STETH, _balances_call(BALANCES_UINT256, 0)): None,
        (POOL_STETH, _balances_call(BALANCES_INT128, 0)): 100 * 10**18,
        (POOL_STETH, _balances_call(BALANCES_UINT256, 1)): None,
        (POOL_STETH, _balances_call(BALANCES_INT128, 1)): 110 * 10**18,
        (STETH_ADDR, DECIMALS): 18,
    }


def test_crypto_balances_int128_selector_fallback() -> None:
    # Exercises the reader's balances(uint256)→balances(int128) selector-probe
    # fallback for a hypothetical pre-NG pool (real steth uses uint256). The mark
    # must come out identical regardless of which overload the pool exposes.
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_int128_fallback_replies()))
    pos = reader.read_position(
        protocol="curve",
        chain="ethereum",
        pool="steth",
        lp_token=LP_STETH,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert pos.family == "crypto"
    assert pos.reserves_wei == [100 * 10**18, 110 * 10**18]  # balances(int128) overload resolved


@pytest.mark.parametrize(
    "override",
    [
        {"total_supply_wei": None},  # supply unreadable
        {"total_supply_wei": 0},  # non-positive supply
        {"reserve_steth_wei": None},  # a reserve leg unreadable
        {"steth_decimals": None},  # a coin's decimals unreadable
    ],
)
def test_read_position_crypto_fails_closed_on_unreadable_input(override: dict[str, int | None]) -> None:
    # Empty ≠ Zero: any missing spot-reserves input → None (UNAVAILABLE), never a
    # partial / fabricated mark.
    replies = _steth_crypto_replies(**override)  # type: ignore[arg-type]
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    assert (
        reader.read_position(
            protocol="curve",
            chain="ethereum",
            pool="steth",
            lp_token=LP_STETH,
            wallet_address=WALLET,
        )
        is None
    )


# ── VIB-5539 — on-chain coins(i) order validation (fail-closed) ───────────────


def test_crypto_coin_order_match_returns_position_unchanged() -> None:
    # (a) When on-chain coins(i) match the registry coin_addresses order, the
    # position is returned exactly as before this gate existed — the reserves,
    # decimals, supply and coin set are untouched. Validation is a pure
    # safety gate, a no-op on the happy path.
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_steth_crypto_replies()))
    pos = reader.read_position(
        protocol="curve", chain="ethereum", pool="steth", lp_token=LP_STETH, wallet_address=WALLET
    )
    assert pos is not None
    assert pos.family == "crypto"
    assert pos.coin_addresses == [ETH_NATIVE, STETH_ADDR]
    assert pos.reserves_wei == [100 * 10**18, 110 * 10**18]
    assert pos.coin_decimals == [18, 18]
    assert pos.total_supply_wei == 200 * 10**18


def test_crypto_native_eth_placeholder_coin0_validates() -> None:
    # (d) The steth pool's coins(0) is the native-ETH sentinel 0xEeee…EEeE, which
    # the registry carries verbatim; a lowercased compare matches with no special
    # case. (The reply uses the checksummed literal — the reader lowercases both
    # sides — so this proves the case-insensitive compare, not a same-case fluke.)
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_steth_crypto_replies(coin0_addr=ETH_NATIVE.lower())))
    pos = reader.read_position(
        protocol="curve", chain="ethereum", pool="steth", lp_token=LP_STETH, wallet_address=WALLET
    )
    assert pos is not None
    assert pos.coin_addresses[0].lower() == ETH_NATIVE.lower()


def test_crypto_coin_order_transposed_fails_closed() -> None:
    # (b) On-chain coins(i) transposed vs the registry (coins(0)=stETH while the
    # registry says coins(0)=ETH) → None. A confident wrong mark (an 18-dec stETH
    # reserve priced as ETH and vice-versa) is worse than UNAVAILABLE.
    replies = _steth_crypto_replies(coin0_addr=STETH_ADDR, coin1_addr=ETH_NATIVE)
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    assert (
        reader.read_position(protocol="curve", chain="ethereum", pool="steth", lp_token=LP_STETH, wallet_address=WALLET)
        is None
    )


def test_crypto_coin_read_miss_fails_closed() -> None:
    # (c) A coins(i) read miss (gateway blip — neither uint256 nor int128 resolves)
    # → None. Empty ≠ Zero: an unmeasured coin address cannot confirm the order,
    # so the mark must not be produced.
    replies = _steth_crypto_replies(coin1_addr=None)
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    assert (
        reader.read_position(protocol="curve", chain="ethereum", pool="steth", lp_token=LP_STETH, wallet_address=WALLET)
        is None
    )


def _coins_int128_fallback_replies() -> dict[tuple[str, str], int | None]:
    # SYNTHETIC selector scenario: a hypothetical pre-NG pool that exposes ONLY
    # coins(int128) (coins(uint256) reverts → None), so the reader's coins
    # selector-probe must fall back to the int128 overload. Reserves resolve on
    # uint256 (orthogonal to the coins selector under test).
    return {
        (LP_STETH, BALANCE_OF + WALLET.lower().removeprefix("0x").zfill(64)): 2 * 10**18,
        (LP_STETH, TOTAL_SUPPLY): 200 * 10**18,
        (POOL_STETH, _balances_call(COINS_UINT256, 0)): None,
        (POOL_STETH, _balances_call(COINS_INT128, 0)): _addr_word(ETH_NATIVE),
        (POOL_STETH, _balances_call(COINS_UINT256, 1)): None,
        (POOL_STETH, _balances_call(COINS_INT128, 1)): _addr_word(STETH_ADDR),
        (POOL_STETH, _balances_call(BALANCES_UINT256, 0)): 100 * 10**18,
        (POOL_STETH, _balances_call(BALANCES_UINT256, 1)): 110 * 10**18,
        (STETH_ADDR, DECIMALS): 18,
    }


def test_crypto_coins_int128_selector_fallback() -> None:
    # (e) Exercises the coins(uint256)→coins(int128) selector-probe fallback for a
    # hypothetical pre-NG pool. Validation must pass identically regardless of
    # which coins overload the pool exposes.
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_coins_int128_fallback_replies()))
    pos = reader.read_position(
        protocol="curve", chain="ethereum", pool="steth", lp_token=LP_STETH, wallet_address=WALLET
    )
    assert pos is not None
    assert pos.family == "crypto"
    assert pos.coin_addresses == [ETH_NATIVE, STETH_ADDR]
    assert pos.reserves_wei == [100 * 10**18, 110 * 10**18]


def test_crypto_coin_order_validated_once_then_cached() -> None:
    # Coin order is immutable post-deployment, so a SUCCESSFUL validation is
    # memoised per (chain, pool): the second valuation must NOT re-read coins(i)
    # (zero new information, N gateway RPCs saved per snapshot). steth has 2 coins
    # → 2 coins(i) reads on the first valuation, 0 on the second.
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_steth_crypto_replies()))
    calls = {"n": 0}
    orig = reader._read_pool_coin_address

    def _counting(chain: str, pool_address: str, index: int) -> str | None:
        calls["n"] += 1
        return orig(chain, pool_address, index)

    reader._read_pool_coin_address = _counting  # type: ignore[method-assign]

    kw = {"protocol": "curve", "chain": "ethereum", "pool": "steth", "lp_token": LP_STETH, "wallet_address": WALLET}
    assert reader.read_position(**kw) is not None
    first = calls["n"]
    assert first == 2  # coins(0), coins(1)
    assert ("ethereum", POOL_STETH.lower()) in reader._validated_coin_order
    assert reader.read_position(**kw) is not None
    assert calls["n"] == first  # cache hit — no further coins(i) reads


def test_crypto_coin_order_failure_not_cached() -> None:
    # A mismatch must NOT be memoised: it stays fail-closed AND is re-validated on
    # the next valuation (a transient gateway blip / a wrong order must never be
    # remembered as a pass). Transposed coins → fails at index 0 each time.
    replies = _steth_crypto_replies(coin0_addr=STETH_ADDR, coin1_addr=ETH_NATIVE)
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    calls = {"n": 0}
    orig = reader._read_pool_coin_address

    def _counting(chain: str, pool_address: str, index: int) -> str | None:
        calls["n"] += 1
        return orig(chain, pool_address, index)

    reader._read_pool_coin_address = _counting  # type: ignore[method-assign]

    kw = {"protocol": "curve", "chain": "ethereum", "pool": "steth", "lp_token": LP_STETH, "wallet_address": WALLET}
    assert reader.read_position(**kw) is None
    after_first = calls["n"]
    assert after_first >= 1
    assert ("ethereum", POOL_STETH.lower()) not in reader._validated_coin_order
    assert reader.read_position(**kw) is None  # still fail-closed
    assert calls["n"] > after_first  # re-validated, NOT cached


# ---------------------------------------------------------------------------
# PortfolioValuer._reprice_curve_lp_enriched
# ---------------------------------------------------------------------------


def _curve_position(details: dict[str, Any]) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=f"curve_3pool_{LP_3POOL}",
        chain="ethereum",
        protocol="curve",
        value_usd=Decimal("10"),
        details=details,
    )


class _StubMarket:
    """Minimal ``MarketDataSource`` stub for the depeg cross-check (VIB-5426).

    Resolves ``price(token)`` by SYMBOL (upper-cased); an unknown key (e.g. a
    coin ADDRESS, which the valuer tries first) raises, so the valuer's
    address→symbol fallback resolves to the symbol price. A symbol mapped to
    ``None`` raises too (simulates an oracle miss for that coin).
    """

    def __init__(self, prices: dict[str, Decimal | None]) -> None:
        self._prices = {k.upper(): v for k, v in prices.items()}

    def price(self, token: str, quote: str = "USD", *, chain: str | None = None) -> Decimal:
        # Mirrors the real MarketSnapshot.price signature (chain keyword-only,
        # VIB-5722): the chain-threaded ``_price_curve_coins`` passes chain=. The
        # stub prices by symbol and ignores chain — behaviour unchanged.
        value = self._prices.get(str(token).upper())
        if value is None:
            raise KeyError(token)
        return value


def _usd_market(**overrides: str | None) -> _StubMarket:
    """A healthy USD 3pool oracle (DAI/USDC/USDT ≈ $1), with per-coin overrides:
    ``_usd_market(USDT="0.90")`` depegs USDT; ``_usd_market(USDT=None)`` makes it
    unpriceable."""
    prices: dict[str, Decimal | None] = {
        "DAI": Decimal("1.0"),
        "USDC": Decimal("1.0"),
        "USDT": Decimal("1.0"),
    }
    for sym, val in overrides.items():
        prices[sym.upper()] = None if val is None else Decimal(val)
    return _StubMarket(prices)


def test_valuer_curve_branch_values_with_virtual_price() -> None:
    valuer = PortfolioValuer(
        _StubGatewayClient(_make_replies(lp_balance_wei=10 * 10**18, virtual_price_wei=1_019_566_780_337_011_070))
    )
    pos = _curve_position({"pool": "3pool", "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"], "wallet": WALLET})
    # VIB-5426: a healthy USD pool (oracle confirms the $1 peg) marks at par as
    # before. The cross-check now requires an independent oracle, so the test
    # supplies one — the par-mark behaviour is unchanged on a confirmed peg.
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=_usd_market())  # type: ignore[arg-type]
    assert repriced is True
    # 10 * 1.019566... ~= 10.1957
    assert value_usd == Decimal("10") * (Decimal("1019566780337011070") / Decimal(10**18))
    assert details["valuation_source"] == "curve_virtual_price"
    assert details["virtual_price"] == str(Decimal("1019566780337011070") / Decimal(10**18))
    assert details["liquidity"] == str(10 * 10**18)
    assert details["peg_usd"] == "1"
    # The peg was actively verified against the oracle, not assumed.
    assert details["oracle_peg_usd"] == "1.0"
    assert details["depeg_divergence_bps"] == "0"
    assert "valuation_status" not in details  # HIGH confidence, no degradation


def test_valuer_curve_branch_uses_strategy_wallet_fallback() -> None:
    # Details omit a wallet (the lp_curve fixture's get_open_positions shape);
    # the valuer falls back to the cached strategy wallet.
    valuer = PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=10**18, virtual_price_wei=10**18)))
    valuer._strategy_wallet_address = WALLET
    pos = _curve_position({"pool": "3pool", "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"]})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=_usd_market())  # type: ignore[arg-type]
    assert repriced is True
    assert value_usd == Decimal("1")


def test_valuer_curve_branch_no_wallet_fails_closed() -> None:
    # No wallet anywhere -> UNAVAILABLE (repriced False), never a stale estimate.
    valuer = PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=10**18, virtual_price_wei=10**18)))
    pos = _curve_position({"pool": "3pool", "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"]})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=None)  # type: ignore[arg-type]
    assert repriced is False
    assert details == {}


def test_valuer_curve_branch_empty_position_measured_zero() -> None:
    valuer = PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=0, virtual_price_wei=10**18)))
    pos = _curve_position({"pool": "3pool", "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"], "wallet": WALLET})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=None)  # type: ignore[arg-type]
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["liquidity"] == "0"


def test_valuer_curve_branch_unmeasured_fails_closed_not_zero() -> None:
    # virtual_price unreadable -> the position is NOT booked as $0; it is
    # UNAVAILABLE (repriced False). Empty != Zero.
    valuer = PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=10**18, virtual_price_wei=None)))
    pos = _curve_position({"pool": "3pool", "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"], "wallet": WALLET})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=None)  # type: ignore[arg-type]
    assert repriced is False
    assert details == {}


def test_valuer_metapool_shape_usd_pegged_values() -> None:
    # A crvUSD/USDC StableSwap pool (USD-pegged base + coin) values identically.
    # Resolve a real USD-stable pool from the registry if one exists; otherwise
    # this asserts the USD-stable allowlist accepts crvUSD.
    from almanak.framework.valuation.curve_lp_position_reader import _USD_STABLE_SYMBOLS

    assert "CRVUSD" in _USD_STABLE_SYMBOLS
    assert "USDC" in _USD_STABLE_SYMBOLS


# ── VIB-5426 / audit P0-2 — oracle-vs-pool depeg cross-check ──────────────────


def _active_3pool_valuer() -> PortfolioValuer:
    return PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=10 * 10**18, virtual_price_wei=10**18)))


def _3pool_pos() -> PositionInfo:
    return _curve_position({"pool": "3pool", "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"], "wallet": WALLET})


def test_curve_depeg_fires_unavailable() -> None:
    # USDT depegs to $0.90 (1000 bps off the $1 peg) — virtual_price is blind to
    # it, but the oracle cross-check fires: the position degrades to a no_path
    # marker (value 0, NOT par), never booking $10 of bleeding value at par.
    valuer = _active_3pool_valuer()
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _3pool_pos(),
        "ethereum",
        market=_usd_market(USDT="0.90"),  # type: ignore[arg-type]
    )
    assert repriced is True  # a marker tuple, not a None fall-through
    assert value_usd == Decimal("0")  # kept OUT of the NAV sum (Empty ≠ Zero)
    assert details["valuation_status"] == "no_path"
    assert details["mark_unmeasured"] is True
    assert details["unavailable_reason"] == "curve_oracle_depeg_divergence"
    assert details["depeg_divergence_bps"] == "1000"
    assert details["depeg_threshold_bps"] == "100"


def test_curve_systemic_depeg_fires() -> None:
    # ALL coins fall to $0.90 together — the inter-coin spread is 0 (median moves
    # with them), so only the peg-LEVEL check vs the expected $1 numeraire catches
    # it. Proves the systemic guard the median-relative check alone would miss.
    valuer = _active_3pool_valuer()
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _3pool_pos(),
        "ethereum",
        market=_usd_market(DAI="0.90", USDC="0.90", USDT="0.90"),  # type: ignore[arg-type]
    )
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["unavailable_reason"] == "curve_oracle_depeg_divergence"
    assert details["depeg_divergence_bps"] == "1000"


def test_curve_oracle_miss_distinct_from_depeg() -> None:
    # One coin unpriceable (oracle outage) — degrade to UNAVAILABLE, but with the
    # honest reason "price_unavailable", NEVER mis-blamed as a depeg.
    valuer = _active_3pool_valuer()
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _3pool_pos(),
        "ethereum",
        market=_usd_market(USDT=None),  # type: ignore[arg-type]
    )
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["valuation_status"] == "no_path"
    assert details["unavailable_reason"] == "curve_oracle_price_unavailable"


def test_curve_no_market_fails_closed() -> None:
    # No oracle at all — the cross-check cannot run, so the position is unmeasured
    # (UNAVAILABLE), never par-marked. A valuation path that trusts par without an
    # oracle is exactly the P0-2 anti-pattern.
    valuer = _active_3pool_valuer()
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _3pool_pos(),
        "ethereum",
        market=None,  # type: ignore[arg-type]
    )
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["unavailable_reason"] == "curve_oracle_price_unavailable"


def test_curve_intent_threshold_override() -> None:
    # 150 bps divergence: fires under the 100-bps default, passes under a 200-bps
    # per-intent override (a deployment that knowingly tolerates a wider band).
    valuer = _active_3pool_valuer()
    market = _usd_market(USDT="0.985")  # 150 bps off the $1 peg

    _, default_details, _ = valuer._reprice_lp_enriched_dispatch(
        _3pool_pos(),
        "ethereum",
        market=market,  # type: ignore[arg-type]
    )
    assert default_details["valuation_status"] == "no_path"
    assert default_details["depeg_divergence_bps"] == "150"

    pos = _curve_position(
        {
            "pool": "3pool",
            "lp_token": LP_3POOL,
            "coins": ["DAI", "USDC", "USDT"],
            "wallet": WALLET,
            "depeg_threshold_bps": "200",
        }
    )
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        pos,
        "ethereum",
        market=market,  # type: ignore[arg-type]
    )
    assert repriced is True
    assert value_usd == Decimal("10")  # par mark — peg within the tolerated band
    assert details["depeg_threshold_bps"] == "200"
    assert "valuation_status" not in details


def test_curve_depeg_marker_forces_snapshot_unavailable() -> None:
    # The marker's job is to force the WHOLE snapshot to UNAVAILABLE so the
    # drawdown fold sees "blind", not "safe at par". Confirm the confidence fold
    # keys on the no_path the depeg path stamps.
    from almanak.framework.valuation.portfolio_valuer import PositionValue, ValueConfidence

    depegged = PositionValue(
        position_type=PositionType.LP,
        protocol="curve",
        chain="ethereum",
        value_usd=Decimal("0"),
        label="curve LP",
        tokens=[],
        details={"valuation_status": "no_path", "unavailable_reason": "curve_oracle_depeg_divergence"},
    )
    conf = PortfolioValuer._determine_value_confidence(
        positions=[depegged],
        wallet_balances=[],
        positions_unavailable=False,
        wallet_data_incomplete=False,
    )
    assert conf == ValueConfidence.UNAVAILABLE


# ── VIB-5427 — USD metapool base-LP decomposition ─────────────────────────────


def _frax3crv_replies(
    *,
    lp_balance_wei: int | None = 10 * 10**18,
    metapool_vp_wei: int | None = 1_020_500_000_000_000_000,  # 1.0205
    base_vp_wei: int | None = 1_030_000_000_000_000_000,  # 1.03
) -> dict[tuple[str, str], int | None]:
    return {
        (LP_FRAX3CRV.lower(), BALANCE_OF): lp_balance_wei,
        (POOL_FRAX3CRV.lower(), GET_VIRTUAL_PRICE): metapool_vp_wei,
        (POOL_3POOL.lower(), GET_VIRTUAL_PRICE): base_vp_wei,
    }


def _frax3crv_market(**overrides: str | None) -> _StubMarket:
    prices: dict[str, Decimal | None] = {
        "FRAX": Decimal("1.0"),
        "DAI": Decimal("1.0"),
        "USDC": Decimal("1.0"),
        "USDT": Decimal("1.0"),
    }
    for sym, val in overrides.items():
        prices[sym.upper()] = None if val is None else Decimal(val)
    return _StubMarket(prices)


def _frax3crv_position() -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=f"curve_frax3crv_{LP_FRAX3CRV}",
        chain="ethereum",
        protocol="curve",
        value_usd=Decimal("10"),
        details={"pool": "frax_3crv", "lp_token": LP_FRAX3CRV, "wallet": WALLET},
    )


def test_read_position_metapool_family_expands_underlying() -> None:
    # balanceOf calldata carries the wallet arg → use the full-calldata stub.
    replies = _frax3crv_replies()
    replies.pop((LP_FRAX3CRV.lower(), BALANCE_OF))
    replies[(LP_FRAX3CRV.lower(), BALANCE_OF + WALLET.lower().removeprefix("0x").zfill(64))] = 10 * 10**18
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    pos = reader.read_position(
        protocol="curve", chain="ethereum", pool="frax_3crv", lp_token=LP_FRAX3CRV, wallet_address=WALLET
    )
    assert pos is not None
    assert pos.family == "metapool_usd"
    assert pos.virtual_price == Decimal("1.0205")
    assert pos.base_pool_virtual_price == Decimal("1.03")
    # The base-LP leg (3CRV) is expanded into the 3pool's underlying stables; the
    # un-priceable LP token symbol is NOT in the depeg coin set.
    assert pos.underlying_coins == ["FRAX", "DAI", "USDC", "USDT"]
    assert "3CRV" not in pos.underlying_coins
    assert pos.underlying_coin_addresses == [FRAX_ADDR, DAI_ADDR, USDC_ADDR, USDT_ADDR]


def test_valuer_metapool_values_at_metapool_virtual_price() -> None:
    valuer = PortfolioValuer(_StubGatewayClient(_frax3crv_replies()))
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _frax3crv_position(), "ethereum", market=_frax3crv_market()
    )
    assert repriced is True
    # lp(10) × metapool vp(1.0205) × $1 — the metapool vp already incorporates the
    # base pool, so we do NOT multiply by base_vp again.
    assert value_usd == Decimal("10") * Decimal("1.0205")
    assert details["valuation_source"] == "curve_virtual_price"
    assert details["base_pool_virtual_price"] == "1.03"
    assert details["underlying_coins"] == ["FRAX", "DAI", "USDC", "USDT"]
    assert "valuation_status" not in details


def test_valuer_metapool_depeg_on_base_coin_fires() -> None:
    # A base-pool coin (USDT) depegs to $0.90 — caught only because the metapool
    # path expands the base-LP leg into [DAI, USDC, USDT]; degrade to UNAVAILABLE.
    valuer = PortfolioValuer(_StubGatewayClient(_frax3crv_replies()))
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _frax3crv_position(), "ethereum", market=_frax3crv_market(USDT="0.90")
    )
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["valuation_status"] == "no_path"
    # depeg (not oracle_unavailable) proves the base coins WERE priced and 3CRV
    # was not required.
    assert details["unavailable_reason"] == "curve_oracle_depeg_divergence"
    assert details["depeg_divergence_bps"] == "1000"


def test_valuer_metapool_meta_coin_depeg_fires() -> None:
    # The meta coin (FRAX) itself depegs — also caught.
    valuer = PortfolioValuer(_StubGatewayClient(_frax3crv_replies()))
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _frax3crv_position(), "ethereum", market=_frax3crv_market(FRAX="0.92")
    )
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["unavailable_reason"] == "curve_oracle_depeg_divergence"


# ── VIB-5428 — crypto pool spot-reserves valuation (valuer) ───────────────────


def _steth_position() -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=f"curve_steth_{LP_STETH}",
        chain="ethereum",
        protocol="curve",
        value_usd=Decimal("6000"),
        details={"pool": "steth", "lp_token": LP_STETH, "wallet": WALLET},
    )


def test_valuer_crypto_values_from_spot_reserves() -> None:
    valuer = PortfolioValuer(_FullCalldataGatewayClient(_steth_crypto_replies()))
    market = _StubMarket({"ETH": Decimal("3000"), "STETH": Decimal("2990")})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(_steth_position(), "ethereum", market=market)
    assert repriced is True
    # ownership = 2/200 = 1%; reserves $ = 100×3000 + 110×2990 = 628_900;
    # value = 0.01 × 628_900 = 6_289.
    assert value_usd == Decimal("6289")
    assert details["valuation_source"] == "curve_spot_reserves"
    assert details["total_supply"] == str(200 * 10**18)
    assert details["coin_prices_usd"] == ["3000", "2990"]
    assert "valuation_status" not in details


def test_valuer_crypto_native_eth_prices_via_weth_fallback() -> None:
    # The steth pool's coin 0 is native ETH (sentinel address, symbol "ETH") —
    # not an ERC-20 the oracle prices by address. With ONLY a WETH price in the
    # oracle (no "ETH" key), the native-ETH leg must still resolve via the WETH
    # fallback (ETH ≈ WETH), never silently dropped (a dropped leg under-values).
    valuer = PortfolioValuer(_FullCalldataGatewayClient(_steth_crypto_replies()))
    market = _StubMarket({"WETH": Decimal("3000"), "STETH": Decimal("2990")})  # no "ETH" key
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(_steth_position(), "ethereum", market=market)
    assert repriced is True
    # ownership 1% × (100×3000 + 110×2990) = 0.01 × 628_900 = 6_289 — ETH leg priced off WETH.
    assert value_usd == Decimal("6289")
    assert details["coin_prices_usd"] == ["3000", "2990"]


def test_valuer_crypto_native_eth_unpriceable_fails_closed() -> None:
    # Neither ETH nor WETH priceable → fail closed, NOT a dropped leg.
    valuer = PortfolioValuer(_FullCalldataGatewayClient(_steth_crypto_replies()))
    market = _StubMarket({"STETH": Decimal("2990")})  # no ETH and no WETH
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(_steth_position(), "ethereum", market=market)
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["unavailable_reason"] == "curve_oracle_price_unavailable"


def test_price_curve_coins_real_address_eth_does_not_proxy_weth() -> None:
    # A REAL ERC-20 named "ETH" (non-sentinel address) must NOT take the WETH
    # proxy (CodeRabbit #2) — it must price by its own address only. With only a
    # WETH price in the oracle, it stays unpriced (None), never mis-priced as WETH.
    valuer = PortfolioValuer(_StubGatewayClient({}))
    real_eth_token = "0x1111111111111111111111111111111111111111"  # NOT a native sentinel
    prices = valuer._price_curve_coins(["ETH"], [real_eth_token], "ethereum", _StubMarket({"WETH": Decimal("3000")}))
    assert prices == [None]  # no proxy — real-address token doesn't borrow the WETH price


def test_price_curve_coins_sentinel_eth_proxies_weth() -> None:
    # A native-sentinel-address ETH leg DOES proxy to WETH.
    valuer = PortfolioValuer(_StubGatewayClient({}))
    prices = valuer._price_curve_coins(["ETH"], [ETH_NATIVE], "ethereum", _StubMarket({"WETH": Decimal("3000")}))
    assert prices == [Decimal("3000")]
    # A missing/empty address also proxies (genuinely unknown native leg).
    prices_empty = valuer._price_curve_coins(["ETH"], [""], "ethereum", _StubMarket({"WETH": Decimal("3000")}))
    assert prices_empty == [Decimal("3000")]


def _crypto_position_obj(**overrides: Any) -> CurveLpPosition:
    base = {
        "lp_token": LP_STETH,
        "pool_address": POOL_STETH,
        "lp_balance_wei": 2 * 10**18,
        "virtual_price": Decimal("0"),
        "coins": ["ETH", "stETH"],
        "coin_addresses": [ETH_NATIVE, STETH_ADDR],
        "family": "crypto",
        "total_supply_wei": 200 * 10**18,
        "reserves_wei": [100 * 10**18, 110 * 10**18],
        "coin_decimals": [18, 18],
    }
    base.update(overrides)
    return CurveLpPosition(**base)  # type: ignore[arg-type]


def test_value_curve_crypto_length_mismatch_fails_closed_no_path() -> None:
    # reserves/decimals not aligned with coins → no_path marker (NOT bare None,
    # which would let the stale strategy estimate leak into NAV). Audit #2 / CR #1.
    valuer = PortfolioValuer(_StubGatewayClient({}))
    on_chain = _crypto_position_obj(coin_decimals=[18])  # 1 decimal for 2 coins
    market = _StubMarket({"ETH": Decimal("3000"), "STETH": Decimal("2990"), "WETH": Decimal("3000")})
    value_usd, details = valuer._value_curve_crypto(_steth_position(), on_chain, "ethereum", market)  # type: ignore[misc]
    assert value_usd == Decimal("0")
    assert details["valuation_status"] == "no_path"
    assert details["unavailable_reason"] == "curve_spot_reserves_read_incomplete"


def test_value_curve_crypto_nonpositive_fails_closed_no_path() -> None:
    # All reserves zero → gross value 0 → no_path marker (NOT bare None).
    valuer = PortfolioValuer(_StubGatewayClient({}))
    on_chain = _crypto_position_obj(reserves_wei=[0, 0])
    market = _StubMarket({"ETH": Decimal("3000"), "STETH": Decimal("2990"), "WETH": Decimal("3000")})
    value_usd, details = valuer._value_curve_crypto(_steth_position(), on_chain, "ethereum", market)  # type: ignore[misc]
    assert value_usd == Decimal("0")
    assert details["valuation_status"] == "no_path"
    assert details["unavailable_reason"] == "curve_spot_reserves_nonpositive"


def test_valuer_crypto_unpriceable_coin_fails_closed() -> None:
    # stETH unpriceable (oracle miss) — no $1 peg to fall back on, so degrade to a
    # no_path UNAVAILABLE marker, never a partial mark.
    valuer = PortfolioValuer(_FullCalldataGatewayClient(_steth_crypto_replies()))
    market = _StubMarket({"ETH": Decimal("3000"), "STETH": None})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(_steth_position(), "ethereum", market=market)
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["valuation_status"] == "no_path"
    assert details["unavailable_reason"] == "curve_oracle_price_unavailable"
    assert details["valuation_source"] == "curve_spot_reserves"


def test_valuer_crypto_no_market_fails_closed() -> None:
    valuer = PortfolioValuer(_FullCalldataGatewayClient(_steth_crypto_replies()))
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(_steth_position(), "ethereum", market=None)
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["unavailable_reason"] == "curve_oracle_price_unavailable"


# ── _classify_family unit coverage (fail-closed branches) ─────────────────────


def test_classify_family_branches() -> None:
    classify = CurveLpPositionReader._classify_family
    # plain USD stable
    assert classify({"coins": ["DAI", "USDC", "USDT"]}, ["DAI", "USDC", "USDT"], coins_overridden=False) == "usd_stable"
    # USD metapool
    meta_usd = {
        "is_metapool": True,
        "base_pool": POOL_3POOL,
        "base_pool_coins": ["DAI", "USDC", "USDT"],
        "coins": ["FRAX", "3CRV"],
    }
    assert classify(meta_usd, ["FRAX", "3CRV"], coins_overridden=False) == "metapool_usd"
    # crypto (non-USD) with full addresses
    meta_crypto = {"coins": ["USDT", "WBTC", "WETH"], "coin_addresses": [USDT_ADDR, "0xb", "0xc"]}
    assert classify(meta_crypto, ["USDT", "WBTC", "WETH"], coins_overridden=False) == "crypto"


def test_classify_family_fails_closed() -> None:
    classify = CurveLpPositionReader._classify_family
    # metapool whose BASE is non-USD → fail closed (never mis-marked at $1)
    meta_bad_base = {
        "is_metapool": True,
        "base_pool": "0xbase",
        "base_pool_coins": ["WETH", "USDC"],  # not all USD
        "coins": ["MIM", "crvFRAX"],
    }
    assert classify(meta_bad_base, ["MIM", "crvFRAX"], coins_overridden=False) is None
    # metapool whose META coin is non-USD → fail closed
    meta_bad_meta = {
        "is_metapool": True,
        "base_pool": POOL_3POOL,
        "base_pool_coins": ["DAI", "USDC", "USDT"],
        "coins": ["WETH", "3CRV"],
    }
    assert classify(meta_bad_meta, ["WETH", "3CRV"], coins_overridden=False) is None
    # non-USD pool MISSING coin addresses → cannot price → fail closed
    meta_no_addr = {"coins": ["WETH", "WBTC"], "coin_addresses": []}
    assert classify(meta_no_addr, ["WETH", "WBTC"], coins_overridden=False) is None
    # empty coins → fail closed
    assert classify({"coins": []}, [], coins_overridden=False) is None


# ── VIB-5428 — uint256 over-long-return decode (FRAX/3CRV metapool quirk) ──────


def test_decode_uint256_word_handles_overlong_return() -> None:
    # The FRAX/3CRV metapool (its own integrated-ERC20 Vyper LP token) returns
    # MORE than 32 bytes from balanceOf/get_virtual_price on-chain: word 0 is the
    # real value, the tail is leftover memory. int(whole_hex,16) would read
    # megabyte-wide garbage; we must decode word 0 only (every ABI decoder does).
    from almanak.framework.valuation.lp_position_reader import _decode_uint256_word

    value = 4892_002_300_000_000_000_000  # ~4892e18
    word = hex(value)[2:].zfill(64)
    # normal 32-byte return
    assert _decode_uint256_word("0x" + word) == value
    # over-long return: word 0 + 127 trailing junk words
    assert _decode_uint256_word("0x" + word + "ab" * 32 * 127) == value
    # Empty ≠ Zero: empty / malformed → None, never a fabricated 0
    assert _decode_uint256_word("0x") is None
    assert _decode_uint256_word("") is None
    assert _decode_uint256_word(None) is None


def test_classify_family_override_cannot_reclassify_crypto() -> None:
    # A coins override that breaks 1:1 registry alignment forfeits the crypto
    # family (the valuer would mis-map an address to the wrong coin).
    classify = CurveLpPositionReader._classify_family
    meta_crypto = {"coins": ["USDT", "WBTC", "WETH"], "coin_addresses": [USDT_ADDR, "0xb", "0xc"]}
    assert classify(meta_crypto, ["WBTC", "WETH"], coins_overridden=True) is None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))


class TestResolveCurvePoolMetaDynamic:
    """VIB-5628: the valuer's MetaRegistry fallback seed-dict builder.

    Branch coverage for the fail-closed guards + the base_pool_coins metapool
    mapping (CRAP-gate + CodeRabbit #3191). ``resolve_pool_metadata`` is mocked —
    these tests exercise ONLY the dynamic wrapper's mapping/fail-closed logic.
    """

    UNCURATED = "0x4DEcE678ceceb27446b35C672dC7d61F30bAD69E"
    LP_ADDR = "0x4DEcE678ceceb27446b35C672dC7d61F30bAD69E"

    @staticmethod
    def _meta(**kw: Any) -> Any:
        from almanak.connectors.curve.pool_resolver import CurvePoolMetadata

        defaults: dict[str, Any] = {
            "address": TestResolveCurvePoolMetaDynamic.UNCURATED,
            "lp_token": TestResolveCurvePoolMetaDynamic.LP_ADDR,
            "coin_addresses": ["0xa0b8", "0xf939"],
            "coin_decimals": [6, 18],
            "coin_symbols": ["USDC", "CRVUSD"],
            "n_coins": 2,
            "pool_type": "stableswap",
            "is_metapool": False,
            "base_pool": None,
            "base_pool_coin_addresses": None,
            "base_pool_coins": None,
        }
        defaults.update(kw)
        return CurvePoolMetadata(**defaults)

    def test_none_gateway_fails_closed(self) -> None:
        assert (
            _resolve_curve_pool_meta_dynamic("ethereum", pool=self.UNCURATED, lp_token="", gateway_client=None) is None
        )

    def test_no_hex_address_fails_closed(self) -> None:
        # Neither pool nor lp_token is a 0x address -> nothing to query on.
        assert _resolve_curve_pool_meta_dynamic("ethereum", pool="3pool", lp_token="", gateway_client=object()) is None

    def test_falls_back_to_lp_token_address(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: dict[str, Any] = {}

        def _fake(*, chain: str, pool_address: str, gateway_client: Any) -> Any:
            seen["pool_address"] = pool_address
            return self._meta()

        monkeypatch.setattr("almanak.connectors.curve.pool_resolver.resolve_pool_metadata", _fake)
        # pool is a name (not 0x); the lp_token address must be used to query.
        out = _resolve_curve_pool_meta_dynamic(
            "ethereum", pool="crvusd_usdc", lp_token=self.LP_ADDR, gateway_client=object()
        )
        assert out is not None
        assert seen["pool_address"] == self.LP_ADDR

    def test_resolver_none_fails_closed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "almanak.connectors.curve.pool_resolver.resolve_pool_metadata",
            lambda **_: None,
        )
        assert (
            _resolve_curve_pool_meta_dynamic("ethereum", pool=self.UNCURATED, lp_token="", gateway_client=object())
            is None
        )

    def test_resolver_raises_fails_closed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _boom(**_: Any) -> Any:
            raise RuntimeError("resolver blew up")

        monkeypatch.setattr("almanak.connectors.curve.pool_resolver.resolve_pool_metadata", _boom)
        assert (
            _resolve_curve_pool_meta_dynamic("ethereum", pool=self.UNCURATED, lp_token="", gateway_client=object())
            is None
        )

    def test_plain_pool_maps_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "almanak.connectors.curve.pool_resolver.resolve_pool_metadata",
            lambda **_: self._meta(),
        )
        out = _resolve_curve_pool_meta_dynamic("ethereum", pool=self.UNCURATED, lp_token="", gateway_client=object())
        assert out is not None
        assert out["coins"] == ["USDC", "CRVUSD"]
        assert out["coin_decimals"] == [6, 18]
        assert out["is_metapool"] is False
        assert out["base_pool_coins"] is None
        assert out["base_pool_coin_addresses"] is None

    def test_metapool_maps_base_pool_coins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "almanak.connectors.curve.pool_resolver.resolve_pool_metadata",
            lambda **_: self._meta(
                is_metapool=True,
                base_pool="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
                base_pool_coin_addresses=["0x6b17", "0xa0b8", "0xdac1"],
                base_pool_coins=["DAI", "USDC", "USDT"],
            ),
        )
        out = _resolve_curve_pool_meta_dynamic("ethereum", pool=self.UNCURATED, lp_token="", gateway_client=object())
        assert out is not None
        assert out["is_metapool"] is True
        # base_pool_coins (SYMBOLS) must be threaded through — the classifier keys on it.
        assert out["base_pool_coins"] == ["DAI", "USDC", "USDT"]
        assert out["base_pool_coin_addresses"] == ["0x6b17", "0xa0b8", "0xdac1"]
