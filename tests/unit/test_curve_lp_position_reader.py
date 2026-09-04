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

from almanak.connectors.curve import pool_resolver
from almanak.framework.data.tokens.models import TokenRef
from almanak.framework.data.tokens.pegs import is_pegged
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.curve_lp_position_reader import (
    CurveLpPosition,
    CurveLpPositionReader,
    _resolve_curve_pool_meta,
    _resolve_curve_pool_meta_dynamic,
)
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer
from tests.support.curve_pool_catalog import curve_test_metadata

# Ethereum 3pool addresses.
POOL_3POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
LP_3POOL = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
WALLET = "0x1234567890123456789012345678901234567890"

# Base 4pool: USDC / USDbC / axlUSDC / crvUSD. StableSwap NG uses the pool as its LP token.
# USDbC and axlUSDC are 1:1 USDC wrappers.
POOL_4POOL_BASE = "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f"
LP_4POOL_BASE = "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f"

GET_VIRTUAL_PRICE = "0xbb7b8b80"
VIRTUAL_PRICE_ALIAS = "0x0c46b72a"
BALANCE_OF = "0x70a08231"
BALANCES_UINT256 = "0x4903b0d1"
BALANCES_INT128 = "0x065a80d8"
COINS_UINT256 = "0xc6610657"  # coins(uint256), verified against the deployed pool
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


# Ethereum steth crypto-family pool: ETH / stETH.
POOL_STETH = "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"
LP_STETH = "0x06325440D014e39736583c165C2963BA99fAf14E"
ETH_NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
STETH_ADDR = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"

# Ethereum frax_3crv USD metapool: [FRAX, 3CRV] over the 3pool base.
POOL_FRAX3CRV = "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B"
LP_FRAX3CRV = "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B"
FRAX_ADDR = "0x853d955aCEf822Db058eb8505911ED77F175b99e"
MIM_ADDR = "0x99D8a9C45b2ecA8864373A26D1459e3Dff1e17F3"
WETH_ADDR = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
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


@pytest.fixture(autouse=True)
def _explicit_live_pool_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """Feed address-selected fixtures through the production live-resolver seam."""

    def resolve_pool_metadata(*, chain: str, pool_address: str, **_kwargs: Any) -> Any:
        return curve_test_metadata(chain, pool_address)

    monkeypatch.setattr(pool_resolver, "resolve_pool_metadata", resolve_pool_metadata)


def test_resolve_pool_meta_by_name_fails_closed() -> None:
    assert _resolve_curve_pool_meta("ethereum", pool="3pool", lp_token="") is None


def test_resolve_pool_meta_by_exact_pool_address() -> None:
    meta = _resolve_curve_pool_meta("ethereum", pool=POOL_3POOL, lp_token=LP_3POOL, gateway_client=object())
    assert meta is not None
    assert meta["address"].lower() == POOL_3POOL.lower()
    assert meta["coins"] == ["DAI", "USDC", "USDT"]


def test_resolve_pool_meta_unknown_returns_none() -> None:
    assert _resolve_curve_pool_meta("ethereum", pool="not_a_pool", lp_token="") is None


def test_resolve_pool_meta_stale_lp_token_falls_back_to_pool_address() -> None:
    meta = _resolve_curve_pool_meta(
        "ethereum",
        pool=POOL_3POOL,
        lp_token="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        gateway_client=object(),
    )
    assert meta is not None
    assert meta["address"].lower() == POOL_3POOL.lower()


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
            pool=POOL_3POOL,
            lp_token=LP_3POOL,
            wallet_address=WALLET,
        )
        is None
    )


def test_read_position_3pool_live_virtual_price() -> None:
    reader = CurveLpPositionReader(
        _StubGatewayClient(_make_replies(lp_balance_wei=10 * 10**18, virtual_price_wei=1_019_566_780_337_011_070))
    )
    pos = reader.read_position(
        protocol="curve",
        chain="ethereum",
        pool=POOL_3POOL,
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
        pool=POOL_3POOL,
        lp_token=LP_3POOL,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert not pos.is_active
    assert pos.lp_balance_wei == 0


def test_read_position_unmeasured_balance_returns_none() -> None:
    # Empty != Zero: an unreadable balance is unavailable, never a fabricated zero.
    reader = CurveLpPositionReader(_StubGatewayClient(_make_replies(lp_balance_wei=None, virtual_price_wei=10**18)))
    assert (
        reader.read_position(
            protocol="curve",
            chain="ethereum",
            pool=POOL_3POOL,
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
            pool=POOL_3POOL,
            lp_token=LP_3POOL,
            wallet_address=WALLET,
        )
        is None
    )


def test_read_position_virtual_price_alias_fallback() -> None:
    # Some pools expose virtual_price() instead of get_virtual_price().
    replies = {
        (LP_3POOL.lower(), BALANCE_OF): 5 * 10**18,
        (POOL_3POOL.lower(), GET_VIRTUAL_PRICE): None,
        (POOL_3POOL.lower(), VIRTUAL_PRICE_ALIAS): 10**18,
    }
    reader = CurveLpPositionReader(_StubGatewayClient(replies))
    pos = reader.read_position(
        protocol="curve",
        chain="ethereum",
        pool=POOL_3POOL,
        lp_token=LP_3POOL,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert pos.virtual_price == Decimal("1")


def test_read_position_base_4pool_usdbc_axlusdc_values() -> None:
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
        pool=POOL_4POOL_BASE,
        lp_token=LP_4POOL_BASE,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert pos.is_active
    assert pos.lp_balance_wei == 10 * 10**18
    assert pos.virtual_price == Decimal("1019566780337011070") / Decimal(10**18)
    assert pos.coins == ["USDC", "USDbC", "axlUSDC", "crvUSD"]


def test_usdbc_and_axlusdc_in_usd_stable_allowlist() -> None:
    # Peg resolution uses exact Base token identities, not symbols alone.
    pos = CurveLpPositionReader(
        _StubGatewayClient(_make_replies(lp_balance_wei=0, virtual_price_wei=10**18))
    ).read_position(protocol="curve", chain="base", pool="4pool", lp_token=LP_4POOL_BASE, wallet_address=WALLET)
    assert pos is not None
    assert all(
        is_pegged(TokenRef(chain="base", address=address, decimals=0, symbol=symbol)) == Decimal("1")
        for symbol, address in zip(pos.coins, pos.coin_addresses, strict=True)
    )


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
    # Deployed steth exposes balances(uint256) and coins(uint256); its int128
    # overloads revert. Native ETH uses the sentinel and fixed 18 decimals.
    # coin*_addr permits transposed or missing on-chain coin-order fixtures.
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
    # Crypto-family valuation reads on-chain spot reserves, supply, and decimals.
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_steth_crypto_replies()))
    pos = reader.read_position(
        protocol="curve",
        chain="ethereum",
        pool=POOL_STETH,
        lp_token=LP_STETH,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert pos.family == "crypto"
    assert pos.lp_balance_wei == 2 * 10**18
    assert pos.total_supply_wei == 200 * 10**18
    assert pos.reserves_wei == [100 * 10**18, 110 * 10**18]
    assert pos.coin_decimals == [18, 18]  # Native ETH is fixed at 18 without an RPC read.
    assert pos.coins == ["ETH", "stETH"]


def _int128_fallback_replies() -> dict[tuple[str, str], int | None]:
    # Synthetic pre-NG shape: balances(uint256) reverts and balances(int128)
    # succeeds. The steth metadata fixture supplies only pool/coin wiring.
    return {
        (LP_STETH, BALANCE_OF + WALLET.lower().removeprefix("0x").zfill(64)): 2 * 10**18,
        (LP_STETH, TOTAL_SUPPLY): 200 * 10**18,
        # Keep coins on uint256 so only the balances selector varies.
        (POOL_STETH, _balances_call(COINS_UINT256, 0)): _addr_word(ETH_NATIVE),
        (POOL_STETH, _balances_call(COINS_UINT256, 1)): _addr_word(STETH_ADDR),
        (POOL_STETH, _balances_call(BALANCES_UINT256, 0)): None,
        (POOL_STETH, _balances_call(BALANCES_INT128, 0)): 100 * 10**18,
        (POOL_STETH, _balances_call(BALANCES_UINT256, 1)): None,
        (POOL_STETH, _balances_call(BALANCES_INT128, 1)): 110 * 10**18,
        (STETH_ADDR, DECIMALS): 18,
    }


def test_crypto_balances_int128_selector_fallback() -> None:
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_int128_fallback_replies()))
    pos = reader.read_position(
        protocol="curve",
        chain="ethereum",
        pool=POOL_STETH,
        lp_token=LP_STETH,
        wallet_address=WALLET,
    )
    assert pos is not None
    assert pos.family == "crypto"
    assert pos.reserves_wei == [100 * 10**18, 110 * 10**18]


@pytest.mark.parametrize(
    "override",
    [
        {"total_supply_wei": None},
        {"total_supply_wei": 0},
        {"reserve_steth_wei": None},
        {"steth_decimals": None},
    ],
)
def test_read_position_crypto_fails_closed_on_unreadable_input(override: dict[str, int | None]) -> None:
    # Empty != Zero: any missing reserve input makes the mark unavailable.
    replies = _steth_crypto_replies(**override)  # type: ignore[arg-type]
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    assert (
        reader.read_position(
            protocol="curve",
            chain="ethereum",
            pool=POOL_STETH,
            lp_token=LP_STETH,
            wallet_address=WALLET,
        )
        is None
    )


def test_crypto_coin_order_match_returns_position_unchanged() -> None:
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_steth_crypto_replies()))
    pos = reader.read_position(
        protocol="curve", chain="ethereum", pool=POOL_STETH, lp_token=LP_STETH, wallet_address=WALLET
    )
    assert pos is not None
    assert pos.family == "crypto"
    assert pos.coin_addresses == [ETH_NATIVE, STETH_ADDR]
    assert pos.reserves_wei == [100 * 10**18, 110 * 10**18]
    assert pos.coin_decimals == [18, 18]
    assert pos.total_supply_wei == 200 * 10**18


def test_crypto_native_eth_placeholder_coin0_validates() -> None:
    # The registry stores the native-ETH sentinel; order validation is case-insensitive.
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_steth_crypto_replies(coin0_addr=ETH_NATIVE.lower())))
    pos = reader.read_position(
        protocol="curve", chain="ethereum", pool=POOL_STETH, lp_token=LP_STETH, wallet_address=WALLET
    )
    assert pos is not None
    assert pos.coin_addresses[0].lower() == ETH_NATIVE.lower()


def test_crypto_coin_order_transposed_fails_closed() -> None:
    # A registry/on-chain coin-order mismatch makes valuation unavailable.
    replies = _steth_crypto_replies(coin0_addr=STETH_ADDR, coin1_addr=ETH_NATIVE)
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    assert (
        reader.read_position(
            protocol="curve", chain="ethereum", pool=POOL_STETH, lp_token=LP_STETH, wallet_address=WALLET
        )
        is None
    )


def test_crypto_coin_read_miss_fails_closed() -> None:
    # Empty != Zero: if neither coins(i) overload resolves, order is unmeasured.
    replies = _steth_crypto_replies(coin1_addr=None)
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    assert (
        reader.read_position(
            protocol="curve", chain="ethereum", pool=POOL_STETH, lp_token=LP_STETH, wallet_address=WALLET
        )
        is None
    )


def _coins_int128_fallback_replies() -> dict[tuple[str, str], int | None]:
    # Synthetic pre-NG shape: coins(uint256) reverts and coins(int128) succeeds.
    # Reserves stay on uint256 so only the coin selector varies.
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
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_coins_int128_fallback_replies()))
    pos = reader.read_position(
        protocol="curve", chain="ethereum", pool=POOL_STETH, lp_token=LP_STETH, wallet_address=WALLET
    )
    assert pos is not None
    assert pos.family == "crypto"
    assert pos.coin_addresses == [ETH_NATIVE, STETH_ADDR]
    assert pos.reserves_wei == [100 * 10**18, 110 * 10**18]


def test_crypto_coin_order_validated_once_then_cached() -> None:
    # Deployed coin order is immutable, so successful validation is cached per (chain, pool).
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(_steth_crypto_replies()))
    calls = {"n": 0}
    orig = reader._read_pool_coin_address

    def _counting(chain: str, pool_address: str, index: int) -> str | None:
        calls["n"] += 1
        return orig(chain, pool_address, index)

    reader._read_pool_coin_address = _counting  # type: ignore[method-assign]

    kw = {
        "protocol": "curve",
        "chain": "ethereum",
        "pool": POOL_STETH,
        "lp_token": LP_STETH,
        "wallet_address": WALLET,
    }
    assert reader.read_position(**kw) is not None
    first = calls["n"]
    assert first == 2
    assert ("ethereum", POOL_STETH.lower()) in reader._validated_coin_order
    assert reader.read_position(**kw) is not None
    assert calls["n"] == first


def test_crypto_coin_order_failure_not_cached() -> None:
    # Failed order checks are never cached; transient read misses and mismatches must be retried.
    replies = _steth_crypto_replies(coin0_addr=STETH_ADDR, coin1_addr=ETH_NATIVE)
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    calls = {"n": 0}
    orig = reader._read_pool_coin_address

    def _counting(chain: str, pool_address: str, index: int) -> str | None:
        calls["n"] += 1
        return orig(chain, pool_address, index)

    reader._read_pool_coin_address = _counting  # type: ignore[method-assign]

    kw = {
        "protocol": "curve",
        "chain": "ethereum",
        "pool": POOL_STETH,
        "lp_token": LP_STETH,
        "wallet_address": WALLET,
    }
    assert reader.read_position(**kw) is None
    after_first = calls["n"]
    assert after_first >= 1
    assert ("ethereum", POOL_STETH.lower()) not in reader._validated_coin_order
    assert reader.read_position(**kw) is None
    assert calls["n"] > after_first


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
        # Match MarketSnapshot.price's keyword-only chain argument; symbol pricing ignores it.
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
    pos = _curve_position(
        {"pool": POOL_3POOL, "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"], "wallet": WALLET}
    )
    # An independent oracle must confirm the $1 peg before virtual-price valuation at par.
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=_usd_market())  # type: ignore[arg-type]
    assert repriced is True
    assert value_usd == Decimal("10") * (Decimal("1019566780337011070") / Decimal(10**18))
    assert details["valuation_source"] == "curve_virtual_price"
    assert details["virtual_price"] == str(Decimal("1019566780337011070") / Decimal(10**18))
    assert details["liquidity"] == str(10 * 10**18)
    assert details["peg_usd"] == "1"
    assert details["oracle_peg_usd"] == "1.0"
    assert details["depeg_divergence_bps"] == "0"
    assert "valuation_status" not in details


def test_valuer_curve_branch_uses_strategy_wallet_fallback() -> None:
    valuer = PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=10**18, virtual_price_wei=10**18)))
    valuer._strategy_wallet_address = WALLET
    pos = _curve_position({"pool": POOL_3POOL, "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"]})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=_usd_market())  # type: ignore[arg-type]
    assert repriced is True
    assert value_usd == Decimal("1")


def test_valuer_curve_branch_no_wallet_fails_closed() -> None:
    # Missing wallet identity makes the mark unavailable rather than stale.
    valuer = PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=10**18, virtual_price_wei=10**18)))
    pos = _curve_position({"pool": POOL_3POOL, "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"]})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=None)  # type: ignore[arg-type]
    assert repriced is False
    assert details == {}


def test_valuer_curve_branch_empty_position_measured_zero() -> None:
    valuer = PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=0, virtual_price_wei=10**18)))
    pos = _curve_position(
        {"pool": POOL_3POOL, "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"], "wallet": WALLET}
    )
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=None)  # type: ignore[arg-type]
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["liquidity"] == "0"


def test_valuer_curve_branch_unmeasured_fails_closed_not_zero() -> None:
    # Empty != Zero: an unreadable virtual price is unavailable, not a zero mark.
    valuer = PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=10**18, virtual_price_wei=None)))
    pos = _curve_position(
        {"pool": POOL_3POOL, "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"], "wallet": WALLET}
    )
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(pos, "ethereum", market=None)  # type: ignore[arg-type]
    assert repriced is False
    assert details == {}


def test_valuer_metapool_shape_usd_pegged_values() -> None:
    assert is_pegged(TokenRef(chain="ethereum", address=USDC_ADDR, decimals=0, symbol="USDC")) == Decimal("1")


def _active_3pool_valuer() -> PortfolioValuer:
    return PortfolioValuer(_StubGatewayClient(_make_replies(lp_balance_wei=10 * 10**18, virtual_price_wei=10**18)))


def _3pool_pos() -> PositionInfo:
    return _curve_position(
        {"pool": POOL_3POOL, "lp_token": LP_3POOL, "coins": ["DAI", "USDC", "USDT"], "wallet": WALLET}
    )


def test_curve_depeg_fires_unavailable() -> None:
    # virtual_price cannot observe a USDT depeg; oracle divergence makes the mark unavailable.
    valuer = _active_3pool_valuer()
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _3pool_pos(),
        "ethereum",
        market=_usd_market(USDT="0.90"),  # type: ignore[arg-type]
    )
    assert repriced is True  # Marker tuple, not dispatch fall-through.
    assert value_usd == Decimal("0")  # Excluded from NAV as unmeasured, not measured zero.
    assert details["valuation_status"] == "no_path"
    assert details["mark_unmeasured"] is True
    assert details["unavailable_reason"] == "curve_oracle_depeg_divergence"
    assert details["depeg_divergence_bps"] == "1000"
    assert details["depeg_threshold_bps"] == "100"


def test_curve_systemic_depeg_fires() -> None:
    # The absolute $1 peg check catches systemic depeg even when inter-coin spread is zero.
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
    # An oracle miss is unavailable data, not evidence of a depeg.
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


def test_curve_exact_address_zero_cannot_fall_back_to_symbol_peg() -> None:
    """A measured zero at the exact address disables the whole pool peg."""
    valuer = _active_3pool_valuer()
    market = _StubMarket(
        {
            "DAI": Decimal("1"),
            "USDC": Decimal("1"),
            "USDT": Decimal("1"),
            USDC_ADDR: Decimal("0"),
        }
    )

    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _3pool_pos(),
        "ethereum",
        market=market,  # type: ignore[arg-type]
    )

    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["valuation_status"] == "no_path"
    assert details["unavailable_reason"] == "curve_oracle_price_unavailable"


def test_curve_no_market_fails_closed() -> None:
    # Without an independent oracle, the pool cannot be safely marked at par.
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
    # A per-intent threshold may explicitly widen the accepted oracle peg band.
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
            "pool": POOL_3POOL,
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
    assert value_usd == Decimal("10")
    assert details["depeg_threshold_bps"] == "200"
    assert "valuation_status" not in details


def test_curve_depeg_marker_forces_snapshot_unavailable() -> None:
    # A no_path marker makes the whole snapshot unavailable rather than safe at par.
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


def _frax3crv_replies(
    *,
    lp_balance_wei: int | None = 10 * 10**18,
    metapool_vp_wei: int | None = 1_020_500_000_000_000_000,
    base_vp_wei: int | None = 1_030_000_000_000_000_000,
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
        details={"pool": POOL_FRAX3CRV, "lp_token": LP_FRAX3CRV, "wallet": WALLET},
    )


def test_read_position_metapool_family_expands_underlying() -> None:
    # balanceOf calldata includes the wallet argument, requiring full-calldata routing.
    replies = _frax3crv_replies()
    replies.pop((LP_FRAX3CRV.lower(), BALANCE_OF))
    replies[(LP_FRAX3CRV.lower(), BALANCE_OF + WALLET.lower().removeprefix("0x").zfill(64))] = 10 * 10**18
    reader = CurveLpPositionReader(_FullCalldataGatewayClient(replies))
    pos = reader.read_position(
        protocol="curve", chain="ethereum", pool=POOL_FRAX3CRV, lp_token=LP_FRAX3CRV, wallet_address=WALLET
    )
    assert pos is not None
    assert pos.family == "metapool_usd"
    assert pos.virtual_price == Decimal("1.0205")
    assert pos.base_pool_virtual_price == Decimal("1.03")
    # Expand 3CRV into ordered 3pool stables; the LP token itself is not oracle-priced.
    assert pos.underlying_coins == ["FRAX", "DAI", "USDC", "USDT"]
    assert "3CRV" not in pos.underlying_coins
    assert pos.underlying_coin_addresses == [FRAX_ADDR, DAI_ADDR, USDC_ADDR, USDT_ADDR]


def test_valuer_metapool_values_at_metapool_virtual_price() -> None:
    valuer = PortfolioValuer(_StubGatewayClient(_frax3crv_replies()))
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _frax3crv_position(), "ethereum", market=_frax3crv_market()
    )
    assert repriced is True
    # Metapool virtual price already incorporates the base pool; multiplying base_vp would double-count.
    assert value_usd == Decimal("10") * Decimal("1.0205")
    assert details["valuation_source"] == "curve_virtual_price"
    assert details["base_pool_virtual_price"] == "1.03"
    assert details["underlying_coins"] == ["FRAX", "DAI", "USDC", "USDT"]
    assert "valuation_status" not in details


def test_valuer_metapool_depeg_on_base_coin_fires() -> None:
    # Base-LP expansion exposes underlying USDT to the oracle depeg check.
    valuer = PortfolioValuer(_StubGatewayClient(_frax3crv_replies()))
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _frax3crv_position(), "ethereum", market=_frax3crv_market(USDT="0.90")
    )
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["valuation_status"] == "no_path"
    # A depeg result confirms that underlying base coins, not 3CRV, were oracle-priced.
    assert details["unavailable_reason"] == "curve_oracle_depeg_divergence"
    assert details["depeg_divergence_bps"] == "1000"


def test_valuer_metapool_meta_coin_depeg_fires() -> None:
    valuer = PortfolioValuer(_StubGatewayClient(_frax3crv_replies()))
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(
        _frax3crv_position(), "ethereum", market=_frax3crv_market(FRAX="0.92")
    )
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["unavailable_reason"] == "curve_oracle_depeg_divergence"


def _steth_position() -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=f"curve_steth_{LP_STETH}",
        chain="ethereum",
        protocol="curve",
        value_usd=Decimal("6000"),
        details={"pool": POOL_STETH, "lp_token": LP_STETH, "wallet": WALLET},
    )


def test_valuer_crypto_values_from_spot_reserves() -> None:
    valuer = PortfolioValuer(_FullCalldataGatewayClient(_steth_crypto_replies()))
    market = _StubMarket({"ETH": Decimal("3000"), "STETH": Decimal("2990")})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(_steth_position(), "ethereum", market=market)
    assert repriced is True
    assert value_usd == Decimal("6289")
    assert details["valuation_source"] == "curve_spot_reserves"
    assert details["total_supply"] == str(200 * 10**18)
    assert details["coin_prices_usd"] == ["3000", "2990"]
    assert "valuation_status" not in details


def test_valuer_crypto_native_eth_prices_via_weth_fallback() -> None:
    # Native-sentinel ETH uses WETH as its oracle fallback; dropping the leg would under-value the pool.
    valuer = PortfolioValuer(_FullCalldataGatewayClient(_steth_crypto_replies()))
    market = _StubMarket({"WETH": Decimal("3000"), "STETH": Decimal("2990")})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(_steth_position(), "ethereum", market=market)
    assert repriced is True
    assert value_usd == Decimal("6289")
    assert details["coin_prices_usd"] == ["3000", "2990"]


def test_valuer_crypto_native_eth_unpriceable_fails_closed() -> None:
    # Without ETH or WETH oracle data, the native leg is unmeasured.
    valuer = PortfolioValuer(_FullCalldataGatewayClient(_steth_crypto_replies()))
    market = _StubMarket({"STETH": Decimal("2990")})
    value_usd, details, repriced = valuer._reprice_lp_enriched_dispatch(_steth_position(), "ethereum", market=market)
    assert repriced is True
    assert value_usd == Decimal("0")
    assert details["unavailable_reason"] == "curve_oracle_price_unavailable"


def test_price_curve_coins_real_address_eth_does_not_proxy_weth() -> None:
    # A non-sentinel ERC-20 named ETH must price by its own address, never proxy WETH.
    valuer = PortfolioValuer(_StubGatewayClient({}))
    real_eth_token = "0x1111111111111111111111111111111111111111"  # Non-sentinel address.
    prices = valuer._price_curve_coins(["ETH"], [real_eth_token], "ethereum", _StubMarket({"WETH": Decimal("3000")}))
    assert prices == [None]


def test_price_curve_coins_sentinel_eth_proxies_weth() -> None:
    # Native-sentinel ETH proxies to WETH.
    valuer = PortfolioValuer(_StubGatewayClient({}))
    prices = valuer._price_curve_coins(["ETH"], [ETH_NATIVE], "ethereum", _StubMarket({"WETH": Decimal("3000")}))
    assert prices == [Decimal("3000")]
    # An empty address represents an unknown native leg and also proxies to WETH.
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
    # Misaligned reserve metadata emits no_path so a stale strategy estimate cannot enter NAV.
    valuer = PortfolioValuer(_StubGatewayClient({}))
    on_chain = _crypto_position_obj(coin_decimals=[18])
    market = _StubMarket({"ETH": Decimal("3000"), "STETH": Decimal("2990"), "WETH": Decimal("3000")})
    value_usd, details = valuer._value_curve_crypto(_steth_position(), on_chain, "ethereum", market)  # type: ignore[misc]
    assert value_usd == Decimal("0")
    assert details["valuation_status"] == "no_path"
    assert details["unavailable_reason"] == "curve_spot_reserves_read_incomplete"


def test_value_curve_crypto_nonpositive_fails_closed_no_path() -> None:
    # Nonpositive gross reserves emit no_path rather than dispatch fall-through.
    valuer = PortfolioValuer(_StubGatewayClient({}))
    on_chain = _crypto_position_obj(reserves_wei=[0, 0])
    market = _StubMarket({"ETH": Decimal("3000"), "STETH": Decimal("2990"), "WETH": Decimal("3000")})
    value_usd, details = valuer._value_curve_crypto(_steth_position(), on_chain, "ethereum", market)  # type: ignore[misc]
    assert value_usd == Decimal("0")
    assert details["valuation_status"] == "no_path"
    assert details["unavailable_reason"] == "curve_spot_reserves_nonpositive"


def test_valuer_crypto_unpriceable_coin_fails_closed() -> None:
    # An unpriceable crypto leg emits an unavailable no_path marker, never a partial mark.
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


def test_classify_family_branches() -> None:
    classify = CurveLpPositionReader._classify_family
    plain = {
        "coins": ["DAI", "USDC", "USDT"],
        "coin_addresses": [DAI_ADDR, USDC_ADDR, USDT_ADDR],
    }
    assert classify(plain, ["DAI", "USDC", "USDT"], chain="ethereum", coins_overridden=False) == "usd_stable"
    meta_usd = {
        "is_metapool": True,
        "base_pool": POOL_3POOL,
        "base_pool_coins": ["DAI", "USDC", "USDT"],
        "base_pool_coin_addresses": [DAI_ADDR, USDC_ADDR, USDT_ADDR],
        "coins": ["FRAX", "3CRV"],
        "coin_addresses": [FRAX_ADDR, LP_3POOL],
    }
    assert classify(meta_usd, ["FRAX", "3CRV"], chain="ethereum", coins_overridden=False) == "metapool_usd"
    meta_crypto = {"coins": ["USDT", "WBTC", "WETH"], "coin_addresses": [USDT_ADDR, "0xb", "0xc"]}
    assert classify(meta_crypto, ["USDT", "WBTC", "WETH"], chain="ethereum", coins_overridden=False) == "crypto"


def test_classify_family_fails_closed() -> None:
    classify = CurveLpPositionReader._classify_family
    meta_bad_base = {
        "is_metapool": True,
        "base_pool": "0xbase",
        "base_pool_coins": ["WETH", "USDC"],
        "base_pool_coin_addresses": [WETH_ADDR, USDC_ADDR],
        "coins": ["MIM", "crvFRAX"],
        "coin_addresses": [MIM_ADDR, LP_3POOL],
    }
    assert classify(meta_bad_base, ["MIM", "crvFRAX"], chain="ethereum", coins_overridden=False) is None
    meta_bad_meta = {
        "is_metapool": True,
        "base_pool": POOL_3POOL,
        "base_pool_coins": ["DAI", "USDC", "USDT"],
        "base_pool_coin_addresses": [DAI_ADDR, USDC_ADDR, USDT_ADDR],
        "coins": ["WETH", "3CRV"],
        "coin_addresses": [WETH_ADDR, LP_3POOL],
    }
    assert classify(meta_bad_meta, ["WETH", "3CRV"], chain="ethereum", coins_overridden=False) is None
    meta_no_addr = {"coins": ["WETH", "WBTC"], "coin_addresses": []}
    assert classify(meta_no_addr, ["WETH", "WBTC"], chain="ethereum", coins_overridden=False) is None
    assert classify({"coins": []}, [], chain="ethereum", coins_overridden=False) is None


def test_decode_uint256_word_handles_overlong_return() -> None:
    # The integrated-ERC20 FRAX/3CRV Vyper pool returns trailing memory after the
    # first ABI word for balanceOf/get_virtual_price; only word 0 is the value.
    from almanak.framework.valuation.lp_position_reader import _decode_uint256_word

    value = 4892_002_300_000_000_000_000
    word = hex(value)[2:].zfill(64)
    assert _decode_uint256_word("0x" + word) == value
    assert _decode_uint256_word("0x" + word + "ab" * 32 * 127) == value
    # Empty != Zero: empty or malformed return data never fabricates zero.
    assert _decode_uint256_word("0x") is None
    assert _decode_uint256_word("") is None
    assert _decode_uint256_word(None) is None


def test_classify_family_override_cannot_reclassify_crypto() -> None:
    # Crypto classification requires one-to-one ordered alignment with registry addresses.
    classify = CurveLpPositionReader._classify_family
    meta_crypto = {"coins": ["USDT", "WBTC", "WETH"], "coin_addresses": [USDT_ADDR, "0xb", "0xc"]}
    assert classify(meta_crypto, ["WBTC", "WETH"], chain="ethereum", coins_overridden=True) is None


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
        assert _resolve_curve_pool_meta_dynamic("ethereum", pool="3pool", lp_token="", gateway_client=object()) is None

    def test_falls_back_to_lp_token_address(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: dict[str, Any] = {}

        def _fake(*, chain: str, pool_address: str, gateway_client: Any) -> Any:
            seen["pool_address"] = pool_address
            return self._meta()

        monkeypatch.setattr("almanak.connectors.curve.pool_resolver.resolve_pool_metadata", _fake)
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
        # The classifier keys on ordered base-pool symbols, not only their addresses.
        assert out["base_pool_coins"] == ["DAI", "USDC", "USDT"]
        assert out["base_pool_coin_addresses"] == ["0x6b17", "0xa0b8", "0xdac1"]
