"""Unit tests for the Compound V3 multi-collateral market-health read (VIB-4851 PR-2).

``read_compound_v3_market_health`` is the connector-owned reader the position-health
gate uses. It preserves the product-owner-chosen SUMMED health factor across every
HELD collateral:

    HF = Σ_over_held_collaterals(value_usd × LCF) / borrow_value_usd

The per-collateral price / scale / liquidation-factor are read ON-CHAIN
(``getAssetInfoByAddress`` / ``getPrice``), never from the catalogue. These tests
drive the reader directly with a fake ``(to, data) -> hex`` ``eth_call`` closure
(no gateway), dispatching on the 4-byte selector to return ABI-encoded Comet blobs,
and assert the Σ-value / Σ(value×LCF) / HF math and the Empty≠Zero fail-closed paths.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._strategy_base.lending_read_base import LendingAccountState
from almanak.connectors.compound_v3.lending_read import read_compound_v3_market_health

# Selectors the reader emits (see ``lending_read_base`` PR-2 primitives).
_COLLATERAL_BALANCE_SELECTOR = "0x5c2549ee"  # collateralBalanceOf(user, asset)
_ASSET_INFO_SELECTOR = "0x3b3bec2e"  # getAssetInfoByAddress(asset)
_GET_PRICE_SELECTOR = "0x41976e09"  # getPrice(priceFeed)
_BORROW_BALANCE_SELECTOR = "0x374c49b4"  # borrowBalanceOf(user)

_COMET = "0xc3d688B66703497DAA19211EEdff47f25384cdc3"
_USER = "0xabcabcabcabcabcabcabcabcabcabcabcabcabca"
_WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
_WBTC = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
_USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


def _pad32(value: int) -> str:
    return f"{value:064x}"


def _asset_info_blob(*, price_feed: str, scale: int, liquidate_cf: int) -> str:
    """8-word AssetInfo struct: only priceFeed[2], scale[3], liquidateCF[5] are decoded."""
    pf_int = int(price_feed.lower().replace("0x", ""), 16)
    words = [
        _pad32(0),  # offset
        _pad32(int("11" * 20, 16)),  # asset
        _pad32(pf_int),  # priceFeed
        _pad32(scale),  # scale
        _pad32(int(Decimal("0.8") * Decimal("1e18"))),  # borrow_cf (unused)
        _pad32(liquidate_cf),  # liquidate_cf
        _pad32(int(Decimal("0.95") * Decimal("1e18"))),  # liquidation_factor (unused)
        _pad32(0),  # supplyCap (unused)
    ]
    return "0x" + "".join(words)


def _asset_arg(data: str) -> str:
    """Extract the asset address (word 1 = 2nd arg) from collateralBalanceOf calldata.

    Layout: selector(10) + pad(user)[word0] + pad(asset)[word1].
    """
    return "0x" + data[10 + 64 + 24 : 10 + 128]


def _addr_word0(data: str) -> str:
    """Extract an address from word 0 of calldata (getAssetInfoByAddress / getPrice).

    Layout: selector(10) + pad(address)[word0].
    """
    return "0x" + data[10 + 24 : 10 + 64]


def _no_oracle(symbol: str) -> Decimal:  # pragma: no cover - should never be called
    raise AssertionError(f"resolve_base_price unexpectedly called for {symbol}")


def _no_decimals(symbol: str, address: str) -> int:  # pragma: no cover
    raise AssertionError(f"resolve_base_decimals unexpectedly called for {symbol}")


def test_two_held_collaterals_sum_value_threshold_and_hf():
    """Two held collaterals: Σvalue, Σ(value×LCF), and HF are summed correctly.

    WETH: 2 tokens @ $2000, LCF 0.895 -> value 4000, threshold 3580.
    WBTC: 0.5 token @ $40000, LCF 0.77 -> value 20000, threshold 15400.
    Σvalue = 24000 ; Σthreshold = 18980 ; borrow 10000 USDC @ $1.
    HF = 18980 / 10000 = 1.898 ; lltv = 18980/24000.
    """
    # Per-collateral on-chain reads keyed by the (distinct) sentinel price feeds.
    weth_feed = "0x" + "11" * 20
    wbtc_feed = "0x" + "22" * 20
    info = {
        weth_feed: _asset_info_blob(
            price_feed=weth_feed, scale=int(Decimal("1e18")), liquidate_cf=int(Decimal("0.895") * Decimal("1e18"))
        ),
        wbtc_feed: _asset_info_blob(
            price_feed=wbtc_feed, scale=int(Decimal("1e8")), liquidate_cf=int(Decimal("0.77") * Decimal("1e18"))
        ),
    }
    # asset -> (balance_raw, its feed, price_8dec)
    per_asset = {
        _WETH.lower(): (int(Decimal("2") * Decimal("1e18")), weth_feed, 2000 * 10**8),
        _WBTC.lower(): (int(Decimal("0.5") * Decimal("1e8")), wbtc_feed, 40000 * 10**8),
    }

    def eth_call(to, data):
        assert to == _COMET
        sel = data[:10].lower()
        if sel == _COLLATERAL_BALANCE_SELECTOR:
            asset = _asset_arg(data).lower()
            bal, _feed, _price = per_asset.get(asset, (0, None, 0))
            return "0x" + _pad32(bal)
        if sel == _ASSET_INFO_SELECTOR:
            asset = _addr_word0(data).lower()
            _bal, feed, _price = per_asset[asset]
            return info[feed]
        if sel == _GET_PRICE_SELECTOR:
            feed = _addr_word0(data)
            for _bal, f, price in per_asset.values():
                if f.lower() == feed.lower():
                    return "0x" + _pad32(price)
            raise AssertionError(f"no price for feed {feed}")
        if sel == _BORROW_BALANCE_SELECTOR:
            return "0x" + _pad32(10_000 * 10**6)
        raise AssertionError(f"unexpected selector {sel}")

    collaterals = {"WETH": {"address": _WETH}, "WBTC": {"address": _WBTC}}
    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals=collaterals,
        base_token="USDC",
        base_token_address=_USDC,
        resolve_base_price=lambda s: Decimal("1"),
        resolve_base_decimals=lambda s, a: 6,
    )
    assert isinstance(state, LendingAccountState)
    assert state.collateral_usd == Decimal("24000")
    assert state.debt_usd == Decimal("10000")
    # HF = Σ(value×lcf) / debt = 18980 / 10000
    assert state.health_factor == Decimal("1.898")
    # lltv = weighted share = 18980 / 24000
    assert state.lltv == Decimal("18980") / Decimal("24000")
    assert state.liquidation_threshold_bps is None
    assert state.e_mode_category is None
    assert state.family is None


def test_zero_balance_collateral_is_skipped():
    """A zero-balance collateral is skipped (no asset_info / price reads for it)."""
    weth_feed = "0x" + "11" * 20
    seen_assets: list[str] = []

    def eth_call(to, data):
        sel = data[:10].lower()
        if sel == _COLLATERAL_BALANCE_SELECTOR:
            asset = _asset_arg(data).lower()
            if asset == _WETH.lower():
                return "0x" + _pad32(int(Decimal("1") * Decimal("1e18")))
            return "0x" + _pad32(0)  # WBTC, etc. -> skipped
        if sel == _ASSET_INFO_SELECTOR:
            seen_assets.append(_addr_word0(data).lower())
            return _asset_info_blob(
                price_feed=weth_feed, scale=int(Decimal("1e18")), liquidate_cf=int(Decimal("0.895") * Decimal("1e18"))
            )
        if sel == _GET_PRICE_SELECTOR:
            return "0x" + _pad32(2000 * 10**8)
        if sel == _BORROW_BALANCE_SELECTOR:
            return "0x" + _pad32(1_000 * 10**6)
        raise AssertionError(f"unexpected selector {sel}")

    collaterals = {"WETH": {"address": _WETH}, "WBTC": {"address": _WBTC}}
    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals=collaterals,
        base_token="USDC",
        base_token_address=_USDC,
        resolve_base_price=lambda s: Decimal("1"),
        resolve_base_decimals=lambda s, a: 6,
    )
    # Only WETH valued: value 2000, threshold 1790, borrow 1000 -> HF 1.79.
    assert state.collateral_usd == Decimal("2000")
    assert state.health_factor == Decimal("1.79")
    # getAssetInfoByAddress was only issued for the HELD (WETH) collateral.
    assert seen_assets == [_WETH.lower()]


def test_short_blob_for_held_collateral_returns_none():
    """A short/None blob for a HELD collateral fails closed (None) — never under-counts.

    Empty ≠ Zero: if a held collateral's asset-info read returns a malformed blob, we
    cannot value it; returning a partial sum would inflate the reported HF, so the
    whole read must fail closed.
    """

    def eth_call(to, data):
        sel = data[:10].lower()
        if sel == _COLLATERAL_BALANCE_SELECTOR:
            asset = _asset_arg(data).lower()
            if asset == _WETH.lower():
                return "0x" + _pad32(int(Decimal("1") * Decimal("1e18")))
            return "0x" + _pad32(0)
        if sel == _ASSET_INFO_SELECTOR:
            return "0x1234"  # malformed / short -> _parse_asset_info_hex returns None
        if sel == _GET_PRICE_SELECTOR:
            return "0x" + _pad32(2000 * 10**8)
        if sel == _BORROW_BALANCE_SELECTOR:
            return "0x" + _pad32(1_000 * 10**6)
        raise AssertionError(f"unexpected selector {sel}")

    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals={"WETH": {"address": _WETH}},
        base_token="USDC",
        base_token_address=_USDC,
        resolve_base_price=lambda s: Decimal("1"),
        resolve_base_decimals=lambda s, a: 6,
    )
    assert state is None


def test_none_balance_blob_for_held_collateral_returns_none():
    """A None balance blob (gateway read failed) fails closed (None), not a fabricated 0."""

    def eth_call(to, data):
        sel = data[:10].lower()
        if sel == _COLLATERAL_BALANCE_SELECTOR:
            return None  # read failed -> cannot tell zero from unmeasured
        if sel == _BORROW_BALANCE_SELECTOR:
            return "0x" + _pad32(0)
        raise AssertionError(f"unexpected selector {sel}")

    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals={"WETH": {"address": _WETH}},
        base_token="USDC",
        base_token_address=_USDC,
        resolve_base_price=lambda s: Decimal("1"),
        resolve_base_decimals=lambda s, a: 6,
    )
    assert state is None


def test_none_borrow_blob_returns_none():
    """A None borrow blob fails closed (None) — debt is unmeasured, never fabricated 0."""

    def eth_call(to, data):
        sel = data[:10].lower()
        if sel == _COLLATERAL_BALANCE_SELECTOR:
            return "0x" + _pad32(0)  # no collateral held
        if sel == _BORROW_BALANCE_SELECTOR:
            return None  # borrow read failed
        raise AssertionError(f"unexpected selector {sel}")

    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals={"WETH": {"address": _WETH}},
        base_token="USDC",
        base_token_address=_USDC,
        resolve_base_price=lambda s: Decimal("1"),
        resolve_base_decimals=lambda s, a: 6,
    )
    assert state is None


def test_base_asset_only_market_no_collateral_no_borrow():
    """Base-asset-only position: no collateral entries, borrow 0 -> measured zero debt, HF None.

    With no held collateral and no borrow, collateral_usd and debt_usd are measured
    ``Decimal("0")`` (Empty ≠ Zero), health_factor is None (no debt — the caller maps
    that to Infinity), and lltv is None (Σvalue == 0). The base-price/decimals
    resolvers are never called because borrow is 0.
    """

    def eth_call(to, data):
        sel = data[:10].lower()
        if sel == _BORROW_BALANCE_SELECTOR:
            return "0x" + _pad32(0)
        raise AssertionError(f"unexpected selector {sel} (no collateral iteration expected)")

    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals={},  # base-asset-only market: no collateral leg
        base_token="USDC",
        base_token_address=_USDC,
        resolve_base_price=_no_oracle,
        resolve_base_decimals=_no_decimals,
    )
    assert isinstance(state, LendingAccountState)
    assert state.collateral_usd == Decimal("0")
    assert state.debt_usd == Decimal("0")
    assert state.health_factor is None
    assert state.lltv is None


def test_base_asset_only_market_with_borrow_uses_injected_base_price():
    """Base-asset-only WETH market with a borrow: debt valued via the injected base price.

    No collateral held -> Σvalue 0, lltv None. Borrow 1 WETH @ injected $2500 -> debt
    2500, HF None (no collateral threshold / debt>0 -> threshold 0, HF 0/2500 = 0).
    """

    def eth_call(to, data):
        sel = data[:10].lower()
        if sel == _BORROW_BALANCE_SELECTOR:
            return "0x" + _pad32(1 * 10**18)
        raise AssertionError(f"unexpected selector {sel}")

    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals={},
        base_token="WETH",
        base_token_address=_WETH,
        resolve_base_price=lambda s: Decimal("2500"),
        resolve_base_decimals=lambda s, a: 18,
    )
    assert state.debt_usd == Decimal("2500")
    assert state.collateral_usd == Decimal("0")
    # debt > 0 but no collateral threshold -> HF = 0 / 2500 = 0 (a real liquidatable value).
    assert state.health_factor == Decimal("0")
    assert state.lltv is None


# ---------------------------------------------------------------------------
# PR #2599 review regressions (Codex / Gemini)
# ---------------------------------------------------------------------------


def test_borrow_with_unconfigured_base_address_fails_closed():
    """Gemini (high): borrow_raw > 0 but base_token_address is None -> None.

    Counting active debt as zero would inflate HF to Infinity and mask the borrow
    (Empty != Zero). resolve_base_price must not be consulted on this path.
    """

    def eth_call(to, data):
        sel = data[:10].lower()
        if sel == _COLLATERAL_BALANCE_SELECTOR:
            return "0x" + _pad32(0)  # no collateral
        if sel == _BORROW_BALANCE_SELECTOR:
            return "0x" + _pad32(1_000 * 10**6)  # active debt
        raise AssertionError(f"unexpected selector {sel}")

    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals={"WETH": {"address": _WETH}},
        base_token="USDC",
        base_token_address=None,  # unconfigured
        resolve_base_price=_no_oracle,  # asserts if called
        resolve_base_decimals=_no_decimals,
    )
    assert state is None


def test_zero_price_feed_fails_closed():
    """Gemini (medium): a held collateral whose getAssetInfoByAddress returns a
    zero priceFeed address is invalid -> None (never read getPrice against 0x0)."""
    zero_feed = "0x" + "00" * 20

    def eth_call(to, data):
        sel = data[:10].lower()
        if sel == _COLLATERAL_BALANCE_SELECTOR:
            return "0x" + _pad32(int(Decimal("1") * Decimal("1e18")))
        if sel == _ASSET_INFO_SELECTOR:
            return _asset_info_blob(
                price_feed=zero_feed, scale=int(Decimal("1e18")), liquidate_cf=int(Decimal("0.9") * Decimal("1e18"))
            )
        if sel == _GET_PRICE_SELECTOR:  # pragma: no cover - must not be reached
            raise AssertionError("getPrice must not be called for a zero price feed")
        if sel == _BORROW_BALANCE_SELECTOR:
            return "0x" + _pad32(1_000 * 10**6)
        raise AssertionError(f"unexpected selector {sel}")

    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals={"WETH": {"address": _WETH}},
        base_token="USDC",
        base_token_address=_USDC,
        resolve_base_price=lambda s: Decimal("1"),
        resolve_base_decimals=lambda s, a: 6,
    )
    assert state is None


def test_unbound_comet_or_base_fails_closed():
    """Gemini (medium): empty comet_address or base_token -> None, before any read."""

    def boom(to, data):  # pragma: no cover - must not be called
        raise AssertionError("eth_call must not run when market inputs are unbound")

    common = dict(
        chain="ethereum",
        user_address=_USER,
        collaterals={"WETH": {"address": _WETH}},
        resolve_base_price=_no_oracle,
        resolve_base_decimals=_no_decimals,
    )
    assert read_compound_v3_market_health(eth_call=boom, comet_address="", base_token="USDC", base_token_address=_USDC, **common) is None
    assert read_compound_v3_market_health(eth_call=boom, comet_address=_COMET, base_token="", base_token_address=_USDC, **common) is None


def test_huge_hf_with_dust_debt_stays_finite():
    """Codex (P2): large collateral + tiny (dust) debt -> a finite HF far above the
    999999 sentinel, with positive debt. The connector returns the UNcapped ratio;
    position_health's _to_position_health keeps it finite (Infinity only when debt==0)."""
    weth_feed = "0x" + "11" * 20

    def eth_call(to, data):
        sel = data[:10].lower()
        if sel == _COLLATERAL_BALANCE_SELECTOR:
            return "0x" + _pad32(int(Decimal("1000") * Decimal("1e18")))  # 1000 WETH
        if sel == _ASSET_INFO_SELECTOR:
            return _asset_info_blob(
                price_feed=weth_feed, scale=int(Decimal("1e18")), liquidate_cf=int(Decimal("0.9") * Decimal("1e18"))
            )
        if sel == _GET_PRICE_SELECTOR:
            return "0x" + _pad32(2000 * 10**8)  # $2000
        if sel == _BORROW_BALANCE_SELECTOR:
            return "0x" + _pad32(1)  # 1 wei base -> dust debt
        raise AssertionError(f"unexpected selector {sel}")

    state = read_compound_v3_market_health(
        eth_call=eth_call,
        chain="ethereum",
        comet_address=_COMET,
        user_address=_USER,
        collaterals={"WETH": {"address": _WETH}},
        base_token="USDC",
        base_token_address=_USDC,
        resolve_base_price=lambda s: Decimal("1"),
        resolve_base_decimals=lambda s, a: 6,
    )
    assert state is not None
    assert state.debt_usd > 0
    assert state.health_factor is not None and state.health_factor.is_finite()
    assert state.health_factor > Decimal("999999")


def test_compound_calldata_builders_exact():
    """Direct exact-calldata assertions for the Compound V3 calldata builders —
    selector + 32-byte-padded argument(s) in the correct order (CodeRabbit #2599).

    The read tests above exercise these only indirectly via the eth_call dispatch;
    this pins each builder's selector, padding, and argument order explicitly.
    """
    from almanak.connectors._strategy_base.lending_read_base import (
        build_compound_asset_info_calldata,
        build_compound_borrow_balance_calldata,
        build_compound_collateral_balance_calldata,
        build_compound_get_price_calldata,
        pad_address,
    )

    user = "0xAbC0000000000000000000000000000000000001"
    asset = "0xdEf0000000000000000000000000000000000002"
    feed = "0x0000000000000000000000000000000000000003"
    pu, pa, pf = pad_address(user), pad_address(asset), pad_address(feed)

    # pad_address: lower-cased, 0x-stripped, left-zero-padded to 32 bytes (64 hex).
    assert pu == "0" * 24 + user[2:].lower() and len(pu) == 64

    # collateralBalanceOf(user, asset): selector + pad(user) + pad(asset).
    assert build_compound_collateral_balance_calldata(user, asset) == "0x5c2549ee" + pu + pa
    # getAssetInfoByAddress(asset): selector + pad(asset).
    assert build_compound_asset_info_calldata(asset) == "0x3b3bec2e" + pa
    # getPrice(priceFeed): selector + pad(priceFeed).
    assert build_compound_get_price_calldata(feed) == "0x41976e09" + pf
    # borrowBalanceOf(user): selector + pad(user).
    assert build_compound_borrow_balance_calldata(user) == "0x374c49b4" + pu

    # Argument ORDER is load-bearing for the 2-arg builder: (user, asset) != (asset, user).
    assert build_compound_collateral_balance_calldata(user, asset) != build_compound_collateral_balance_calldata(
        asset, user
    )
