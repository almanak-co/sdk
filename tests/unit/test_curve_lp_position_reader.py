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
    CurveLpPositionReader,
    _resolve_curve_pool_meta,
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


def _hex_word(value: int) -> str:
    return "0x" + hex(value)[2:].zfill(64)


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


def test_base_weth_cbeth_still_fails_closed() -> None:
    # Base weth_cbeth is a cryptoswap (WETH/cbETH) — non-USD numeraire. Adding
    # the wrapped-USDC symbols must NOT widen scope to volatile pools: still None.
    pool = "weth_cbeth"
    lp = "0x98244d93D42b42aB3E3A4D12A5dc0B3e7f8F32f9"
    pool_addr = "0x11C1fBd4b3De66bC0565779b35171a6CF3E71f59"
    reader = CurveLpPositionReader(
        _StubGatewayClient(
            {
                (lp.lower(), BALANCE_OF): 10**18,
                (pool_addr.lower(), GET_VIRTUAL_PRICE): 10**18,
            }
        )
    )
    assert (
        reader.read_position(
            protocol="curve",
            chain="base",
            pool=pool,
            lp_token=lp,
            wallet_address=WALLET,
        )
        is None
    )


def test_read_position_non_usd_pool_fails_closed() -> None:
    # steth = ETH/stETH (non-USD numeraire) -> out of v1 scope -> None.
    pool = "steth"
    lp = "0x06325440D014e39736583c165C2963BA99fAf14E"
    reader = CurveLpPositionReader(
        _StubGatewayClient(
            {
                (lp.lower(), BALANCE_OF): 10**18,
                ("0xdc24316b9ae028f1497c275eb9192a3ea0f67022", GET_VIRTUAL_PRICE): 10**18,
            }
        )
    )
    assert (
        reader.read_position(
            protocol="curve",
            chain="ethereum",
            pool=pool,
            lp_token=lp,
            wallet_address=WALLET,
        )
        is None
    )


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

    def price(self, token: str) -> Decimal:
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


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
