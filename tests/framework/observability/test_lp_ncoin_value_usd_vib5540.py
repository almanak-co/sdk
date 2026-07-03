"""VIB-5540 (Seam B) — N-complete ``value_usd`` for a fungible LP open/close.

An N-coin fungible LP event (Curve StableSwap / CryptoSwap, Balancer) touches
ALL N pool coins, so the canonical 2-coin ``token0/token1 × amount0/amount1``
product structurally misses coins 3..N and — for a single-sided Curve deposit —
leaves ``value_usd`` unmeasured, which then zeroes ``principal_deposited`` /
``principal_recovered`` in ``attribute_lp`` (LP5 / principal_*).

``compute_lp_ncoin_value_usd`` values the event N-completely over every coin
leg, fails closed as a whole (Empty≠Zero), and treats a measured ``0`` leg as a
legitimate zero contribution.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

import almanak.framework.observability.position_events as pe
from almanak.framework.observability.position_events import compute_lp_ncoin_value_usd

# Well-known decimals for the fixture coins.
_DECIMALS = {"DAI": 18, "USDC": 6, "USDT": 6, "WBTC": 8, "WETH": 18}


@pytest.fixture
def stub_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve token decimals from a static table — no network, no gateway."""

    class _Resolver:
        def resolve(self, symbol: str, chain: str = "", log_errors: bool = False):  # noqa: ARG002
            if symbol not in _DECIMALS:
                raise KeyError(f"unknown token {symbol}")
            return SimpleNamespace(decimals=_DECIMALS[symbol])

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _Resolver(),
    )


def _raw(human: str, decimals: int) -> int:
    return int(Decimal(human) * (Decimal(10) ** decimals))


class TestComputeLpNcoinValueUsd:
    def test_three_coin_sum_all_measured(self, stub_resolver: None) -> None:
        # 46.198 DAI + 46.544 USDC + 207.3 USDT, all ≈ $1.
        coin_symbols = ["DAI", "USDC", "USDT"]
        all_amounts = [_raw("46.198", 18), _raw("46.544", 6), _raw("207.3", 6)]
        prices = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        out = compute_lp_ncoin_value_usd(coin_symbols, all_amounts, prices, chain="ethereum")
        assert Decimal(out) == Decimal("46.198") + Decimal("46.544") + Decimal("207.3")

    def test_single_sided_open_unfunded_legs_are_measured_zero(self, stub_resolver: None) -> None:
        # Single-sided USDC deposit: DAI + USDT legs are a measured 0 (funded 0),
        # NOT unmeasured. Value = the USDC leg only.
        coin_symbols = ["DAI", "USDC", "USDT"]
        all_amounts = [0, _raw("300", 6), 0]
        prices = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        out = compute_lp_ncoin_value_usd(coin_symbols, all_amounts, prices, chain="ethereum")
        assert Decimal(out) == Decimal("300")

    def test_non_dollar_numeraire_crypto_pool(self, stub_resolver: None) -> None:
        # tricrypto-style: WBTC leg priced far from $1 (VIB-5566).
        coin_symbols = ["USDT", "WBTC", "WETH"]
        all_amounts = [_raw("100", 6), _raw("0.01", 8), _raw("0.05", 18)]
        prices = {"USDT": Decimal("1"), "WBTC": Decimal("60000"), "WETH": Decimal("3000")}
        out = compute_lp_ncoin_value_usd(coin_symbols, all_amounts, prices, chain="ethereum")
        # 100 + 0.01*60000 + 0.05*3000 = 100 + 600 + 150 = 850
        assert Decimal(out) == Decimal("850")

    def test_fails_closed_on_missing_price(self, stub_resolver: None) -> None:
        coin_symbols = ["DAI", "USDC", "USDT"]
        all_amounts = [_raw("46", 18), _raw("46", 6), _raw("207", 6)]
        prices = {"DAI": Decimal("1"), "USDC": Decimal("1")}  # USDT price missing
        out = compute_lp_ncoin_value_usd(coin_symbols, all_amounts, prices, chain="ethereum")
        assert out == ""  # Empty≠Zero — whole close unmeasured, never a partial sum

    def test_fails_closed_on_unmeasured_amount(self, stub_resolver: None) -> None:
        # A None amount is UNMEASURED (Empty≠Zero) → whole value unmeasured.
        coin_symbols = ["DAI", "USDC", "USDT"]
        all_amounts = [_raw("46", 18), None, _raw("207", 6)]
        prices = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        out = compute_lp_ncoin_value_usd(coin_symbols, all_amounts, prices, chain="ethereum")
        assert out == ""

    def test_fails_closed_on_unresolvable_decimals(self, stub_resolver: None) -> None:
        coin_symbols = ["DAI", "MYSTERY", "USDT"]
        all_amounts = [_raw("46", 18), 1000, _raw("207", 6)]
        prices = {"DAI": Decimal("1"), "MYSTERY": Decimal("1"), "USDT": Decimal("1")}
        out = compute_lp_ncoin_value_usd(coin_symbols, all_amounts, prices, chain="ethereum")
        assert out == ""

    def test_empty_coin_symbols_returns_empty(self, stub_resolver: None) -> None:
        assert compute_lp_ncoin_value_usd([], [], {}, chain="ethereum") == ""

    def test_length_mismatch_returns_empty(self, stub_resolver: None) -> None:
        out = compute_lp_ncoin_value_usd(["DAI", "USDC"], [1], {"DAI": Decimal("1")}, chain="ethereum")
        assert out == ""

    def test_nested_price_dict_supported(self, stub_resolver: None) -> None:
        coin_symbols = ["USDC"]
        all_amounts = [_raw("300", 6)]
        prices = {"USDC": {"price_usd": "1.0"}}
        out = compute_lp_ncoin_value_usd(coin_symbols, all_amounts, prices, chain="ethereum")
        assert Decimal(out) == Decimal("300")


class TestApplyLpCloseValueUsdNcoin:
    def test_owns_close_when_coin_symbols_present(self, stub_resolver: None) -> None:
        event = pe.PositionEvent(
            deployment_id="d1",
            position_id="curve-3pool",
            position_type="LP",
            event_type="CLOSE",
        )
        lp_close = SimpleNamespace(
            coin_symbols=["DAI", "USDC", "USDT"],
            all_amounts=[_raw("100", 18), _raw("100", 6), _raw("100", 6)],
        )
        ctx = SimpleNamespace(extracted={"lp_close_data": lp_close}, chain="ethereum")
        prices = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        owned = pe._apply_lp_close_value_usd_ncoin(event, ctx, prices)
        assert owned is True
        assert Decimal(event.value_usd) == Decimal("300")

    def test_not_applicable_for_two_coin_venue(self, stub_resolver: None) -> None:
        event = pe.PositionEvent(deployment_id="d1", position_id="p1", position_type="LP", event_type="CLOSE")
        lp_close = SimpleNamespace(coin_symbols=None, all_amounts=[1, 2])
        ctx = SimpleNamespace(extracted={"lp_close_data": lp_close}, chain="ethereum")
        owned = pe._apply_lp_close_value_usd_ncoin(event, ctx, {})
        assert owned is False
        assert not event.value_usd  # 2-coin path is left to run

    def test_owns_close_but_stays_unmeasured_on_missing_price(self, stub_resolver: None) -> None:
        event = pe.PositionEvent(deployment_id="d1", position_id="p1", position_type="LP", event_type="CLOSE")
        lp_close = SimpleNamespace(
            coin_symbols=["DAI", "USDC", "USDT"],
            all_amounts=[_raw("100", 18), _raw("100", 6), _raw("100", 6)],
        )
        ctx = SimpleNamespace(extracted={"lp_close_data": lp_close}, chain="ethereum")
        owned = pe._apply_lp_close_value_usd_ncoin(event, ctx, {"DAI": Decimal("1")})
        assert owned is True  # N-coin path OWNS the close — no misleading 2-coin fallback
        assert not event.value_usd  # but stays unmeasured (Empty≠Zero)


class TestApplyLpCloseColumns:
    """End-to-end over the changed ``_apply_lp_close_columns`` branch: the N-coin
    close is valued N-completely; a 2-coin close still runs the canonical path."""

    def _ctx(self, lp_close, pool: str) -> SimpleNamespace:
        return SimpleNamespace(
            extracted={"lp_close_data": lp_close},
            intent=SimpleNamespace(pool=pool),
            chain="ethereum",
        )

    def test_ncoin_curve_close_valued_over_all_legs(self, stub_resolver: None) -> None:
        # Single-sided Curve close: no token0/token1 direction (pool="3pool" has
        # no slash), so the canonical 2-coin path fails closed — but coin_symbols
        # carries the full universe and the N-coin path values every returned leg.
        event = pe.PositionEvent(deployment_id="d1", position_id="curve-3pool", position_type="LP", event_type="CLOSE")
        lp_close = SimpleNamespace(
            coin_symbols=["DAI", "USDC", "USDT"],
            all_amounts=[_raw("46.2", 18), _raw("46.5", 6), _raw("207.3", 6)],
            # attrs read by _pair_tokens_from_declared_legs / _apply_lp_close
            amount0_received=None,
            amount1_received=None,
        )
        prices = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        pe._apply_lp_close_columns(event, self._ctx(lp_close, "3pool"), None, prices)
        assert Decimal(event.value_usd) == Decimal("46.2") + Decimal("46.5") + Decimal("207.3")

    def test_two_coin_close_uses_canonical_path(self, stub_resolver: None) -> None:
        # A concentrated-liquidity close (no coin_symbols) keeps the 2-coin path;
        # tokens resolve from the pool descriptor and value = a0*p0 + a1*p1.
        event = pe.PositionEvent(
            deployment_id="d1",
            position_id="uni-1",
            position_type="LP",
            event_type="CLOSE",
            amount0=str(_raw("2", 18)),
            amount1=str(_raw("5000", 6)),
        )
        lp_close = SimpleNamespace(coin_symbols=None, all_amounts=[_raw("2", 18), _raw("5000", 6)])
        prices = {"WETH": Decimal("3000"), "USDC": Decimal("1")}
        pe._apply_lp_close_columns(event, self._ctx(lp_close, "WETH/USDC/3000"), None, prices)
        # 2*3000 + 5000*1 = 11000
        assert Decimal(event.value_usd) == Decimal("11000")
