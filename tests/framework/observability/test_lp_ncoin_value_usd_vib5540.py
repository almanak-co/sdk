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
from tests.support.token_resolver import FakeToken, FakeTokenResolver

# Well-known decimals AND real mainnet addresses for the fixture coins.
#
# The addresses are not decoration. ``FakeToken`` requires them (VIB-6100 review
# of PR #3472) because a double that yields ``address=""`` returns a token
# production cannot construct — ``ResolvedToken`` raises on an empty address —
# and any address-dependent branch downstream is then silently inert against it.
# These values are the static registry's own (verified with
# ``resolve(symbol, chain=..., skip_gateway=True)``), lowercase as the resolver
# emits them for EVM chains. Solana mints are base58 and case-sensitive, so the
# resolver does NOT lowercase them — the mint below is verbatim.
_COINS = {
    "DAI": ("0x6b175474e89094c44da98b954eedeac495271d0f", 18, "ethereum"),
    "USDC": ("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 6, "ethereum"),
    "USDT": ("0xdac17f958d2ee523a2206206994597c13d831ec7", 6, "ethereum"),
    "WBTC": ("0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", 8, "ethereum"),
    "WETH": ("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", 18, "ethereum"),
}

# The Solana leg is keyed by MINT, not symbol — that is the identity the caller
# passes for a Solana pool, and it is why this entry carries its own chain.
_SOL_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


@pytest.fixture
def stub_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve token decimals from a static table — no network, no gateway."""

    # VIB-6100: use the SHARED double. The hand-rolled stub this replaced did
    # not accept ``skip_gateway`` and raised a bare ``KeyError`` on a miss —
    # both were swallowed by the old fail-open ``except Exception``, so
    # ``test_two_coin_close_uses_canonical_path`` passed while exercising the
    # fallback branch it was named for. Exactly the trap VIB-6100 documents.
    resolver = FakeTokenResolver()
    for symbol, (address, decimals, chain) in _COINS.items():
        resolver.add(symbol, FakeToken(symbol=symbol, address=address, decimals=decimals, chain=chain))
    resolver.add(
        _SOL_USDC,
        FakeToken(symbol="USDC", address=_SOL_USDC, decimals=6, chain="solana"),
    )

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda *_a, **_k: resolver,
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

    def test_mixed_case_solana_mint_is_preserved(self, stub_resolver: None) -> None:
        prices = {f"solana:{_SOL_USDC}": {"price_usd": "1"}}

        out = compute_lp_ncoin_value_usd([_SOL_USDC], [_raw("3", 6)], prices, chain="solana")

        assert Decimal(out) == Decimal("3")


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
        #
        # Legs are in CHAIN order — USDC first, because on ethereum USDC
        # (0xa0b8…) is the lower address than WETH (0xc02a…), so a real
        # Uniswap V3 USDC/WETH pool reports token0=USDC and its parser emits
        # amount0 as the USDC leg. The descriptor previously read "WETH/USDC"
        # with a WETH amount0, which is not a pool that exists on this chain;
        # it only passed because the stub resolver returned empty addresses and
        # left the pair realignment inert — the very false-green this file's
        # ``stub_resolver`` note describes, one layer down. With real addresses
        # the realignment correctly sorts the symbols to (USDC, WETH) while the
        # amounts stay put, mis-pairing 18-dec with 6-dec and turning $11,000
        # into $2e12. (VIB-6100 review of PR #3472.)
        event = pe.PositionEvent(
            deployment_id="d1",
            position_id="uni-1",
            position_type="LP",
            event_type="CLOSE",
            amount0=str(_raw("5000", 6)),
            amount1=str(_raw("2", 18)),
        )
        lp_close = SimpleNamespace(coin_symbols=None, all_amounts=[_raw("5000", 6), _raw("2", 18)])
        prices = {"WETH": Decimal("3000"), "USDC": Decimal("1")}
        pe._apply_lp_close_columns(event, self._ctx(lp_close, "USDC/WETH/3000"), None, prices)
        # 5000*1 + 2*3000 = 11000
        assert Decimal(event.value_usd) == Decimal("11000")
