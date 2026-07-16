"""Unit coverage for ``lp_range_excludes_spot_warning`` (VIB-exp19).

LP_OPEN range bounds are frequently derived from ``market.price()`` -- a USD
*valuation* oracle -- instead of the pool's own price via
``market.pool_price()``. The two are not guaranteed to agree (hardcoded 1.0
for stablecoins; drift for volatile pairs), so a range centered on the oracle
can miss live spot entirely and mint a position that earns zero fees, with no
error raised anywhere. This guard reuses the slot0 read the compiler already
performs for ``maybe_recompute_lp_amounts_from_slot0`` (no extra RPC call) to
surface that condition loudly -- as a WARNING, never a hard failure, since
one-sided / out-of-range LP_OPEN is an established, intentional pattern this
SDK already supports (see the module docstring on
``lp_range_excludes_spot_warning`` for the fail-closed-would-break-existing-
behavior rationale).

SCOPE / known limitation: this is a **containment** check (is spot inside
``[tick_lower, tick_upper)``?). It catches the zero-fee out-of-range mint. It
does NOT catch an in-range-but-heavily-lopsided mint, where spot sits just
inside an edge and the amount recompute strands most of the requested
notional in the wallet -- the position does earn fees there, so containment
is the wrong signal for it. See
``test_volatile_pair_inside_but_near_edge_is_silent_documented_limitation``.
"""

from __future__ import annotations

import logging
from decimal import Decimal

from almanak.connectors._strategy_base.base.cl_math import lp_range_excludes_spot_warning
from almanak.connectors._strategy_base.cl_range import price_band_to_ticks


class TestSpotInsideRange:
    def test_spot_inside_range_returns_none(self):
        assert (
            lp_range_excludes_spot_warning(
                tick_lower=-100,
                tick_upper=100,
                slot0=(2**96, 0),
            )
            is None
        )

    def test_spot_at_lower_boundary_is_inside(self):
        # [tick_lower, tick_upper) is half-open -- lower bound itself is in range.
        assert (
            lp_range_excludes_spot_warning(
                tick_lower=0,
                tick_upper=100,
                slot0=(2**96, 0),
            )
            is None
        )

    def test_spot_at_upper_boundary_is_outside(self):
        # Upper bound is exclusive, mirroring Uniswap's own [lower, upper) convention.
        assert (
            lp_range_excludes_spot_warning(
                tick_lower=0,
                tick_upper=100,
                slot0=(2**96, 100),
            )
            is not None
        )


class TestSpotOutsideRange:
    def test_spot_below_range_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            msg = lp_range_excludes_spot_warning(
                tick_lower=1000,
                tick_upper=2000,
                slot0=(2**96, 500),
                range_lower=None,
                range_upper=None,
                pool_address="0x" + "cc" * 20,
                protocol="uniswap_v3",
            )
        assert msg is not None
        assert "below the range" in msg
        assert "SINGLE-SIDED" in msg
        assert "ZERO fees" in msg
        assert "0x" + "cc" * 20 in msg
        assert "uniswap_v3" in msg
        # Loud: also logged, not just returned for the caller to surface.
        assert any("does not contain" in record.message for record in caplog.records)

    def test_spot_above_range_warns(self):
        msg = lp_range_excludes_spot_warning(
            tick_lower=-2000,
            tick_upper=-1000,
            slot0=(2**96, 0),
        )
        assert msg is not None
        assert "above the range" in msg

    def test_message_includes_human_range_when_provided(self):
        msg = lp_range_excludes_spot_warning(
            tick_lower=1000,
            tick_upper=2000,
            slot0=(2**96, 500),
            range_lower="1500",
            range_upper="2500",
        )
        assert msg is not None
        assert "[1500, 2500]" in msg


class TestRealMeasuredCases:
    """The two field-measured Exp19 cases this guard exists for."""

    def test_stable_pair_oracle_centered_range_misses_real_pool_tick(self):
        """USDC/USDT 0.01% on Arbitrum (pool 0xbE3aD6a5...), the measured case.

        ``ax price USDC`` and ``ax price USDT`` BOTH return exactly 1.0
        (``source: stablecoin_peg``), so an oracle-centered +/-0.03% band is
        [0.9997, 1.0003] -> ticks [-4, 2). The real pool was measured at tick 7
        (1.0007) and, ~5h later, tick 4 (1.0004) -- BOTH outside that band.
        The position earns zero fees; nothing raises. The guard must fire.

        This also pins the stable-pair identity from the bug report: the
        ORACLE's own belief (tick 0) sits inside the band unconditionally,
        which is exactly why a strategy that both centers AND range-tests on
        market.price() holds forever and never rebalances -- while the real
        pool sits outside the whole time.
        """
        band = price_band_to_ticks(
            range_lower=Decimal("0.9997"),
            range_upper=Decimal("1.0003"),
            token0_decimals=6,
            token1_decimals=6,
            tokens_swapped=False,
            tick_spacing=1,
            current_tick=None,
        )
        assert (band.tick_lower, band.tick_upper) == (-4, 2)

        for measured_tick in (7, 4):
            msg = lp_range_excludes_spot_warning(
                tick_lower=band.tick_lower,
                tick_upper=band.tick_upper,
                slot0=(2**96, measured_tick),
                range_lower=Decimal("0.9997"),
                range_upper=Decimal("1.0003"),
                pool_address="0xbE3aD6a5669Dc0B8b12FeBC03608860C31E2eef6",
                protocol="uniswap_v3",
            )
            assert msg is not None, f"guard must fire for measured pool tick {measured_tick}"
            assert "above the range" in msg

        # The oracle's own (wrong) belief is inside -> silent. This is the
        # blind spot the guard exists to see past, not a guard failure.
        assert (
            lp_range_excludes_spot_warning(
                tick_lower=band.tick_lower,
                tick_upper=band.tick_upper,
                slot0=(2**96, 0),
            )
            is None
        )

    def test_volatile_pair_inside_but_near_edge_is_silent_documented_limitation(self):
        """Base PancakeSwap V3 WETH/USDC +/-0.10%, the second measured case.

        The observed symptom was a LOPSIDED mint (~$1.90 WETH vs $0.24 USDC of
        a $4 budget, ~$1.76 left idle) because pool spot sat NEAR the range's
        lower bound. Near, but -- per the nonzero USDC leg -- strictly INSIDE
        it: a V3 mint at/below tick_lower takes 100% token0 and exactly zero
        token1, so a $0.24 token1 leg proves spot was above tick_lower.

        This guard is a CONTAINMENT check, so it is correctly silent here: the
        position is in range and does earn fees. The lopsidedness is a
        different defect (capital stranded by the amount recompute), and
        catching it needs a different signal -- see the module docstring and
        the VIB-exp19 follow-up note.
        """
        # +/-0.10% around a 1937.05 oracle mark; WETH(18)/USDC(6), 0.05% tier.
        band = price_band_to_ticks(
            range_lower=Decimal("1935.113"),
            range_upper=Decimal("1938.987"),
            token0_decimals=18,
            token1_decimals=6,
            tokens_swapped=False,
            tick_spacing=10,
            current_tick=None,
        )
        inside_near_lower = band.tick_lower + 1
        assert band.tick_lower <= inside_near_lower < band.tick_upper

        assert (
            lp_range_excludes_spot_warning(
                tick_lower=band.tick_lower,
                tick_upper=band.tick_upper,
                slot0=(2**96, inside_near_lower),
            )
            is None
        )

        # Had spot actually drifted outside the band, the guard WOULD fire.
        assert (
            lp_range_excludes_spot_warning(
                tick_lower=band.tick_lower,
                tick_upper=band.tick_upper,
                slot0=(2**96, band.tick_lower - 1),
            )
            is not None
        )


class TestGuardReach:
    """Which LP_OPEN compile paths this guard actually covers.

    ``pancakeswap_v3`` and ``sushiswap_v3`` are Uniswap V3 forks whose
    connector manifests bind ``compiler=UniswapV3Compiler`` -- i.e. they run
    the *same* ``compile_lp_open`` the guard is wired into, so they inherit it
    for free. This pins that inheritance: if a fork ever gets its own
    compiler, this test fails and whoever forks it must wire the guard there
    too (rather than silently losing it).
    """

    def test_v3_forks_share_the_guarded_uniswap_v3_compiler(self):
        from almanak.connectors.uniswap_v3.compiler import UniswapV3Compiler
        from almanak.framework.intents.compiler import get_connector_compiler

        for protocol in ("uniswap_v3", "pancakeswap_v3", "sushiswap_v3"):
            compiler = get_connector_compiler(protocol)
            assert isinstance(compiler, UniswapV3Compiler), (
                f"{protocol} no longer routes to UniswapV3Compiler -- "
                f"wire lp_range_excludes_spot_warning into its new compiler"
            )

    def test_guarded_compile_paths_reference_the_guard(self):
        """Source-level pin: the two compile paths we wired must keep calling it."""
        import inspect

        from almanak.connectors.aerodrome.compiler import compile_lp_open_aerodrome_slipstream
        from almanak.connectors.uniswap_v3.compiler import UniswapV3Compiler

        assert "lp_range_excludes_spot_warning" in inspect.getsource(UniswapV3Compiler.compile_lp_open)
        assert "lp_range_excludes_spot_warning" in inspect.getsource(compile_lp_open_aerodrome_slipstream)


class TestNoOp:
    def test_slot0_none_returns_none(self):
        assert (
            lp_range_excludes_spot_warning(
                tick_lower=1000,
                tick_upper=2000,
                slot0=None,
            )
            is None
        )

    def test_degenerate_range_returns_none(self):
        # tick_lower >= tick_upper is a collapsed range already rejected
        # upstream by _compute_lp_ticks -- this guard must not double-report it.
        assert (
            lp_range_excludes_spot_warning(
                tick_lower=100,
                tick_upper=100,
                slot0=(2**96, 100),
            )
            is None
        )
