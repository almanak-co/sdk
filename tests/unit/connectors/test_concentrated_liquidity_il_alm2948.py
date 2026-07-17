"""True V3 IL on the shared CL kernel (ALM-2948, live half).

Covers the kernel's tier-2 Decimal math (composition, IL), the live
ILCalculator replacement, the live-vs-backtest agreement contract (the
ticket's acceptance cell), and lp_valuer's delegation parity.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.concentrated_liquidity_math import (
    concentrated_il,
    position_token_amounts,
    price_to_tick,
    sqrt_price_decimal,
    tick_to_sqrt_price_decimal,
)


class TestPositionTokenAmounts:
    def test_below_range_all_token0(self):
        amount0, amount1 = position_token_amounts(
            Decimal(1), sqrt_price_decimal(1000), sqrt_price_decimal(1500), sqrt_price_decimal(2500)
        )
        assert amount0 > 0 and amount1 == 0

    def test_above_range_all_token1(self):
        amount0, amount1 = position_token_amounts(
            Decimal(1), sqrt_price_decimal(3000), sqrt_price_decimal(1500), sqrt_price_decimal(2500)
        )
        assert amount0 == 0 and amount1 > 0

    def test_in_range_mixed(self):
        amount0, amount1 = position_token_amounts(
            Decimal(1), sqrt_price_decimal(2000), sqrt_price_decimal(1500), sqrt_price_decimal(2500)
        )
        assert amount0 > 0 and amount1 > 0

    def test_composition_continuous_at_bounds(self):
        # Approaching the bound from inside converges to the single-sided case.
        at_bound = position_token_amounts(
            Decimal(1), sqrt_price_decimal(2500), sqrt_price_decimal(1500), sqrt_price_decimal(2500)
        )
        just_inside = position_token_amounts(
            Decimal(1), sqrt_price_decimal("2499.99"), sqrt_price_decimal(1500), sqrt_price_decimal(2500)
        )
        assert at_bound[0] == 0
        assert just_inside[0] < Decimal("0.0000001")


class TestConcentratedIL:
    def test_il_never_positive(self):
        # IL vs holding is <= 0 for any move (fees excluded by definition).
        for current in (Decimal(800), Decimal(1500), Decimal(2000), Decimal(2400), Decimal(9000)):
            il, _, _ = concentrated_il(Decimal(2000), current, Decimal(1500), Decimal(2500))
            assert il <= 0, current

    def test_no_move_zero(self):
        il, _, _ = concentrated_il(Decimal(2000), Decimal(2000), Decimal(1500), Decimal(2500))
        assert il == 0

    def test_narrow_range_loses_more_than_wide(self):
        wide, _, _ = concentrated_il(Decimal(2000), Decimal(2600), Decimal(1000), Decimal(4000))
        narrow, _, _ = concentrated_il(Decimal(2000), Decimal(2600), Decimal(1800), Decimal(2200))
        assert narrow < wide < 0

    def test_full_range_approaches_v2_il(self):
        # A very wide range converges to the classic 2*sqrt(r)/(1+r) - 1.
        entry, current = Decimal(2000), Decimal(3000)
        il, _, _ = concentrated_il(entry, current, Decimal("0.0001"), Decimal(10**9))
        r = current / entry
        v2 = 2 * sqrt_price_decimal(r) / (1 + r) - 1
        assert abs(il - v2) < Decimal("0.001")

    def test_degenerate_range_raises(self):
        with pytest.raises(ValueError, match="Degenerate"):
            concentrated_il(Decimal(2000), Decimal(2100), Decimal(1500), Decimal(1500))


class TestLiveBacktestAgreement:
    """The ALM-2948 acceptance contract: live IL and backtest IL agree.

    The backtest lane (`ImpermanentLossCalculator.calculate_il_v3`) computes
    IL from (prices, ticks, liquidity) on its own plane; the live lane now
    computes from the same math via the kernel. On an equal-decimals pair
    (ticks == human plane) they must agree within rounding.
    """

    @pytest.mark.parametrize("current", ["1600", "2000", "2300", "2500", "3200"])
    def test_agreement_equal_decimals_pair(self, current: str):
        from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
            ImpermanentLossCalculator,
        )
        from almanak.framework.data.lp.calculator import ILCalculator

        entry = Decimal("2000")
        now = Decimal(current)
        tick_lower = price_to_tick(Decimal("1500"), 18, 18)
        tick_upper = price_to_tick(Decimal("2500"), 18, 18)

        live = ILCalculator().calculate_il_concentrated(
            entry_price_a=entry,
            entry_price_b=Decimal("1"),
            current_price_a=now,
            current_price_b=Decimal("1"),
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            decimals0=18,
            decimals1=18,
        )
        backtest_il, _amount0, _amount1 = ImpermanentLossCalculator().calculate_il_v3(
            entry_price=entry,
            current_price=now,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=Decimal("1000"),
        )
        # Conventions: live il_ratio is negative on loss; the backtest lane
        # returns positive loss fractions.
        assert abs(-live.il_ratio - backtest_il) < Decimal("0.001"), (live.il_ratio, backtest_il)

    def test_decimal_asymmetric_pair_matches_equal_decimals_semantics(self):
        """WETH/USDC raw ticks + (18, 6) must equal the same human range at (18, 18).

        This is the plane bug the old heuristic had: raw ticks were read as
        if decimals never existed.
        """
        from almanak.framework.data.lp.calculator import ILCalculator

        calc = ILCalculator()
        human_lower, human_upper = Decimal("1500"), Decimal("2500")

        asymmetric = calc.calculate_il_concentrated(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("2400"),
            current_price_b=Decimal("1"),
            tick_lower=price_to_tick(human_lower, 18, 6),
            tick_upper=price_to_tick(human_upper, 18, 6),
            decimals0=18,
            decimals1=6,
        )
        equal = calc.calculate_il_concentrated(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("2400"),
            current_price_b=Decimal("1"),
            tick_lower=price_to_tick(human_lower, 18, 18),
            tick_upper=price_to_tick(human_upper, 18, 18),
            decimals0=18,
            decimals1=18,
        )
        assert abs(asymmetric.il_ratio - equal.il_ratio) < Decimal("0.0001")


class TestLpValuerParity:
    def test_compute_amounts_matches_kernel(self):
        from almanak.framework.valuation.lp_valuer import _compute_amounts, _tick_to_sqrt_price

        sqrt_lower = _tick_to_sqrt_price(-201180)
        sqrt_upper = _tick_to_sqrt_price(-190000)
        sqrt_now = _tick_to_sqrt_price(-196000)
        amounts = _compute_amounts(Decimal("1000000"), sqrt_now, sqrt_lower, sqrt_upper)

        kernel0, kernel1 = position_token_amounts(Decimal("1000000"), sqrt_now, sqrt_lower, sqrt_upper)
        assert amounts.amount0 == kernel0
        assert amounts.amount1 == kernel1

    def test_tick_to_sqrt_price_matches_kernel_and_x96(self):
        from almanak.connectors._strategy_base.concentrated_liquidity_math import Q96, tick_to_sqrt_price_x96
        from almanak.framework.valuation.lp_valuer import _tick_to_sqrt_price

        for tick in (-201180, -1, 0, 1, 100000):
            dec = tick_to_sqrt_price_decimal(tick)
            wrapper = _tick_to_sqrt_price(tick)
            x96 = Decimal(tick_to_sqrt_price_x96(tick)) / Decimal(Q96)
            assert wrapper == dec, tick
            assert abs(dec - x96) / dec < Decimal("0.000001"), tick

    def test_negative_liquidity_rejected(self):
        with pytest.raises(ValueError, match="non-negative"):
            position_token_amounts(
                Decimal(-1), sqrt_price_decimal(2000), sqrt_price_decimal(1500), sqrt_price_decimal(2500)
            )
