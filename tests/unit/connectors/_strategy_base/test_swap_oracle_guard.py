"""Unit tests for the oracle-vs-pool swap-execution divergence guard (VIB-5439).

Covers the pure guard contract: a sandwiched / depegged pool (pool quote far
below oracle-fair) fires; a good fill (pool above oracle) and routine fee+impact
(below threshold) pass; an unmeasured oracle degrades open by default and fails
closed under strict — the swap-execution sibling of check_peg_divergence's
Empty ≠ Zero discipline.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._strategy_base.swap_oracle_guard import (
    DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS,
    check_swap_oracle_divergence,
)


def _check(amount_in, pool_out, price_ratio, **kw):
    return check_swap_oracle_divergence(
        amount_in=Decimal(str(amount_in)),
        pool_quoted_out=Decimal(str(pool_out)),
        price_ratio=None if price_ratio is None else Decimal(str(price_ratio)),
        **kw,
    )


class TestSandwichAndDepeg:
    def test_pool_far_below_oracle_blocks(self) -> None:
        """A pool quoting 50% under oracle-fair (front-run / depeg-into) fires."""
        # 1000 in, oracle says fair out = 1000*2 = 2000, pool quotes only 1000.
        res = _check(1000, 1000, price_ratio=2)
        assert res.ok is False
        assert res.reason == "pool_below_oracle"
        assert res.shortfall_bps == 5000  # (2000-1000)/2000 = 50%
        assert res.oracle_fair_out == Decimal("2000")

    def test_just_over_threshold_blocks(self) -> None:
        """A shortfall above the threshold fires (boundary is strictly-greater).

        Construct a pool_out a couple of bps past the threshold; the ``+2`` (not
        ``+1``) absorbs the int() truncation toward zero in shortfall_bps so the
        measured value lands strictly above the threshold, not exactly on it.
        """
        thr = DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS
        pool_out = Decimal(1000) * (Decimal(10_000 - (thr + 2)) / Decimal(10_000))
        res = _check(1000, pool_out, price_ratio=1)
        assert res.ok is False
        assert res.shortfall_bps > thr

    def test_depeg_into_token_out(self) -> None:
        """token_out depegged (oracle price_out low → price_ratio high) blocks."""
        # USDC($1) -> USDT, USDT depegged to $0.90 => price_ratio = 1/0.90.
        # Oracle-fair out = 1000 * 1.111 = 1111 USDT; a par pool quotes ~1000.
        res = _check(1000, 1000, price_ratio=Decimal(1) / Decimal("0.90"))
        assert res.ok is False
        assert res.reason == "pool_below_oracle"


class TestGoodFillsAndHealthy:
    def test_pool_above_oracle_is_good_fill(self) -> None:
        """A pool quoting MORE than oracle-fair is never blocked (clamped to 0)."""
        res = _check(1000, 1100, price_ratio=1)
        assert res.ok is True
        assert res.shortfall_bps == 0
        assert res.reason is None

    def test_routine_fee_impact_passes(self) -> None:
        """A few-bps fee+impact under the threshold passes untouched."""
        # 10 bps below oracle-fair — well under the 150 bps default.
        res = _check(1000, Decimal("999.0"), price_ratio=1)
        assert res.ok is True
        assert res.reason is None
        assert res.shortfall_bps == 10

    def test_exactly_at_threshold_passes(self) -> None:
        """Shortfall exactly equal to the threshold passes (strictly-greater fires)."""
        thr = 100
        pool_out = Decimal(1000) * (Decimal(10_000 - thr) / Decimal(10_000))
        res = _check(1000, pool_out, price_ratio=1, threshold_bps=thr)
        assert res.shortfall_bps == thr
        assert res.ok is True


class TestUnmeasuredOracle:
    def test_none_ratio_degrades_open(self) -> None:
        """No oracle price → unmeasured, degrade-open by default (Empty ≠ Zero)."""
        res = _check(1000, 1000, price_ratio=None)
        assert res.ok is True
        assert res.reason == "oracle_unmeasured"
        assert res.oracle_fair_out is None

    def test_none_ratio_strict_fails_closed(self) -> None:
        """Strict mode refuses to trade without an oracle reference."""
        res = _check(1000, 1000, price_ratio=None, strict_when_unmeasured=True)
        assert res.ok is False
        assert res.reason == "oracle_unmeasured"

    def test_nonpositive_ratio_is_unmeasured(self) -> None:
        res = _check(1000, 1000, price_ratio=0)
        assert res.reason == "oracle_unmeasured"
        assert res.oracle_fair_out is None

    def test_nonpositive_amount_in_is_unmeasured(self) -> None:
        res = _check(0, 1000, price_ratio=1)
        assert res.reason == "oracle_unmeasured"


class TestThresholdOverride:
    def test_tight_threshold_catches_small_dislocation(self) -> None:
        res = _check(1000, Decimal("995"), price_ratio=1, threshold_bps=10)
        assert res.ok is False  # 50 bps shortfall > 10 bps threshold
        assert res.shortfall_bps == 50

    def test_wide_threshold_tolerates_large_impact(self) -> None:
        res = _check(1000, Decimal("950"), price_ratio=1, threshold_bps=1000)
        assert res.ok is True  # 500 bps shortfall < 1000 bps threshold
        assert res.shortfall_bps == 500
