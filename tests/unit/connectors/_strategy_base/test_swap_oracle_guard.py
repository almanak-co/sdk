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
    DEFAULT_STABLE_ORACLE_FLOOR_RESIDUAL_BPS,
    DEFAULT_STABLE_ORACLE_FLOOR_TOLERANCE_BPS,
    DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS,
    DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS,
    DEFAULT_VOLATILE_ORACLE_FLOOR_TOLERANCE_BPS,
    check_swap_oracle_divergence,
    clamp_min_out_to_oracle,
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


# =============================================================================
# Executed-floor oracle anchor (VIB-5490): clamp_min_out_to_oracle
# =============================================================================


def _clamp(pool_floor_wei, pool_quoted_out_wei, amount_in, price_ratio, decimals, tol_bps, residual_bps=50):
    return clamp_min_out_to_oracle(
        pool_floor_wei=pool_floor_wei,
        pool_quoted_out_wei=pool_quoted_out_wei,
        amount_in=Decimal(str(amount_in)),
        price_ratio=None if price_ratio is None else Decimal(str(price_ratio)),
        token_out_decimals=decimals,
        tolerance_bps=tol_bps,
        residual_bps=residual_bps,
    )


class TestClampRaisesFloorTowardOracle:
    def test_loose_slippage_floor_is_raised_to_oracle(self) -> None:
        """The whole point: a wide-slippage pool floor is raised to the oracle.

        1000 in @ ratio 1 → oracle_fair 1000 (6-dec = 1e9). Pool quote also 1000,
        but the operator set a very wide slippage so pool_floor is only 800. With
        a 150 bps tolerance the oracle floor is 1000*(1-0.015)=985, and the 50 bps
        residual cap is pool_quote*(1-0.005)=995 ≥ 985, so the oracle floor binds:
        the clamp raises the on-chain floor from 800 to 985, capping sandwich
        extraction at 150 bps instead of the 2000 bps the slippage would allow.
        """
        oracle_fair_wei = 1000 * 10**6
        res = _clamp(
            pool_floor_wei=800 * 10**6,
            pool_quoted_out_wei=oracle_fair_wei,
            amount_in=1000,
            price_ratio=1,
            decimals=6,
            tol_bps=150,
            residual_bps=50,
        )
        assert res.clamped is True
        assert res.reason is None
        assert res.min_out_wei == oracle_fair_wei * (10_000 - 150) // 10_000  # 985e6
        assert res.min_out_wei > res.pool_floor_wei

    def test_tight_slippage_floor_not_lowered(self) -> None:
        """The clamp NEVER lowers an already-tight pool floor (max semantics)."""
        oracle_fair_wei = 1000 * 10**6
        pool_floor = 999 * 10**6  # 10 bps slippage, tighter than the 150 bps tol
        res = _clamp(
            pool_floor_wei=pool_floor,
            pool_quoted_out_wei=oracle_fair_wei,
            amount_in=1000,
            price_ratio=1,
            decimals=6,
            tol_bps=150,
        )
        assert res.clamped is False
        assert res.min_out_wei == pool_floor  # unchanged, not lowered to 985e6


class TestClampPreservesResidualDriftBuffer:
    """VIB-5490 fix: the cap is ``pool_quote × (1 − residual)``, NOT the raw quote.
    A genuine >tolerance-impact swap keeps a benign inter-block-drift buffer so it
    fills against a drifted pool, instead of being pinned to the exact clean quote
    (zero buffer → revert on any adverse drift)."""

    def test_high_impact_volatile_swap_keeps_residual_buffer(self) -> None:
        """oracle_fair = 1000, pool quote only 900 (1000 bps real impact, > the
        500 bps volatile tolerance) with a WIDE operator slippage (pool_floor low).

        Raw-quote cap (the old bug) would pin min_out to the exact 900 clean quote,
        leaving ZERO buffer → benign drift reverts. The residual cap pins it to
        pool_quote*(1-200bps)=882, preserving an 18-unit (200 bps) drift buffer
        BELOW the clean quote while still raising the floor above the loose
        pool_floor.
        """
        pool_quote_wei = 900 * 10**6  # 1000 bps genuine impact
        pool_floor_wei = 810 * 10**6  # 10% operator slippage — loose
        residual = DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS  # 200
        res = _clamp(
            pool_floor_wei=pool_floor_wei,
            pool_quoted_out_wei=pool_quote_wei,
            amount_in=1000,
            price_ratio=1,
            decimals=6,
            tol_bps=DEFAULT_VOLATILE_ORACLE_FLOOR_TOLERANCE_BPS,  # 500
            residual_bps=residual,
        )
        expected_cap = pool_quote_wei * (10_000 - residual) // 10_000  # 882e6
        assert res.oracle_floor_wei == expected_cap
        assert res.min_out_wei == expected_cap
        # The load-bearing property: a strictly-positive drift buffer below the
        # clean quote (NOT min_out == quote), so benign drift does not revert.
        assert res.min_out_wei < pool_quote_wei
        assert pool_quote_wei - res.min_out_wei == pool_quote_wei * residual // 10_000
        assert res.min_out_wei > pool_floor_wei  # still tighter than the loose floor

    def test_residual_cap_binds_only_above_pool_floor(self) -> None:
        """Self-scoping: when the operator's own slippage is TIGHTER than the
        residual, pool_floor dominates and the clamp is a no-op (never loosens)."""
        pool_quote_wei = 900 * 10**6
        pool_floor_wei = 895 * 10**6  # ~56 bps slippage, tighter than 200 bps residual
        res = _clamp(
            pool_floor_wei=pool_floor_wei,
            pool_quoted_out_wei=pool_quote_wei,
            amount_in=1000,
            price_ratio=1,
            decimals=6,
            tol_bps=DEFAULT_VOLATILE_ORACLE_FLOOR_TOLERANCE_BPS,
            residual_bps=DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS,
        )
        assert res.min_out_wei == pool_floor_wei  # no-op, operator floor respected
        assert res.clamped is False

    def test_floor_always_below_quote_with_residual_and_above_pool_floor(self) -> None:
        """Invariant sweep: whenever the clamp raises the floor, it leaves at least
        the residual buffer below the clean quote (never pins to the raw quote),
        and never drops below pool_floor."""
        for tol in (10, 150, 500, 2000):
            for residual in (30, 50, 200, 300):
                for pool_quote in (850, 900, 1000, 1100):
                    pool_quote_wei = pool_quote * 10**6
                    res = _clamp(
                        pool_floor_wei=int(pool_quote * 0.80) * 10**6,  # loose 20% floor
                        pool_quoted_out_wei=pool_quote_wei,
                        amount_in=1000,
                        price_ratio=1,
                        decimals=6,
                        tol_bps=tol,
                        residual_bps=residual,
                    )
                    quote_cap = pool_quote_wei * (10_000 - residual) // 10_000
                    # min_out never exceeds the residual-buffered cap → always a
                    # benign-drift buffer below the clean quote.
                    assert res.min_out_wei <= quote_cap
                    assert res.min_out_wei < pool_quote_wei
                    assert res.min_out_wei >= res.pool_floor_wei


class TestClampDegradesOpen:
    def test_no_oracle_leaves_pool_floor_untouched(self) -> None:
        """Empty ≠ Zero: no oracle ratio must not fabricate a higher floor."""
        pool_floor = 800 * 10**6
        res = _clamp(pool_floor, 1000 * 10**6, 1000, price_ratio=None, decimals=6, tol_bps=150)
        assert res.clamped is False
        assert res.reason == "oracle_unmeasured"
        assert res.min_out_wei == pool_floor
        assert res.oracle_floor_wei is None

    def test_nonpositive_ratio_degrades_open(self) -> None:
        pool_floor = 800 * 10**6
        res = _clamp(pool_floor, 1000 * 10**6, 1000, price_ratio=0, decimals=6, tol_bps=150)
        assert res.min_out_wei == pool_floor
        assert res.reason == "oracle_unmeasured"

    def test_nonpositive_amount_degrades_open(self) -> None:
        pool_floor = 800 * 10**6
        res = _clamp(pool_floor, 1000 * 10**6, 0, price_ratio=1, decimals=6, tol_bps=150)
        assert res.min_out_wei == pool_floor
        assert res.reason == "oracle_unmeasured"

    def test_nonpositive_tolerance_degrades_open(self) -> None:
        """A zero / negative tolerance is a misconfig, not a 0-bps clamp that would
        demand the full oracle-fair output on-chain (guaranteed revert). Fail-loud
        with a config-invalid reason."""
        pool_floor = 800 * 10**6
        res = _clamp(pool_floor, 1000 * 10**6, 1000, price_ratio=1, decimals=6, tol_bps=0)
        assert res.min_out_wei == pool_floor
        assert res.reason == "oracle_config_invalid"

    def test_tolerance_above_bps_max_degrades_loud(self) -> None:
        """A ``tolerance_bps > 10_000`` (a fat-fingered wide ``oracle_guard_bps``
        override) would drive ``_BPS - tol`` negative → a negative oracle floor →
        the outer ``max`` silently falls back to the pool floor, SILENTLY DISABLING
        the anchor. Must instead degrade-open LOUDLY (config-invalid), never a
        negative floor."""
        pool_floor = 800 * 10**6
        res = _clamp(pool_floor, 1000 * 10**6, 1000, price_ratio=1, decimals=6, tol_bps=10_001)
        assert res.min_out_wei == pool_floor  # pool floor, NOT a negative oracle floor
        assert res.min_out_wei >= 0
        assert res.reason == "oracle_config_invalid"
        assert res.oracle_floor_wei is None

    def test_tolerance_at_bps_max_is_valid(self) -> None:
        """Exactly 10_000 bps (a 100% tolerance → oracle floor 0) is the boundary
        and is still a VALID config (degrades to the pool floor via the cap/max, not
        a config error)."""
        pool_floor = 800 * 10**6
        res = _clamp(pool_floor, 1000 * 10**6, 1000, price_ratio=1, decimals=6, tol_bps=10_000)
        assert res.reason is None
        assert res.min_out_wei == pool_floor  # oracle floor is 0 → max keeps pool floor

    def test_nonpositive_residual_degrades_open(self) -> None:
        """A zero / negative residual is a misconfig, NOT a raw-quote cap (which
        would leave zero drift buffer → revert). Fail-loud config-invalid."""
        pool_floor = 800 * 10**6
        res = _clamp(pool_floor, 1000 * 10**6, 1000, price_ratio=1, decimals=6, tol_bps=150, residual_bps=0)
        assert res.min_out_wei == pool_floor
        assert res.reason == "oracle_config_invalid"

    def test_residual_above_bps_max_degrades_loud(self) -> None:
        """``residual_bps > 10_000`` would drive the quote cap negative → degrade-open
        LOUDLY rather than a negative floor."""
        pool_floor = 800 * 10**6
        res = _clamp(pool_floor, 1000 * 10**6, 1000, price_ratio=1, decimals=6, tol_bps=150, residual_bps=10_001)
        assert res.min_out_wei == pool_floor
        assert res.min_out_wei >= 0
        assert res.reason == "oracle_config_invalid"

    def test_nonpositive_quote_degrades_open(self) -> None:
        """Nit #4: a non-positive pool quote must degrade open, NOT apply an
        uncapped oracle floor (which could exceed any real fill → guaranteed
        revert)."""
        pool_floor = 800 * 10**6
        res = _clamp(pool_floor, 0, 1000, price_ratio=1, decimals=6, tol_bps=150)
        assert res.min_out_wei == pool_floor
        assert res.reason == "oracle_unmeasured"
        assert res.oracle_floor_wei is None


class TestClampToleranceDefaults:
    def test_stable_default_matches_detection_threshold(self) -> None:
        """Stable clamp tolerance reuses the detection threshold — one knob, no
        second threshold source (VIB-5490 design constraint)."""
        assert DEFAULT_STABLE_ORACLE_FLOOR_TOLERANCE_BPS == DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS

    def test_volatile_default_is_wider_than_stable(self) -> None:
        assert DEFAULT_VOLATILE_ORACLE_FLOOR_TOLERANCE_BPS > DEFAULT_STABLE_ORACLE_FLOOR_TOLERANCE_BPS

    def test_volatile_residual_is_wider_than_stable(self) -> None:
        """Volatile pools drift more block-to-block, so need a wider drift buffer."""
        assert DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS > DEFAULT_STABLE_ORACLE_FLOOR_RESIDUAL_BPS

    def test_residuals_are_tighter_than_tolerances(self) -> None:
        """The drift residual is a smaller buffer than the anchor tolerance on each
        pool type — so the anchor still bites well before the residual floor."""
        assert DEFAULT_STABLE_ORACLE_FLOOR_RESIDUAL_BPS < DEFAULT_STABLE_ORACLE_FLOOR_TOLERANCE_BPS
        assert DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS < DEFAULT_VOLATILE_ORACLE_FLOOR_TOLERANCE_BPS
