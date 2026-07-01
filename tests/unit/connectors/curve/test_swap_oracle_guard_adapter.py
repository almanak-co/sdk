"""Adapter-level wiring tests for the P0-8 swap oracle/MEV min-out guard (VIB-5439).

Proves the guard is actually wired into ``CurveAdapter.swap`` / ``swap_underlying``:
a pool quote far below the oracle ``price_ratio`` blocks the swap BEFORE any tx is
built; a healthy quote passes; no ``price_ratio`` degrades open (preserving the
pre-guard behaviour) unless strict is requested.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors.curve.adapter import CURVE_POOLS, CurveAdapter, CurveConfig


@pytest.fixture
def adapter() -> CurveAdapter:
    return CurveAdapter(
        CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_slippage_bps=50,
        )
    )


@pytest.fixture
def pool_address() -> str:
    return CURVE_POOLS["ethereum"]["3pool"]["address"]


@pytest.fixture
def metapool_address() -> str:
    """A metapool (FRAX/3CRV) for the ``swap_underlying`` (zap) executed-floor path."""
    return CURVE_POOLS["ethereum"]["frax_3crv"]["address"]


class TestSwapGuardWiring:
    def test_healthy_quote_passes(self, adapter: CurveAdapter, pool_address: str) -> None:
        """A ~1:1 stable pool with a matching oracle ratio builds the swap."""
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("1"),  # oracle agrees with the ~1:1 pool estimate
        )
        assert result.success is True
        assert result.transactions

    def test_pool_below_oracle_blocks(self, adapter: CurveAdapter, pool_address: str) -> None:
        """Oracle fair-out far above the pool quote => pre-moved pool, blocked."""
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("2"),  # oracle: 1 USDC = 2 DAI; pool quotes ~1:1
        )
        assert result.success is False
        assert "below oracle-fair" in (result.error or "")
        assert not result.transactions

    def test_no_oracle_degrades_open(self, adapter: CurveAdapter, pool_address: str) -> None:
        """No price_ratio => unmeasured => degrade open (unchanged pre-guard path)."""
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=None,
        )
        assert result.success is True

    def test_no_oracle_strict_blocks(self, adapter: CurveAdapter, pool_address: str) -> None:
        """Strict mode refuses to trade with no oracle reference."""
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=None,
            strict_oracle_guard=True,
        )
        assert result.success is False
        assert "without an independent oracle reference" in (result.error or "")

    def test_placeholder_prices_degrade_open(self, adapter: CurveAdapter, pool_address: str) -> None:
        """A diverging price_ratio that is NOT a real oracle (placeholder /
        offline mode) must not fire the guard — known-fake prices are unmeasured."""
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("2"),  # would block if treated as a real oracle
            oracle_prices_real=False,  # ...but it is a placeholder
        )
        assert result.success is True

    def test_wide_override_tolerates_divergence(self, adapter: CurveAdapter, pool_address: str) -> None:
        """A wide per-intent override lets a large oracle gap through."""
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("2"),
            oracle_guard_bps=9000,  # tolerate up to 90% shortfall
        )
        assert result.success is True


class TestVolatilePoolSkipped:
    """The execution-rate-vs-oracle guard is StableSwap-only: on a volatile pool
    the get_dy vs oracle-mid gap legitimately includes real price impact, so the
    guard is skipped (else it false-blocks legit swaps — a 637 bps arb-tricrypto
    fill tripped CI). Volatile min-out protection is the slippage floor."""

    def test_volatile_pool_with_divergent_oracle_is_not_blocked(self, adapter: CurveAdapter) -> None:
        tricrypto_addr = CURVE_POOLS["ethereum"]["tricrypto2"]["address"]
        # A wildly divergent oracle ratio would block a StableSwap pool, but a
        # volatile pool must NOT be blocked by the execution-rate guard.
        result = adapter.swap(
            pool_address=tricrypto_addr,
            token_in="USDT",
            token_out="WETH",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("1") / Decimal("2500"),  # USDT $1 / WETH $2500
        )
        assert result.success is True, result.error
        assert result.transactions


class TestExecutedFloorOracleAnchor:
    """VIB-5490: the EXECUTED min_amount_out floor is anchored to the oracle so an
    atomic same-block sandwich is bounded by the oracle tolerance, not the (wide)
    operator slippage. Applies to BOTH stable and volatile pools; the floor is
    capped at ``pool_quote × (1 − residual)`` so a genuine >tolerance-impact swap
    keeps a benign inter-block-drift buffer and never false-reverts."""

    def test_stable_wide_slippage_floor_is_raised(self, adapter: CurveAdapter, pool_address: str) -> None:
        """A wide slippage on a stable pool would leave a loose floor; the oracle
        anchor raises amount_out_minimum toward oracle-fair."""
        wide = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("1"),
            slippage_bps=2000,  # 20% — a sandwich-exploitable loose floor
        )
        assert wide.success is True
        # With a 150 bps oracle tolerance the floor sits far above the 80%-of-quote
        # a 2000 bps slippage would have allowed. Compare against the same swap with
        # NO oracle (degrade-open) which keeps the loose pool-self-referential floor.
        loose = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=None,  # unmeasured → no clamp, keeps loose floor
            slippage_bps=2000,
        )
        assert loose.success is True
        assert wide.amount_out_minimum > loose.amount_out_minimum

    def test_swap_underlying_metapool_floor_is_raised(self, adapter: CurveAdapter, metapool_address: str) -> None:
        """The executed-floor anchor is wired into the metapool zap path too
        (``swap_underlying``), not just ``swap``: a wide slippage on a FRAX/3CRV
        metapool underlying swap would leave a loose floor; the oracle anchor
        raises ``amount_out_minimum`` toward the oracle floor and keeps a residual
        buffer below the quote. Without this test the metapool call-site wiring —
        an identical money-path security control — was entirely untested."""
        wide = adapter.swap_underlying(
            pool_address=metapool_address,
            token_in="FRAX",
            token_out="USDC",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("1"),
            slippage_bps=2000,  # 20% — a sandwich-exploitable loose floor
        )
        assert wide.success is True, wide.error
        # Compare against the same underlying swap with NO oracle (degrade-open),
        # which keeps the loose pool-self-referential floor.
        loose = adapter.swap_underlying(
            pool_address=metapool_address,
            token_in="FRAX",
            token_out="USDC",
            amount_in=Decimal("1000"),
            price_ratio=None,  # unmeasured → no clamp, keeps loose floor
            slippage_bps=2000,
        )
        assert loose.success is True, loose.error
        # Anchor engaged: min-out raised above the loose floor.
        assert wide.amount_out_minimum > loose.amount_out_minimum
        # ...but still strictly below the quote → a residual drift buffer remains
        # (never pinned to the raw quote), same guarantee as the swap() path.
        assert wide.amount_out_minimum < wide.amount_out_estimate

    def test_clamp_keeps_residual_buffer_below_quote(self, adapter: CurveAdapter, pool_address: str) -> None:
        """Revert-safety: the anchored floor stays strictly BELOW the pool's own
        quote — it is never pinned to the raw quote (which would leave zero drift
        buffer). The residual buffer is what lets a drifted pool still fill."""
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("1"),
            slippage_bps=2000,
        )
        assert result.success is True
        # Strictly less than the quote → a positive benign-drift buffer remains.
        assert result.amount_out_minimum < result.amount_out_estimate

    def test_volatile_pool_swap_not_reverted_by_clamp(self, adapter: CurveAdapter) -> None:
        """A volatile-pool swap still builds — the clamp is capped at
        ``pool_quote × (1 − residual)`` so a large-but-fair high-impact fill keeps a
        drift buffer and is never false-reverted (the 637 bps arb-tricrypto lesson,
        now as no-false-revert on the executed floor)."""
        tricrypto_addr = CURVE_POOLS["ethereum"]["tricrypto2"]["address"]
        result = adapter.swap(
            pool_address=tricrypto_addr,
            token_in="USDT",
            token_out="WETH",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("1") / Decimal("2500"),
        )
        assert result.success is True, result.error
        assert result.transactions
        assert result.amount_out_minimum < result.amount_out_estimate

    def test_volatile_tight_override_still_keeps_residual(self, adapter: CurveAdapter) -> None:
        """Finding #3: a TIGHT per-intent oracle_guard_bps override on a volatile
        pool must NOT force a revert — the residual is pool-type-fixed (not driven
        by the override), so the executed floor still keeps its drift buffer below
        the quote."""
        tricrypto_addr = CURVE_POOLS["ethereum"]["tricrypto2"]["address"]
        result = adapter.swap(
            pool_address=tricrypto_addr,
            token_in="USDT",
            token_out="WETH",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("1") / Decimal("2500"),
            oracle_guard_bps=10,  # a very tight anchor tolerance
        )
        assert result.success is True, result.error
        assert result.transactions
        # Residual buffer preserved despite the tight tolerance override.
        assert result.amount_out_minimum < result.amount_out_estimate

    def test_out_of_range_override_degrades_safely_not_silently(self, adapter: CurveAdapter, pool_address: str) -> None:
        """A fat-fingered oracle_guard_bps > 10_000 must NOT silently disable the
        anchor via a negative oracle floor. The clamp degrades open (config-invalid),
        so the executed floor stays at the pool-self-referential floor — never
        negative, never above the quote."""
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("1"),
            oracle_guard_bps=20_000,  # invalid: > 10_000 bps
            slippage_bps=2000,
        )
        assert result.success is True, result.error
        # Degrades to the pool floor (2000 bps below quote) — a sane, non-negative
        # floor, not a garbage value from a negative (_BPS - 20000) factor.
        assert result.amount_out_minimum > 0
        assert result.amount_out_minimum < result.amount_out_estimate
        expected_pool_floor = result.amount_out_estimate * (10_000 - 2000) // 10_000
        assert result.amount_out_minimum == expected_pool_floor

    def test_placeholder_oracle_keeps_pool_floor(self, adapter: CurveAdapter, pool_address: str) -> None:
        """A placeholder price must not fabricate a higher executed floor."""
        placeholder = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=Decimal("1"),
            oracle_prices_real=False,
            slippage_bps=2000,
        )
        no_oracle = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
            price_ratio=None,
            slippage_bps=2000,
        )
        assert placeholder.success is True
        assert no_oracle.success is True
        # Placeholder is treated as unmeasured → identical loose floor.
        assert placeholder.amount_out_minimum == no_oracle.amount_out_minimum


class TestResolveOracleGuardBps:
    """Per-intent oracle_guard_bps override resolution (Gemini review on #3069)."""

    @staticmethod
    def _resolve(value):
        from almanak.connectors.curve.compiler import _resolve_oracle_guard_bps

        return _resolve_oracle_guard_bps({"oracle_guard_bps": value} if value is not _MISSING else {})

    def test_valid_int_passes_through(self) -> None:
        assert self._resolve(300) == 300

    def test_missing_is_none(self) -> None:
        assert self._resolve(_MISSING) is None

    def test_bool_true_rejected_not_coerced_to_1(self) -> None:
        # int(True) == 1 would be a 1 bps threshold — must be rejected, not coerced.
        assert self._resolve(True) is None

    def test_bool_false_rejected(self) -> None:
        assert self._resolve(False) is None

    def test_non_positive_rejected(self) -> None:
        assert self._resolve(0) is None
        assert self._resolve(-5) is None

    def test_non_integer_rejected(self) -> None:
        assert self._resolve("abc") is None


_MISSING = object()
