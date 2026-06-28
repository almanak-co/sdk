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
