"""VIB-3823 — LP_OPEN compile-time M0 (zero-liquidity) pre-flight.

Pins the contract that a UniV3 LP_OPEN intent which would mint zero
liquidity on-chain (the ``M0`` revert in ``UniswapV3Pool.mint()``)
fails compilation with the typed
``LpOpenZeroLiquidityError`` message — instead of submitting a tx that
burns gas on revert.

Coverage:
    * No-slot0 fallback path uses the geometric range midpoint **only
      when both legs are non-zero**. Single-sided mints without a live
      sqrt-price are passed through (a single-token LP_OPEN can mint
      validly when the live price is outside the range; the midpoint
      always classifies as in-range, so checking there would falsely
      block the mint).
    * In-range with both legs positive → SUCCESS (regression guard).
    * Two-legged tight range that mints zero → FAILED with the stable
      error-prefix strategies match on.
    * Slot0-aware path still surfaces the same prefix when
      ``recompute_lp_amounts`` rounds to (0, 0).
    * Slot0 below range + only ``amount0`` supplied → SUCCESS
      (single-sided out-of-range mint is valid on-chain).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.uniswap_v3.compiler import UniswapV3Compiler
from almanak.framework.intents import LpOpenZeroLiquidityError
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import Intent

LP_ADAPTER_CLS = "almanak.connectors.uniswap_v3.adapter.UniswapV3LPAdapter"
VALIDATE_POOL = "almanak.framework.intents.pool_validation.validate_v3_pool"
FETCH_SQRT = "almanak.framework.intents.pool_validation.fetch_v3_pool_sqrt_price_x96"


_DEFAULT_PRICES: dict[str, Decimal] = {
    "WETH": Decimal("2000"),
    "STETH": Decimal("2000"),
    "USDC": Decimal("1"),
}


def _make_compiler(chain: str = "ethereum") -> IntentCompiler:
    return IntentCompiler(
        chain=chain,
        wallet_address="0x1111111111111111111111111111111111111111",
        config=IntentCompilerConfig(),
        price_oracle=_DEFAULT_PRICES,
    )


def _make_mock_adapter(
    *,
    position_manager: str = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    mint_calldata: bytes = b"\xa1\xb2",
) -> MagicMock:
    adapter = MagicMock(name="MockUniV3LPAdapter")
    adapter.get_position_manager_address.return_value = position_manager
    adapter.get_mint_calldata.return_value = mint_calldata
    adapter.estimate_mint_gas.return_value = 500_000
    return adapter


def _ok_pool_check(pool_address: str | None = None) -> MagicMock:
    """Pool validator returns "exists, ABI matches, no on-chain check skip"."""
    pool = MagicMock(name="PoolValidationResult")
    pool.exists = True
    pool.is_skipped = False
    pool.error = None
    pool.warning = None
    pool.pool_address = pool_address  # None forces the no-slot0 fallback branch
    return pool


@pytest.fixture
def lp_compiler():
    return _make_compiler()


@pytest.fixture
def v3_compiler() -> UniswapV3Compiler:
    return UniswapV3Compiler()


# ---------------------------------------------------------------------------
# No-slot0 fallback: pre-flight uses geometric range midpoint
# ---------------------------------------------------------------------------


class TestPreflightNoSlot0Fallback:
    """When no live pool sqrt-price is available, the pre-flight uses the
    geometric range midpoint and still catches zero-liquidity inputs."""

    @patch(VALIDATE_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_in_range_both_legs_positive_succeeds(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
        lp_compiler: IntentCompiler,
    ) -> None:
        """Healthy LP_OPEN: both legs positive, in-range — compile succeeds."""
        mock_adapter_cls.return_value = _make_mock_adapter()
        mock_validate.return_value = _ok_pool_check(pool_address=None)

        intent = Intent.lp_open(
            pool="WETH/USDC/3000",
            amount0=Decimal("1"),  # 1 WETH
            amount1=Decimal("2000"),  # 2000 USDC
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
            protocol="uniswap_v3",
        )
        result = lp_compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, result.error

    @patch(VALIDATE_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_single_sided_no_slot0_skips_preflight(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
        lp_compiler: IntentCompiler,
    ) -> None:
        """Single-sided LP_OPEN without slot0 must pass preflight (CR + Codex P2).

        Without a live sqrt-price we cannot tell whether the live pool
        sits inside or outside the requested range. UniswapV3 mints
        valid liquidity for a single-token deposit when the live price
        is outside the range, so the geometric-midpoint check (which
        is always in-range) would false-positive. The fix: skip the
        preflight when slot0 is unavailable AND the mint is one-sided.
        Compile proceeds through to bundle build.
        """
        mock_adapter_cls.return_value = _make_mock_adapter()
        mock_validate.return_value = _ok_pool_check(pool_address=None)

        intent = Intent.lp_open(
            pool="STETH/WETH/100",
            amount0=Decimal("1"),  # only token0
            amount1=Decimal("0"),
            range_lower=Decimal("0.995"),
            range_upper=Decimal("1.005"),
            protocol="uniswap_v3",
        )
        result = lp_compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, result.error


# ---------------------------------------------------------------------------
# Slot0-aware path: failed slot0 lookup still falls back to midpoint
# ---------------------------------------------------------------------------


class TestPreflightSlot0Aware:
    """When slot0 IS fetched, the live sqrt-price classifies the position."""

    @patch(FETCH_SQRT)
    @patch(VALIDATE_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_slot0_returns_zero_liquidity_falls_through(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
        mock_slot0: MagicMock,
        lp_compiler: IntentCompiler,
    ) -> None:
        """If recompute returns (0, 0), the existing FAILED path uses the typed prefix."""
        from almanak.framework.intents.lp_math import tick_to_sqrt_ratio_x96

        mock_adapter_cls.return_value = _make_mock_adapter()
        mock_validate.return_value = _ok_pool_check(pool_address="0xpoolpoolpoolpool")
        # Pool is far above the requested range AND the user supplied only
        # token0. recompute_lp_amounts will return (0, 0) — the slot0
        # connector compiler's slot0 recompute branch fails with the typed
        # prefix.
        mock_slot0.return_value = (tick_to_sqrt_ratio_x96(5000), 5000)

        intent = Intent.lp_open(
            pool="WETH/USDC/3000",
            amount0=Decimal("1"),  # only token0
            amount1=Decimal("0"),
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
            protocol="uniswap_v3",
        )
        result = lp_compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert result.error is not None
        assert result.error.startswith(LpOpenZeroLiquidityError.ERROR_PREFIX), result.error

    @patch(FETCH_SQRT)
    @patch(VALIDATE_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_slot0_in_range_both_legs_positive_succeeds(
        self,
        mock_adapter_cls: MagicMock,
        mock_validate: MagicMock,
        mock_slot0: MagicMock,
        lp_compiler: IntentCompiler,
    ) -> None:
        """Slot0-aware happy path: live pool in range, both legs positive."""
        from almanak.framework.intents.lp_math import tick_to_sqrt_ratio_x96

        mock_adapter_cls.return_value = _make_mock_adapter()
        mock_validate.return_value = _ok_pool_check(pool_address="0xpoolpoolpoolpool")
        # tick=0 sits inside the [-1000, 1000]-equivalent range USDC/WETH
        # produces here.
        mock_slot0.return_value = (tick_to_sqrt_ratio_x96(0), 0)

        intent = Intent.lp_open(
            pool="WETH/USDC/3000",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
            protocol="uniswap_v3",
        )
        result = lp_compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, result.error


# ---------------------------------------------------------------------------
# Direct unit-test of _preflight_lp_liquidity (no full compile pipeline)
# ---------------------------------------------------------------------------


class TestPreflightDirectInvocation:
    """Drive ``_preflight_lp_liquidity`` directly to pin its return shape."""

    def test_zero_amounts_returns_none(self, v3_compiler: UniswapV3Compiler) -> None:
        # Both inputs zero → caller is responsible for that error surface;
        # pre-flight stays out of the way.
        out = v3_compiler._preflight_lp_liquidity(
            tick_lower=-1000,
            tick_upper=1000,
            amount0_desired=0,
            amount1_desired=0,
            intent_id="test-zero-zero",
        )
        assert out is None

    def test_positive_in_range_returns_none(self, v3_compiler: UniswapV3Compiler) -> None:
        out = v3_compiler._preflight_lp_liquidity(
            tick_lower=-1000,
            tick_upper=1000,
            amount0_desired=10**18,
            amount1_desired=10**18,
            intent_id="test-healthy",
        )
        assert out is None

    def test_one_leg_no_slot0_returns_none(
        self, v3_compiler: UniswapV3Compiler
    ) -> None:
        # Single-sided mints WITHOUT slot0 must pass preflight (return
        # None). The midpoint is always in-range and would falsely
        # block legitimate single-token out-of-range mints. Reviewed
        # by CodeRabbit + Codex P2 + Claude pr-auditor (3/3 auditors).
        out = v3_compiler._preflight_lp_liquidity(
            tick_lower=-10,
            tick_upper=10,
            amount0_desired=1_000_000,
            amount1_desired=0,
            intent_id="test-one-leg-no-slot0",
        )
        assert out is None

    def test_two_leg_full_range_minuscule_amounts_returns_failed(
        self, v3_compiler: UniswapV3Compiler
    ) -> None:
        # Full-range LP at 1-wei amounts: liquidity per unit price
        # range is so small it truncates to 0 in both legs — the
        # canonical "amounts too small for the chosen range" zero-
        # liquidity revert the preflight protects against.
        out = v3_compiler._preflight_lp_liquidity(
            tick_lower=-887200,
            tick_upper=887200,
            amount0_desired=1,
            amount1_desired=1,
            intent_id="test-fullrange-two-leg",
        )
        assert out is not None
        assert out.status == CompilationStatus.FAILED
        assert out.intent_id == "test-fullrange-two-leg"
        assert out.error is not None
        assert out.error.startswith(LpOpenZeroLiquidityError.ERROR_PREFIX)

    def test_one_leg_with_slot0_below_range_returns_none(
        self, v3_compiler: UniswapV3Compiler
    ) -> None:
        # Slot0-aware single-sided success: live price below the range
        # AND only amount0 supplied. UniswapV3 mints valid liquidity
        # here. Preflight uses the live sqrt and must NOT block.
        # Regression guard for the VIB-3823 follow-up fix (pr-auditor #12).
        from almanak.framework.intents.lp_math import tick_to_sqrt_ratio_x96

        sqrt_below = tick_to_sqrt_ratio_x96(-1000)  # below [-10, 10]
        out = v3_compiler._preflight_lp_liquidity(
            tick_lower=-10,
            tick_upper=10,
            amount0_desired=1_000_000_000,
            amount1_desired=0,
            intent_id="test-one-leg-below-range",
            slot0=(sqrt_below, -1000),
        )
        assert out is None

    def test_degenerate_range_returns_none(self, v3_compiler: UniswapV3Compiler) -> None:
        # Same lower/upper -> midpoint helper returns 0 -> pre-flight no-ops
        # (the real degenerate-range error is raised earlier in
        # _compute_lp_ticks; pre-flight must not double-report).
        out = v3_compiler._preflight_lp_liquidity(
            tick_lower=100,
            tick_upper=100,
            amount0_desired=10**18,
            amount1_desired=10**18,
            intent_id="test-degenerate",
        )
        assert out is None
