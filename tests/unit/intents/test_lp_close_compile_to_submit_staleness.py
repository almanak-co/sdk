"""VIB-6269 Q4, burn side: price movement injected BETWEEN LP_CLOSE compile and submit.

The close-side decision requires this test and states that the mint-side one does
not cover it: a burn's revert geometry is different. ``decreaseLiquidity`` credits
``amounts_for_liquidity(price_at_execution)`` and reverts iff either leg is below
its floor. The default 0.99 tolerance survives ordinary in-range movement, but
can still revert close to a range edge as an expected leg approaches its 1% floor.
Crossing the range edge guarantees that result because one leg vanishes. The
constant barely moves the range-width-driven threshold.

A DECLARED tight tolerance is honoured and therefore carries the other cost: on an
in-range position the leg amounts are an amplified function of price, so a 0.5%
per-leg floor reverts on a 0.1% move. Both behaviours are pinned here so neither
can drift silently.

Every check evaluates the EMITTED floors against the contract math at the
execution price, so a green run means the burn would not have reverted.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors._strategy_base.base.cl_math import LP_CLOSE_SLIPPAGE_DEFAULT, compute_lp_close_mins
from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors._strategy_base.v3_pool_validation import V3PositionState
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.lp_math import amounts_for_liquidity, tick_to_sqrt_ratio_x96
from almanak.framework.intents.vocabulary import Intent

POOL_VALIDATION = "almanak.connectors.uniswap_v3.pool_validation"
LIQUIDITY = 10**15

# The 2026-08-03 Arbitrum WETH/USDC 0.05% position and the archive slot0 reads that
# bracket its mint incident (see test_lp_open_compile_to_submit_staleness.py).
INCIDENT_TICK_LOWER = -202100
INCIDENT_TICK_UPPER = -200090
INCIDENT_SQRT_COMPILE = 3416797190762740332110120
INCIDENT_SQRT_EXEC = 3416324775937034248257536


class _CloseIntent:
    def __init__(self, tolerance: Decimal | None) -> None:
        self.protocol_params = None
        self.max_slippage = tolerance


def _sqrt_after_price_move(sqrt_price_x96: int, fraction: Decimal) -> int:
    return int(Decimal(sqrt_price_x96) * (Decimal(1) + fraction).sqrt())


def _burn_reverts(
    *, mins: tuple[int, int], sqrt_exec: int, tick_lower: int, tick_upper: int, liquidity: int = LIQUIDITY
) -> bool:
    """NonfungiblePositionManager.decreaseLiquidity: ``require(amount >= amountMin)`` per leg."""
    amount0, amount1 = amounts_for_liquidity(sqrt_exec, tick_lower, tick_upper, liquidity)
    return amount0 < mins[0] or amount1 < mins[1]


def _floors(tolerance: Decimal | None, sqrt_compile: int, tick_lower: int, tick_upper: int) -> tuple[int, int]:
    mins = compute_lp_close_mins(
        intent=_CloseIntent(tolerance),
        sqrt_price_x96=sqrt_compile,
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        liquidity=LIQUIDITY,
    )
    assert max(mins.amount0_min, mins.amount1_min) > 0
    return mins.amount0_min, mins.amount1_min


def _symmetric_range(half_width: Decimal) -> tuple[int, int]:
    """Ticks whose price bounds are (1 - h, 1 + h) around tick 0."""
    import math

    upper = int(math.log(1 + float(half_width)) / math.log(1.0001))
    lower = int(math.log(1 - float(half_width)) / math.log(1.0001))
    return lower, upper


class TestDefaultToleranceIsALivenessBackstop:
    @pytest.mark.parametrize("move", ["-0.019", "-0.01", "-0.001", "0.001", "0.01", "0.019"])
    def test_default_floors_survive_ordinary_in_range_move(self, move: str) -> None:
        tick_lower, tick_upper = _symmetric_range(Decimal("0.02"))
        mins = _floors(None, 2**96, tick_lower, tick_upper)
        assert not _burn_reverts(
            mins=mins,
            sqrt_exec=_sqrt_after_price_move(2**96, Decimal(move)),
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )

    @pytest.mark.parametrize("half_width", ["0.01", "0.02", "0.05", "0.10", "0.25"])
    def test_default_floors_revert_after_price_exits_the_range(self, half_width: str) -> None:
        """Crossing an edge empties one leg below every positive close floor."""
        h = Decimal(half_width)
        tick_lower, tick_upper = _symmetric_range(h)
        mins = _floors(None, 2**96, tick_lower, tick_upper)
        inside = _sqrt_after_price_move(2**96, h * Decimal("0.98"))
        outside = _sqrt_after_price_move(2**96, h * Decimal("1.02"))
        assert not _burn_reverts(mins=mins, sqrt_exec=inside, tick_lower=tick_lower, tick_upper=tick_upper)
        assert _burn_reverts(mins=mins, sqrt_exec=outside, tick_lower=tick_lower, tick_upper=tick_upper)

    def test_default_floors_can_revert_while_price_is_still_in_range(self) -> None:
        """The loose default is a backstop, not an all-in-range liveness guarantee."""
        tick_lower, tick_upper = -202, 198
        mins = _floors(None, 2**96, tick_lower, tick_upper)
        near_upper = _sqrt_after_price_move(2**96, Decimal("0.0199"))
        assert near_upper < tick_to_sqrt_ratio_x96(tick_upper)
        assert _burn_reverts(mins=mins, sqrt_exec=near_upper, tick_lower=tick_lower, tick_upper=tick_upper)

    def test_incident_position_default_floors_survive_the_three_tick_move(self) -> None:
        mins = _floors(None, INCIDENT_SQRT_COMPILE, INCIDENT_TICK_LOWER, INCIDENT_TICK_UPPER)
        assert not _burn_reverts(
            mins=mins, sqrt_exec=INCIDENT_SQRT_EXEC, tick_lower=INCIDENT_TICK_LOWER, tick_upper=INCIDENT_TICK_UPPER
        )
        assert LP_CLOSE_SLIPPAGE_DEFAULT == Decimal("0.99")


class TestDeclaredTightToleranceIsHonouredAndCostly:
    def test_half_percent_floor_survives_the_incident_move_but_not_ordinary_drift(self) -> None:
        """Amplification on the burn: 0.5% per leg tolerates ~0.05% of price, not 0.5%."""
        mins = _floors(Decimal("0.005"), INCIDENT_SQRT_COMPILE, INCIDENT_TICK_LOWER, INCIDENT_TICK_UPPER)
        kwargs = {"mins": mins, "tick_lower": INCIDENT_TICK_LOWER, "tick_upper": INCIDENT_TICK_UPPER}
        assert not _burn_reverts(sqrt_exec=INCIDENT_SQRT_EXEC, **kwargs)  # -0.0277%
        assert not _burn_reverts(sqrt_exec=_sqrt_after_price_move(INCIDENT_SQRT_COMPILE, Decimal("-0.0005")), **kwargs)
        assert _burn_reverts(sqrt_exec=_sqrt_after_price_move(INCIDENT_SQRT_COMPILE, Decimal("-0.001")), **kwargs)
        assert _burn_reverts(sqrt_exec=_sqrt_after_price_move(INCIDENT_SQRT_COMPILE, Decimal("0.001")), **kwargs)


class TestThroughTheCompiler:
    def _compile(self, sqrt_compile: int, tick_lower: int, tick_upper: int, tolerance: Decimal | None):
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x1111111111111111111111111111111111111111",
            config=IntentCompilerConfig(),
            price_oracle={"WETH": Decimal("2000"), "USDC": Decimal("1")},
        )
        state = V3PositionState(
            token0="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            token1="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            fee_tier=500,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=LIQUIDITY,
        )
        with (
            patch.object(compiler, "_query_position_liquidity", return_value=LIQUIDITY),
            patch.object(compiler, "_query_position_tokens_owed", return_value=(0, 0)),
            patch(f"{POOL_VALIDATION}.read_v3_position_state", return_value=state),
            patch(
                f"{POOL_VALIDATION}.validate_v3_pool",
                return_value=PoolValidationResult(
                    exists=True, reason=PoolValidationReason.CONFIRMED, pool_address="0x" + "cc" * 20
                ),
            ),
            patch(f"{POOL_VALIDATION}.fetch_v3_pool_sqrt_price_x96", return_value=(sqrt_compile, 0)),
        ):
            result = compiler.compile(
                Intent.lp_close(position_id="7", pool="WETH/USDC/500", protocol="uniswap_v3", max_slippage=tolerance)
            )
        assert result.status is CompilationStatus.SUCCESS, result.error
        decrease = next(tx for tx in result.transactions if tx.tx_type == "lp_decrease_liquidity")
        body = bytes.fromhex(decrease.data[2:])[4:]
        return int.from_bytes(body[64:96], "big"), int.from_bytes(body[96:128], "big")

    def test_emitted_default_floors_bound_the_burn_for_ordinary_price_movement(self) -> None:
        mins = self._compile(INCIDENT_SQRT_COMPILE, INCIDENT_TICK_LOWER, INCIDENT_TICK_UPPER, None)
        assert mins[0] > 0 and mins[1] > 0
        kwargs = {"mins": mins, "tick_lower": INCIDENT_TICK_LOWER, "tick_upper": INCIDENT_TICK_UPPER}
        assert not _burn_reverts(sqrt_exec=INCIDENT_SQRT_EXEC, **kwargs)
        assert not _burn_reverts(sqrt_exec=_sqrt_after_price_move(INCIDENT_SQRT_COMPILE, Decimal("0.05")), **kwargs)
        assert _burn_reverts(sqrt_exec=tick_to_sqrt_ratio_x96(INCIDENT_TICK_UPPER + 1), **kwargs)
        assert _burn_reverts(sqrt_exec=tick_to_sqrt_ratio_x96(INCIDENT_TICK_LOWER - 1), **kwargs)

    def test_emitted_declared_floors_match_the_helper_the_burn_math_was_checked_against(self) -> None:
        emitted = self._compile(INCIDENT_SQRT_COMPILE, INCIDENT_TICK_LOWER, INCIDENT_TICK_UPPER, Decimal("0.005"))
        assert emitted == _floors(Decimal("0.005"), INCIDENT_SQRT_COMPILE, INCIDENT_TICK_LOWER, INCIDENT_TICK_UPPER)
