"""ALM-10001 / ALM-3742: a Uniswap V3-family LP_CLOSE floors ``decreaseLiquidity``.

The compiler used to encode ``amount0Min = amount1Min = 0`` on every V3-shaped
close, so the chain accepted any withdrawal output. The burn floor is now sized
from the position's own liquidity at the pool's live ``sqrtPriceX96`` and haircut
by the BURN tolerance -- a loose liveness backstop (``LP_CLOSE_SLIPPAGE_DEFAULT``,
0.99), never the mint's price band -- per the VIB-6269 close-side decision.

Every assertion here decodes the emitted calldata; none reads a metadata field
as proof. The three V3 forks share one compiler, so the parametrisation is the
statement that the fix lands for all of them at once.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors._strategy_base.base.cl_math import LP_CLOSE_SLIPPAGE_DEFAULT
from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors._strategy_base.slippage import compute_min_amount_out
from almanak.connectors._strategy_base.v3_pool_validation import V3PositionBindingReadError, V3PositionState
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.lp_math import amounts_for_liquidity, tick_to_sqrt_ratio_x96
from almanak.framework.intents.vocabulary import Intent
from tests.intents._parameter_fidelity import TxOutcome, check_calldata

POOL_VALIDATION = "almanak.connectors.uniswap_v3.pool_validation"
V3_FORKS = ["uniswap_v3", "sushiswap_v3", "pancakeswap_v3"]

TICK_LOWER, TICK_UPPER = -2000, 2000
LIQUIDITY = 10**15
SQRT_MID = 2**96  # tick 0, inside the range
SQRT_ABOVE = tick_to_sqrt_ratio_x96(TICK_UPPER + 500)
SQRT_BELOW = tick_to_sqrt_ratio_x96(TICK_LOWER - 500)
POOL = "0x" + "cc" * 20


def _state(liquidity: int = LIQUIDITY) -> V3PositionState:
    return V3PositionState(
        token0="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        token1="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        fee_tier=500,
        tick_lower=TICK_LOWER,
        tick_upper=TICK_UPPER,
        liquidity=liquidity,
    )


def _compile_close(
    protocol: str,
    *,
    liquidity: int = LIQUIDITY,
    sqrt_price_x96: int | None = SQRT_MID,
    max_slippage: Decimal | None = None,
    protocol_params: dict | None = None,
    state: V3PositionState | Exception | None = "default",  # type: ignore[assignment]
    pool_exists: bool = True,
    permission_discovery: bool = False,
):
    compiler = IntentCompiler(
        chain="arbitrum",
        wallet_address="0x1111111111111111111111111111111111111111",
        config=IntentCompilerConfig(permission_discovery=permission_discovery),
        price_oracle={"WETH": Decimal("2000"), "USDC": Decimal("1")},
    )
    intent = Intent.lp_close(
        position_id="777",
        pool="WETH/USDC/500",
        protocol=protocol,
        max_slippage=max_slippage,
        protocol_params=protocol_params,
    )
    if state == "default":
        state = _state(liquidity)
    state_reader = patch(
        f"{POOL_VALIDATION}.read_v3_position_state",
        side_effect=state if isinstance(state, Exception) else None,
        return_value=None if isinstance(state, Exception) else state,
    )
    pool_result = (
        PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=POOL)
        if pool_exists
        else PoolValidationResult(exists=False, reason=PoolValidationReason.NOT_FOUND, error="no pool")
    )
    with (
        patch.object(compiler, "_query_position_liquidity", return_value=liquidity),
        patch.object(compiler, "_query_position_tokens_owed", return_value=(5, 5)),
        state_reader as read_state,
        patch(f"{POOL_VALIDATION}.validate_v3_pool", return_value=pool_result),
        patch(
            f"{POOL_VALIDATION}.fetch_v3_pool_sqrt_price_x96",
            return_value=None if sqrt_price_x96 is None else (sqrt_price_x96, 0),
        ) as read_slot0,
    ):
        result = compiler.compile(intent)
    return result, read_state, read_slot0


def _decoded_decrease(result) -> tuple[dict[str, int], str, str]:
    decrease = next(tx for tx in result.transactions if tx.tx_type == "lp_decrease_liquidity")
    body = bytes.fromhex(decrease.data[2:])[4:]
    words = [int.from_bytes(body[i * 32 : (i + 1) * 32], "big") for i in range(5)]
    decoded = dict(zip(("token_id", "liquidity", "amount0_min", "amount1_min", "deadline"), words, strict=True))
    return decoded, decrease.to, decrease.data


class TestFloorsReachTheCalldata:
    @pytest.mark.parametrize("protocol", V3_FORKS)
    def test_default_tolerance_floors_both_legs_at_one_percent_of_expected(self, protocol: str) -> None:
        result, _, _ = _compile_close(protocol)

        assert result.status is CompilationStatus.SUCCESS, result.error
        decoded, to, data = _decoded_decrease(result)
        expected0, expected1 = amounts_for_liquidity(SQRT_MID, TICK_LOWER, TICK_UPPER, LIQUIDITY)
        assert decoded["liquidity"] == LIQUIDITY
        assert decoded["amount0_min"] == compute_min_amount_out(expected0, LP_CLOSE_SLIPPAGE_DEFAULT) > 0
        assert decoded["amount1_min"] == compute_min_amount_out(expected1, LP_CLOSE_SLIPPAGE_DEFAULT) > 0
        # The QA Lab proof node's exact verdict: decoded floors bind.
        assert check_calldata(to, data).outcome is TxOutcome.PROTECTED
        metadata = result.action_bundle.metadata
        assert metadata["lp_slippage"] == "0.99"
        assert metadata["lp_slippage_declared"] is False
        assert metadata["amount0_min"] == str(decoded["amount0_min"])
        assert metadata["amount1_min"] == str(decoded["amount1_min"])
        assert metadata["pool_address"] == POOL
        assert (metadata["tick_lower"], metadata["tick_upper"]) == (TICK_LOWER, TICK_UPPER)

    def test_declared_max_slippage_is_honoured_per_leg(self) -> None:
        result, _, _ = _compile_close("uniswap_v3", max_slippage=Decimal("0.005"))

        assert result.status is CompilationStatus.SUCCESS, result.error
        decoded, _, _ = _decoded_decrease(result)
        expected0, expected1 = amounts_for_liquidity(SQRT_MID, TICK_LOWER, TICK_UPPER, LIQUIDITY)
        assert decoded["amount0_min"] == compute_min_amount_out(expected0, Decimal("0.005"))
        assert decoded["amount1_min"] == compute_min_amount_out(expected1, Decimal("0.005"))
        assert result.action_bundle.metadata["lp_slippage_declared"] is True

    def test_protocol_params_lp_slippage_outranks_max_slippage(self) -> None:
        result, _, _ = _compile_close(
            "uniswap_v3", max_slippage=Decimal("0.005"), protocol_params={"lp_slippage": "0.5"}
        )

        assert result.status is CompilationStatus.SUCCESS, result.error
        decoded, _, _ = _decoded_decrease(result)
        expected0, _ = amounts_for_liquidity(SQRT_MID, TICK_LOWER, TICK_UPPER, LIQUIDITY)
        assert decoded["amount0_min"] == compute_min_amount_out(expected0, Decimal("0.5"))
        assert result.action_bundle.metadata["lp_slippage"] == "0.5"

    @pytest.mark.parametrize(
        ("sqrt_price_x96", "empty_leg", "held_leg"),
        [(SQRT_ABOVE, "amount0_min", "amount1_min"), (SQRT_BELOW, "amount1_min", "amount0_min")],
        ids=("above-range", "below-range"),
    )
    def test_out_of_range_position_floors_only_the_leg_it_holds(
        self, sqrt_price_x96: int, empty_leg: str, held_leg: str
    ) -> None:
        """A single-asset redemption legitimately has one zero floor; the other still binds."""
        result, _, _ = _compile_close("uniswap_v3", sqrt_price_x96=sqrt_price_x96)

        assert result.status is CompilationStatus.SUCCESS, result.error
        decoded, to, data = _decoded_decrease(result)
        assert decoded[empty_leg] == 0
        assert decoded[held_leg] > 0
        assert check_calldata(to, data).outcome is TxOutcome.PROTECTED
        assert any("out of range" in warning for warning in result.warnings)


class TestRefusalsAndFailClosed:
    def test_dust_position_whose_floors_both_truncate_to_zero_is_a_safety_refusal(self) -> None:
        result, _, _ = _compile_close("uniswap_v3", liquidity=10)

        assert result.status is CompilationStatus.FAILED
        assert result.is_safety_refusal is True
        assert result.is_transient is False
        assert result.transactions == []
        assert "minimum output must be > 0" in (result.error or "")

    def test_tolerance_at_or_above_one_is_a_safety_refusal(self) -> None:
        result, _, _ = _compile_close("uniswap_v3", protocol_params={"lp_slippage": "1"})

        assert result.status is CompilationStatus.FAILED
        assert result.is_safety_refusal is True
        assert result.transactions == []

    def test_unreadable_position_state_fails_closed_as_transient(self) -> None:
        result, _, _ = _compile_close("uniswap_v3", state=V3PositionBindingReadError("rpc down"))

        assert result.status is CompilationStatus.FAILED
        assert result.is_transient is True
        assert result.is_safety_refusal is False
        assert result.transactions == []
        assert "unfloored decreaseLiquidity" in (result.error or "")

    def test_malformed_position_state_fails_closed_as_transient(self) -> None:
        result, _, _ = _compile_close("uniswap_v3", state=None)

        assert result.status is CompilationStatus.FAILED
        assert result.is_transient is True
        assert result.transactions == []

    def test_unresolvable_pool_fails_closed_as_transient(self) -> None:
        result, _, _ = _compile_close("uniswap_v3", pool_exists=False)

        assert result.status is CompilationStatus.FAILED
        assert result.is_transient is True
        assert result.transactions == []

    def test_unreadable_slot0_fails_closed_as_transient(self) -> None:
        result, _, _ = _compile_close("uniswap_v3", sqrt_price_x96=None)

        assert result.status is CompilationStatus.FAILED
        assert result.is_transient is True
        assert result.transactions == []


class TestPathsThatNeedNoFloor:
    def test_zero_liquidity_position_performs_no_pool_reads(self) -> None:
        result, read_state, read_slot0 = _compile_close("uniswap_v3", liquidity=0)

        assert result.status is CompilationStatus.SUCCESS, result.error
        assert [tx.tx_type for tx in result.transactions] == ["lp_collect", "lp_burn"]
        read_state.assert_not_called()
        read_slot0.assert_not_called()
        assert "amount0_min" not in result.action_bundle.metadata

    def test_permission_discovery_compiles_selectors_without_pool_reads(self) -> None:
        result, read_state, read_slot0 = _compile_close("uniswap_v3", permission_discovery=True)

        assert result.status is CompilationStatus.SUCCESS, result.error
        read_state.assert_not_called()
        read_slot0.assert_not_called()
        decoded, _, _ = _decoded_decrease(result)
        assert (decoded["amount0_min"], decoded["amount1_min"]) == (0, 0)
        assert "amount0_min" not in result.action_bundle.metadata
