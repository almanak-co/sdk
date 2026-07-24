"""Unit tests for the pure preview helpers in ``almanak.framework.api.teardown``.

Covers every branch of ``_generate_steps`` and ``_generate_warnings``, plus two
alignment invariants (drift found during CRAP-reduction round 3, PR #3401):

1. ``_generate_steps`` must describe EVERY ``PositionType`` in close-order
   priority — a preview that silently drops VAULT/STAKE/PREDICTION/CEX
   under-reports to the operator what teardown will actually do.
2. The "large position" warning must fire at the shared
   ``LARGE_POSITION_WARNING_THRESHOLD_USD`` — the same threshold
   ``TeardownManager._generate_warnings`` uses. The two preview surfaces
   describe the same teardown and may never disagree.
"""

from __future__ import annotations

from typing import Any

import pytest

from almanak.framework.api.teardown import (
    _STEP_DESCRIPTIONS,
    _generate_steps,
    _generate_warnings,
)
from almanak.framework.teardown.models import (
    LARGE_POSITION_WARNING_THRESHOLD_USD,
    PositionType,
)

LARGE_POSITION_WARNING = "Large position value. Extra care will be taken to minimize slippage."


def _position(ptype: PositionType, value_usd: float = 1000.0) -> dict[str, Any]:
    return {
        "type": ptype.value,
        "position_id": f"{ptype.value.lower()}-1",
        "chain": "arbitrum",
        "protocol": "test_protocol",
        "value_usd": value_usd,
    }


# ---------------------------------------------------------------------------
# _generate_steps
# ---------------------------------------------------------------------------


class TestGenerateSteps:
    def test_step_descriptions_cover_every_position_type(self) -> None:
        assert set(_STEP_DESCRIPTIONS) == set(PositionType), (
            "Every PositionType needs preview text in _STEP_DESCRIPTIONS — a "
            "type without an entry silently disappears from the close preview."
        )

    @pytest.mark.parametrize(
        "ptype",
        [t for t in PositionType if t is not PositionType.TOKEN],
        ids=lambda t: t.value,
    )
    def test_every_position_type_produces_a_step(self, ptype: PositionType) -> None:
        steps = _generate_steps([_position(ptype)], mode="graceful")
        assert steps == [
            _STEP_DESCRIPTIONS[ptype],
            _STEP_DESCRIPTIONS[PositionType.TOKEN],
        ]

    def test_steps_follow_close_order_priority(self) -> None:
        # Deliberately shuffled input: ordering must come from
        # PositionType.priority, not from input order.
        shuffled = [
            PositionType.CEX,
            PositionType.LP,
            PositionType.PERP,
            PositionType.VAULT,
            PositionType.STAKE,
            PositionType.BORROW,
            PositionType.PREDICTION,
            PositionType.SUPPLY,
        ]
        steps = _generate_steps([_position(t) for t in shuffled], mode="graceful")
        expected = [_STEP_DESCRIPTIONS[t] for t in sorted(shuffled, key=lambda t: t.priority)]
        expected.append(_STEP_DESCRIPTIONS[PositionType.TOKEN])
        assert steps == expected

    def test_core_defi_types_emit_golden_step_text(self) -> None:
        positions = [
            {"type": "LP"},
            {"type": "SUPPLY"},
            {"type": "BORROW"},
            {"type": "PERP"},
        ]

        steps = _generate_steps(positions, mode="graceful")

        # Fixed safety ordering: perps first, then repay, withdraw, LP, swap.
        assert steps == [
            "Close perpetual position(s)",
            "Repay borrowed amounts",
            "Withdraw supplied collateral",
            "Close LP position(s) and collect fees",
            "Swap all tokens to USDC",
        ]

    def test_only_matching_types_emit_steps(self) -> None:
        steps = _generate_steps([{"type": "LP"}], mode="emergency")
        assert steps == [
            "Close LP position(s) and collect fees",
            "Swap all tokens to USDC",
        ]

    def test_duplicate_types_emit_single_step(self) -> None:
        steps = _generate_steps([{"type": "PERP"}, {"type": "PERP"}], mode="graceful")
        assert steps == ["Close perpetual position(s)", "Swap all tokens to USDC"]

    def test_final_swap_step_always_last_and_exactly_once(self) -> None:
        # An explicit TOKEN position folds into the unconditional final
        # consolidation step — no duplicate.
        steps = _generate_steps(
            [_position(PositionType.TOKEN), _position(PositionType.LP)],
            mode="graceful",
        )
        assert steps == [
            _STEP_DESCRIPTIONS[PositionType.LP],
            _STEP_DESCRIPTIONS[PositionType.TOKEN],
        ]

    def test_no_positions_still_swaps_to_usdc(self) -> None:
        assert _generate_steps([], mode="graceful") == ["Swap all tokens to USDC"]


# ---------------------------------------------------------------------------
# _generate_warnings
# ---------------------------------------------------------------------------


class TestGenerateWarnings:
    def test_no_warnings_for_small_healthy_graceful(self) -> None:
        strategy = {"total_value_usd": 1000, "health_factor": 2.5}
        assert _generate_warnings(strategy, mode="graceful") == []

    def test_low_health_factor_warns(self) -> None:
        strategy = {"total_value_usd": 1000, "health_factor": 1.2}

        warnings = _generate_warnings(strategy, mode="graceful")

        assert len(warnings) == 1
        assert "Low health factor (1.2)" in warnings[0]
        assert "liquidation risk" in warnings[0]

    def test_emergency_without_health_factor_suggests_graceful(self) -> None:
        strategy = {"total_value_usd": 1000, "health_factor": None}

        warnings = _generate_warnings(strategy, mode="emergency")

        assert len(warnings) == 1
        assert "Emergency mode selected but no immediate liquidation risk" in warnings[0]
        assert "graceful mode" in warnings[0]

    def test_emergency_with_health_factor_does_not_suggest_graceful(self) -> None:
        strategy = {"total_value_usd": 1000, "health_factor": 3.0}
        assert _generate_warnings(strategy, mode="emergency") == []

    def test_large_position_warning_above_shared_threshold(self) -> None:
        strategy = {"total_value_usd": float(LARGE_POSITION_WARNING_THRESHOLD_USD) + 1.0}
        assert LARGE_POSITION_WARNING in _generate_warnings(strategy, mode="graceful")

    def test_no_large_position_warning_at_threshold(self) -> None:
        strategy = {"total_value_usd": float(LARGE_POSITION_WARNING_THRESHOLD_USD)}
        assert LARGE_POSITION_WARNING not in _generate_warnings(strategy, mode="graceful")

    def test_no_large_position_warning_at_legacy_api_threshold(self) -> None:
        # Pre-alignment the API warned above $100K while TeardownManager
        # warned above $500K. Values in between must NOT warn any more.
        for total_value in (150_000.0, 250_000.0):
            assert LARGE_POSITION_WARNING not in _generate_warnings({"total_value_usd": total_value}, mode="graceful")

    def test_all_warnings_stack(self) -> None:
        strategy = {
            "total_value_usd": float(LARGE_POSITION_WARNING_THRESHOLD_USD) + 1.0,
            "health_factor": 1.1,
        }

        warnings = _generate_warnings(strategy, mode="graceful")

        assert len(warnings) == 2
        assert "Low health factor" in warnings[0]
        assert "Large position value" in warnings[1]

    def test_missing_fields_default_safely(self) -> None:
        assert _generate_warnings({}, mode="graceful") == []
