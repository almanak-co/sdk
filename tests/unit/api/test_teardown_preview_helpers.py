"""Unit tests for the pure preview helpers in ``almanak.framework.api.teardown``.

Covers every branch of ``_generate_steps`` (per-position-type step emission and
ordering) and ``_generate_warnings`` (health-factor, emergency-mode, and
position-size warnings).
"""

from __future__ import annotations

from almanak.framework.api.teardown import _generate_steps, _generate_warnings

# ---------------------------------------------------------------------------
# _generate_steps
# ---------------------------------------------------------------------------


class TestGenerateSteps:
    def test_no_positions_still_swaps_to_usdc(self) -> None:
        assert _generate_steps([], mode="graceful") == ["Swap all tokens to USDC"]

    def test_all_position_types_emit_ordered_steps(self) -> None:
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

    def test_unknown_type_ignored(self) -> None:
        steps = _generate_steps([{"type": "TOKEN"}], mode="graceful")
        assert steps == ["Swap all tokens to USDC"]


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

    def test_large_position_warns_about_slippage(self) -> None:
        strategy = {"total_value_usd": 250_000, "health_factor": 2.0}

        warnings = _generate_warnings(strategy, mode="graceful")

        assert warnings == ["Large position value. Extra care will be taken to minimize slippage."]

    def test_all_warnings_stack(self) -> None:
        strategy = {"total_value_usd": 500_000, "health_factor": 1.1}

        warnings = _generate_warnings(strategy, mode="graceful")

        assert len(warnings) == 2
        assert "Low health factor" in warnings[0]
        assert "Large position value" in warnings[1]

    def test_missing_fields_default_safely(self) -> None:
        assert _generate_warnings({}, mode="graceful") == []
