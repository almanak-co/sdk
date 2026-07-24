"""Unit tests for ``SafetyGuard.validate_slippage_escalation``.

Covers every branch of the escalation ladder against the default
``TeardownConfig`` (approval threshold 3%, absolute cap 10%) and the
position-aware loss cap from ``calculate_max_acceptable_loss``:

- above the absolute cap -> blocked regardless of approval
- above the approval threshold without approval -> blocked
- above the approval threshold with approval -> allowed
- within threshold but above the position-aware cap without approval -> blocked
- position-aware cap bypassed with approval
- small escalation -> allowed
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.teardown.safety_guard import SafetyGuard


def _guard() -> SafetyGuard:
    return SafetyGuard()  # default TeardownConfig


class TestAbsoluteCap:
    def test_above_absolute_max_blocked_even_with_approval(self) -> None:
        result = _guard().validate_slippage_escalation(
            current_slippage=Decimal("0.02"),
            new_slippage=Decimal("0.15"),
            position_value=Decimal("10000"),
            has_approval=True,
        )

        assert not result
        assert result.check_name == "absolute_slippage_cap"
        assert "15.0%" in result.message
        assert "10.0%" in result.message
        assert result.details == {"requested": "0.15", "maximum": "0.10"}


class TestApprovalThreshold:
    def test_above_threshold_without_approval_blocked(self) -> None:
        result = _guard().validate_slippage_escalation(
            current_slippage=Decimal("0.02"),
            new_slippage=Decimal("0.05"),
            position_value=Decimal("10000"),
            has_approval=False,
        )

        assert not result
        assert result.check_name == "approval_required"
        assert "requires human approval" in result.message
        assert result.details == {"requested": "0.05", "threshold": "0.03"}

    def test_above_threshold_with_approval_allowed(self) -> None:
        result = _guard().validate_slippage_escalation(
            current_slippage=Decimal("0.02"),
            new_slippage=Decimal("0.05"),
            position_value=Decimal("10000"),
            has_approval=True,
        )

        assert result.passed
        assert result.check_name == "slippage_escalation"
        assert "approved" in result.message


class TestPositionAwareCap:
    def test_within_threshold_but_above_position_cap_blocked(self) -> None:
        # $100K position -> 2.5% max acceptable loss; 3% slippage is within
        # the approval threshold but exceeds the position-aware cap.
        result = _guard().validate_slippage_escalation(
            current_slippage=Decimal("0.02"),
            new_slippage=Decimal("0.03"),
            position_value=Decimal("100000"),
            has_approval=False,
        )

        assert not result
        assert result.check_name == "position_aware_cap"
        assert "$3000.00 > $2500.00" in result.message
        assert result.details["position_value"] == "100000"

    def test_position_cap_bypassed_with_approval(self) -> None:
        result = _guard().validate_slippage_escalation(
            current_slippage=Decimal("0.02"),
            new_slippage=Decimal("0.03"),
            position_value=Decimal("100000"),
            has_approval=True,
        )

        assert result.passed
        assert result.check_name == "slippage_escalation"


class TestHappyPath:
    def test_small_escalation_allowed_without_approval(self) -> None:
        # $10K position -> 3% cap; 2.5% is under every gate.
        result = _guard().validate_slippage_escalation(
            current_slippage=Decimal("0.01"),
            new_slippage=Decimal("0.025"),
            position_value=Decimal("10000"),
            has_approval=False,
        )

        assert result.passed
        assert bool(result) is True
        assert "2.5%" in result.message
