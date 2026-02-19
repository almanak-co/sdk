"""Safety Guard for the Strategy Teardown System.

Enforces all safety invariants during teardown operations.
The Safety Contract: Every teardown operation enforces these invariants:

1. Position-Aware Loss Cap - Scales with position size
2. MEV Protection - Private mempool for all swaps
3. Cancel Window - 10 seconds for all modes
4. Simulation Required - Fast-path sim for emergency, never skip
5. Atomic Bundling - MultiSend for Safe wallets
6. Post-Verify - Query on-chain to confirm closure
7. Resumable State - Survive restarts, continue from checkpoint

The Safety Promise: "No action in the system can result in catastrophic loss."
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    calculate_max_acceptable_loss,
)

logger = logging.getLogger(__name__)


@dataclass
class SafetyCheckResult:
    """Result of a safety check."""

    passed: bool
    check_name: str
    message: str
    details: dict[str, Any] | None = None

    def __bool__(self) -> bool:
        return self.passed


@dataclass
class SafetyValidation:
    """Complete safety validation result."""

    all_passed: bool
    checks: list[SafetyCheckResult]
    blocked_reason: str | None = None

    @property
    def failed_checks(self) -> list[SafetyCheckResult]:
        """Get list of failed checks."""
        return [c for c in self.checks if not c.passed]


class SafetyGuard:
    """Enforces all safety invariants for teardown operations.

    This is the central safety enforcement layer. All teardown operations
    pass through the SafetyGuard, which validates:

    - Loss caps are respected
    - Slippage is within limits
    - Simulations pass before execution
    - Post-execution verification succeeds

    No bypass path exists - all operations must pass safety checks.
    """

    def __init__(self, config: TeardownConfig | None = None):
        """Initialize the SafetyGuard.

        Args:
            config: Teardown configuration. Uses defaults if not provided.
        """
        self.config = config or TeardownConfig.default()

    def validate_teardown_request(
        self,
        positions: TeardownPositionSummary,
        mode: TeardownMode,
        requested_slippage: Decimal | None = None,
    ) -> SafetyValidation:
        """Validate a teardown request before execution.

        Performs all pre-execution safety checks:
        - Loss cap validation
        - Slippage validation
        - Position state validation

        Args:
            positions: Summary of positions to teardown
            mode: Teardown mode (SOFT/HARD)
            requested_slippage: Optional specific slippage to validate

        Returns:
            SafetyValidation with results of all checks
        """
        checks: list[SafetyCheckResult] = []

        # Check 1: Validate positions exist
        checks.append(self._check_positions_exist(positions))

        # Check 2: Validate loss cap
        checks.append(self._check_loss_cap(positions))

        # Check 3: Validate slippage if specified
        if requested_slippage is not None:
            checks.append(self._check_slippage(requested_slippage, positions))

        # Check 4: Validate mode-specific requirements
        checks.append(self._check_mode_requirements(mode, positions))

        # Determine overall result
        all_passed = all(c.passed for c in checks)
        blocked_reason = None
        if not all_passed:
            failed = [c for c in checks if not c.passed]
            blocked_reason = f"Safety check failed: {failed[0].check_name} - {failed[0].message}"

        return SafetyValidation(
            all_passed=all_passed,
            checks=checks,
            blocked_reason=blocked_reason,
        )

    def validate_slippage_escalation(
        self,
        current_slippage: Decimal,
        new_slippage: Decimal,
        position_value: Decimal,
        has_approval: bool = False,
    ) -> SafetyCheckResult:
        """Validate a slippage escalation request.

        Args:
            current_slippage: Current slippage level
            new_slippage: Requested new slippage level
            position_value: Total position value
            has_approval: Whether human approval was given

        Returns:
            SafetyCheckResult indicating if escalation is allowed
        """
        # Check absolute maximum
        if new_slippage > self.config.absolute_max_slippage:
            return SafetyCheckResult(
                passed=False,
                check_name="absolute_slippage_cap",
                message=f"Requested slippage {new_slippage:.1%} exceeds absolute maximum {self.config.absolute_max_slippage:.1%}",
                details={
                    "requested": str(new_slippage),
                    "maximum": str(self.config.absolute_max_slippage),
                },
            )

        # Check if approval is required but not given
        if new_slippage > self.config.manual_approval_threshold and not has_approval:
            return SafetyCheckResult(
                passed=False,
                check_name="approval_required",
                message=f"Slippage {new_slippage:.1%} requires human approval",
                details={
                    "requested": str(new_slippage),
                    "threshold": str(self.config.manual_approval_threshold),
                },
            )

        # Check position-aware loss cap
        max_loss_percent = calculate_max_acceptable_loss(position_value)
        max_acceptable_loss = position_value * max_loss_percent

        if new_slippage > max_loss_percent and not has_approval:
            estimated_loss = position_value * new_slippage
            return SafetyCheckResult(
                passed=False,
                check_name="position_aware_cap",
                message=f"Slippage would exceed position-aware cap (${estimated_loss:.2f} > ${max_acceptable_loss:.2f})",
                details={
                    "estimated_loss": str(estimated_loss),
                    "max_acceptable": str(max_acceptable_loss),
                    "position_value": str(position_value),
                },
            )

        return SafetyCheckResult(
            passed=True,
            check_name="slippage_escalation",
            message=f"Slippage escalation to {new_slippage:.1%} approved",
        )

    def validate_execution_result(
        self,
        starting_value: Decimal,
        final_value: Decimal,
        position_value: Decimal,
    ) -> SafetyCheckResult:
        """Validate the result of a teardown execution.

        Checks that the actual loss is within acceptable limits.

        Args:
            starting_value: Value before execution
            final_value: Value after execution
            position_value: Original position value

        Returns:
            SafetyCheckResult indicating if result is acceptable
        """
        actual_loss = starting_value - final_value
        actual_loss_percent = actual_loss / starting_value if starting_value > 0 else Decimal("0")

        max_loss_percent = calculate_max_acceptable_loss(position_value)
        max_acceptable_loss = position_value * max_loss_percent

        # Allow a small buffer for rounding
        buffer = Decimal("1.01")  # 1% buffer

        if actual_loss > max_acceptable_loss * buffer:
            return SafetyCheckResult(
                passed=False,
                check_name="post_execution_loss",
                message=f"Actual loss ${actual_loss:.2f} ({actual_loss_percent:.1%}) exceeded cap ${max_acceptable_loss:.2f}",
                details={
                    "actual_loss": str(actual_loss),
                    "actual_loss_percent": str(actual_loss_percent),
                    "max_acceptable": str(max_acceptable_loss),
                    "starting_value": str(starting_value),
                    "final_value": str(final_value),
                },
            )

        return SafetyCheckResult(
            passed=True,
            check_name="post_execution_loss",
            message=f"Execution completed within loss limits (${actual_loss:.2f}, {actual_loss_percent:.1%})",
        )

    def calculate_protected_minimum(self, position_value: Decimal) -> Decimal:
        """Calculate the protected minimum value for a position.

        This is the minimum amount the user is guaranteed to receive
        (barring extreme market conditions requiring approval).

        Args:
            position_value: Total position value

        Returns:
            Protected minimum value in USD
        """
        max_loss_percent = calculate_max_acceptable_loss(position_value)
        return position_value * (Decimal("1") - max_loss_percent)

    def calculate_estimated_return_range(
        self,
        position_value: Decimal,
        mode: TeardownMode,
    ) -> tuple[Decimal, Decimal]:
        """Calculate estimated return range for a teardown.

        Args:
            position_value: Total position value
            mode: Teardown mode

        Returns:
            Tuple of (min_return, max_return)
        """
        max_loss_percent = calculate_max_acceptable_loss(position_value)

        if mode == TeardownMode.SOFT:
            # Graceful: expect lower costs
            min_cost = Decimal("0.003")  # 0.3% gas + minimal slippage
            max_cost = max_loss_percent * Decimal("0.5")  # Half of max
        else:
            # Emergency: expect higher costs
            min_cost = Decimal("0.01")  # 1% expected
            max_cost = max_loss_percent

        min_return = position_value * (Decimal("1") - max_cost)
        max_return = position_value * (Decimal("1") - min_cost)

        return (min_return, max_return)

    def _check_positions_exist(self, positions: TeardownPositionSummary) -> SafetyCheckResult:
        """Check that positions exist for teardown."""
        if not positions.positions:
            return SafetyCheckResult(
                passed=True,  # Empty is OK - nothing to do
                check_name="positions_exist",
                message="No positions to teardown",
            )

        return SafetyCheckResult(
            passed=True,
            check_name="positions_exist",
            message=f"Found {len(positions.positions)} positions totaling ${positions.total_value_usd:,.2f}",
        )

    def _check_loss_cap(self, positions: TeardownPositionSummary) -> SafetyCheckResult:
        """Check that loss cap configuration is valid."""
        max_loss = calculate_max_acceptable_loss(positions.total_value_usd)
        protected_min = positions.total_value_usd * (Decimal("1") - max_loss)

        # If custom cap is set, validate it
        if self.config.custom_max_loss_percent is not None:
            if self.config.custom_max_loss_percent > max_loss:
                return SafetyCheckResult(
                    passed=False,
                    check_name="loss_cap",
                    message=f"Custom loss cap {self.config.custom_max_loss_percent:.1%} exceeds position-aware cap {max_loss:.1%}",
                )

        return SafetyCheckResult(
            passed=True,
            check_name="loss_cap",
            message=f"Protected minimum: ${protected_min:,.2f} (max loss: {max_loss:.1%})",
            details={
                "max_loss_percent": str(max_loss),
                "protected_minimum": str(protected_min),
                "total_value": str(positions.total_value_usd),
            },
        )

    def _check_slippage(
        self,
        slippage: Decimal,
        positions: TeardownPositionSummary,
    ) -> SafetyCheckResult:
        """Check that requested slippage is within limits."""
        if slippage > self.config.absolute_max_slippage:
            return SafetyCheckResult(
                passed=False,
                check_name="slippage",
                message=f"Slippage {slippage:.1%} exceeds absolute maximum {self.config.absolute_max_slippage:.1%}",
            )

        max_loss = calculate_max_acceptable_loss(positions.total_value_usd)
        if slippage > max_loss:
            return SafetyCheckResult(
                passed=False,
                check_name="slippage",
                message=f"Slippage {slippage:.1%} exceeds position-aware cap {max_loss:.1%}",
                details={
                    "requires_approval": True,
                    "position_aware_cap": str(max_loss),
                },
            )

        return SafetyCheckResult(
            passed=True,
            check_name="slippage",
            message=f"Slippage {slippage:.1%} is within limits",
        )

    def _check_mode_requirements(
        self,
        mode: TeardownMode,
        positions: TeardownPositionSummary,
    ) -> SafetyCheckResult:
        """Check mode-specific requirements."""
        if mode == TeardownMode.HARD:
            # Emergency mode: warn if no liquidation risk
            if not positions.has_liquidation_risk:
                return SafetyCheckResult(
                    passed=True,  # Still allowed, just a warning
                    check_name="mode_requirements",
                    message="Emergency mode selected but no liquidation risk detected. Consider graceful mode for lower costs.",
                    details={"warning": True},
                )

        return SafetyCheckResult(
            passed=True,
            check_name="mode_requirements",
            message=f"Mode {mode.value} is appropriate",
        )

    def get_safety_summary(self, positions: TeardownPositionSummary) -> dict[str, Any]:
        """Get a summary of safety protections for display.

        Args:
            positions: Position summary

        Returns:
            Dictionary with safety information for UX display
        """
        max_loss_percent = calculate_max_acceptable_loss(positions.total_value_usd)
        max_loss_usd = positions.total_value_usd * max_loss_percent
        protected_min = positions.total_value_usd - max_loss_usd

        return {
            "current_value_usd": float(positions.total_value_usd),
            "protected_minimum_usd": float(protected_min),
            "max_loss_percent": float(max_loss_percent * 100),
            "max_loss_usd": float(max_loss_usd),
            "protections": [
                "Position-aware loss cap enforced",
                "MEV protection on all swaps",
                "10-second cancel window",
                "Simulation before execution",
                "Post-execution verification",
            ],
            "has_liquidation_risk": positions.has_liquidation_risk,
        }
