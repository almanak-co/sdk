"""Escalating Slippage Manager for the Strategy Teardown System.

Manages slippage escalation with human checkpoints. Instead of accepting
"any slippage" in emergency mode, we escalate with approval gates:

Escalation Ladder:
1. Try at 2% slippage (auto-approve, 3 retries)
2. Try at 3% slippage (auto-approve, 2 retries)
3. PAUSE - Ask operator for approval (5%, show $ cost)
4. PAUSE - Explicit warning (8%, high risk)
5. Manual intervention required (>8%)

This ensures users always have agency over costs while still enabling
fast exits when needed.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC
from decimal import Decimal
from typing import Any, Protocol

from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.models import (
    ApprovalRequest,
    ApprovalResponse,
    EscalationConfig,
    EscalationLevel,
    calculate_max_acceptable_loss,
)

logger = logging.getLogger(__name__)


class Intent(Protocol):
    """Protocol for intent objects."""

    @property
    def intent_type(self) -> str:
        """Get the intent type."""
        ...


@dataclass
class ExecutionAttempt:
    """Result of a single execution attempt.

    Callers MUST set ``retryable=False`` for deterministic failures (e.g.,
    compilation errors, missing prices, unknown tokens) so the slippage
    manager skips further escalation.  The default ``True`` is correct for
    transient errors (RPC timeout, nonce conflict, gas estimation).
    """

    success: bool
    slippage_used: Decimal
    actual_slippage: Decimal | None = None
    error: str | None = None
    retry_count: int = 0
    retryable: bool = True


@dataclass
class ExecutionResult:
    """Result of executing with escalating slippage."""

    success: bool
    final_slippage: Decimal
    status: str  # "completed", "paused_awaiting_approval", "failed_manual_intervention"
    attempts: list[ExecutionAttempt]
    current_level: EscalationLevel | None = None
    message: str | None = None
    approval_request: ApprovalRequest | None = None

    @property
    def total_attempts(self) -> int:
        """Get total number of execution attempts."""
        return sum(a.retry_count + 1 for a in self.attempts)


# Type alias for execution function
ExecuteFunc = Callable[[Any, Decimal], Awaitable[ExecutionAttempt]]

# Type alias for approval callback
ApprovalCallback = Callable[[ApprovalRequest], Awaitable[ApprovalResponse]]


class EscalatingSlippageManager:
    """Manages slippage escalation with human checkpoints.

    The manager handles the escalation ladder, retries at each level,
    and pauses for human approval when crossing thresholds.

    Key behaviors:
    - Levels 1-2: Auto-approve, multiple retries
    - Levels 3-4: Require human approval, single retry after approval
    - Level 5+: Requires manual intervention, no auto-execution
    """

    # Default escalation levels
    DEFAULT_LEVELS = [
        {"level": EscalationLevel.LEVEL_1, "slippage": Decimal("0.02"), "auto_approve": True, "retries": 3},
        {"level": EscalationLevel.LEVEL_2, "slippage": Decimal("0.03"), "auto_approve": True, "retries": 2},
        {"level": EscalationLevel.LEVEL_3, "slippage": Decimal("0.05"), "auto_approve": False, "retries": 1},
        {"level": EscalationLevel.LEVEL_4, "slippage": Decimal("0.08"), "auto_approve": False, "retries": 1},
    ]

    def __init__(
        self,
        config: TeardownConfig | None = None,
        levels: list[dict] | None = None,
    ):
        """Initialize the slippage manager.

        Args:
            config: Teardown configuration
            levels: Custom escalation levels (optional)
        """
        self.config = config or TeardownConfig.default()
        self.levels = self._build_levels(levels)

    def _build_levels(self, custom_levels: list[dict] | None) -> list[EscalationConfig]:
        """Build escalation level configurations."""
        if custom_levels:
            return [
                EscalationConfig(
                    level=lvl["level"],
                    slippage=Decimal(str(lvl["slippage"])),
                    auto_approve=lvl["auto_approve"],
                    retries=lvl["retries"],
                )
                for lvl in custom_levels
            ]

        return EscalationConfig.default_levels()

    async def execute_with_escalation(
        self,
        intent: Any,
        position_value: Decimal,
        execute_func: ExecuteFunc,
        on_approval_needed: ApprovalCallback | None = None,
        teardown_id: str = "",
        strategy_id: str = "",
        is_auto_mode: bool = False,
    ) -> ExecutionResult:
        """Execute an intent with escalating slippage.

        Tries execution at increasing slippage levels. For auto-approve
        levels, retries automatically. For approval-required levels,
        pauses and requests human approval.

        Args:
            intent: The intent to execute
            position_value: Value of the position (for loss calculations)
            execute_func: Async function that attempts execution at given slippage
            on_approval_needed: Callback when human approval is needed
            teardown_id: ID of the teardown operation
            strategy_id: ID of the strategy
            is_auto_mode: Whether this is an auto-protect triggered exit

        Returns:
            ExecutionResult with outcome and details
        """
        attempts: list[ExecutionAttempt] = []
        max_loss_percent = calculate_max_acceptable_loss(position_value)

        for level_config in self.levels:
            slippage = level_config.slippage

            # In auto mode, don't exceed configured max
            if is_auto_mode and slippage > self.config.auto_max_slippage:
                logger.info(
                    f"Auto mode: stopping at {self.config.auto_max_slippage:.1%} "
                    f"(level {level_config.level.value} requires {slippage:.1%})"
                )
                return ExecutionResult(
                    success=False,
                    final_slippage=self.config.auto_max_slippage,
                    status="paused_auto_limit_reached",
                    attempts=attempts,
                    current_level=level_config.level,
                    message=f"Auto-exit paused. Market requires {slippage:.1%} slippage but auto limit is {self.config.auto_max_slippage:.1%}. Manual intervention needed.",
                )

            # Check if approval is needed
            if not level_config.auto_approve:
                # Check if slippage exceeds position-aware cap
                if slippage > max_loss_percent or slippage > self.config.manual_approval_threshold:
                    if on_approval_needed is None:
                        # No approval callback - pause
                        return ExecutionResult(
                            success=False,
                            final_slippage=slippage,
                            status="paused_awaiting_approval",
                            attempts=attempts,
                            current_level=level_config.level,
                            message=f"Approval required for {slippage:.1%} slippage",
                            approval_request=self._create_approval_request(
                                teardown_id=teardown_id,
                                strategy_id=strategy_id,
                                level=level_config.level,
                                slippage=slippage,
                                position_value=position_value,
                            ),
                        )

                    # Request approval
                    approval_request = self._create_approval_request(
                        teardown_id=teardown_id,
                        strategy_id=strategy_id,
                        level=level_config.level,
                        slippage=slippage,
                        position_value=position_value,
                    )

                    logger.info(
                        f"Requesting approval for {slippage:.1%} slippage "
                        f"(estimated loss: ${position_value * slippage:,.2f})"
                    )

                    approval_response = await on_approval_needed(approval_request)

                    if not approval_response.approved:
                        if approval_response.action == "wait_and_retry":
                            # Wait and retry at same level
                            logger.info("User chose to wait and retry")
                            await asyncio.sleep(self.config.retry_delay_seconds * 2)
                            # Will continue to next iteration
                            continue
                        else:
                            # User cancelled or chose different action
                            return ExecutionResult(
                                success=False,
                                final_slippage=slippage,
                                status="cancelled_by_user",
                                attempts=attempts,
                                current_level=level_config.level,
                                message=f"User declined approval for {slippage:.1%} slippage",
                            )

            # Try execution at this level
            for retry in range(level_config.retries):
                logger.info(
                    f"Attempting execution at {slippage:.1%} slippage "
                    f"(level {level_config.level.value}, attempt {retry + 1}/{level_config.retries})"
                )

                attempt = await execute_func(intent, slippage)
                attempt.retry_count = retry

                attempts.append(attempt)

                if attempt.success:
                    return ExecutionResult(
                        success=True,
                        final_slippage=attempt.actual_slippage or slippage,
                        status="completed",
                        attempts=attempts,
                        current_level=level_config.level,
                        message=f"Executed successfully at {slippage:.1%} slippage",
                    )

                # Failed - log and potentially retry
                logger.warning(f"Execution failed at {slippage:.1%}: {attempt.error}")

                # Don't retry deterministic failures (missing price, unknown token, etc.)
                if not attempt.retryable:
                    logger.info(
                        f"Non-retryable failure at {slippage:.1%}: {attempt.error}. Skipping further escalation."
                    )
                    return ExecutionResult(
                        success=False,
                        final_slippage=slippage,
                        status="failed_non_retryable",
                        attempts=attempts,
                        current_level=level_config.level,
                        message=f"Non-retryable error: {attempt.error}",
                    )

                if retry < level_config.retries - 1:
                    await asyncio.sleep(self.config.retry_delay_seconds)

        # All levels exhausted
        return ExecutionResult(
            success=False,
            final_slippage=self.levels[-1].slippage,
            status="failed_manual_intervention_required",
            attempts=attempts,
            current_level=EscalationLevel.LEVEL_5,
            message="Slippage exceeds all automatic levels. Manual intervention required.",
        )

    def get_initial_slippage(self, mode: str, is_auto: bool = False) -> Decimal:
        """Get the initial slippage for a teardown mode.

        Args:
            mode: "graceful" or "emergency"
            is_auto: Whether this is an auto-protect triggered exit

        Returns:
            Initial slippage to use
        """
        if is_auto:
            # Auto mode starts at configured max
            return self.config.auto_max_slippage

        if mode == "graceful":
            return Decimal("0.005")  # 0.5% for graceful

        # Emergency mode starts at level 1
        return self.levels[0].slippage

    def get_level_for_slippage(self, slippage: Decimal) -> EscalationLevel | None:
        """Get the escalation level for a given slippage.

        Args:
            slippage: The slippage percentage

        Returns:
            The corresponding escalation level, or None if exceeds all levels
        """
        for level_config in self.levels:
            if slippage <= level_config.slippage:
                return level_config.level

        return EscalationLevel.LEVEL_5

    def requires_approval(self, slippage: Decimal, position_value: Decimal) -> bool:
        """Check if the given slippage requires approval.

        Args:
            slippage: The slippage to check
            position_value: Position value for cap calculation

        Returns:
            True if human approval is required
        """
        max_loss = calculate_max_acceptable_loss(position_value)

        # Requires approval if exceeds position-aware cap or approval threshold
        return slippage > max_loss or slippage > self.config.manual_approval_threshold

    def _create_approval_request(
        self,
        teardown_id: str,
        strategy_id: str,
        level: EscalationLevel,
        slippage: Decimal,
        position_value: Decimal,
    ) -> ApprovalRequest:
        """Create an approval request for a slippage escalation."""
        from datetime import datetime, timedelta

        estimated_loss = position_value * slippage

        # Determine reason based on level
        if level == EscalationLevel.LEVEL_3:
            reason = f"Market conditions require {slippage:.1%} slippage (~${estimated_loss:,.2f} cost)"
        elif level == EscalationLevel.LEVEL_4:
            reason = f"Extreme conditions. {slippage:.1%} slippage required (~${estimated_loss:,.2f} cost). This exceeds normal safety limits."
        else:
            reason = f"Slippage of {slippage:.1%} requires your approval"

        return ApprovalRequest(
            teardown_id=teardown_id,
            strategy_id=strategy_id,
            current_level=level,
            current_slippage=slippage,
            estimated_loss_usd=estimated_loss,
            position_value_usd=position_value,
            reason=reason,
            options=["Accept cost", "Wait & Retry", "Cancel"],
            requested_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )

    def get_escalation_summary(self) -> list[dict[str, Any]]:
        """Get a summary of escalation levels for display.

        Returns:
            List of level summaries with slippage and approval info
        """
        return [
            {
                "level": config.level.value,
                "slippage_percent": float(config.slippage * 100),
                "auto_approve": config.auto_approve,
                "retries": config.retries,
            }
            for config in self.levels
        ]
