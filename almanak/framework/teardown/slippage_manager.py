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

    ``disposition`` (VIB-4532 / VIB-4664 / VIB-4258) refines that decision for
    execution/simulation reverts. It is set by the teardown manager from
    :func:`almanak.framework.teardown.error_taxonomy.classify_teardown_failure`:

    * ``"escalate"`` (default) — walk the slippage ladder, as before.
    * ``"non_retryable"`` — deterministic revert no slippage level can fix;
      short-circuit (equivalent to ``retryable=False``).
    * ``"retry_same_level"`` — transient transport/RPC error; retry at the SAME
      slippage level then abort, never escalating to an operator-approval gate.
    """

    success: bool
    slippage_used: Decimal
    actual_slippage: Decimal | None = None
    error: str | None = None
    retry_count: int = 0
    retryable: bool = True
    retry_after_seconds: float | None = None
    disposition: str = "escalate"


@dataclass
class ExecutionResult:
    """Result of executing with escalating slippage."""

    success: bool
    final_slippage: Decimal
    status: str  # "completed", "paused_awaiting_approval", "failed_manual_intervention_required",
    # "failed_non_retryable" (deterministic revert), "failed_rpc_unreachable" (transport/RPC)
    attempts: list[ExecutionAttempt]
    current_level: EscalationLevel | None = None
    message: str | None = None
    approval_request: ApprovalRequest | None = None

    @property
    def total_attempts(self) -> int:
        """Get total number of execution attempts."""
        return sum(a.retry_count + 1 for a in self.attempts)


@dataclass(frozen=True)
class _EscalationPlan:
    """Prepared ladder and auto-mode ceiling for one execution."""

    levels: list[EscalationConfig]
    auto_max_slippage: Decimal


@dataclass(frozen=True)
class _ApprovalDecision:
    """Whether the current rung may execute or has a terminal result."""

    execute_level: bool
    result: ExecutionResult | None = None


@dataclass(frozen=True)
class _AttemptDecision:
    """Folded outcome of an attempt and its retry disposition."""

    disposition: str
    result: ExecutionResult | None = None


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

    def _prepare_escalation(self, intent_slippage: Decimal | None) -> _EscalationPlan:
        """Copy the ladder, apply the intent ceiling, and derive the auto-mode cap."""
        effective_levels = [
            EscalationConfig(
                level=level.level,
                slippage=level.slippage,
                auto_approve=level.auto_approve,
                retries=level.retries,
            )
            for level in self.levels
        ]
        effective_auto_max = self.config.auto_max_slippage
        if intent_slippage is None or intent_slippage <= Decimal("0"):
            return _EscalationPlan(effective_levels, effective_auto_max)

        if intent_slippage > self.config.absolute_max_slippage:
            logger.warning(
                "Intent slippage %.1f%% exceeds absolute max %.1f%%, clamping.",
                float(intent_slippage * 100),
                float(self.config.absolute_max_slippage * 100),
            )
            intent_slippage = self.config.absolute_max_slippage

        if not any(level.slippage == intent_slippage for level in effective_levels):
            injected_level = self.get_level_for_slippage(intent_slippage) or EscalationLevel.LEVEL_4
            logger.info(
                "Injecting auto-approve level at %.1f%% from strategy teardown config.",
                float(intent_slippage * 100),
            )
            effective_levels.append(
                EscalationConfig(
                    level=injected_level,
                    slippage=intent_slippage,
                    auto_approve=True,
                    retries=1,
                )
            )

        for level in effective_levels:
            if level.slippage <= intent_slippage and not level.auto_approve:
                logger.info(
                    "Overriding level at %.1f%% to auto-approve (at or below intent slippage %.1f%%).",
                    float(level.slippage * 100),
                    float(intent_slippage * 100),
                )
                level.auto_approve = True

        effective_levels.sort(key=lambda level: level.slippage)
        if intent_slippage > effective_auto_max:
            effective_auto_max = min(intent_slippage, self.config.absolute_max_slippage)
            logger.info(
                "Raising auto-mode slippage cap from %.1f%% to %.1f%% (intent_slippage=%.1f%%).",
                float(self.config.auto_max_slippage * 100),
                float(effective_auto_max * 100),
                float(intent_slippage * 100),
            )

        return _EscalationPlan(effective_levels, effective_auto_max)

    async def _resolve_level_approval(
        self,
        *,
        level_config: EscalationConfig,
        max_loss_percent: Decimal,
        position_value: Decimal,
        attempts: list[ExecutionAttempt],
        on_approval_needed: ApprovalCallback | None,
        teardown_id: str,
        deployment_id: str,
    ) -> _ApprovalDecision:
        """Apply the rung's human gate without changing its approved slippage."""
        slippage = level_config.slippage
        approval_required = not level_config.auto_approve and (
            slippage > max_loss_percent or slippage > self.config.manual_approval_threshold
        )
        if not approval_required:
            return _ApprovalDecision(execute_level=True)

        approval_request = self._create_approval_request(
            teardown_id=teardown_id,
            deployment_id=deployment_id,
            level=level_config.level,
            slippage=slippage,
            position_value=position_value,
        )
        if on_approval_needed is None:
            return _ApprovalDecision(
                execute_level=False,
                result=ExecutionResult(
                    success=False,
                    final_slippage=slippage,
                    status="paused_awaiting_approval",
                    attempts=attempts,
                    current_level=level_config.level,
                    message=f"Approval required for {slippage:.1%} slippage",
                    approval_request=approval_request,
                ),
            )

        logger.info(
            f"Requesting approval for {slippage:.1%} slippage (estimated loss: ${position_value * slippage:,.2f})"
        )
        approval_response = await on_approval_needed(approval_request)
        if approval_response.approved:
            return _ApprovalDecision(execute_level=True)

        if approval_response.action == "wait_and_escalate":
            logger.info(
                "Operator declined level %s; sleeping %.1fs then escalating to next level",
                level_config.level,
                self.config.retry_delay_seconds * 2,
            )
            await asyncio.sleep(self.config.retry_delay_seconds * 2)
            return _ApprovalDecision(execute_level=False)

        return _ApprovalDecision(
            execute_level=False,
            result=ExecutionResult(
                success=False,
                final_slippage=slippage,
                status="cancelled_by_user",
                attempts=attempts,
                current_level=level_config.level,
                message=f"User declined approval for {slippage:.1%} slippage",
            ),
        )

    def _fold_attempt_result(
        self,
        *,
        attempt: ExecutionAttempt,
        level_config: EscalationConfig,
        retry: int,
        attempts: list[ExecutionAttempt],
    ) -> _AttemptDecision:
        """Convert terminal attempt states into the public execution result contract."""
        slippage = level_config.slippage
        if attempt.success:
            final_slippage = attempt.actual_slippage if attempt.actual_slippage is not None else slippage
            return _AttemptDecision(
                disposition="escalate",
                result=ExecutionResult(
                    success=True,
                    final_slippage=final_slippage,
                    status="completed",
                    attempts=attempts,
                    current_level=level_config.level,
                    message=f"Executed successfully at {slippage:.1%} slippage",
                ),
            )

        disposition = getattr(attempt, "disposition", "escalate")
        if not attempt.retryable or disposition == "non_retryable":
            logger.info(f"Non-retryable failure at {slippage:.1%}: {attempt.error}. Skipping further escalation.")
            return _AttemptDecision(
                disposition=disposition,
                result=ExecutionResult(
                    success=False,
                    final_slippage=slippage,
                    status="failed_non_retryable",
                    attempts=attempts,
                    current_level=level_config.level,
                    message=f"Non-retryable error: {attempt.error}",
                ),
            )

        if disposition == "retry_same_level" and retry == level_config.retries - 1:
            logger.warning(
                "Transport/RPC failure persisted after %d same-level retries; aborting without escalation: %s",
                level_config.retries,
                attempt.error,
            )
            return _AttemptDecision(
                disposition=disposition,
                result=ExecutionResult(
                    success=False,
                    final_slippage=slippage,
                    status="failed_rpc_unreachable",
                    attempts=attempts,
                    current_level=level_config.level,
                    message=f"Transport/RPC unreachable: {attempt.error}",
                ),
            )

        return _AttemptDecision(disposition=disposition)

    async def _dispatch_level(
        self,
        *,
        intent: Any,
        level_config: EscalationConfig,
        execute_func: ExecuteFunc,
        attempts: list[ExecutionAttempt],
    ) -> ExecutionResult | None:
        """Dispatch simulation/execution attempts sequentially for one ladder rung."""
        slippage = level_config.slippage
        for retry in range(level_config.retries):
            logger.info(
                f"Attempting execution at {slippage:.1%} slippage "
                f"(level {level_config.level.value}, attempt {retry + 1}/{level_config.retries})"
            )
            attempt = await execute_func(intent, slippage)
            attempt.retry_count = retry
            attempts.append(attempt)

            if not attempt.success:
                logger.warning(f"Execution failed at {slippage:.1%}: {attempt.error}")
            decision = self._fold_attempt_result(
                attempt=attempt,
                level_config=level_config,
                retry=retry,
                attempts=attempts,
            )
            if decision.result is not None:
                return decision.result

            if decision.disposition == "retry_same_level":
                retry_delay = float(self.config.retry_delay_seconds)
                if attempt.retry_after_seconds is not None:
                    retry_delay = max(retry_delay, attempt.retry_after_seconds)
                logger.info(
                    "Transient transport failure at %.1f%%: %s. Retrying at the same level (%.2fs backoff).",
                    slippage * 100,
                    attempt.error,
                    retry_delay,
                )
                await asyncio.sleep(retry_delay)
            elif retry < level_config.retries - 1:
                retry_delay = float(self.config.retry_delay_seconds)
                if attempt.retry_after_seconds is not None:
                    retry_delay = max(retry_delay, attempt.retry_after_seconds)
                    logger.info("Retryable failure requested %.2fs backoff before retry.", retry_delay)
                await asyncio.sleep(retry_delay)
            elif attempt.retry_after_seconds is not None:
                retry_delay = max(self.config.retry_delay_seconds, attempt.retry_after_seconds)
                logger.info("Retryable failure requested %.2fs backoff before escalation.", retry_delay)
                await asyncio.sleep(retry_delay)

        return None

    async def execute_with_escalation(
        self,
        intent: Any,
        position_value: Decimal,
        execute_func: ExecuteFunc,
        on_approval_needed: ApprovalCallback | None = None,
        teardown_id: str = "",
        deployment_id: str = "",
        is_auto_mode: bool = False,
        intent_slippage: Decimal | None = None,
    ) -> ExecutionResult:
        """Execute an intent with escalating slippage.

        Tries execution at increasing slippage levels. For auto-approve
        levels, retries automatically. For approval-required levels,
        pauses and requests human approval.

        When ``intent_slippage`` is provided (from the strategy's teardown
        intent), the manager uses it as a ceiling for auto-approval: all
        escalation levels at or below ``intent_slippage`` are auto-approved,
        and a new level is injected at ``intent_slippage`` if one doesn't
        already exist.  The ladder is then sorted to ensure monotonic
        escalation.

        Args:
            intent: The intent to execute
            position_value: Value of the position (for loss calculations)
            execute_func: Async function that attempts execution at given slippage
            on_approval_needed: Callback when human approval is needed
            teardown_id: ID of the teardown operation
            deployment_id: ID of the strategy
            is_auto_mode: Whether this is an auto-protect triggered exit
            intent_slippage: Strategy-configured slippage ceiling for auto-approval

        Returns:
            ExecutionResult with outcome and details
        """
        attempts: list[ExecutionAttempt] = []
        max_loss_percent = calculate_max_acceptable_loss(position_value)
        plan = self._prepare_escalation(intent_slippage)

        for level_config in plan.levels:
            slippage = level_config.slippage
            if is_auto_mode and slippage > plan.auto_max_slippage:
                logger.info(
                    f"Auto mode: stopping at {plan.auto_max_slippage:.1%} "
                    f"(level {level_config.level.value} requires {slippage:.1%})"
                )
                return ExecutionResult(
                    success=False,
                    final_slippage=plan.auto_max_slippage,
                    status="paused_auto_limit_reached",
                    attempts=attempts,
                    current_level=level_config.level,
                    message=(
                        f"Auto-exit paused. Market requires {slippage:.1%} slippage but auto limit is "
                        f"{plan.auto_max_slippage:.1%}. Manual intervention needed."
                    ),
                )

            approval = await self._resolve_level_approval(
                level_config=level_config,
                max_loss_percent=max_loss_percent,
                position_value=position_value,
                attempts=attempts,
                on_approval_needed=on_approval_needed,
                teardown_id=teardown_id,
                deployment_id=deployment_id,
            )
            if approval.result is not None:
                return approval.result
            if not approval.execute_level:
                continue

            execution_result = await self._dispatch_level(
                intent=intent,
                level_config=level_config,
                execute_func=execute_func,
                attempts=attempts,
            )
            if execution_result is not None:
                return execution_result

        return ExecutionResult(
            success=False,
            final_slippage=plan.levels[-1].slippage,
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
        deployment_id: str,
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
            deployment_id=deployment_id,
            current_level=level,
            current_slippage=slippage,
            estimated_loss_usd=estimated_loss,
            position_value_usd=position_value,
            reason=reason,
            options=["Accept cost", "Wait & Escalate to next level", "Cancel"],
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
