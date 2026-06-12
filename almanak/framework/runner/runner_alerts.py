"""Operator alerting and emergency-trigger collaborator for StrategyRunner.

Extracted from strategy_runner.py (Plan 015). ``RunnerAlerter`` holds a
back-reference to the runner and reads ALL runner state through it at call
time — never snapshot attributes at construction, because tests patch
runner attributes (alert_manager, _operator_card_generator, _stuck_detector,
_circuit_breaker, _emergency_manager, _is_managed_deployment,
request_shutdown, _query_portfolio_value, _lifecycle_write_state) after the
runner is built. The runner keeps thin ``_alert_*`` / ``_handle_execution_error``
/ ``_maybe_trigger_emergency`` delegation wrappers; production code and tests
continue to call those wrappers.

Per-call construction (``RunnerAlerter(self)`` in each wrapper rather than a
cached ``self._alerter``) is deliberate: the collaborator is stateless (a
single runner back-reference), alert paths are cold error-handling paths, and
per-call construction keeps the wrappers working for test stubs that bypass
``StrategyRunner.__init__`` (the ``_Runner`` pattern in
test_accounting_persistence.py). Do NOT cache the collaborator on the runner
— that breaks post-construction patching.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ..models.actions import AvailableAction, SuggestedAction
from ..models.operator_card import EventType, OperatorCard, PositionSummary, Severity
from ..models.stuck_reason import StuckReason

if TYPE_CHECKING:
    from ..execution.extract_result import CriticalAccountingError
    from ..execution.orchestrator import ExecutionResult
    from .runner_models import IterationResult, StrategyProtocol

# Pinned to the original module's logger name so existing log-capture tests
# and log-filtering rules continue to work (same pattern as runner_state.py).
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


class RunnerAlerter:
    def __init__(self, runner: Any) -> None:
        self._runner = runner

    async def handle_execution_error(
        self,
        strategy: StrategyProtocol,
        execution_result: ExecutionResult,
    ) -> None:
        """Handle execution errors with alerting.

        When an OperatorCardGenerator is configured, generates rich cards with
        auto-detected StuckReason, computed severity, and suggested actions.
        Falls back to a basic card when no generator is available.
        """
        if not self._runner.config.enable_alerting or not self._runner.alert_manager:
            return

        try:
            exec_total_value, exec_available = self._runner._query_portfolio_value(strategy)
            if self._runner._operator_card_generator is not None:
                from ..services.operator_card_generator import ErrorContext, StrategyState

                error_ctx = ErrorContext(
                    error_type=type(execution_result).__name__,
                    error_message=execution_result.error or "Unknown execution error",
                    gas_used=execution_result.total_gas_used,
                    revert_reason=getattr(execution_result, "revert_reason", None),
                )
                strategy_state = StrategyState(
                    deployment_id=strategy.deployment_id,
                    status="error",
                    total_value_usd=exec_total_value,
                    available_balance_usd=exec_available,
                    stuck_since=self._runner._first_error_at,
                    last_successful_action=None,
                )
                card = self._runner._operator_card_generator.generate_card(
                    strategy_state=strategy_state,
                    error_context=error_ctx,
                    event_type=EventType.ERROR,
                )
            else:
                card = OperatorCard(
                    deployment_id=strategy.deployment_id,
                    timestamp=datetime.now(UTC),
                    event_type=EventType.ERROR,
                    reason=StuckReason.TRANSACTION_REVERTED,
                    context={
                        "phase": execution_result.phase.value if execution_result.phase else "unknown",
                        "error": execution_result.error or "Unknown error",
                        "gas_used": execution_result.total_gas_used,
                    },
                    severity=Severity.HIGH,
                    position_summary=PositionSummary(
                        total_value_usd=exec_total_value,
                        available_balance_usd=exec_available,
                    ),
                    risk_description="Strategy execution failed - positions may be at risk",
                    suggested_actions=[
                        SuggestedAction(
                            action=AvailableAction.RESUME,
                            description="Resume to retry the failed transaction",
                            priority=1,
                            is_recommended=True,
                        ),
                    ],
                    available_actions=[AvailableAction.RESUME, AvailableAction.PAUSE],
                )

            await self._runner.alert_manager.send_alert(card)

        except Exception as e:
            logger.error(f"Failed to send execution error alert: {e}")

    async def alert_accounting_failure(
        self,
        strategy: StrategyProtocol,
        error: Exception,
    ) -> None:
        """Send a CRITICAL operator alert for accounting persistence failure.

        The on-chain state changed but the durable accounting write did not
        succeed. This is a book-keeping emergency -- paused strategy and
        manual reconciliation are required before resuming. Severity is
        CRITICAL rather than HIGH because silent accounting loss is
        irrecoverable once alerting is missed.
        """
        if not self._runner.config.enable_alerting or not self._runner.alert_manager:
            return

        try:
            total_value, available = self._runner._query_portfolio_value(strategy)
            write_kind = getattr(error, "write_kind", "unknown")
            card = OperatorCard(
                deployment_id=strategy.deployment_id,
                timestamp=datetime.now(UTC),
                event_type=EventType.ERROR,
                reason=StuckReason.UNKNOWN,
                context={
                    "accounting_write_kind": write_kind,
                    "error": str(error),
                },
                severity=Severity.CRITICAL,
                position_summary=PositionSummary(
                    total_value_usd=total_value,
                    available_balance_usd=available,
                ),
                risk_description=(
                    f"Accounting persistence failed ({write_kind}). On-chain state may have "
                    "changed without a durable ledger/snapshot/metrics record. Manual "
                    "reconciliation required before resuming."
                ),
                suggested_actions=[
                    SuggestedAction(
                        action=AvailableAction.PAUSE,
                        description="Pause strategy and investigate accounting backend",
                        priority=1,
                        is_recommended=True,
                    ),
                ],
                available_actions=[AvailableAction.PAUSE, AvailableAction.RESUME],
            )
            await self._runner.alert_manager.send_alert(card)
        except Exception as alert_err:  # noqa: BLE001
            logger.error("Failed to send accounting failure alert: %s", alert_err)

    async def alert_enrichment_failure(
        self,
        strategy: StrategyProtocol,
        error: CriticalAccountingError,
    ) -> None:
        """Send a CRITICAL operator alert for receipt-enrichment failure.

        The on-chain transaction succeeded but the framework cannot reliably
        parse what happened — position IDs, swap amounts, and other enriched
        fields are unavailable. Strategies that depend on these fields may
        enter a ghost-position state. Manual reconciliation is required.

        Distinct from ``alert_accounting_failure`` (which covers ledger /
        snapshot / metrics write failures) so monitoring rules can route the
        two failure classes to the appropriate on-call rotation and runbook.
        """
        if not self._runner.config.enable_alerting or not self._runner.alert_manager:
            return

        try:
            total_value, available = self._runner._query_portfolio_value(strategy)
            card = OperatorCard(
                deployment_id=strategy.deployment_id,
                timestamp=datetime.now(UTC),
                event_type=EventType.ERROR,
                reason=StuckReason.UNKNOWN,
                context={
                    "accounting_write_kind": "enrichment",
                    "field_name": error.field_name or "unknown",
                    "intent_type": error.intent_type or "unknown",
                    "protocol": error.protocol or "unknown",
                    "error": str(error),
                },
                severity=Severity.CRITICAL,
                position_summary=PositionSummary(
                    total_value_usd=total_value,
                    available_balance_usd=available,
                ),
                risk_description=(
                    f"Receipt enrichment failed (field={error.field_name}, "
                    f"intent={error.intent_type}, protocol={error.protocol}). "
                    "On-chain state changed but framework cannot parse the outcome — "
                    "ghost-position risk. Manual reconciliation required before resuming."
                ),
                suggested_actions=[
                    SuggestedAction(
                        action=AvailableAction.PAUSE,
                        description="Pause strategy and reconcile on-chain state with strategy state",
                        priority=1,
                        is_recommended=True,
                    ),
                ],
                available_actions=[AvailableAction.PAUSE, AvailableAction.RESUME],
            )
            await self._runner.alert_manager.send_alert(card)
        except Exception as alert_err:  # noqa: BLE001
            logger.error("Failed to send enrichment failure alert: %s", alert_err)

    async def alert_consecutive_errors(
        self,
        strategy: StrategyProtocol,
        last_result: IterationResult,
    ) -> None:
        """Send alert for consecutive errors threshold breach.

        When StuckDetector and OperatorCardGenerator are configured, produces
        intelligent failure classification with root-cause analysis and
        actionable remediation steps. Falls back to a basic card otherwise.
        """
        if not self._runner.config.enable_alerting or not self._runner.alert_manager:
            return

        try:
            consec_total_value, consec_available = self._runner._query_portfolio_value(strategy)
            if self._runner._operator_card_generator is not None:
                from ..services.operator_card_generator import ErrorContext, StrategyState

                # Build ErrorContext from the last iteration result
                error_ctx = ErrorContext(
                    error_type=last_result.status.value,
                    error_message=last_result.error or "Unknown error",
                )

                # Build StrategyState with what we know from the runner
                strategy_state = StrategyState(
                    deployment_id=strategy.deployment_id,
                    status="stuck"
                    if self._runner._consecutive_errors >= self._runner.config.max_consecutive_errors
                    else "error",
                    total_value_usd=consec_total_value,
                    available_balance_usd=consec_available,
                    stuck_since=self._runner._first_error_at,
                    last_successful_action=None,
                )

                # Use StuckDetector for intelligent classification if available
                stuck_reason = None
                if self._runner._stuck_detector is not None:
                    from ..execution.circuit_breaker import CircuitBreakerState
                    from ..services.stuck_detector import StrategySnapshot

                    snapshot = StrategySnapshot(
                        deployment_id=strategy.deployment_id,
                        chain=getattr(strategy, "chain", "unknown"),
                        current_state=last_result.status.value,
                        state_entered_at=self._runner._first_error_at or datetime.now(UTC),
                        pending_transactions=[],
                        circuit_breaker_triggered=(
                            self._runner._circuit_breaker is not None
                            and self._runner._circuit_breaker.state == CircuitBreakerState.OPEN
                        ),
                        rpc_healthy="rpc" not in (last_result.error or "").lower(),
                        last_rpc_error=(
                            last_result.error if last_result.error and "rpc" in last_result.error.lower() else None
                        ),
                    )
                    detection = self._runner._stuck_detector.detect_stuck(snapshot)
                    if detection.is_stuck and detection.reason:
                        stuck_reason = detection.reason
                        logger.info(
                            "StuckDetector classified %s as %s",
                            strategy.deployment_id,
                            stuck_reason.value,
                        )

                # Generate rich card via OperatorCardGenerator
                event_type = EventType.STUCK if stuck_reason else EventType.WARNING
                card = self._runner._operator_card_generator.generate_card(
                    strategy_state=strategy_state,
                    error_context=error_ctx,
                    event_type=event_type,
                )
            else:
                # Fallback: basic card without intelligent classification
                card = OperatorCard(
                    deployment_id=strategy.deployment_id,
                    timestamp=datetime.now(UTC),
                    event_type=EventType.WARNING,
                    reason=StuckReason.UNKNOWN,
                    context={
                        "consecutive_errors": self._runner._consecutive_errors,
                        "max_allowed": self._runner.config.max_consecutive_errors,
                        "last_error": last_result.error or "Unknown",
                        "last_status": last_result.status.value,
                    },
                    severity=Severity.MEDIUM,
                    position_summary=PositionSummary(
                        total_value_usd=consec_total_value,
                        available_balance_usd=consec_available,
                    ),
                    risk_description=(f"Strategy has failed {self._runner._consecutive_errors} consecutive times"),
                    suggested_actions=[
                        SuggestedAction(
                            action=AvailableAction.PAUSE,
                            description="Pause strategy to review error logs",
                            priority=1,
                            is_recommended=True,
                        ),
                    ],
                    available_actions=[AvailableAction.PAUSE, AvailableAction.RESUME],
                )

            await self._runner.alert_manager.send_alert(card)

        except Exception as e:
            logger.error(f"Failed to send consecutive errors alert: {e}")

    async def maybe_trigger_emergency(
        self,
        strategy: StrategyProtocol,
        last_result: IterationResult,
    ) -> None:
        """Trigger emergency stop if the circuit breaker just tripped to OPEN.

        Called after every failure recording. Only fires once per OPEN transition
        by tracking whether we've already triggered for this OPEN state via
        the _emergency_triggered_for_open flag.
        """
        if self._runner._emergency_manager is None or self._runner._circuit_breaker is None:
            return

        # Only trigger when breaker is OPEN
        from ..execution.circuit_breaker import CircuitBreakerState

        if self._runner._circuit_breaker.state != CircuitBreakerState.OPEN:
            self._runner._emergency_triggered_for_open = False
            return

        # Don't trigger more than once per OPEN episode
        if self._runner._emergency_triggered_for_open:
            return

        try:
            cb_check = self._runner._circuit_breaker.check()
            reason = (
                f"Circuit breaker tripped after {cb_check.consecutive_failures} "
                f"consecutive failures: {last_result.error or 'unknown error'}"
            )
            logger.warning(
                "EMERGENCY: triggering emergency stop for %s — %s",
                strategy.deployment_id,
                reason,
            )
            await self._runner._emergency_manager.emergency_stop_async(
                deployment_id=strategy.deployment_id,
                reason=reason,
                chain=getattr(strategy, "chain", ""),
                trigger_context={
                    "consecutive_failures": cb_check.consecutive_failures,
                    "cumulative_loss_usd": str(cb_check.cumulative_loss_usd),
                    "last_status": last_result.status.value,
                    "last_error": last_result.error,
                },
            )
            # Only mark as triggered after successful emergency stop
            self._runner._emergency_triggered_for_open = True

            # In managed deployments, write ERROR state and exit so the pod
            # terminates and K8s resources are freed.  Local development keeps
            # the loop alive for debugging.
            if self._runner._is_managed_deployment():
                # A trip driven purely by market-data failures (no execution
                # faults) must NOT kill the process. The correct response to a
                # data outage / quiet-pool staleness is to idle-HOLD and let the
                # breaker cool down -> HALF_OPEN -> recover when data returns.
                # Exiting would turn a transient or scheduled data gap into a
                # permanently dead agent (and abandon any position the breaker is
                # meant to protect). Exit only when action-class failures
                # contributed to the trip.
                if self._runner._circuit_breaker.tripped_on_data_class_only:
                    logger.warning(
                        "Circuit breaker tripped on market-data failures only for %s — "
                        "keeping process alive to auto-recover after cooldown (no process exit)",
                        strategy.deployment_id,
                    )
                else:
                    self._runner._terminal_lifecycle_state = "ERROR"
                    self._runner._terminal_lifecycle_error_message = (
                        f"Circuit breaker tripped: {last_result.error or 'unknown'}"
                    )
                    self._runner._lifecycle_write_state(
                        strategy.deployment_id,
                        "ERROR",
                        error_message=self._runner._terminal_lifecycle_error_message,
                    )
                    logger.critical("Circuit breaker tripped in managed deployment — exiting process")
                    self._runner.request_shutdown()
        except Exception as e:
            logger.error(f"Failed to trigger emergency stop for {strategy.deployment_id}: {e}")
