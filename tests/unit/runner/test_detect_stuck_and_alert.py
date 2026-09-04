"""Branch characterization for the runner's stuck-detection alert adapter."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from almanak.framework.models import StuckReason
from almanak.framework.runner import runner_state
from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.services.operator_card_generator import OperatorCardGenerator
from almanak.framework.services.stuck_detector import StuckDetectionResult, StuckDetector

_DEFAULT = object()


def _detector(*, is_stuck: bool = True, reason: StuckReason | None = StuckReason.RPC_FAILURE) -> MagicMock:
    detector = MagicMock(spec=StuckDetector)
    detector.detect_stuck.return_value = StuckDetectionResult(
        is_stuck=is_stuck,
        reason=reason,
        time_in_state_seconds=601,
    )
    return detector


def _generator(card: object | None = None) -> MagicMock:
    generator = MagicMock(spec=OperatorCardGenerator)
    generator.generate_card.return_value = card or object()
    return generator


def _runner(
    *,
    enable_alerting: bool = True,
    alert_manager: object = _DEFAULT,
    detector: object = _DEFAULT,
    generator: object = _DEFAULT,
    first_error_at: datetime | None = None,
    circuit_breaker: object | None = None,
) -> SimpleNamespace:
    if alert_manager is _DEFAULT:
        alert_manager = SimpleNamespace(send_alert=AsyncMock())
    if detector is _DEFAULT:
        detector = _detector()
    if generator is _DEFAULT:
        generator = _generator()
    return SimpleNamespace(
        config=SimpleNamespace(enable_alerting=enable_alerting),
        alert_manager=alert_manager,
        _stuck_detector=detector,
        _operator_card_generator=generator,
        _first_error_at=first_error_at,
        _circuit_breaker=circuit_breaker,
        _query_portfolio_value=MagicMock(return_value=(Decimal("125.50"), Decimal("25.25"))),
    )


def _strategy(*, include_chain: bool = True) -> SimpleNamespace:
    values = {"deployment_id": "deployment:canonical"}
    if include_chain:
        values["chain"] = "arbitrum"
    return SimpleNamespace(**values)


def _result(*, error: str | None = "rpc unavailable", execution_result: object | None = None) -> IterationResult:
    return IterationResult(
        status=IterationStatus.EXECUTION_FAILED,
        error=error,
        execution_result=execution_result,
        deployment_id="result-id-is-not-alert-identity",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("enable_alerting", "alert_manager"),
    [(False, SimpleNamespace(send_alert=AsyncMock())), (True, None)],
)
async def test_disabled_alerting_has_no_detection_or_initialization_side_effects(
    enable_alerting: bool,
    alert_manager: object | None,
) -> None:
    runner = _runner(
        enable_alerting=enable_alerting,
        alert_manager=alert_manager,
        detector=None,
        generator=None,
    )

    with (
        patch("almanak.framework.services.stuck_detector.StuckDetector") as detector_type,
        patch("almanak.framework.services.operator_card_generator.OperatorCardGenerator") as generator_type,
    ):
        await runner_state.detect_stuck_and_alert(runner, _strategy(), _result())

    detector_type.assert_not_called()
    generator_type.assert_not_called()
    assert runner._stuck_detector is None
    assert runner._operator_card_generator is None
    if alert_manager is not None:
        alert_manager.send_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_lazy_components_are_reused_and_repeated_detections_delegate_to_alert_manager() -> None:
    detector = _detector()
    card = object()
    generator = _generator(card)
    runner = _runner(detector=None, generator=None)

    with (
        patch(
            "almanak.framework.services.stuck_detector.StuckDetector",
            return_value=detector,
        ) as detector_type,
        patch(
            "almanak.framework.services.operator_card_generator.OperatorCardGenerator",
            return_value=generator,
        ) as generator_type,
    ):
        await runner_state.detect_stuck_and_alert(runner, _strategy(), _result())
        await runner_state.detect_stuck_and_alert(runner, _strategy(), _result())

    detector_type.assert_called_once_with(emit_events=True)
    generator_type.assert_called_once_with()
    assert runner._stuck_detector is detector
    assert runner._operator_card_generator is generator
    assert detector.detect_stuck.call_count == 2
    assert generator.generate_card.call_count == 2
    assert runner.alert_manager.send_alert.await_args_list == [call(card), call(card)]


@pytest.mark.asyncio
@pytest.mark.parametrize(("seconds_in_error", "expected_alert"), [(599, False), (600, True)])
async def test_default_stuck_threshold_boundary_is_preserved(seconds_in_error: int, expected_alert: bool) -> None:
    detector = StuckDetector(emit_events=False)
    generator = _generator()
    runner = _runner(
        detector=detector,
        generator=generator,
        first_error_at=datetime.now(UTC) - timedelta(seconds=seconds_in_error),
    )

    await runner_state.detect_stuck_and_alert(runner, _strategy(), _result())

    assert generator.generate_card.called is expected_alert
    assert (runner.alert_manager.send_alert.await_count == 1) is expected_alert


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("circuit_breaker", "triggered"),
    [
        (None, False),
        (SimpleNamespace(state=SimpleNamespace(value="closed")), False),
        (SimpleNamespace(state=SimpleNamespace(value="half_open")), True),
    ],
)
async def test_detection_snapshot_preserves_circuit_breaker_state_and_chain_fallback(
    circuit_breaker: object | None,
    triggered: bool,
) -> None:
    detector = _detector(is_stuck=False)
    runner = _runner(detector=detector, circuit_breaker=circuit_breaker)

    await runner_state.detect_stuck_and_alert(runner, _strategy(include_chain=False), _result())

    snapshot = detector.detect_stuck.call_args.args[0]
    assert snapshot.deployment_id == "deployment:canonical"
    assert snapshot.chain == "unknown"
    assert snapshot.current_state == IterationStatus.EXECUTION_FAILED.value
    assert snapshot.circuit_breaker_triggered is triggered
    runner._operator_card_generator.generate_card.assert_not_called()
    runner.alert_manager.send_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_stuck_alert_payload_preserves_deployment_state_error_and_portfolio_values() -> None:
    entered_at = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    detector = _detector(reason=None)
    card = object()
    generator = _generator(card)
    runner = _runner(detector=detector, generator=generator, first_error_at=entered_at)
    log = MagicMock()

    with patch.object(runner_state, "logger", log):
        await runner_state.detect_stuck_and_alert(runner, _strategy(), _result(error=None))

    snapshot = detector.detect_stuck.call_args.args[0]
    assert snapshot.deployment_id == "deployment:canonical"
    assert snapshot.state_entered_at == entered_at
    payload = generator.generate_card.call_args.kwargs
    assert payload["strategy_state"].deployment_id == "deployment:canonical"
    assert payload["strategy_state"].status == "stuck"
    assert payload["strategy_state"].total_value_usd == Decimal("125.50")
    assert payload["strategy_state"].available_balance_usd == Decimal("25.25")
    assert payload["strategy_state"].stuck_since == entered_at
    assert payload["error_context"].error_type == IterationStatus.EXECUTION_FAILED.value
    assert payload["error_context"].error_message == "unknown"
    runner.alert_manager.send_alert.assert_awaited_once_with(card)
    log.warning.assert_called_once_with(
        "StuckDetector: %s is stuck (reason=%s, duration=%.0fs)",
        "deployment:canonical",
        "unknown",
        601,
    )


@pytest.mark.asyncio
async def test_execution_hash_is_not_fabricated_into_pending_transaction_context() -> None:
    detector = _detector()
    generator = _generator()
    runner = _runner(detector=detector, generator=generator)
    execution_result = SimpleNamespace(tx_hashes=["0xknown-but-not-proven-pending"])

    await runner_state.detect_stuck_and_alert(
        runner,
        _strategy(),
        _result(execution_result=execution_result),
    )

    snapshot = detector.detect_stuck.call_args.args[0]
    payload = generator.generate_card.call_args.kwargs
    assert snapshot.pending_transactions == []
    assert payload["strategy_state"].pending_tx_hash is None
    assert payload["error_context"].tx_hash is None


@pytest.mark.asyncio
async def test_manager_removed_during_card_generation_is_not_used() -> None:
    alert_manager = SimpleNamespace(send_alert=AsyncMock())
    generator = _generator()
    runner = _runner(alert_manager=alert_manager, generator=generator)

    def remove_manager(**_kwargs: object) -> object:
        runner.alert_manager = None
        return object()

    generator.generate_card.side_effect = remove_manager

    await runner_state.detect_stuck_and_alert(runner, _strategy(), _result())

    alert_manager.send_alert.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["detection", "portfolio", "generation"])
async def test_detection_and_card_failures_are_non_fatal_and_logged(failure_stage: str) -> None:
    detector = _detector()
    generator = _generator()
    runner = _runner(detector=detector, generator=generator)
    error = RuntimeError(f"{failure_stage} failed")
    if failure_stage == "detection":
        detector.detect_stuck.side_effect = error
    elif failure_stage == "portfolio":
        runner._query_portfolio_value.side_effect = error
    else:
        generator.generate_card.side_effect = error
    log = MagicMock()

    with patch.object(runner_state, "logger", log):
        await runner_state.detect_stuck_and_alert(runner, _strategy(), _result())

    runner.alert_manager.send_alert.assert_not_awaited()
    log.debug.assert_called_once_with("Stuck detection failed (non-fatal): %s", error)


@pytest.mark.asyncio
async def test_alert_delivery_failure_is_non_fatal_and_uses_specific_log() -> None:
    error = RuntimeError("delivery failed")
    alert_manager = SimpleNamespace(send_alert=AsyncMock(side_effect=error))
    runner = _runner(alert_manager=alert_manager)
    log = MagicMock()

    with patch.object(runner_state, "logger", log):
        await runner_state.detect_stuck_and_alert(runner, _strategy(), _result())

    alert_manager.send_alert.assert_awaited_once()
    log.debug.assert_called_once_with("Failed to send stuck alert (non-fatal): %s", error)
