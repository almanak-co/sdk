from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.core.lifecycle import LifecycleState
from almanak.framework.api.timeline import TimelineEventType
from almanak.framework.runner import _run_loop_helpers as helpers
from almanak.framework.runner.boot_strand_detection import OnChainStrandError
from almanak.framework.runner.strategy_runner import StrategyRunner


def _runner(*, persistence: bool = True, live: bool = False, gateway: object | None = None) -> SimpleNamespace:
    state_manager = SimpleNamespace(
        initialize=AsyncMock(),
        load_state=AsyncMock(return_value=None),
    )
    return SimpleNamespace(
        config=SimpleNamespace(
            enable_state_persistence=persistence,
            chain="arbitrum",
            default_interval_seconds=17,
        ),
        state_manager=state_manager,
        _is_live_mode=MagicMock(return_value=live),
        _recover_incomplete_sessions=AsyncMock(return_value=0),
        _get_gateway_client=MagicMock(return_value=gateway),
        _register_with_gateway=MagicMock(),
        _lifecycle_write_state=MagicMock(),
        _shutdown_requested=True,
        _signal_received=True,
        _terminal_lifecycle_state=LifecycleState.ERROR,
        _terminal_lifecycle_error_message="old error",
    )


def _strategy(*, activity_provider: object | None = None, deployment_id: str = "strategy-copy") -> SimpleNamespace:
    return SimpleNamespace(
        deployment_id=deployment_id,
        _wallet_activity_provider=activity_provider,
    )


@pytest.mark.asyncio
async def test_state_manager_stage_skips_disabled_persistence() -> None:
    runner = _runner(persistence=False)

    assert await helpers._initialize_run_loop_state_manager(runner, "deployment:boot") is False
    runner.state_manager.initialize.assert_not_awaited()


@pytest.mark.asyncio
async def test_state_manager_stage_returns_ready_after_initialization() -> None:
    runner = _runner()

    assert await helpers._initialize_run_loop_state_manager(runner, "deployment:boot") is True
    runner.state_manager.initialize.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("live", [False, True])
async def test_state_manager_stage_preserves_mode_aware_failure_policy(
    live: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _runner(live=live)
    failure = OSError("backend unavailable")
    runner.state_manager.initialize.side_effect = failure

    if live:
        with pytest.raises(RuntimeError, match="Failed to initialize state manager") as raised:
            await helpers._initialize_run_loop_state_manager(runner, "deployment:boot")
        assert raised.value.__cause__ is failure
    else:
        with caplog.at_level(logging.ERROR):
            assert await helpers._initialize_run_loop_state_manager(runner, "deployment:boot") is False
        assert "Failed to initialize state manager: backend unavailable" in caplog.text


@pytest.mark.asyncio
async def test_persisted_state_stage_preserves_recovery_order_and_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner()
    strategy = _strategy()
    calls: list[tuple[str, str]] = []

    async def capture(_runner: object, _strategy: object, deployment_id: str) -> None:
        calls.append(("capture", deployment_id))

    def reconstruct(_runner: object, _strategy: object, deployment_id: str) -> None:
        calls.append(("reconstruct", deployment_id))

    async def hydrate(_runner: object, _strategy: object) -> None:
        calls.append(("hydrate", ""))

    async def cutover(_runner: object, _strategy: object, deployment_id: str) -> None:
        calls.append(("cutover", deployment_id))

    def install(_runner: object, deployment_id: str) -> None:
        calls.append(("preflight", deployment_id))

    def sweep(_runner: object, deployment_id: str) -> None:
        calls.append(("sweep", deployment_id))

    async def drain(_runner: object, deployment_id: str) -> None:
        calls.append(("drain", deployment_id))

    monkeypatch.setattr(helpers, "capture_boot_snapshot_with_accounting", capture)
    monkeypatch.setattr(helpers, "reconstruct_lending_basis_store", reconstruct)
    monkeypatch.setattr(helpers, "hydrate_recent_open_events_cache", hydrate)
    monkeypatch.setattr(helpers, "_run_cutover_boot_guard", cutover)
    monkeypatch.setattr(helpers, "_install_registry_preflight", install)
    monkeypatch.setattr(helpers, "_sweep_stale_executing_teardowns", sweep)
    monkeypatch.setattr(helpers, "_drain_pending_accounting_outbox", drain)

    await helpers._recover_persisted_run_state(
        runner,
        strategy,
        "deployment:boot",
        state_manager_ready=True,
    )

    assert calls == [
        ("capture", "deployment:boot"),
        ("reconstruct", "deployment:boot"),
        ("hydrate", ""),
        ("cutover", "deployment:boot"),
        ("preflight", "deployment:boot"),
        ("sweep", "deployment:boot"),
        ("drain", "deployment:boot"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("persistence", "state_manager_ready"),
    [(True, False), (False, False), (False, True)],
)
async def test_persisted_state_stage_gates_backend_recovery_but_always_sweeps(
    persistence: bool,
    state_manager_ready: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(persistence=persistence)
    strategy = _strategy()
    capture = AsyncMock()
    reconstruct = MagicMock()
    hydrate = AsyncMock()
    cutover = AsyncMock()
    install = MagicMock()
    sweep = MagicMock()
    drain = AsyncMock()
    monkeypatch.setattr(helpers, "capture_boot_snapshot_with_accounting", capture)
    monkeypatch.setattr(helpers, "reconstruct_lending_basis_store", reconstruct)
    monkeypatch.setattr(helpers, "hydrate_recent_open_events_cache", hydrate)
    monkeypatch.setattr(helpers, "_run_cutover_boot_guard", cutover)
    monkeypatch.setattr(helpers, "_install_registry_preflight", install)
    monkeypatch.setattr(helpers, "_sweep_stale_executing_teardowns", sweep)
    monkeypatch.setattr(helpers, "_drain_pending_accounting_outbox", drain)

    await helpers._recover_persisted_run_state(
        runner,
        strategy,
        "deployment:boot",
        state_manager_ready=state_manager_ready,
    )

    sweep.assert_called_once_with(runner, "deployment:boot")
    if state_manager_ready:
        capture.assert_awaited_once()
        reconstruct.assert_called_once()
        hydrate.assert_awaited_once()
        cutover.assert_awaited_once()
        install.assert_called_once()
    else:
        capture.assert_not_awaited()
        reconstruct.assert_not_called()
        hydrate.assert_not_awaited()
        cutover.assert_not_awaited()
        install.assert_not_called()
    drain.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drained", [0, 2])
async def test_outbox_drain_uses_immutable_boot_identity(drained: int) -> None:
    runner = _runner()
    processor = SimpleNamespace(_deployment_id="", drain_pending=AsyncMock(return_value=drained))
    runner._accounting_processor = processor

    await helpers._drain_pending_accounting_outbox(runner, "deployment:boot")

    assert processor._deployment_id == "deployment:boot"
    processor.drain_pending.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_outbox_drain_is_noop_without_processor() -> None:
    await helpers._drain_pending_accounting_outbox(_runner(), "deployment:boot")


@pytest.mark.asyncio
@pytest.mark.parametrize("live", [False, True])
async def test_outbox_drain_preserves_mode_aware_failure_policy(
    live: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _runner(live=live)
    failure = OSError("outbox unavailable")
    runner._accounting_processor = SimpleNamespace(
        _deployment_id="",
        drain_pending=AsyncMock(side_effect=failure),
    )

    if live:
        with pytest.raises(RuntimeError, match=r"AccountingProcessor\.drain_pending failed") as raised:
            await helpers._drain_pending_accounting_outbox(runner, "deployment:boot")
        assert raised.value.__cause__ is failure
    else:
        with caplog.at_level(logging.WARNING):
            await helpers._drain_pending_accounting_outbox(runner, "deployment:boot")
        assert "AccountingProcessor.drain_pending failed on startup" in caplog.text


@pytest.mark.asyncio
async def test_control_state_stage_recovers_sessions_copy_cursor_and_resets_flags() -> None:
    provider = SimpleNamespace(set_state=MagicMock())
    runner = _runner()
    runner._recover_incomplete_sessions.return_value = 3
    runner.state_manager.load_state.return_value = SimpleNamespace(
        state={"copy_trading_state": {"cursor": 7}},
    )

    result = await helpers._recover_run_loop_control_state(
        runner,
        _strategy(activity_provider=provider),
        "deployment:boot",
    )

    assert result is provider
    provider.set_state.assert_called_once_with({"cursor": 7})
    runner.state_manager.load_state.assert_awaited_once_with("deployment:boot")
    assert runner._shutdown_requested is False
    assert runner._signal_received is False
    assert runner._terminal_lifecycle_state is None
    assert runner._terminal_lifecycle_error_message is None


@pytest.mark.asyncio
async def test_control_state_stage_logs_recovery_failures_and_still_resets(
    caplog: pytest.LogCaptureFixture,
) -> None:
    provider = SimpleNamespace(set_state=MagicMock())
    runner = _runner()
    runner._recover_incomplete_sessions.side_effect = RuntimeError("session read failed")
    runner.state_manager.load_state.side_effect = RuntimeError("cursor read failed")

    with caplog.at_level(logging.WARNING):
        result = await helpers._recover_run_loop_control_state(
            runner,
            _strategy(activity_provider=provider),
            "deployment:boot",
        )

    assert result is provider
    assert "Failed to recover incomplete sessions: session read failed" in caplog.text
    assert "Failed to restore copy trading state: cursor read failed" in caplog.text
    provider.set_state.assert_not_called()
    assert runner._shutdown_requested is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("persistence", "provider", "state", "expected_load"),
    [
        (True, None, None, False),
        (False, SimpleNamespace(set_state=MagicMock()), None, False),
        (True, SimpleNamespace(set_state=MagicMock()), None, True),
        (True, SimpleNamespace(set_state=MagicMock()), SimpleNamespace(state={"other": 1}), True),
    ],
)
async def test_control_state_stage_copy_restore_gates(
    persistence: bool,
    provider: object | None,
    state: object | None,
    expected_load: bool,
) -> None:
    runner = _runner(persistence=persistence)
    runner.state_manager.load_state.return_value = state

    result = await helpers._recover_run_loop_control_state(
        runner,
        _strategy(activity_provider=provider),
        "deployment:boot",
    )

    assert result is provider
    assert runner.state_manager.load_state.await_count == int(expected_load)
    if provider is not None:
        provider.set_state.assert_not_called()


@pytest.mark.asyncio
async def test_gateway_stage_skips_gateway_only_work_without_client() -> None:
    runner = _runner(gateway=None)
    strategy = _strategy()

    await helpers._configure_run_loop_gateway(
        runner,
        strategy,
        "deployment:boot",
        state_manager_ready=True,
    )

    runner.state_manager.load_state.assert_not_awaited()
    runner._register_with_gateway.assert_called_once_with(strategy)


@pytest.mark.asyncio
async def test_gateway_stage_preserves_wiring_guard_resume_registration_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = object()
    runner = _runner(gateway=gateway)
    strategy = _strategy()
    calls: list[tuple[str, object]] = []

    def set_gateway(client: object) -> None:
        calls.append(("timeline", client))

    async def enforce(_runner: object, _strategy: object, deployment_id: str) -> None:
        calls.append(("strand", deployment_id))

    async def load_state(deployment_id: str) -> object:
        calls.append(("load", deployment_id))
        return object()

    def warn(_runner: object, _strategy: object, deployment_id: str, *, is_resume: bool) -> None:
        calls.append(("resume", (deployment_id, is_resume)))

    def register(_strategy: object) -> None:
        calls.append(("register", _strategy))

    runner.state_manager.load_state.side_effect = load_state
    runner._register_with_gateway.side_effect = register
    monkeypatch.setattr("almanak.framework.api.timeline.set_event_gateway_client", set_gateway)
    monkeypatch.setattr("almanak.framework.runner.boot_strand_detection.enforce_no_boot_strands", enforce)
    monkeypatch.setattr("almanak.framework.runner.resume_terminal_guard.warn_on_resume_into_terminal", warn)

    await helpers._configure_run_loop_gateway(
        runner,
        strategy,
        "deployment:boot",
        state_manager_ready=True,
    )

    assert calls == [
        ("timeline", gateway),
        ("strand", "deployment:boot"),
        ("load", "deployment:boot"),
        ("resume", ("deployment:boot", True)),
        ("register", strategy),
    ]


@pytest.mark.asyncio
async def test_gateway_stage_without_ready_state_skips_strand_and_marks_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(gateway=object())
    strategy = _strategy()
    strand = AsyncMock()
    resume = MagicMock()
    monkeypatch.setattr("almanak.framework.api.timeline.set_event_gateway_client", MagicMock())
    monkeypatch.setattr("almanak.framework.runner.boot_strand_detection.enforce_no_boot_strands", strand)
    monkeypatch.setattr("almanak.framework.runner.resume_terminal_guard.warn_on_resume_into_terminal", resume)

    await helpers._configure_run_loop_gateway(
        runner,
        strategy,
        "deployment:boot",
        state_manager_ready=False,
    )

    strand.assert_not_awaited()
    runner.state_manager.load_state.assert_not_awaited()
    resume.assert_called_once_with(runner, strategy, "deployment:boot", is_resume=False)


@pytest.mark.asyncio
async def test_gateway_stage_propagates_typed_strand_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(gateway=object())
    error = OnChainStrandError("unaccounted position")
    monkeypatch.setattr("almanak.framework.api.timeline.set_event_gateway_client", MagicMock())
    monkeypatch.setattr(
        "almanak.framework.runner.boot_strand_detection.enforce_no_boot_strands",
        AsyncMock(side_effect=error),
    )
    resume = MagicMock()
    monkeypatch.setattr("almanak.framework.runner.resume_terminal_guard.warn_on_resume_into_terminal", resume)

    with pytest.raises(OnChainStrandError) as raised:
        await helpers._configure_run_loop_gateway(
            runner,
            _strategy(),
            "deployment:boot",
            state_manager_ready=True,
        )

    assert raised.value is error
    resume.assert_not_called()
    runner._register_with_gateway.assert_not_called()


@pytest.mark.asyncio
async def test_gateway_stage_logs_unexpected_strand_error_and_continues(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _runner(gateway=object())
    strategy = _strategy()
    monkeypatch.setattr("almanak.framework.api.timeline.set_event_gateway_client", MagicMock())
    monkeypatch.setattr(
        "almanak.framework.runner.boot_strand_detection.enforce_no_boot_strands",
        AsyncMock(side_effect=RuntimeError("scan failed")),
    )
    resume = MagicMock()
    monkeypatch.setattr("almanak.framework.runner.resume_terminal_guard.warn_on_resume_into_terminal", resume)

    with caplog.at_level(logging.WARNING):
        await helpers._configure_run_loop_gateway(
            runner,
            strategy,
            "deployment:boot",
            state_manager_ready=True,
        )

    assert "Boot strand detection skipped (unexpected error): scan failed" in caplog.text
    resume.assert_called_once_with(runner, strategy, "deployment:boot", is_resume=False)
    runner._register_with_gateway.assert_called_once_with(strategy)


@pytest.mark.asyncio
async def test_gateway_stage_treats_resume_state_read_failure_as_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(gateway=object())
    runner.state_manager.load_state.side_effect = RuntimeError("state read failed")
    strategy = _strategy()
    monkeypatch.setattr("almanak.framework.api.timeline.set_event_gateway_client", MagicMock())
    monkeypatch.setattr(
        "almanak.framework.runner.boot_strand_detection.enforce_no_boot_strands",
        AsyncMock(),
    )
    resume = MagicMock()
    monkeypatch.setattr("almanak.framework.runner.resume_terminal_guard.warn_on_resume_into_terminal", resume)

    await helpers._configure_run_loop_gateway(
        runner,
        strategy,
        "deployment:boot",
        state_manager_ready=True,
    )

    resume.assert_called_once_with(runner, strategy, "deployment:boot", is_resume=False)
    runner._register_with_gateway.assert_called_once_with(strategy)


@pytest.mark.asyncio
async def test_gateway_stage_logs_resume_advisory_failure_and_registers(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _runner(gateway=object())
    strategy = _strategy()
    monkeypatch.setattr("almanak.framework.api.timeline.set_event_gateway_client", MagicMock())
    monkeypatch.setattr(
        "almanak.framework.runner.boot_strand_detection.enforce_no_boot_strands",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "almanak.framework.runner.resume_terminal_guard.warn_on_resume_into_terminal",
        MagicMock(side_effect=RuntimeError("advisory failed")),
    )

    with caplog.at_level(logging.WARNING):
        await helpers._configure_run_loop_gateway(
            runner,
            strategy,
            "deployment:boot",
            state_manager_ready=True,
        )

    assert "Resume-into-terminal guard skipped (unexpected error): advisory failed" in caplog.text
    runner._register_with_gateway.assert_called_once_with(strategy)


def test_announce_stage_writes_lifecycle_before_exact_timeline_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner()
    calls: list[tuple[str, object]] = []
    runner._lifecycle_write_state.side_effect = lambda *args: calls.append(("lifecycle", args))
    monkeypatch.setattr(helpers, "add_event", lambda event: calls.append(("event", event)))

    helpers._announce_run_loop_started(runner, "deployment:boot", 23)

    assert calls[0] == ("lifecycle", ("deployment:boot", LifecycleState.RUNNING))
    event = calls[1][1]
    assert event.event_type is TimelineEventType.STRATEGY_STARTED
    assert event.deployment_id == "deployment:boot"
    assert event.chain == "arbitrum"
    assert event.description == "Strategy deployment:boot started with interval=23s"
    assert event.details == {"interval_seconds": 23, "enable_state_persistence": True}


@pytest.mark.asyncio
async def test_initialize_run_loop_runs_extracted_stages_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner()
    strategy = _strategy()
    provider = object()
    calls: list[tuple[str, object]] = []

    async def initialize(_runner: object, deployment_id: str) -> bool:
        calls.append(("initialize", deployment_id))
        return True

    async def recover_persisted(*args: object, state_manager_ready: bool) -> None:
        calls.append(("persisted", (args, state_manager_ready)))

    async def recover_control(*args: object) -> object:
        calls.append(("control", args))
        return provider

    async def configure_gateway(*args: object, state_manager_ready: bool) -> None:
        calls.append(("gateway", (args, state_manager_ready)))

    def announce(*args: object) -> None:
        calls.append(("announce", args))

    monkeypatch.setattr(helpers, "_initialize_run_loop_state_manager", initialize)
    monkeypatch.setattr(helpers, "_recover_persisted_run_state", recover_persisted)
    monkeypatch.setattr(helpers, "_recover_run_loop_control_state", recover_control)
    monkeypatch.setattr(helpers, "_configure_run_loop_gateway", configure_gateway)
    monkeypatch.setattr(helpers, "_announce_run_loop_started", announce)

    result = await helpers.initialize_run_loop(runner, strategy, "deployment:boot", 23)

    assert result is provider
    assert [name for name, _ in calls] == ["initialize", "persisted", "control", "gateway", "announce"]
    assert calls[1][1][1] is True
    assert calls[3][1][1] is True
    assert calls[4][1] == (runner, "deployment:boot", 23)


@pytest.mark.asyncio
async def test_public_run_loop_enters_initialization_before_shutdown_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(persistence=False)
    strategy = _strategy(deployment_id="deployment:public")
    calls: list[tuple[str, object]] = []

    async def initialize(
        passed_runner: object,
        passed_strategy: object,
        deployment_id: str,
        interval: int,
    ) -> None:
        calls.append(("initialize", (passed_runner, passed_strategy, deployment_id, interval)))
        passed_runner._shutdown_requested = True

    async def finalize(passed_runner: object, passed_strategy: object, deployment_id: str) -> None:
        calls.append(("finalize", (passed_runner, passed_strategy, deployment_id)))

    runner._shutdown_requested = False
    monkeypatch.setattr(helpers, "initialize_run_loop", initialize)
    monkeypatch.setattr(helpers, "finalize_run_loop", finalize)

    await StrategyRunner.run_loop(runner, strategy, interval_seconds=0)

    assert calls == [
        ("initialize", (runner, strategy, "deployment:public", 0)),
        ("finalize", (runner, strategy, "deployment:public")),
    ]
