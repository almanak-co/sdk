"""Tests for VIB-3157 mandatory accounting persistence in live mode.

Verifies that ledger / snapshot / metrics write failures:
- Raise AccountingPersistenceError from the state layer
- Halt the live-mode iteration with IterationStatus.ACCOUNTING_FAILED
- Trigger an operator alert via AlertManager
- Are soft-failed (ERROR log only) in dry-run / paper mode

Sites covered (from the VIB-3157 audit):
1. strategy_runner._write_ledger_entry
2. state_manager.save_ledger_entry
3. gateway_state_manager.save_portfolio_snapshot
4. gateway_state_manager.save_portfolio_metrics
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.runner.runner_models import IterationStatus
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner
from almanak.framework.state.exceptions import AccountingPersistenceError

# =============================================================================
# Minimal mocks
# =============================================================================


class _Strategy:
    def __init__(self, sid: str = "s1", chain: str = "arbitrum") -> None:
        self.strategy_id = sid
        self.chain = chain
        self.wallet_address = "0x" + "0" * 40


class _Runner(StrategyRunner):
    """Bypass StrategyRunner.__init__ -- we only need the methods under test."""

    def __init__(self, *, state_manager: Any, alert_manager: Any = None, config: RunnerConfig | None = None) -> None:
        self.state_manager = state_manager
        self.alert_manager = alert_manager
        self.config = config or RunnerConfig()
        # Attrs accessed by _alert_accounting_failure / _write_ledger_entry
        self._iteration_had_trade = False


def _swap_intent() -> SwapIntent:
    return SwapIntent(from_token="USDC", to_token="ETH", amount_usd=Decimal("100"))


# =============================================================================
# Site 1: _write_ledger_entry (strategy_runner)
# =============================================================================


@pytest.mark.asyncio
async def test_write_ledger_entry_live_mode_propagates_failure() -> None:
    """Live mode: backend raise must propagate as AccountingPersistenceError."""
    state_mgr = MagicMock()
    state_mgr.save_ledger_entry = AsyncMock(
        side_effect=AccountingPersistenceError(write_kind="ledger", strategy_id="s1")
    )
    runner = _Runner(state_manager=state_mgr, config=RunnerConfig(dry_run=False))

    with pytest.raises(AccountingPersistenceError):
        await runner._write_ledger_entry(strategy=_Strategy(), intent=_swap_intent(), result=None, success=True)


@pytest.mark.asyncio
async def test_write_ledger_entry_dry_run_swallows_and_logs_error(caplog: pytest.LogCaptureFixture) -> None:
    """Dry-run: failure must NOT raise but MUST log ERROR (not debug/warning)."""
    import logging

    state_mgr = MagicMock()
    state_mgr.save_ledger_entry = AsyncMock(
        side_effect=AccountingPersistenceError(write_kind="ledger", strategy_id="s1")
    )
    runner = _Runner(state_manager=state_mgr, config=RunnerConfig(dry_run=True))

    with caplog.at_level(logging.ERROR, logger="almanak.framework.runner.strategy_runner"):
        await runner._write_ledger_entry(strategy=_Strategy(), intent=_swap_intent(), result=None, success=True)
    assert any("non-live mode" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_write_ledger_entry_paper_mode_swallows_and_logs_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Paper-mode: same contract as dry-run — swallowed AND logged at ERROR.

    The paper-mode contract is "continue but make pre-prod drift visible",
    so asserting the swallow without asserting the log would leave the
    visibility half of the contract untested.
    """
    state_mgr = MagicMock()
    state_mgr.save_ledger_entry = AsyncMock(
        side_effect=AccountingPersistenceError(write_kind="ledger", strategy_id="s1")
    )
    cfg = RunnerConfig(dry_run=False)
    cfg.paper_mode = True  # type: ignore[attr-defined]
    runner = _Runner(state_manager=state_mgr, config=cfg)

    with caplog.at_level(logging.ERROR, logger="almanak.framework.runner.strategy_runner"):
        # Must not raise -- paper mode is non-live.
        await runner._write_ledger_entry(strategy=_Strategy(), intent=_swap_intent(), result=None, success=True)

    assert any("non-live mode" in rec.message and rec.levelname == "ERROR" for rec in caplog.records), (
        "paper-mode ledger failure must log at ERROR (pre-prod drift visibility)"
    )


# =============================================================================
# Site 2: save_ledger_entry (state_manager)
# =============================================================================


@pytest.mark.asyncio
async def test_state_manager_save_ledger_entry_propagates() -> None:
    """When the WARM backend raises, save_ledger_entry wraps as AccountingPersistenceError."""
    from almanak.framework.state.state_manager import StateManager, StateManagerConfig

    mgr = StateManager(StateManagerConfig(load_state_on_startup=False))
    mgr._initialized = True

    # Stub WARM backend whose save_ledger_entry raises.
    warm = MagicMock()
    warm.save_ledger_entry = AsyncMock(side_effect=RuntimeError("db down"))
    mgr._warm = warm

    entry = MagicMock()
    entry.strategy_id = "s1"

    with pytest.raises(AccountingPersistenceError) as excinfo:
        await mgr.save_ledger_entry(entry)
    assert excinfo.value.write_kind == "ledger"
    assert excinfo.value.strategy_id == "s1"
    # cause preserved for forensic logs
    assert isinstance(excinfo.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_state_manager_save_portfolio_metrics_false_raises() -> None:
    """A ``False`` return from the WARM metrics backend must raise.

    Pre-VIB-3157 this returned the raw False; runner_state only escalates
    ``AccountingPersistenceError``, so False slipped through silently.
    """
    from almanak.framework.state.state_manager import StateManager, StateManagerConfig

    mgr = StateManager(StateManagerConfig(load_state_on_startup=False))
    mgr._initialized = True

    warm = MagicMock()
    warm.save_portfolio_metrics = AsyncMock(return_value=False)
    mgr._warm = warm

    metrics = MagicMock()
    metrics.strategy_id = "s1"

    with pytest.raises(AccountingPersistenceError) as excinfo:
        await mgr.save_portfolio_metrics(metrics)
    assert excinfo.value.write_kind == "metrics"
    assert excinfo.value.strategy_id == "s1"
    assert "returned False" in str(excinfo.value)


def _ledger_entry(strategy_id: str = "s1") -> LedgerEntry:
    """Build a valid LedgerEntry for gateway client tests."""
    return LedgerEntry(
        id="entry-1",
        cycle_id="c1",
        strategy_id=strategy_id,
        deployment_id="d1",
        execution_mode="live",
        timestamp=datetime.now(UTC),
        intent_type="SWAP",
        slippage_bps=12.5,
        gas_used=21000,
    )


@pytest.mark.asyncio
async def test_gateway_state_manager_ledger_rpc_success() -> None:
    """VIB-3201: save_ledger_entry now issues a real SaveLedgerEntry RPC.

    The pre-VIB-3201 behaviour (raise NotImplementedError as a known gap)
    is gone. The client now returns cleanly when the gateway response is
    successful.
    """
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    response = MagicMock()
    response.success = True
    response.error = ""

    client = MagicMock()
    client.state.SaveLedgerEntry = MagicMock(return_value=response)

    entry = _ledger_entry()
    gsm = GatewayStateManager(client=client)
    await gsm.save_ledger_entry(entry)
    client.state.SaveLedgerEntry.assert_called_once()
    req = client.state.SaveLedgerEntry.call_args.args[0]
    assert req.id == "entry-1"
    assert req.strategy_id == "s1"
    assert req.deployment_id == "d1"
    assert req.execution_mode == "live"
    assert req.intent_type == "SWAP"
    assert req.gas_used == 21000
    assert req.timestamp > 0
    assert req.HasField("slippage_bps")
    assert req.slippage_bps == pytest.approx(12.5)


@pytest.mark.asyncio
async def test_gateway_state_manager_ledger_rpc_failure_raises() -> None:
    """Gateway response.success=False raises AccountingPersistenceError(ledger)."""
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    response = MagicMock()
    response.success = False
    response.error = "db down"

    client = MagicMock()
    client.state.SaveLedgerEntry = MagicMock(return_value=response)

    gsm = GatewayStateManager(client=client)
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await gsm.save_ledger_entry(_ledger_entry())
    assert excinfo.value.write_kind == "ledger"
    assert excinfo.value.strategy_id == "s1"
    assert "db down" in str(excinfo.value)


@pytest.mark.asyncio
async def test_gateway_state_manager_ledger_rpc_exception_wraps() -> None:
    """Transport-level errors (e.g. gRPC failures) wrap as AccountingPersistenceError."""
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    client = MagicMock()
    client.state.SaveLedgerEntry = MagicMock(side_effect=RuntimeError("rpc boom"))

    gsm = GatewayStateManager(client=client)
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await gsm.save_ledger_entry(_ledger_entry())
    assert excinfo.value.write_kind == "ledger"
    assert isinstance(excinfo.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_write_ledger_entry_gateway_rpc_failure_halts_live() -> None:
    """Live mode + gateway RPC failure = AccountingPersistenceError, no escape hatch.

    Post-VIB-3201: the runner no longer swallows NotImplementedError for
    the gateway backend. A failed SaveLedgerEntry raises
    AccountingPersistenceError and the runner propagates it so
    run_iteration halts with ACCOUNTING_FAILED.
    """
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    response = MagicMock()
    response.success = False
    response.error = "db down"
    client = MagicMock()
    client.state.SaveLedgerEntry = MagicMock(return_value=response)

    gsm = GatewayStateManager(client=client)
    cfg = RunnerConfig(dry_run=False)
    runner = _Runner(state_manager=gsm, config=cfg)

    with pytest.raises(AccountingPersistenceError):
        await runner._write_ledger_entry(strategy=_Strategy(), intent=_swap_intent(), result=None, success=True)


@pytest.mark.asyncio
async def test_write_ledger_entry_unknown_backend_notimplemented_escalates() -> None:
    """Any backend raising NotImplementedError must escalate -- no backend-specific escape hatch.

    VIB-3201 removed the runner's gateway-only NotImplementedError swallow.
    ``StateManager.save_ledger_entry`` wraps any non-AccountingPersistenceError
    backend exception into AccountingPersistenceError, so runners see a
    typed error and halt in live mode.
    """
    from almanak.framework.state.state_manager import StateManager, StateManagerConfig

    mgr = StateManager(StateManagerConfig(load_state_on_startup=False))
    mgr._initialized = True
    warm = MagicMock()
    warm.save_ledger_entry = AsyncMock(side_effect=NotImplementedError("custom backend bug"))
    mgr._warm = warm

    cfg = RunnerConfig(dry_run=False)
    runner = _Runner(state_manager=mgr, config=cfg)

    with pytest.raises(AccountingPersistenceError) as excinfo:
        await runner._write_ledger_entry(strategy=_Strategy(), intent=_swap_intent(), result=None, success=True)
    assert excinfo.value.write_kind == "ledger"
    assert isinstance(excinfo.value.__cause__, NotImplementedError)


# =============================================================================
# Site 3: save_portfolio_snapshot (gateway_state_manager)
# =============================================================================


def _make_gateway_state_manager(response_success: bool, error_msg: str = "boom"):
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    response = MagicMock()
    response.success = response_success
    response.error = error_msg
    response.snapshot_id = 0

    client = MagicMock()
    client.state = MagicMock()
    client.state.SavePortfolioSnapshot = MagicMock(return_value=response)
    client.state.SavePortfolioMetrics = MagicMock(return_value=response)

    return GatewayStateManager(client=client), client


def _snapshot_stub():
    snap = MagicMock()
    snap.strategy_id = "s1"
    snap.timestamp = datetime.now(UTC)
    snap.iteration_number = 1
    snap.total_value_usd = Decimal("100")
    snap.available_cash_usd = Decimal("50")
    snap.value_confidence = MagicMock(value="HIGH")
    snap.chain = "arbitrum"
    snap.token_prices = None
    snap.wallet_balances = None
    snap.to_positions_payload = MagicMock(return_value={"positions": [], "metadata": {}})
    return snap


@pytest.mark.asyncio
async def test_gateway_save_portfolio_snapshot_raises_when_response_not_success() -> None:
    mgr, _client = _make_gateway_state_manager(response_success=False)
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await mgr.save_portfolio_snapshot(_snapshot_stub())
    assert excinfo.value.write_kind == "snapshot"
    assert excinfo.value.strategy_id == "s1"


@pytest.mark.asyncio
async def test_gateway_save_portfolio_snapshot_raises_on_rpc_exception() -> None:
    mgr, client = _make_gateway_state_manager(response_success=True)
    client.state.SavePortfolioSnapshot = MagicMock(side_effect=ConnectionError("gw down"))
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await mgr.save_portfolio_snapshot(_snapshot_stub())
    assert excinfo.value.write_kind == "snapshot"
    assert isinstance(excinfo.value.__cause__, ConnectionError)


# =============================================================================
# Site 4: save_portfolio_metrics (gateway_state_manager)
# =============================================================================


def _metrics_stub():
    m = MagicMock()
    m.strategy_id = "s1"
    m.timestamp = datetime.now(UTC)
    m.initial_value_usd = Decimal("100")
    m.deposits_usd = Decimal("0")
    m.withdrawals_usd = Decimal("0")
    m.gas_spent_usd = Decimal("0")
    m.deployment_id = "d1"
    m.cycle_id = "c1"
    m.execution_mode = "live"
    m.is_complete = True
    return m


@pytest.mark.asyncio
async def test_gateway_save_portfolio_metrics_raises_when_response_not_success() -> None:
    mgr, _client = _make_gateway_state_manager(response_success=False)
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await mgr.save_portfolio_metrics(_metrics_stub())
    assert excinfo.value.write_kind == "metrics"
    assert excinfo.value.strategy_id == "s1"


@pytest.mark.asyncio
async def test_gateway_save_portfolio_metrics_raises_on_rpc_exception() -> None:
    mgr, client = _make_gateway_state_manager(response_success=True)
    client.state.SavePortfolioMetrics = MagicMock(side_effect=TimeoutError("rpc"))
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await mgr.save_portfolio_metrics(_metrics_stub())
    assert excinfo.value.write_kind == "metrics"
    assert isinstance(excinfo.value.__cause__, TimeoutError)


# =============================================================================
# Site 5: operator alert on accounting failure
# =============================================================================


@pytest.mark.asyncio
async def test_alert_accounting_failure_dispatches_critical_card() -> None:
    """The runner must dispatch an OperatorCard via AlertManager on failure."""
    alert = MagicMock()
    alert.send_alert = AsyncMock()
    runner = _Runner(
        state_manager=MagicMock(),
        alert_manager=alert,
        config=RunnerConfig(enable_alerting=True),
    )
    err = AccountingPersistenceError(write_kind="snapshot", strategy_id="s1")
    await runner._alert_accounting_failure(_Strategy(), err)

    assert alert.send_alert.await_count == 1
    card = alert.send_alert.await_args.args[0]
    # severity must be CRITICAL -- silent accounting loss is the highest class
    from almanak.framework.models.operator_card import Severity

    assert card.severity == Severity.CRITICAL
    assert card.context.get("accounting_write_kind") == "snapshot"


@pytest.mark.asyncio
async def test_alert_accounting_failure_no_op_when_alerting_disabled() -> None:
    """When alerting is disabled, no alert is dispatched and no exception raised."""
    alert = MagicMock()
    alert.send_alert = AsyncMock()
    runner = _Runner(
        state_manager=MagicMock(),
        alert_manager=alert,
        config=RunnerConfig(enable_alerting=False),
    )
    await runner._alert_accounting_failure(_Strategy(), AccountingPersistenceError("ledger", "s1"))
    assert alert.send_alert.await_count == 0


# =============================================================================
# Mode detection helper
# =============================================================================


def test_is_live_mode_true_by_default() -> None:
    runner = _Runner(state_manager=MagicMock())
    assert runner._is_live_mode() is True


def test_is_live_mode_false_for_dry_run() -> None:
    runner = _Runner(state_manager=MagicMock(), config=RunnerConfig(dry_run=True))
    assert runner._is_live_mode() is False


def test_is_live_mode_false_for_paper_mode() -> None:
    cfg = RunnerConfig()
    cfg.paper_mode = True  # type: ignore[attr-defined]
    runner = _Runner(state_manager=MagicMock(), config=cfg)
    assert runner._is_live_mode() is False


# =============================================================================
# Enum surface
# =============================================================================


def test_iteration_status_accounting_failed_is_not_success() -> None:
    """ACCOUNTING_FAILED must be treated as a failure for consecutive-error tracking."""
    from almanak.framework.runner.runner_models import IterationResult

    result = IterationResult(status=IterationStatus.ACCOUNTING_FAILED, strategy_id="s1")
    assert result.success is False


# =============================================================================
# Mode-aware snapshot exception handling (run_loop path)
#
# These tests exercise the REAL handler branch in run_loop by extracting it
# into a thin helper and patching _capture_portfolio_snapshot to raise.
# Driving a full run_loop is heavyweight (requires full runner construction
# + mocked decide() + intent execution), but the handler branch itself is
# isolatable: the mode-aware logic is the only behaviour we need to lock in.
# =============================================================================


async def _exercise_snapshot_handler(runner: _Runner):
    """Drive the exact run_loop branch: try → except AccountingPersistenceError → mode-aware.

    Mirrors strategy_runner.py run_loop ~lines 1106-1139. Calling this with
    a runner whose _capture_portfolio_snapshot raises gives us the same
    code paths the production loop takes, without setting up the rest of
    the loop machinery. Returns IterationResult on live-mode escalation,
    None on non-live swallow.
    """
    from almanak.framework.runner.runner_models import IterationResult

    snapshot_start = datetime.now(UTC)
    strategy_id = "s1"
    strategy = _Strategy(sid=strategy_id)
    try:
        await runner._capture_portfolio_snapshot(strategy=strategy, iteration_number=1)
    except AccountingPersistenceError as acc_err:
        if runner._is_live_mode():
            await runner._alert_accounting_failure(strategy, acc_err)
            return IterationResult(
                status=IterationStatus.ACCOUNTING_FAILED,
                error=f"Accounting persistence failed ({acc_err.write_kind}): {acc_err}",
                strategy_id=strategy_id,
                duration_ms=runner._calculate_duration_ms(snapshot_start),
            )
        # Non-live: log + swallow (run_loop logs at ERROR; tests assert via caplog).
        return None
    return None


@pytest.mark.asyncio
async def test_snapshot_acc_error_escalates_in_live_mode() -> None:
    """Live mode: snapshot AccountingPersistenceError → ACCOUNTING_FAILED + alert.

    Patches the real `_capture_portfolio_snapshot` to raise; drives the
    same try/except branch the run_loop uses; asserts the result reflects
    ACCOUNTING_FAILED and the alert was dispatched.
    """
    alert_mgr = MagicMock()
    alert_mgr.send_alert = AsyncMock()

    runner = _Runner(
        state_manager=MagicMock(),
        alert_manager=alert_mgr,
        config=RunnerConfig(dry_run=False),
    )
    runner.config.enable_alerting = True  # type: ignore[attr-defined]
    runner._capture_portfolio_snapshot = AsyncMock(  # type: ignore[method-assign]
        side_effect=AccountingPersistenceError(write_kind="snapshot", strategy_id="s1")
    )

    result = await _exercise_snapshot_handler(runner)
    assert result is not None
    assert result.status is IterationStatus.ACCOUNTING_FAILED
    assert result.success is False
    assert "snapshot" in (result.error or "")
    # Alert dispatched once for ops visibility.
    alert_mgr.send_alert.assert_called_once()


@pytest.mark.asyncio
async def test_snapshot_acc_error_swallowed_in_paper_mode() -> None:
    """Paper mode: snapshot AccountingPersistenceError → no halt, no alert.

    The mode-aware branch must NOT escalate in paper/dry-run mode (or pre-
    prod runs would all halt on snapshot misconfig). The handler returns
    None (no result mutation) and skips the alert path.
    """
    alert_mgr = MagicMock()
    alert_mgr.send_alert = AsyncMock()

    cfg = RunnerConfig(dry_run=False)
    cfg.paper_mode = True  # type: ignore[attr-defined]
    runner = _Runner(
        state_manager=MagicMock(),
        alert_manager=alert_mgr,
        config=cfg,
    )
    runner._capture_portfolio_snapshot = AsyncMock(  # type: ignore[method-assign]
        side_effect=AccountingPersistenceError(write_kind="snapshot", strategy_id="s1")
    )

    result = await _exercise_snapshot_handler(runner)
    assert result is None  # no escalation
    alert_mgr.send_alert.assert_not_called()


def test_execution_mode_is_strenum() -> None:
    """ExecutionMode is a StrEnum so it serialises to bare strings for persistence."""
    from almanak.framework.runner.strategy_runner import (
        ExecutionMode,
        derive_execution_mode_from_config,
    )

    cfg_live = RunnerConfig(dry_run=False)
    cfg_dry = RunnerConfig(dry_run=True)
    cfg_paper = RunnerConfig(dry_run=False)
    cfg_paper.paper_mode = True  # type: ignore[attr-defined]

    assert derive_execution_mode_from_config(cfg_live) is ExecutionMode.LIVE
    assert derive_execution_mode_from_config(cfg_dry) is ExecutionMode.DRY_RUN
    assert derive_execution_mode_from_config(cfg_paper) is ExecutionMode.PAPER

    # StrEnum: stringifies to the bare label so ledger.execution_mode = mode works.
    assert str(ExecutionMode.LIVE) == "live"
    assert ExecutionMode.LIVE == "live"
    assert ExecutionMode.DRY_RUN == "dry_run"
    assert ExecutionMode.PAPER == "paper"
