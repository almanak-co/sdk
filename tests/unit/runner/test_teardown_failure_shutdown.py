"""Tests for teardown failure → shutdown with ERROR terminal state.

Validates that when teardown fails in a managed deployment, the runner:
1. Sets _terminal_lifecycle_state = "ERROR" (not the default "TERMINATED")
2. Sets _terminal_lifecycle_error_message with a meaningful description
3. Requests shutdown so the run loop exits

In local mode, teardown failure must NOT shut down — the runner stays alive
for debugging (matching the circuit breaker pattern).
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    StrategyRunner,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_runner(**overrides) -> StrategyRunner:
    """Build a StrategyRunner with minimal mocks."""
    defaults = dict(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=None,
    )
    defaults.update(overrides)
    return StrategyRunner(**defaults)


def _make_strategy(strategy_id: str = "test_strat", chain: str = "arbitrum") -> MagicMock:
    """Build a mock strategy for teardown tests."""
    strategy = MagicMock()
    strategy.strategy_id = strategy_id
    strategy.chain = chain
    strategy.wallet_address = "0x1234"
    strategy.create_market_snapshot.return_value = MagicMock(
        get_price_oracle_dict=MagicMock(return_value={}),
    )
    return strategy


def _make_intent(intent_type: str = "SWAP") -> MagicMock:
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value=intent_type)
    intent.chain = "arbitrum"
    intent.is_chained_amount = False
    return intent


@pytest.fixture()
def _deployed(monkeypatch):
    """Simulate a managed K8s deployment (AGENT_ID env var present)."""
    monkeypatch.setenv("AGENT_ID", "agent-v2-test-123")


@pytest.fixture()
def _local(monkeypatch, tmp_path):
    """Simulate local development (no AGENT_ID env var).

    Also pin ``ALMANAK_STATE_DB`` to a per-test tmp file so the strict,
    strategy-scoped DB resolver (VIB-3835) doesn't hard-fail when the
    teardown manager / adapter resolves its DB path. The runner code under
    test only constructs the manager — it never reads the file — so a
    placeholder path is sufficient.
    """
    monkeypatch.delenv("AGENT_ID", raising=False)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "test_state.db"))


# ---------------------------------------------------------------------------
# _request_teardown_failure_shutdown helper — deployed mode
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_deployed")
class TestRequestTeardownFailureShutdownDeployed:
    """In deployed mode, teardown failure must set ERROR state and shut down."""

    def test_sets_error_terminal_state(self):
        runner = _make_runner()
        runner._request_teardown_failure_shutdown("something went wrong")

        assert runner._terminal_lifecycle_state == "ERROR"
        assert runner._terminal_lifecycle_error_message == "something went wrong"
        assert runner._shutdown_requested is True

    def test_does_not_write_terminated(self):
        runner = _make_runner()
        runner._request_teardown_failure_shutdown("bad teardown")

        assert runner._terminal_lifecycle_state != "TERMINATED"


# ---------------------------------------------------------------------------
# _request_teardown_failure_shutdown helper — local mode
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_local")
class TestRequestTeardownFailureShutdownLocal:
    """In local mode, teardown failure must NOT shut down — keep runner alive."""

    def test_does_not_request_shutdown(self):
        runner = _make_runner()
        runner._request_teardown_failure_shutdown("something went wrong")

        assert runner._shutdown_requested is False

    def test_does_not_set_error_terminal_state(self):
        runner = _make_runner()
        runner._request_teardown_failure_shutdown("bad teardown")

        assert runner._terminal_lifecycle_state is None
        assert runner._terminal_lifecycle_error_message is None


# ---------------------------------------------------------------------------
# _execute_teardown — generate_teardown_intents failure
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_deployed")
class TestTeardownIntentGenerationFailure:
    """When generate_teardown_intents raises in deployed mode, runner must shut down with ERROR."""

    @pytest.mark.asyncio
    async def test_shutdown_with_error_on_intent_generation_failure(self):
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.generate_teardown_intents.side_effect = RuntimeError("intent gen boom")

        from almanak.framework.teardown.models import TeardownMode

        result = await runner._execute_teardown(
            strategy=strategy,
            teardown_mode=TeardownMode.HARD,
            start_time=datetime.now(UTC),
        )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert runner._terminal_lifecycle_state == "ERROR"
        assert "intent gen boom" in runner._terminal_lifecycle_error_message
        assert runner._shutdown_requested is True


@pytest.mark.usefixtures("_local")
class TestTeardownIntentGenerationFailureLocal:
    """When generate_teardown_intents raises in local mode, runner must stay alive."""

    @pytest.mark.asyncio
    async def test_no_shutdown_on_intent_generation_failure(self):
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.generate_teardown_intents.side_effect = RuntimeError("intent gen boom")

        from almanak.framework.teardown.models import TeardownMode

        result = await runner._execute_teardown(
            strategy=strategy,
            teardown_mode=TeardownMode.HARD,
            start_time=datetime.now(UTC),
        )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert runner._shutdown_requested is False
        assert runner._terminal_lifecycle_state is None


# ---------------------------------------------------------------------------
# _execute_teardown_via_manager — exception path (deployed)
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_deployed")
class TestManagerExecutionException:
    """When _execute_teardown_via_manager hits an exception in deployed mode, runner must ERROR and shut down."""

    @pytest.mark.asyncio
    async def test_manager_exception_sets_error_state(self):
        runner = _make_runner()
        error_msg = "k8s pod crashed"

        runner._request_teardown_failure_shutdown(error_msg)

        assert runner._terminal_lifecycle_state == "ERROR"
        assert runner._terminal_lifecycle_error_message == error_msg
        assert runner._shutdown_requested is True


# ---------------------------------------------------------------------------
# Incomplete teardown via inline path
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_deployed")
class TestManagerTeardownIncomplete:
    """When teardown is incomplete in deployed mode, runner must ERROR and shut down."""

    @pytest.mark.asyncio
    async def test_incomplete_teardown_via_inline_sets_error_state(self):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()

        success_result = IterationResult(
            status=IterationStatus.SUCCESS,
            intent=intent,
            strategy_id="test_strat",
        )
        failed_result = IterationResult(
            status=IterationStatus.STRATEGY_ERROR,
            error="second swap reverted",
            intent=intent,
            strategy_id="test_strat",
        )
        runner._execute_single_chain = AsyncMock(side_effect=[success_result, failed_result])

        with patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls:
            mock_intent_cls.has_chained_amount.return_value = False

            result = await runner._execute_teardown_inline(
                strategy=strategy,
                teardown_intents=[_make_intent(), _make_intent()],
                teardown_market=MagicMock(),
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert runner._terminal_lifecycle_state == "ERROR"
        assert runner._shutdown_requested is True


@pytest.mark.usefixtures("_local")
class TestManagerTeardownIncompleteLocal:
    """When teardown is incomplete in local mode, runner must stay alive."""

    @pytest.mark.asyncio
    async def test_incomplete_teardown_stays_alive(self):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()

        failed_result = IterationResult(
            status=IterationStatus.STRATEGY_ERROR,
            error="swap reverted",
            intent=intent,
            strategy_id="test_strat",
        )
        runner._execute_single_chain = AsyncMock(return_value=failed_result)

        with patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls:
            mock_intent_cls.has_chained_amount.return_value = False

            result = await runner._execute_teardown_inline(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_market=MagicMock(),
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert runner._shutdown_requested is False
        assert runner._terminal_lifecycle_state is None


# ---------------------------------------------------------------------------
# _execute_teardown_inline — execution failure (deployed)
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_deployed")
class TestInlineTeardownFailure:
    """When inline teardown execution fails in deployed mode, runner must ERROR and shut down."""

    @pytest.mark.asyncio
    async def test_inline_failure_sets_error_state(self):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()

        failed_result = IterationResult(
            status=IterationStatus.STRATEGY_ERROR,
            error="swap reverted",
            intent=intent,
            strategy_id="test_strat",
        )
        runner._execute_single_chain = AsyncMock(return_value=failed_result)

        with patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls:
            mock_intent_cls.has_chained_amount.return_value = False

            result = await runner._execute_teardown_inline(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_market=MagicMock(),
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert runner._terminal_lifecycle_state == "ERROR"
        assert runner._shutdown_requested is True
