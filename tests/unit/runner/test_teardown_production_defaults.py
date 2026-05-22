"""Tests for the production-default teardown path.

With ``RunnerConfig.allow_unsafe_teardown_fallback=False`` (the production
default), the runner must NOT silently fall back to the inline path when the
TeardownManager compiler or position query fails — it must fail-closed,
mark the teardown request as failed, and request a runner shutdown.

Complements test_teardown_flow.py / test_teardown_manager_wiring.py which
cover the legacy True fallback behaviour.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.teardown.models import TeardownMode


@pytest.fixture(autouse=True)
def _isolated_teardown_state_db(monkeypatch, tmp_path):
    """Pin ``ALMANAK_STATE_DB`` to a per-test tmp file so the strict,
    strategy-scoped DB resolver (VIB-3835) doesn't hard-fail when the
    runner builds the TeardownStateAdapter. Tests mock the manager so the
    file is never read.
    """
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "test_state.db"))


def _runner() -> StrategyRunner:
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=None,
    )


def _strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "test_strat"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234"
    strategy.should_teardown.return_value = True
    strategy.create_market_snapshot.return_value = MagicMock(
        get_price_oracle_dict=MagicMock(return_value={}),
    )
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value="SWAP")
    intent.chain = "arbitrum"
    intent.is_chained_amount = False
    strategy.generate_teardown_intents.return_value = [intent]
    return strategy


def _request() -> MagicMock:
    req = MagicMock()
    req.requested_by = "risk_guard"
    return req


class TestProductionDefaults:
    def test_allow_unsafe_fallback_defaults_to_false(self) -> None:
        assert RunnerConfig().allow_unsafe_teardown_fallback is False

    @pytest.mark.asyncio
    async def test_compiler_failure_fails_closed_not_fallback(self) -> None:
        """With the flag at its default (False), a compiler build failure must
        surface as STRATEGY_ERROR, mark the request failed, and request a
        shutdown — NOT silently fall through to the unsafe inline path.
        """
        runner = _runner()
        # Default config — allow_unsafe_teardown_fallback is False.
        assert runner.config.allow_unsafe_teardown_fallback is False

        strategy = _strategy()
        runner._build_teardown_compiler = MagicMock(return_value=None)
        runner._execute_teardown_inline = AsyncMock()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner._create_error_result = MagicMock(
            return_value=IterationResult(status=IterationStatus.STRATEGY_ERROR, deployment_id="test_strat"),
        )

        state_manager = MagicMock()
        start_time = datetime.now(UTC)

        from almanak.framework.runner.runner_teardown import execute_teardown_via_manager

        result = await execute_teardown_via_manager(
            runner=runner,
            strategy=strategy,
            teardown_intents=strategy.generate_teardown_intents.return_value,
            teardown_market=None,
            teardown_mode=TeardownMode.SOFT,
            start_time=start_time,
            request=_request(),
            state_manager=state_manager,
        )

        # Must have raised the safety guard — not fallen back.
        runner._execute_teardown_inline.assert_not_called()
        runner._request_teardown_failure_shutdown.assert_called_once()
        state_manager.mark_failed.assert_called_once()
        assert result.status == IterationStatus.STRATEGY_ERROR

    @pytest.mark.asyncio
    async def test_positions_fetch_failure_fails_closed(self) -> None:
        """get_open_positions raising must not silently route through inline
        execution with default-False fallback.
        """
        runner = _runner()
        assert runner.config.allow_unsafe_teardown_fallback is False

        strategy = _strategy()
        strategy.get_open_positions.side_effect = RuntimeError("RPC flap")

        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())
        runner._execute_teardown_inline = AsyncMock()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner._create_error_result = MagicMock(
            return_value=IterationResult(status=IterationStatus.STRATEGY_ERROR, deployment_id="test_strat"),
        )

        state_manager = MagicMock()
        start_time = datetime.now(UTC)

        from almanak.framework.runner.runner_teardown import execute_teardown_via_manager

        result = await execute_teardown_via_manager(
            runner=runner,
            strategy=strategy,
            teardown_intents=strategy.generate_teardown_intents.return_value,
            teardown_market=None,
            teardown_mode=TeardownMode.SOFT,
            start_time=start_time,
            request=_request(),
            state_manager=state_manager,
        )

        runner._execute_teardown_inline.assert_not_called()
        runner._request_teardown_failure_shutdown.assert_called_once()
        state_manager.mark_failed.assert_called_once()
        assert result.status == IterationStatus.STRATEGY_ERROR


class TestAutoModeTaxonomy:
    """The auto-mode check is a safety-critical switch — test every known source
    by calling the real ``derive_teardown_auto_mode`` helper that the production
    ``execute_teardown_via_manager`` path uses. Drift between the tested predicate
    and the runtime predicate is impossible because there's only one predicate.
    """

    @pytest.mark.parametrize(
        "requested_by,expected_auto",
        [
            # Operator-present sources → manual mode (approval callback wired)
            ("cli", False),
            ("dashboard", False),
            ("dashboard_api", False),
            # No-operator sources → auto mode (hard slippage caps, no polling)
            ("risk_guard", True),
            ("config", True),
            ("lifecycle", True),
            ("emergency_manager", True),
            # Unknown source → auto (fail-closed; don't block on approval from
            # nobody because we don't recognise the source).
            ("brand_new_source_we_forgot", True),
        ],
    )
    def test_auto_mode_derivation(self, requested_by: str, expected_auto: bool) -> None:
        """Route through the real predicate the production code calls."""
        from almanak.framework.runner.runner_teardown import derive_teardown_auto_mode

        request = MagicMock()
        request.requested_by = requested_by

        assert derive_teardown_auto_mode(request) is expected_auto

    def test_request_none_is_auto_mode(self) -> None:
        """strategy.should_teardown() self-signal creates no request — treat as auto."""
        from almanak.framework.runner.runner_teardown import derive_teardown_auto_mode

        assert derive_teardown_auto_mode(None) is True

    def test_request_missing_requested_by_is_auto_mode(self) -> None:
        """Defensive: a malformed request missing requested_by must not crash."""
        from almanak.framework.runner.runner_teardown import derive_teardown_auto_mode

        class _BadRequest:
            pass

        assert derive_teardown_auto_mode(_BadRequest()) is True

    def test_manual_requesters_is_explicit_small_whitelist(self) -> None:
        """Guardrail: every additional entry in _MANUAL_TEARDOWN_REQUESTERS is a
        deliberate decision. If this assertion fails because the set grew,
        confirm the new source really has a human operator on the other side."""
        from almanak.framework.runner.runner_teardown import _MANUAL_TEARDOWN_REQUESTERS

        assert _MANUAL_TEARDOWN_REQUESTERS == frozenset({"cli", "dashboard", "dashboard_api"})
