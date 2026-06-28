"""Explicit opt-in teardown gate — VIB-5474 (TD-16), subsumes VIB-5370.

Teardown eligibility used to gate on ``hasattr(strategy, "get_open_positions")``
— a presence-sniff that never gated anything (``get_open_positions`` is abstract
on ``IntentStrategy``, so it is always present) while an author's
``supports_teardown() -> False`` was silently ignored (the VIB-5370 trap).

These tests pin the replacement contract:

* ``strategy_supports_teardown`` is the single, default-safe source of truth.
* ``IntentStrategy`` (and ``StatelessStrategy``) default to ``True`` so a
  position-holding strategy is never silently dropped from teardown eligibility.
* An explicit ``supports_teardown() -> False`` is now HONOURED at both the
  dashboard position-snapshot gate and the runner teardown trigger.
* The dead ``hasattr(strategy, "get_open_positions")`` signal is gone.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.runner.runner_gateway import (
    _can_collect_position_snapshot,
    collect_position_snapshot,
)
from almanak.framework.runner.runner_models import (
    RunnerConfig,
    strategy_supports_teardown,
)
from almanak.framework.runner.strategy_runner import StrategyRunner
from almanak.framework.strategies.intent_strategy import IntentStrategy
from almanak.framework.strategies.stateless_strategy import StatelessStrategy

# =============================================================================
# strategy_supports_teardown — the authoritative, default-safe helper
# =============================================================================


class TestStrategySupportsTeardownHelper:
    def test_honours_explicit_true(self) -> None:
        strategy = SimpleNamespace(supports_teardown=lambda: True)
        assert strategy_supports_teardown(strategy) is True

    def test_honours_explicit_false(self) -> None:
        """The VIB-5370 fix: an explicit opt-out is no longer silently ignored."""
        strategy = SimpleNamespace(supports_teardown=lambda: False)
        assert strategy_supports_teardown(strategy) is False

    def test_default_safe_when_method_absent(self) -> None:
        """No method → eligible. Never silently strand a position-holder."""
        assert strategy_supports_teardown(SimpleNamespace()) is True

    def test_default_safe_when_strategy_is_none(self) -> None:
        """Absent strategy (``None``) → eligible, not an ``AttributeError``.

        ``getattr(None, ...)`` raises rather than returning the default, so the
        helper guards ``None`` explicitly and stays default-safe.
        """
        assert strategy_supports_teardown(None) is True

    def test_default_safe_when_method_not_callable(self) -> None:
        assert strategy_supports_teardown(SimpleNamespace(supports_teardown=True)) is True

    def test_default_safe_when_method_raises(self) -> None:
        def _boom() -> bool:
            raise RuntimeError("boom")

        assert strategy_supports_teardown(SimpleNamespace(supports_teardown=_boom)) is True

    def test_default_safe_when_override_returns_none(self) -> None:
        """A forgotten ``return`` (None) must NOT opt out — only literal False does.

        ``bool(None)`` would be False and silently strand a position-holder; the
        helper's contract is "only an explicit ``False`` makes a strategy
        ineligible", so a malformed override defaults to eligible.
        """
        assert strategy_supports_teardown(SimpleNamespace(supports_teardown=lambda: None)) is True

    def test_only_literal_false_opts_out(self) -> None:
        """Non-False falsy returns (0, "") are eligible; only literal False opts out."""
        assert strategy_supports_teardown(SimpleNamespace(supports_teardown=lambda: False)) is False
        assert strategy_supports_teardown(SimpleNamespace(supports_teardown=lambda: 0)) is True
        assert strategy_supports_teardown(SimpleNamespace(supports_teardown=lambda: "")) is True
        assert strategy_supports_teardown(SimpleNamespace(supports_teardown=lambda: 1)) is True
        assert strategy_supports_teardown(SimpleNamespace(supports_teardown=lambda: True)) is True


# =============================================================================
# Base-class defaults — default-safe True
# =============================================================================


class TestBaseClassDefault:
    def test_intent_strategy_default_is_true(self) -> None:
        # supports_teardown ignores self, so the unbound call is sufficient and
        # avoids constructing an abstract base.
        assert IntentStrategy.supports_teardown(SimpleNamespace()) is True

    def test_stateless_strategy_inherits_true(self) -> None:
        # StatelessStrategy keeps True so the operator stop-signal still
        # completes a (trivial, empty) teardown rather than being refused.
        assert StatelessStrategy.supports_teardown(SimpleNamespace()) is True

    def test_subclass_can_opt_out(self) -> None:
        class OptedOut(IntentStrategy):
            def supports_teardown(self) -> bool:
                return False

        # supports_teardown ignores self; the unbound call exercises the override
        # without instantiating the abstract base (decide() is still abstract).
        assert OptedOut.supports_teardown(SimpleNamespace()) is False
        assert strategy_supports_teardown(SimpleNamespace(supports_teardown=lambda: False)) is False


# =============================================================================
# Dashboard position-snapshot gate (runner_gateway)
#
# Observability is DECOUPLED from teardown eligibility (VIB-5474): an opted-out
# strategy still holds positions the operator must monitor/recover manually, so
# the dashboard must keep snapshotting them. The snapshot gate must NOT consult
# supports_teardown() — only gateway-client presence.
# =============================================================================


def _gateway_runner(*, gateway: bool = True) -> MagicMock:
    runner = MagicMock()
    runner._get_gateway_client.return_value = MagicMock() if gateway else None
    return runner


def _summary(positions: list) -> SimpleNamespace:
    return SimpleNamespace(positions=positions)


class TestSnapshotGate:
    def test_opted_out_strategy_is_still_snapshotted(self) -> None:
        """supports_teardown() == False must NOT suppress dashboard position reporting.

        These are exactly the positions the operator needs to see to recover
        manually — gating observability on the teardown opt-in would blind them.
        """
        position = MagicMock()
        position.details = {}
        position.health_factor = None
        strategy = SimpleNamespace(
            supports_teardown=lambda: False,
            get_open_positions=MagicMock(return_value=_summary([position])),
        )
        result = collect_position_snapshot(_gateway_runner(), strategy)
        # Positions ARE read and reported despite the teardown opt-out.
        strategy.get_open_positions.assert_called_once_with()
        assert result is not None and len(result) == 1

    def test_eligible_strategy_reads_positions(self) -> None:
        """supports_teardown() == True → get_open_positions IS consulted."""
        strategy = SimpleNamespace(
            supports_teardown=lambda: True,
            get_open_positions=MagicMock(return_value=_summary([])),
        )
        # Empty summary still returns None, but the gate let the read happen.
        assert collect_position_snapshot(_gateway_runner(), strategy) is None
        strategy.get_open_positions.assert_called_once_with()

    def test_no_gateway_client_skips_snapshot(self) -> None:
        """No gateway client → no snapshot (nothing to report to)."""
        strategy = SimpleNamespace(
            supports_teardown=lambda: True,
            get_open_positions=MagicMock(return_value=_summary([MagicMock()])),
        )
        assert collect_position_snapshot(_gateway_runner(gateway=False), strategy) is None
        strategy.get_open_positions.assert_not_called()


# =============================================================================
# Runner teardown trigger gate (_check_teardown_requested)
# =============================================================================


def _make_runner() -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


def _requesting_strategy(*, supports: bool) -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "deployment:abc123"
    strategy.should_teardown.return_value = True
    strategy.supports_teardown.return_value = supports
    return strategy


class TestTriggerGate:
    def test_opted_out_strategy_refuses_teardown(self) -> None:
        """A requested teardown is REFUSED (None) when the author opted out.

        Repeated checks keep refusing (the request stays pending), and the loud
        warning is throttled to once per deployment.
        """
        from almanak.framework.runner import strategy_runner as sr_mod

        runner = _make_runner()
        strategy = _requesting_strategy(supports=False)
        sr_mod._TEARDOWN_OPTOUT_WARNED.discard(strategy.deployment_id)

        with patch.object(sr_mod.logger, "warning") as warn:
            first = runner._check_teardown_requested(strategy)
            second = runner._check_teardown_requested(strategy)

        assert first is None and second is None
        # The request is left pending (not consumed) so the operator sees it.
        strategy.acknowledge_teardown_request.assert_not_called()
        # Loud once, then throttled — not re-warned every iteration.
        assert warn.call_count == 1
        sr_mod._TEARDOWN_OPTOUT_WARNED.discard(strategy.deployment_id)

    def test_eligible_strategy_returns_mode(self) -> None:
        """A requested teardown fires (returns a mode) for an eligible strategy."""
        from almanak.framework.teardown import TeardownMode

        runner = _make_runner()
        strategy = _requesting_strategy(supports=True)

        manager = MagicMock()
        manager.get_active_request.return_value = SimpleNamespace(mode=TeardownMode.SOFT)

        with (
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch(
                "almanak.framework.teardown.get_teardown_state_manager_for_runtime",
                return_value=manager,
            ),
        ):
            result = runner._check_teardown_requested(strategy)

        assert result == TeardownMode.SOFT
        strategy.acknowledge_teardown_request.assert_called_once()

    def test_no_request_returns_none_regardless(self) -> None:
        runner = _make_runner()
        strategy = _requesting_strategy(supports=True)
        strategy.should_teardown.return_value = False

        assert runner._check_teardown_requested(strategy) is None


# =============================================================================
# The dead hasattr(get_open_positions) presence-sniff is gone — proven by OUTCOME
#
# These pin the same regression intent as the old ``inspect.getsource`` source-greps
# (which were fragile: OSError under packaged/.pyc runs, and broken by an unrelated
# comment that merely *mentions* the forbidden string). Instead of asserting the
# module *text* no longer contains ``hasattr(strategy, "get_open_positions")``, we
# drive the snapshot gate directly and assert its DECISION.
# =============================================================================


class TestSnapshotGateIsGatewayOnly:
    def test_gate_passes_without_get_open_positions(self) -> None:
        """A strategy lacking ``get_open_positions`` STILL passes the gate.

        This is the behavioral proof that the dead ``hasattr(get_open_positions)``
        sniff is removed: were it still ANDed into the gate, a strategy without the
        attribute would be refused. Gateway present → gate True regardless.
        """
        strategy = SimpleNamespace()  # no get_open_positions, no supports_teardown
        assert _can_collect_position_snapshot(_gateway_runner(), strategy) is True

    def test_gate_passes_for_opted_out_strategy(self) -> None:
        """``supports_teardown() == False`` does NOT couple into the snapshot gate.

        Observability stays decoupled from teardown eligibility: an opted-out
        strategy's positions must remain visible to the operator (VIB-5474).
        """
        strategy = SimpleNamespace(supports_teardown=lambda: False)
        assert _can_collect_position_snapshot(_gateway_runner(), strategy) is True

    def test_gate_refused_without_gateway(self) -> None:
        """No gateway client → gate False, irrespective of the strategy shape.

        Confirms the gate is decided SOLELY by gateway-client presence.
        """
        strategy = SimpleNamespace(
            supports_teardown=lambda: True,
            get_open_positions=lambda: _summary([]),
        )
        assert _can_collect_position_snapshot(_gateway_runner(gateway=False), strategy) is False


def test_eligibility_decided_only_at_runner_trigger() -> None:
    """The teardown *eligibility* gate lives solely at the runner trigger.

    Behavioral (not source-grep) proof of the decoupling: for the SAME opted-out
    strategy, the runner teardown trigger REFUSES the request (leaving it pending)
    while the dashboard snapshot gate still ADMITS it (observability preserved).
    """
    from almanak.framework.runner import strategy_runner as sr_mod

    # Runner trigger consults the authoritative helper → opted-out ⇒ refused/pending.
    runner = _make_runner()
    strategy = _requesting_strategy(supports=False)
    sr_mod._TEARDOWN_OPTOUT_WARNED.discard(strategy.deployment_id)
    with patch.object(sr_mod.logger, "warning"):
        assert runner._check_teardown_requested(strategy) is None
    strategy.acknowledge_teardown_request.assert_not_called()
    sr_mod._TEARDOWN_OPTOUT_WARNED.discard(strategy.deployment_id)

    # Snapshot gate does NOT consult it → the same opt-out is still snapshotted.
    observed = SimpleNamespace(supports_teardown=lambda: False)
    assert _can_collect_position_snapshot(_gateway_runner(), observed) is True


@pytest.mark.parametrize("supports", [True, False])
def test_helper_is_pure(supports: bool) -> None:
    """The helper does not mutate the strategy."""
    calls: list[int] = []

    def _probe() -> bool:
        calls.append(1)
        return supports

    strategy = SimpleNamespace(supports_teardown=_probe)
    assert strategy_supports_teardown(strategy) is supports
    assert len(calls) == 1
