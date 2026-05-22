"""VIB-4587 / F5 — teardown sweep DX warning.

When ``asset_policy=target_token`` resolves a teardown SWAP's
``amount='all'`` into the full wallet balance, the sweep is wallet-scoped
(not strategy-scoped). A wallet shared between strategies (or with
pre-existing balances the strategy never emitted) is swept too —
working as designed but a silent surprise.
``warn_if_sweep_non_strategy_balance`` (in
``almanak/framework/teardown/sweep_warning.py``) makes that visible.

Behaviour is unchanged: the warning fires but the sweep still proceeds.

The helper is invoked from two places — covered by separate tests:

* the inline single-chain teardown fallback in ``runner_teardown.py``;
* the manager-driven teardown path in ``teardown_manager.py``
  (via ``TeardownRunnerHelpers.warn_sweep_non_strategy_balance`` bound by
  ``build_runner_helpers``).
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.runner.runner_teardown import _warn_if_sweep_non_strategy_balance
from almanak.framework.teardown.sweep_warning import warn_if_sweep_non_strategy_balance


def _swap_intent(from_token: str = "WETH") -> SimpleNamespace:
    return SimpleNamespace(
        intent_type=SimpleNamespace(value="SWAP"),
        amount="all",
        from_token=from_token,
        to_token="USDC",
    )


def _supply_event_for(token: str) -> dict:
    return {
        "event_type": "SUPPLY",
        "payload_json": json.dumps({"token_in": token, "amount_in": "10"}),
    }


def _swap_event(token_in: str, token_out: str) -> dict:
    return {
        "event_type": "SWAP",
        "payload_json": json.dumps({"token_in": token_in, "token_out": token_out}),
    }


class TestSweepWarning:
    def test_warns_when_sweeping_non_strategy_token(self, caplog: pytest.LogCaptureFixture) -> None:
        """Strategy emitted USDC/USDT events; teardown sweeps WETH → WARN."""
        state_manager = MagicMock()
        state_manager.get_accounting_events_sync.return_value = [
            _supply_event_for("USDC"),
            _swap_event("USDC", "USDT"),
        ]
        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            _warn_if_sweep_non_strategy_balance(
                state_manager=state_manager,
                deployment_id="LoopStrat:abc",
                intent=_swap_intent(from_token="WETH"),
                balance_token="WETH",
                balance_value=Decimal("0.0015"),
            )
        records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert records, "expected a WARNING when sweeping foreign balance"
        msg = records[0].getMessage()
        assert "WETH" in msg
        assert "0.0015" in msg
        assert "wallet-scoped" in msg
        # Confirms emitted-token enumeration is exposed for debugging.
        assert "USDC" in msg and "USDT" in msg

    def test_no_warning_when_token_is_strategy_emitted(self, caplog: pytest.LogCaptureFixture) -> None:
        """Strategy emitted WETH events; teardown sweeps WETH → no warning."""
        state_manager = MagicMock()
        state_manager.get_accounting_events_sync.return_value = [
            _swap_event("USDC", "WETH"),
        ]
        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            _warn_if_sweep_non_strategy_balance(
                state_manager=state_manager,
                deployment_id="LoopStrat:abc",
                intent=_swap_intent(from_token="WETH"),
                balance_token="WETH",
                balance_value=Decimal("0.5"),
            )
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_no_warning_for_non_swap_intent(self, caplog: pytest.LogCaptureFixture) -> None:
        """Only SWAP intents trigger the sweep warning."""
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="WITHDRAW"),
            amount="all",
            token="USDC",
        )
        state_manager = MagicMock()
        state_manager.get_accounting_events_sync.return_value = [_supply_event_for("WBTC")]
        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            _warn_if_sweep_non_strategy_balance(
                state_manager=state_manager,
                deployment_id="LoopStrat:abc",
                intent=intent,
                balance_token="USDC",
                balance_value=Decimal("100"),
            )
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_no_warning_when_strategy_has_no_events_yet(self, caplog: pytest.LogCaptureFixture) -> None:
        """Empty event history → suppress warning (no baseline to compare)."""
        state_manager = MagicMock()
        state_manager.get_accounting_events_sync.return_value = []
        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            _warn_if_sweep_non_strategy_balance(
                state_manager=state_manager,
                deployment_id="LoopStrat:abc",
                intent=_swap_intent(from_token="WETH"),
                balance_token="WETH",
                balance_value=Decimal("0.0015"),
            )
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_state_manager_failure_swallowed(self, caplog: pytest.LogCaptureFixture) -> None:
        """A read failure in state_manager MUST NOT block the unwind."""
        state_manager = MagicMock()
        state_manager.get_accounting_events_sync.side_effect = RuntimeError("db locked")
        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            _warn_if_sweep_non_strategy_balance(
                state_manager=state_manager,
                deployment_id="LoopStrat:abc",
                intent=_swap_intent(from_token="WETH"),
                balance_token="WETH",
                balance_value=Decimal("0.0015"),
            )
        # No exception, no WARNING (silent best-effort failure).
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_warns_when_dict_shaped_swap_intent_resumed_from_manager_path(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The teardown manager round-trips intents through SQLite for resume.
        On the way back out (``teardown_manager.py:~829``) intents surface in
        dict shape — ``{"intent_type": "SWAP", "from_token": "WETH", ...}`` —
        not as Pydantic objects. The pre-fix gate keyed on
        ``intent.intent_type.value`` and silently bypassed every resumed SWAP.

        Regression guard (CodeRabbit, 2026-05-18 second pass): a dict-shaped
        SWAP whose from-token isn't in the strategy's emitted-token set must
        still WARN. Same expectation as the object-intent case below.
        """
        state_manager = MagicMock()
        state_manager.get_accounting_events_sync.return_value = [
            _supply_event_for("USDC"),
            _swap_event("USDC", "USDT"),
        ]
        # Dict-shaped intent (resumed-path serialisation). Try both flavours
        # the manager may produce: bare "SWAP" and "IntentType.SWAP".
        for intent_type_value in ("SWAP", "IntentType.SWAP"):
            caplog.clear()
            dict_intent = {
                "intent_type": intent_type_value,
                "amount": "all",
                "from_token": "WETH",
                "to_token": "USDC",
            }
            with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
                warn_if_sweep_non_strategy_balance(
                    state_manager=state_manager,
                    deployment_id="LoopStrat:abc",
                    intent=dict_intent,
                    balance_token="WETH",
                    balance_value=Decimal("0.0015"),
                )
            records = [r for r in caplog.records if r.levelno == logging.WARNING]
            assert records, f"expected WARNING for dict intent_type={intent_type_value!r}"

    def test_no_warning_for_dict_non_swap_intent(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Dict-shaped non-SWAP intents must still be ignored by the gate.

        Companion to the SWAP test above — proves the dict-aware branch
        didn't accidentally widen scope. A REPAY intent (dict-shaped) is
        not a wallet-scoped sweep concern and must not WARN.
        """
        state_manager = MagicMock()
        state_manager.get_accounting_events_sync.return_value = [
            _supply_event_for("USDC"),
        ]
        dict_intent = {
            "intent_type": "REPAY",
            "amount": "all",
            "token": "USDC",
        }
        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            warn_if_sweep_non_strategy_balance(
                state_manager=state_manager,
                deployment_id="LoopStrat:abc",
                intent=dict_intent,
                balance_token="USDC",
                balance_value=Decimal("1.0"),
            )
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_no_warning_when_state_manager_lacks_method(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Hand the helper a state manager without ``get_accounting_events_sync``
        (e.g. a ``TeardownStateManager`` mistakenly passed in): it must silently
        no-op rather than rely on the try/except catching an ``AttributeError``.

        Regression guard for the original VIB-4587 / F5 bug — the helper used
        to be wired with the teardown lifecycle SM and never fired at all
        because the try/except swallowed every call.
        """
        # No spec → MagicMock auto-creates every attribute. Use a real object
        # so ``get_accounting_events_sync`` is genuinely absent.

        class _TeardownOnlySM:
            def save_teardown_state(self, *args, **kwargs):  # pragma: no cover
                raise NotImplementedError

        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            warn_if_sweep_non_strategy_balance(
                state_manager=_TeardownOnlySM(),
                deployment_id="LoopStrat:abc",
                intent=_swap_intent(from_token="WETH"),
                balance_token="WETH",
                balance_value=Decimal("0.0015"),
            )
        # Silent no-op (defensive hasattr check fires first).
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]


class TestSweepWarningRunnerHelpersBinding:
    """Verify ``build_runner_helpers`` binds the warning to the runner's
    accounting state manager — not the teardown lifecycle SM. Without this
    binding the manager-driven teardown path would silently skip the warning.
    """

    def test_runner_helpers_warn_sweep_uses_runner_accounting_state_manager(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Build a fake runner whose ``state_manager`` (the **accounting** SM)
        # returns one event involving USDC only — sweeping WETH must WARN.
        accounting_sm = MagicMock()
        accounting_sm.get_accounting_events_sync.return_value = [_supply_event_for("USDC")]
        runner = SimpleNamespace(
            state_manager=accounting_sm,
            execution_orchestrator=None,
            alert_manager=None,
            _get_gateway_client=lambda: None,
            _capture_lending_state_safe=lambda **kw: None,
            _teardown_price_oracle=None,
        )

        # build_runner_helpers reaches into ``runner_state`` / ``teardown_commit``
        # for other helpers; we only exercise the sweep-warning callable here.
        from almanak.framework.teardown.runner_helpers import build_runner_helpers

        helpers = build_runner_helpers(runner)
        assert helpers.has_sweep_warning, "expected warn_sweep_non_strategy_balance to be bound"

        strategy = SimpleNamespace(deployment_id="LoopStrat")
        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            helpers.warn_sweep_non_strategy_balance(  # type: ignore[misc]
                strategy,
                _swap_intent(from_token="WETH"),
                "WETH",
                Decimal("0.0015"),
            )
        records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert records, "expected WARNING via runner_helpers binding"
        accounting_sm.get_accounting_events_sync.assert_called_once_with("LoopStrat")
