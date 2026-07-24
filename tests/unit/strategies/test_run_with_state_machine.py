"""Branch coverage for IntentStrategy.run_with_state_machine.

Covers decide()-result normalization (None / IntentSequence / list /
single intent), the HOLD fast path, execution via a faked
IntentStateMachine (receipt provider present and absent, retry delay),
the non-fatal framework hook failures, and the outer error path.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.framework.intents import HoldIntent, Intent, IntentSequence
from almanak.framework.strategies.intent_strategy import IntentStrategy


class _FakeStateMachine:
    """Scripted state machine: yields queued step results, then completes."""

    def __init__(self, steps, *, success=True, error=None):
        self._steps = list(steps)
        self.is_complete = not self._steps
        self.success = success
        self.error = error
        self.receipts = []

    def step(self):
        step = self._steps.pop(0)
        if not self._steps:
            self.is_complete = True
        return step

    def set_receipt(self, receipt):
        self.receipts.append(receipt)


def _step(*, action_bundle=None, needs_execution=False, retry_delay=None):
    return SimpleNamespace(
        action_bundle=action_bundle,
        needs_execution=needs_execution,
        retry_delay=retry_delay,
    )


class _Strategy(IntentStrategy):
    """Minimal concrete strategy; bypasses IntentStrategy.__init__."""

    STRATEGY_NAME = "test_run_with_state_machine"

    def __init__(self, decide_result=None):  # type: ignore[override]
        self._decide_result = decide_result
        self._current_intent = None
        self.compiler = MagicMock()
        self.state_machine_config = MagicMock()
        self.on_sadflow_enter = None
        self.on_sadflow_exit = None
        self.on_retry = None
        self.injected = []
        self.recorded = []
        self.inject_raises = False
        self.record_raises = False

    def create_market_snapshot(self):  # type: ignore[override]
        return MagicMock()

    def decide(self, market):  # type: ignore[override]
        result = self._decide_result
        if callable(result):
            return result()
        return result

    def get_open_positions(self):  # type: ignore[override]
        return None

    def generate_teardown_intents(self, mode, market=None):  # type: ignore[override]
        return []

    def _framework_inject_intent_params(self, intent):
        if self.inject_raises:
            raise RuntimeError("inject boom")
        self.injected.append(intent)
        return intent

    def _framework_record_intent_execution(self, intent, success, result):
        if self.record_raises:
            raise RuntimeError("record boom")
        self.recorded.append((intent, success))


def _wire_machine(monkeypatch, machine):
    created = []

    def _factory(**kwargs):
        created.append(kwargs)
        return machine

    monkeypatch.setattr(
        "almanak.framework.strategies.intent_strategy.IntentStateMachine", _factory
    )
    return created


def _swap_intent():
    return Intent.swap("USDC", "WETH", amount=100, chain="ethereum")


class TestDecideNormalization:
    def test_none_becomes_hold(self):
        result = _Strategy(decide_result=None).run_with_state_machine()
        assert result.success
        assert isinstance(result.intent, HoldIntent)
        assert result.intent.reason == "decide() returned None"

    def test_hold_intent_short_circuits(self):
        result = _Strategy(Intent.hold(reason="flat market")).run_with_state_machine()
        assert result.success
        assert result.intent.reason == "flat market"
        assert result.action_bundle is None

    def test_empty_list_becomes_hold(self):
        result = _Strategy([]).run_with_state_machine()
        assert result.success
        assert isinstance(result.intent, HoldIntent)
        assert result.intent.reason == "Empty result list"

    def test_sequence_uses_first_intent(self, monkeypatch):
        first, second = _swap_intent(), _swap_intent()
        machine = _FakeStateMachine([_step()])
        _wire_machine(monkeypatch, machine)
        strategy = _Strategy(IntentSequence(intents=[first, second]))
        result = strategy.run_with_state_machine()
        assert result.intent is first
        assert result.success

    def test_list_uses_first_intent(self, monkeypatch):
        first, second = _swap_intent(), _swap_intent()
        machine = _FakeStateMachine([_step()])
        _wire_machine(monkeypatch, machine)
        result = _Strategy([first, second]).run_with_state_machine()
        assert result.intent is first

    def test_list_of_sequences_uses_first_of_first(self, monkeypatch):
        inner = _swap_intent()
        machine = _FakeStateMachine([_step()])
        _wire_machine(monkeypatch, machine)
        result = _Strategy([IntentSequence(intents=[inner])]).run_with_state_machine()
        assert result.intent is inner


class TestStateMachineExecution:
    def test_completes_with_machine_success(self, monkeypatch):
        machine = _FakeStateMachine([_step()], success=True)
        created = _wire_machine(monkeypatch, machine)
        strategy = _Strategy(_swap_intent())
        result = strategy.run_with_state_machine()
        assert result.success
        assert result.error is None
        assert strategy.recorded == [(result.intent, True)]
        assert created[0]["compiler"] is strategy.compiler
        assert result.execution_time_ms is not None

    def test_machine_failure_propagates_error(self, monkeypatch):
        machine = _FakeStateMachine([_step()], success=False, error="compile failed")
        _wire_machine(monkeypatch, machine)
        result = _Strategy(_swap_intent()).run_with_state_machine()
        assert not result.success
        assert result.error == "compile failed"

    def test_needs_execution_without_provider_returns_after_compile(self, monkeypatch):
        bundle = MagicMock()
        machine = _FakeStateMachine(
            [_step(action_bundle=bundle, needs_execution=True), _step()]
        )
        _wire_machine(monkeypatch, machine)
        result = _Strategy(_swap_intent()).run_with_state_machine()
        assert result.success
        assert result.action_bundle is bundle
        assert machine.receipts == []

    def test_needs_execution_with_provider_feeds_receipt(self, monkeypatch):
        bundle, receipt = MagicMock(), MagicMock()
        machine = _FakeStateMachine([_step(action_bundle=bundle, needs_execution=True)])
        _wire_machine(monkeypatch, machine)
        result = _Strategy(_swap_intent()).run_with_state_machine(
            receipt_provider=lambda b: receipt
        )
        assert machine.receipts == [receipt]
        assert result.action_bundle is bundle
        assert result.success

    def test_retry_delay_sleeps(self, monkeypatch):
        sleeps = []
        monkeypatch.setattr("time.sleep", sleeps.append)
        machine = _FakeStateMachine([_step(retry_delay=0.25), _step()])
        _wire_machine(monkeypatch, machine)
        result = _Strategy(_swap_intent()).run_with_state_machine()
        assert sleeps == [0.25]
        assert result.success


class TestFrameworkHooks:
    def test_inject_hook_failure_is_non_fatal(self, monkeypatch):
        machine = _FakeStateMachine([_step()])
        _wire_machine(monkeypatch, machine)
        strategy = _Strategy(_swap_intent())
        strategy.inject_raises = True
        result = strategy.run_with_state_machine()
        assert result.success
        assert strategy.injected == []

    def test_record_hook_failure_is_non_fatal(self, monkeypatch):
        machine = _FakeStateMachine([_step()])
        _wire_machine(monkeypatch, machine)
        strategy = _Strategy(_swap_intent())
        strategy.record_raises = True
        result = strategy.run_with_state_machine()
        assert result.success


class TestErrorPath:
    def test_decide_exception_returns_failed_result(self):
        def _boom():
            raise RuntimeError("market data unavailable")

        result = _Strategy(_boom).run_with_state_machine()
        assert not result.success
        assert "market data unavailable" in result.error
        assert result.execution_time_ms is not None


def _wire_compiler(strategy, *, status=None, bundle=None, error=None):
    from almanak.framework.intents.compiler import CompilationStatus

    result = SimpleNamespace(
        status=status or CompilationStatus.SUCCESS,
        action_bundle=bundle if bundle is not None else MagicMock(),
        error=error,
    )
    strategy.compiler.compile = MagicMock(return_value=result)
    return result


class TestRun:
    """Covers the single-intent run() compile path (same decide() normalization)."""

    def test_none_decision_holds(self):
        strategy = _Strategy(decide_result=None)
        assert strategy.run() is None
        assert isinstance(strategy._current_intent, HoldIntent)
        assert strategy._current_intent.reason == "decide() returned None"

    def test_empty_list_holds(self):
        strategy = _Strategy([])
        assert strategy.run() is None
        assert strategy._current_intent.reason == "Empty result"

    def test_hold_intent_returns_none_without_compiling(self):
        strategy = _Strategy(Intent.hold(reason="flat"))
        _wire_compiler(strategy)
        assert strategy.run() is None
        strategy.compiler.compile.assert_not_called()

    def test_single_intent_compiles_to_bundle(self):
        strategy = _Strategy(_swap_intent())
        result = _wire_compiler(strategy)
        assert strategy.run() is result.action_bundle
        strategy.compiler.compile.assert_called_once()
        assert strategy.injected  # framework hook applied

    def test_sequence_compiles_first_intent(self):
        first, second = _swap_intent(), _swap_intent()
        strategy = _Strategy(IntentSequence(intents=[first, second]))
        _wire_compiler(strategy)
        assert strategy.run() is not None
        assert strategy.compiler.compile.call_args[0][0] is first

    def test_multiple_items_compiles_first_only(self):
        first, second = _swap_intent(), _swap_intent()
        strategy = _Strategy([first, second])
        _wire_compiler(strategy)
        assert strategy.run() is not None
        assert strategy.compiler.compile.call_args[0][0] is first
        assert strategy.compiler.compile.call_count == 1

    def test_inject_hook_failure_is_non_fatal(self):
        strategy = _Strategy(_swap_intent())
        strategy.inject_raises = True
        _wire_compiler(strategy)
        assert strategy.run() is not None

    def test_compile_failure_returns_none(self):
        from almanak.framework.intents.compiler import CompilationStatus

        strategy = _Strategy(_swap_intent())
        _wire_compiler(strategy, status=CompilationStatus.FAILED, error="no route")
        assert strategy.run() is None

    def test_decide_exception_returns_none(self):
        def _boom():
            raise RuntimeError("snapshot unavailable")

        strategy = _Strategy(_boom)
        assert strategy.run() is None


class TestRunMulti:
    """Covers run_multi(): raw DecideResult pass-through + _current_intent tracking."""

    def test_none_result_sets_hold_and_returns_none(self):
        strategy = _Strategy(decide_result=None)
        assert strategy.run_multi() is None
        assert isinstance(strategy._current_intent, HoldIntent)
        assert strategy._current_intent.reason == "decide() returned None"

    def test_single_intent_passthrough(self):
        intent = _swap_intent()
        strategy = _Strategy(intent)
        assert strategy.run_multi() is intent
        assert strategy._current_intent is intent

    def test_sequence_returned_whole_and_first_tracked(self):
        first, second = _swap_intent(), _swap_intent()
        sequence = IntentSequence(intents=[first, second])
        strategy = _Strategy(sequence)
        assert strategy.run_multi() is sequence
        assert strategy._current_intent is first

    def test_list_returned_whole_and_first_item_tracked(self):
        first, second = _swap_intent(), _swap_intent()
        items = [first, second]
        strategy = _Strategy(items)
        assert strategy.run_multi() is items
        assert strategy._current_intent is first

    def test_list_with_leading_sequence_tracks_its_first(self):
        inner = _swap_intent()
        items = [IntentSequence(intents=[inner]), _swap_intent()]
        strategy = _Strategy(items)
        assert strategy.run_multi() is items
        assert strategy._current_intent is inner

    def test_empty_list_returned_and_current_intent_cleared(self):
        strategy = _Strategy([])
        strategy._current_intent = _swap_intent()  # stale from a prior iteration
        result = strategy.run_multi()
        assert result == []
        assert strategy._current_intent is None

    def test_decide_exception_returns_none(self):
        def _boom():
            raise RuntimeError("market data unavailable")

        strategy = _Strategy(_boom)
        assert strategy.run_multi() is None
