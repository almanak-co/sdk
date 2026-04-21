"""Tests for the ``RunIterationState`` dataclass (Phase 3b refactor).

``RunIterationState`` is the per-iteration mutable bag threaded through
the ``_step_*`` helpers on ``StrategyRunner``. These tests pin the default
construction shape and the mutation points the step helpers rely on.
"""

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.intents.vocabulary import HoldIntent
from almanak.framework.runner.strategy_runner import RunIterationState


class _DummyStrategy:
    strategy_id = "test-strategy"
    chain = "arbitrum"


class TestRunIterationStateDefaults:
    def test_required_fields_bind(self) -> None:
        strategy = _DummyStrategy()
        start = datetime.now(UTC)
        state = RunIterationState(
            strategy=strategy,  # type: ignore[arg-type]
            strategy_id=strategy.strategy_id,
            start_time=start,
        )
        assert state.strategy is strategy
        assert state.strategy_id == "test-strategy"
        assert state.start_time is start

    def test_optional_fields_default_empty(self) -> None:
        state = RunIterationState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            strategy_id="s",
            start_time=datetime.now(UTC),
        )
        assert state.market is None
        assert state.decide_result is None
        assert state.intents == []
        assert state.teardown_mode is None
        assert state.pre_balances == {}
        assert state.intent_tokens == []


class TestRunIterationStateMutation:
    def test_state_mutations_persist_across_step_boundaries(self) -> None:
        """Each step helper writes one or two fields; the driver reads them back."""
        state = RunIterationState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            strategy_id="s",
            start_time=datetime.now(UTC),
        )

        # _step_build_snapshot writes market
        state.market = object()
        # _step_decide writes decide_result
        state.decide_result = HoldIntent(reason="unit test")
        # _step_extract_intents writes intents
        state.intents = [HoldIntent(reason="a"), HoldIntent(reason="b")]
        # _step_teardown_and_cb_gate writes teardown_mode
        state.teardown_mode = None
        # _step_snapshot_pre_balances writes pre_balances + intent_tokens
        state.pre_balances = {"USDC": Decimal("1000"), "ETH": Decimal("1")}
        state.intent_tokens = ["USDC", "ETH"]

        assert state.market is not None
        assert isinstance(state.decide_result, HoldIntent)
        assert len(state.intents) == 2
        assert state.teardown_mode is None
        assert state.pre_balances["USDC"] == Decimal("1000")
        assert state.intent_tokens == ["USDC", "ETH"]

    def test_default_collections_are_independent_per_instance(self) -> None:
        """Regression guard: mutable default_factory collections must not be shared."""
        a = RunIterationState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            strategy_id="a",
            start_time=datetime.now(UTC),
        )
        b = RunIterationState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            strategy_id="b",
            start_time=datetime.now(UTC),
        )
        a.intents.append(HoldIntent(reason="only-a"))
        a.pre_balances["USDC"] = Decimal("1")
        a.intent_tokens.append("USDC")

        assert b.intents == []
        assert b.pre_balances == {}
        assert b.intent_tokens == []
