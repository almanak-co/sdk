"""Tests for the ``SingleChainExecutionState`` / ``BridgeWaitState`` dataclasses.

Phase 3c introduced two per-execution mutable bags threaded through the step
helpers that ``_execute_single_chain`` and ``_execute_with_bridge_waiting``
dispatch to. These tests pin the default construction shape and the mutation
points the helpers rely on, so regressions in the contract surface at unit
level rather than inside the full execution flow.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.intents.vocabulary import HoldIntent
from almanak.framework.runner.strategy_runner import (
    BridgeWaitState,
    SingleChainExecutionState,
)


class _DummyStrategy:
    strategy_id = "test-strategy"
    chain = "arbitrum"


class _DummyOrchestrator:
    wallet_address = "0xabc"
    primary_chain = "arbitrum"


class TestSingleChainExecutionStateDefaults:
    def test_required_fields_bind(self) -> None:
        strategy = _DummyStrategy()
        intent = HoldIntent(reason="test")
        start = datetime.now(UTC)
        state = SingleChainExecutionState(
            strategy=strategy,  # type: ignore[arg-type]
            intent=intent,
            start_time=start,
            strategy_id=strategy.strategy_id,
        )
        assert state.strategy is strategy
        assert state.intent is intent
        assert state.start_time is start
        assert state.strategy_id == "test-strategy"

    def test_optional_fields_default_none(self) -> None:
        state = SingleChainExecutionState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            intent=HoldIntent(reason="x"),
            start_time=datetime.now(UTC),
        )
        # Setup handles default to None
        assert state.gateway_client is None
        assert state.rpc_url is None
        assert state.price_oracle is None
        assert state.polymarket_config is None
        assert state.clob_handler is None
        assert state.clob_client is None
        assert state.compiler is None
        assert state.state_machine is None
        assert state.pre_snapshot is None
        # Running-bookkeeping defaults
        assert state.last_execution_result is None
        assert state.last_execution_context is None
        assert state.last_bundle_metadata is None
        # Inputs
        assert state.total_intents == 1
        assert state.market is None
        assert state.record_metrics is True

    def test_record_metrics_flag_is_respected(self) -> None:
        state = SingleChainExecutionState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            intent=HoldIntent(reason="x"),
            start_time=datetime.now(UTC),
            record_metrics=False,
        )
        assert state.record_metrics is False


class TestSingleChainExecutionStateMutation:
    def test_state_mutations_persist_across_step_boundaries(self) -> None:
        """Each step helper writes a subset of fields; later helpers read them back."""
        state = SingleChainExecutionState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            intent=HoldIntent(reason="x"),
            start_time=datetime.now(UTC),
            strategy_id="s",
        )

        # _init_single_chain_state writes these
        state.gateway_client = object()
        state.price_oracle = {"USDC": "1.00", "ETH": "2000"}
        state.compiler = object()
        state.state_machine = object()
        state.pre_snapshot = object()

        # _single_chain_execute_step writes these
        state.last_bundle_metadata = {"expected_output_human": "1.23"}
        state.last_execution_context = object()
        state.last_execution_result = object()

        assert state.gateway_client is not None
        assert state.price_oracle == {"USDC": "1.00", "ETH": "2000"}
        assert state.compiler is not None
        assert state.state_machine is not None
        assert state.pre_snapshot is not None
        assert state.last_bundle_metadata == {"expected_output_human": "1.23"}
        assert state.last_execution_context is not None
        assert state.last_execution_result is not None


class TestBridgeWaitStateDefaults:
    def test_required_fields_bind(self) -> None:
        strategy = _DummyStrategy()
        orchestrator = _DummyOrchestrator()
        intents = [HoldIntent(reason="a"), HoldIntent(reason="b")]
        start = datetime.now(UTC)
        state = BridgeWaitState(
            strategy=strategy,  # type: ignore[arg-type]
            intents=intents,
            orchestrator=orchestrator,  # type: ignore[arg-type]
            start_time=start,
            strategy_id=strategy.strategy_id,
            first_intent=intents[0],
        )
        assert state.strategy is strategy
        assert state.intents is intents
        assert state.orchestrator is orchestrator
        assert state.start_time is start
        assert state.strategy_id == "test-strategy"
        assert state.first_intent is intents[0]

    def test_optional_fields_default_empty(self) -> None:
        state = BridgeWaitState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            intents=[],
            orchestrator=_DummyOrchestrator(),  # type: ignore[arg-type]
            start_time=datetime.now(UTC),
        )
        # Inputs that may be None
        assert state.resume_progress is None
        assert state.price_map is None
        assert state.price_oracle is None
        # Derived handles
        assert state.wallet_address == ""
        assert state.rpc_urls == {}
        assert state.gateway_client is None
        assert state.state_provider is None
        assert state.start_step_index == 0
        assert state.previous_amount_received is None
        assert state.progress is None
        # Running bookkeeping
        assert state.successful_count == 0
        assert state.failed_step is None
        assert state.error_message is None
        assert state.failed_result is None
        assert state.callback_fired is False
        assert state.current_intent is None


class TestBridgeWaitStateMutation:
    def test_default_collections_are_independent_per_instance(self) -> None:
        """Regression guard: mutable default_factory collections must not be shared."""
        a = BridgeWaitState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            intents=[],
            orchestrator=_DummyOrchestrator(),  # type: ignore[arg-type]
            start_time=datetime.now(UTC),
        )
        b = BridgeWaitState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            intents=[],
            orchestrator=_DummyOrchestrator(),  # type: ignore[arg-type]
            start_time=datetime.now(UTC),
        )
        a.rpc_urls["arbitrum"] = "https://a"

        assert b.rpc_urls == {}

    def test_failure_state_mutations_roundtrip(self) -> None:
        """The per-intent helper sets failed_step; finalize reads it back."""
        state = BridgeWaitState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            intents=[HoldIntent(reason="only")],
            orchestrator=_DummyOrchestrator(),  # type: ignore[arg-type]
            start_time=datetime.now(UTC),
        )

        state.failed_step = "step-1"
        state.error_message = "boom"
        state.callback_fired = True
        state.current_intent = state.intents[0]
        state.failed_result = object()

        assert state.failed_step == "step-1"
        assert state.error_message == "boom"
        assert state.callback_fired is True
        assert state.current_intent is state.intents[0]
        assert state.failed_result is not None

    def test_amount_chaining_mutation(self) -> None:
        """previous_amount_received is updated after each successful step."""
        state = BridgeWaitState(
            strategy=_DummyStrategy(),  # type: ignore[arg-type]
            intents=[],
            orchestrator=_DummyOrchestrator(),  # type: ignore[arg-type]
            start_time=datetime.now(UTC),
        )
        assert state.previous_amount_received is None
        state.previous_amount_received = Decimal("123.456")
        assert state.previous_amount_received == Decimal("123.456")
