"""Unit tests for the shared ``MockBacktestStrategy``.

Issue #1701 consolidated three near-duplicate mock classes (previously
inline in ``almanak/framework/cli/backtest/sweep.py``):

- ``MockSweepStrategy`` (strategy_id="mock-sweep")
- ``MockOptimizeStrategy`` (strategy_id="mock-optimize")
- ``MockWorkerStrategy`` (strategy_id="mock-worker")

All three were behaviourally identical. These tests lock in the
consolidated class's behaviour: configurable id, config retention,
no-op decide.
"""

from __future__ import annotations

from almanak.framework.backtesting import (
    MockBacktestStrategy,
    make_mock_strategy_class,
)


class TestMockBacktestStrategy:
    def test_default_strategy_id(self) -> None:
        s = MockBacktestStrategy()
        assert s.strategy_id == "mock-backtest"

    def test_custom_strategy_id(self) -> None:
        s = MockBacktestStrategy(strategy_id="mock-sweep")
        assert s.strategy_id == "mock-sweep"

    def test_config_defaults_to_empty_dict(self) -> None:
        s = MockBacktestStrategy()
        assert s.config == {}

    def test_config_is_retained(self) -> None:
        cfg = {"a": 1, "b": [1, 2]}
        s = MockBacktestStrategy(cfg)
        assert s.config is cfg

    def test_decide_always_returns_none(self) -> None:
        s = MockBacktestStrategy()
        assert s.decide(market=None) is None
        assert s.decide(market={"price": 1}) is None


class TestMakeMockStrategyClass:
    def test_preserves_strategy_id_on_instances(self) -> None:
        cls = make_mock_strategy_class("mock-sweep")
        instance = cls({"cfg": True})
        assert instance.strategy_id == "mock-sweep"
        assert instance.config == {"cfg": True}

    def test_each_id_gets_independent_class(self) -> None:
        sweep_cls = make_mock_strategy_class("mock-sweep")
        optimize_cls = make_mock_strategy_class("mock-optimize")
        worker_cls = make_mock_strategy_class("mock-worker")

        assert sweep_cls is not optimize_cls
        assert optimize_cls is not worker_cls
        assert sweep_cls().strategy_id == "mock-sweep"
        assert optimize_cls().strategy_id == "mock-optimize"
        assert worker_cls().strategy_id == "mock-worker"

    def test_generated_class_name_is_descriptive(self) -> None:
        cls = make_mock_strategy_class("mock-sweep")
        # The class name is used in debug output / pytest failure messages,
        # so it must reflect the id.
        assert "mock_sweep" in cls.__name__
        assert "MockBacktestStrategy" in cls.__name__

    def test_constructor_matches_legacy_mock_signature(self) -> None:
        """Old mocks accepted `config` as the sole positional arg.

        Sweep / optimize / worker call sites pass the dict positionally and
        do not know about the `strategy_id` kwarg. The bound subclass must
        not regress that contract.
        """
        cls = make_mock_strategy_class("mock-worker")
        # Legacy call shape: positional config
        instance = cls({"k": "v"})
        assert instance.config == {"k": "v"}
        # Also supports no-args
        assert cls().config == {}

    def test_instance_decide_returns_none(self) -> None:
        cls = make_mock_strategy_class("mock-sweep")
        assert cls().decide(market=None) is None
