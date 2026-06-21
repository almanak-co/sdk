"""Regression tests for demo LP teardown state handling."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock


def _mock_intent(intent_type: str) -> MagicMock:
    intent = MagicMock()
    intent.intent_type.value = intent_type
    # Bare MagicMock auto-vivifies range_lower/range_upper into truthy mocks,
    # which the LP drift-tracking path would try to Decimal()-convert. Real
    # LP_OPEN intents carry these; tests that don't exercise drift set None.
    intent.range_lower = None
    intent.range_upper = None
    return intent


def test_traderjoe_lp_tracks_bin_ids_from_extracted_data() -> None:
    from almanak.demo_strategies.traderjoe_lp.strategy import TraderJoeLPStrategy

    strategy = TraderJoeLPStrategy.__new__(TraderJoeLPStrategy)
    strategy._position_bin_ids = []
    strategy.pool = "WAVAX/USDC/20"
    strategy.bin_step = 20
    strategy._deployment_id = "test-traderjoe"

    result = SimpleNamespace(bin_ids=None, extracted_data={"bin_ids": [1, 2, 3]})

    strategy.on_intent_executed(_mock_intent("LP_OPEN"), True, result)

    assert strategy._position_bin_ids == [1, 2, 3]


def test_traderjoe_lp_persists_and_restores_bin_ids(monkeypatch) -> None:
    from almanak.demo_strategies.traderjoe_lp.strategy import TraderJoeLPStrategy
    from almanak.framework.strategies import IntentStrategy

    monkeypatch.setattr(IntentStrategy, "get_persistent_state", lambda self: {}, raising=False)
    monkeypatch.setattr(IntentStrategy, "load_persistent_state", lambda self, state: None, raising=False)

    strategy = TraderJoeLPStrategy.__new__(TraderJoeLPStrategy)
    strategy._position_bin_ids = [11, 12, 13]

    state = strategy.get_persistent_state()
    assert state["position_bin_ids"] == [11, 12, 13]

    restored = TraderJoeLPStrategy.__new__(TraderJoeLPStrategy)
    restored._position_bin_ids = []
    restored.load_persistent_state(state)

    assert restored._position_bin_ids == [11, 12, 13]


def test_traderjoe_lp_teardown_intent_carries_known_bin_ids() -> None:
    from almanak.demo_strategies.traderjoe_lp.strategy import TraderJoeLPStrategy

    strategy = TraderJoeLPStrategy.__new__(TraderJoeLPStrategy)
    strategy._position_bin_ids = [101, 102, 103]
    strategy.pool = "WAVAX/USDC/20"

    intents = strategy.generate_teardown_intents(mode=SimpleNamespace(value="soft"))

    assert len(intents) == 1
    assert intents[0].protocol_params == {"bin_ids": [101, 102, 103]}


def test_uniswap_lp_close_clears_cached_position() -> None:
    from almanak.demo_strategies.uniswap_lp.strategy import UniswapLPStrategy

    strategy = UniswapLPStrategy.__new__(UniswapLPStrategy)
    strategy._current_position_id = "5443505"

    strategy.on_intent_executed(_mock_intent("LP_CLOSE"), True, SimpleNamespace())

    assert strategy._current_position_id is None


# NOTE: The ``traderjoe_lp_lifecycle`` regression tests (VIB-3296) that used to
# live here were relocated to
# ``strategies/internal/tests/unit/strategies/test_traderjoe_lp_lifecycle_teardown_regressions.py``
# when that demo was parked under ``strategies/internal/demo_catalog/`` (#2954).
# The tests above exercise the still-packaged golden demos and stay here.
