"""The Aster Pro demo withdraws on teardown whenever a deposit may have reached the venue."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.teardown import TeardownMode

_STRATEGY = Path(__file__).resolve().parents[4] / "strategies/internal/demo_catalog/aster_perps_basic/strategy.py"


def _demo(state: dict):
    spec = importlib.util.spec_from_file_location("aster_perps_basic_demo_strategy", _STRATEGY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    strategy = module.AsterPerpsBasicStrategy.__new__(module.AsterPerpsBasicStrategy)
    strategy.config = {"market": "SOL/USD", "deposit_usd": "2.0", "is_long": False}
    strategy.state = state
    return strategy


def _teardown_types(state: dict) -> list[IntentType]:
    return [i.intent_type for i in _demo(state).generate_teardown_intents(TeardownMode.SOFT)]


def test_a_deposit_sent_but_never_booked_is_withdrawn_on_teardown() -> None:
    strategy = _demo({"phase": "deposit"})
    strategy.decide(market=None)  # type: ignore[arg-type]
    assert _teardown_types(dict(strategy.state)) == [IntentType.PERP_WITHDRAW]


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({"phase": "deposit"}, []),
        ({"phase": "settle"}, [IntentType.PERP_WITHDRAW]),
        ({"phase": "close"}, [IntentType.PERP_CLOSE, IntentType.PERP_WITHDRAW]),
        ({"phase": "done"}, []),
    ],
)
def test_teardown_follows_where_the_money_can_be(state: dict, expected: list[IntentType]) -> None:
    assert _teardown_types(state) == expected
