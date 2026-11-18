"""Generated strategy memory survives freeze, persistence as JSON and thaw unchanged."""

import dataclasses
import math
from typing import Annotated

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel, PlainSerializer

from almanak.memory_codec import freeze, hydrate
from almanak.strategy_api import Memory, StrategyState

finite = st.floats(allow_nan=False, allow_infinity=False)
scalars = st.none() | st.booleans() | st.integers() | finite | st.text(max_size=16)
json_values = st.recursive(
    scalars,
    lambda children: (
        st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=8), children, max_size=4)
    ),
    max_leaves=16,
)


class State(StrategyState):
    count: int = 0
    label: str = ""
    prices: list[float] = []
    flags: dict[str, bool] = {}
    note: str | None = None


NAMESPACE = {"State": State}
states = st.builds(
    State,
    count=st.integers(),
    label=st.text(max_size=16),
    prices=st.lists(finite, max_size=6),
    flags=st.dictionaries(st.text(max_size=8), st.booleans(), max_size=4),
    note=st.none() | st.text(max_size=16),
)


def persisted(memory):
    """What a later run reads back from durable storage."""
    return Memory.model_validate_json(memory.model_dump_json())


@given(st.dictionaries(st.text(max_size=8), json_values, max_size=6), st.integers(1, 2**64))
def test_untyped_memory_survives_freeze_and_thaw(values, version):
    memory = Memory(version=version, values=values)
    frozen = freeze({}, memory)
    # Compare JSON, not models: pydantic equality treats True, 1 and 1.0 as equal.
    assert frozen.model_dump_json() == memory.model_dump_json()
    assert hydrate({}, persisted(frozen)).model_dump_json() == memory.model_dump_json()


@given(states)
def test_typed_memory_survives_freeze_and_thaw(state):
    frozen = freeze(NAMESPACE, state)
    restored = hydrate(NAMESPACE, persisted(frozen))
    assert type(restored) is State
    assert restored.model_dump_json() == state.model_dump_json()
    assert freeze(NAMESPACE, restored) == frozen


non_finite = st.sampled_from([math.inf, -math.inf, math.nan])


@given(non_finite, st.lists(finite, max_size=3))
def test_untyped_non_finite_floats_are_refused_with_their_field(value, before):
    # JSON has no inf or nan; writing null would hand the strategy None on its next decision.
    memory = Memory(values={"series": {"closes": [*before, value]}})
    with pytest.raises(ValueError, match=rf"values\.series\.closes\[{len(before)}\]"):
        freeze({}, memory)


@given(states, non_finite, st.data())
def test_typed_non_finite_floats_are_refused_with_their_field(state, value, data):
    index = data.draw(st.integers(0, len(state.prices)))
    prices = [*state.prices[:index], value, *state.prices[index:]]
    with pytest.raises(ValueError, match=rf"prices\[{index}\]"):
        freeze(NAMESPACE, state.updated(prices=prices))


class Reading(BaseModel):
    price: float


@dataclasses.dataclass
class Window:
    low: float


def substituted(sentinel, value):
    """A JSON-only serializer that writes value for sentinel, invisible to a Python dump."""
    return PlainSerializer(lambda v: value if v == sentinel else v, when_used="json")


@given(non_finite, finite)
def test_non_finite_floats_in_nested_models_and_json_serializers_are_refused(value, sentinel):
    class Nested(StrategyState):
        reading: Reading | None = None
        window: Window | None = None
        mark: Annotated[float, substituted(sentinel, value)] = 0.0

    namespace = {"State": Nested}
    for state, field in (
        (Nested(reading=Reading(price=value)), r"reading\.price"),
        (Nested(window=Window(low=value)), r"window\.low"),
        (Nested(mark=sentinel), "mark"),
    ):
        with pytest.raises(ValueError, match=rf"memory field {field} is"):
            freeze(namespace, state)
