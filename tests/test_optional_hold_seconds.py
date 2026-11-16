"""`hold_seconds` is optional: omitted, the allocation has no scheduled exit."""

import importlib.util
from copy import deepcopy
from datetime import UTC, datetime
from functools import partial
from types import SimpleNamespace

import pytest
import yaml

from almanak.configuration.unresolved import parse_inputs
from almanak.configuration.yaml import parse
from almanak.strategy import StrategyDefinition
from tests.legacy_references import (
    before_hold_guard,
    before_hold_guard_digests,
    reviewed_current_imports,
    reviewed_hold_guard,
)
from tests.reference_parameters import authored_parameters
from tests.test_dx_schemas import ROOT, validator
from tests.test_reference_conversion import (
    effective,
    legacy_snapshot,
    reads_hold,
    resolved_positions,
)

REFERENCES = [
    "uniswap_v3_base_lp",
    "uniswap_v4_lp",
    "curve_ethereum_stable_lp",
    "aerodrome_slipstream_base_lp",
    "gmx_v2_arbitrum_position",
]


def reference(name):
    return yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())


def resolve(raw, name):
    inputs = parse_inputs(parse(yaml.safe_dump(raw))).inputs
    return resolved_positions(inputs, effective(legacy_snapshot(name), "config.yaml"))


def without_hold(raw):
    raw = deepcopy(raw)
    for allocation in raw["strategy"]["allocations"].values():
        allocation.pop("hold_seconds")
    return raw


@pytest.mark.parametrize("name", REFERENCES)
def test_an_allocation_without_hold_seconds_validates_and_resolves(name):
    raw = without_hold(reference(name))
    validator("strategy.schema.json").validate(raw)
    for allocation in resolve(raw, name).values():
        assert "hold_seconds" not in allocation


@pytest.mark.parametrize("name", REFERENCES)
def test_a_stated_hold_resolves_as_before(name):
    for allocation in resolve(reference(name), name).values():
        assert allocation["hold_seconds"] > 0


def test_v4_fee_collection_need_not_precede_an_absent_hold():
    raw = without_hold(reference("uniswap_v4_lp"))
    (allocation,) = raw["strategy"]["allocations"].values()
    assert allocation["collect_seconds"]
    (resolved,) = resolve(raw, "uniswap_v4_lp").values()
    assert resolved["collect_seconds"] > 0


def test_v4_fee_collection_must_still_precede_a_stated_hold():
    raw = reference("uniswap_v4_lp")
    (allocation,) = raw["strategy"]["allocations"].values()
    allocation["hold_seconds"] = allocation["collect_seconds"]
    with pytest.raises(ValueError, match="fee collection must precede scheduled closure"):
        resolve(raw, "uniswap_v4_lp")


def test_a_reference_states_a_hold_only_when_its_strategy_reads_it():
    dead = [
        path.parent.name
        for path in sorted((ROOT / "strategies").glob("*/config.yaml"))
        if "hold_seconds" in path.read_text() and not reads_hold(path.parent.name)
    ]
    assert not dead, f"references state a hold their strategy never reads: {dead}"


def strategy_module(name):
    path = ROOT / "strategies" / name / "strategy.py"
    spec = importlib.util.spec_from_file_location(f"{name}_hold_reference", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Actions:
    def __getattr__(self, kind):
        return lambda allocation, **_: (kind, allocation)


def decision_context(strategy, memory, observed, hold, teardown):
    allocation = SimpleNamespace(
        name=observed.allocation, hold_seconds=hold, entry_amount=1, entry_minimum_output=1
    )
    return SimpleNamespace(
        memory=memory,
        teardown=teardown,
        actions=Actions(),
        observations=(observed,),
        require_observation=lambda _: observed,
        config=SimpleNamespace(allocation=lambda _: allocation),
        decision=lambda intent=None, reason=None: intent,
    )


ENTERED = 1_000
LATER = datetime.fromtimestamp(ENTERED + 10**7, UTC)


def uniswap_v3_lp(hold, teardown):
    strategy = strategy_module("uniswap_v3_base_lp")
    done = ("increase_liquidity", "decrease_liquidity", "collect_liquidity_fees")
    memory = strategy.State(minted_at={"lp": ENTERED}, completed={"lp": done})
    observed = SimpleNamespace(
        allocation="lp",
        effective_at=LATER,
        owned=SimpleNamespace(position=SimpleNamespace(liquidity=2), entry_complete=True),
    )
    return strategy.decide(decision_context(strategy, memory, observed, hold, teardown))


def gmx_swap(hold, teardown, chain="arbitrum"):
    strategy = strategy_module(f"gmx_v2_{chain}_swap")
    memory = strategy.State(held_at={"Main": ENTERED})
    observed = SimpleNamespace(
        allocation="Main", block=SimpleNamespace(timestamp=int(LATER.timestamp()))
    )
    return strategy.decide(decision_context(strategy, memory, observed, hold, teardown))


@pytest.mark.parametrize(
    "decide,exit_kind",
    [
        (uniswap_v3_lp, ("close_liquidity", "lp")),
        (gmx_swap, ("exit", "Main")),
        (partial(gmx_swap, chain="avalanche"), ("exit", "Main")),
    ],
)
def test_an_entered_reference_without_a_hold_holds_until_teardown(decide, exit_kind):
    assert decide(None, teardown=False) is None
    assert decide(None, teardown=True) == exit_kind
    assert decide(60, teardown=False) == exit_kind


def test_the_atr_breakout_refuses_a_missing_hold_clearly():
    strategy = strategy_module("uniswap_v3_base_atr_breakout")
    config = SimpleNamespace(
        allocations=[SimpleNamespace(range=None, band=None, hold_seconds=None)],
        parameters=authored_parameters("uniswap_v3_base_atr_breakout"),
    )
    with pytest.raises(ValueError, match="needs hold_seconds"):
        strategy.validate_config(config)


GUARDED = [
    path
    for path in sorted((ROOT / "strategies").glob("*/strategy.py"))
    if "allocation.hold_seconds is not None" in path.read_text()
]

# One pre-review form of each guarded shape: the guard inserts exactly these lines.
UNGUARDED_SHAPES = (
    "    due = (\n"
    "        started is not None\n"
    "        and int(observed.effective_at.timestamp()) >= started + allocation.hold_seconds\n"
    "    )\n",
    "    elapsed = held_at is not None and observed.block.timestamp >= held_at + "
    "allocation.hold_seconds\n",
    "    expired = current.bought_at is not None and (\n"
    "        int(observed.effective_at.timestamp()) >= current.bought_at + "
    "allocation.hold_seconds\n"
    "    )\n",
    "def validate_config(config):\n"
    "    if any(a.hold_seconds > HOLD_LIMIT for a in config.allocations):\n"
    '        raise ValueError("limit")\n',
)


@pytest.mark.parametrize("shape", UNGUARDED_SHAPES)
def test_the_reviewed_hold_guard_round_trips_each_shape(shape):
    guarded = reviewed_hold_guard(shape)
    assert guarded != shape
    assert "hold_seconds is not None" in guarded or "hold_seconds is None" in guarded
    assert reviewed_hold_guard(guarded) == guarded
    assert shape in before_hold_guard(guarded)


@pytest.mark.parametrize("path", GUARDED, ids=lambda path: path.parent.name)
def test_every_guarded_reference_is_its_own_reviewed_form(path):
    current = StrategyDefinition.read(path)
    assert reviewed_hold_guard(current.source) == current.source
    assert reviewed_current_imports(current) == current
    candidates = before_hold_guard(current.source)
    assert current.source in candidates and len(candidates) > 1
    assert all(reviewed_hold_guard(text) == current.source for text in candidates)
    assert len(before_hold_guard_digests(current)) == len(candidates)
