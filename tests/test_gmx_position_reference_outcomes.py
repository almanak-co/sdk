"""Outcome memory of the GMX position references on Arbitrum and Avalanche.

The strategy files are loaded in-process and called with stand-in outcome contexts;
their sources stay pinned by the reviewed source-shape tests.
"""

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2.api import LiquidationResult

STRATEGIES = Path(__file__).parents[1] / "strategies"
NAMES = ("gmx_v2_arbitrum_position", "gmx_v2_avalanche_position")
OPENED = datetime(2026, 9, 1, 12, tzinfo=UTC)


def load(name):
    spec = importlib.util.spec_from_file_location(name, STRATEGIES / name / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(params=NAMES, scope="module")
def strategy(request):
    return load(request.param)


def context(strategy, result, **memory):
    return SimpleNamespace(result=result, memory=strategy.State(**memory))


def action(kind="increase_position", *, complete=True, success=True, name="main"):
    return SimpleNamespace(
        complete=complete,
        success=success,
        intent=SimpleNamespace(kind=kind, target=SimpleNamespace(allocation=name)),
        effective_at=OPENED,
    )


def liquidation(name="main"):
    value = LiquidationResult.model_construct(target=SimpleNamespace(allocation=name))
    return value


def test_completed_increase_records_its_opening_time(strategy):
    memory = strategy.on_outcome(context(strategy, action()))
    assert memory.opened_at == {"main": int(OPENED.timestamp())}
    assert memory.finished == ()


def test_intermediate_outcome_keeps_memory(strategy):
    current = context(strategy, action(complete=False), opened_at={"main": 1})
    assert strategy.on_outcome(current) is current.memory


def test_completed_exit_or_failed_increase_finishes_the_allocation_once(strategy):
    closed = strategy.on_outcome(context(strategy, action("decrease_position")))
    assert closed.finished == ("main",)
    failed = strategy.on_outcome(context(strategy, action(success=False)))
    assert failed.finished == ("main",) and failed.opened_at == {}
    again = context(strategy, action("decrease_position"), finished=("main",))
    assert strategy.on_outcome(again) is again.memory


def test_liquidation_finishes_its_allocation_without_duplicates(strategy):
    first = strategy.on_outcome(context(strategy, liquidation(), finished=("other",)))
    assert first.finished == ("other", "main")
    repeated = strategy.on_outcome(context(strategy, liquidation(), finished=("main",)))
    assert repeated.finished == ("main",)
