"""The Across reference's own decisions: when it bridges, revokes and stops asking."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.across.api import BridgeIntent, RevokeBridgeIntent
from tests.across_support import AMOUNT, CONFIG

SOURCE = Path(__file__).parents[1] / "strategies/across_usdc_arbitrum_base_bridge/strategy.py"


@pytest.fixture(scope="module")
def reference():
    spec = importlib.util.spec_from_file_location("across_reference", SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def decided(reference, memory, *, allowance=0, teardown=False, source=True):
    context = SimpleNamespace(
        memory=memory,
        teardown=teardown,
        config=CONFIG,
        actions=CONFIG,
        observation=lambda name: SimpleNamespace(allowance=allowance) if source else None,
        decision=lambda intent=None, reason=None: SimpleNamespace(intent=intent, reason=reason),
    )
    return reference.decide(context)


def acted(reference, memory, intent, status):
    result = SimpleNamespace(intent=intent, status=status, transfer=None)
    return reference.on_outcome(SimpleNamespace(memory=memory, result=result))


def test_a_failed_bridge_revokes_its_approval_once_and_then_holds(reference, monkeypatch):
    monkeypatch.setattr(reference, "bridge_amount", lambda config: AMOUNT)
    memory = reference.State()
    bridge = decided(reference, memory).intent
    assert isinstance(bridge, BridgeIntent)
    memory = acted(reference, memory, bridge, "failed")
    assert (memory.failed, memory.revoke_failed) == (True, False)
    revoke = decided(reference, memory, allowance=AMOUNT).intent
    assert isinstance(revoke, RevokeBridgeIntent)
    memory = acted(reference, memory, revoke, "failed")
    assert (memory.failed, memory.revoke_failed) == (True, True)
    # The allowance still stands, and a running strategy does not ask every tick.
    held = decided(reference, memory, allowance=AMOUNT)
    assert held.intent is None and "teardown clears it" in held.reason
    assert decided(reference, memory, source=False).intent is None
    # Teardown asks again for as long as the approval stands, and stops when it is gone.
    again = decided(reference, memory, allowance=AMOUNT, teardown=True).intent
    assert isinstance(again, RevokeBridgeIntent)
    cleared = acted(reference, memory, again, "completed")
    assert (cleared.failed, cleared.revoke_failed) == (True, False)
    assert decided(reference, cleared, teardown=True).intent is None
    assert decided(reference, cleared).intent is None


def test_teardown_revokes_a_standing_approval_and_never_bridges(reference):
    memory = reference.State()
    assert isinstance(
        decided(reference, memory, allowance=1, teardown=True).intent, RevokeBridgeIntent
    )
    assert decided(reference, memory, teardown=True).intent is None
