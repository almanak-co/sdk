"""The composed bridge-and-lend reference's own hooks, executed offline.

The hooks are the reference's, loaded from `strategies/`. The context is a stand-in
that records which action a decision asked for; the results are stand-ins of the
shapes the hooks read, except the arrival and the refund, which are the real types.
"""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.across.api import (
    ArrivalResult,
    BridgeIntent,
    RefundResult,
    RevokeBridgeIntent,
)
from almanak.models import AssetAmount
from tests.across_support import AMOUNT, BASE_USDC, BASE_WETH, USDC
from tests.test_across_records import config as config  # noqa: F401 - a fixture
from tests.test_across_settlement import OFFERED

SOURCE = Path(__file__).parents[1] / "strategies/arbitrum_base_bridge_and_lend/strategy.py"


@pytest.fixture(scope="module")
def reference():
    spec = importlib.util.spec_from_file_location("composed_bridge_reference", SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def configured(quantity="1000", **earn):
    """The shape `validate_config` reads: one bridge and one lending allocation."""
    bridge = SimpleNamespace(
        name="move",
        capital=AMOUNT,
        destination_account="base",
        route=SimpleNamespace(asset=USDC, destination_asset=BASE_USDC),
    )
    lending = SimpleNamespace(
        name="earn",
        account=earn.get("account", "base"),
        borrow=earn.get("borrow"),
        reserve=SimpleNamespace(
            asset=earn.get("asset", BASE_USDC), is_base=earn.get("is_base", True)
        ),
    )
    allocations = {"move": bridge, "earn": lending}
    return SimpleNamespace(
        positions=tuple(allocations.values()) + earn.get("extra", ()),
        allocation=allocations.__getitem__,
        asset_metadata={USDC: SimpleNamespace(decimals=6)},
        parameters=SimpleNamespace(bridge_quantity=quantity),
    )


class Actions:
    """Names the action a decision asked for, with its allocation and amount."""

    def __getattr__(self, kind):
        return lambda allocation, amount=None: (kind, allocation, amount)


def decided(reference, memory, *, side, teardown=False, allowance=0, lent=False):
    seen = {
        "source": {"move": SimpleNamespace(allowance=allowance)},
        "destination": {"earn": SimpleNamespace(allowance=allowance, lent=lent)},
        "neither": {},
    }[side]
    context = SimpleNamespace(
        memory=memory,
        teardown=teardown,
        config=configured(),
        actions=Actions(),
        observation=seen.get,
        require_observation=seen.__getitem__,
        decision=lambda intent=None, reason=None: SimpleNamespace(intent=intent, reason=reason),
    )
    return reference.decide(context)


@pytest.fixture(autouse=True)
def lending_position(reference, monkeypatch):
    """What the stand-in observation holds in the market, read as the reference reads it."""
    monkeypatch.setattr(
        reference, "owned", lambda observed: SimpleNamespace(empty=not observed.lent)
    )


def outcome(reference, memory, result):
    return reference.on_outcome(SimpleNamespace(memory=memory, result=result))


def acted(reference, memory, intent, status, **facts):
    return outcome(reference, memory, SimpleNamespace(intent=intent, status=status, **facts))


def bridge_intent(config, kind=BridgeIntent):  # noqa: F811
    component = config
    if kind is BridgeIntent:
        return component.bridge("move", AMOUNT)
    return component.revoke("move")


def amount(asset, value):
    return AssetAmount(asset=asset, amount=value)


def test_the_configuration_is_accepted_only_in_the_shape_the_reference_lends(reference):
    reference.validate_config(configured())
    assert reference.bridge_amount(configured()) == AMOUNT
    refusals = {
        "requires the allocations": configured(extra=(SimpleNamespace(name="other"),)),
        "exceeds the bridge allocation's capital": configured(quantity="1000.000001"),
        "belongs to the account the bridge sends to": configured(account="elsewhere"),
        "supplies the asset the bridge delivers": configured(asset=BASE_WETH),
        "only supplies and withdraws": configured(borrow=SimpleNamespace()),
    }
    for message, broken in refusals.items():
        with pytest.raises(ValueError, match=message):
            reference.validate_config(broken)
    # Collateral of the delivered asset is not the base supply this reference makes.
    with pytest.raises(ValueError, match="supplies the asset the bridge delivers"):
        reference.validate_config(configured(is_base=False))


def test_the_source_chain_bridges_once_and_then_waits(reference):
    memory = reference.State()
    assert decided(reference, memory, side="source").intent == ("bridge", "move", AMOUNT)
    sent = memory.updated(sent=AMOUNT)
    waiting = decided(reference, sent, side="source")
    assert waiting.intent is None and waiting.reason.startswith("In flight")
    # Neither chain's data: the decision states where the strategy stands and asks nothing.
    idle = decided(reference, memory, side="neither")
    assert idle.intent is None and idle.reason.startswith("Waiting for the source")


def test_the_destination_chain_lends_the_measured_arrival_once(reference):
    memory = reference.State(sent=AMOUNT)
    # In flight: nothing is supplied from the offer.
    assert decided(reference, memory, side="destination").intent is None
    arrived = memory.updated(received=OFFERED)
    assert decided(reference, arrived, side="destination").intent == ("supply", "earn", OFFERED)
    assert decided(reference, arrived, side="neither").reason == (
        "Arrived on the destination account"
    )
    supplied = arrived.updated(supplied=OFFERED)
    held = decided(reference, supplied, side="destination", lent=True)
    assert held.intent is None and held.reason.endswith("holding")
    refunded = memory.updated(returned=AMOUNT)
    done = decided(reference, refunded, side="destination")
    assert done.intent is None and done.reason.startswith("Refunded to the source")


def test_teardown_withdraws_what_is_lent_and_clears_approvals_and_never_bridges(reference):
    supplied = reference.State(sent=AMOUNT, received=OFFERED, supplied=OFFERED)
    withdraw = ("withdraw", "earn", None)
    assert decided(reference, supplied, side="destination", teardown=True, lent=True).intent == (
        withdraw
    )
    # A standing approval alone is cleared through the same withdrawal intent.
    assert decided(reference, supplied, side="destination", teardown=True, allowance=1).intent == (
        withdraw
    )
    assert decided(reference, supplied, side="destination", teardown=True).intent is None
    fresh = reference.State()
    assert decided(reference, fresh, side="source", teardown=True, allowance=1).intent == (
        "revoke",
        "move",
        None,
    )
    assert decided(reference, fresh, side="source", teardown=True).intent is None


def test_a_failed_step_stops_new_exposure_and_asks_to_clear_an_approval_once(reference):
    failed = reference.State(failed=True)
    assert decided(reference, failed, side="source").intent is None
    assert decided(reference, failed, side="source", allowance=1).intent == (
        "revoke",
        "move",
        None,
    )
    asked = failed.updated(revoke_failed=True)
    held = decided(reference, asked, side="source", allowance=1)
    assert held.intent is None and held.reason.startswith("A step failed")
    # Teardown asks again for as long as the approval stands.
    assert decided(reference, asked, side="source", allowance=1, teardown=True).intent == (
        "revoke",
        "move",
        None,
    )
    # A failure after the arrival withdraws nothing that was never lent, and what was.
    assert decided(reference, failed, side="destination").intent is None
    assert decided(reference, failed, side="destination", lent=True).intent == (
        "withdraw",
        "earn",
        None,
    )


def test_outcomes_record_the_deposit_the_arrival_the_refund_and_the_supply(
    reference,
    config,  # noqa: F811
):
    memory = reference.State()
    bridge, revoke = bridge_intent(config), bridge_intent(config, RevokeBridgeIntent)
    # The approval of a bridge opens no transfer and records nothing.
    assert acted(reference, memory, bridge, "completed", transfer=None) == memory
    sent = acted(
        reference, memory, bridge, "completed", transfer="opened", sent=amount(USDC, AMOUNT)
    )
    assert sent.sent == AMOUNT
    arrival = ArrivalResult.model_construct(received=amount(BASE_USDC, OFFERED))
    arrived = outcome(reference, sent, arrival)
    assert (arrived.sent, arrived.received) == (AMOUNT, OFFERED)
    refund = RefundResult.model_construct(returned=amount(USDC, AMOUNT))
    assert outcome(reference, sent, refund).returned == AMOUNT
    supply = SimpleNamespace(kind="supply")
    supplied = acted(reference, arrived, supply, "completed", assets=amount(BASE_USDC, OFFERED))
    assert supplied.supplied == OFFERED
    # A withdrawal, or a supply still on its approval step, records nothing.
    assert acted(reference, supplied, SimpleNamespace(kind="withdraw"), "completed") == supplied
    assert acted(reference, arrived, supply, "pending") == arrived
    assert acted(reference, arrived, supply, "failed").failed
    failed = acted(reference, memory, bridge, "failed")
    assert (failed.failed, failed.revoke_failed) == (True, False)
    stuck = acted(reference, failed, revoke, "failed")
    assert (stuck.failed, stuck.revoke_failed) == (True, True)
    assert acted(reference, stuck, revoke, "completed").revoke_failed is False
    # A bridge that fails after a failed revoke keeps that mark.
    assert acted(reference, stuck, bridge, "failed").revoke_failed is True
