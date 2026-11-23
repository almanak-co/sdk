"""A transfer in flight is owned by both chains it touches and resolved by one of them."""

from types import SimpleNamespace
from typing import Literal

import pytest
from pydantic import ValidationError

from almanak.models import Model
from almanak.transfers import (
    Arrived,
    Refunded,
    Transfer,
    merged,
    opened,
    resolution_chain,
    resolved,
    visible,
)
from almanak.work import ChainWork, merge, project, transfer_fields
from tests.bridge_support import DESTINATION, ORIGIN, RECEIVED, SENT, THIRD, amount, transfer

ARRIVED = Arrived(received=amount(RECEIVED, 985))
REFUNDED = Refunded(returned=amount(SENT, 1_000))


class Flight(Model):
    """The fields the chain projection reads, with value in flight."""

    config: None = None
    work: tuple[ChainWork[None, None, None], ...]
    scheduler_cursor: int = 0
    initialized: bool = False
    decision: None = None
    plan: None = None
    pending: None = None
    integrity_error: str | None = None
    owned: tuple = ()
    baseline: tuple = ()
    closure: tuple = ()
    outcomes: tuple = ()
    delivered: int = 0
    memory: str = ""
    schedule: str = ""
    mode: Literal["running", "closed"] = "running"
    transfers: tuple[Transfer, ...] = ()


class Plain(Model):
    work: tuple = ()


@pytest.fixture
def unbound(monkeypatch):
    plain = SimpleNamespace(requests=None, external_effects=None)
    monkeypatch.setattr("almanak.connectors.composition.connector_for", lambda _: plain)


def flight(*transfers):
    chains = (ORIGIN, DESTINATION, THIRD)
    return Flight(work=tuple(ChainWork(chain=c) for c in chains), transfers=transfers)


def test_an_open_transfer_round_trips_and_carries_no_received_amount():
    item = transfer()
    assert item.open and item.resolution is None
    assert "received" not in item.model_dump_json()
    assert Transfer.model_validate_json(item.model_dump_json()) == item


@pytest.mark.parametrize("resolution", [ARRIVED, REFUNDED])
def test_a_resolved_transfer_round_trips_with_its_measured_amount(resolution):
    item = transfer(resolution=resolution)
    restored = Transfer.model_validate_json(item.model_dump_json())
    assert restored == item and type(restored.resolution) is type(resolution)
    assert resolution_chain(item) == (DESTINATION if resolution is ARRIVED else ORIGIN)


def test_an_arrival_may_differ_from_the_offer():
    assert transfer(resolution=ARRIVED).offered.amount != ARRIVED.received.amount


@pytest.mark.parametrize(
    ("changes", "cause"),
    [
        ({"destination": transfer().source, "offered": amount(SENT, 990)}, "two different chains"),
        ({"sent": amount(RECEIVED, 1_000)}, "their own wallet assets"),
        ({"offered": amount(SENT, 990)}, "their own wallet assets"),
        ({"sent": amount(SENT, 0)}, "positive sent amount"),
        ({"resolution": Arrived(received=amount(SENT, 985))}, "asset of the wallet"),
        ({"resolution": Refunded(returned=amount(RECEIVED, 1_000))}, "asset of the wallet"),
        ({"resolution": Arrived(received=amount(RECEIVED, 0))}, "measured positive amount"),
        ({"reference": "two words"}, "reference"),
    ],
)
def test_a_transfer_refuses_inconsistent_scope_or_evidence(changes, cause):
    with pytest.raises(ValidationError, match=cause):
        transfer(**changes)


def test_an_open_transfer_has_no_resolving_chain():
    with pytest.raises(ValueError, match="no resolving chain"):
        resolution_chain(transfer())


def test_opening_appends_one_open_transfer_per_identity():
    first = transfer()
    assert opened((), first) == (first,)
    with pytest.raises(ValueError, match="only once"):
        opened((first,), transfer())
    with pytest.raises(ValueError, match="without a resolution"):
        opened((), transfer(resolution=ARRIVED))
    assert len(opened((first,), transfer("deposit-2"))) == 2


def test_resolving_closes_exactly_the_named_open_transfer():
    first, second = transfer(), transfer("deposit-2")
    changed = resolved((first, second), second.identity, REFUNDED)
    assert changed == (first, transfer("deposit-2", resolution=REFUNDED))
    with pytest.raises(ValueError, match="resolved again"):
        resolved(changed, second.identity, ARRIVED)
    with pytest.raises(ValueError, match="exactly one"):
        resolved((first,), second.identity, ARRIVED)
    with pytest.raises(ValidationError, match="asset of the wallet"):
        resolved((first,), first.identity, Arrived(received=amount(SENT, 985)))


def test_a_chain_sees_the_transfers_it_sends_or_receives():
    outbound, elsewhere = transfer(), transfer("deposit-2", leaves=DESTINATION, reaches=THIRD)
    assert visible((outbound, elsewhere), ORIGIN) == (outbound,)
    assert visible((outbound, elsewhere), DESTINATION) == (outbound, elsewhere)
    assert visible((outbound, elsewhere), THIRD) == (elsewhere,)


def test_the_source_chain_opens_and_refunds():
    hidden = transfer("deposit-0", leaves=DESTINATION, reaches=THIRD)
    first = transfer()
    assert merged((hidden,), (first,), ORIGIN) == (hidden, first)
    refunded = transfer(resolution=REFUNDED)
    assert merged((hidden, first), (refunded,), ORIGIN) == (hidden, refunded)


def test_the_destination_chain_records_the_arrival_and_keeps_order():
    first, second = transfer(), transfer("deposit-2", leaves=DESTINATION, reaches=THIRD)
    arrived = transfer(resolution=ARRIVED)
    assert merged((first, second), (arrived, second), DESTINATION) == (arrived, second)


@pytest.mark.parametrize(
    ("changed", "chain", "cause"),
    [
        ((), ORIGIN, "cannot drop"),
        ((transfer(resolution=ARRIVED),), ORIGIN, "chain that measured"),
        ((transfer(resolution=REFUNDED),), DESTINATION, "chain that measured"),
        ((transfer(offered=amount(RECEIVED, 1)),), ORIGIN, "alter or reopen"),
        ((transfer(), transfer("deposit-2")), DESTINATION, "only on its source chain"),
        ((transfer(), transfer("deposit-2", resolution=REFUNDED)), ORIGIN, "without a resolution"),
        ((transfer(), transfer()), ORIGIN, "only once"),
    ],
)
def test_a_scoped_change_is_refused(changed, chain, cause):
    with pytest.raises(ValueError, match=cause):
        merged((transfer(),), changed, chain)


def test_a_scoped_change_cannot_reopen_reorder_or_replace_a_resolution():
    first, second = transfer(resolution=ARRIVED), transfer("deposit-2")
    for changed in ((transfer(), second), (second, first), (transfer(resolution=REFUNDED), second)):
        with pytest.raises(ValueError, match="alter or reopen"):
            merged((first, second), changed, ORIGIN)


def test_a_checkpoint_without_transfers_is_untouched():
    assert transfer_fields(Plain(), ORIGIN) == {}
    assert transfer_fields(Plain(), ORIGIN, Plain()) == {}


def test_the_chain_projection_shows_and_merges_transfers(unbound):
    outbound, elsewhere = transfer(), transfer("deposit-2", leaves=DESTINATION, reaches=THIRD)
    state = flight(outbound, elsewhere)
    assert project(state, ORIGIN).transfers == (outbound,)
    assert project(state, THIRD).transfers == (elsewhere,)
    local = project(state, DESTINATION)
    assert local.transfers == (outbound, elsewhere)
    arrived = local.model_copy(update={"transfers": (transfer(resolution=ARRIVED), elsewhere)})
    assert merge(state, DESTINATION, arrived).transfers == (arrived.transfers[0], elsewhere)
    with pytest.raises(ValueError, match="chain that measured"):
        merge(
            state,
            ORIGIN,
            project(state, ORIGIN).model_copy(update={"transfers": (arrived.transfers[0],)}),
        )
    with pytest.raises(ValueError, match="cannot drop"):
        merge(state, DESTINATION, local.model_copy(update={"transfers": (elsewhere,)}))
