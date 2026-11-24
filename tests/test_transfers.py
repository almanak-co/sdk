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
    proven_unfilled,
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
    item = transfer(unfilled=resolution is REFUNDED, resolution=resolution)
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
        ({"sent": amount(SENT, 0)}, "positive sent and offered"),
        ({"resolution": Arrived(received=amount(SENT, 985))}, "asset of the wallet"),
        (
            {"unfilled": True, "resolution": Refunded(returned=amount(RECEIVED, 1_000))},
            "asset of the wallet",
        ),
        ({"resolution": REFUNDED}, "needs a transfer proven unfilled"),
        ({"unfilled": True, "resolution": ARRIVED}, "needs a transfer proven unfilled"),
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
    with pytest.raises(ValidationError, match="needs a transfer proven unfilled"):
        resolved((first, second), second.identity, REFUNDED)
    proven = proven_unfilled((first, second), second.identity)
    assert proven[1].open and proven[1].unfilled and proven[0] == first
    with pytest.raises(ValueError, match="only one retained open transfer"):
        proven_unfilled(resolved((first,), first.identity, ARRIVED), first.identity)
    with pytest.raises(ValidationError, match="needs a transfer proven unfilled"):
        resolved(proven, second.identity, ARRIVED)
    changed = resolved(proven, second.identity, REFUNDED)
    assert changed == (first, transfer("deposit-2", unfilled=True, resolution=REFUNDED))
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
    refunded = transfer(unfilled=True, resolution=REFUNDED)
    with pytest.raises(ValueError, match="separate steps"):
        merged((hidden, first), (refunded,), ORIGIN)
    proven = transfer(unfilled=True)
    assert merged((hidden, proven), (refunded,), ORIGIN) == (hidden, refunded)


def test_only_the_destination_proves_a_transfer_unfilled_and_only_the_source_refunds_it():
    first, proven = transfer(), transfer(unfilled=True)
    assert merged((first,), (proven,), DESTINATION) == (proven,)
    with pytest.raises(ValueError, match="chain that measured"):
        merged((first,), (proven,), ORIGIN)
    with pytest.raises(ValueError, match="stays so"):
        merged((proven,), (first,), DESTINATION)
    # Never unfilled to arrived, on either chain; the model itself refuses the pair.
    with pytest.raises(ValidationError, match="needs a transfer proven unfilled"):
        transfer(unfilled=True, resolution=ARRIVED)
    with pytest.raises(ValueError, match="stays so"):
        merged((proven,), (transfer(resolution=ARRIVED),), DESTINATION)
    refunded = transfer(unfilled=True, resolution=REFUNDED)
    with pytest.raises(ValueError, match="chain that measured"):
        merged((proven,), (refunded,), DESTINATION)
    with pytest.raises(ValueError, match="alter or reopen"):
        merged((refunded,), (proven,), ORIGIN)
    with pytest.raises(ValueError, match="without a resolution"):
        merged((), (proven,), ORIGIN)
    with pytest.raises(ValueError, match="without a resolution"):
        opened((), proven)


def test_the_destination_chain_records_the_arrival_and_keeps_order():
    first, second = transfer(), transfer("deposit-2", leaves=DESTINATION, reaches=THIRD)
    arrived = transfer(resolution=ARRIVED)
    assert merged((first, second), (arrived, second), DESTINATION) == (arrived, second)


@pytest.mark.parametrize(
    ("changed", "chain", "cause"),
    [
        ((), ORIGIN, "cannot drop"),
        ((transfer(resolution=ARRIVED),), ORIGIN, "chain that measured"),
        ((transfer(unfilled=True, resolution=REFUNDED),), DESTINATION, "chain that measured"),
        ((transfer(offered=amount(RECEIVED, 1)),), ORIGIN, "alter or reopen"),
        ((transfer(), transfer("deposit-2")), DESTINATION, "only on its source chain"),
        (
            (transfer(), transfer("deposit-2", unfilled=True, resolution=REFUNDED)),
            ORIGIN,
            "without a resolution",
        ),
        ((transfer(), transfer()), ORIGIN, "only once"),
    ],
)
def test_a_scoped_change_is_refused(changed, chain, cause):
    with pytest.raises(ValueError, match=cause):
        merged((transfer(),), changed, chain)


def test_a_scoped_change_cannot_reopen_reorder_or_replace_a_resolution():
    first, second = transfer(resolution=ARRIVED), transfer("deposit-2")
    for changed in (
        (transfer(), second),
        (second, first),
        (transfer(unfilled=True, resolution=REFUNDED), second),
    ):
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


PRODUCTION = DESTINATION.model_copy(update={"environment": "production"})


@pytest.mark.parametrize(
    ("changes", "cause"),
    [
        ({"offered": amount(RECEIVED, 0)}, "positive sent and offered"),
        ({"unfilled": True, "resolution": Refunded(returned=amount(SENT, 1_001))}, "cannot exceed"),
    ],
)
def test_a_transfer_refuses_unmeasurable_amounts(changes, cause):
    with pytest.raises(ValidationError, match=cause):
        transfer(**changes)


def test_a_transfer_stays_within_one_environment_and_between_two_accounts():
    there = transfer().destination
    with pytest.raises(ValidationError, match="one environment"):
        transfer(destination=there.model_copy(update={"chain": PRODUCTION}))
    with pytest.raises(ValidationError, match="two different named accounts"):
        transfer(destination=there.model_copy(update={"account": "here"}))


@pytest.mark.parametrize(
    ("update", "chain"),
    [
        ({"resolution": Arrived(received=amount(RECEIVED, 0))}, DESTINATION),
        ({"resolution": Arrived(received=amount(SENT, 5))}, DESTINATION),
    ],
)
def test_a_scoped_change_built_without_validation_is_refused(update, chain):
    open_transfer = transfer()
    with pytest.raises(ValidationError):
        merged((open_transfer,), (open_transfer.model_copy(update=update),), chain)


def test_an_unvalidated_appended_transfer_is_refused():
    invalid = transfer().model_copy(update={"sent": amount(SENT, 0)})
    with pytest.raises(ValidationError):
        merged((), (invalid,), ORIGIN)


def test_two_chains_merge_one_after_the_other():
    first, second = transfer("deposit-1"), transfer("deposit-2")
    sent = merged((first,), (first, second), ORIGIN)
    arrived = resolved(visible(sent, DESTINATION), first.identity, ARRIVED)
    settled = merged(sent, arrived, DESTINATION)
    proven = merged(settled, proven_unfilled(settled, second.identity), DESTINATION)
    refunded = resolved(visible(proven, ORIGIN), second.identity, REFUNDED)
    final = merged(proven, refunded, ORIGIN)
    assert [t.resolution for t in final] == [ARRIVED, REFUNDED]
    with pytest.raises(ValueError, match="alter or reopen"):
        merged(final, sent, ORIGIN)


def test_a_chain_appends_and_refunds_in_one_change():
    first, second = transfer("deposit-1", unfilled=True), transfer("deposit-2")
    changed = (*resolved((first,), first.identity, REFUNDED), second)
    assert merged((first,), changed, ORIGIN) == changed


def test_two_fork_nodes_are_one_environment():
    here = ORIGIN.model_copy(update={"environment": "anvil:aaaa"})
    there = DESTINATION.model_copy(update={"environment": "anvil:bbbb"})
    source, destination = transfer().source, transfer().destination
    forked = transfer(
        source=source.model_copy(update={"chain": here}),
        destination=destination.model_copy(update={"chain": there}),
    )
    assert forked.open
    with pytest.raises(ValidationError, match="one environment"):
        transfer(destination=destination.model_copy(update={"chain": there}))


def test_a_fork_prefix_alone_names_no_environment():
    from almanak.gateway.environments import same_environment

    bare = ORIGIN.model_copy(update={"environment": "anvil:"})
    node = DESTINATION.model_copy(update={"environment": "anvil:bbbb"})
    live = DESTINATION.model_copy(update={"environment": "mainnet"})
    assert not same_environment(bare, bare) and not same_environment(bare, node)
    assert not same_environment(node, live) and same_environment(live, live)
