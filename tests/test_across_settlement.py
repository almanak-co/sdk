"""An Across transfer ends by evidence: a measured arrival or a measured refund.

Each chain's tick is driven as the coordinator and the gateway drive it: the page is
proposed in the chain's projection, committed, re-derived independently and merged.
The chains are fakes with evenly spaced blocks. No run mode is declared for Across, so
the `config` fixture installs a fork declaration for the test alone.
"""

import pytest
from pydantic import ValidationError

from almanak.connectors.across import settlement
from almanak.connectors.across.asset_effects import effects
from almanak.connectors.across.domain import spoke_pool
from almanak.connectors.across.lifecycle import ownership, validate_closure
from almanak.connectors.across.operations import word
from almanak.connectors.across.receipts import opened_transfer
from almanak.connectors.across.relay import (
    deposit_reference,
    deposited_relay,
    node_number,
    parse_reference,
    relay_hash,
)
from almanak.connectors.across.settlement_reader import MAXIMUM_EVENT_BLOCK_SPAN as SPAN
from almanak.connectors.across.state import Checkpoint
from almanak.gateway import external_effects as gateway_effects
from almanak.gateway.ethereum import DataError
from almanak.work import merge, project, work_for
from tests.across_settlement_support import (
    RELAYER,
    FakeChain,
    backend,
    fill,
    payment,
    refund_leaf,
    settle,
)
from tests.across_support import AMOUNT, BASE, BASE_USDC, NOW, USDC
from tests.test_across_deposit import DEPOSIT_ID, MINED
from tests.test_across_records import (
    FORK_ARB,
    FORK_BASE,
    checkpoint,
    decision,
    deposit,
    mined_deposit,
    observed,
)
from tests.test_across_records import config as config  # noqa: F401 - a fixture

OFFERED = 999_897_038
QUOTED_AT, FILL_DEADLINE = NOW - 69, NOW - 69 + 7200
GENESIS = NOW - 20_000
POOL_HERE, POOL_THERE = spoke_pool(FORK_ARB.chain_id).address, spoke_pool(BASE.chain_id).address
TOKEN_HERE, TOKEN_THERE = USDC.reference, BASE_USDC.reference
FILL_TX, REFUND_TX = "0x" + "f1" * 32, "0x" + "e2" * 32


def there(head_at=NOW + 20):
    chain = FakeChain(FORK_BASE, GENESIS, 2, 0)
    chain.head = chain.at(head_at)
    return chain


def here(head_at=NOW + 20):
    chain = FakeChain(FORK_ARB, GENESIS, 1, 0)
    chain.head = chain.at(head_at)
    return chain


def mined(config, serial=0):
    return {**MINED, "deposit_id": DEPOSIT_ID + serial}


def flight(config, serial=0):
    return opened_transfer(deposit(config), mined(config, serial))


def relay(config, serial=0):
    return deposited_relay(deposit(config), mined(config, serial))


def flying(config, *transfers, **changes):
    state = checkpoint(config, transfers=transfers, **changes)
    works = tuple(work.model_copy(update={"initialized": True}) for work in state.work)
    return state.model_copy(update={"work": works})


def delivered(state):
    return state.model_copy(update={"delivered": len(state.outcomes)})


def fund(config, side):
    target = config.target("move")
    return (target.fund if side == "source" else target.destination_fund).address


def filled(config, chain, number, serial=0, **changes):
    """Mine a relayer's fill of the deposit: the event, then the payment to the fund."""
    data = relay(config, serial)
    paid = changes.get("paid", OFFERED)
    moved = changes.pop("moved", paid)
    rows = [
        fill(POOL_THERE, data, **changes),
        payment(TOKEN_THERE, RELAYER, fund(config, "destination"), moved),
    ]
    chain.views["fillStatuses(bytes32)"] = 2
    return chain.mine(number, FILL_TX, rows)


def refunded(config, chain, number, amounts, tx=REFUND_TX, payments=None, token=TOKEN_HERE):
    payee = fund(config, "source")
    leaf = refund_leaf(POOL_HERE, token, [RELAYER, *[payee] * len(amounts)], [7, *amounts])
    paid = amounts if payments is None else payments
    rows = [*(payment(TOKEN_HERE, POOL_HERE, payee, amount) for amount in paid), leaf]
    return chain.mine(number, tx, rows)


def test_the_reference_retains_what_a_fill_match_needs(config):
    from tests.across_support import quote as venue_quote
    from tests.test_across_deposit import deposit as mainnet_deposit

    operation = mainnet_deposit()
    reference = deposit_reference(operation, MINED)
    assert parse_reference(reference.text) == reference
    assert (reference.deposit_id, reference.quoted_at) == (DEPOSIT_ID, venue_quote().timestamp)
    data = deposited_relay(operation, MINED)
    assert reference.relay_hash == relay_hash(data, 8453) and data[7] == 42161
    # Every field of the relay and the destination chain are part of the hash.
    for index in range(len(data) - 1):
        other = 5 if isinstance(data[index], int) else b"\x01" * 32
        changed = (*data[:index], other, *data[index + 1 :])
        assert relay_hash(changed, 8453) != reference.relay_hash
    assert relay_hash(data, 42161) != reference.relay_hash
    # A fork node answers the isolated signing chain to its SpokePool on both sides.
    forked = deposited_relay(deposit(config), MINED)
    assert (forked[7], node_number(FORK_BASE), node_number(BASE)) == (31337, 31337, 8453)
    assert flight(config).reference.split(":")[1] == relay_hash(forked, 31337)
    for text in ("7", "7:0x12:9", "x:0x" + "00" * 32 + ":9", "07:0x" + "00" * 32 + ":9"):
        with pytest.raises(DataError, match="malformed"):
            parse_reference(text)


def test_an_arrival_is_measured_on_the_destination_chain_and_resolves_the_transfer(config):
    transfer, chain = flight(config), there()
    state = settle(flying(config, transfer), config, chain)
    # The first page anchors the scan at the last block not after the quote and is kept.
    (first,) = state.external_scans
    assert first.start == chain.block(chain.at(QUOTED_AT)) and first.end.number == chain.head
    assert (first.outcome, first.expired, state.transfers) == (None, False, (transfer,))
    bisected = chain.pins
    assert settle(state, config, chain) == state
    chain.head += 40
    # A shorter empty page is read again, not committed, and the scan is not bisected again.
    assert settle(state, config, chain) == state and chain.pins - bisected < 12
    filled(config, chain, chain.head + 5)
    chain.head += 30
    state = settle(state, config, chain)
    page = state.external_scans[-1]
    arrival = page.outcome
    assert page.start.number == first.end.number + 1 and page.end == arrival.block
    assert arrival.block == chain.block(chain.head - 30) and arrival.fill_type == "fast"
    assert (arrival.received.amount, arrival.offered.amount) == (OFFERED, OFFERED)
    assert state.transfers[0].resolution.received == arrival.received
    assert state.outcomes == (arrival,) and project(state, FORK_ARB).outcomes == ()
    result = arrival.strategy_result()
    assert (result.kind, result.transfer, result.tx_hash) == (
        "bridge_arrival",
        transfer.reference,
        FILL_TX,
    )
    (credit,) = effects(arrival)
    assert (credit.key.account, credit.key.chain, credit.amount) == ("base", FORK_BASE, OFFERED)
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state
    # Nothing further is scanned for a settled transfer.
    assert settle(delivered(state), config, chain) == delivered(state)
    settlement.audit(backend(config, chain), project(state, FORK_BASE))


def test_a_slow_fill_below_the_offer_is_a_measurement(config):
    chain = there()
    state = settle(flying(config, flight(config)), config, chain)
    filled(config, chain, chain.head + 1, paid=OFFERED - 500, fill_type=2)
    state = settle(state, config, chain)
    arrival = state.outcomes[0]
    assert (arrival.fill_type, arrival.received.amount) == ("slow", OFFERED - 500)
    assert arrival.offered.amount == OFFERED and arrival.accounting_issues == ()


def test_a_fill_of_another_relay_under_the_same_deposit_identifier_is_not_ours(config):
    chain = there()
    state = settle(flying(config, flight(config)), config, chain)
    data = relay(config)
    stranger = (data[0], word("0x" + "99" * 20), *data[2:])
    rows = [fill(POOL_THERE, stranger), fill(POOL_THERE, data, message_hash=b"\x05" * 32)]
    chain.mine(chain.head + 1, "0x" + "a7" * 32, rows)
    chain.head += SPAN
    state = settle(state, config, chain)
    page = state.external_scans[-1]
    # The page is full: it retains both logs and attributes neither.
    assert (len(page.events), page.outcome, state.transfers[0].open) == (2, None, True)
    assert page.end.number - page.start.number + 1 == SPAN


@pytest.mark.parametrize(
    ("changes", "kind", "cause"),
    [
        ({"moved": OFFERED - 1}, "accounting", "not the 999897038 its event states"),
        ({"recipient": word("0x" + "99" * 20)}, "integrity", "another recipient"),
    ],
)
def test_a_fill_that_differs_from_its_event_is_refused(config, changes, kind, cause):
    chain = there()
    state = settle(flying(config, flight(config)), config, chain)
    filled(config, chain, chain.head + 1, **changes)
    with pytest.raises(DataError, match=cause) as error:
        settle(state, config, chain)
    assert error.value.kind == kind


def test_an_arrival_scan_ends_past_the_fill_deadline_and_cross_checks_the_status(config):
    chain = there()
    state = settle(flying(config, flight(config)), config, chain)
    chain.head = chain.at(FILL_DEADLINE) + 1
    # The deadline is further than one page away: a full empty page comes first.
    state = settle(state, config, chain)
    assert not state.external_scans[-1].expired
    assert state.external_scans[-1].end.number - state.external_scans[-1].start.number + 1 == SPAN
    chain.views["fillStatuses(bytes32)"] = 2
    with pytest.raises(DataError, match="filled outside its scanned pages"):
        settle(state, config, chain)
    chain.views["fillStatuses(bytes32)"] = 1
    state = settle(state, config, chain)
    page = state.external_scans[-1]
    assert page.expired and page.outcome is None and state.transfers[0].open
    assert page.end.timestamp > FILL_DEADLINE
    chain.head += 500
    assert settle(state, config, chain) == state


def test_a_fill_event_without_a_filled_status_is_refused(config):
    chain = there()
    state = settle(flying(config, flight(config)), config, chain)
    filled(config, chain, chain.head + 1)
    chain.views["fillStatuses(bytes32)"] = 0
    with pytest.raises(DataError, match="no filled status"):
        settle(state, config, chain)


def test_a_refund_is_read_only_after_the_fill_deadline_and_resolves_the_transfer(config):
    transfer, chain = flight(config), here()
    state = flying(config, transfer)
    # Before the deadline nothing is read: a refund is never inferred from time alone.
    refunded(config, chain, chain.at(NOW + 10), [AMOUNT], tx="0x" + "0a" * 32)
    assert settle(state, config, chain) == state
    chain.head = chain.at(FILL_DEADLINE) + 50
    state = settle(state, config, chain)
    (first,) = state.external_scans
    assert first.side == "refund" and first.start == chain.block(chain.at(FILL_DEADLINE))
    assert (first.outcome, state.transfers) == (None, (transfer,))
    refunded(config, chain, chain.head + 100, [AMOUNT])
    state = settle(state, config, chain)
    refund = state.outcomes[0]
    assert refund.transfers == (transfer.reference,) and refund.returned.amount == AMOUNT
    assert state.transfers[0].resolution.returned == transfer.sent
    assert state.external_scans[-1].end == refund.block == chain.block(chain.head)
    result = refund.strategy_result()
    assert (result.kind, result.transfers, result.tx_hash) == (
        "bridge_refund",
        (transfer.reference,),
        REFUND_TX,
    )
    (credit,) = effects(refund)
    assert (credit.key.account, credit.key.chain, credit.amount) == ("arb", FORK_ARB, AMOUNT)
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state
    assert project(state, FORK_BASE).outcomes == ()


def test_one_refund_of_the_sum_resolves_every_expired_transfer(config):
    one, two = flight(config), flight(config, 1)
    chain = here(FILL_DEADLINE + 50)
    state = settle(flying(config, one, two), config, chain)
    refunded(config, chain, chain.head + 3, [2 * AMOUNT])
    state = settle(state, config, chain)
    assert state.outcomes[0].transfers == (one.reference, two.reference)
    assert [t.resolution.returned.amount for t in state.transfers] == [AMOUNT, AMOUNT]


@pytest.mark.parametrize(
    ("amounts", "payments", "kind", "cause"),
    [
        # Two equal transfers and a refund of one: it cannot be told which.
        ([AMOUNT], None, "accounting", "matches 2 sets"),
        ([AMOUNT + 1], None, "accounting", "matches 0 sets"),
        ([2 * AMOUNT], [AMOUNT], "accounting", "did not pay the fund what its leaf names"),
    ],
)
def test_a_refund_that_cannot_be_attributed_stops_with_its_cause(
    config, amounts, payments, kind, cause
):
    chain = here(FILL_DEADLINE + 50)
    state = settle(flying(config, flight(config), flight(config, 1)), config, chain)
    refunded(config, chain, chain.head + 3, amounts, payments=payments)
    with pytest.raises(DataError, match=cause) as error:
        settle(state, config, chain)
    assert error.value.kind == kind and state.transfers[0].open


def test_a_payment_from_the_spoke_pool_without_a_refund_leaf_is_not_a_refund(config):
    chain = here(FILL_DEADLINE + 50)
    state = settle(flying(config, flight(config)), config, chain)
    payee = fund(config, "source")
    chain.mine(chain.head + 1, "0x" + "5f" * 32, [payment(TOKEN_HERE, POOL_HERE, payee, AMOUNT)])
    # A leaf for another token names the fund but pays nothing in the sent asset.
    refunded(config, chain, chain.head + 1, [AMOUNT], payments=[], token="0x" + "44" * 20)
    chain.head += SPAN
    state = settle(state, config, chain)
    assert state.external_scans[-1].outcome is None and state.transfers[0].open
    assert len(state.external_scans[-1].events) == 1


def test_two_refunds_in_one_block_and_a_deferred_refund_are_refused(config):
    chain = here(FILL_DEADLINE + 50)
    state = settle(flying(config, flight(config)), config, chain)
    chain.views["getRelayerRefund(address,address)"] = AMOUNT
    chain.head += SPAN
    with pytest.raises(DataError, match="unresolved until the fund claims it") as error:
        settle(state, config, chain)
    assert error.value.kind == "refund_deferred"
    chain.views.clear()
    number = chain.head - 5
    refunded(config, chain, number, [AMOUNT])
    refunded(config, chain, number, [AMOUNT], tx="0x" + "e3" * 32)
    with pytest.raises(DataError, match="share one block") as error:
        settle(state, config, chain)
    assert error.value.kind == "ordering"


def test_a_fill_seen_after_a_refund_halts_with_both(config):
    source, destination = here(FILL_DEADLINE + 50), there(NOW + 20)
    state = settle(flying(config, flight(config)), config, destination)
    state = settle(state, config, source)
    refunded(config, source, source.head + 3, [AMOUNT])
    state = delivered(settle(state, config, source))
    assert state.transfers[0].resolution.kind == "refunded"
    filled(config, destination, destination.head + 4)
    with pytest.raises(DataError, match="was refunded and is also filled") as error:
        settle(state, config, destination)
    assert error.value.kind == "integrity" and FILL_TX in str(error.value)


def test_the_source_keeps_working_while_the_destination_is_unavailable(config):
    """The destination never ticks here; the source opens a second transfer and refunds."""
    one, two = flight(config), flight(config, 1)
    source = here(FILL_DEADLINE + 50)
    state = flying(config, one)
    local = project(state, FORK_ARB)
    state = merge(state, FORK_ARB, local.model_copy(update={"transfers": (one, two)}))
    assert state.transfers == (one, two)
    state = settle(state, config, source)
    refunded(config, source, source.head + 3, [2 * AMOUNT])
    state = settle(state, config, source)
    assert not any(t.open for t in state.transfers)


def test_teardown_waits_on_both_chains_and_closes_once_the_transfer_is_resolved(config):
    transfer, chain = flight(config), there()
    state = flying(config, transfer, mode="teardown")
    for scope, seen in ((FORK_ARB, (observed(config),)), (FORK_BASE, ())):
        with pytest.raises(DataError) as error:
            validate_closure(project(state, scope), seen)
        assert error.value.kind == "transfer_in_flight"
    state = settle(state, config, chain)
    filled(config, chain, chain.head + 1)
    state = delivered(settle(state, config, chain))
    assert state.mode == "teardown"
    for scope, seen in ((FORK_ARB, (observed(config),)), (FORK_BASE, ())):
        local = project(state, scope)
        validate_closure(local, seen)
        state = merge(state, scope, local.model_copy(update={"mode": "closed"}))
    assert state.mode == "closed" and all(work.closed for work in state.work)


def test_a_transfer_opened_towards_a_closed_chain_reopens_it(config):
    """The destination has nothing left and closes while the source's signed deposit is
    still unmined. Once that deposit opens a transfer the destination owes its arrival."""
    state = flying(config, mode="teardown")
    local = project(state, FORK_BASE)
    state = merge(state, FORK_BASE, local.model_copy(update={"mode": "closed"}))
    assert work_for(state, FORK_BASE).closed and state.mode == "teardown"
    local = project(state, FORK_ARB)
    state = merge(state, FORK_ARB, local.model_copy(update={"transfers": (flight(config),)}))
    assert not work_for(state, FORK_BASE).closed and state.mode == "teardown"
    assert project(state, FORK_BASE).mode == "teardown"


def test_the_gateway_refuses_a_page_that_differs_from_the_chain(config):
    chain = there()
    before = project(flying(config, flight(config)), FORK_BASE)
    fake = backend(config, chain)
    page = settlement.reconcile(fake, before)

    def commit(changed):
        from almanak import external_effects

        return gateway_effects.verified_commit(
            fake, before, external_effects.commit(before, before, changed)
        )

    assert commit(page).external_scans == (page,)
    later = page.model_copy(update={"start": chain.block(page.start.number + 1)})
    with pytest.raises(DataError, match="changed its retained bounded cursor"):
        commit(later)
    forged = page.model_copy(update={"events": ({"address": POOL_THERE},)})
    with pytest.raises(ValueError, match="differs from independently verified"):
        commit(forged)
    wide = page.model_copy(update={"end": chain.block(page.start.number + SPAN)})
    chain.head += 2 * SPAN
    with pytest.raises(DataError, match="exceeds its bounded span"):
        commit(wide)


def test_an_audit_refuses_retained_pages_that_are_no_longer_canonical(config):
    chain = there()
    state = settle(flying(config, flight(config)), config, chain)
    filled(config, chain, chain.head + 1)
    state = project(settle(state, config, chain), FORK_BASE)
    fake = backend(config, chain)
    settlement.audit(fake, state)
    settlement.audit(backend(config, here()), state)
    chain.receipts[FILL_TX] = {**chain.receipts[FILL_TX], "status": "0x0"}
    with pytest.raises(DataError, match="receipt has changed"):
        settlement.audit(fake, state)
    chain.logs.clear()
    with pytest.raises(DataError, match="events are no longer canonical"):
        settlement.audit(fake, state)
    chain.genesis += 1
    with pytest.raises(DataError, match="boundaries are no longer canonical"):
        settlement.audit(fake, state)


def test_a_scan_page_keeps_one_side_and_its_own_settlement(config):
    chain = there()
    state = settle(flying(config, flight(config)), config, chain)
    filled(config, chain, chain.head + 1)
    page = settle(state, config, chain).external_scans[-1]
    for changes, cause in (
        ({"side": "refund"}, "explicit execution scope"),
        ({"transfer": None}, "follows one transfer"),
        ({"expired": True}, "differs from its page"),
        ({"end": chain.block(page.end.number + 1)}, "settlement block"),
        ({"transfer": "9:0x" + "00" * 32 + ":1"}, "differs from the transfer"),
    ):
        with pytest.raises(ValidationError, match=cause):
            type(page)(**{**dict(page), **changes})


def opened(monkeypatch, config, **changes):
    """A deposit reconciled by the real gateway path, recorded and delivered."""
    outcome = mined_deposit(monkeypatch, config)
    state = flying(config, **changes)
    state = state.model_copy(update={**ownership(state, outcome), "outcomes": (outcome,)})
    return delivered(state), outcome.transfer


def close(state, config):
    """Each chain's teardown tick with nothing left to do: verify closure, then close."""
    for scope, seen in ((FORK_ARB, (observed(config),)), (FORK_BASE, ())):
        local = project(state, scope)
        validate_closure(local, seen)
        state = merge(state, scope, local.model_copy(update={"mode": "closed"}))
    return state


def test_a_deposit_arrives_is_delivered_and_the_deployment_closes(monkeypatch, config):
    """Component flow, not a process: each step is the real gateway or coordinator code
    for that step, joined here by hand instead of by a running coordinator."""
    state, transfer = opened(monkeypatch, config, mode="teardown")
    assert transfer.open and project(state, FORK_BASE).outcomes == ()
    chain = there()
    state = settle(state, config, chain)
    filled(config, chain, chain.head + 1)
    state = settle(state, config, chain)
    deposit_outcome, arrival = state.outcomes
    # The undelivered arrival is the destination chain's next delivery.
    assert state.delivered == 1 and project(state, FORK_BASE).outcomes == (arrival,)
    result = arrival.strategy_result()
    assert (result.kind, result.received.amount) == ("bridge_arrival", OFFERED)
    assert result.offered == transfer.offered and deposit_outcome.transfer == transfer
    with pytest.raises(DataError) as error:
        validate_closure(project(delivered(state), FORK_ARB), (observed(config, allowance=1),))
    assert error.value.kind == "remaining_obligations"
    assert close(delivered(state), config).mode == "closed"


def test_a_deposit_is_refunded_is_delivered_and_the_deployment_closes(monkeypatch, config):
    state, transfer = opened(monkeypatch, config, mode="teardown")
    chain = here(FILL_DEADLINE + 50)
    state = settle(state, config, chain)
    with pytest.raises(DataError) as error:
        close(state, config)
    assert error.value.kind == "transfer_in_flight"
    refunded(config, chain, chain.head + 3, [AMOUNT])
    state = settle(state, config, chain)
    refund = state.outcomes[-1]
    assert project(state, FORK_ARB).outcomes[-1] == refund
    result = refund.strategy_result()
    assert (result.kind, result.returned.amount) == ("bridge_refund", AMOUNT)
    assert close(delivered(state), config).mode == "closed"


def test_the_destination_tick_decides_with_nothing_owned_and_may_not_bridge(monkeypatch, config):
    """The destination shares the decision schedule but holds no allocation: its
    projection owns nothing, and a bridge decided there is refused as routed elsewhere."""
    from almanak.work import validate_local_work

    state, _ = opened(monkeypatch, config)
    local = project(state, FORK_BASE)
    assert (local.owned, local.outcomes, len(local.transfers)) == ((), (), 1)
    work = work_for(state, FORK_BASE).model_copy(update={"decision": decision(config)})
    with pytest.raises(ValueError, match="routed to another chain"):
        validate_local_work(work)
