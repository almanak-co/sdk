"""An Across transfer ends by evidence: a measured arrival or a measured refund.

Each chain's tick is driven as the coordinator and the gateway drive it: the page is
proposed in the chain's projection, committed, re-derived independently and merged.
The chains are fakes with evenly spaced blocks. No run mode is declared for Across, so
the `config` fixture installs a fork declaration for the test alone.
"""

from datetime import timedelta

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
from almanak.connectors.across.settlement_reader import decoded_fill
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


def proven(config, *transfers, **changes):
    """Transfers the destination chain has already proven unfilled."""
    marked = (transfer.model_copy(update={"unfilled": True}) for transfer in transfers)
    return flying(config, *marked, **changes)


def expire(state, config):
    """Tick the destination chain past the fill deadline: its scan proves every open
    transfer unfilled, through the same commit, verification and merge as any page."""
    chain = there(FILL_DEADLINE + 10)
    for _ in range(3 * len(state.transfers)):
        state = settle(state, config, chain)
    return state


def later(transfer, seconds):
    """Another transfer of the same route whose fill deadline is `seconds` later."""
    identifier, relay_hash, quoted_at = transfer.reference.split(":")
    return transfer.model_copy(
        update={
            "reference": f"{int(identifier) + 500}:{relay_hash}:{quoted_at}",
            "fill_deadline": transfer.fill_deadline + timedelta(seconds=seconds),
        }
    )


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
    tx = changes.pop("tx", FILL_TX)
    payer = changes.pop("payer", POOL_THERE if changes.get("fill_type") == 2 else RELAYER)
    rows = [
        fill(POOL_THERE, data, **changes),
        payment(TOKEN_THERE, payer, fund(config, "destination"), moved),
    ]
    chain.views["fillStatuses(bytes32)"] = 2
    return chain.mine(number, tx, rows)


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


def test_a_slow_fill_paid_by_anyone_but_the_spoke_pool_is_refused(config):
    chain = there()
    state = settle(flying(config, flight(config)), config, chain)
    filled(config, chain, chain.head + 1, fill_type=2, payer=RELAYER)
    with pytest.raises(DataError, match="was not paid by the SpokePool") as error:
        settle(state, config, chain)
    assert error.value.kind == "accounting"


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
    # The page is the destination's proof: the transfer is unfilled and still in flight.
    assert state.transfers[0].unfilled and project(state, FORK_ARB).transfers[0].unfilled
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


def test_a_refund_waits_for_the_destination_to_prove_the_transfer_unfilled(config):
    """The destination is unavailable: the deadline has passed and a refund-leaf payment
    is on the source chain, yet nothing is read. The source chain still opens transfers."""
    transfer, chain = flight(config), here(FILL_DEADLINE + 50)
    state = flying(config, transfer)
    refunded(config, chain, chain.head - 5, [AMOUNT])
    assert settle(state, config, chain) == state
    local = project(state, FORK_ARB)
    second = flight(config, 1)
    state = merge(state, FORK_ARB, local.model_copy(update={"transfers": (transfer, second)}))
    assert settle(state, config, chain) == state and state.transfers == (transfer, second)
    # The source chain cannot state the proof itself.
    claimed = local.model_copy(update={"transfers": proven(config, transfer).transfers})
    with pytest.raises(ValueError, match="chain that measured"):
        merge(flying(config, transfer), FORK_ARB, claimed)
    state = expire(state, config)
    assert [(t.open, t.unfilled) for t in state.transfers] == [(True, True), (True, True)]
    assert [page.expired for page in state.external_scans] == [False, True, False, True]
    assert state.outcomes == ()


def test_a_refund_of_a_proven_unfilled_transfer_is_measured_and_resolves_it(config):
    transfer, chain = flight(config), here(FILL_DEADLINE + 50)
    state = expire(flying(config, transfer), config)
    state = settle(state, config, chain)
    first = state.external_scans[-1]
    assert first.side == "refund" and first.start == chain.block(chain.at(FILL_DEADLINE))
    assert (first.outcome, first.unsolicited, state.transfers[0].open) == (None, None, True)
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


def test_one_refund_of_the_sum_resolves_every_proven_transfer(config):
    one, two = flight(config), flight(config, 1)
    chain = here(FILL_DEADLINE + 50)
    state = settle(proven(config, one, two), config, chain)
    refunded(config, chain, chain.head + 3, [2 * AMOUNT])
    state = settle(state, config, chain)
    assert state.outcomes[0].transfers == (one.reference, two.reference)
    assert [t.resolution.returned.amount for t in state.transfers] == [AMOUNT, AMOUNT]


def test_equal_transfers_refunded_in_two_bundles_resolve_oldest_first(config):
    one, two = flight(config, 1), flight(config)
    chain = here(FILL_DEADLINE + 50)
    state = settle(proven(config, one, two), config, chain)
    refunded(config, chain, chain.head + 3, [AMOUNT])
    state = delivered(settle(state, config, chain))
    # Equal deadlines: the lower reference is taken first, whatever the retained order.
    assert state.outcomes[0].transfers == (two.reference,)
    assert [t.open for t in state.transfers] == [True, False]
    refunded(config, chain, chain.head + 3, [AMOUNT], tx="0x" + "e4" * 32)
    state = settle(state, config, chain)
    assert state.outcomes[1].transfers == (one.reference,)
    older = later(one, -60)
    waiting = settlement.candidates(proven(config, one, older), config.target("move"), NOW * 2)
    assert [t.reference for t in waiting] == [older.reference, one.reference]


def test_a_refund_leaf_payment_that_matches_nothing_is_retained_and_stops_nothing(config):
    """A stranger deposits dust naming the fund as depositor and lets it expire."""
    from almanak.connectors.across.connector import AcrossProjection

    transfer, chain = flight(config), here(FILL_DEADLINE + 50)
    state = settle(proven(config, transfer), config, chain)
    refunded(config, chain, chain.head + 3, [5], tx="0x" + "d0" * 32)
    state = settle(state, config, chain)
    page = state.external_scans[-1]
    assert (page.unsolicited.amount.amount, page.unsolicited.tx_hash) == (5, "0x" + "d0" * 32)
    # No outcome, so no accounting issue that would stop new exposure on this chain.
    assert (page.outcome, state.outcomes, state.transfers[0].open) == (None, (), True)
    assert page.end == chain.block(chain.head)
    listed = AcrossProjection.report(state)["unattributed_refund_payments"]
    assert listed == [{"block": page.end.number, **page.unsolicited.model_dump(mode="json")}]
    refunded(config, chain, chain.head + 3, [AMOUNT])
    state = settle(state, config, chain)
    assert state.outcomes[0].transfers == (transfer.reference,)
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state
    settlement.audit(backend(config, chain), project(state, FORK_ARB))


def test_a_foreign_refund_of_an_equal_amount_resolves_the_transfer_and_ours_is_left(config):
    transfer, chain = flight(config), here(FILL_DEADLINE + 50)
    state = settle(proven(config, transfer), config, chain)
    refunded(config, chain, chain.head + 3, [AMOUNT], tx="0x" + "f0" * 32)
    state = delivered(settle(state, config, chain))
    assert state.outcomes[0].tx_hash == "0x" + "f0" * 32
    assert state.transfers[0].resolution.returned.amount == AMOUNT
    # The transfer's own refund pays the same amount later. Nothing waits for it.
    refunded(config, chain, chain.head + 3, [AMOUNT])
    chain.head += SPAN
    assert settle(state, config, chain) == state


def test_a_refund_in_the_deadline_block_itself_pays_no_transfer_of_that_deadline(config):
    """A transfer can still be filled in the block whose time equals its deadline."""
    chain = here(FILL_DEADLINE + 50)
    refunded(config, chain, chain.at(FILL_DEADLINE), [AMOUNT])
    state = settle(proven(config, flight(config)), config, chain)
    (page,) = state.external_scans
    assert page.start == page.end == chain.block(chain.at(FILL_DEADLINE))
    assert page.unsolicited.amount.amount == AMOUNT and state.transfers[0].open


def test_only_the_twelve_oldest_transfers_are_considered_for_one_refund(config):
    transfers = [flight(config, serial) for serial in range(13)]
    chain = here(FILL_DEADLINE + 50)
    state = settle(proven(config, *transfers), config, chain)
    refunded(config, chain, chain.head + 3, [13 * AMOUNT], tx="0x" + "d1" * 32)
    state = settle(state, config, chain)
    assert state.external_scans[-1].unsolicited.amount.amount == 13 * AMOUNT
    refunded(config, chain, chain.head + 3, [12 * AMOUNT])
    state = settle(state, config, chain)
    assert state.outcomes[0].transfers == tuple(t.reference for t in transfers[:12])
    assert [t.open for t in state.transfers] == [False] * 12 + [True]


def test_two_allocations_of_one_source_fund_and_asset_are_refused_at_configuration():
    from tests.across_support import allocation, parameters

    with pytest.raises(ValidationError, match="one allocation per source fund and sent asset"):
        parameters(allocation(), allocation(name="again"))


def test_the_refund_scan_jumps_to_a_later_deadline_and_waits_for_an_unproven_one(config):
    one, chain = flight(config), here(FILL_DEADLINE + 50)
    two = later(one, 6000)
    state = settle(proven(config, one), config, chain)
    refunded(config, chain, chain.head + 3, [AMOUNT])
    state = delivered(settle(state, config, chain))
    reached = state.external_scans[-1].end
    # A second transfer is in flight and not proven: the scan stays short of its deadline.
    transfers = (state.transfers[0], two)
    state = state.model_copy(update={"transfers": transfers})
    chain.head = chain.at(FILL_DEADLINE + 6000) + 40
    assert settle(state, config, chain) == state
    state = state.model_copy(
        update={"transfers": (transfers[0], two.model_copy(update={"unfilled": True}))}
    )
    state = settle(state, config, chain)
    page = state.external_scans[-1]
    assert page.start == chain.block(chain.at(FILL_DEADLINE + 6000))
    assert page.start.number > reached.number + SPAN


def test_a_refund_whose_leaf_and_payment_differ_is_refused(config):
    chain = here(FILL_DEADLINE + 50)
    state = settle(proven(config, flight(config), flight(config, 1)), config, chain)
    refunded(config, chain, chain.head + 3, [2 * AMOUNT], payments=[AMOUNT])
    with pytest.raises(DataError, match="did not pay the fund what its leaf names") as error:
        settle(state, config, chain)
    assert error.value.kind == "accounting" and state.transfers[0].open


def test_a_payment_from_the_spoke_pool_without_a_refund_leaf_is_not_a_refund(config):
    chain = here(FILL_DEADLINE + 50)
    state = settle(proven(config, flight(config)), config, chain)
    payee = fund(config, "source")
    chain.mine(chain.head + 1, "0x" + "5f" * 32, [payment(TOKEN_HERE, POOL_HERE, payee, AMOUNT)])
    # A leaf for another token names the fund but pays nothing in the sent asset.
    refunded(config, chain, chain.head + 1, [AMOUNT], payments=[], token="0x" + "44" * 20)
    chain.head += SPAN
    state = settle(state, config, chain)
    page = state.external_scans[-1]
    assert (page.outcome, page.unsolicited, state.transfers[0].open) == (None, None, True)
    assert len(page.events) == 1


def test_two_refunds_in_one_block_and_a_deferred_refund_are_refused(config):
    chain = here(FILL_DEADLINE + 50)
    state = settle(proven(config, flight(config)), config, chain)
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


def test_a_fill_found_for_a_transfer_marked_unfilled_halts_with_both(config):
    """The proof ends the arrival scan, so this state cannot be reached through it. Were
    a transfer ever marked without that page, a fill found for it stops the chain."""
    chain = there()
    state = proven(config, flight(config))
    filled(config, chain, chain.at(QUOTED_AT) + 3)
    with pytest.raises(DataError, match="proven unfilled or refunded and is also filled") as error:
        settle(state, config, chain)
    assert error.value.kind == "integrity" and FILL_TX in str(error.value)


def test_a_fill_in_the_first_block_and_one_in_the_deadline_block_are_both_found(config):
    chain = there()
    filled(config, chain, chain.at(QUOTED_AT))
    state = settle(flying(config, flight(config)), config, chain)
    (page,) = state.external_scans
    assert page.start == page.end == chain.block(chain.at(QUOTED_AT)) and page.outcome
    # The deadline block is exactly one in which the SpokePool still accepts a fill.
    chain = FakeChain(FORK_BASE, GENESIS, 1, 0)
    chain.head = chain.at(FILL_DEADLINE) - 100
    state = flying(config, flight(config))
    for _ in range(5):
        state = settle(state, config, chain)
    chain.head = chain.at(FILL_DEADLINE)
    assert chain.block(chain.head).timestamp == FILL_DEADLINE
    state = settle(state, config, chain)
    assert not any(page.expired for page in state.external_scans)
    filled(config, chain, chain.head)
    chain.head += 1
    state = settle(state, config, chain)
    assert state.outcomes[0].block.timestamp == FILL_DEADLINE and not state.transfers[0].unfilled


def test_a_relay_filled_twice_and_a_log_from_another_emitter_are_refused(config):
    chain = there()
    state = settle(flying(config, flight(config)), config, chain)
    filled(config, chain, chain.head + 1)
    filled(config, chain, chain.head + 1, tx="0x" + "f2" * 32)
    with pytest.raises(DataError, match="filled one relay twice"):
        settle(state, config, chain)
    chain.logs[-2] = {**chain.logs[-2], "address": "0x" + "66" * 20}
    answered = chain.logs
    chain._rpc = lambda method, params, allow_none=False: answered
    with pytest.raises(DataError, match="a log outside its query"):
        settle(state, config, chain)


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
    assert settle(state, config, chain) == state
    state = settle(expire(state, config), config, chain)
    # Proven unfilled is not closed: the refund is still owed on both chains' views.
    assert state.transfers[0].unfilled and state.mode == "teardown"
    for scope, seen in ((FORK_ARB, (observed(config),)), (FORK_BASE, ())):
        with pytest.raises(DataError) as error:
            validate_closure(project(state, scope), seen)
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


# A real deposit on Arbitrum One and its fill on Base, read from the two chains. The
# fill's log is kept as the chain returned it. The Base SpokePool answered
# `fillStatuses` with 2 (filled) under HASH at the fill's block and with 0 under the
# same relay hashed with the origin's chain number, so HASH is the contract's own key.
DEPOSIT_TX = "0xc0fb18ef5c5d4c633ede0ceb77cbfe882acb368dd2f4eaaa78ebf947ccd8b6cb"
REAL_FILL_TX = "0x1f6021e82723595d59ee42b992e13d3e28f9b19e7ed57c5252cfe999021762f8"
REAL_HASH = "0x95c216247297ae4375838e9f2d1bf6a2fa3ea29378659bcf4f1d13d1baa2dacf"
REAL_DEPOSITOR = "0x62a63f10d6a6689b90cca9acf2b3ab82f1576b86"
REAL_EXCLUSIVE = "0x000000000004598d17aad017bf0734a364c5588b"
REAL_DEPOSIT_ID = int("0x82f0a284fbb8a93294419bdd5fb3dab4e3b19949954d6da2d441e46d529506e7", 16)
REAL_FILL = {
    "address": POOL_THERE,
    "topics": [
        "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208",
        "0x000000000000000000000000000000000000000000000000000000000000a4b1",
        "0x82f0a284fbb8a93294419bdd5fb3dab4e3b19949954d6da2d441e46d529506e7",
        "0x000000000000000000000000394311a6aaa0d8e3411d8b62de4578d41322d1bd",
    ],
    "data": (
        "0x000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831"
        "000000000000000000000000833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        "0000000000000000000000000000000000000000000000000000000000989680"
        "0000000000000000000000000000000000000000000000000000000000989680"
        "000000000000000000000000000000000000000000000000000000000000a4b1"
        "000000000000000000000000000000000000000000000000000000006b040cc4"
        "000000000000000000000000000000000000000000000000000000006b040cc4"
        "000000000000000000000000000000000004598d17aad017bf0734a364c5588b"
        "00000000000000000000000062a63f10d6a6689b90cca9acf2b3ab82f1576b86"
        "00000000000000000000000062a63f10d6a6689b90cca9acf2b3ab82f1576b86"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000062a63f10d6a6689b90cca9acf2b3ab82f1576b86"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000989680"
        "0000000000000000000000000000000000000000000000000000000000000000"
    ),
    "blockNumber": hex(61185152),
    "logIndex": "0x0",
    "transactionHash": REAL_FILL_TX,
}


def test_the_relay_hash_equals_the_key_a_real_spoke_pool_filled():
    """Known answer: the relay is rebuilt from the deposit's own event fields, in the
    order the deposit states them, and hashed for another chain than its origin."""
    relay = (
        word(REAL_DEPOSITOR),
        word(REAL_DEPOSITOR),
        word(REAL_EXCLUSIVE),
        word(USDC.reference),
        word(BASE_USDC.reference),
        10_000_000,
        10_000_000,
        42161,
        REAL_DEPOSIT_ID,
        1_795_427_524,
        1_795_427_524,
        b"",
    )
    assert relay_hash(relay, node_number(BASE)) == REAL_HASH
    assert relay_hash(relay, 42161) != REAL_HASH
    # The fill's own log decodes to the same relay and is matched by that hash alone.
    assert decoded_fill(REAL_FILL)["relay"] == relay
    row, fill = settlement.matching_fill((REAL_FILL,), REAL_HASH, 8453)
    assert (row, fill["fill_type"], fill["updated_output"]) == (REAL_FILL, 0, 10_000_000)
    assert settlement.matching_fill((REAL_FILL,), REAL_HASH, 42161) is None


class Crash(Exception):
    """The coordinator process dies at a named fault point."""


class ChainCoordinator:
    """One chain's external-effects step as the coordinator runs it, over a durable store.

    `external_effects.progress` is the runtime's own: it asks the gateway for a page,
    passes the runtime's fault points and commits through the journal. The gateway side
    is the connector's reader over the fake chain, and the journal side commits in the
    chain's projection, re-derives the commit as the gateway does and merges it. Only the
    process around them is a fake; a process kill is proven by the supervised lifecycles.
    """

    def __init__(self, config, reader, store, crash=None):
        self.config, self.reader, self.store, self.crash = config, reader, store, crash
        self.gateway = self.journal = self
        self.pages = []

    def load(self):
        return project(self.store["state"], self.reader.chain)

    def external_reconcile(self):
        return settlement.reconcile(backend(self.config, self.reader), self.load())

    def update(self, change):
        local = self.load()
        committed = change(local)
        fake = backend(self.config, self.reader)
        assert gateway_effects.verified_commit(fake, local, committed) == committed
        self.store["state"] = merge(self.store["state"], self.reader.chain, committed)

    def fault(self, stage, page):
        self.pages.append((stage, page))
        if stage == self.crash:
            raise Crash(stage)

    def progress(self):
        from almanak import external_effects

        return external_effects.progress(self, self.load())


def arrivals(state):
    return [o for o in state.outcomes if getattr(o, "kind", None) == "across_arrival"]


def awaiting_arrival(config):
    """A durable store whose next destination page holds the transfer's mined fill."""
    chain = there()
    store = {"state": settle(flying(config, flight(config)), config, chain)}
    filled(config, chain, chain.head + 5)
    chain.head += 30
    return chain, store


def test_a_crash_between_arrival_evidence_and_its_commit_records_the_arrival_once(config):
    chain, store = awaiting_arrival(config)
    before = store["state"]
    crashed = ChainCoordinator(config, chain, store, crash="external_evidence")
    with pytest.raises(Crash):
        crashed.progress()
    ((stage, read),) = crashed.pages
    # The evidence was read and nothing was committed: the transfer is still in flight.
    assert stage == "external_evidence" and read.outcome.kind == "across_arrival"
    assert store["state"] == before and store["state"].transfers[0].open
    restarted = ChainCoordinator(config, chain, store)
    assert restarted.progress()
    (evidence, committed) = restarted.pages
    # The restart reads the same page again and commits it, once.
    assert evidence == ("external_evidence", read) and committed == ("external_commit", read)
    state = store["state"]
    assert arrivals(state) == [read.outcome] and state.external_scans[-1] == read
    assert state.external_scans.count(read) == 1
    assert state.transfers[0].resolution.received == read.outcome.received


def test_a_crash_after_the_arrival_commit_does_not_record_it_twice(config):
    from almanak import external_effects

    chain, store = awaiting_arrival(config)
    crashed = ChainCoordinator(config, chain, store, crash="external_commit")
    with pytest.raises(Crash):
        crashed.progress()
    (_, (stage, page)) = crashed.pages
    state = store["state"]
    assert stage == "external_commit" and arrivals(state) == [page.outcome]
    assert state.delivered == len(state.outcomes) - 1
    restarted = ChainCoordinator(config, chain, store)
    # A restarted coordinator delivers the committed arrival before any further page;
    # until then the commit refuses new external evidence.
    with pytest.raises(ValueError, match="initialized, delivered, new evidence"):
        external_effects.commit(restarted.load(), restarted.load(), page)
    store["state"] = delivered(state)
    with pytest.raises(ValueError, match="initialized, delivered, new evidence"):
        external_effects.commit(restarted.load(), restarted.load(), page)
    chain.head += 40
    while restarted.progress():
        pass
    final = store["state"]
    assert arrivals(final) == [page.outcome] and final.external_scans.count(page) == 1
    assert all(p.outcome is None for p in final.external_scans if p != page)
    assert final.transfers[0].resolution.received == page.outcome.received
