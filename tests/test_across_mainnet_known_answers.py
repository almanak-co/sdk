"""Across code held to what mainnet and the venue's API really returned.

`tests/fixtures/across-mainnet-captures.json` holds read-only captures: one answer of
the venue's quote API with the Arbitrum block it was admitted against, one plain
`deposit` a stranger sent to the Arbitrum SpokePool, the `FilledRelay` on Base that
filled it, and one Arbitrum transaction that executed relayer refund roots. Nothing in
it was sent by this repository, and none of it is a lifecycle proof: each test runs one
reader or encoder over real bytes instead of bytes a fixture constructed.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import decode

from almanak.connectors.across import quote as venue
from almanak.connectors.across import settlement
from almanak.connectors.across import settlement_reader as chain
from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.across.observations import PoolState, require_pinned
from almanak.connectors.across.operations import (
    DEPOSIT_TYPES,
    DepositOperation,
    deposit_arguments,
    plan_call,
)
from almanak.connectors.across.receipts import movement, opened_transfer
from almanak.connectors.across.relay import parse_reference, relay_hash
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount, StrategyAccounts
from tests.across_mainnet_replay import Recorded, backend, deployment
from tests.across_support import ARBITRUM, BASE, BASE_USDC, USDC, allocation, eoa, parameters

CAPTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "across-mainnet-captures.json").read_text()
)
QUOTE, DEPOSIT, FILL, REFUND = (CAPTURES[name] for name in ("quote", "deposit", "fill", "refund"))
POOL_HERE, POOL_THERE = SPOKE_POOLS[ARBITRUM.chain_id], SPOKE_POOLS[BASE.chain_id]
INTEGRATOR_TAG = "73c0de"


def target_for(sender: str, recipient: str, amount: int, **route):
    accounts = StrategyAccounts(
        bindings=(eoa("arb", ARBITRUM, sender), eoa("base", BASE, recipient))
    )
    changes = {"capital": amount}
    if route:
        changes["route"] = allocation().route.model_copy(update=route)
    return parameters(allocation(**changes), accounts=accounts).target("move")


def config_for(target):
    """The deployment configuration whose one route `target` is."""
    accounts = StrategyAccounts(
        bindings=(
            eoa("arb", ARBITRUM, target.fund.address),
            eoa("base", BASE, target.destination_fund.address),
        )
    )
    return parameters(allocation(route=target.route), accounts=accounts)


def address(word: bytes) -> str:
    return "0x" + word[12:].hex()


# The venue's quote API.


def quoted():
    target = target_for("0x" + "11" * 20, QUOTE["recipient"], QUOTE["amount"])
    return target, venue.parse(QUOTE["answer"], QUOTE["amount"])


def test_a_real_venue_answer_is_parsed_retained_and_admitted_at_its_own_block():
    target, quote = quoted()
    pool = PoolState.model_validate(QUOTE["pool"])
    require_pinned(pool, ARBITRUM.chain_id)
    assert (
        list(venue.refusals(quote, target, QUOTE["amount"], pool, QUOTE["block_timestamp"])) == []
    )
    venue.admit(quote, target, QUOTE["amount"], pool, QUOTE["block_timestamp"])
    # The answer is retained whole, well inside the bound, and reads back as it came.
    assert json.loads(quote.response) == QUOTE["answer"]
    assert len(quote.response) < venue.MAX_ANSWER_BYTES // 8
    assert venue.Quote.model_validate_json(quote.model_dump_json()) == quote


def test_the_real_answer_names_the_pinned_pools_and_the_routes_own_tokens():
    target, quote = quoted()
    assert (quote.spoke_pool, quote.destination_spoke_pool) == (
        POOL_HERE.address,
        POOL_THERE.address,
    )
    assert (quote.input_token, quote.output_token) == (USDC.reference, BASE_USDC.reference)
    assert (quote.input_decimals, quote.output_decimals) == (6, 6)
    # The venue sends its timestamps as strings and its exclusivity as a small offset.
    assert isinstance(QUOTE["answer"]["timestamp"], str)
    assert 0 < quote.exclusivity_parameter <= venue.MAX_EXCLUSIVITY_OFFSET
    assert quote.fill_deadline - quote.timestamp == 7200
    # The quote is older than the block it is admitted at, by minutes, never ahead of it.
    assert 0 < QUOTE["block_timestamp"] - quote.timestamp < pool_buffer()


def pool_buffer() -> int:
    return QUOTE["pool"]["quote_time_buffer"]


def test_the_real_fee_is_admitted_at_its_own_size_and_refused_one_unit_below():
    target, quote = quoted()
    fee = QUOTE["amount"] - quote.output_amount
    assert 0 < fee < venue.fee_ceiling(QUOTE["amount"], target.route.max_fee_bps)
    pool = PoolState.model_validate(QUOTE["pool"])
    # 1 bps of the amount is below the real fee, so the same answer is refused there.
    tight = target_for("0x" + "11" * 20, QUOTE["recipient"], QUOTE["amount"], max_fee_bps=1)
    assert venue.fee_ceiling(QUOTE["amount"], 1) < fee
    with pytest.raises(DataError, match="exceeds the ceiling") as refused:
        venue.admit(quote, tight, QUOTE["amount"], pool, QUOTE["block_timestamp"])
    assert refused.value.kind == "fee_above_ceiling"


def test_the_real_destination_pool_is_the_pinned_implementation():
    pool = PoolState.model_validate(QUOTE["destination_pool"])
    require_pinned(pool, BASE.chain_id)


# A real deposit and its fill.


def real_deposit() -> DepositOperation:
    """The stranger's deposit as this connector would have signed it, from its calldata."""
    arguments = decode(DEPOSIT_TYPES, bytes.fromhex(DEPOSIT["input"][10:]))
    depositor, recipient, _, _, sent, offered, destination, relayer = arguments[:8]
    quoted_at, fill_deadline, exclusivity, message = arguments[8:]
    assert (destination, message) == (8453, b"")
    target = target_for(address(depositor), address(recipient), sent)
    quote = venue.Quote(
        source="across_api",
        input_amount=sent,
        output_amount=offered,
        timestamp=quoted_at,
        fill_deadline=fill_deadline,
        exclusive_relayer=address(relayer),
        exclusivity_parameter=exclusivity,
        spoke_pool=POOL_HERE.address,
        destination_spoke_pool=POOL_THERE.address,
        input_token=USDC.reference,
        input_decimals=6,
        output_token=BASE_USDC.reference,
        output_decimals=6,
        amount_too_low=False,
        response="{}",
    )
    return DepositOperation(
        target=target,
        amount=AssetAmount(asset=USDC, amount=sent),
        quote=quote,
        quote_time_buffer=3600,
    )


def test_the_deposit_calldata_equals_what_a_real_depositor_sent_the_spoke_pool():
    operation = real_deposit()
    call = plan_call(operation)
    assert (call.to, call.value) == (DEPOSIT["to"].lower(), 0)
    # The depositor's front end appended a three-byte integrator tag after the ABI
    # encoding, which the SpokePool does not read. Everything the contract decodes is
    # byte for byte what this connector encodes; it appends no tag of its own.
    assert DEPOSIT["input"] == call.data + INTEGRATOR_TAG
    assert DEPOSIT["from"].lower() == operation.target.fund.address
    assert operation.amount.amount > operation.quote.output_amount


def test_a_real_deposit_receipt_is_reconciled_and_opens_the_transfer_its_fill_matches():
    operation = real_deposit()
    moved, deposit = movement(DEPOSIT["receipt"], operation, DEPOSIT["block_timestamp"])
    assert moved == operation.amount.amount
    # The quote stated exclusivity as an offset; the event states the mined block's time
    # plus that offset, which the relay is hashed with.
    assert deposit["exclusivity_deadline"] == (
        DEPOSIT["block_timestamp"] + operation.quote.exclusivity_parameter
    )
    transfer = opened_transfer(operation, deposit)
    reference = parse_reference(transfer.reference)
    assert reference.relay_hash == FILL["relay_hash"]
    assert reference.quoted_at == operation.quote.timestamp
    # The deposit was mined after its quote and inside the SpokePool's quote buffer.
    assert operation.quote.timestamp < DEPOSIT["block_timestamp"] <= operation.deadline


def test_a_deposit_receipt_of_other_terms_is_refused():
    operation = real_deposit()
    other = operation.model_copy(
        update={"quote": operation.quote.model_copy(update={"output_amount": 1})}
    )
    with pytest.raises(DataError, match="differs from the signed deposit"):
        movement(DEPOSIT["receipt"], other, DEPOSIT["block_timestamp"])
    with pytest.raises(DataError, match="exclusivity deadline"):
        movement(DEPOSIT["receipt"], operation, DEPOSIT["block_timestamp"] + 1)


class Receipts:
    """A reader that serves one retained receipt and nothing else."""

    def __init__(self, receipt):
        self.receipt = receipt

    def _rpc(self, method, params, *, allow_none=False):
        assert (method, params) == ("eth_getTransactionReceipt", [self.receipt["transactionHash"]])
        return self.receipt


def test_the_real_fill_is_matched_by_the_whole_relay_and_measured_from_its_receipt():
    operation = real_deposit()
    _, deposit = movement(DEPOSIT["receipt"], operation, DEPOSIT["block_timestamp"])
    reference = parse_reference(opened_transfer(operation, deposit).reference)
    row, fill = settlement.matching_fill((FILL["log"],), reference.relay_hash, 8453)
    # Hashed for another destination, the same relay is another relay.
    assert settlement.matching_fill((FILL["log"],), reference.relay_hash, 42161) is None
    assert relay_hash(fill["relay"], 8453) == FILL["relay_hash"]
    assert settlement.FILL_TYPES[fill["fill_type"]] == "fast"
    receipt, paid = settlement.measured_arrival(
        Receipts(FILL["receipt"]), operation.target, row, fill
    )
    assert receipt == FILL["receipt"]
    assert paid == fill["updated_output"] == operation.quote.output_amount
    # The fill followed the deposit by a second: no fill of these terms precedes the quote.
    assert reference.quoted_at <= FILL["block_timestamp"]


def test_a_real_fill_is_not_an_arrival_for_another_fund():
    operation = real_deposit()
    row, fill = settlement.matching_fill((FILL["log"],), FILL["relay_hash"], 8453)
    elsewhere = target_for(operation.target.fund.address, "0x" + "22" * 20, 1)
    with pytest.raises(DataError, match="another recipient"):
        settlement.measured_arrival(Receipts(FILL["receipt"]), elsewhere, row, fill)


# A real refund root execution.


def refund_leaves():
    """Every (payee, amount) the transaction's USDC leaves name, in event order."""
    pairs = []
    for row in REFUND["receipt"]["logs"]:
        if row["address"] != POOL_HERE.address or row["topics"][0] != chain.REFUND_ROOT_TOPIC:
            continue
        _, amounts, token, payees, _, _ = decode(chain.REFUND_TYPES, bytes.fromhex(row["data"][2:]))
        if token.lower() == USDC.reference:
            pairs += [
                (payee.lower(), amount) for payee, amount in zip(payees, amounts, strict=True)
            ]
    return pairs


def test_a_real_refund_execution_pays_every_named_payee_exactly_its_leaf_amounts():
    leaves = [(payee, amount) for payee, amount in refund_leaves() if amount]
    assert len(leaves) > 1
    for payee in {payee for payee, _ in leaves}:
        target = target_for(payee, "0x" + "22" * 20, 1)
        named = sorted(amount for other, amount in leaves if other == payee)
        total = sum(named)
        assert settlement.refund_payment(REFUND["receipt"], target) == (total, total, True)
        assert (
            sorted(
                chain.refund_payments(REFUND["receipt"], USDC.reference, POOL_HERE.address, payee)
            )
            == named
        )


def test_one_real_transaction_executes_several_refund_roots():
    """The reader sums a fund's leaves over the whole transaction, not one event."""
    roots = [
        row
        for row in REFUND["receipt"]["logs"]
        if row["address"] == POOL_HERE.address and row["topics"][0] == chain.REFUND_ROOT_TOPIC
    ]
    assert len(roots) > 1 and all(len(row["topics"]) == 4 for row in roots)


def test_a_real_refund_execution_names_nothing_for_a_fund_it_does_not_pay():
    stranger = target_for("0x" + "33" * 20, "0x" + "22" * 20, 1)
    assert settlement.refund_payment(REFUND["receipt"], stranger) is None


def test_the_signed_arguments_are_the_relay_the_destination_hashes():
    """The deposit's signed call and the fill's event are the same twelve terms."""
    operation = real_deposit()
    signed = deposit_arguments(operation)
    relay = chain.decoded_fill(FILL["log"])["relay"]
    depositor, recipient, relayer, sent_token, received_token, sent, offered = relay[:7]
    assert (depositor, recipient, sent_token, received_token, sent, offered) == signed[:6]
    assert (relayer, relay[9]) == (signed[7], signed[9])
    assert relay[7] == 42161 and relay[11] == b""


# The settlement pages over the reads mainnet really gave.


def refund_case(payee: str, total: int, mined_at: int):
    """A deployment whose source fund is a payee of the real refund execution, holding one
    transfer of exactly what that execution paid it, proven unfilled and past its fill
    deadline. The logs, the receipt and every read are real; the transfer is constructed,
    because the real payee is a relayer and no real deposit of ours was ever refunded."""
    target = target_for(payee, "0x" + "22" * 20, total)
    quote = real_deposit().quote.model_copy(
        update={
            "input_amount": total,
            "output_amount": total - 1,
            "timestamp": mined_at - 7300,
            "fill_deadline": mined_at - 100,
        }
    )
    operation = DepositOperation(
        target=target,
        amount=AssetAmount(asset=USDC, amount=total),
        quote=quote,
        quote_time_buffer=3600,
    )
    mined = {"deposit_id": 1, "exclusivity_deadline": mined_at - 7290}
    transfer = opened_transfer(operation, mined).model_copy(update={"unfilled": True})
    return SimpleNamespace(config=config_for(target), target=target, transfer=transfer)


PAGES = CAPTURES["pages"]


def same(read, retained) -> bool:
    """Whether a page's own read states what the earlier, trimmed capture retained."""
    if isinstance(retained, dict):
        return all(same(read[name], value) for name, value in retained.items())
    if isinstance(retained, list):
        return len(read) == len(retained) and all(map(same, read, retained))
    return read == retained


def real_transfer():
    operation = real_deposit()
    _, deposit = movement(DEPOSIT["receipt"], operation, DEPOSIT["block_timestamp"])
    return operation, opened_transfer(operation, deposit)


def test_the_relay_hash_is_the_key_the_base_spoke_pool_holds_as_filled():
    """A known answer of the contract itself: `fillStatuses` of the hash this connector
    computes from the deposit, read on Base one block before the fill and after it."""
    _, transfer = real_transfer()
    computed = parse_reference(transfer.reference).relay_hash
    reads = PAGES["fill_status"]
    reader = Recorded(BASE, reads)
    fill_block = int(FILL["log"]["blockNumber"], 16)
    assert (reads["before"], reads["after"]) == (fill_block - 1, fill_block + 10)
    before = chain.fill_status(
        reader, POOL_THERE.address, computed, reader.pin_block(reads["before"])
    )
    after = chain.fill_status(
        reader, POOL_THERE.address, computed, reader.pin_block(reads["after"])
    )
    assert (before, after) == (0, chain.FILLED)
    # Another hash is another key: the capture holds no answer for it.
    with pytest.raises(AssertionError, match="the capture does not hold"):
        chain.fill_status(
            reader, POOL_THERE.address, "0x" + "00" * 32, reader.pin_block(reads["after"])
        )


def test_the_arrival_page_finds_and_measures_the_real_fill_through_reconciliation():
    """The production entry point over Base's own answers: the page starts at the last
    block not after the quote, filters `FilledRelay` by origin and deposit identifier,
    ends at the fill, measures the payment and cross-checks the pool's fill status."""
    operation, transfer = real_transfer()
    config = config_for(operation.target)
    reader = Recorded(BASE, PAGES["arrival"])
    page = settlement.reconcile(backend(config, reader), deployment(config, transfer))
    reference = parse_reference(transfer.reference)
    assert page.side == "arrival" and page.transfer == transfer.reference
    assert page.start.timestamp <= reference.quoted_at
    assert reader.pin_block(page.start.number + 1).timestamp > reference.quoted_at
    assert page.end.number == int(FILL["log"]["blockNumber"], 16)
    assert [same(row, FILL["log"]) for row in page.events] == [True] and not page.expired
    arrival = page.outcome
    assert (arrival.fill_type, arrival.tx_hash) == ("fast", FILL["receipt"]["transactionHash"])
    assert arrival.received.amount == operation.quote.output_amount
    assert arrival.received.asset == BASE_USDC and same(arrival.receipt, FILL["receipt"])
    # The log query carried the topic filter, and the status was read at the page's end.
    kinds = [kind for kind, _ in reader.asked]
    queries = [json.loads(name) for kind, name in reader.asked if kind == "rpc"]
    logs = [params[0] for method, params in queries if method == "eth_getLogs"]
    assert logs and all(
        query["address"] == POOL_THERE.address
        and query["topics"]
        == [chain.FILLED_RELAY_TOPIC, chain.uint_topic(42161), chain.uint_topic(4832241)]
        for query in logs
    )
    status = [json.loads(name) for kind, name in reader.asked if kind == "calls"]
    assert kinds.count("calls") == 1
    assert status[0][:3] == [POOL_THERE.address, "fillStatuses(bytes32)", page.end.number]
    assert status[0][3] == [reference.relay_hash]
    # The gateway's own re-derivation of the committed page reads the same chain.
    again = Recorded(BASE, PAGES["arrival"])
    state = deployment(config, transfer)
    assert settlement.reconcile(backend(config, again), state, reference=page) == page


def test_the_arrival_page_of_another_deposit_finds_nothing_in_the_same_blocks():
    """With another relay hash the real fill is another relay's and the status differs."""
    operation, transfer = real_transfer()
    reference = parse_reference(transfer.reference)
    forged = transfer.model_copy(
        update={"reference": f"{reference.deposit_id}:0x{'11' * 32}:{reference.quoted_at}"}
    )
    config = config_for(operation.target)
    reader = Recorded(BASE, PAGES["arrival"])
    with pytest.raises(AssertionError, match="the capture does not hold: calls"):
        settlement.reconcile(backend(config, reader), deployment(config, forged))


def test_the_refund_page_attributes_a_real_refund_execution_through_reconciliation():
    """The production entry point over Arbitrum's own answers. The payment, its refund
    leaf and every read are real. The transfer it is attributed to is constructed."""
    reads = PAGES["refund"]
    case = refund_case(reads["payee"], reads["total"], reads["mined_at"])
    reader = Recorded(ARBITRUM, reads)
    state = deployment(case.config, case.transfer)
    page = settlement.reconcile(backend(case.config, reader), state)
    refund_block = int(REFUND["receipt"]["blockNumber"], 16)
    assert page.side == "refund" and page.end.number == refund_block
    assert page.start.timestamp <= reads["mined_at"] - 100 and page.unsolicited == ()
    refund = page.outcome
    assert refund.transfers == (case.transfer.reference,)
    assert (refund.returned.asset, refund.returned.amount) == (USDC, reads["total"])
    assert refund.tx_hash == REFUND["receipt"]["transactionHash"]
    assert same(refund.receipt, REFUND["receipt"])
    queries = [json.loads(name) for kind, name in reader.asked if kind == "rpc"]
    logs = [params[0] for method, params in queries if method == "eth_getLogs"]
    assert logs and all(
        query["address"] == USDC.reference
        and query["topics"][1:]
        == [chain.address_topic(POOL_HERE.address), chain.address_topic(reads["payee"])]
        for query in logs
    )
    # The deferred-refund read closes the page: the pool holds nothing back for the fund.
    held = [json.loads(name) for kind, name in reader.asked if kind == "calls"]
    assert [call[1:3] for call in held] == [["getRelayerRefund(address,address)", refund_block]]
    again = Recorded(ARBITRUM, reads)
    assert settlement.reconcile(backend(case.config, again), state, reference=page) == page


def test_a_real_refund_of_another_amount_is_retained_as_unattributed():
    """The same execution, with a transfer of one base unit more: nothing sums to the
    payment, so it is no outcome and is kept as an unattributed refund payment."""
    reads = PAGES["refund"]
    case = refund_case(reads["payee"], reads["total"], reads["mined_at"])
    other = case.transfer.model_copy(
        update={"sent": case.transfer.sent.model_copy(update={"amount": reads["total"] + 1})}
    )
    reader = Recorded(ARBITRUM, reads)
    page = settlement.reconcile(backend(case.config, reader), deployment(case.config, other))
    assert page.outcome is None
    (stray,) = page.unsolicited
    assert (stray.amount.amount, stray.leaf_amount) == (reads["total"], None)
    assert stray.tx_hash == REFUND["receipt"]["transactionHash"]
