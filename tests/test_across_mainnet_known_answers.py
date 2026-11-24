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
        assert settlement.refund_total(REFUND["receipt"], target) == sum(named)
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
    assert settlement.refund_total(REFUND["receipt"], stranger) is None


def test_the_signed_arguments_are_the_relay_the_destination_hashes():
    """The deposit's signed call and the fill's event are the same twelve terms."""
    operation = real_deposit()
    signed = deposit_arguments(operation)
    relay = chain.decoded_fill(FILL["log"])["relay"]
    depositor, recipient, relayer, sent_token, received_token, sent, offered = relay[:7]
    assert (depositor, recipient, sent_token, received_token, sent, offered) == signed[:6]
    assert (relayer, relay[9]) == (signed[7], signed[9])
    assert relay[7] == 42161 and relay[11] == b""
