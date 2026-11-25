"""Across routes that leave Base and Ethereum, held to what mainnet really returned.

`tests/fixtures/across-route-captures.json` holds read-only captures per route: one
answer of the venue's quote API with both pools' state, one plain `deposit` a stranger
sent to the source SpokePool, the `FilledRelay` that filled it and the destination's
`fillStatuses` around the fill; and per chain one execution of relayer refund roots.
Nothing in it was sent by this repository. None of it is a lifecycle proof: no run of
this SDK has left these chains. Each test runs one reader or encoder over real bytes.
"""

import json
from pathlib import Path

import pytest
from eth_abi import decode

from almanak.caip import AssetType, ChainId
from almanak.connectors.across import quote as venue
from almanak.connectors.across import settlement
from almanak.connectors.across import settlement_reader as chain
from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.across.observations import PoolState, require_pinned
from almanak.connectors.across.operations import DEPOSIT_TYPES, DepositOperation, plan_call
from almanak.connectors.across.receipts import movement, opened_transfer
from almanak.connectors.across.relay import parse_reference
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount, Chain, StrategyAccounts
from tests.across_support import allocation, eoa, parameters, route

CAPTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "across-route-captures.json").read_text()
)
ROUTES = {f"{row['source']}>{row['destination']}": row for row in CAPTURES["routes"]}
REFUNDS = CAPTURES["refunds"]
# Front ends append a short tag after the ABI encoding, which the SpokePool does not read.
MAX_TAG_BYTES = 8


@pytest.fixture(params=sorted(ROUTES))
def capture(request):
    return ROUTES[request.param]


def number(chain_id: str) -> int:
    return int(chain_id.split(":")[1])


def target_for(capture, sender: str, recipient: str, amount: int, **changes):
    here = Chain(chain_id=ChainId(capture["source"]), environment="mainnet")
    there = Chain(chain_id=ChainId(capture["destination"]), environment="mainnet")
    accounts = StrategyAccounts(bindings=(eoa("arb", here, sender), eoa("base", there, recipient)))
    leg = route(
        chain=here,
        asset=AssetType(capture["asset"]),
        destination_chain=there.chain_id,
        destination_asset=AssetType(capture["destination_asset"]),
        spoke_pool=SPOKE_POOLS[here.chain_id].address,
        destination_pool=SPOKE_POOLS[there.chain_id],
        **changes,
    )
    held = allocation(route=leg, capital=amount, native_asset=native_asset(here.chain_id))
    return parameters(held, accounts=accounts).target("move")


def address(word: bytes) -> str:
    return "0x" + word[12:].hex()


def test_a_route_is_captured_from_base_and_from_ethereum():
    assert sorted(ROUTES) == ["eip155:1>eip155:42161", "eip155:8453>eip155:1"]


def test_the_real_answer_is_admitted_at_its_own_block_against_both_pinned_pools(capture):
    asked = capture["quote"]
    target = target_for(capture, "0x" + "11" * 20, asked["recipient"], asked["amount"])
    quote = venue.parse(asked["answer"], asked["amount"])
    pool = PoolState.model_validate(asked["pool"])
    require_pinned(pool, ChainId(capture["source"]))
    require_pinned(
        PoolState.model_validate(asked["destination_pool"]), ChainId(capture["destination"])
    )
    venue.admit(quote, target, asked["amount"], pool, asked["block_timestamp"])
    assert (quote.spoke_pool, quote.destination_spoke_pool) == (
        SPOKE_POOLS[ChainId(capture["source"])].address,
        SPOKE_POOLS[ChainId(capture["destination"])].address,
    )
    assert (quote.input_decimals, quote.output_decimals) == (6, 6)
    assert quote.fill_deadline - quote.timestamp == 7200
    assert json.loads(quote.response) == asked["answer"]
    # Under a ceiling below the real fee the same answer is refused.
    tight = target_for(
        capture, "0x" + "11" * 20, asked["recipient"], asked["amount"], max_fee_bps=1
    )
    assert venue.fee_ceiling(asked["amount"], 1) < asked["amount"] - quote.output_amount
    with pytest.raises(DataError, match="exceeds the ceiling"):
        venue.admit(quote, tight, asked["amount"], pool, asked["block_timestamp"])


def test_the_venue_states_exclusivity_as_an_offset_from_base_and_a_time_from_ethereum():
    """Both forms are admitted above; this holds which chain sent which."""
    parsed = {
        name: venue.parse(row["quote"]["answer"], row["quote"]["amount"])
        for name, row in ROUTES.items()
    }
    offset, absolute = parsed["eip155:8453>eip155:1"], parsed["eip155:1>eip155:42161"]
    assert 0 < offset.exclusivity_parameter <= venue.MAX_EXCLUSIVITY_OFFSET
    assert absolute.timestamp < absolute.exclusivity_parameter < absolute.fill_deadline


def real_deposit(capture) -> DepositOperation:
    """The stranger's deposit as this connector would have signed it, from its calldata."""
    sent_call = capture["deposit"]
    arguments = decode(DEPOSIT_TYPES, bytes.fromhex(sent_call["input"][10:]))
    depositor, recipient, _, _, sent, offered, destination, relayer = arguments[:8]
    quoted_at, fill_deadline, exclusivity, message = arguments[8:]
    assert (destination, message) == (number(capture["destination"]), b"")
    target = target_for(capture, address(depositor), address(recipient), sent)
    quote = venue.Quote(
        source="across_api",
        input_amount=sent,
        output_amount=offered,
        timestamp=quoted_at,
        fill_deadline=fill_deadline,
        exclusive_relayer=address(relayer),
        exclusivity_parameter=exclusivity,
        spoke_pool=target.route.spoke_pool,
        destination_spoke_pool=target.route.destination_pool.address,
        input_token=target.asset.reference,
        input_decimals=6,
        output_token=target.destination_asset.reference,
        output_decimals=6,
        amount_too_low=False,
        response="{}",
    )
    return DepositOperation(
        target=target,
        amount=AssetAmount(asset=target.asset, amount=sent),
        quote=quote,
        quote_time_buffer=3600,
    )


def test_the_deposit_calldata_equals_what_a_real_depositor_sent_the_spoke_pool(capture):
    operation = real_deposit(capture)
    call, sent_call = plan_call(operation), capture["deposit"]
    assert (call.to, call.value) == (SPOKE_POOLS[ChainId(capture["source"])].address, 0)
    assert sent_call["to"].lower() == call.to
    # Everything the contract decodes is byte for byte what this connector encodes.
    assert sent_call["input"].startswith(call.data)
    tag = sent_call["input"][len(call.data) :]
    assert 0 < len(tag) <= 2 * MAX_TAG_BYTES
    assert sent_call["from"].lower() == operation.target.fund.address


class Receipts:
    """A reader that serves one retained receipt and nothing else."""

    def __init__(self, receipt):
        self.receipt = receipt

    def _rpc(self, method, params, *, allow_none=False):
        assert (method, params) == ("eth_getTransactionReceipt", [self.receipt["transactionHash"]])
        return self.receipt


def test_the_relay_hashed_from_the_deposit_is_the_key_the_destination_pool_filled(capture):
    """The source receipt alone yields the hash, and the destination SpokePool's own
    `fillStatuses` of that hash turned to filled in the block of the matching fill."""
    operation, sent_call, fill = real_deposit(capture), capture["deposit"], capture["fill"]
    moved, deposit = movement(sent_call["receipt"], operation, sent_call["block_timestamp"])
    assert moved == operation.amount.amount
    transfer = opened_transfer(operation, deposit)
    reference = parse_reference(transfer.reference)
    assert reference.relay_hash == fill["relay_hash"]
    assert (fill["status_before"], fill["status_after"]) == (0, chain.FILLED)
    assert operation.quote.timestamp < sent_call["block_timestamp"] <= operation.deadline
    destination = number(capture["destination"])
    row, decoded = settlement.matching_fill((fill["log"],), reference.relay_hash, destination)
    # Hashed for another destination, the same relay is another relay.
    other = number(capture["source"])
    assert settlement.matching_fill((fill["log"],), reference.relay_hash, other) is None
    receipt, paid = settlement.measured_arrival(
        Receipts(fill["receipt"]), operation.target, row, decoded
    )
    assert receipt == fill["receipt"]
    assert paid == decoded["updated_output"] == operation.quote.output_amount
    assert row["address"].lower() == SPOKE_POOLS[ChainId(capture["destination"])].address


def test_a_deposit_receipt_of_other_terms_is_refused(capture):
    operation, sent_call = real_deposit(capture), capture["deposit"]
    other = operation.model_copy(
        update={"quote": operation.quote.model_copy(update={"output_amount": 1})}
    )
    with pytest.raises(DataError, match="differs from the signed deposit"):
        movement(sent_call["receipt"], other, sent_call["block_timestamp"])


@pytest.mark.parametrize("chain_id", sorted(REFUNDS))
def test_a_real_refund_execution_pays_every_named_payee_exactly_its_leaf_amounts(chain_id):
    """Ethereum's and Base's SpokePool variants emit the refund root and pay as the
    reader expects. The logs are those of the SpokePool and of the asset only."""
    refund, pool = REFUNDS[chain_id], SPOKE_POOLS[ChainId(chain_id)].address
    asset = AssetType(refund["asset"])
    leaves = []
    for row in refund["receipt"]["logs"]:
        if row["address"].lower() != pool or row["topics"][0] != chain.REFUND_ROOT_TOPIC:
            continue
        _, amounts, token, payees, _, _ = decode(chain.REFUND_TYPES, bytes.fromhex(row["data"][2:]))
        if token.lower() == asset.reference:
            leaves += [(p.lower(), a) for p, a in zip(payees, amounts, strict=True) if a]
    assert len(leaves) > 1
    capture = {
        "source": chain_id,
        "destination": "eip155:42161" if chain_id != "eip155:42161" else "eip155:8453",
        "asset": refund["asset"],
        "destination_asset": "eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    }
    for payee in {payee for payee, _ in leaves}:
        target = target_for(capture, payee, "0x" + "22" * 20, 1)
        total = sum(amount for other, amount in leaves if other == payee)
        assert settlement.refund_payment(refund["receipt"], target) == (total, total, True)
    stranger = target_for(capture, "0x" + "33" * 20, "0x" + "22" * 20, 1)
    assert settlement.refund_payment(refund["receipt"], stranger) is None


def test_a_route_between_assets_of_different_decimals_is_refused_before_signing():
    """USDC has 18 decimals on BNB Smart Chain and 6 elsewhere. The fee ceiling compares
    base units, so the gateway refuses the venue's answer for such a route."""
    asked = ROUTES["eip155:1>eip155:42161"]["quote"]
    capture = ROUTES["eip155:1>eip155:42161"]
    target = target_for(capture, "0x" + "11" * 20, asked["recipient"], asked["amount"])
    answer = json.loads(json.dumps(asked["answer"]))
    answer["outputToken"]["decimals"] = 18
    quote = venue.parse(answer, asked["amount"])
    pool = PoolState.model_validate(asked["pool"])
    kinds = [kind for kind, _ in venue.refusals(quote, target, asked["amount"], pool, 0)]
    assert "decimals_differ" in kinds
    with pytest.raises(DataError):
        venue.admit(quote, target, asked["amount"], pool, asked["block_timestamp"])
