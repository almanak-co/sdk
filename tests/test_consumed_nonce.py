"""A signed step whose nonce a final transaction took is proven dead and dropped.

Component evidence against a scripted chain, without signing or chain access. The
fork rehearsal is `tests/fork/test_ax_recover_consumed_nonce.py`.
"""

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from almanak import chains
from almanak.caip import ChainId
from almanak.gateway import consumed_nonce
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.models import Block, Chain
from almanak.operator_deployment import ConsumedNonce
from almanak.request_runtime import drop_superseded, release_pending
from tests.test_expired_attempt_release import gateway_service, releasable
from tests.test_production import backend_for
from tests.test_production import production_attempt as production_attempt
from tests.test_production import production_recorded as production_recorded

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
SUBMITTER = "0x" + "5a" * 20
OTHER = "0x" + "0b" * 20
RECORDED = "0x" + "aa" * 32
FOREIGN = "0x" + "bb" * 32
# The attempt was planned at block 100 with nonce 7. Blocks are 12 seconds apart.
PLANNED, NONCE, SPACING = 100, 7, 12
# The scripted network counts a block final 600 seconds, 50 blocks, behind the head.
MARGIN = 600


@pytest.fixture(autouse=True)
def margin(monkeypatch):
    stated = SimpleNamespace(finality_margin_seconds=MARGIN)
    monkeypatch.setattr(consumed_nonce, "evm_network", lambda chain: stated)
    return stated


def block_hash(number: int) -> str:
    return "0x" + f"{number:064x}"


def transaction(sender, nonce, tx_hash):
    return {"from": sender, "nonce": hex(nonce), "hash": tx_hash}


def shouted(value: str) -> str:
    """The value as a provider that answers in upper case writes it."""
    return "0x" + value[2:].upper()


class Chained:
    """A chain on which the submitter's nonce 7 is used in block `used`, if any."""

    def __init__(self, finalized, used=None, consumer=FOREIGN, head=None):
        self.finalized, self.used = finalized, used
        self.head = finalized + 1000 if head is None else head
        self.chain = CHAIN
        self.calls = []
        self.counts = {}
        self.transactions = [
            transaction(OTHER, NONCE, "0x" + "cc" * 32),
            transaction(shouted(SUBMITTER), NONCE, consumer),
        ]

    def pin_block(self, selector):
        self.calls.append(("pin_block", selector))
        return self.block({"finalized": self.finalized, "latest": self.head}[selector])

    def block(self, number):
        return Block(
            chain=CHAIN, number=number, hash=block_hash(number), timestamp=number * SPACING
        )

    @contextmanager
    def retained_pin(self):
        yield

    def count(self, number):
        if number in self.counts:
            return self.counts[number]
        return NONCE + 1 if self.used is not None and number >= self.used else NONCE

    def _rpc(self, method, params, **options):
        self.calls.append((method, params))
        if method == "eth_getTransactionCount":
            assert params[0] == SUBMITTER
            return hex(self.count(int(params[1], 16)))
        assert (method, params[1]) == ("eth_getBlockByNumber", True)
        number = int(params[0], 16)
        return {
            "number": hex(number),
            "hash": block_hash(number),
            "timestamp": hex(number * SPACING),
            "transactions": self.transactions if number == self.used else [],
        }

    def read(self, method):
        return [call for call in self.calls if call[0] == method]


def attempt(**changes):
    fields = {
        "nonce": NONCE,
        "tx_hash": RECORDED,
        "family": None,
        "operation": SimpleNamespace(kind="swap"),
        "execution": SimpleNamespace(submitter=SimpleNamespace(address=SUBMITTER)),
        "before": SimpleNamespace(block=SimpleNamespace(number=PLANNED)),
    }
    return SimpleNamespace(**{**fields, **changes})


def backend(reader, receipt=None):
    return SimpleNamespace(guard=lambda: None, receipt=lambda pending: receipt, reader=reader)


def proof(reader, used):
    return ConsumedNonce(
        operation="swap",
        transaction=RECORDED,
        submitter=SUBMITTER,
        nonce=NONCE,
        consumed_by=FOREIGN,
        block=reader.block(used),
    )


@pytest.mark.parametrize("used", [101, 102, 137, 4999, 5000])
def test_the_proof_names_the_final_transaction_that_took_the_nonce(used):
    reader = Chained(finalized=5000, used=used)
    assert consumed_nonce.prove(backend(reader), attempt()) == proof(reader, used)
    # The block is found by bisection, and only that one block is read whole.
    assert len(reader.read("eth_getTransactionCount")) <= 15
    assert reader.read("eth_getBlockByNumber") == [("eth_getBlockByNumber", [hex(used), True])]


@pytest.mark.parametrize(
    ("finalized", "used"),
    # Unused, used in a block that is not final yet, and a finalized block that is
    # not past the block the attempt was planned at.
    [(5000, None), (5000, 5001), (PLANNED, PLANNED + 1), (PLANNED - 1, PLANNED + 1)],
)
def test_a_nonce_that_is_not_consumed_at_the_finalized_block_is_refused(finalized, used):
    reader = Chained(finalized=finalized, used=used)
    with pytest.raises(DataError, match="not consumed at the finalized block") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "nonce_conflict"
    assert not reader.read("eth_getBlockByNumber")


@pytest.mark.parametrize(("behind", "released"), [(0, False), (49, False), (50, True)])
def test_a_finalized_tag_is_not_enough_within_the_networks_margin(behind, released):
    """An endpoint that serves its head as `finalized` releases nothing too early."""
    reader = Chained(finalized=5000, used=5000 - behind, head=5000)
    if released:
        assert consumed_nonce.prove(backend(reader), attempt()) == proof(reader, 5000 - behind)
        return
    with pytest.raises(DataError, match=f"counts as final {MARGIN}s behind it") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "nonce_conflict"


def test_a_network_without_a_stated_margin_releases_nothing(margin, monkeypatch):
    reader = Chained(finalized=5000, used=200)
    margin.finality_margin_seconds = None
    with pytest.raises(DataError, match="states no finality margin") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert (refusal.value.kind, reader.calls) == ("nonce_conflict", [])

    def unknown(chain):
        raise ValueError("no verified EVM network definition")

    monkeypatch.setattr(consumed_nonce, "evm_network", unknown)
    with pytest.raises(DataError, match="states no finality margin"):
        consumed_nonce.prove(backend(reader), attempt())


def test_every_network_states_its_margin_or_none():
    stated = {
        str(network.chain): network.finality_margin_seconds
        for network in (
            chains.ETHEREUM,
            chains.AVALANCHE,
            chains.BASE,
            chains.ARBITRUM,
            chains.BNB,
            chains.OPTIMISM,
            chains.POLYGON,
            chains.ROBINHOOD,
            chains.HYPEREVM,
        )
    }
    assert stated == MARGINS


def test_a_mined_family_or_venue_attempt_is_never_proven_superseded():
    reader = Chained(finalized=5000, used=200)
    with pytest.raises(DataError, match="recorded outcome"):
        consumed_nonce.prove(backend(reader, receipt={"status": "0x1"}), attempt())
    for other in (attempt(family=object()), attempt(nonce=None)):
        with pytest.raises(DataError, match="single signed transaction") as refusal:
            consumed_nonce.prove(backend(reader), other)
        assert refusal.value.kind == "nonce_conflict"
    assert reader.calls == []


@pytest.mark.parametrize("served", [RECORDED, shouted(RECORDED)])
def test_the_recorded_transaction_in_the_block_is_not_proof_against_itself(served):
    """A provider that serves the block but no receipt has not shown the step dead."""
    reader = Chained(finalized=5000, used=200, consumer=served)
    with pytest.raises(DataError, match="is in block 200 but has no receipt") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "inconsistent"


def test_hashes_served_in_upper_case_are_read_in_lower_case():
    reader = Chained(finalized=5000, used=200, consumer=shouted(FOREIGN))
    served = reader._rpc

    def upper(method, params, **options):
        result = served(method, params, **options)
        if method == "eth_getBlockByNumber":
            result["hash"] = shouted(result["hash"])
        return result

    reader._rpc = upper
    assert consumed_nonce.prove(backend(reader), attempt()) == proof(reader, 200)


def test_a_nonce_consumed_without_a_transaction_of_the_submitter_is_not_identified():
    reader = Chained(finalized=5000, used=200)
    reader.transactions = reader.transactions[:1]
    with pytest.raises(DataError, match="is not identified") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "nonce_conflict"


def test_two_transactions_of_the_submitter_at_the_nonce_identify_none():
    """No chain holds both, so a block that shows both is no proof of either."""
    reader = Chained(finalized=5000, used=200)
    reader.transactions.append(transaction(SUBMITTER, NONCE, "0x" + "dd" * 32))
    with pytest.raises(DataError, match="holds no single transaction") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "nonce_conflict"


def test_a_block_with_several_transactions_of_the_submitter_names_the_one_at_the_nonce():
    """The count at the planned block may be below the nonce: earlier steps came first."""
    reader = Chained(finalized=5000, used=200)
    reader.counts = {number: NONCE - 1 for number in range(PLANNED, 200)}
    reader.transactions += [
        transaction(SUBMITTER, NONCE - 1, "0x" + "d1" * 32),
        transaction(SUBMITTER, NONCE + 1, "0x" + "d2" * 32),
    ]
    assert consumed_nonce.prove(backend(reader), attempt()) == proof(reader, 200)


@pytest.mark.parametrize("dip", [150, 1000, 2550, 3775])
def test_counts_that_fall_along_the_chain_never_name_another_transaction(dip):
    """Whatever a provider's counts say, the release rests on the block read whole."""
    reader = Chained(finalized=5000, used=200)
    reader.counts = {dip: NONCE + 1 if dip < 200 else NONCE}
    try:
        assert consumed_nonce.prove(backend(reader), attempt()) == proof(reader, 200)
    except DataError as refusal:
        assert (refusal.kind, "is not identified" in str(refusal)) == ("nonce_conflict", True)


def test_a_nonce_already_used_where_the_attempt_was_planned_is_inconsistent():
    reader = Chained(finalized=5000, used=PLANNED)
    with pytest.raises(DataError, match="already used at the block") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "inconsistent"


@pytest.mark.parametrize("tag", ["latest", "finalized"])
def test_a_failing_block_tag_read_refuses(tag):
    reader = Chained(finalized=5000, used=200)
    pin = reader.pin_block

    def failing(selector):
        if selector == tag:
            raise DataError("rpc", f"{tag} is not served")
        return pin(selector)

    reader.pin_block = failing
    with pytest.raises(DataError, match="is not served"):
        consumed_nonce.prove(backend(reader), attempt())
    assert not reader.read("eth_getBlockByNumber")


@pytest.mark.parametrize("damage", ["transactions", "sender", "number", "hash"])
def test_an_incomplete_or_different_block_is_refused(damage):
    reader = Chained(finalized=5000, used=200)
    served = reader._rpc

    def damaged(method, params, **options):
        result = served(method, params, **options)
        if method != "eth_getBlockByNumber":
            return result
        if damage == "transactions":
            del result["transactions"]
        elif damage == "sender":
            result["transactions"] = [{"nonce": hex(NONCE), "hash": FOREIGN}]
        elif damage == "hash":
            result["transactions"] = [transaction(SUBMITTER, NONCE, "0xbb")]
        else:
            result["number"] = hex(201)
        return result

    reader._rpc = damaged
    with pytest.raises(DataError) as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "malformed"


def test_the_adapter_serves_the_proof(monkeypatch):
    gateway = object.__new__(EvmGateway)
    monkeypatch.setattr(consumed_nonce, "prove", lambda backend, pending: (backend, pending))
    assert gateway.superseded("attempt") == (gateway, "attempt")


def operated(state):
    return state.model_copy(update={"operator": True})


def evidence_for(pending, consumed_by=FOREIGN):
    return ConsumedNonce(
        operation=pending.operation.kind,
        transaction=pending.tx_hash,
        submitter=pending.execution.submitter.address,
        nonce=pending.nonce,
        consumed_by=consumed_by,
        block=pending.before.position.reserve.block,
    )


def proving(proofs, evidence):
    def superseded(pending):
        proofs.append(pending)
        return evidence

    return SimpleNamespace(superseded=superseded)


def test_the_gateway_admits_a_drop_only_with_the_proof_it_records(production_attempt):
    state, pending, _ = production_attempt
    evidence, proofs = evidence_for(pending), []
    # A lending checkpoint retains no released attempts, and none is needed here.
    before = operated(state)
    assert "superseded" not in before.model_dump()
    after = drop_superseded(before, evidence)
    assert (after.pending, after.decision, after.outcomes) == (None, None, before.outcomes)
    assert after.superseded == (evidence,)
    service = gateway_service(before, proving(proofs, evidence))
    service.validate_pending_resolution(before, after)
    service.validate_released_record(before, after)
    assert proofs == [pending]

    # The request ends with the step, and the record is the gateway's own proof.
    kept = after.model_copy(update={"decision": before.decision})
    with pytest.raises(ValueError, match="only with its proof and its decision"):
        service.validate_pending_resolution(before, kept)
    other = drop_superseded(before, evidence_for(pending, "0x" + "cc" * 32))
    with pytest.raises(ValueError, match="differs from the gateway's own"):
        service.validate_pending_resolution(before, other)
    # A drop that records no proof is a release, which this connector cannot make.
    bare = after.model_copy(update={"superseded": ()})
    with pytest.raises(ValueError, match="cannot release signed work"):
        service.validate_pending_resolution(before, bare)


def test_a_proof_is_recorded_only_with_the_drop_of_its_step(production_attempt):
    state, pending, _ = production_attempt
    evidence = evidence_for(pending)
    before = operated(state)
    service = gateway_service(before, proving([], evidence))
    dropped = drop_superseded(before, evidence)
    for changed in (
        before.model_copy(update={"superseded": (evidence,)}),
        dropped.model_copy(update={"superseded": (evidence, evidence)}),
    ):
        with pytest.raises(ValueError, match="only with the drop of its signed step"):
            service.validate_pending_resolution(before, changed)
    # Once recorded, a proof is neither removed nor added to without another drop.
    for changed in ((), (evidence, evidence)):
        later = dropped.model_copy(update={"superseded": changed})
        with pytest.raises(ValueError, match="only with the drop of its signed step"):
            service.validate_pending_resolution(dropped, later)


def test_a_strategy_deployment_never_drops_a_step_this_way(production_attempt):
    state, pending, _ = production_attempt
    evidence, proofs = evidence_for(pending), []
    service = gateway_service(state, proving(proofs, evidence))
    for strategy in (state, releasable(state)):
        with pytest.raises(ValueError, match="only an operator deployment"):
            service.validate_pending_resolution(strategy, drop_superseded(strategy, evidence))
    # Nor does its checkpoint ever load with such a record.
    carried = {**state.model_dump(mode="json"), "superseded": [evidence.model_dump(mode="json")]}
    with pytest.raises(ValueError, match="only an operator deployment"):
        type(state).model_validate_json(json.dumps(carried))
    assert proofs == []


def test_a_refused_proof_refuses_the_drop(production_attempt):
    state, pending, _ = production_attempt
    before = operated(releasable(state))

    def unproven(pending):
        raise DataError("nonce_conflict", "not consumed at the finalized block")

    service = gateway_service(before, SimpleNamespace(superseded=unproven))
    after = drop_superseded(before, evidence_for(pending))
    with pytest.raises(DataError, match="not consumed at the finalized block"):
        service.validate_pending_resolution(before, after)
    # An operator deployment that can retain released attempts retains none for a drop.
    assert after.released == ()
    service.validate_released_record(before, after)


def test_a_never_sent_step_is_released_or_dropped_each_by_its_own_proof(production_attempt):
    """Dropping a never-sent step always takes the consumed-nonce proof: a lending
    checkpoint, which cannot release, gains no release through the drop."""
    state, pending, _ = production_attempt
    unsent = pending.model_copy(update={"unsent": True})
    asked = []
    adapter = SimpleNamespace(
        require_releasable=lambda step: asked.append("releasable"),
        superseded=lambda step: asked.append("superseded") or evidence_for(step),
    )
    lending = operated(state).model_copy(update={"pending": unsent})
    service = gateway_service(lending, adapter)
    with pytest.raises(ValueError, match="cannot release signed work"):
        service.validate_pending_resolution(lending, lending.model_copy(update={"pending": None}))
    assert asked == []
    service.validate_pending_resolution(lending, drop_superseded(lending, evidence_for(unsent)))
    assert asked == ["superseded"]

    asked.clear()
    before = operated(releasable(state)).model_copy(update={"pending": unsent})
    service = gateway_service(before, adapter)
    service.validate_pending_resolution(before, release_pending(before))
    service.validate_pending_resolution(before, drop_superseded(before, evidence_for(unsent)))
    assert asked == ["releasable", "superseded"]


def test_only_an_operator_gateway_serves_the_proof(production_attempt):
    state, pending, _ = production_attempt
    served = SimpleNamespace(superseded=lambda attempt: ("proof", attempt))
    service = gateway_service(state, served)
    with pytest.raises(ValueError, match="only an operator deployment"):
        service.dispatch_pending("superseded", state)
    assert service.dispatch_pending("superseded", operated(state)) == ("proof", pending)


class Scripted(Chained):
    """The scripted chain behind the production adapter's own reads."""

    def __init__(self, pending, used, **options):
        planned = pending.before.block
        super().__init__(finalized=planned.number + 4000, used=used, **options)
        self.chain, self.transport_chain = planned.chain, planned.chain.chain_id
        self.planned, self.submitter = planned, pending.execution.submitter.address
        self.transactions = [transaction(shouted(self.submitter), pending.nonce, FOREIGN)]
        self.nonce, self.receipts = pending.nonce, {}

    def pin_block(self, selector):
        if type(selector) is int:
            return self.block(selector)
        return super().pin_block(selector)

    def block(self, number):
        if number == self.planned.number:
            return self.planned
        stamp = self.planned.timestamp + (number - self.planned.number) * SPACING
        return Block(chain=self.chain, number=number, hash=block_hash(number), timestamp=stamp)

    def count(self, number):
        return self.nonce + (self.used is not None and number >= self.used)

    def _rpc(self, method, params, **options):
        self.calls.append((method, params))
        if method == "eth_getTransactionReceipt":
            return self.receipts.get(params[0])
        if method == "eth_getTransactionCount":
            assert params[0] == self.submitter
            return hex(self.count(int(params[1], 16)))
        block = self.block(int(params[0], 16))
        return {
            "number": hex(block.number),
            "hash": block.hash,
            "timestamp": hex(block.timestamp),
            "transactions": self.transactions if block.number == self.used else [],
        }


def production_backend(state, pending, reader):
    """The production adapter over the scripted chain. Only the check of the captured
    signature is left out: the capture's bytes are synthetic."""
    adapter = backend_for(state.config, pending.before.block)
    adapter.reader = reader
    adapter.validate_pending = adapter.validate_pending_block
    return adapter


def test_the_receipt_read_the_proof_and_the_admission_hold_together(production_attempt):
    state, pending, outcome = production_attempt
    before = operated(state)
    used = pending.before.block.number + 37
    reader = Scripted(pending, used)
    adapter = production_backend(state, pending, reader)
    service = gateway_service(before, adapter)

    evidence = service.dispatch_pending("superseded", before)
    assert (evidence.transaction, evidence.consumed_by) == (pending.tx_hash, FOREIGN)
    assert (evidence.block, evidence.operation) == (reader.block(used), pending.operation.kind)
    after = drop_superseded(before, evidence)
    service.validate_pending_resolution(before, after)
    service.validate_released_record(before, after)
    # Each proof read the receipt of the recorded transaction first, and found none.
    assert (
        reader.read("eth_getTransactionReceipt")
        == [("eth_getTransactionReceipt", [pending.tx_hash])] * 2
    )

    # Once the recorded transaction has a canonical receipt, neither holds any more.
    mined = {**outcome.receipt, "blockNumber": hex(used), "blockHash": block_hash(used)}
    reader.receipts[pending.tx_hash] = mined
    for refused in (
        lambda: service.dispatch_pending("superseded", before),
        lambda: service.validate_pending_resolution(before, after),
    ):
        with pytest.raises(DataError, match="resolved only by its recorded outcome"):
            refused()


def test_the_admission_refuses_a_proof_the_chain_no_longer_gives(production_attempt):
    state, pending, _ = production_attempt
    before = operated(state)
    used = pending.before.block.number + 37
    reader = Scripted(pending, used)
    service = gateway_service(before, production_backend(state, pending, reader))
    after = drop_superseded(before, service.dispatch_pending("superseded", before))

    # The endpoint now shows the nonce unused at its finalized block.
    reader.used = None
    with pytest.raises(DataError, match="not consumed at the finalized block"):
        service.validate_pending_resolution(before, after)
    # Or another transaction in its place.
    reader.used = used
    reader.transactions = [transaction(reader.submitter, pending.nonce, "0x" + "cc" * 32)]
    with pytest.raises(ValueError, match="differs from the gateway's own"):
        service.validate_pending_resolution(before, after)


# Seconds behind the head, per network; None releases nothing. The reason for each
# figure is stated beside the network in `almanak.chains`.
MARGINS: dict[str, int | None] = {
    "eip155:1": 1152,
    "eip155:43114": 5,
    "eip155:8453": 1320,
    "eip155:42161": 1152,
    "eip155:56": 180,
    "eip155:10": 1800,
    "eip155:137": None,
    "eip155:4663": 1152,
    "eip155:999": None,
}
