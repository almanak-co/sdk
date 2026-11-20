"""A signed step whose nonce a finalized transaction took is proven dead and dropped.

Component evidence against a scripted chain, without signing or chain access. The
fork rehearsal is `tests/fork/test_ax_recover_consumed_nonce.py`.
"""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.gateway import consumed_nonce
from almanak.gateway.consumed_nonce import ConsumedNonce
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.models import Block, Chain
from almanak.request_runtime import drop_superseded
from tests.test_expired_attempt_release import gateway_service, releasable
from tests.test_production import production_attempt as production_attempt
from tests.test_production import production_recorded as production_recorded

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
SUBMITTER = "0x" + "5a" * 20
OTHER = "0x" + "0b" * 20
RECORDED = "0x" + "aa" * 32
FOREIGN = "0x" + "bb" * 32
# The attempt was planned at block 100 with nonce 7.
PLANNED, NONCE = 100, 7


def block_hash(number: int) -> str:
    return "0x" + f"{number:064x}"


def transaction(sender, nonce, tx_hash):
    return {"from": sender, "nonce": hex(nonce), "hash": tx_hash}


class Chained:
    """A chain on which the submitter's nonce 7 is used in block `used`, if any."""

    def __init__(self, finalized, used=None, consumer=FOREIGN):
        self.finalized, self.used, self.consumer = finalized, used, consumer
        self.chain = CHAIN
        self.calls = []
        # Checksummed, as providers answer; the submitter is compared in lower case.
        self.transactions = [
            transaction(OTHER, NONCE, "0x" + "cc" * 32),
            transaction(SUBMITTER.upper().replace("0X", "0x"), NONCE, consumer),
        ]

    def pin_block(self, selector):
        assert selector == "finalized"
        return self.block(self.finalized)

    def block(self, number):
        return Block(chain=CHAIN, number=number, hash=block_hash(number), timestamp=number * 12)

    @contextmanager
    def retained_pin(self):
        yield

    def count(self, number):
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
            "timestamp": hex(number * 12),
            "transactions": self.transactions if number == self.used else [],
        }


def attempt(**changes):
    fields = {
        "nonce": NONCE,
        "tx_hash": RECORDED,
        "family": None,
        "execution": SimpleNamespace(submitter=SimpleNamespace(address=SUBMITTER)),
        "before": SimpleNamespace(block=SimpleNamespace(number=PLANNED)),
    }
    return SimpleNamespace(**{**fields, **changes})


def backend(reader, receipt=None):
    return SimpleNamespace(guard=lambda: None, receipt=lambda pending: receipt, reader=reader)


@pytest.mark.parametrize("used", [101, 102, 137, 4999, 5000])
def test_the_proof_names_the_finalized_transaction_that_took_the_nonce(used):
    reader = Chained(finalized=5000, used=used)
    proof = consumed_nonce.prove(backend(reader), attempt())
    assert proof == ConsumedNonce(
        transaction=RECORDED,
        submitter=SUBMITTER,
        nonce=NONCE,
        consumed_by=FOREIGN,
        block=reader.block(used),
        finalized=reader.block(5000),
    )
    # The block is found by bisection, and only that one block is read whole.
    counts = [call for call in reader.calls if call[0] == "eth_getTransactionCount"]
    blocks = [call for call in reader.calls if call[0] == "eth_getBlockByNumber"]
    assert len(counts) <= 15 and blocks == [("eth_getBlockByNumber", [hex(used), True])]


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
    assert not [call for call in reader.calls if call[0] == "eth_getBlockByNumber"]


def test_a_mined_family_or_venue_attempt_is_never_proven_superseded():
    reader = Chained(finalized=5000, used=200)
    with pytest.raises(DataError, match="recorded outcome"):
        consumed_nonce.prove(backend(reader, receipt={"status": "0x1"}), attempt())
    for other in (attempt(family=object()), attempt(nonce=None)):
        with pytest.raises(DataError, match="single signed transaction") as refusal:
            consumed_nonce.prove(backend(reader), other)
        assert refusal.value.kind == "nonce_conflict"
    assert reader.calls == []


def test_the_recorded_transaction_in_the_block_is_not_proof_against_itself():
    """A provider that serves the block but no receipt has not shown the step dead."""
    reader = Chained(finalized=5000, used=200, consumer=RECORDED)
    with pytest.raises(DataError, match="is in block 200 but has no receipt") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "inconsistent"


def test_a_nonce_consumed_without_a_transaction_of_the_submitter_is_not_identified():
    reader = Chained(finalized=5000, used=200)
    reader.transactions = reader.transactions[:1]
    with pytest.raises(DataError, match="is not identified") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "nonce_conflict"


def test_a_nonce_already_used_where_the_attempt_was_planned_is_inconsistent():
    reader = Chained(finalized=5000, used=PLANNED)
    with pytest.raises(DataError, match="already used at the block") as refusal:
        consumed_nonce.prove(backend(reader), attempt())
    assert refusal.value.kind == "inconsistent"


@pytest.mark.parametrize("damage", ["transactions", "sender", "number"])
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


def proving(proofs, releasable_refusal=None):
    def require_releasable(pending):
        proofs.append(("releasable", pending))
        if releasable_refusal is not None:
            raise releasable_refusal

    return SimpleNamespace(
        require_releasable=require_releasable,
        superseded=lambda pending: proofs.append(("superseded", pending)),
    )


def test_the_gateway_admits_a_drop_only_for_an_operator_and_with_its_proof(production_attempt):
    state, pending, _ = production_attempt
    proofs = []
    # A lending checkpoint retains no released attempts, and none is needed here.
    before = operated(state)
    after = drop_superseded(before)
    assert (after.pending, after.decision, after.outcomes) == (None, None, before.outcomes)
    service = gateway_service(before, proving(proofs))
    service.validate_pending_resolution(before, after)
    service.validate_released_record(before, after)
    assert proofs == [("superseded", pending)]
    # The request ends with the step: keeping its decision is not this update.
    kept = after.model_copy(update={"decision": before.decision})
    with pytest.raises(ValueError, match="cannot release signed work"):
        service.validate_pending_resolution(before, kept)

    # A strategy deployment never drops a step this way, whatever its connector.
    with pytest.raises(ValueError, match="cannot release signed work"):
        service.validate_pending_resolution(state, drop_superseded(state))
    strategy = releasable(state)
    with pytest.raises(ValueError, match="only with its decision"):
        service.validate_pending_resolution(strategy, drop_superseded(strategy))
    with pytest.raises(ValueError, match="retained until an attempt at their nonce settles"):
        service.validate_released_record(strategy, drop_superseded(strategy))
    assert proofs == [("superseded", pending)]


def test_a_refused_proof_refuses_the_drop(production_attempt):
    state, _, _ = production_attempt
    before = operated(releasable(state))

    def unproven(pending):
        raise DataError("nonce_conflict", "not consumed at the finalized block")

    adapter = SimpleNamespace(superseded=unproven)
    service = gateway_service(before, adapter)
    with pytest.raises(DataError, match="not consumed at the finalized block"):
        service.validate_pending_resolution(before, drop_superseded(before))
    # An operator deployment that can retain released attempts retains none for a drop.
    service.validate_released_record(before, drop_superseded(before))


def test_a_never_sent_step_stays_a_release_while_its_nonce_is_unused(production_attempt):
    state, pending, _ = production_attempt
    unsent = pending.model_copy(update={"unsent": True})
    before = operated(releasable(state)).model_copy(update={"pending": unsent})
    after = drop_superseded(before)
    proofs = []
    service = gateway_service(before, proving(proofs))
    service.validate_pending_resolution(before, after)
    assert proofs == [("releasable", unsent)]

    # Once its nonce is taken the release proof fails, and the drop needs its own.
    taken = DataError("nonce_conflict", "unknown execution occupies the recorded nonce")
    proofs.clear()
    service = gateway_service(before, proving(proofs, taken))
    service.validate_pending_resolution(before, after)
    assert proofs == [("releasable", unsent), ("superseded", unsent)]

    # Any other refusal of the release stands.
    proofs.clear()
    service = gateway_service(before, proving(proofs, DataError("reorganization", "moved")))
    with pytest.raises(DataError, match="moved"):
        service.validate_pending_resolution(before, after)
    assert proofs == [("releasable", unsent)]


def test_only_an_operator_gateway_serves_the_proof(production_attempt):
    state, pending, _ = production_attempt
    served = SimpleNamespace(superseded=lambda attempt: ("proof", attempt))
    service = gateway_service(state, served)
    with pytest.raises(ValueError, match="only an operator deployment"):
        service.dispatch_pending("superseded", state)
    assert service.dispatch_pending("superseded", operated(state)) == ("proof", pending)
