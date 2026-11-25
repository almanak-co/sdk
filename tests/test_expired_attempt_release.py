"""A signed attempt past its protocol deadline is released and planned afresh, never replayed."""

import json
from types import SimpleNamespace

import pytest
from pydantic import Field

from almanak.connectors.aave_v4.state import Checkpoint as AaveCheckpoint
from almanak.connectors.aave_v4.state import Pending as AavePending
from almanak.connectors.composition import connector_for
from almanak.connectors.uniswap_v4 import state as v4
from almanak.connectors.uniswap_v4 import validation as v4_validation
from almanak.connectors.v3_pool_operations import Approval, Collect
from almanak.connectors.v3_pool_validation import expired
from almanak.gateway.admission import validate_execution_update, verify_closure
from almanak.gateway.ethereum import DataError
from almanak.gateway.service import GatewayService
from almanak.request_runtime import release_pending
from almanak.runtime import commit_outcome
from tests.test_production import backend_for
from tests.test_production import production_attempt as production_attempt
from tests.test_production import production_recorded as production_recorded
from tests.test_runtime_recovery import coordinator


class Releasable(AaveCheckpoint):
    """A stand-in declaring the field expirable connectors carry; Aave itself has no deadline."""

    released: tuple[AavePending, ...] = Field(default=(), exclude_if=lambda value: not value)


def releasable(state):
    return Releasable.model_validate(state.model_dump())


class MemoryJournal:
    def __init__(self, state):
        self.state = state

    def load(self):
        return self.state

    def update(self, change):
        self.state = change(self.state)
        return self.state


CLEAN = {"eth_getTransactionByHash": None, "eth_getTransactionReceipt": None, "eth_getCode": "0x"}


def expirable(backend, pending, *, past=True, **responses):
    """A backend whose connector reports the operation expired (or not) at the head."""
    backend.validate_pending = lambda p: None
    backend.validate_current = lambda p: p.before
    backend.expired_at_head = lambda operation: past
    # This connector states no deadline; the pinned re-check has its own test on Across.
    backend.require_unused_past_deadline = lambda p: None
    backend.reader.responses = {
        **CLEAN,
        "eth_getTransactionCount": hex(pending.nonce),
        "eth_sendRawTransaction": pending.tx_hash,
        **responses,
    }
    return backend


def sends(backend):
    return [params for method, params in backend.reader.calls if method == "eth_sendRawTransaction"]


def test_v3_operations_expire_at_their_protocol_deadline():
    at = SimpleNamespace(timestamp=100)
    assert expired(SimpleNamespace(deadline=99), at)
    # Periphery still accepts a block at the deadline, and on Arbitrum or BNB a later
    # block can share the head's timestamp: equality is not proof the call is dead.
    assert not expired(SimpleNamespace(deadline=100), at)
    # An approval carries no deadline and stays valid; it is never released as expired.
    assert not expired(Approval.model_construct(), at)
    # Nor does a standalone collect: the position manager's `collect` takes no deadline.
    assert not expired(Collect.model_construct(deadline=1), at)


@pytest.mark.parametrize("operation", [v4.Swap, v4.Mint, v4.Collect, v4.Burn])
def test_v4_router_and_position_manager_calls_expire_at_their_deadline(operation):
    at = SimpleNamespace(timestamp=100)
    assert v4_validation.expired(operation.model_construct(deadline=99), at)
    # The deadline itself still executes; a later block may share the head's timestamp.
    assert not v4_validation.expired(operation.model_construct(deadline=100), at)
    # A Permit2 expiration bounds the allowance, not the call; the approval never expires.
    assert not v4_validation.expired(v4.Approval.model_construct(expiration=100), at)


def v4_plan(operation, intent_deadline, **signed):
    return SimpleNamespace(
        before=SimpleNamespace(owned=None, allowances=()),
        operation=operation,
        decision=SimpleNamespace(intent=SimpleNamespace(deadline=intent_deadline)),
        **signed,
    )


def v4_current(timestamp):
    from datetime import UTC, datetime

    block = SimpleNamespace(timestamp=timestamp)
    return SimpleNamespace(
        block=block, effective_at=datetime.fromtimestamp(timestamp, UTC), allowances=()
    )


def test_a_signed_v4_approval_is_rebroadcast_after_its_intent_expires(monkeypatch):
    """The intent window bounds signing; recorded signed V4 bytes only their own deadline."""
    monkeypatch.setattr(v4_validation, "validate_owned", lambda owned, current: None)
    allocation = SimpleNamespace(deadline_seconds=300)
    backend = SimpleNamespace(config=SimpleNamespace(allocation=lambda name: allocation))
    approval = v4.Approval.model_construct(
        target=SimpleNamespace(allocation="a"), kind="approve_permit2", expiration=200
    )
    swap = v4.Swap.model_construct(target=SimpleNamespace(allocation="a"), deadline=200)
    v4_validation.validate_current(backend, v4_plan(approval, 200), v4_current(100))
    # Signing still requires the intent's admission window.
    with pytest.raises(DataError, match="retained transaction deadline expired"):
        v4_validation.validate_current(backend, v4_plan(approval, 200), v4_current(200))
    # The recorded signed approval stays valid for rebroadcast after the intent expires.
    v4_validation.validate_current(
        backend, v4_plan(approval, 200, raw_transaction="0x01"), v4_current(200)
    )
    # A signed swap past its own deadline is still refused before submission; the gateway
    # releases it as expired before reaching this check.
    v4_validation.validate_current(
        backend, v4_plan(swap, 200, raw_transaction="0x01"), v4_current(200)
    )
    with pytest.raises(DataError, match="prepared protocol deadline expired"):
        v4_validation.validate_current(
            backend, v4_plan(swap, 200, raw_transaction="0x01"), v4_current(201)
        )


def test_only_connectors_with_protocol_deadlines_declare_expiry():
    from almanak.connectors.aave_v4.execution import AaveExecution
    from almanak.connectors.aerodrome.execution import AerodromeExecution
    from almanak.connectors.aerodrome.state import Checkpoint as AerodromeCheckpoint
    from almanak.connectors.composed.execution import ComposedExecution
    from almanak.connectors.curve.execution import CurveExecution
    from almanak.connectors.curve.state import Checkpoint as CurveCheckpoint
    from almanak.connectors.pancakeswap_v3.execution import PancakeExecution
    from almanak.connectors.uniswap_v3.execution import UniswapExecution
    from almanak.connectors.uniswap_v3.state import Checkpoint as UniswapCheckpoint
    from almanak.connectors.uniswap_v4.execution import UniswapExecution as UniswapV4Execution
    from almanak.connectors.uniswap_v4.state import Checkpoint as UniswapV4Checkpoint
    from almanak.work import ChainWork

    assert UniswapExecution.expired is expired and PancakeExecution.expired is expired
    assert callable(AerodromeExecution.expired)
    # Curve pool calls encode no deadline, so a signed Curve attempt is never provably dead.
    assert getattr(CurveExecution, "expired", None) is None
    assert UniswapV4Execution.expired is v4_validation.expired
    for checkpoint in (
        UniswapCheckpoint,
        UniswapV4Checkpoint,
        AerodromeCheckpoint,
        CurveCheckpoint,
        ChainWork,
    ):
        assert "released" in checkpoint.model_fields
    # Without a protocol deadline a signed attempt is never provably dead; it keeps failing.
    assert getattr(AaveExecution, "expired", None) is None
    assert "released" not in AaveCheckpoint.model_fields
    # Composed deployments apply the owning component's declaration.
    from almanak.connectors.composed.state import Checkpoint as ComposedCheckpoint

    assert callable(ComposedExecution.expired)
    assert "released" in ComposedCheckpoint.model_fields


def test_broadcast_refuses_an_expired_transaction_without_sending_it(production_attempt):
    state, pending, _ = production_attempt
    backend = expirable(backend_for(state.config, pending.before.position.reserve.block), pending)
    with pytest.raises(DataError) as error:
        backend.broadcast(pending)
    assert error.value.kind == "transaction_expired" and sends(backend) == []
    # An occupied nonce is reported first: that attempt is not provably unsent.
    backend.reader.responses["eth_getTransactionCount"] = hex(pending.nonce + 1)
    with pytest.raises(DataError) as error:
        backend.broadcast(pending)
    assert error.value.kind == "nonce_conflict"
    backend.expired_at_head = lambda operation: False
    backend.reader.responses["eth_getTransactionCount"] = hex(pending.nonce)
    assert backend.broadcast(pending) is True
    assert sends(backend) == [[pending.raw_transaction]]


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"past": False}, "past its protocol deadline"),
        ({"eth_getTransactionByHash": {}}, "provider still holds"),
        ({"eth_getTransactionCount": "0x0"}, "recorded nonce"),
    ],
)
def test_release_requires_proof_the_attempt_can_never_take_effect(
    production_attempt, change, reason
):
    state, pending, _ = production_attempt
    backend = expirable(backend_for(state.config, pending.before.position.reserve.block), pending)
    backend.require_releasable(pending)
    past = change.pop("past", True)
    backend = expirable(
        backend_for(state.config, pending.before.position.reserve.block),
        pending,
        past=past,
        **change,
    )
    with pytest.raises((ValueError, DataError), match=reason):
        backend.require_releasable(pending)


def test_a_mined_or_family_attempt_is_never_released(production_attempt):
    state, pending, outcome = production_attempt
    backend = expirable(backend_for(state.config, pending.before.position.reserve.block), pending)
    backend.receipt = lambda p: outcome.receipt
    with pytest.raises(ValueError, match="recorded outcome"):
        backend.require_releasable(pending)
    family = pending.model_copy(update={"family": object()})
    with pytest.raises(ValueError, match="family"):
        backend.require_releasable(family)


def gateway_service(state, adapter):
    service = object.__new__(GatewayService)
    service.config = state.config
    service.connector = connector_for(state.config)
    service.backend = adapter
    service.route_chain = None
    return service


def test_the_gateway_admits_a_release_only_with_its_independent_proof(production_attempt):
    state, pending, _ = production_attempt
    state = releasable(state)
    # An absent release is omitted, so existing checkpoints serialize unchanged.
    assert "released" not in state.model_dump()
    proofs = []
    adapter = SimpleNamespace(require_releasable=proofs.append)
    service = gateway_service(state, adapter)
    released = release_pending(state)
    assert released.pending is None and released.decision is None
    assert released.released == (pending,) and released.outcomes == state.outcomes
    service.validate_pending_resolution(state, released)
    service.validate_released_record(state, released)
    service.validate_owned_evidence(state, released)
    validate_execution_update(adapter, state, released)
    assert proofs == [pending]
    kept = released.model_copy(update={"decision": state.decision})
    with pytest.raises(ValueError, match="only with its decision"):
        service.validate_pending_resolution(state, kept)
    with pytest.raises(ValueError, match="cannot release"):
        release_pending(production_attempt[0])

    def refused(pending):
        raise ValueError("only a transaction past its protocol deadline can be released")

    service = gateway_service(state, SimpleNamespace(require_releasable=refused))
    with pytest.raises(ValueError, match="protocol deadline"):
        service.validate_pending_resolution(state, released)


def test_released_attempts_are_retained_until_an_attempt_at_their_nonce_settles(
    production_attempt,
):
    state, pending, outcome = production_attempt
    service = gateway_service(state, SimpleNamespace())
    released = release_pending(releasable(state))
    retry = pending.model_copy(update={"tx_hash": "0x" + "ab" * 32})
    claimed = released.model_copy(update={"pending": retry})
    # A new signed attempt at the released nonce keeps the old identity: its bytes may still land.
    service.validate_released_record(released, claimed)
    with pytest.raises(ValueError, match="retained until an attempt at their nonce settles"):
        service.validate_released_record(released, claimed.model_copy(update={"released": ()}))
    with pytest.raises(ValueError, match="retained until an attempt at their nonce settles"):
        service.validate_released_record(released, released.model_copy(update={"released": ()}))
    # Its reconciled outcome at that nonce settles the released attempts, and only then.
    settled = commit_outcome(claimed, retry, outcome)
    assert settled.released == () and settled.pending is None
    service.validate_released_record(claimed, settled)
    with pytest.raises(ValueError, match="retained until an attempt at their nonce settles"):
        service.validate_released_record(
            claimed, settled.model_copy(update={"released": (pending,)})
        )
    # An outcome at another submitter nonce leaves them in place.
    other = retry.model_copy(update={"nonce": pending.nonce + 1})
    unrelated = released.model_copy(update={"pending": other})
    assert commit_outcome(unrelated, other, outcome).released == (pending,)
    # A second expiry at the same nonce is retained alongside the first.
    again = release_pending(claimed)
    assert again.released == (pending, retry)
    service.backend = SimpleNamespace(require_releasable=lambda attempt: None)
    service.validate_pending_resolution(claimed, again)
    service.validate_released_record(claimed, again)


def planning_backend(state, pending, count):
    backend = backend_for(state.config, pending.before.position.reserve.block)
    backend.reader.responses["eth_getTransactionCount"] = hex(count)
    backend.prepare_fees = lambda *args: {"gas": 1}
    return backend


def test_fresh_planning_stops_when_a_released_copy_took_its_nonce(production_attempt):
    state, pending, _ = production_attempt
    released = release_pending(releasable(state))
    execution = pending.execution
    arguments = (pending.operation, pending.before, execution)
    backend = planning_backend(state, pending, pending.nonce)
    assert backend.estimate_execution(released, *arguments) == (pending.nonce, {"gas": 1})
    backend = planning_backend(state, pending, pending.nonce + 1)
    with pytest.raises(DataError, match="released attempt's nonce was consumed"):
        backend.estimate_execution(released, *arguments)
    # Without a released attempt the next nonce is simply the next attempt's.
    assert backend.estimate_execution(state, *arguments)[0] == pending.nonce + 1


def test_closure_refuses_a_released_copy_that_took_its_nonce(production_attempt):
    state, pending, _ = production_attempt
    closing = releasable(state).model_copy(
        update={"pending": None, "decision": None, "released": (pending,)}
    )

    def consumed(attempt):
        raise DataError("nonce_conflict", "unknown execution occupies the recorded nonce")

    with pytest.raises(DataError, match="recorded nonce"):
        verify_closure(SimpleNamespace(require_nonce_unused=consumed), closing)


def test_the_runtime_releases_an_expired_attempt_and_decides_again(production_attempt):
    state, pending, _ = production_attempt
    journal = MemoryJournal(releasable(state))

    def expired_broadcast(p):
        raise DataError("transaction_expired", "past its deadline")

    faults = []
    gateway = SimpleNamespace(receipt=lambda p: None, broadcast=expired_broadcast)
    runtime = coordinator(journal, gateway, fault=lambda stage, p: faults.append(stage))
    assert runtime.tick() is False
    assert journal.load().pending is None and journal.load().decision is None
    assert journal.load().released == (pending,) and journal.load().outcomes == state.outcomes
    assert faults == ["released"]


def test_other_broadcast_refusals_still_stop_the_coordinator(production_attempt):
    state, pending, _ = production_attempt
    journal = MemoryJournal(releasable(state))

    def reverted(p):
        raise DataError("rpc", "eth_call: RPC rejected the requested read (JSON-RPC 3)")

    runtime = coordinator(journal, SimpleNamespace(receipt=lambda p: None, broadcast=reverted))
    with pytest.raises(DataError, match="JSON-RPC 3"):
        runtime.tick()
    assert journal.load().pending == pending and journal.load().released == ()


def v4_pending_state(timestamp_offset):
    """A retained Uniswap V4 swap held as a pending attempt, and a head relative to its deadline."""
    from tests.uniswap_support import live_state

    recorded = live_state("trading")
    index = next(i for i, o in enumerate(recorded.outcomes) if isinstance(o.operation, v4.Swap))
    outcome = recorded.outcomes[index]
    pending = v4.Pending(
        attempt_id="v4-expired",
        decision=outcome.decision,
        operation=outcome.operation,
        before=outcome.before,
        call=outcome.call,
        execution=outcome.execution,
        nonce=outcome.nonce,
        gas_estimate=100_000,
        gas=120_000,
        gas_price=1,
        raw_transaction="0x01",
        tx_hash=outcome.tx_hash,
    )
    state = recorded.model_copy(
        update={
            "mode": "running",
            "decision": pending.decision,
            "owned": (outcome.before.owned,),
            "outcomes": recorded.outcomes[:index],
            "delivered": index,
            "pending": pending,
            "closure": (),
        }
    )
    head = pending.before.block.model_copy(
        update={"timestamp": pending.operation.deadline + timestamp_offset}
    )
    return state, pending, head


def v4_gateway(state, pending, head):
    from almanak.gateway.execution import EvmGateway
    from tests.test_production import Reader

    gateway = object.__new__(EvmGateway)
    gateway.config = state.config
    gateway.reader = Reader(head)
    gateway.reader.responses.update(CLEAN, eth_getTransactionCount=hex(pending.nonce))
    gateway.guard = lambda: None
    gateway.verify_authority = lambda manifest, account: None
    gateway.validate_pending = lambda p: None
    gateway.current = []
    gateway.validate_current = gateway.current.append
    return gateway


def test_a_v4_swap_past_its_deadline_is_released_and_its_checkpoint_round_trips():
    from almanak.connectors.uniswap_v4.state import Checkpoint as V4Checkpoint

    # At the deadline the router still accepts a block sharing the head's timestamp.
    state, pending, head = v4_pending_state(0)
    gateway = v4_gateway(state, pending, head)
    gateway.check_submission(pending, None)
    assert gateway.current == [pending]

    state, pending, head = v4_pending_state(1)
    gateway = v4_gateway(state, pending, head)
    with pytest.raises(DataError) as error:
        gateway.check_submission(pending, None)
    assert error.value.kind == "transaction_expired" and gateway.current == []

    journal = MemoryJournal(state)
    runtime = coordinator(
        journal,
        SimpleNamespace(
            receipt=lambda p: None, broadcast=lambda p: gateway.check_submission(p, None)
        ),
    )
    assert runtime.tick() is False
    released = journal.load()
    assert released.pending is None and released.decision is None
    assert released.released == (pending,) and released.outcomes == state.outcomes

    # The gateway re-proves the release against the same V4 connector before admitting it.
    service = gateway_service(state, gateway)
    service.validate_pending_resolution(state, released)
    service.validate_released_record(state, released)
    gateway.reader.responses["eth_getTransactionByHash"] = {}
    with pytest.raises(ValueError, match="provider still holds"):
        service.validate_pending_resolution(state, released)

    dumped = released.model_dump_json()
    assert V4Checkpoint.model_validate_json(dumped) == released
    assert "released" in json.loads(dumped)
    assert "released" not in json.loads(state.model_dump_json())


COMPOSED = {
    # A V4 swap and a Uniswap V3 swap, each beside Aave V3 lending in one composed fund.
    "uniswap_v4": "composed-v4-closed-fork.json.gz",
    "uniswap_v3": "composed-base-safe-lifecycle.json.gz",
}


def composed_pending_state(connector, timestamp_offset):
    from pathlib import Path

    from almanak.connectors.composed.codec import codec
    from almanak.connectors.composition import checkpoint_from_json
    from almanak.connectors.target_routing import target_connector
    from tests.lending_captures import read_capture
    from tests.uniswap_support import scope_only_json

    path = Path(__file__).with_name("fixtures") / COMPOSED[connector]
    closed = checkpoint_from_json(scope_only_json(read_capture(path)))
    index = next(
        i
        for i, o in enumerate(closed.outcomes)
        if str(target_connector(o.operation.target)) == connector and o.operation.kind == "swap"
    )
    outcome = closed.outcomes[index]
    pending = codec().pending(
        **{f: getattr(outcome, f) for f in ("decision", "operation", "before", "call")},
        attempt_id="a" * 32,
        execution=outcome.execution,
        nonce=outcome.nonce,
        gas_estimate=1,
        gas=1,
        gas_price=1,
        raw_transaction="0x12",
        tx_hash=outcome.tx_hash,
    )
    state = closed.model_copy(
        update={
            "mode": "running",
            "decision": pending.decision,
            "pending": pending,
            "outcomes": closed.outcomes[:index],
            "delivered": index,
            "closure": (),
        }
    )
    head = pending.before.block.model_copy(
        update={"timestamp": pending.operation.deadline + timestamp_offset}
    )
    return closed, state, pending, head


@pytest.mark.parametrize("connector", sorted(COMPOSED))
def test_a_composed_swap_past_its_deadline_is_released_through_its_component(
    connector, monkeypatch
):
    from almanak.connectors.composed import state as composed
    from almanak.connectors.composed.execution import ComposedExecution

    closed, state, pending, head = composed_pending_state(connector, 0)
    assert not ComposedExecution.expired(pending.operation, head)
    lending = next(o for o in closed.outcomes if o.operation.kind == "supply")
    assert not ComposedExecution.expired(lending.operation, SimpleNamespace(timestamp=2**63))

    closed, state, pending, head = composed_pending_state(connector, 1)
    assert ComposedExecution.expired(pending.operation, head)
    gateway = v4_gateway(state, pending, head)
    with pytest.raises(DataError) as error:
        gateway.check_submission(pending, None)
    assert error.value.kind == "transaction_expired" and gateway.current == []

    journal = MemoryJournal(state)
    runtime = coordinator(
        journal,
        SimpleNamespace(
            receipt=lambda p: None,
            broadcast=lambda p: gateway.check_submission(p, None),
            # Composed funds scan for external effects first; there are none here.
            external_reconcile=lambda: None,
        ),
    )
    assert runtime.tick() is False
    released = journal.load()
    assert released.pending is None and released.decision is None
    assert released.released == (pending,) and released.outcomes == state.outcomes
    service = gateway_service(state, gateway)
    service.validate_pending_resolution(state, released)
    service.validate_released_record(state, released)
    service.validate_owned_evidence(state, released)
    validate_execution_update(gateway, state, released)

    # The released attempt decodes as its component's record, never through the full union.
    body = released.model_dump_json()
    assert len(json.loads(body)["released"]) == 1
    requested = []
    original = composed.record_adapter
    monkeypatch.setattr(
        composed, "record_adapter", lambda kind: requested.append(kind) or original(kind)
    )
    restored = composed.Checkpoint.model_validate_json(body)
    monkeypatch.undo()
    assert restored == released and type(restored.released[0]) is type(pending)
    assert composed.RECORDS[connector][2] in requested
    assert not set(requested) & set(composed.UNIONS)
    # A checkpoint retained before composed release decodes and serializes unchanged.
    assert "released" not in json.loads(closed.model_dump_json())
    assert composed.Checkpoint.model_validate_json(closed.model_dump_json()) == closed


def test_released_chain_work_is_routed_by_its_own_target(monkeypatch):
    from almanak.connectors.composed import state as composed
    from tests.test_composed_record_routing import rewound_work

    active = rewound_work()
    item = active.work[0]
    held = active.model_copy(
        update={
            "work": (
                item.model_copy(
                    update={"pending": None, "decision": None, "released": (item.pending,)}
                ),
                *active.work[1:],
            )
        }
    )
    body = held.model_dump_json()
    requested = []
    original = composed.record_adapter
    monkeypatch.setattr(
        composed, "record_adapter", lambda kind: requested.append(kind) or original(kind)
    )
    restored = composed.Checkpoint.model_validate_json(body)
    monkeypatch.undo()
    assert restored == held and restored.work[0].released == (item.pending,)
    assert not set(requested) & set(composed.UNIONS)
    # The released attempt and the other chain's pending each decode as the component's
    # Pending; an unrouted release would decode inside the chain-work shell instead.
    pending_type = composed.RECORDS["aave_v3"][2]
    assert type(item.pending) is pending_type and requested.count(pending_type) == 2
    expected = composed.record_adapter(composed.Work).validate_json(
        json.dumps(json.loads(body)["work"][0])
    )
    assert restored.work[0] == expected
    assert restored.work[0].model_fields_set == expected.model_fields_set


def v4_attempt(kind, *, unsent):
    """A retained V4 attempt of `kind`, recorded as never sent or as maybe sent."""
    state, pending, head = v4_pending_state(-1)
    from tests.uniswap_support import live_state

    outcome = next(o for o in live_state("trading").outcomes if o.operation.kind == kind)
    pending = pending.model_copy(
        update={
            f: getattr(outcome, f) for f in ("decision", "operation", "before", "call", "nonce")
        }
        | {"execution": outcome.execution, "tx_hash": outcome.tx_hash, "unsent": unsent}
    )
    state = state.model_copy(update={"pending": pending, "decision": pending.decision})
    return state, pending, head


def refusing(gateway, kind):
    def refuse(pending):
        raise DataError(kind, "the pool moved past the signed minimums")

    gateway.validate_current = refuse
    return gateway


@pytest.mark.parametrize("kind", ["approve_permit2", "swap"])
def test_a_never_sent_attempt_failing_validation_is_released_and_replanned(kind):
    state, pending, head = v4_attempt(kind, unsent=True)
    gateway = refusing(v4_gateway(state, pending, head), "simulation_reverted")
    with pytest.raises(DataError) as error:
        gateway.check_submission(pending, None)
    # Never sent, the refusal is reported as is; the runtime releases the attempt.
    assert error.value.kind == "simulation_reverted"
    journal = MemoryJournal(state)
    runtime = coordinator(
        journal,
        SimpleNamespace(
            receipt=lambda p: None, broadcast=lambda p: gateway.check_submission(p, None)
        ),
    )
    assert runtime.tick() is False
    released = journal.load()
    assert released.pending is None and released.decision is None
    # Bytes never sent cannot land, so the release retains nothing at the nonce.
    assert released.released == ()
    # The gateway admits it on its retained never-sent proof, with no deadline passed.
    service = gateway_service(state, gateway)
    service.validate_pending_resolution(state, released)
    service.validate_released_record(state, released)


@pytest.mark.parametrize("unsent", [False, None])
def test_a_maybe_sent_attempt_without_a_calldata_deadline_stops_with_its_remedy(unsent):
    state, pending, head = v4_attempt("approve_permit2", unsent=bool(unsent))
    if unsent is None:
        # Attempts recorded before the marker existed carry none: maybe sent.
        raw = json.loads(pending.model_dump_json())
        assert "unsent" not in raw
        pending = v4.Pending.model_validate_json(json.dumps(raw))
        state = state.model_copy(update={"pending": pending})
    gateway = refusing(v4_gateway(state, pending, head), "simulation_reverted")
    with pytest.raises(DataError) as error:
        gateway.check_submission(pending, None)
    assert error.value.kind == "signed_attempt_unsendable"
    submitter = state.config.accounts.binding(pending.operation.account).profile.submitter
    for fact in (f"nonce {pending.nonce}", submitter.address, "simulation_reverted", "cancel"):
        assert fact in str(error.value)
    journal = MemoryJournal(state)
    runtime = coordinator(
        journal,
        SimpleNamespace(
            receipt=lambda p: None, broadcast=lambda p: gateway.check_submission(p, None)
        ),
    )
    with pytest.raises(DataError, match="signed_attempt_unsendable|no longer passes"):
        runtime.tick()
    assert journal.load().pending == pending
    with pytest.raises(ValueError, match="never sent or past its protocol deadline"):
        gateway.require_releasable(pending)


def test_a_maybe_sent_attempt_with_a_calldata_deadline_waits_for_it():
    state, pending, head = v4_attempt("swap", unsent=False)
    gateway = refusing(v4_gateway(state, pending, head), "simulation_reverted")
    # Its deadline will prove it dead, so the refusal stands until the release.
    with pytest.raises(DataError) as error:
        gateway.check_submission(pending, None)
    assert error.value.kind == "simulation_reverted"


def test_only_calldata_deadlines_are_enforced():
    from almanak.connectors.aerodrome.execution import AerodromeExecution
    from almanak.connectors.aerodrome.operations import Collect as AerodromeCollect
    from almanak.connectors.aerodrome.operations import Swap as AerodromeSwap
    from almanak.connectors.composed.execution import ComposedExecution
    from almanak.connectors.curve.execution import CurveExecution
    from almanak.connectors.curve.operations import Removal as CurveRemoval
    from almanak.connectors.curve.operations import Swap as CurveSwap
    from almanak.connectors.uniswap_v3.execution import UniswapExecution
    from almanak.connectors.uniswap_v3.operations import Close, Swap
    from almanak.connectors.uniswap_v4.execution import UniswapExecution as V4Execution
    from almanak.gateway.execution import EvmGateway

    def enforces(execution, operation):
        backend = SimpleNamespace(connector=SimpleNamespace(execution=execution))
        return EvmGateway.enforces_deadline(backend, operation)

    for operation in (CurveSwap.model_construct(deadline=1), CurveRemoval.model_construct()):
        assert not enforces(CurveExecution, operation)
    assert not enforces(UniswapExecution, Collect.model_construct(deadline=1))
    assert enforces(UniswapExecution, Swap.model_construct(deadline=1))
    assert enforces(UniswapExecution, Close.model_construct(deadline=1))
    assert not enforces(AerodromeExecution, AerodromeCollect.model_construct(deadline=1))
    assert enforces(AerodromeExecution, AerodromeSwap.model_construct(deadline=1))
    assert not enforces(V4Execution, v4.Approval.model_construct(expiration=1))
    assert enforces(V4Execution, v4.Swap.model_construct(deadline=1))
    _, _, pending, _ = composed_pending_state("uniswap_v4", 1)
    assert enforces(ComposedExecution, pending.operation)


def test_signing_records_an_attempt_as_never_sent_until_its_send():
    from almanak.transaction_family import is_unsent, original_plan

    state, pending, head = v4_pending_state(-1)
    gateway = v4_gateway(state, pending, head)
    plan = original_plan(pending)
    signed = SimpleNamespace(raw_transaction=b"\x01", hash=bytes.fromhex(pending.tx_hash[2:]))
    fresh = gateway.signed_pending(SimpleNamespace(plan=plan), signed)
    assert is_unsent(fresh) and original_plan(fresh) == plan
    assert json.loads(fresh.model_dump_json())["unsent"] is True
    # The send itself refuses bytes whose send is not yet durably recorded.
    with pytest.raises(ValueError, match="durably recorded"):
        gateway.broadcast(fresh)


def journal_holding(path, state):
    """A durable journal whose checkpoint is `state`, built as `Journal.create` lays it out."""
    import sqlite3

    from almanak.state import Journal

    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE checkpoint (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
        connection.execute("INSERT INTO checkpoint VALUES (1, ?)", (state.model_dump_json(),))
        connection.execute(
            "CREATE TABLE coordinator (id INTEGER PRIMARY KEY, identity TEXT, expires REAL)"
        )
        connection.execute("INSERT INTO coordinator VALUES (1, NULL, 0)")
    journal = Journal(path)
    journal.acquire("coordinator", 30)
    return journal


def fenced_service(tmp_path, state, adapter):
    from almanak.gateway.service import GatewayService

    journal = journal_holding(tmp_path / "state.sqlite", state)
    service = object.__new__(GatewayService)
    service.config, service.journal, service.lease_seconds = state.config, journal, 30
    service.backend, service.route_chain, service.check = adapter, None, lambda state: None
    adapter.audit_history = lambda state: None
    return service


def fenced_run(service, chain=None):
    request = SimpleNamespace(coordinator="coordinator", chain=chain)
    body = service.journal.read_fenced("coordinator")
    commit = service.fenced_commit(request)
    return service.run_fenced("broadcast", request, body, commit, durable=commit)


def fenced_broadcast(tmp_path, adapter):
    state, pending, _ = v4_attempt("swap", unsent=True)
    service = fenced_service(tmp_path, state, adapter)
    result, failure = fenced_run(service)
    return service.journal, pending, result, failure


def test_the_send_is_recorded_durably_before_the_bytes_leave(tmp_path):
    from almanak.transaction_family import is_unsent

    seen = []

    def submit(state):
        # Read through another connection while the send holds the lock.
        from almanak.state import Journal

        seen.append((state.pending, Journal(tmp_path / "state.sqlite").load().pending))
        return state, True

    adapter = SimpleNamespace(prepare_submission=lambda state: None, submit=submit)
    journal, pending, result, failure = fenced_broadcast(tmp_path, adapter)
    assert failure is None and result is True
    [(sent, durable)] = seen
    assert not is_unsent(sent) and sent == durable
    assert sent == pending.model_copy(update={"unsent": False})
    assert journal.load().pending == sent


def test_a_refused_first_send_keeps_the_never_sent_proof(tmp_path):
    def refuse(state):
        raise DataError("simulation_reverted", "the pool moved past the signed minimums")

    from almanak.state import Journal

    adapter = SimpleNamespace(prepare_submission=refuse, submit=None)
    with pytest.raises(DataError, match="signed minimums"):
        fenced_broadcast(tmp_path, adapter)
    _, pending, _ = v4_attempt("swap", unsent=True)
    assert Journal(tmp_path / "state.sqlite").load().pending == pending


def test_a_crash_after_recording_the_send_leaves_the_attempt_maybe_sent(tmp_path):
    from almanak.transaction_family import is_unsent

    def crash(state):
        raise RuntimeError("gateway killed before the send returned")

    adapter = SimpleNamespace(prepare_submission=lambda state: None, submit=crash)
    with pytest.raises(RuntimeError, match="killed"):
        fenced_broadcast(tmp_path, adapter)
    from almanak.state import Journal

    retained = Journal(tmp_path / "state.sqlite").load().pending
    assert retained is not None and not is_unsent(retained)


def planned_attempt():
    from almanak.transaction_family import original_plan

    state, pending, head = v4_attempt("swap", unsent=True)
    planned = state.model_copy(update={"pending": None, "plan": original_plan(pending)})
    return planned, pending


@pytest.mark.parametrize("kind", ["simulation_reverted", "precondition_changed"])
def test_a_sign_then_refuse_loop_stops_at_the_replan_limit(kind):
    from almanak.runtime import REPLAN_ATTEMPTS, ReplansExhausted

    planned, pending = planned_attempt()

    def refuse(p):
        raise DataError(kind, "the first send is refused every time")

    journal = MemoryJournal(planned)
    gateway = SimpleNamespace(sign=lambda: pending, receipt=lambda p: None, broadcast=refuse)
    runtime = coordinator(journal, gateway)
    for attempt in range(1, REPLAN_ATTEMPTS + 1):
        # A fresh decision plans and signs the same step again at the same nonce.
        journal.state = planned
        assert runtime.tick() is False and journal.load().pending == pending
        if attempt == REPLAN_ATTEMPTS:
            with pytest.raises(ReplansExhausted):
                runtime.tick()
            break
        assert runtime.tick() is False
        released = journal.load()
        assert released.pending is None and released.released == ()
        assert runtime.replanning().attempts == {None: attempt}


def test_an_operator_learns_its_released_request_was_not_executed():
    from almanak.operator_runtime import OperatorRuntime

    state, pending, head = v4_attempt("swap", unsent=True)
    gateway = refusing(v4_gateway(state, pending, head), "simulation_reverted")
    journal = MemoryJournal(state)
    runtime = coordinator(
        journal,
        SimpleNamespace(
            receipt=lambda p: None, broadcast=lambda p: gateway.check_submission(p, None)
        ),
    )
    runtime.__class__ = OperatorRuntime
    with pytest.raises(DataError, match="submit the request again") as error:
        runtime.tick()
    assert error.value.kind == "simulation_reverted"
    assert journal.load().pending is None and journal.load().decision is None


def test_connectors_without_the_never_sent_proof_keep_their_own_refusal(production_attempt):
    state, pending, _ = production_attempt
    backend = expirable(backend_for(state.config, pending.before.position.reserve.block), pending)
    backend.verify_authority = lambda manifest, account: None
    backend.expired_at_head = lambda operation: False

    def refuse(p):
        raise DataError("stale_plan", "GMX swap admission deadline expired before submission")

    backend.validate_current = refuse
    with pytest.raises(DataError) as error:
        backend.check_submission(pending, None)
    assert error.value.kind == "stale_plan"


@pytest.mark.parametrize("stale", ["head", "transient"])
def test_a_lagging_head_or_transient_read_is_not_a_stuck_attempt(stale):
    from almanak.gateway.execution import StaleObservation

    state, pending, head = v4_attempt("approve_permit2", unsent=False)
    gateway = v4_gateway(state, pending, head)

    def refuse(p):
        if stale == "head":
            raise StaleObservation("stale_plan", "execution observation expired")
        raise DataError("simulation_reverted", "provider read failed", transient=True)

    gateway.validate_current = refuse
    with pytest.raises(DataError) as error:
        gateway.check_submission(pending, None)
    assert error.value.kind != "signed_attempt_unsendable"


def multichain_attempts():
    """The two-chain composed capture with each chain's last V3 swap signed, never sent."""
    from almanak.connectors.composed import state as composed
    from almanak.connectors.composed.codec import codec
    from tests.test_composed_record_routing import capture

    closed = composed.Checkpoint.model_validate_json(
        capture("composed-multichain-safe-lifecycle.json.gz")
    )
    swaps = {
        o.decision.intent.target.fund.chain: (i, o)
        for i, o in enumerate(closed.outcomes)
        if o.operation.kind == "swap"
    }
    first = min(i for i, _ in swaps.values())
    work = []
    for item in closed.work:
        _, outcome = swaps[item.chain]
        pending = codec().pending(
            **{f: getattr(outcome, f) for f in ("decision", "operation", "before", "call")},
            attempt_id="a" * 32,
            execution=outcome.execution,
            nonce=outcome.nonce,
            gas_estimate=1,
            gas=1,
            gas_price=1,
            raw_transaction="0x12",
            tx_hash=outcome.tx_hash,
            unsent=True,
        )
        work.append(
            item.model_copy(
                update={"closed": False, "decision": outcome.decision, "pending": pending}
            )
        )
    return closed.model_copy(
        update={
            "mode": "running",
            "work": tuple(work),
            "outcomes": closed.outcomes[:first],
            "delivered": first,
            "closure": (),
        }
    )


def test_a_chain_scoped_send_is_recorded_on_its_own_chain_work(tmp_path):
    from almanak.state import Journal
    from almanak.transaction_family import is_unsent
    from almanak.work import project

    state = multichain_attempts()
    chain = state.work[1].chain
    seen = []
    adapter = SimpleNamespace(
        reader=SimpleNamespace(chain=chain),
        prepare_submission=lambda local: None,
        submit=lambda local: (
            seen.append(Journal(tmp_path / "state.sqlite").load()),
            (local, True),
        )[1],
    )
    service = fenced_service(tmp_path, state, adapter)
    result, failure = fenced_run(service, chain)
    assert failure is None and result is True
    [durable] = seen
    assert not is_unsent(project(durable, chain).pending)
    # The other chain's attempt keeps its never-sent proof.
    other = state.work[0].chain
    assert is_unsent(project(durable, other).pending)
    assert project(durable, other) == project(state, other)


def test_a_write_ahead_refuses_a_checkpoint_changed_since_it_was_read(tmp_path):
    from almanak.gateway.fenced import Writes
    from almanak.state import Journal

    state, pending, _ = v4_attempt("swap", unsent=True)
    service = fenced_service(tmp_path, state, SimpleNamespace())
    request = SimpleNamespace(coordinator="coordinator", chain=None)
    body = service.journal.read_fenced("coordinator")
    changed = state.model_copy(update={"pending": pending.model_copy(update={"nonce": 99})})
    with service.journal.authorized("coordinator", 30) as (db, _):
        db.execute("UPDATE checkpoint SET body=? WHERE id=1", (changed.model_dump_json(),))
    writes = Writes(body, service.fenced_commit(request))
    with pytest.raises(DataError) as error:
        writes.write_ahead(lambda current: current.model_copy(update={"pending": None}))
    assert error.value.kind == "conflict"
    assert Journal(tmp_path / "state.sqlite").load() == changed


def test_a_lease_lost_between_recording_and_sending_sends_nothing(tmp_path):
    import sqlite3
    import time

    from almanak.state import Journal
    from almanak.transaction_family import is_unsent

    sent = []
    adapter = SimpleNamespace(
        prepare_submission=lambda state: None, submit=lambda state: sent.append(state)
    )
    state, pending, _ = v4_attempt("swap", unsent=True)
    service = fenced_service(tmp_path, state, adapter)
    record = service.record_send

    def record_then_expire(db, state, local):
        marked = record(db, state, local)
        with sqlite3.connect(tmp_path / "state.sqlite") as connection:
            connection.execute("UPDATE coordinator SET expires=?", (time.time() - 1,))
        return marked

    service.record_send = record_then_expire
    with pytest.raises(RuntimeError, match="lease"):
        fenced_run(service)
    assert sent == []
    retained = Journal(tmp_path / "state.sqlite").load().pending
    assert retained is not None and not is_unsent(retained)
