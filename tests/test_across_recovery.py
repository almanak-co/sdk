"""A signed Across deposit that can no longer be mined: released past its quote deadline,
and held with a conflict when another transaction took its nonce.

Component level: the real gateway checks and the real release, over fixed chain answers.
No process is killed and no fork runs here.
"""

import pytest

from almanak.connectors.across.execution import AcrossExecution
from almanak.connectors.across.operations import ApproveOperation
from almanak.connectors.across.state import Checkpoint
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.request_runtime import release_pending
from almanak.work import merge, project, work_for
from tests.test_across_records import (  # noqa: F401
    FORK_ARB,
    FORK_BASE,
    FORK_BLOCK,
    checkpoint,
    config,
    deposit,
    observed,
    pending,
)
from tests.test_expired_attempt_release import CLEAN, gateway_service
from tests.test_production import Reader


def signed(config, operation=None):  # noqa: F811
    """The deployment with a signed and sent step on its source chain."""
    attempt = pending(config, operation or deposit(config), observed(config))
    state = checkpoint(config)
    work = tuple(
        w.model_copy(update={"pending": attempt, "decision": attempt.decision})
        if w.chain == FORK_ARB
        else w
        for w in state.work
    )
    return state.model_copy(update={"work": work}), attempt


def gateway(config, attempt, head_time, **responses):  # noqa: F811
    backend = object.__new__(EvmGateway)
    backend.config = config
    backend.reader = Reader(FORK_BLOCK.model_copy(update={"timestamp": head_time}))
    backend.reader.responses.update(
        {**CLEAN, "eth_getTransactionCount": hex(attempt.nonce), **responses}
    )
    backend.guard = lambda: None
    backend.verify_authority = lambda manifest, account: None
    backend.validate_pending = lambda p: None
    backend.current = []
    backend.validate_current = backend.current.append
    return backend


def test_a_deposit_past_its_quote_deadline_is_released_on_its_source_chain(config):  # noqa: F811
    state, attempt = signed(config)
    deadline = attempt.operation.deadline
    assert deadline == attempt.operation.quote.timestamp + attempt.operation.quote_time_buffer

    # At the deadline the SpokePool still mines it, so it is sent.
    live = gateway(config, attempt, deadline)
    live.check_submission(attempt, None)
    assert live.current == [attempt]
    with pytest.raises(ValueError, match="never sent or past its protocol deadline"):
        live.require_releasable(attempt)

    late = gateway(config, attempt, deadline + 1)
    with pytest.raises(DataError) as error:
        late.check_submission(attempt, None)
    assert error.value.kind == "transaction_expired" and late.current == []
    late.require_releasable(attempt)

    local = project(state, FORK_ARB)
    released = release_pending(local)
    assert released.pending is None and released.decision is None
    assert released.released == (attempt,)
    service = gateway_service(local, late)
    service.validate_pending_resolution(local, released)
    service.validate_released_record(local, released)

    # The release stays on the source chain, opens no transfer, and survives a round trip.
    saved = merge(state, FORK_ARB, released)
    assert work_for(saved, FORK_ARB).released == (attempt,)
    assert work_for(saved, FORK_BASE).released == () and saved.transfers == ()
    assert Checkpoint.model_validate_json(saved.model_dump_json()) == saved


@pytest.mark.parametrize(
    ("answers", "refusal"),
    [
        ({"eth_getTransactionReceipt": {"status": "0x1"}}, "resolved only by its recorded"),
        ({"eth_getTransactionByHash": {}}, "provider still holds"),
    ],
)
def test_an_expired_deposit_the_chain_may_still_hold_is_not_released(
    config,  # noqa: F811
    answers,
    refusal,
):
    state, attempt = signed(config)
    late = gateway(config, attempt, attempt.operation.deadline + 1, **answers)
    late.receipt = lambda p: answers.get("eth_getTransactionReceipt")
    with pytest.raises(ValueError, match=refusal):
        late.require_releasable(attempt)


def test_a_deposit_whose_nonce_was_taken_stops_with_a_conflict(config):  # noqa: F811
    """A strategy deployment never drops a step for a consumed nonce: before and after
    the quote deadline the step is held with `nonce_conflict` and is not released."""
    state, attempt = signed(config)
    for head in (attempt.operation.deadline, attempt.operation.deadline + 1):
        taken = gateway(config, attempt, head, eth_getTransactionCount=hex(attempt.nonce + 1))
        with pytest.raises(DataError) as error:
            taken.check_submission(attempt, None)
        assert error.value.kind == "nonce_conflict" and taken.current == []
        with pytest.raises(DataError) as error:
            taken.require_releasable(attempt)
        assert error.value.kind == "nonce_conflict"


def test_an_approval_carries_no_deadline_and_is_never_released_as_expired(config):  # noqa: F811
    approval = ApproveOperation(
        target=config.target("move"), amount=config.bridge("move", 1).amount
    )
    state, attempt = signed(config, approval)
    assert not EvmGateway.enforces_deadline(gateway(config, attempt, 2**62), approval)
    assert not AcrossExecution.expired(approval, FORK_BLOCK.model_copy(update={"timestamp": 2**62}))
    late = gateway(config, attempt, 2**62)
    late.check_submission(attempt, None)
    assert late.current == [attempt]
    with pytest.raises(ValueError, match="never sent or past its protocol deadline"):
        late.require_releasable(attempt)
