"""Identity and fallback branches for runner session recovery."""

from types import SimpleNamespace

import pytest

from almanak.framework.execution.session import ExecutionSession, TransactionState
from almanak.framework.runner.runner_recovery import is_duplicate_transaction, recover_session


def _runner() -> SimpleNamespace:
    return SimpleNamespace(
        _recovered_tx_hashes={"0xrecovered"},
        _recovered_nonces={"deployment:one": {7}},
    )


@pytest.mark.asyncio
async def test_recover_session_rejects_unknown_phase_without_mutating_existing_identity_scope() -> None:
    runner = _runner()
    session = ExecutionSession(
        session_id="session-unknown",
        deployment_id="deployment:one",
        intent_id="intent-unknown",
    )
    session.phase = SimpleNamespace(value="SETTLED")
    session.transactions = [TransactionState()]

    assert await recover_session(runner, session) is False
    assert runner._recovered_tx_hashes == {"0xrecovered"}
    assert runner._recovered_nonces == {"deployment:one": {7}}


def test_duplicate_transaction_hash_is_global_across_deployments() -> None:
    assert is_duplicate_transaction(
        _runner(),
        tx_hash="0xrecovered",
        deployment_id="deployment:two",
    )


def test_duplicate_transaction_nonce_is_scoped_to_deployment() -> None:
    runner = _runner()

    assert is_duplicate_transaction(runner, nonce=7, deployment_id="deployment:one")
    assert not is_duplicate_transaction(runner, nonce=7, deployment_id="deployment:two")


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"tx_hash": "0xnew"},
        {"nonce": 7},
        {"nonce": 0, "deployment_id": "deployment:one"},
    ],
)
def test_duplicate_transaction_requires_a_recovered_hash_or_scoped_nonce(kwargs: dict[str, object]) -> None:
    assert not is_duplicate_transaction(_runner(), **kwargs)
