"""Branch coverage for session recovery of submitted transactions.

Covers ``recover_submitted_session`` (no-hash, multi-chain guard, receipt
outcomes, timeout, hard error), plus the ``recover_session`` phase routing
and ``recover_incomplete_sessions`` aggregation. The runner is duck-typed;
everything is faked in memory.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.session import (
    ExecutionPhase,
    ExecutionSession,
    TransactionState,
    TransactionStatus,
)
from almanak.framework.runner.runner_recovery import (
    recover_incomplete_sessions,
    recover_session,
    recover_submitted_session,
)


def _session(phase=ExecutionPhase.SUBMITTED, tx_hashes=("0xaaa",), nonces=None):
    session = ExecutionSession(
        session_id="sess-1",
        deployment_id="deployment:abc123",
        intent_id="intent-1",
        phase=phase,
    )
    for i, tx_hash in enumerate(tx_hashes):
        nonce = (nonces or {}).get(tx_hash, i + 1)
        session.transactions.append(
            TransactionState(tx_hash=tx_hash, nonce=nonce, status=TransactionStatus.SUBMITTED)
        )
    return session


def _receipt(tx_hash="0xaaa", success=True):
    return SimpleNamespace(tx_hash=tx_hash, success=success, gas_used=21000, block_number=777)


def _runner(*, receipts=None, multi_chain=False, side_effect=None):
    submitter = MagicMock()
    submitter.get_receipts = AsyncMock(return_value=receipts or [], side_effect=side_effect)
    return SimpleNamespace(
        _session_store=MagicMock(),
        _is_multi_chain=multi_chain,
        execution_orchestrator=SimpleNamespace(submitter=submitter),
        _recovered_tx_hashes=set(),
        _recovered_nonces={},
    )


@pytest.fixture(autouse=True)
def _no_state_update(monkeypatch):
    updates = []

    async def _update(runner, session):
        updates.append(session.session_id)

    monkeypatch.setattr(
        "almanak.framework.runner.runner_recovery.update_recovered_state", _update
    )
    return updates


class TestRecoverSubmittedSession:
    def test_no_session_store_returns_false(self):
        runner = _runner()
        runner._session_store = None
        assert asyncio.run(recover_submitted_session(runner, _session())) is False

    def test_no_tx_hashes_marks_failed(self):
        runner = _runner()
        session = _session(tx_hashes=())
        assert asyncio.run(recover_submitted_session(runner, session)) is True
        assert session.completed
        assert not session.success
        assert "No transaction hashes" in session.last_error
        runner._session_store.save.assert_called_once_with(session)

    def test_multi_chain_mode_marks_failed(self):
        runner = _runner(multi_chain=True)
        session = _session()
        assert asyncio.run(recover_submitted_session(runner, session)) is True
        assert session.completed
        assert not session.success
        assert "multi-chain" in session.last_error

    def test_all_confirmed_marks_success_and_updates_state(self, _no_state_update):
        runner = _runner(receipts=[_receipt("0xaaa"), _receipt("0xbbb")])
        session = _session(tx_hashes=("0xaaa", "0xbbb"))
        assert asyncio.run(recover_submitted_session(runner, session)) is True
        assert session.success
        assert all(tx.status == TransactionStatus.CONFIRMED for tx in session.transactions)
        assert session.transactions[0].gas_used == 21000
        assert _no_state_update == ["sess-1"]

    def test_reverted_receipt_marks_failure(self, _no_state_update):
        runner = _runner(receipts=[_receipt("0xaaa", success=False)])
        session = _session()
        assert asyncio.run(recover_submitted_session(runner, session)) is True
        assert session.completed
        assert not session.success
        assert session.transactions[0].status == TransactionStatus.FAILED
        assert _no_state_update == []

    def test_timeout_marks_failed_without_raising(self):
        runner = _runner(side_effect=TimeoutError())
        session = _session()
        assert asyncio.run(recover_submitted_session(runner, session)) is True
        assert not session.success
        assert "Timeout" in session.last_error

    def test_unexpected_error_propagates(self):
        runner = _runner(side_effect=RuntimeError("rpc exploded"))
        with pytest.raises(RuntimeError, match="rpc exploded"):
            asyncio.run(recover_submitted_session(runner, _session()))


class TestRecoverSessionRouting:
    @pytest.mark.parametrize(
        "phase", [ExecutionPhase.SUBMITTED, ExecutionPhase.CONFIRMING]
    )
    def test_submitted_phases_poll_receipts(self, phase):
        runner = _runner(receipts=[_receipt()])
        session = _session(phase=phase)
        assert asyncio.run(recover_session(runner, session)) is True
        runner.execution_orchestrator.submitter.get_receipts.assert_awaited_once()

    @pytest.mark.parametrize("phase", [ExecutionPhase.PREPARING, ExecutionPhase.SIGNING])
    def test_early_phases_are_abandoned(self, phase):
        runner = _runner()
        session = _session(phase=phase, tx_hashes=())
        assert asyncio.run(recover_session(runner, session)) is True
        assert session.completed
        assert not session.success

    def test_tracks_recovered_hashes_and_nonces(self):
        runner = _runner(receipts=[_receipt()])
        session = _session(tx_hashes=("0xaaa",), nonces={"0xaaa": 5})
        asyncio.run(recover_session(runner, session))
        assert "0xaaa" in runner._recovered_tx_hashes
        assert runner._recovered_nonces["deployment:abc123"] == {5}


class TestRecoverIncompleteSessions:
    def test_no_store_returns_zero(self):
        runner = _runner()
        runner._session_store = None
        assert asyncio.run(recover_incomplete_sessions(runner)) == 0

    def test_no_incomplete_sessions(self):
        runner = _runner()
        runner._session_store.get_incomplete_sessions.return_value = []
        assert asyncio.run(recover_incomplete_sessions(runner)) == 0

    def test_counts_recovered_sessions(self):
        runner = _runner(receipts=[_receipt()])
        runner._session_store.get_incomplete_sessions.return_value = [_session()]
        assert asyncio.run(recover_incomplete_sessions(runner)) == 1

    def test_recovery_error_marks_session_failed(self):
        runner = _runner(side_effect=RuntimeError("rpc exploded"))
        session = _session()
        runner._session_store.get_incomplete_sessions.return_value = [session]
        assert asyncio.run(recover_incomplete_sessions(runner)) == 0
        assert session.completed
        assert not session.success
        assert "Recovery failed" in session.last_error
