"""End-to-end driver smoke tests for ``ExecutionOrchestrator.execute``.

These tests construct a real ``ExecutionOrchestrator`` with mocked signer /
submitter / simulator and drive ``execute()`` end-to-end to verify the
Phase 3a pipeline wiring still behaves as it did pre-refactor. Each test
asserts observable outcomes (final ExecutionResult state, error_phase,
event emissions) rather than implementation details.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.events import ExecutionEventType
from almanak.framework.execution.interfaces import (
    SimulationResult,
    SubmissionError,
    TransactionReceipt,
    TransactionRevertedError,
)
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
)
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.framework.strategies.base import RiskGuardResult


def _make_receipt(
    *,
    success: bool,
    tx_hash: str = "0xabc",
    gas_used: int = 21000,
    effective_gas_price: int = 1,
    block_number: int = 1,
) -> TransactionReceipt:
    return TransactionReceipt(
        tx_hash=tx_hash,
        block_number=block_number,
        block_hash="0xblock",
        gas_used=gas_used,
        effective_gas_price=effective_gas_price,
        status=1 if success else 0,
        logs=[],
    )


@pytest.fixture
def orchestrator():
    signer = MagicMock()
    signer.address = "0x1111111111111111111111111111111111111111"
    submitter = MagicMock()
    simulator = MagicMock()
    return ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain="arbitrum",
    )


def _install_capture(orchestrator):
    emitted: list[tuple] = []
    orig_emit = orchestrator._emit_event

    def capture(evt_type, ctx, details=None):
        emitted.append((evt_type, details or {}))
        orig_emit(evt_type, ctx, details)

    orchestrator._emit_event = capture
    return emitted


def _wire_for_happy_path(orchestrator, *, tx_count: int = 1):
    """Stub all the helpers execute() calls so we can drive a happy-path run."""
    # _phase_build helpers
    orchestrator._check_token_balance_before_submit = AsyncMock()
    # Pass the built txs through so downstream phases operate on the same
    # fabricated transactions rather than a fabricated-but-empty list.
    orchestrator._maybe_estimate_gas_limits = AsyncMock(side_effect=lambda txs, _ctx: (txs, []))
    # _phase_validate helpers
    orchestrator._validate_transactions = AsyncMock(return_value=RiskGuardResult(passed=True, violations=[]))
    orchestrator._preflight_balance_check = AsyncMock(return_value=None)
    # _phase_gas helpers
    orchestrator._update_gas_prices = AsyncMock(side_effect=lambda txs: txs)
    orchestrator._validate_gas_prices = MagicMock(return_value=RiskGuardResult(passed=True, violations=[]))
    # _phase_sign helpers
    orchestrator._assign_nonces = AsyncMock(side_effect=lambda txs, ctx: txs)
    orchestrator.signer.sign_batch = AsyncMock(return_value=[MagicMock() for _ in range(tx_count)])
    # _phase_submit_and_confirm
    submissions = [MagicMock(submitted=True, tx_hash=f"0xtx{i}", error=None) for i in range(tx_count)]
    orchestrator.submitter.submit = AsyncMock(return_value=submissions)
    orchestrator.submitter.get_receipts = AsyncMock(
        return_value=[_make_receipt(success=True, tx_hash=f"0xtx{i}") for i in range(tx_count)]
    )
    # Nonce update after success
    mock_web3 = MagicMock()
    mock_web3.eth.get_transaction_count = AsyncMock(return_value=7)
    mock_web3.to_checksum_address = lambda x: x
    orchestrator._get_web3 = AsyncMock(return_value=mock_web3)


# =============================================================================
# HOLD short-circuit
# =============================================================================


class TestExecuteHoldShortCircuit:
    @pytest.mark.asyncio
    async def test_hold_bundle_returns_success_without_signing_or_submitting(self, orchestrator):
        bundle = ActionBundle(intent_type="HOLD", transactions=[])
        orchestrator._check_token_balance_before_submit = AsyncMock()
        orchestrator.signer.sign_batch = AsyncMock()
        orchestrator.submitter.submit = AsyncMock()

        result = await orchestrator.execute(bundle)

        assert result.success is True
        assert result.phase == ExecutionPhase.COMPLETE
        # Short-circuited before any signing or submission
        orchestrator.signer.sign_batch.assert_not_called()
        orchestrator.submitter.submit.assert_not_called()


# =============================================================================
# No-op bundle
# =============================================================================


class TestExecuteNoOpBundle:
    @pytest.mark.asyncio
    async def test_no_op_metadata_with_swap_intent_returns_success(self, orchestrator):
        bundle = ActionBundle(
            intent_type="SWAP",
            transactions=[],
            metadata={"no_op": True, "reason": "nothing to swap"},
        )
        orchestrator._check_token_balance_before_submit = AsyncMock()

        result = await orchestrator.execute(bundle)

        assert result.success is True
        assert result.phase == ExecutionPhase.COMPLETE


class TestExecuteEmptyBundleFailure:
    @pytest.mark.asyncio
    async def test_swap_with_zero_txs_and_no_no_op_fails(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[])
        orchestrator._check_token_balance_before_submit = AsyncMock()

        result = await orchestrator.execute(bundle)

        assert result.success is False
        assert result.error is not None
        assert "Empty ActionBundle" in result.error
        assert result.phase == ExecutionPhase.COMPLETE


# =============================================================================
# RiskGuard block
# =============================================================================


class TestExecuteRiskGuardBlock:
    @pytest.mark.asyncio
    async def test_risk_guard_block_returns_validation_error(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 10**19}])
        orchestrator._check_token_balance_before_submit = AsyncMock()
        orchestrator._validate_transactions = AsyncMock(
            return_value=RiskGuardResult(passed=False, violations=["too big"])
        )

        events = _install_capture(orchestrator)
        result = await orchestrator.execute(bundle)

        assert result.success is False
        assert result.error_phase == ExecutionPhase.VALIDATION
        assert "too big" in (result.error or "")
        assert any(t == ExecutionEventType.RISK_BLOCKED for t, _ in events)


# =============================================================================
# Simulation failure
# =============================================================================


class TestExecuteSimulationFailure:
    @pytest.mark.asyncio
    async def test_simulation_failure_returns_simulation_error_phase(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        orchestrator._check_token_balance_before_submit = AsyncMock()
        orchestrator._validate_transactions = AsyncMock(return_value=RiskGuardResult(passed=True, violations=[]))
        orchestrator._preflight_balance_check = AsyncMock(return_value=None)
        # Enable simulation
        ctx = ExecutionContext(strategy_id="t", simulation_enabled=True)
        orchestrator.simulator.simulate = AsyncMock(
            return_value=SimulationResult(success=False, simulated=True, revert_reason="ERC20: insufficient")
        )

        events = _install_capture(orchestrator)
        result = await orchestrator.execute(bundle, ctx)

        assert result.success is False
        assert result.error_phase == ExecutionPhase.SIMULATION
        assert "ERC20: insufficient" in (result.error or "")
        assert any(t == ExecutionEventType.SIMULATION_FAILED for t, _ in events)


# =============================================================================
# Signer failure
# =============================================================================


class TestExecuteSignerFailure:
    @pytest.mark.asyncio
    async def test_signer_raises_execution_error_mapped_to_current_phase(self, orchestrator):
        from almanak.framework.execution.interfaces import ExecutionError

        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        _wire_for_happy_path(orchestrator, tx_count=1)
        # Override sign_batch to raise an ExecutionError
        orchestrator.signer.sign_batch = AsyncMock(side_effect=ExecutionError("keystore locked"))

        result = await orchestrator.execute(bundle)

        assert result.success is False
        assert "keystore locked" in (result.error or "")
        # error_phase mirrors result.phase at time of exception (SIGNING)
        assert result.error_phase == ExecutionPhase.SIGNING


# =============================================================================
# Submitter failure
# =============================================================================


class TestExecuteSubmitterFailure:
    @pytest.mark.asyncio
    async def test_submission_raises_submission_error_preserves_partial_hashes(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        _wire_for_happy_path(orchestrator, tx_count=1)
        # Override submit to raise SubmissionError (timeout)
        orchestrator.submitter.submit = AsyncMock(side_effect=SubmissionError("timeout waiting for receipt"))

        result = await orchestrator.execute(bundle)

        assert result.success is False
        assert result.error_phase == ExecutionPhase.SUBMISSION
        assert "timeout" in (result.error or "")


# =============================================================================
# Receipt timeout with partial hashes preserved
# =============================================================================


class TestReceiptTimeoutPreservesPartialHashes:
    @pytest.mark.asyncio
    async def test_partial_hashes_from_session_preserved_on_submission_error(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        _wire_for_happy_path(orchestrator, tx_count=1)

        # Force the submitter to raise AFTER session.transactions contains
        # a partial tx_hash -> exception-handler must preserve it.
        async def submit_that_records_and_raises(signed_txs):
            # Simulate: session already has a tx state with tx_hash from sequential path
            pass

        # Attach a session with a partial tx_hash that the handler will find
        class DummyTxState:
            def __init__(self, tx_hash):
                self.tx_hash = tx_hash

        class DummySession:
            def __init__(self):
                self.transactions = [DummyTxState("0xpartial")]

            def update_transaction(self, **kwargs):
                pass

        orchestrator._create_session = MagicMock(return_value=DummySession())
        orchestrator._complete_session = MagicMock()
        orchestrator._checkpoint_session = MagicMock()

        # Now, make submit raise SubmissionError
        orchestrator.submitter.submit = AsyncMock(side_effect=SubmissionError("timeout waiting for receipt"))

        result = await orchestrator.execute(bundle)

        assert result.success is False
        assert result.error_phase == ExecutionPhase.SUBMISSION
        # _handle_execution_exception should have synthesized a tx result from session
        assert len(result.transaction_results) == 1
        assert result.transaction_results[0].tx_hash == "0xpartial"
        assert result.transaction_results[0].error == "timeout_waiting_for_receipt"


# =============================================================================
# Receipt revert with verbose report
# =============================================================================


class TestReceiptRevertVerboseReport:
    @pytest.mark.asyncio
    async def test_tx_reverts_triggers_verbose_report_and_failure(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        _wire_for_happy_path(orchestrator, tx_count=1)
        # Override get_receipts to return a reverted receipt
        orchestrator.submitter.get_receipts = AsyncMock(return_value=[_make_receipt(success=False, tx_hash="0xdead")])

        events = _install_capture(orchestrator)
        with patch("almanak.framework.execution.orchestrator.build_verbose_revert_report") as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "VERBOSE FORMATTED REPORT"
            mock_report.to_dict.return_value = {"r": True}
            mock_build.return_value = mock_report

            result = await orchestrator.execute(bundle)

        assert result.success is False
        assert result.error == "VERBOSE FORMATTED REPORT"
        assert result.error_phase == ExecutionPhase.CONFIRMATION
        # Both TX_REVERTED (per-tx) and EXECUTION_FAILED (summary) must have fired
        assert any(t == ExecutionEventType.TX_REVERTED for t, _ in events)
        assert any(t == ExecutionEventType.EXECUTION_FAILED for t, _ in events)


# =============================================================================
# Happy-path full flow
# =============================================================================


class TestExecuteHappyPath:
    @pytest.mark.asyncio
    async def test_full_pipeline_returns_success(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        _wire_for_happy_path(orchestrator, tx_count=1)

        events = _install_capture(orchestrator)
        result = await orchestrator.execute(bundle)

        assert result.success is True
        assert result.phase == ExecutionPhase.COMPLETE
        assert result.completed_at is not None
        assert len(result.transaction_results) == 1
        assert result.transaction_results[0].success is True
        # Canonical success event was emitted
        assert any(t == ExecutionEventType.EXECUTION_SUCCESS for t, _ in events)
        # TX_CONFIRMED per receipt
        assert any(t == ExecutionEventType.TX_CONFIRMED for t, _ in events)

    @pytest.mark.asyncio
    async def test_dry_run_context_short_circuits_after_signing(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        _wire_for_happy_path(orchestrator, tx_count=1)

        ctx = ExecutionContext(strategy_id="t", dry_run=True)
        result = await orchestrator.execute(bundle, ctx)

        assert result.success is True
        assert result.phase == ExecutionPhase.COMPLETE
        # Submitter.submit must NOT have been called under dry_run
        orchestrator.submitter.submit.assert_not_called()


# =============================================================================
# Transaction reverted mid-pipeline (exception path)
# =============================================================================


class TestReceiptRevertedMidPipeline:
    @pytest.mark.asyncio
    async def test_transaction_reverted_error_raised_by_helper_mapped_to_confirmation(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        _wire_for_happy_path(orchestrator, tx_count=1)
        # Have get_receipts raise TransactionRevertedError (simulating a reverted path
        # that raises rather than returning a failed receipt).
        orchestrator.submitter.get_receipts = AsyncMock(
            side_effect=TransactionRevertedError(tx_hash="0xdead", revert_reason="oops")
        )

        with patch("almanak.framework.execution.orchestrator.build_verbose_revert_report") as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "FROM EXCEPTION"
            mock_report.to_dict.return_value = {}
            mock_build.return_value = mock_report

            result = await orchestrator.execute(bundle)

        assert result.success is False
        assert result.error_phase == ExecutionPhase.CONFIRMATION
        assert result.error == "FROM EXCEPTION"
