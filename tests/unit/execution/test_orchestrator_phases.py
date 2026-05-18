"""Unit tests for ExecutionOrchestrator phase helpers (Phase 3a).

Tests each extracted phase helper (`_phase_build`, `_phase_validate`, ...)
and the consolidated `_handle_execution_exception` in isolation. These
complement the existing end-to-end tests and guard the refactored
pipeline against behavioural drift.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution._pipeline_state import ExecutionPipelineState
from almanak.framework.execution.events import ExecutionEventType
from almanak.framework.execution.interfaces import (
    ExecutionError,
    GasEstimationError,
    InsufficientFundsError,
    NonceError,
    SubmissionError,
    TransactionRevertedError,
)
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.framework.strategies.base import RiskGuardResult


# =============================================================================
# Fixtures and helpers
# =============================================================================


@pytest.fixture
def orchestrator():
    """Create an orchestrator with mocked signer/submitter/simulator."""
    signer = MagicMock()
    signer.address = "0x1234567890abcdef1234567890abcdef12345678"
    submitter = MagicMock()
    simulator = MagicMock()
    return ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain="arbitrum",
    )


def _make_state(
    orchestrator: ExecutionOrchestrator,
    *,
    intent_type: str = "SWAP",
    transactions: list | None = None,
    metadata: dict | None = None,
) -> ExecutionPipelineState:
    """Build a minimal pipeline state targeted at the phase under test."""
    bundle = ActionBundle(
        intent_type=intent_type,
        transactions=transactions or [],
        metadata=metadata or {},
    )
    context = ExecutionContext(
        strategy_id="test",
        intent_id="test-intent",
        chain="arbitrum",
        wallet_address=orchestrator.signer.address,
    )
    result = ExecutionResult(
        success=False,
        phase=ExecutionPhase.VALIDATION,
        correlation_id=context.correlation_id,
    )
    return ExecutionPipelineState(
        action_bundle=bundle,
        context=context,
        result=result,
    )


# =============================================================================
# _phase_build
# =============================================================================


class TestPhaseBuild:
    @pytest.mark.asyncio
    async def test_happy_path_populates_unsigned_txs_and_returns_none(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])

        # Stub downstream helpers
        orchestrator._check_token_balance_before_submit = AsyncMock()

        early = await orchestrator._phase_build(state)

        assert early is None
        assert state.unsigned_txs is not None and len(state.unsigned_txs) == 1

    @pytest.mark.asyncio
    async def test_hold_empty_bundle_short_circuits_success(self, orchestrator):
        state = _make_state(orchestrator, intent_type="HOLD", transactions=[])

        early = await orchestrator._phase_build(state)

        assert early is not None
        assert early.success is True
        assert early.phase == ExecutionPhase.COMPLETE
        assert early.error is None

    @pytest.mark.asyncio
    async def test_no_op_metadata_short_circuits_success(self, orchestrator):
        state = _make_state(
            orchestrator,
            intent_type="WITHDRAW",
            transactions=[],
            metadata={"no_op": True, "reason": "nothing to withdraw"},
        )

        early = await orchestrator._phase_build(state)

        assert early is not None
        assert early.success is True
        assert early.phase == ExecutionPhase.COMPLETE
        assert early.error is None

    @pytest.mark.asyncio
    async def test_non_hold_empty_bundle_short_circuits_failure(self, orchestrator):
        state = _make_state(orchestrator, intent_type="SWAP", transactions=[])

        early = await orchestrator._phase_build(state)

        assert early is not None
        assert early.success is False
        assert early.error is not None
        assert "Empty ActionBundle" in early.error
        assert "SWAP" in early.error
        assert early.phase == ExecutionPhase.COMPLETE
        assert early.error_phase == ExecutionPhase.COMPLETE


# =============================================================================
# _phase_validate
# =============================================================================


class TestPhaseValidate:
    @pytest.mark.asyncio
    async def test_risk_guard_pass_returns_none(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        state.unsigned_txs = []  # RiskGuard only reads length

        orchestrator._validate_transactions = AsyncMock(
            return_value=RiskGuardResult(passed=True, violations=[])
        )
        orchestrator._preflight_balance_check = AsyncMock(return_value=None)

        early = await orchestrator._phase_validate(state)

        assert early is None

    @pytest.mark.asyncio
    async def test_risk_guard_block_returns_validation_failure(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        state.unsigned_txs = []

        orchestrator._validate_transactions = AsyncMock(
            return_value=RiskGuardResult(passed=False, violations=["too big"])
        )

        early = await orchestrator._phase_validate(state)

        assert early is not None
        assert early.success is False
        assert early.error_phase == ExecutionPhase.VALIDATION
        assert early.error is not None
        assert early.error.startswith("RiskGuard blocked:")
        assert "too big" in early.error

    @pytest.mark.asyncio
    async def test_preflight_balance_failure_returns_validation_failure(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        state.unsigned_txs = []

        orchestrator._validate_transactions = AsyncMock(
            return_value=RiskGuardResult(passed=True, violations=[])
        )
        orchestrator._preflight_balance_check = AsyncMock(return_value="Insufficient USDC")

        early = await orchestrator._phase_validate(state)

        assert early is not None
        assert early.success is False
        assert early.error_phase == ExecutionPhase.VALIDATION
        assert early.error == "Insufficient USDC"


# =============================================================================
# _phase_gas
# =============================================================================


class TestPhaseGas:
    @pytest.mark.asyncio
    async def test_gas_price_cap_violation_returns_validation_failure(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        state.unsigned_txs = []

        orchestrator._update_gas_prices = AsyncMock(return_value=[])
        # Force the cap check to fail.
        orchestrator._validate_gas_prices = MagicMock(
            return_value=RiskGuardResult(passed=False, violations=["Transaction 0: 50 gwei > 10 gwei"])
        )

        early = await orchestrator._phase_gas(state)

        assert early is not None
        assert early.success is False
        assert early.error_phase == ExecutionPhase.VALIDATION
        assert early.error is not None
        assert early.error.startswith("Gas price cap exceeded:")

    @pytest.mark.asyncio
    async def test_gas_price_cap_pass_returns_none(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        state.unsigned_txs = []

        orchestrator._update_gas_prices = AsyncMock(return_value=[])
        orchestrator._validate_gas_prices = MagicMock(
            return_value=RiskGuardResult(passed=True, violations=[])
        )

        early = await orchestrator._phase_gas(state)

        assert early is None


# =============================================================================
# _phase_sign
# =============================================================================


class TestPhaseSign:
    @pytest.mark.asyncio
    async def test_dry_run_short_circuits_success_and_emits_event(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        state.context.dry_run = True
        state.unsigned_txs = []

        # Sign and nonce helpers are stubbed to no-op
        orchestrator._assign_nonces = AsyncMock(return_value=[])
        orchestrator.signer.sign_batch = AsyncMock(return_value=[])

        emitted: list[tuple] = []
        orig_emit = orchestrator._emit_event

        def capture(evt_type, ctx, details=None):
            emitted.append((evt_type, details or {}))
            orig_emit(evt_type, ctx, details)

        orchestrator._emit_event = capture

        early = await orchestrator._phase_sign(state)

        assert early is not None
        assert early.success is True
        assert early.phase == ExecutionPhase.COMPLETE
        assert early.completed_at is not None

        # The SIGNING event + the EXECUTION_SUCCESS "Dry run completed" event
        # must both have been emitted.
        success_events = [
            (t, d) for t, d in emitted if t == ExecutionEventType.EXECUTION_SUCCESS
        ]
        assert success_events, "Expected EXECUTION_SUCCESS to be emitted on dry run"
        assert any(d.get("message") == "Dry run completed" for _t, d in success_events)


# =============================================================================
# _handle_execution_exception
# =============================================================================


class TestHandleExecutionException:
    def _install_capture(self, orchestrator):
        emitted: list[tuple] = []
        orig_emit = orchestrator._emit_event

        def capture(evt_type, ctx, details=None):
            emitted.append((evt_type, details or {}))
            orig_emit(evt_type, ctx, details)

        orchestrator._emit_event = capture
        return emitted

    def test_nonce_error_sets_nonce_phase_and_execution_failed(self, orchestrator):
        state = _make_state(orchestrator)
        events = self._install_capture(orchestrator)

        out = orchestrator._handle_execution_exception(state, NonceError("bad nonce"))

        assert out is state.result
        assert out.error_phase == ExecutionPhase.NONCE_ASSIGNMENT
        assert out.error is not None
        assert "bad nonce" in out.error
        assert any(t == ExecutionEventType.EXECUTION_FAILED for t, _ in events)
        assert any(d.get("error_type") == "NonceError" for _, d in events if "error_type" in d)

    def test_insufficient_funds_preserves_current_phase(self, orchestrator):
        state = _make_state(orchestrator)
        state.result.phase = ExecutionPhase.SIMULATION
        events = self._install_capture(orchestrator)

        out = orchestrator._handle_execution_exception(
            state, InsufficientFundsError(required=100, available=50)
        )

        assert out.error_phase == ExecutionPhase.SIMULATION
        assert any(d.get("error_type") == "InsufficientFundsError" for _, d in events if "error_type" in d)

    def test_gas_estimation_error_preserves_current_phase(self, orchestrator):
        state = _make_state(orchestrator)
        state.result.phase = ExecutionPhase.VALIDATION
        events = self._install_capture(orchestrator)

        out = orchestrator._handle_execution_exception(state, GasEstimationError("estim fail"))

        assert out.error_phase == ExecutionPhase.VALIDATION
        assert any(d.get("error_type") == "GasEstimationError" for _, d in events if "error_type" in d)

    def test_transaction_reverted_emits_tx_reverted_event(self, orchestrator):
        state = _make_state(orchestrator)
        events = self._install_capture(orchestrator)

        exc = TransactionRevertedError(tx_hash="0xabc", revert_reason="SafeMath sub")
        with patch(
            "almanak.framework.execution.orchestrator.build_verbose_revert_report"
        ) as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "verbose report text"
            mock_report.to_dict.return_value = {"report": "ok"}
            mock_build.return_value = mock_report

            out = orchestrator._handle_execution_exception(state, exc)

        assert out.error_phase == ExecutionPhase.CONFIRMATION
        assert out.error == "verbose report text"
        assert any(t == ExecutionEventType.TX_REVERTED for t, _ in events)

    def test_transaction_reverted_records_tx_hash_for_ledger(self, orchestrator):
        """VIB-4581 / F1.A regression — exception-path revert must populate
        ``transaction_results`` so the ledger writer persists the mined-tx hash.
        """
        state = _make_state(orchestrator)

        exc = TransactionRevertedError(
            tx_hash="0xdeadbeef",
            revert_reason="TRANSFER_FROM_FAILED",
            gas_used=120_000,
            block_number=42,
        )
        with patch(
            "almanak.framework.execution.orchestrator.build_verbose_revert_report"
        ) as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "verbose report text"
            mock_report.to_dict.return_value = {"report": "ok"}
            mock_build.return_value = mock_report

            out = orchestrator._handle_execution_exception(state, exc)

        assert len(out.transaction_results) == 1
        tr = out.transaction_results[0]
        assert tr.tx_hash == "0xdeadbeef"
        assert tr.success is False
        assert tr.gas_used == 120_000
        assert tr.error == "TRANSFER_FROM_FAILED"
        assert out.total_gas_used == 120_000

    def test_transaction_reverted_does_not_duplicate_existing_tx_hash(self, orchestrator):
        """Idempotency — if ``_phase_enrich`` already appended the receipt
        before the exception path runs (defensive: belt-and-braces against
        future call-order changes), we must not record the same hash twice.
        """
        state = _make_state(orchestrator)
        # Simulate a TransactionResult already on the result from _phase_enrich.
        from almanak.framework.execution.orchestrator import TransactionResult

        state.result.transaction_results.append(
            TransactionResult(tx_hash="0xdeadbeef", success=False, gas_used=80_000)
        )
        state.result.total_gas_used = 80_000

        exc = TransactionRevertedError(
            tx_hash="0xdeadbeef",
            revert_reason="reverted",
            gas_used=80_000,
        )
        with patch(
            "almanak.framework.execution.orchestrator.build_verbose_revert_report"
        ) as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "verbose report text"
            mock_report.to_dict.return_value = {"report": "ok"}
            mock_build.return_value = mock_report

            out = orchestrator._handle_execution_exception(state, exc)

        # Exactly one entry, no double-counted gas.
        assert len(out.transaction_results) == 1
        assert out.total_gas_used == 80_000

    def test_submission_error_preserves_partial_tx_hashes_from_session(self, orchestrator):
        state = _make_state(orchestrator)
        # Session with a partially-submitted tx_hash
        session = MagicMock()
        tx_state = MagicMock()
        tx_state.tx_hash = "0xpartial"
        session.transactions = [tx_state]
        state.session = session
        # Ensure _complete_session no-ops (no real store wired)
        orchestrator._complete_session = MagicMock()

        events = self._install_capture(orchestrator)

        out = orchestrator._handle_execution_exception(state, SubmissionError("timeout"))

        assert out.error_phase == ExecutionPhase.SUBMISSION
        assert len(out.transaction_results) == 1
        assert out.transaction_results[0].tx_hash == "0xpartial"
        assert out.transaction_results[0].success is False
        assert out.transaction_results[0].error == "timeout_waiting_for_receipt"
        assert any(t == ExecutionEventType.EXECUTION_FAILED for t, _ in events)

    def test_execution_error_preserves_current_phase(self, orchestrator):
        state = _make_state(orchestrator)
        state.result.phase = ExecutionPhase.SUBMISSION
        events = self._install_capture(orchestrator)

        out = orchestrator._handle_execution_exception(state, ExecutionError("internal boom"))

        assert out.error_phase == ExecutionPhase.SUBMISSION
        assert any(d.get("error_type") == "ExecutionError" for _, d in events if "error_type" in d)

    def test_generic_exception_falls_through_to_unexpected_error(self, orchestrator):
        state = _make_state(orchestrator)
        state.result.phase = ExecutionPhase.SIGNING
        events = self._install_capture(orchestrator)

        out = orchestrator._handle_execution_exception(state, RuntimeError("kaboom"))

        assert out.error_phase == ExecutionPhase.SIGNING
        assert out.error is not None
        assert out.error.startswith("Unexpected error:")
        assert any(d.get("error_type") == "RuntimeError" for _, d in events if "error_type" in d)
