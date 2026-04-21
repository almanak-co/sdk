"""Extended unit tests for ExecutionOrchestrator phase helpers (Phase 3d).

Complements ``test_orchestrator_phases.py`` (Phase 3a) with additional
branch coverage for the pipeline helpers. Each test drills a specific
branch not exercised by the 3a suite: state-mutation invariants,
exception paths, and side-effect ordering.
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
    SimulationResult,
    SubmissionError,
    TransactionReceipt,
    TransactionRevertedError,
)


def _make_receipt(
    *,
    success: bool,
    tx_hash: str,
    gas_used: int = 21000,
    effective_gas_price: int = 1,
    block_number: int = 1,
    block_hash: str = "0xblock",
    logs: list | None = None,
) -> TransactionReceipt:
    """Build a TransactionReceipt with the conventional success->status mapping."""
    return TransactionReceipt(
        tx_hash=tx_hash,
        block_number=block_number,
        block_hash=block_hash,
        gas_used=gas_used,
        effective_gas_price=effective_gas_price,
        status=1 if success else 0,
        logs=logs or [],
    )


from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
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


def _install_capture(orchestrator):
    emitted: list[tuple] = []
    orig_emit = orchestrator._emit_event

    def capture(evt_type, ctx, details=None):
        emitted.append((evt_type, details or {}))
        orig_emit(evt_type, ctx, details)

    orchestrator._emit_event = capture
    return emitted


# =============================================================================
# _init_pipeline_state
# =============================================================================


class TestInitPipelineState:
    def test_none_context_generates_default(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[])
        state = orchestrator._init_pipeline_state(bundle, None)

        assert state.context.chain == "arbitrum"
        assert state.context.wallet_address == orchestrator.signer.address
        # Correlation id is non-empty (uuid generated in __post_init__)
        assert state.context.correlation_id
        # Default intent description populated
        assert state.context.intent_description
        assert state.result.success is False
        assert state.result.phase == ExecutionPhase.VALIDATION
        assert state.action_bundle is bundle

    def test_provided_context_missing_fields_populated(self, orchestrator):
        # wallet_address and chain empty -> fill from orchestrator defaults
        bundle = ActionBundle(intent_type="HOLD", transactions=[])
        context = ExecutionContext(strategy_id="my-strat")
        context.wallet_address = ""
        context.chain = ""

        state = orchestrator._init_pipeline_state(bundle, context)

        assert state.context.wallet_address == orchestrator.signer.address
        assert state.context.chain == "arbitrum"
        assert state.context.strategy_id == "my-strat"

    def test_provided_context_non_empty_fields_preserved(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[])
        context = ExecutionContext(
            strategy_id="foo",
            wallet_address="0xDEADBEEF",
            chain="base",
            intent_description="already set",
        )
        state = orchestrator._init_pipeline_state(bundle, context)

        assert state.context.wallet_address == "0xDEADBEEF"
        assert state.context.chain == "base"
        # existing non-empty intent_description preserved
        assert state.context.intent_description == "already set"

    def test_result_correlation_id_matches_context(self, orchestrator):
        bundle = ActionBundle(intent_type="SWAP", transactions=[])
        context = ExecutionContext(strategy_id="foo", correlation_id="fixed-corr-id")
        state = orchestrator._init_pipeline_state(bundle, context)

        assert state.result.correlation_id == "fixed-corr-id"


# =============================================================================
# _phase_build
# =============================================================================


class TestPhaseBuildExtended:
    @pytest.mark.asyncio
    async def test_build_propagates_gas_warnings_when_simulation_disabled(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        state.context.simulation_enabled = False
        orchestrator._check_token_balance_before_submit = AsyncMock()
        orchestrator._maybe_estimate_gas_limits = AsyncMock(return_value=([MagicMock()], ["gas warning msg"]))

        early = await orchestrator._phase_build(state)

        assert early is None
        assert state.result.gas_warnings == ["gas warning msg"]
        orchestrator._maybe_estimate_gas_limits.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_build_skips_gas_estimation_when_simulation_enabled(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        state.context.simulation_enabled = True
        orchestrator._check_token_balance_before_submit = AsyncMock()
        orchestrator._maybe_estimate_gas_limits = AsyncMock(return_value=([], []))

        await orchestrator._phase_build(state)

        orchestrator._maybe_estimate_gas_limits.assert_not_called()
        assert state.result.gas_warnings == []

    @pytest.mark.asyncio
    async def test_build_calls_token_balance_preflight(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        orchestrator._check_token_balance_before_submit = AsyncMock()

        await orchestrator._phase_build(state)

        orchestrator._check_token_balance_before_submit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_build_refreshes_deferred_bundle_first(self, orchestrator):
        """_phase_build must call refresh_deferred_bundle BEFORE building unsigned txs."""
        state = _make_state(orchestrator, transactions=[{"to": "0x00", "data": "0x", "value": 0}])
        orchestrator._check_token_balance_before_submit = AsyncMock()

        refreshed_bundle = ActionBundle(
            intent_type="SWAP",
            transactions=[{"to": "0x99", "data": "0x", "value": 0}],
        )
        with patch(
            "almanak.framework.execution.deferred_refresh.refresh_deferred_bundle",
            return_value=refreshed_bundle,
        ) as mock_refresh:
            early = await orchestrator._phase_build(state)

        assert early is None
        mock_refresh.assert_called_once()
        # The refreshed bundle is now on state
        assert state.action_bundle is refreshed_bundle
        # unsigned_txs was built from the refreshed bundle (single tx)
        assert state.unsigned_txs is not None
        assert len(state.unsigned_txs) == 1


# =============================================================================
# _handle_empty_bundle
# =============================================================================


class TestHandleEmptyBundle:
    def test_hold_emits_execution_success_event(self, orchestrator):
        state = _make_state(orchestrator, intent_type="HOLD", transactions=[])
        events = _install_capture(orchestrator)

        result = orchestrator._handle_empty_bundle(state)

        assert result.success is True
        assert result.completed_at is not None
        success = [t for t, _ in events if t == ExecutionEventType.EXECUTION_SUCCESS]
        assert success, "Expected EXECUTION_SUCCESS for HOLD empty bundle"

    def test_no_op_uses_metadata_reason(self, orchestrator):
        state = _make_state(
            orchestrator,
            intent_type="WITHDRAW",
            transactions=[],
            metadata={"no_op": True, "reason": "already closed"},
        )
        events = _install_capture(orchestrator)

        result = orchestrator._handle_empty_bundle(state)
        assert result.success is True
        details_msgs = [d.get("message") for _, d in events if d.get("message")]
        assert "already closed" in details_msgs

    def test_no_op_without_reason_falls_back_to_default(self, orchestrator):
        state = _make_state(
            orchestrator,
            intent_type="WITHDRAW",
            transactions=[],
            metadata={"no_op": True},  # no reason
        )
        events = _install_capture(orchestrator)
        orchestrator._handle_empty_bundle(state)
        details_msgs = [d.get("message") for _, d in events if d.get("message")]
        assert "No-op bundle" in details_msgs

    def test_non_hold_empty_emits_execution_failed(self, orchestrator):
        state = _make_state(orchestrator, intent_type="SWAP", transactions=[])
        events = _install_capture(orchestrator)

        result = orchestrator._handle_empty_bundle(state)
        assert result.success is False
        assert any(t == ExecutionEventType.EXECUTION_FAILED for t, _ in events)

    def test_lowercase_intent_type_normalized_to_uppercase(self, orchestrator):
        """`intent_type` is upper-cased before the HOLD check; 'hold' must short-circuit success."""
        state = _make_state(orchestrator, intent_type="hold", transactions=[])

        result = orchestrator._handle_empty_bundle(state)
        assert result.success is True
        assert result.phase == ExecutionPhase.COMPLETE


# =============================================================================
# _phase_simulate
# =============================================================================


class TestPhaseSimulateExtended:
    @pytest.mark.asyncio
    async def test_simulation_disabled_returns_none_without_calling_simulator(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.context.simulation_enabled = False
        state.unsigned_txs = []
        orchestrator.simulator.simulate = AsyncMock()

        early = await orchestrator._phase_simulate(state)

        assert early is None
        orchestrator.simulator.simulate.assert_not_called()

    @pytest.mark.asyncio
    async def test_simulation_failure_returns_simulation_failure_result(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.context.simulation_enabled = True
        state.unsigned_txs = []
        orchestrator.simulator.simulate = AsyncMock(
            return_value=SimulationResult(success=False, simulated=True, revert_reason="ERC20: insufficient")
        )

        early = await orchestrator._phase_simulate(state)

        assert early is not None
        assert early.success is False
        assert early.error_phase == ExecutionPhase.SIMULATION
        assert "ERC20: insufficient" in (early.error or "")

    @pytest.mark.asyncio
    async def test_simulation_success_attaches_simulation_result(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.context.simulation_enabled = True
        state.unsigned_txs = []
        sim_result = SimulationResult(success=True, simulated=True, gas_estimates=[])
        orchestrator.simulator.simulate = AsyncMock(return_value=sim_result)

        early = await orchestrator._phase_simulate(state)

        assert early is None
        assert state.result.simulation_result is sim_result
        assert state.result.phase == ExecutionPhase.SIMULATION

    @pytest.mark.asyncio
    async def test_simulation_failure_missing_revert_reason_uses_unknown(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.context.simulation_enabled = True
        state.unsigned_txs = []
        orchestrator.simulator.simulate = AsyncMock(
            return_value=SimulationResult(success=False, simulated=True, revert_reason=None)
        )
        # Suppress the SIMULATION_FAILED emit path (which trips on revert_reason=None,
        # see bugs-found-while-testing note in the 3d PR). This test only validates
        # that _phase_simulate itself builds the "Unknown reason" error message.
        orchestrator._emit_event = MagicMock()

        early = await orchestrator._phase_simulate(state)

        assert early is not None
        assert "Unknown reason" in (early.error or "")


# =============================================================================
# _phase_gas
# =============================================================================


class TestPhaseGasExtended:
    @pytest.mark.asyncio
    async def test_gas_pass_does_not_short_circuit_and_keeps_state(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.unsigned_txs = [MagicMock()]

        orchestrator._update_gas_prices = AsyncMock(return_value=[MagicMock(), MagicMock()])
        orchestrator._validate_gas_prices = MagicMock(return_value=RiskGuardResult(passed=True, violations=[]))

        early = await orchestrator._phase_gas(state)
        assert early is None
        # _update_gas_prices may mutate the list; unsigned_txs should now reflect the mock return
        assert state.unsigned_txs is not None and len(state.unsigned_txs) == 2

    @pytest.mark.asyncio
    async def test_gas_cap_violation_emits_risk_blocked_event(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.unsigned_txs = []

        orchestrator._update_gas_prices = AsyncMock(return_value=[])
        orchestrator._validate_gas_prices = MagicMock(
            return_value=RiskGuardResult(passed=False, violations=["too high"])
        )
        events = _install_capture(orchestrator)

        early = await orchestrator._phase_gas(state)
        assert early is not None
        assert any(t == ExecutionEventType.RISK_BLOCKED for t, _ in events)


# =============================================================================
# _phase_sign
# =============================================================================


class TestPhaseSignExtended:
    @pytest.mark.asyncio
    async def test_non_dry_run_populates_signed_txs_and_returns_none(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.context.dry_run = False
        state.unsigned_txs = [MagicMock(nonce=1), MagicMock(nonce=2)]

        orchestrator._assign_nonces = AsyncMock(return_value=state.unsigned_txs)
        signed_list = [MagicMock(), MagicMock()]
        orchestrator.signer.sign_batch = AsyncMock(return_value=signed_list)

        early = await orchestrator._phase_sign(state)

        assert early is None
        assert state.signed_txs is signed_list
        # phase advances to SUBMISSION for the downstream phase
        assert state.result.phase == ExecutionPhase.SUBMISSION

    @pytest.mark.asyncio
    async def test_sets_nonce_assignment_phase_before_signing(self, orchestrator):
        """Result.phase transitions through NONCE_ASSIGNMENT -> SIGNING -> SUBMISSION."""
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.context.dry_run = False

        phases_observed: list[ExecutionPhase] = []

        async def capture_phase_during_assign(*_args, **_kwargs):
            phases_observed.append(state.result.phase)
            return state.unsigned_txs or []

        state.unsigned_txs = [MagicMock(nonce=1)]
        orchestrator._assign_nonces = capture_phase_during_assign
        orchestrator.signer.sign_batch = AsyncMock(return_value=[MagicMock()])

        await orchestrator._phase_sign(state)

        assert phases_observed == [ExecutionPhase.NONCE_ASSIGNMENT]

    @pytest.mark.asyncio
    async def test_dry_run_does_not_submit(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.context.dry_run = True
        state.unsigned_txs = [MagicMock()]
        orchestrator._assign_nonces = AsyncMock(return_value=state.unsigned_txs)
        orchestrator.signer.sign_batch = AsyncMock(return_value=[MagicMock()])
        orchestrator.submitter.submit = AsyncMock()

        early = await orchestrator._phase_sign(state)
        assert early is not None and early.success is True
        # Submit path was never reached (it lives in _phase_submit_and_confirm)
        orchestrator.submitter.submit.assert_not_called()


# =============================================================================
# _phase_submit_and_confirm
# =============================================================================


class TestPhaseSubmitAndConfirmExtended:
    @pytest.mark.asyncio
    async def test_single_tx_uses_parallel_path(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.signed_txs = [MagicMock()]

        submission = MagicMock(submitted=True, tx_hash="0xabc", error=None)
        orchestrator.submitter.submit = AsyncMock(return_value=[submission])
        orchestrator.submitter.get_receipts = AsyncMock(return_value=[_make_receipt(success=True, tx_hash="0xabc")])

        early = await orchestrator._phase_submit_and_confirm(state)
        assert early is None
        assert state.use_sequential is False
        # Parallel path pulls receipts via get_receipts
        orchestrator.submitter.get_receipts.assert_awaited_once()
        assert state.receipts is not None and state.receipts[0].success

    @pytest.mark.asyncio
    async def test_single_tx_submission_failure_short_circuits(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.signed_txs = [MagicMock()]

        submission = MagicMock(submitted=False, tx_hash=None, error="nonce too low")
        orchestrator.submitter.submit = AsyncMock(return_value=[submission])

        early = await orchestrator._phase_submit_and_confirm(state)
        assert early is not None
        assert early.success is False
        assert early.error_phase == ExecutionPhase.SUBMISSION
        assert "nonce too low" in (early.error or "")

    @pytest.mark.asyncio
    async def test_multi_tx_with_public_submitter_uses_sequential(self, orchestrator):
        """2+ txs with non-Safe signer and PublicMempoolSubmitter take the sequential path."""
        from almanak.framework.execution.submitter.public import PublicMempoolSubmitter

        state = _make_state(
            orchestrator,
            transactions=[
                {"to": "0x0", "data": "0x", "value": 0},
                {"to": "0x0", "data": "0x", "value": 0},
            ],
        )
        state.signed_txs = [MagicMock(), MagicMock()]

        receipts = [
            _make_receipt(success=True, tx_hash="0xa"),
            _make_receipt(success=True, tx_hash="0xb"),
        ]
        submissions = [
            MagicMock(submitted=True, tx_hash="0xa", error=None),
            MagicMock(submitted=True, tx_hash="0xb", error=None),
        ]

        # Build a real PublicMempoolSubmitter-typed mock so the isinstance check passes
        fake_submitter = MagicMock(spec=PublicMempoolSubmitter)
        fake_submitter.submit_sequential = AsyncMock(return_value=(submissions, receipts))
        orchestrator.submitter = fake_submitter

        early = await orchestrator._phase_submit_and_confirm(state)

        assert early is None
        assert state.use_sequential is True
        assert state.receipts == receipts
        fake_submitter.submit_sequential.assert_awaited_once()


# =============================================================================
# _phase_enrich
# =============================================================================


class TestPhaseEnrich:
    @pytest.mark.asyncio
    async def test_all_receipts_success_sets_success_and_updates_nonce(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.receipts = [
            _make_receipt(
                success=True,
                tx_hash="0xabc",
                gas_used=21000,
                effective_gas_price=5,  # gas_cost_wei will be 105000
                block_number=123,
            )
        ]

        # Mock the web3 nonce bump
        mock_web3 = MagicMock()
        mock_web3.eth.get_transaction_count = AsyncMock(return_value=42)
        mock_web3.to_checksum_address = lambda x: x
        orchestrator._get_web3 = AsyncMock(return_value=mock_web3)

        early = await orchestrator._phase_enrich(state)

        assert early is not None
        assert early.success is True
        assert early.phase == ExecutionPhase.COMPLETE
        assert early.completed_at is not None
        assert early.total_gas_used == 21000
        assert early.total_gas_cost_wei == 21000 * 5
        assert len(early.transaction_results) == 1
        # Local nonce cache was bumped
        wallet_key = state.context.wallet_address.lower()
        assert orchestrator._local_nonce[wallet_key] == 42

    @pytest.mark.asyncio
    async def test_reverted_receipt_returns_failure_with_verbose_report(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.receipts = [
            _make_receipt(
                success=False,
                tx_hash="0xdead",
                gas_used=50000,
                effective_gas_price=4,
                block_number=456,
            )
        ]

        events = _install_capture(orchestrator)

        with patch("almanak.framework.execution.orchestrator.build_verbose_revert_report") as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "VERBOSE REVERT"
            mock_report.to_dict.return_value = {"ok": True}
            mock_build.return_value = mock_report

            early = await orchestrator._phase_enrich(state)

        assert early is not None
        assert early.success is False
        assert early.error_phase == ExecutionPhase.CONFIRMATION
        assert early.error == "VERBOSE REVERT"
        assert any(t == ExecutionEventType.EXECUTION_FAILED for t, _ in events)
        # Also emitted TX_REVERTED for the single reverted tx
        assert any(t == ExecutionEventType.TX_REVERTED for t, _ in events)

    @pytest.mark.asyncio
    async def test_mixed_success_and_revert_returns_failure(self, orchestrator):
        """Any reverted tx in the batch should fail the whole ExecutionResult."""
        state = _make_state(
            orchestrator,
            transactions=[
                {"to": "0x0", "data": "0x", "value": 0},
                {"to": "0x0", "data": "0x", "value": 0},
            ],
        )
        state.receipts = [
            _make_receipt(success=True, tx_hash="0xok"),
            _make_receipt(success=False, tx_hash="0xbad", gas_used=50000),
        ]

        with patch("almanak.framework.execution.orchestrator.build_verbose_revert_report") as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "REVERT REPORT"
            mock_report.to_dict.return_value = {}
            mock_build.return_value = mock_report

            early = await orchestrator._phase_enrich(state)

        assert early is not None
        assert early.success is False
        # Both tx results recorded, aggregate gas_used sums both
        assert len(early.transaction_results) == 2
        assert early.total_gas_used == 71000

    @pytest.mark.asyncio
    async def test_reverted_receipt_error_propagates_to_tx_result_and_report(self, orchestrator):
        """Regression for #1659: receipt.error must be copied onto TransactionResult.error
        and forwarded to build_verbose_revert_report(raw_error=...).
        """
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        receipt = _make_receipt(
            success=False,
            tx_hash="0xdead",
            gas_used=50000,
            effective_gas_price=4,
            block_number=456,
        )
        # Submitter implementations may attach a `.error` attribute carrying the
        # decoded revert reason. Simulate that here.
        receipt.error = "SomeRevertReason"
        state.receipts = [receipt]

        with patch("almanak.framework.execution.orchestrator.build_verbose_revert_report") as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "VERBOSE REVERT"
            mock_report.to_dict.return_value = {}
            mock_build.return_value = mock_report

            early = await orchestrator._phase_enrich(state)

        assert early is not None
        assert early.success is False
        # The receipt error must now live on the TransactionResult (was None pre-fix).
        assert len(early.transaction_results) == 1
        assert early.transaction_results[0].error == "SomeRevertReason"
        # And it must have been forwarded to the verbose report builder.
        _, kwargs = mock_build.call_args
        assert kwargs["raw_error"] == "SomeRevertReason"

    @pytest.mark.asyncio
    async def test_reverted_receipt_raw_error_fallback_propagates(self, orchestrator):
        """Regression for #1659: when a receipt exposes `raw_error` (not `error`),
        it must still be copied onto TransactionResult.error.
        """
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        receipt = _make_receipt(success=False, tx_hash="0xdeadbeef", gas_used=50000)
        receipt.raw_error = "RawRevertReason"
        state.receipts = [receipt]

        with patch("almanak.framework.execution.orchestrator.build_verbose_revert_report") as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "VERBOSE REVERT"
            mock_report.to_dict.return_value = {}
            mock_build.return_value = mock_report

            early = await orchestrator._phase_enrich(state)

        assert early is not None
        assert early.transaction_results[0].error == "RawRevertReason"
        _, kwargs = mock_build.call_args
        assert kwargs["raw_error"] == "RawRevertReason"


# =============================================================================
# _handle_execution_exception - extended
# =============================================================================


class TestHandleExecutionExceptionExtended:
    def test_nonce_error_when_result_phase_is_signing_still_uses_nonce_assignment(self, orchestrator):
        """NonceError ALWAYS sets error_phase=NONCE_ASSIGNMENT, regardless of current phase."""
        state = _make_state(orchestrator)
        state.result.phase = ExecutionPhase.SIGNING  # pretend phase advanced

        out = orchestrator._handle_execution_exception(state, NonceError("replacement underpriced"))

        assert out.error_phase == ExecutionPhase.NONCE_ASSIGNMENT

    def test_submission_error_without_session_still_records_error_phase(self, orchestrator):
        """No session + no transaction_results -> just error_phase + message; no partials added."""
        state = _make_state(orchestrator)
        state.session = None
        state.result.transaction_results = []  # no partials

        out = orchestrator._handle_execution_exception(state, SubmissionError("timeout"))

        assert out.error_phase == ExecutionPhase.SUBMISSION
        assert "timeout" in (out.error or "")
        # No partials materialized
        assert out.transaction_results == []

    def test_submission_error_with_session_and_existing_tx_results_skips_partial(self, orchestrator):
        """If result.transaction_results is already populated, partial extraction is skipped."""
        state = _make_state(orchestrator)
        # Preexisting tx result -> must not be overwritten
        state.result.transaction_results = [TransactionResult(tx_hash="0xpre", success=False, error="prev error")]

        session = MagicMock()
        tx_state = MagicMock()
        tx_state.tx_hash = "0xignore"
        session.transactions = [tx_state]
        state.session = session
        orchestrator._complete_session = MagicMock()

        out = orchestrator._handle_execution_exception(state, SubmissionError("timeout"))

        # Partial tx was NOT added because transaction_results was already populated
        assert len(out.transaction_results) == 1
        assert out.transaction_results[0].tx_hash == "0xpre"

    def test_transaction_reverted_error_includes_verbose_report_dict_in_event(self, orchestrator):
        state = _make_state(orchestrator)
        events = _install_capture(orchestrator)

        with patch("almanak.framework.execution.orchestrator.build_verbose_revert_report") as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "formatted"
            mock_report.to_dict.return_value = {"details": "x"}
            mock_build.return_value = mock_report

            orchestrator._handle_execution_exception(
                state,
                TransactionRevertedError(tx_hash="0xabc", revert_reason="custom"),
            )

        # TX_REVERTED event must carry the verbose_report dict
        reverted = [d for t, d in events if t == ExecutionEventType.TX_REVERTED]
        assert reverted
        assert reverted[0].get("verbose_report") == {"details": "x"}
        assert reverted[0].get("revert_reason") == "custom"
        assert reverted[0].get("tx_hash") == "0xabc"

    def test_insufficient_funds_when_phase_is_complete_preserves_complete(self, orchestrator):
        """error_phase mirrors result.phase, even if phase already advanced to COMPLETE."""
        state = _make_state(orchestrator)
        state.result.phase = ExecutionPhase.COMPLETE

        out = orchestrator._handle_execution_exception(state, InsufficientFundsError(required=100, available=50))
        assert out.error_phase == ExecutionPhase.COMPLETE

    def test_gas_estimation_error_message_preserved(self, orchestrator):
        state = _make_state(orchestrator)
        state.result.phase = ExecutionPhase.SIMULATION

        out = orchestrator._handle_execution_exception(state, GasEstimationError("reverted on estimate"))
        assert out.error_phase == ExecutionPhase.SIMULATION
        assert "reverted on estimate" in (out.error or "")

    def test_execution_error_custom_subclass_still_mapped(self, orchestrator):
        """A subclass of ExecutionError that isn't one of the explicit types hits the fallback."""

        class MyWeirdExecError(ExecutionError):
            pass

        state = _make_state(orchestrator)
        state.result.phase = ExecutionPhase.VALIDATION

        out = orchestrator._handle_execution_exception(state, MyWeirdExecError("oops"))
        assert out.error_phase == ExecutionPhase.VALIDATION
        assert "oops" in (out.error or "")

    def test_generic_exception_message_prefixed_with_unexpected(self, orchestrator):
        state = _make_state(orchestrator)
        out = orchestrator._handle_execution_exception(state, ValueError("bad input"))
        assert (out.error or "").startswith("Unexpected error:")

    def test_submission_error_partial_results_skipped_when_session_has_no_hashes(self, orchestrator):
        """Session exists but tx_states have no tx_hash -> nothing synthesized."""
        state = _make_state(orchestrator)
        session = MagicMock()
        tx_state = MagicMock()
        tx_state.tx_hash = None
        session.transactions = [tx_state]
        state.session = session
        orchestrator._complete_session = MagicMock()

        out = orchestrator._handle_execution_exception(state, SubmissionError("timeout"))
        assert out.transaction_results == []


# =============================================================================
# Pipeline state invariants
# =============================================================================


class TestPipelineStateInvariants:
    @pytest.mark.asyncio
    async def test_phase_build_mutates_only_expected_fields(self, orchestrator):
        """_phase_build owns action_bundle (refresh) + unsigned_txs; doesn't touch signed/receipts."""
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.signed_txs = None
        state.receipts = None
        orchestrator._check_token_balance_before_submit = AsyncMock()

        await orchestrator._phase_build(state)

        assert state.unsigned_txs is not None
        assert state.signed_txs is None  # unchanged
        assert state.receipts is None  # unchanged

    @pytest.mark.asyncio
    async def test_phase_sign_does_not_set_receipts_or_submission_results(self, orchestrator):
        state = _make_state(orchestrator, transactions=[{"to": "0x0", "data": "0x", "value": 0}])
        state.context.dry_run = False
        state.unsigned_txs = [MagicMock(nonce=1)]
        orchestrator._assign_nonces = AsyncMock(return_value=state.unsigned_txs)
        orchestrator.signer.sign_batch = AsyncMock(return_value=[MagicMock()])

        await orchestrator._phase_sign(state)

        assert state.signed_txs is not None
        assert state.receipts is None
        assert state.submission_results is None
