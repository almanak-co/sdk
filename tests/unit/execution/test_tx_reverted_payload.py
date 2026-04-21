"""Regression tests for the canonical ``TX_REVERTED`` payload schema (#1664).

``ExecutionOrchestrator`` emits ``ExecutionEventType.TX_REVERTED`` from two
sites with historically inconsistent payload shapes:

1. ``_phase_enrich`` (receipt-level revert) - full confirmed receipt.
2. ``_handle_execution_exception`` on ``TransactionRevertedError`` - no
   confirmed receipt yet.

These tests lock in the behaviour that BOTH sites produce payloads compliant
with ``TxRevertedPayload``: every required key is present, and fields
unavailable in a given path are ``None`` rather than missing.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution._pipeline_state import ExecutionPipelineState
from almanak.framework.execution.events import (
    TX_REVERTED_REQUIRED_KEYS,
    ExecutionEventType,
    TxRevertedPayload,
    build_tx_reverted_payload,
)
from almanak.framework.execution.interfaces import (
    TransactionReceipt,
    TransactionRevertedError,
)
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.models.reproduction_bundle import ActionBundle


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def orchestrator() -> ExecutionOrchestrator:
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


def _make_state(orchestrator: ExecutionOrchestrator) -> ExecutionPipelineState:
    bundle = ActionBundle(intent_type="SWAP", transactions=[], metadata={})
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


def _install_emit_capture(orchestrator: ExecutionOrchestrator) -> list[tuple]:
    emitted: list[tuple] = []
    orig_emit = orchestrator._emit_event

    def capture(evt_type, ctx, details=None):
        emitted.append((evt_type, dict(details or {})))
        orig_emit(evt_type, ctx, details)

    orchestrator._emit_event = capture  # type: ignore[method-assign]
    return emitted


# =============================================================================
# Schema helper
# =============================================================================


class TestBuildTxRevertedPayload:
    def test_all_required_keys_present_even_when_all_none(self) -> None:
        payload = build_tx_reverted_payload()
        for key in TX_REVERTED_REQUIRED_KEYS:
            assert key in payload, f"missing required key {key!r} on empty payload"
            assert payload[key] is None  # type: ignore[literal-required]

    def test_keys_populated_when_supplied(self) -> None:
        payload = build_tx_reverted_payload(
            tx_hash="0xabc",
            block_number=12345,
            gas_used=42000,
            revert_reason="ERC20: transfer amount exceeds balance",
            error="exec reverted",
            verbose_report={"k": "v"},
        )
        assert payload["tx_hash"] == "0xabc"
        assert payload["block_number"] == 12345
        assert payload["gas_used"] == 42000
        assert payload["revert_reason"] == "ERC20: transfer amount exceeds balance"
        assert payload["error"] == "exec reverted"
        assert payload["verbose_report"] == {"k": "v"}


# =============================================================================
# Receipt-level revert: _phase_enrich
# =============================================================================


def _make_receipt(
    *,
    tx_hash: str = "0xreceipt",
    block_number: int = 99,
    gas_used: int = 21000,
    effective_gas_price: int = 10**9,
    status: int = 0,
) -> TransactionReceipt:
    return TransactionReceipt(
        tx_hash=tx_hash,
        block_number=block_number,
        block_hash="0xblock",
        gas_used=gas_used,
        effective_gas_price=effective_gas_price,
        status=status,
    )


class TestReceiptLevelRevertPayload:
    @pytest.mark.asyncio
    async def test_receipt_revert_emits_full_schema(self, orchestrator) -> None:
        state = _make_state(orchestrator)
        state.receipts = [_make_receipt()]
        events = _install_emit_capture(orchestrator)

        # Stub out verbose revert report build to keep the test focused on
        # the emitted payload shape.
        with patch(
            "almanak.framework.execution.orchestrator.build_verbose_revert_report"
        ) as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "verbose report text"
            mock_report.to_dict.return_value = {"report": "ok"}
            mock_build.return_value = mock_report

            orchestrator._complete_session = MagicMock()
            await orchestrator._phase_enrich(state)

        tx_reverted_events = [d for t, d in events if t == ExecutionEventType.TX_REVERTED]
        assert len(tx_reverted_events) == 1
        payload = tx_reverted_events[0]

        # Every required key present.
        for key in TX_REVERTED_REQUIRED_KEYS:
            assert key in payload, f"receipt-path missing {key!r}"

        # Receipt path has concrete values for these three, None for the
        # decoded/verbose fields.
        assert payload["tx_hash"] == "0xreceipt"
        assert payload["block_number"] == 99
        assert payload["gas_used"] == 21000
        assert payload["revert_reason"] is None
        assert payload["verbose_report"] is None
        # ``error`` must be a non-null string at this site: ``tx_result.error``
        # is unset in the receipt loop so the emit site falls back to a
        # human-readable sentinel, which keeps downstream consumers using
        # ``payload.get("error", <default>)`` from surfacing a literal ``None``.
        assert isinstance(payload["error"], str) and payload["error"] != ""


# =============================================================================
# Exception-path revert: _handle_execution_exception
# =============================================================================


class TestExceptionPathRevertPayload:
    def test_exception_revert_with_only_tx_hash_and_reason_fills_nones(
        self, orchestrator
    ) -> None:
        state = _make_state(orchestrator)
        events = _install_emit_capture(orchestrator)
        exc = TransactionRevertedError(
            tx_hash="0xexc",
            revert_reason="SafeMath sub",
        )

        with patch(
            "almanak.framework.execution.orchestrator.build_verbose_revert_report"
        ) as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "verbose report text"
            mock_report.to_dict.return_value = {"report": "ok"}
            mock_build.return_value = mock_report
            orchestrator._complete_session = MagicMock()

            orchestrator._handle_execution_exception(state, exc)

        tx_reverted_events = [d for t, d in events if t == ExecutionEventType.TX_REVERTED]
        assert len(tx_reverted_events) == 1
        payload = tx_reverted_events[0]

        # Every required key present even when exception carries no block/gas.
        for key in TX_REVERTED_REQUIRED_KEYS:
            assert key in payload, f"exception-path missing {key!r}"

        assert payload["tx_hash"] == "0xexc"
        assert payload["revert_reason"] == "SafeMath sub"
        assert payload["verbose_report"] == {"report": "ok"}
        # block_number / gas_used unavailable at exception time; must be None,
        # NOT missing.
        assert payload["block_number"] is None
        assert payload["gas_used"] is None
        # error surfaces str(exc) for debuggability.
        assert payload["error"] is not None and "0xexc" in payload["error"]

    def test_exception_revert_with_full_metadata_populates_all_keys(
        self, orchestrator
    ) -> None:
        state = _make_state(orchestrator)
        events = _install_emit_capture(orchestrator)
        exc = TransactionRevertedError(
            tx_hash="0xexcfull",
            revert_reason="custom",
            gas_used=55555,
            block_number=77,
        )

        with patch(
            "almanak.framework.execution.orchestrator.build_verbose_revert_report"
        ) as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "verbose report text"
            mock_report.to_dict.return_value = {"report": "ok"}
            mock_build.return_value = mock_report
            orchestrator._complete_session = MagicMock()

            orchestrator._handle_execution_exception(state, exc)

        payload = next(
            d for t, d in events if t == ExecutionEventType.TX_REVERTED
        )
        assert payload["block_number"] == 77
        assert payload["gas_used"] == 55555


# =============================================================================
# Both paths produce the same key-set (superset invariant)
# =============================================================================


class TestBothPathsAgreeOnKeys:
    @pytest.mark.asyncio
    async def test_receipt_and_exception_payloads_share_required_keys(
        self, orchestrator
    ) -> None:
        # Receipt path
        state_r = _make_state(orchestrator)
        state_r.receipts = [_make_receipt()]
        events_r = _install_emit_capture(orchestrator)
        with patch(
            "almanak.framework.execution.orchestrator.build_verbose_revert_report"
        ) as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "verbose report text"
            mock_report.to_dict.return_value = {"report": "ok"}
            mock_build.return_value = mock_report
            orchestrator._complete_session = MagicMock()
            await orchestrator._phase_enrich(state_r)
        receipt_payload = next(
            d for t, d in events_r if t == ExecutionEventType.TX_REVERTED
        )

        # Exception path - reuses the same ``orchestrator`` fixture.
        # ``_install_emit_capture`` re-wraps ``_emit_event`` so it chains
        # through the previously installed capture: both ``events_r`` and
        # ``events_e`` will therefore observe events emitted after this point.
        # The test tolerates that because each side calls ``next(...)`` to pick
        # the first matching event on its own list, which is the receipt-path
        # event for ``events_r`` and the exception-path event for ``events_e``.
        state_e = _make_state(orchestrator)
        events_e = _install_emit_capture(orchestrator)
        exc = TransactionRevertedError(tx_hash="0xe", revert_reason="r")
        with patch(
            "almanak.framework.execution.orchestrator.build_verbose_revert_report"
        ) as mock_build:
            mock_report = MagicMock()
            mock_report.format.return_value = "verbose report text"
            mock_report.to_dict.return_value = {"report": "ok"}
            mock_build.return_value = mock_report
            orchestrator._complete_session = MagicMock()
            orchestrator._handle_execution_exception(state_e, exc)
        exception_payload = next(
            d for t, d in events_e if t == ExecutionEventType.TX_REVERTED
        )

        # Both payloads expose the full required key-set (superset invariant).
        # ``correlation_id`` is injected by ``_emit_event``; required fields
        # come from ``TxRevertedPayload``.
        for key in TX_REVERTED_REQUIRED_KEYS:
            assert key in receipt_payload, f"receipt payload missing {key!r}"
            assert key in exception_payload, f"exception payload missing {key!r}"


# =============================================================================
# Type-annotation check
# =============================================================================


class TestTxRevertedPayloadTyping:
    """``TypedDict`` does not carry a runtime class, so we assert structural
    compliance by comparing the set of emitted keys against the declared
    schema's required set.
    """

    def test_typed_dict_annotation_exposes_required_keys(self) -> None:
        annotations = TxRevertedPayload.__annotations__
        # Every required key declared on the TypedDict.
        for key in TX_REVERTED_REQUIRED_KEYS:
            assert key in annotations, f"{key!r} missing from TxRevertedPayload schema"
