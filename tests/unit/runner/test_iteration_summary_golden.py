"""Golden and branch-table coverage for iteration summary emission."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
import structlog.testing

from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.runner.runner_state import emit_iteration_summary


@dataclass
class _Intent:
    type_value: str
    serialized: dict[str, Any]

    @property
    def intent_type(self) -> SimpleNamespace:
        return SimpleNamespace(value=self.type_value)

    def serialize(self) -> dict[str, Any]:
        return self.serialized


@dataclass
class _TransactionResult:
    tx_hash: str | None


class _SerializationFailureIntent(_Intent):
    def serialize(self) -> dict[str, Any]:
        raise ValueError("serialization failed")


def _runner(*, dry_run: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(dry_run=dry_run),
        _total_iterations=17,
    )


def _execution_result(
    *,
    transaction_results: list[Any] | None = None,
    tx_hashes: list[str] | None = None,
    receipts: list[dict[str, Any]] | None = None,
    total_gas_used: int | None = 0,
    extracted_data: dict[str, Any] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        transaction_results=transaction_results or [],
        tx_hashes=tx_hashes,
        receipts=[] if receipts is None else receipts,
        total_gas_used=total_gas_used,
        extracted_data={} if extracted_data is None else extracted_data,
    )


def _capture(result: IterationResult, *, dry_run: bool = False, chain: str | None = "base") -> list[dict]:
    with structlog.testing.capture_logs() as captured:
        emit_iteration_summary(_runner(dry_run=dry_run), result, chain)
    return captured


def _summary(captured: list[dict]) -> dict:
    summaries = [event for event in captured if event.get("event") == "iteration_summary"]
    assert len(summaries) == 1
    return summaries[0]


def test_iteration_summary_complex_payload_golden() -> None:
    completed_at = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
    serialized_intent = {"type": "PREDICTION_BUY", "size": "0"}
    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_Intent("PREDICTION_BUY", serialized_intent),  # type: ignore[arg-type]
        execution_result=_execution_result(
            transaction_results=[
                _TransactionResult("0xfirst"),
                SimpleNamespace(),
                _TransactionResult(None),
                _TransactionResult("0xsecond"),
            ],
            tx_hashes=["0xfallback"],
            receipts=[{"status": 1}, {"status": 1}, {"status": 1}],
            total_gas_used=0,
            extracted_data={"order_id": 1234, "clob_status": "MATCHED"},
        ),
        deployment_id="golden-deployment",
        duration_ms=12.345,
        timestamp=completed_at,
        balance_reconciliation={"incident": False, "warnings": []},
    )

    captured = _capture(result, chain="polygon")

    assert captured == [
        {
            "event_type": "iteration_summary",
            "deployment_id": "golden-deployment",
            "chain": "polygon",
            "iteration": 17,
            "decision": "PREDICTION_BUY",
            "intents": [serialized_intent],
            "dry_run": False,
            "txs_planned": 3,
            "txs_sent": 2,
            "tx_hashes": ["0xfirst", "0xsecond"],
            "gas_used": 0,
            "status": "SUCCESS",
            "duration_ms": 12.3,
            "hold_reason": None,
            "hold_reason_code": None,
            "reconciliation_ok": True,
            "error": None,
            "order_id": "1234",
            "clob_status": "MATCHED",
            "event": "iteration_summary",
            "log_level": "info",
        }
    ]
    assert "timestamp" not in captured[0]


def test_iteration_summary_empty_payload_golden_preserves_none_and_zero() -> None:
    result = IterationResult(
        status=IterationStatus.EXECUTION_FAILED,
        deployment_id="empty-deployment",
        duration_ms=0,
        error="failed",
        balance_reconciliation=None,
    )

    assert _capture(result, chain=None) == [
        {
            "event_type": "iteration_summary",
            "deployment_id": "empty-deployment",
            "chain": None,
            "iteration": 17,
            "decision": None,
            "intents": [],
            "dry_run": False,
            "txs_planned": 0,
            "txs_sent": 0,
            "tx_hashes": [],
            "gas_used": 0,
            "status": "EXECUTION_FAILED",
            "duration_ms": 0,
            "hold_reason": None,
            "hold_reason_code": None,
            "reconciliation_ok": None,
            "error": "failed",
            "event": "iteration_summary",
            "log_level": "info",
        }
    ]


@pytest.mark.parametrize(
    ("status", "dry_run", "intent_type", "tx_hashes", "order_id", "expected_status"),
    [
        pytest.param(IterationStatus.SUCCESS, False, "SWAP", [], None, "EXECUTION_NOOP", id="all-guards-match"),
        pytest.param(IterationStatus.SUCCESS, True, "SWAP", [], None, "SUCCESS", id="dry-run"),
        pytest.param(IterationStatus.SUCCESS, False, None, [], None, "SUCCESS", id="unknown-intent"),
        pytest.param(IterationStatus.SUCCESS, False, "HOLD", [], None, "SUCCESS", id="hold"),
        pytest.param(IterationStatus.SUCCESS, False, "SWAP", ["0xtx"], None, "SUCCESS", id="transaction"),
        pytest.param(IterationStatus.SUCCESS, False, "PREDICTION_SELL", [], "order-1", "SUCCESS", id="clob-order"),
        pytest.param(
            IterationStatus.EXECUTION_FAILED,
            False,
            "SWAP",
            [],
            None,
            "EXECUTION_FAILED",
            id="non-success",
        ),
    ],
)
def test_iteration_summary_outcome_classification_table(
    status: IterationStatus,
    dry_run: bool,
    intent_type: str | None,
    tx_hashes: list[str],
    order_id: str | None,
    expected_status: str,
) -> None:
    intent = _Intent(intent_type, {"type": intent_type}) if intent_type is not None else None
    extracted_data = {"order_id": order_id} if order_id is not None else {}
    result = IterationResult(
        status=status,
        intent=intent,  # type: ignore[arg-type]
        execution_result=_execution_result(tx_hashes=tx_hashes or None, extracted_data=extracted_data),
        deployment_id="classification-deployment",
    )

    captured = _capture(result, dry_run=dry_run)
    summary = _summary(captured)

    assert summary["status"] == expected_status
    assert result.status is status
    if expected_status == "EXECUTION_NOOP":
        assert [event["log_level"] for event in captured] == ["warning", "info"]
        assert captured[0]["event"].startswith(
            "Faux SUCCESS detected: re-classifying iteration_summary status to EXECUTION_NOOP"
        )
        assert summary["noop_reason"] == (
            "SUCCESS reported but no on-chain tx_hash and no CLOB order_id "
            "captured — iteration produced no trade-effective output"
        )
    else:
        assert len(captured) == 1
        assert "noop_reason" not in summary


@pytest.mark.parametrize(
    ("reconciliation", "expected"),
    [
        pytest.param(None, None, id="unchecked"),
        pytest.param({}, True, id="empty-clean-report"),
        pytest.param({"incident": False, "warnings": []}, True, id="explicit-clean"),
        pytest.param({"incident": True, "warnings": []}, False, id="incident"),
        pytest.param({"incident": False, "warnings": ["degraded"]}, False, id="warning"),
        pytest.param({"incident": True, "warnings": ["degraded"]}, False, id="incident-and-warning"),
    ],
)
def test_iteration_summary_reconciliation_classification_table(
    reconciliation: dict[str, Any] | None,
    expected: bool | None,
) -> None:
    result = IterationResult(
        status=IterationStatus.HOLD,
        deployment_id="reconciliation-deployment",
        balance_reconciliation=reconciliation,
    )

    assert _summary(_capture(result))["reconciliation_ok"] is expected


def test_iteration_summary_serialization_failure_logs_debug_then_summary() -> None:
    result = IterationResult(
        status=IterationStatus.EXECUTION_FAILED,
        intent=_SerializationFailureIntent("SWAP", {}),  # type: ignore[arg-type]
        deployment_id="serialization-deployment",
    )

    captured = _capture(result)

    assert [event["log_level"] for event in captured] == ["debug", "info"]
    assert captured[0]["event"] == "Failed to serialize intent for iteration_summary"
    assert captured[0]["exc_info"] is True
    assert _summary(captured)["intents"] == []


def test_iteration_summary_gateway_hashes_and_none_gas_golden() -> None:
    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=_Intent("LP_OPEN", {"type": "LP_OPEN"}),  # type: ignore[arg-type]
        execution_result=_execution_result(
            tx_hashes=["0xgateway-1", "0xgateway-2"],
            receipts=[{"status": 1}],
            total_gas_used=None,
        ),
        deployment_id="gateway-deployment",
    )

    summary = _summary(_capture(result))

    assert summary["tx_hashes"] == ["0xgateway-1", "0xgateway-2"]
    assert summary["txs_sent"] == 2
    assert summary["txs_planned"] == 2
    assert summary["gas_used"] == 0
