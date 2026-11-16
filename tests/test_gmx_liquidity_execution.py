"""GM escrow entry keeps payer reserves while cancellation remains independent risk reduction."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import execution
from almanak.connectors.gmx_v2.liquidity_operations import CancelLiquidityRequest
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import before_state
from tests.gmx_native_runtime import gm_completed as gm_completed


def test_gm_operation_exposure_distinguishes_deposits_from_withdrawals_and_cancellation(completed):
    for outcome in completed.outcomes:
        if outcome.operation.kind not in ("gmx_create_deposit", "gmx_create_withdrawal"):
            continue
        assert execution.Execution.increases_exposure(completed.config, outcome.operation) == (
            outcome.operation.kind == "gmx_create_deposit"
        )
        cancellation = CancelLiquidityRequest(
            target=outcome.operation.target,
            request_kind=outcome.request.kind,
            request_key=outcome.request.key,
        )
        assert not execution.Execution.increases_exposure(completed.config, cancellation)


def test_deposit_cancellation_retains_entry_decision_without_requiring_running_mode(
    completed, monkeypatch
):
    index = next(
        i for i, o in enumerate(completed.outcomes) if o.operation.kind == "gmx_create_deposit"
    )
    original = completed.outcomes[index]
    state = before_state(completed, index).model_copy(update={"mode": "teardown"})
    cancellation = CancelLiquidityRequest(
        target=original.operation.target,
        request_kind="deposit",
        request_key=original.request.key,
    )
    monkeypatch.setattr(execution.planning, "operation", lambda *args: cancellation)
    execution.validate_operation(SimpleNamespace(), state, cancellation, original.before)
    monkeypatch.setattr(execution.planning, "operation", lambda *args: original.operation)
    with pytest.raises(DataError, match="running reconciled deployment"):
        execution.validate_operation(SimpleNamespace(), state, original.operation, original.before)
