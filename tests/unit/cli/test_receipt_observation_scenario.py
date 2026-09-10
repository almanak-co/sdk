"""Synthetic receipt loss is fork-only, hash-preserving, and consumed durably."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.cli._receipt_observation_scenario import _ExecutionObservations
from almanak.gateway.proto import gateway_pb2


def client():
    result = MagicMock()
    result.rpc.Call.return_value = gateway_pb2.RpcResponse(success=True, result='"anvil/v1"')
    result.execution.Execute.return_value = gateway_pb2.ExecutionResult(
        success=True,
        receipts=b"[]",
        tx_hashes=["0x" + "11" * 32],
        execution_plan_hash="a" * 64,
        submission_provenance=gateway_pb2.SUBMISSION_PROVENANCE_ATTEMPTED,
    )
    return result


def request():
    return gateway_pb2.ExecuteRequest(
        chain="bsc",
        deployment_id="deployment:test",
        intent_id="intent",
        action_bundle=b'{"intent_type":"SWAP"}',
    )


def wrapper(delegate, marker, **kwargs):
    return _ExecutionObservations(
        delegate, network="anvil", managed=True, chain="bsc", marker=marker, digest="digest", **kwargs
    )


def test_receipts_withheld_once_across_wrapper_restart_without_changing_hashes(tmp_path):
    delegate = client()
    marker = tmp_path / "consumed.json"
    first = wrapper(delegate, marker).Execute(request())
    assert not first.success
    assert first.receipts == b""
    assert first.error_code == "RECEIPT_SET_INCOMPLETE"
    assert list(first.tx_hashes) == list(delegate.execution.Execute.return_value.tx_hashes)
    assert first.execution_plan_hash == "a" * 64
    assert first.submission_provenance == gateway_pb2.SUBMISSION_PROVENANCE_ATTEMPTED
    assert delegate.execution.Execute.return_value.success
    assert wrapper(delegate, marker).Execute(request()).success
    assert delegate.execution.Execute.call_count == 2
    wrapper(delegate, marker).GetTransactionStatus(SimpleNamespace())
    delegate.execution.GetTransactionStatus.assert_called_once()


@pytest.mark.parametrize(
    "network,managed,hosted", [("mainnet", True, False), ("anvil", False, False), ("anvil", True, True)]
)
def test_unsafe_runtime_refuses_before_execution(tmp_path, network, managed, hosted):
    delegate = client()
    hook = _ExecutionObservations(
        delegate, network=network, managed=managed, chain="bsc", marker=tmp_path / "c", digest="d"
    )
    with patch("almanak.framework.cli._reference_scenario.is_hosted", return_value=hosted):
        with pytest.raises(ValueError):
            hook.Execute(request())
    delegate.execution.Execute.assert_not_called()


def test_wrong_rpc_identity_refuses_before_execution(tmp_path):
    delegate = client()
    delegate.rpc.Call.return_value.result = '"Geth/v1"'
    with pytest.raises(ValueError):
        wrapper(delegate, tmp_path / "c").Execute(request())
    delegate.execution.Execute.assert_not_called()


def test_failed_real_execution_does_not_consume_fault(tmp_path):
    delegate = client()
    delegate.execution.Execute.return_value.success = False
    marker = tmp_path / "c"
    assert not wrapper(delegate, marker).Execute(request()).success
    assert not marker.exists()


@pytest.mark.parametrize("success", [False, True])
@pytest.mark.parametrize("payload", [b"not-json", b"null", b"[]"])
def test_malformed_bundle_does_not_replace_gateway_response(tmp_path, success, payload):
    delegate = client()
    delegate.execution.Execute.return_value.success = success
    req = request()
    req.action_bundle = payload
    marker = tmp_path / "c"
    assert wrapper(delegate, marker).Execute(req) is delegate.execution.Execute.return_value
    assert not marker.exists()
