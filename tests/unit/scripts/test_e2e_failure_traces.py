import json
from types import SimpleNamespace

import pytest
from hexbytes import HexBytes

from qa_lab.e2e_failure_traces import capture_failure_traces

TX = "0x" + "aa" * 32
BLOCK = "0x" + "bb" * 32
WALLET = "0x" + "11" * 20


def setup_capture(tmp_path, mutation=None):
    observed = {
        "transactions": [
            {"tx_hash": TX, "receipt_status": 0, "block_number": 12, "block_hash": BLOCK, "wallet": WALLET}
        ]
    }
    receipt = {"status": 0, "blockNumber": 12, "blockHash": HexBytes(BLOCK), "from": WALLET}
    trace = {
        "from": WALLET,
        "type": "CALL",
        "error": "execution reverted",
        "calls": [{"type": "CALL", "error": "out of gas"}],
    }
    calls = []
    if mutation == "receipt":
        receipt["blockNumber"] += 1
    if mutation == "sender":
        trace["from"] = "0x" + "22" * 20
    if mutation == "size":
        trace["output"] = "0" * (1024 * 1024)

    def request(method, params):
        calls.append((method, params))
        if mutation == "transport":
            raise RuntimeError("credential-bearing provider error")
        return {"error": {"message": "secret"}} if mutation == "unavailable" else {"result": trace}

    client = SimpleNamespace(
        eth=SimpleNamespace(get_transaction_receipt=lambda tx: receipt), provider=SimpleNamespace(make_request=request)
    )
    context = SimpleNamespace(
        chain="arbitrum",
        network="anvil",
        require_owned=lambda path: path,
        assert_rpc_identity=lambda *args: client,
        public_identity=lambda: {"instance_id": "test-fork"},
    )
    return context, observed, calls, trace


def test_failed_transaction_retains_raw_call_tree_without_admission(tmp_path):
    context, observed, calls, trace = setup_capture(tmp_path)
    result = capture_failure_traces(context, observed, tmp_path)
    assert calls == [("debug_traceTransaction", [TX, {"tracer": "callTracer"}])]
    assert result["status"] == "CAPTURED"
    assert result["e2e_admission"] == "UNMEASURED"
    raw = json.loads((tmp_path / result["traces"][0]["artifact"]).read_text())
    assert raw["trace"] == trace
    assert raw["block_hash"] == BLOCK
    assert raw["e2e_admission"] == "UNMEASURED"


@pytest.mark.parametrize("mutation", ["receipt", "sender", "size", "transport", "unavailable", "network"])
def test_unavailable_or_mismatched_trace_is_unmeasured_without_erasing_cleanup(tmp_path, mutation):
    context, observed, calls, _ = setup_capture(tmp_path, mutation)
    if mutation == "network":
        context.network = "mainnet"
    result = capture_failure_traces(context, observed, tmp_path)
    assert result["status"] == "UNMEASURED"
    assert not list(tmp_path.iterdir())
    assert "secret" not in json.dumps(result)
    assert "credential" not in json.dumps(result)
    if mutation in {"receipt", "network"}:
        assert not calls


def test_trace_attempts_are_bounded_and_successful_transactions_not_traced(tmp_path):
    context, observed, calls, _ = setup_capture(tmp_path, "unavailable")
    observed["transactions"] *= 5
    observed["transactions"].append({"receipt_status": 1})
    result = capture_failure_traces(context, observed, tmp_path)
    assert len(calls) == 4
    assert result["not_attempted"] == 1
    assert result["status"] == "UNMEASURED"
    assert (
        capture_failure_traces(context, {"transactions": [{"receipt_status": 1}]}, tmp_path)["status"] == "NOT_REQUIRED"
    )
