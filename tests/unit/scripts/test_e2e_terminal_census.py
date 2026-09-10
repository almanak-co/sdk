"""A shared terminal block bounds the independent actor submission census."""

import pytest

from qa_lab import accounting_dedicated as observer

WALLET = "0x" + "11" * 20
TX = "0x" + "22" * 32
PIN = (101, "0x" + "aa" * 32)


@pytest.fixture
def rpc(monkeypatch):
    calls = []

    def read(url, method, params):
        calls.append((method, params))
        if method == "anvil_metadata":
            return {"forkedNetwork": {"forkBlockNumber": 100}}
        if method == "eth_blockNumber":
            return "0x66"
        if method == "eth_getTransactionByHash":
            return {"from": WALLET}
        if method == "eth_chainId":
            return "0xa4b1"
        if method == "eth_getBlockByNumber":
            assert params[0] == "0x65"
            return {"number": "0x65", "hash": PIN[1], "transactions": [{"hash": TX, "from": WALLET}]}
        raise AssertionError(method)

    monkeypatch.setattr(observer, "_rpc", read)
    return calls, read


def test_pinned_census_does_not_follow_advancing_latest_head(rpc):
    rows, census = observer._fork_submissions("unused", [{"tx_hash": TX}], terminal_block=PIN)
    assert census["status"] == "PASS"
    assert census["last_block"] == PIN[0]
    assert rows == [{"tx_hash": TX}]
    assert not any(method == "eth_blockNumber" for method, _ in rpc[0])


def test_later_ledger_submission_cannot_disappear_behind_pin(rpc):
    late = "0x" + "33" * 32
    _, census = observer._fork_submissions("unused", [{"tx_hash": TX}, {"tx_hash": late}], terminal_block=PIN)
    assert census["status"] == "FAIL"
    assert census["missing_tx_hashes"] == [late]


@pytest.mark.parametrize("reorg", [False, True])
def test_wrong_or_replaced_pin_cannot_produce_complete_census(rpc, monkeypatch, tmp_path, reorg):
    read = rpc[1]

    def changed(url, method, params):
        result = read(url, method, params)
        if method == "eth_getBlockByNumber" and (not reorg or params[1] is False):
            return {**result, "hash": "0x" + "bb" * 32}
        return result

    monkeypatch.setattr(observer, "_rpc", changed)
    _, census, path = observer._capture_census("unused", [{"tx_hash": TX}], tmp_path, terminal_block=PIN)
    assert census["status"] == "UNMEASURED"
    assert "shared pin" in census["diagnostic"]
    assert path.is_file()
