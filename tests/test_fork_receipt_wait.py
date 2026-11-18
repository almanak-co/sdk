"""Fixture submission keeps one identity while waiting for an Anvil receipt."""

from types import SimpleNamespace

import pytest

from almanak.gateway.fork_fixture import Rpc


class FixtureRpc(Rpc):
    def __init__(self, receipts):
        super().__init__("http://unused.invalid")
        self.receipts = iter(receipts)
        self.calls = []

    def __call__(self, method, params=(), *, timeout=30):
        self.calls.append((method, params))
        if method == "eth_getTransactionReceipt":
            return next(self.receipts)
        return {
            "eth_estimateGas": "0x5208",
            "eth_getTransactionCount": "0x0",
            "eth_gasPrice": "0x1",
            "eth_sendRawTransaction": "0x" + "ab" * 32,
        }[method]


def test_delayed_fixture_receipt_never_creates_another_submission():
    receipt = {"status": "0x1", "transactionHash": "0x" + "ab" * 32}
    rpc = FixtureRpc((None, None, receipt))
    signer = SimpleNamespace(
        address="0x" + "12" * 20,
        sign_transaction=lambda _: SimpleNamespace(raw_transaction=b"test-only"),
    )
    assert rpc.send(signer, "0x" + "34" * 20) == receipt
    assert sum(method == "eth_sendRawTransaction" for method, _ in rpc.calls) == 1
    lookups = [params for method, params in rpc.calls if method == "eth_getTransactionReceipt"]
    assert lookups == [[receipt["transactionHash"]]] * 3


def test_fixture_receipt_deadline_preserves_unknown_identity(monkeypatch):
    times = iter((0, 0, 1, 1))
    monkeypatch.setattr("almanak.gateway.fork_fixture.time.monotonic", lambda: next(times))
    rpc = FixtureRpc((None,))
    with pytest.raises(TimeoutError, match="receipt remains unknown: retained-hash"):
        rpc.wait_receipt("retained-hash", timeout=1)
    assert rpc.calls == [("eth_getTransactionReceipt", ["retained-hash"])]


def test_fixture_revert_preserves_receipt_evidence():
    rpc = FixtureRpc(({"status": "0x0", "blockNumber": "0x123"},))
    with pytest.raises(RuntimeError, match="transaction reverted: retained-hash.*0x123"):
        rpc.wait_receipt("retained-hash")
    assert len(rpc.calls) == 1
