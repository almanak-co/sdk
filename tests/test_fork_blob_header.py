"""Every fork gets one local block, so its head never carries the upstream header."""

import pytest

from almanak.gateway.fork_fixture import Fork


def node_with(heads):
    node = object.__new__(Fork)
    calls = []
    heads = iter(heads)

    def rpc(method, params=None):
        calls.append(method)
        if method == "eth_getBlockByNumber":
            return next(heads)
        assert method == "evm_mine"
        return "0x0"

    node.rpc = rpc
    return node, calls


def test_fork_head_is_sealed_once():
    node, calls = node_with([{"number": "0x11", "excessBlobGas": "0x0"}])
    node._seal_local_head()
    assert calls == ["evm_mine", "eth_getBlockByNumber"]


def test_unsealed_head_after_mining_is_refused():
    node, _ = node_with([{"number": "0x11", "excessBlobGas": None}])
    with pytest.raises(RuntimeError, match="blob fields"):
        node._seal_local_head()


def test_a_failed_seal_is_not_retried_by_the_readiness_wait(monkeypatch):
    node, calls = node_with([{"number": "0x11"}])
    node.process = type("Running", (), {"poll": lambda self: None})()
    original = node.rpc

    def rpc(method, params=None):
        return {} if method == "anvil_metadata" else original(method, params)

    node.rpc = rpc
    monkeypatch.setattr(Fork, "_validate_origin", lambda self: None)
    with pytest.raises(RuntimeError, match="still lacks Cancun blob fields"):
        node._wait_ready()
    assert calls.count("evm_mine") == 1
