"""The mainnet runner must supply what connector execution validators require.

A connector that declares an ``execution_validator`` receives the orchestrator's
``operation_observer_factory`` as its ``gateway``. Uniswap V4 refuses to sign
without one. Every Anvil intent conftest wires that factory, so a runner that
does not wire it stays green on forks and refuses only against live money.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.framework.venues import GatewayClientVenueVerificationGateway
from qa_lab.operator_gateway import OperatorGatewayClient

HEAD = 61513759
HEAD_HASH = "0x" + "ab" * 32


def _runner() -> Any:
    root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(root / "qa_lab"))
    spec = importlib.util.spec_from_file_location(
        "qa_lab.run_mainnet_intent", root / "qa_lab" / "run_mainnet_intent.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("qa_lab.run_mainnet_intent", module)
    spec.loader.exec_module(module)
    return module


RUNNER = _runner()

# A funded-looking key the signer will accept; it signs nothing in these tests.
KEY = "0x" + "11" * 32


class _Provider:
    def make_request(self, method: str, params: list[Any]) -> dict[str, Any]:
        if method == "eth_getBlockByNumber":
            return {"result": {"number": params[0], "hash": HEAD_HASH, "timestamp": hex(1757721600)}}
        raise AssertionError(f"unexpected operator read {method}")


class _Web3:
    def __init__(self) -> None:
        self.provider = _Provider()
        self.eth = SimpleNamespace(block_number=HEAD)
        self.liveness_probes = 0

    def is_connected(self) -> bool:
        self.liveness_probes += 1
        return True


def _factory_and_web3() -> tuple[Any, _Web3]:
    web3 = _Web3()
    orchestrator = RUNNER._orchestrator(
        private_key=KEY,
        rpc_url="http://127.0.0.1:8545",
        chain="robinhood",
        gateway_client=OperatorGatewayClient(web3, "robinhood"),
    )
    return orchestrator.operation_observer_factory, web3


def _observer() -> Any:
    return _factory_and_web3()[0]


def test_every_connector_validator_receives_an_observer() -> None:
    """The invariant, stated over the registry rather than over one connector."""
    validating = [c.name for c in CONNECTOR_REGISTRY.all() if c.execution_validator is not None]
    assert validating, "no connector declares an execution_validator; this test would be vacuous"
    factory = _observer()
    assert callable(factory), f"{validating} would each be validated with gateway=None"
    assert isinstance(factory(), GatewayClientVenueVerificationGateway)


def test_the_observer_reads_the_blocks_v4_freshness_asks_for() -> None:
    """Prove the transport, not just the wiring.

    ``_validate_fresh_evidence`` calls exactly these two methods. ``block_identity``
    reaches the chain through ``client.rpc.Call`` with ``eth_getBlockByNumber``,
    a path ``OperatorGatewayClient`` allows only because that method is in
    ``_READ_METHODS``.
    """
    gateway = _observer()()
    assert gateway.block_number(chain="robinhood") == HEAD
    identity = gateway.block_identity(chain="robinhood", block_number=HEAD)
    assert identity.number == HEAD
    assert identity.block_hash == HEAD_HASH
    assert identity.timestamp == 1757721600


def test_the_observer_refuses_a_chain_it_was_not_built_for() -> None:
    """A cross-chain read must not fall through to the operator's own chain."""
    gateway = _observer()()
    with pytest.raises(ValueError):
        gateway.block_identity(chain="base", block_number=HEAD)


def test_the_observer_is_built_once_not_per_validation() -> None:
    """A per-call factory would make every bundle depend on a live RPC probe.

    ``validate_connector_execution`` invokes the factory for each connector that
    declares an ``execution_validator`` -- before that validator's own ``applies``
    gate -- so it runs on Morpho and V3 bundles too, twice per ``execute()``. The
    gateway's constructor probes ``is_connected`` over the wire and raises when it
    is falsy, which web3 returns for any ``OSError``. Rebuilding per call would let
    one blip refuse an unrelated cleanup bundle and strand live collateral.
    """
    factory, web3 = _factory_and_web3()
    probes_after_construction = web3.liveness_probes
    first = factory()
    for _ in range(4):
        assert factory() is first, "the observer must be reused, not rebuilt per validation"
    assert web3.liveness_probes == probes_after_construction, (
        "calling the factory re-probed the RPC; the gateway must be constructed once"
    )
