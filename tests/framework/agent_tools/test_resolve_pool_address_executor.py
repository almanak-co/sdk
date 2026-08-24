"""resolve_pool_address executor dispatch (ALM-3368).

The executor iterates connector-declared identity probes from the pool-reader
registry (first claim wins, faulting probes abstain), falls back to the
neutral ERC-20 classification, then reports ``unknown``. Probe behaviour
itself is covered in ``tests/unit/connectors/test_pool_identity_probes.py``.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponseStatus
from almanak.framework.agent_tools.tracing import DecisionTracer

POOL = "0x0b1c2dcbbfa744ebd3fc17ff1a96a1e1eb4b2d69"


def _make_executor() -> ToolExecutor:
    return ToolExecutor(
        gateway_client=MagicMock(),
        policy=AgentPolicy(),
        wallet_address="0x0000000000000000000000000000000000000001",
        deployment_id="test-deployment",
        default_chain="base",
        tracer=DecisionTracer(),
    )


def _probe_spec(probe) -> MagicMock:
    spec = MagicMock()
    spec.identity_probe.load.return_value = probe
    return spec


def _run(executor, specs, address=POOL):
    registry = MagicMock()
    registry.all.return_value = tuple(specs)
    with patch("almanak.connectors._strategy_pool_reader_registry.POOL_READER_REGISTRY", registry):
        return asyncio.run(executor._execute_resolve_pool_address({"address": address, "chain": "base"}))


def test_first_claiming_probe_wins():
    executor = _make_executor()
    claimed = {"kind": "pool", "protocol": "uniswap_v3", "factory_verified": "verified"}
    abstain = _probe_spec(lambda spec, chain, address, **kw: None)
    claim = _probe_spec(lambda spec, chain, address, **kw: claimed)
    resp = _run(executor, [abstain, claim])
    assert resp.status == ToolResponseStatus.SUCCESS
    assert resp.data["protocol"] == "uniswap_v3"
    assert resp.data["address"] == POOL


def test_faulting_probe_abstains_and_others_run():
    executor = _make_executor()

    def boom(spec, chain, address, **kw):
        raise RuntimeError("registry down")

    claimed = {"kind": "pool", "protocol": "curve", "factory_verified": "verified"}
    resp = _run(executor, [_probe_spec(boom), _probe_spec(lambda *a, **kw: claimed)])
    assert resp.status == ToolResponseStatus.SUCCESS
    assert resp.data["protocol"] == "curve"


def test_probe_fault_with_no_claim_is_recoverable_error_not_unknown():
    # A faulted probe might be the very protocol that owns the address —
    # a definitive "unknown"/plain-token verdict would misidentify it.
    executor = _make_executor()

    def boom(spec, chain, address, **kw):
        raise RuntimeError("registry down")

    with patch("almanak.connectors._strategy_base.pool_identity_base.identify_erc20") as erc20_mock:
        resp = _run(executor, [_probe_spec(boom), _probe_spec(lambda *a, **kw: None)])
    erc20_mock.assert_not_called()
    assert resp.status == ToolResponseStatus.ERROR
    assert "could not answer" in str(resp.error)
    assert "rpc_failed" in str(resp.error)


def test_erc20_fallback_when_no_probe_claims():
    executor = _make_executor()
    erc20 = {"kind": "erc20", "symbol": "yvUSDC", "factory_verified": "unverified"}
    with patch("almanak.connectors._strategy_base.pool_identity_base.identify_erc20", return_value=erc20):
        resp = _run(executor, [_probe_spec(lambda *a, **kw: None)])
    assert resp.data["kind"] == "erc20"
    assert resp.data["symbol"] == "yvUSDC"


def test_unknown_when_nothing_answers():
    executor = _make_executor()
    with patch("almanak.connectors._strategy_base.pool_identity_base.identify_erc20", return_value=None):
        resp = _run(executor, [])
    assert resp.data["kind"] == "unknown"
    assert any("No registered protocol claims" in n for n in resp.data["notes"])


def test_pool_id_input_skips_erc20_fallback():
    executor = _make_executor()
    with patch("almanak.connectors._strategy_base.pool_identity_base.identify_erc20") as erc20_mock:
        resp = _run(executor, [], address="0x" + "ab" * 32)
    erc20_mock.assert_not_called()
    assert resp.data["kind"] == "unknown"


def test_stalled_probe_sweep_settles_within_deadline():
    import time as _time

    from almanak.framework.agent_tools import executor as executor_module

    ex = _make_executor()

    def stall(spec, chain, address, **kw):
        _time.sleep(1.5)
        return None

    async def timed():
        # Measured inside the loop: asyncio.run's shutdown joins the lingering
        # worker thread afterwards, but the tool has already responded — the
        # production executor runs on a long-lived loop.
        start = _time.monotonic()
        resp = await ex._execute_resolve_pool_address({"address": POOL, "chain": "base"})
        return resp, _time.monotonic() - start

    registry = MagicMock()
    registry.all.return_value = (_probe_spec(stall),)
    with (
        patch.object(executor_module, "_IDENTITY_SWEEP_DEADLINE_S", 0.2),
        patch("almanak.connectors._strategy_pool_reader_registry.POOL_READER_REGISTRY", registry),
    ):
        resp, elapsed = asyncio.run(timed())
    assert elapsed < 1.0
    assert resp.status == ToolResponseStatus.ERROR
    assert "exceeded" in str(resp.error)
    assert "rpc_failed" in str(resp.error)


def test_rejects_non_address_input():
    executor = _make_executor()
    for bad in ("not-an-address", POOL.removeprefix("0x")):  # bare hex lacks the documented 0x prefix
        resp = asyncio.run(executor._execute_resolve_pool_address({"address": bad, "chain": "base"}))
        assert resp.status == ToolResponseStatus.ERROR, bad
        assert "Not an address" in str(resp.error)
