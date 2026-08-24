"""get_pool_state dispatch to connector-declared pair resolvers (ALM-3365).

The factory lane (agent-read capability) stays untouched; when it misses and
the protocol's PoolReaderSpec declares a ``pair_resolver``, the executor loads
and calls it with the gateway client + its own price lookup, and wraps the
payload / honest miss. Protocol names never appear in the executor.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponseStatus
from almanak.framework.agent_tools.tracing import DecisionTracer

WETH = "0x4200000000000000000000000000000000000006"
CBETH = "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22"
POOL = "0x11c1fbd4b3de66bc0565779b35171a6cf3e71f59"


def _make_executor() -> ToolExecutor:
    return ToolExecutor(
        gateway_client=MagicMock(),
        policy=AgentPolicy(),
        wallet_address="0x0000000000000000000000000000000000000001",
        deployment_id="test-deployment",
        default_chain="base",
        tracer=DecisionTracer(),
    )


class _FakeToken:
    def __init__(self, address: str) -> None:
        self.address = address


def _fake_spec(resolver) -> MagicMock:
    spec = MagicMock()
    spec.pair_resolver.load.return_value = resolver
    return spec


def _run(executor, spec, args=None):
    resolver_tokens = {"CBETH": _FakeToken(CBETH), "WETH": _FakeToken(WETH)}
    token_resolver = MagicMock()
    token_resolver.resolve_for_swap.side_effect = lambda sym, chain: resolver_tokens.get(sym.upper())
    with (
        patch(
            "almanak.framework.agent_tools.executor._resolve_pool_state_capability",
            return_value=(None, "curveish", spec),
        ),
        patch("almanak.framework.data.tokens.get_token_resolver", return_value=token_resolver),
    ):
        return asyncio.run(
            executor._execute_get_pool_state(
                {"token_a": "cbETH", "token_b": "WETH", "chain": "base", "protocol": "curveish", **(args or {})}
            )
        )


def test_dispatches_to_pair_resolver_and_wraps_payload():
    executor = _make_executor()
    payload = {"pool_address": POOL, "lp_token": "0x9824", "resolved_via": "meta_registry_find_pool_for_coins"}
    calls: list[dict] = []

    def resolver(chain, token_a, token_b, *, fee_tier=None, gateway_client=None, usd_price=None, **kw):
        calls.append(
            {"chain": chain, "a": token_a, "b": token_b, "fee_tier": fee_tier, "gw": gateway_client, "usd": usd_price}
        )
        return payload

    resp = _run(executor, _fake_spec(resolver), {"fee_tier": 7})
    assert resp.status == ToolResponseStatus.SUCCESS
    assert resp.data["pool_address"] == POOL
    assert resp.data["requested_token_a"] == CBETH
    assert resp.data["requested_token_b"] == WETH
    assert calls[0]["chain"] == "base"
    assert calls[0]["a"] == CBETH and calls[0]["b"] == WETH
    assert calls[0]["fee_tier"] == 7
    assert calls[0]["gw"] is executor._client
    assert callable(calls[0]["usd"])


def test_honest_miss_maps_to_empty_pool_error():
    executor = _make_executor()
    resp = _run(executor, _fake_spec(lambda *a, **kw: None))
    assert resp.status == ToolResponseStatus.ERROR
    assert "returned no viable pool" in str(resp.error)


def test_resolver_fault_is_recoverable_rpc_error():
    executor = _make_executor()

    def resolver(*a, **kw):
        raise RuntimeError("registry unreachable")

    resp = _run(executor, _fake_spec(resolver))
    assert resp.status == ToolResponseStatus.ERROR
    assert "Pair resolution failed" in str(resp.error)


def test_resolver_validation_fault_maps_to_validation_error():
    executor = _make_executor()

    def resolver(*a, **kw):
        raise ValueError("fee_tier carries the stable flag: pass 0 or 1")

    resp = _run(executor, _fake_spec(resolver))
    assert resp.status == ToolResponseStatus.ERROR
    assert "stable flag" in str(resp.error)
    assert "validation_error" in str(resp.error)


def test_payload_cannot_clobber_request_identity():
    executor = _make_executor()
    payload = {"pool_address": POOL, "requested_token_a": "0xspoof", "requested_token_b": "0xspoof"}
    resp = _run(executor, _fake_spec(lambda *a, **kw: dict(payload)))
    assert resp.status == ToolResponseStatus.SUCCESS
    assert resp.data["requested_token_a"] == CBETH
    assert resp.data["requested_token_b"] == WETH


def test_usd_price_callback_is_bound_to_requested_chain():
    executor = _make_executor()
    seen: list[str | None] = []

    def resolver(chain, token_a, token_b, *, usd_price=None, **kw):
        usd_price("WETH")
        return {"pool_address": POOL}

    with patch.object(executor, "_lookup_token_price", side_effect=lambda token, chain=None: seen.append(chain)):
        resp = _run(executor, _fake_spec(resolver), {"chain": "arbitrum"})
    assert resp.status == ToolResponseStatus.SUCCESS
    assert seen == ["arbitrum"]


def test_explicit_pool_address_reads_that_pool_not_the_pair_sweep():
    from decimal import Decimal
    from types import SimpleNamespace

    executor = _make_executor()
    state = SimpleNamespace(
        price=Decimal("1.001"), tick=None, liquidity=777, fee_tier=400, token0_decimals=6, token1_decimals=18
    )
    reader = MagicMock()
    reader.read_pool_price.return_value = SimpleNamespace(value=state)
    spec = _fake_spec(lambda *a, **kw: (_ for _ in ()).throw(AssertionError("pair sweep must not run")))
    spec.reader.load.return_value = MagicMock(return_value=reader)

    resp = _run(executor, spec, {"pool_address": POOL})
    assert resp.status == ToolResponseStatus.SUCCESS
    assert resp.data["pool_address"] == POOL
    assert resp.data["current_price"] == "1.001"
    assert resp.data["liquidity"] == "777"
    assert resp.data["fee_tier_source"] == "unspecified"
    reader.read_pool_price.assert_called_once_with(POOL, "base")


def test_no_resolver_keeps_unsupported_protocol_error():
    executor = _make_executor()
    spec = MagicMock()
    spec.pair_resolver = None
    with patch(
        "almanak.framework.agent_tools.executor._resolve_pool_state_capability",
        return_value=(None, "curveish", spec),
    ):
        resp = asyncio.run(
            executor._execute_get_pool_state({"token_a": "A", "token_b": "B", "chain": "base", "protocol": "curveish"})
        )
    assert resp.status == ToolResponseStatus.ERROR
    assert "Unsupported protocol" in str(resp.error)
