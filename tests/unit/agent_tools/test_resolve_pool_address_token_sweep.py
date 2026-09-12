"""Full ``resolve_pool_address`` sweep against a scripted RPC (ALM-10098).

Every registered identity probe runs over a real gateway client whose transport
is scripted, so the ERC-20 verdict under assertion is the payload the probe
really produces — the shape a builder reads back from ``ax pool <token>`` —
rather than one a stubbed ``identify_erc20`` was told to return.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponseStatus
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.gateway.proto import gateway_pb2

# The reported tokenized-RWA listing: an ERC-20 supplied where a pool was meant.
TOKEN = "0x02fca66c1d1afb4e2a7884261eb00f63598a7436"
CHAIN = "bsc"

_REVERT = gateway_pb2.RpcResponse(success=False, error='{"code": 3, "message": "execution reverted"}')


def _word(value: int) -> str:
    return "0x" + f"{value:064x}"


def _string_word(text: str) -> str:
    body = text.encode().hex().ljust(64, "0")
    return "0x" + f"{32:064x}" + f"{len(text):064x}" + body


@pytest.fixture
def token_only_gateway() -> GatewayClient:
    """A transport where only the ERC-20 reads answer; every other ABI reverts."""
    from almanak.connectors._strategy_base.pool_identity_base import (
        DECIMALS_SELECTOR,
        SYMBOL_SELECTOR,
        TOTAL_SUPPLY_SELECTOR,
    )
    from almanak.connectors.curve import pool_resolver

    answers = {
        DECIMALS_SELECTOR: _word(18),
        TOTAL_SUPPLY_SELECTOR: _word(10**24),
        SYMBOL_SELECTOR: _string_word("NVDAB"),
    }

    registry = "0x" + "22" * 20

    def call(request, timeout):
        target = json.loads(request.params)[0]
        # The transport is HEALTHY — the reported reproduction is a live chain
        # on which no registry knows the address, not a degraded provider.
        if target["to"].lower() == pool_resolver._ADDRESS_PROVIDER.lower():
            return gateway_pb2.RpcResponse(success=True, result=json.dumps(_word(int(registry, 16))))
        if target["to"].lower() != TOKEN:
            return _REVERT
        answer = answers.get(target["data"][:10])
        if answer is None:
            return _REVERT
        return gateway_pb2.RpcResponse(success=True, result=json.dumps(answer))

    pool_resolver._clear_cache()
    gateway = GatewayClient(GatewayClientConfig())
    gateway._channel = MagicMock()
    gateway._rpc_stub = MagicMock()
    gateway._rpc_stub.Call.side_effect = call
    yield gateway
    pool_resolver._clear_cache()


@pytest.fixture
def executor(token_only_gateway: GatewayClient, tmp_path) -> ToolExecutor:
    from almanak.framework.agent_tools.bundle_cache import BundleCache

    policy = AgentPolicy(
        allowed_chains={CHAIN},
        max_tool_calls_per_minute=100,
        max_single_trade_usd=Decimal("999999999"),
        max_daily_spend_usd=Decimal("999999999"),
        max_position_size_usd=Decimal("999999999"),
        require_human_approval_above_usd=Decimal("999999999"),
        cooldown_seconds=0,
        require_rebalance_check=False,
    )
    return ToolExecutor(
        token_only_gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-strategy",
        default_chain=CHAIN,
        bundle_cache=BundleCache(cache_dir=tmp_path),
    )


@pytest.mark.asyncio
async def test_token_supplied_as_pool_target_is_not_given_a_pool_address(executor: ToolExecutor) -> None:
    response = await executor.execute("resolve_pool_address", {"address": TOKEN, "chain": CHAIN})

    assert response.status == ToolResponseStatus.SUCCESS
    assert response.data["kind"] == "erc20"
    assert response.data["address"] == TOKEN
    assert response.data["symbol"] == "NVDAB"
    assert response.data["decimals"] == 18
    # The reported defect: a plain token echoed back as an executable venue.
    assert "pool_address" not in response.data
    assert TOKEN not in {v for k, v in response.data.items() if k != "address" and isinstance(v, str)}


@pytest.mark.asyncio
async def test_token_verdict_carries_the_token_to_pool_next_action(executor: ToolExecutor) -> None:
    response = await executor.execute("resolve_pool_address", {"address": TOKEN, "chain": CHAIN})

    notes = " ".join(response.data["notes"])
    assert f"almanak ax -c {CHAIN} dex-pools {TOKEN}" in notes
    assert "not usable as a pool execution target" in notes


@pytest.mark.asyncio
async def test_an_unrecognised_revert_dialect_fails_closed_rather_than_guessing(
    token_only_gateway: GatewayClient, executor: ToolExecutor
) -> None:
    """A provider whose revert is neither 'execution reverted' nor code 3/-32015.

    The classifier reads it as transport, so probes that infer non-membership
    from a revert abstain and the sweep reports a RECOVERABLE error instead of
    the plain-token verdict. That is the deliberate direction — a loose revert
    matcher would turn provider outages into definitive answers — but it means
    the verdict depends on the provider's dialect, so pin it rather than let it
    change by accident.
    """
    opaque = gateway_pb2.RpcResponse(success=False, error='{"code": -32000, "message": "call failed"}')
    original = token_only_gateway._rpc_stub.Call.side_effect

    def call(request, timeout):
        answer = original(request, timeout)
        return opaque if answer is _REVERT else answer

    token_only_gateway._rpc_stub.Call.side_effect = call

    response = await executor.execute("resolve_pool_address", {"address": TOKEN, "chain": CHAIN})

    assert response.status == ToolResponseStatus.ERROR
    assert "could not answer" in str(response.error)
    assert "retry" in str(response.error)
