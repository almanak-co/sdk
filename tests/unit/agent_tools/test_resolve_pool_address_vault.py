"""Executor-level regression for the ERC-4626 leg of ``resolve_pool_address``.

The dispatch tests only prove the route is wired; these drive ``ToolExecutor``
end to end with every pool probe abstaining and pin the fallback ORDER the
identity sweep promises: pool probes → ERC-4626 vault probe → bare ERC-20.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.agent_tools.bundle_cache import BundleCache
from almanak.framework.agent_tools.errors import AgentErrorCode
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponseStatus
from tests.support.erc20_probe import erc20_probe_calls

VAULT = "0xbeef0e0834849acc03f0089f01f4f1eeb06873c9"
_PROBES = "almanak.connectors._strategy_base.pool_identity_base"


@pytest.fixture
def executor(tmp_path) -> ToolExecutor:
    gateway = MagicMock()
    gateway.is_connected = True
    policy = AgentPolicy(
        allowed_chains={"base"},
        max_tool_calls_per_minute=100,
        max_single_trade_usd=Decimal("999999999"),
        max_daily_spend_usd=Decimal("999999999"),
        max_position_size_usd=Decimal("999999999"),
        require_human_approval_above_usd=Decimal("999999999"),
        cooldown_seconds=0,
        require_rebalance_check=False,
    )
    return ToolExecutor(
        gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-strategy",
        bundle_cache=BundleCache(cache_dir=tmp_path),
    )


def _no_pool_probes():
    registry = MagicMock()
    registry.all.return_value = []
    return patch("almanak.connectors._strategy_pool_reader_registry.POOL_READER_REGISTRY", registry)


@pytest.mark.asyncio
async def test_vault_probe_answers_before_the_erc20_fallback(executor: ToolExecutor) -> None:
    vault_payload = {"kind": "erc4626_vault", "family": "erc4626", "protocol": "metamorpho", "vault_version": "v2"}
    with (
        _no_pool_probes(),
        patch(f"{_PROBES}.identify_erc4626_vault", return_value=vault_payload) as vault_probe,
        patch(f"{_PROBES}.identify_erc20") as erc20_probe,
    ):
        response = await executor.execute("resolve_pool_address", {"address": VAULT, "chain": "base"})

    assert response.status == ToolResponseStatus.SUCCESS
    assert response.data["address"] == VAULT
    assert response.data["kind"] == "erc4626_vault"
    assert response.data["vault_version"] == "v2"
    vault_probe.assert_called_once()
    erc20_probe.assert_not_called()


@pytest.mark.asyncio
async def test_non_vault_falls_through_to_the_erc20_verdict(executor: ToolExecutor) -> None:
    # The ERC-20 leg runs for real: a mocked ``identify_erc20`` would only
    # replay the mock's own payload, so the pool-target assertion below could
    # not fail on a probe regression.
    with (
        _no_pool_probes(),
        patch(f"{_PROBES}.identify_erc4626_vault", return_value=None) as vault_probe,
        patch(f"{_PROBES}.probe_call", side_effect=erc20_probe_calls(VAULT, symbol="X")),
    ):
        response = await executor.execute("resolve_pool_address", {"address": VAULT, "chain": "base"})

    assert response.status == ToolResponseStatus.SUCCESS
    assert response.data["kind"] == "erc20"
    assert response.data["address"] == VAULT
    assert response.data["symbol"] == "X"
    assert "pool_address" not in response.data
    vault_probe.assert_called_once()


@pytest.mark.asyncio
async def test_neither_probe_answers_is_unknown_not_a_guess(executor: ToolExecutor) -> None:
    with (
        _no_pool_probes(),
        patch(f"{_PROBES}.identify_erc4626_vault", return_value=None),
        patch(f"{_PROBES}.identify_erc20", return_value=None),
    ):
        response = await executor.execute("resolve_pool_address", {"address": VAULT, "chain": "base"})

    assert response.status == ToolResponseStatus.SUCCESS
    assert response.data["kind"] == "unknown"


@pytest.mark.asyncio
async def test_vault_probe_transport_failure_does_not_fall_through_to_erc20(executor: ToolExecutor) -> None:
    with (
        _no_pool_probes(),
        patch(
            f"{_PROBES}.identify_erc4626_vault",
            side_effect=ValueError("Gateway eth_call transport error: UNAVAILABLE timeout"),
        ),
        patch(f"{_PROBES}.identify_erc20") as erc20_probe,
    ):
        response = await executor.execute("resolve_pool_address", {"address": VAULT, "chain": "base"})

    assert response.status == ToolResponseStatus.ERROR
    assert response.error is not None
    assert response.error["error_code"] == AgentErrorCode.RPC_FAILED
    erc20_probe.assert_not_called()
