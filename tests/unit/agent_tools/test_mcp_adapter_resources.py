"""MCP resource URI, payload, and policy-boundary contracts."""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.intent_types import IntentType
from almanak.framework.agent_tools.adapters.mcp_adapter import (
    RESOURCE_CHAINS,
    RESOURCE_PROTOCOLS,
    RESOURCE_RISK_POLICY,
    RESOURCE_WALLET,
    AlmanakMCPServer,
)
from almanak.framework.agent_tools.policy import AgentPolicy


def _server(*, policy: AgentPolicy | None = None):
    executor = SimpleNamespace(
        execute=AsyncMock(),
        _policy_engine=SimpleNamespace(policy=policy or AgentPolicy()),
        _wallet_address="0x1234",
        _deployment_id="deployment:test",
    )
    catalog = MagicMock()
    catalog.list_names.return_value = ["get_price", "swap_tokens"]
    return AlmanakMCPServer(executor=executor, catalog=catalog), executor, catalog


def _payload(server: AlmanakMCPServer, uri: str) -> dict:
    result = server.resources_read(uri)
    assert result.keys() == {"contents"}
    assert result["contents"] == [
        {
            "uri": uri,
            "mimeType": "application/json",
            "text": result["contents"][0]["text"],
        }
    ]
    return json.loads(result["contents"][0]["text"])


def test_chains_resource_uses_canonical_registry_names() -> None:
    server, executor, _ = _server()

    assert _payload(server, RESOURCE_CHAINS) == {"chains": list(ChainRegistry.names())}
    executor.execute.assert_not_awaited()


def test_protocols_resource_uses_sorted_connector_registry_names() -> None:
    from almanak.connectors._connector import CONNECTOR_REGISTRY

    server, executor, _ = _server()

    assert _payload(server, RESOURCE_PROTOCOLS) == {
        "protocols": sorted(connector.name for connector in CONNECTOR_REGISTRY.all())
    }
    executor.execute.assert_not_awaited()


def test_risk_policy_resource_serializes_decimals_sets_and_intent_values() -> None:
    policy = AgentPolicy(
        max_single_trade_usd=Decimal("12.50"),
        allowed_tools={"swap_tokens", "get_price"},
        allowed_chains=set(),
        allowed_intent_types={IntentType.SUPPLY, IntentType.SWAP},
    )
    server, executor, _ = _server(policy=policy)

    payload = _payload(server, RESOURCE_RISK_POLICY)

    assert payload == {
        "max_single_trade_usd": "12.50",
        "max_daily_spend_usd": "50000",
        "max_position_size_usd": "100000",
        "allowed_tools": ["get_price", "swap_tokens"],
        "allowed_chains": [],
        "allowed_protocols": None,
        "allowed_tokens": None,
        "allowed_intent_types": ["SUPPLY", "SWAP"],
        "allowed_execution_wallets": None,
        "require_human_approval_above_usd": "10000",
        "require_simulation_before_execution": True,
        "max_trades_per_hour": 10,
        "max_tool_calls_per_minute": 60,
        "stop_loss_pct": "5.0",
        "max_consecutive_failures": 3,
        "min_rebalance_benefit_usd": "10",
        "cooldown_seconds": 300,
        "require_rebalance_check": True,
    }
    executor.execute.assert_not_awaited()


def test_wallet_resource_reports_executor_identity_and_catalog_tools() -> None:
    server, executor, catalog = _server()

    assert _payload(server, RESOURCE_WALLET) == {
        "wallet_address": "0x1234",
        "deployment_id": "deployment:test",
        "tools_available": ["get_price", "swap_tokens"],
    }
    catalog.list_names.assert_called_once_with()
    executor.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "uri",
    [
        "",
        "almanak://chains/",
        "almanak://chains?scope=all",
        "ALMANAK://chains",
        "https://example.com/chains",
    ],
)
def test_unknown_or_noncanonical_resource_uri_fails_closed(uri: str) -> None:
    server, executor, catalog = _server()

    assert server.resources_read(uri) == {"contents": []}
    executor.execute.assert_not_awaited()
    catalog.list_names.assert_not_called()
