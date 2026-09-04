"""Branch and golden-output tests for ``almanak strat list``."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from almanak.framework.cli import status as status_mod
from almanak.framework.cli.status import list_strategies
from almanak.gateway.proto import gateway_pb2


def _summary(**overrides) -> gateway_pb2.StrategySummary:
    values = {
        "deployment_id": "deployment:abc123",
        "name": "Example",
        "status": "RUNNING",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "total_value_usd": "100.00",
        "pnl_24h_usd": "2.50",
        "last_action_at": 1_700_000_000,
        "attention_required": False,
        "attention_reason": "",
        "is_multi_chain": False,
        "chains": [],
        "consecutive_errors": 0,
        "last_iteration_at": 1_700_000_100,
        "pnl_since_deploy_usd": "5.00",
    }
    values.update(overrides)
    return gateway_pb2.StrategySummary(**values)


def _client(*, strategies=(), error: Exception | None = None) -> MagicMock:
    client = MagicMock()
    if error is not None:
        client.dashboard.ListStrategies.side_effect = error
    else:
        client.dashboard.ListStrategies.return_value = SimpleNamespace(strategies=list(strategies))
    return client


def _invoke(client: MagicMock, *args: str):
    with patch.object(status_mod, "_make_client", return_value=client):
        return CliRunner().invoke(list_strategies, list(args))


def test_list_strategies_json_golden_and_filters() -> None:
    strategies = [
        _summary(
            deployment_id="deployment:multi",
            is_multi_chain=True,
            chains=["arbitrum", "base"],
            pnl_since_deploy_usd="0",
        ),
        _summary(
            deployment_id="deployment:single",
            name="Single",
            status="PAUSED",
            chain="base",
            pnl_since_deploy_usd="",
        ),
    ]
    client = _client(strategies=strategies)

    result = _invoke(client, "--status", "running", "--chain", "base", "--json")

    assert result.exit_code == 0, result.output
    assert (
        result.output
        == json.dumps(
            [
                {
                    "deployment_id": "deployment:multi",
                    "name": "Example",
                    "status": "RUNNING",
                    "chain": "arbitrum",
                    "chains": ["arbitrum", "base"],
                    "protocol": "uniswap_v3",
                    "total_value_usd": "100.00",
                    "pnl_24h_usd": "2.50",
                    "last_action_at": 1_700_000_000,
                    "attention_required": False,
                    "attention_reason": "",
                    "consecutive_errors": 0,
                    "last_iteration_at": 1_700_000_100,
                    "pnl_since_deploy_usd": "0",
                },
                {
                    "deployment_id": "deployment:single",
                    "name": "Single",
                    "status": "PAUSED",
                    "chain": "base",
                    "chains": ["base"],
                    "protocol": "uniswap_v3",
                    "total_value_usd": "100.00",
                    "pnl_24h_usd": "2.50",
                    "last_action_at": 1_700_000_000,
                    "attention_required": False,
                    "attention_reason": "",
                    "consecutive_errors": 0,
                    "last_iteration_at": 1_700_000_100,
                    "pnl_since_deploy_usd": None,
                },
            ],
            indent=2,
        )
        + "\n"
    )
    request = client.dashboard.ListStrategies.call_args.args[0]
    assert request == gateway_pb2.ListStrategiesRequest(
        status_filter="RUNNING",
        chain_filter="base",
        include_position=False,
    )
    client.disconnect.assert_called_once_with()


def test_list_strategies_pretty_golden() -> None:
    long_id = "deployment:" + "x" * 40
    strategies = [
        _summary(
            deployment_id=long_id,
            is_multi_chain=True,
            chains=["arbitrum", "base"],
            total_value_usd="",
            pnl_24h_usd="",
            attention_required=True,
            attention_reason="manual review",
        ),
        _summary(
            deployment_id="short",
            status="CUSTOM",
            chain="",
            total_value_usd="25",
            pnl_24h_usd="-1",
            last_action_at=0,
        ),
    ]
    client = _client(strategies=strategies)

    with patch.object(status_mod, "_format_relative_time", side_effect=lambda value: f"relative-{value}"):
        result = _invoke(client)

    id_w = 35
    header = f"{'ID':<{id_w}}  {'STATUS':<10}  {'CHAIN':<12}  {'VALUE (USD)':>12}  {'PnL 24h':>10}  {'LAST ACTIVE':<14}"
    expected = (
        f"\nStrategies (2)\n\n{header}\n{'-' * len(header)}\n"
        f"{long_id[:id_w]:<{id_w}}  {'RUNNING':<10}  {'arbitrum,base':<12}  {'-':>12}  {'-':>10}  "
        f"{'relative-1700000000':<14}\n"
        "  ! manual review\n"
        f"{'short':<{id_w}}  {'CUSTOM':<10}  {'-':<12}  {'25':>12}  {'-1':>10}  {'relative-0':<14}\n"
        "\nTotal: 2 strategies\n"
    )
    assert result.exit_code == 0, result.output
    assert result.output == expected
    request = client.dashboard.ListStrategies.call_args.args[0]
    assert request.status_filter == "REGISTRY"
    assert request.chain_filter == ""
    client.disconnect.assert_called_once_with()


def test_list_strategies_empty_pretty_and_json() -> None:
    pretty_client = _client()
    json_client = _client()

    pretty_result = _invoke(pretty_client)
    json_result = _invoke(json_client, "--json")

    assert pretty_result.exit_code == 0
    assert pretty_result.output == "No strategies found.\n"
    assert json_result.exit_code == 0
    assert json_result.output == "[]\n"
    pretty_client.disconnect.assert_called_once_with()
    json_client.disconnect.assert_called_once_with()


def test_list_strategies_rpc_error_exits_and_disconnects() -> None:
    client = _client(error=RuntimeError("gateway unavailable"))

    result = _invoke(client)

    assert result.exit_code == 1
    assert result.output == "Failed to list strategies: gateway unavailable\n"
    client.disconnect.assert_called_once_with()
