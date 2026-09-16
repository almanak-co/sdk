"""Exact-address pool inspection retains identity and measured state."""

import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from almanak.cli.cli import almanak
from almanak.framework.agent_tools.schemas import ToolResponse
from tests.unit.cli.test_ax_commands import _error_response

ADDRESS = "0xc6962004f452be9203591991d15f6b388e09e8d0"
IDENTITY = {
    "address": ADDRESS,
    "pool_address": ADDRESS,
    "kind": "pool",
    "protocol": "uniswap_v3",
    "token0": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
    "token1": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    "factory_verified": "verified",
    "fee_tier": 500,
}
STATE = {
    "pool_address": ADDRESS,
    "token0": "WETH",
    "token1": "USDC",
    "liquidity": "123",
    "tick": -198471,
    "current_price": "2404",
    "tvl_usd": "35000000",
    "volume_24h_usd": "90000000",
    "fee_apr": "46",
}


@pytest.mark.parametrize("as_json", [False, True])
def test_address_reads_exact_pool_state_and_preserves_identity(as_json):
    with patch("almanak.framework.cli.ax._run_tool") as run:
        run.side_effect = [ToolResponse(status="success", data=IDENTITY), ToolResponse(status="success", data=STATE)]
        result = CliRunner().invoke(
            almanak, ["ax", *(["--json"] if as_json else []), "pool", ADDRESS, "-c", "arbitrum"]
        )
    assert result.exit_code == 0, result.output
    args = run.call_args_list[1].args
    assert args[1] == "get_pool_state"
    assert args[2] == {
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "pool_address": ADDRESS,
        "token_a": IDENTITY["token0"],
        "token_b": IDENTITY["token1"],
        "fee_tier": 500,
    }
    if as_json:
        data = json.loads(result.output)["data"]
        assert data["token0"] == IDENTITY["token0"]
        assert data["token0_symbol"] == "WETH"
        assert data["factory_verified"] == "verified"
        for key in ("liquidity", "tick", "current_price", "tvl_usd", "volume_24h_usd", "fee_apr"):
            assert data[key] == STATE[key]
    else:
        assert "90000000" in result.output
        assert "verified" in result.output


@pytest.mark.parametrize(
    "identity",
    [
        {**IDENTITY, "kind": "erc4626_vault"},
        {**IDENTITY, "factory_verified": "unverified"},
    ],
)
def test_unavailable_state_retains_identity_without_guessing(identity):
    with patch("almanak.framework.cli.ax._run_tool", return_value=ToolResponse(status="success", data=identity)) as run:
        result = CliRunner().invoke(almanak, ["ax", "--json", "pool", ADDRESS])
    assert result.exit_code == 0
    assert run.call_count == 1
    data = json.loads(result.output)["data"]
    assert data["address"] == ADDRESS
    assert data["state_status"] == "unavailable"
    assert data["state_reason"]


@pytest.mark.parametrize(
    "state",
    [
        _error_response("RPC timeout"),
        ToolResponse(status="success", data={**STATE, "pool_address": "0x" + "11" * 20}),
    ],
)
def test_state_failure_never_claims_another_pool_or_loses_identity(state):
    with patch("almanak.framework.cli.ax._run_tool") as run:
        run.side_effect = [ToolResponse(status="success", data=IDENTITY), state]
        result = CliRunner().invoke(almanak, ["ax", "--json", "pool", ADDRESS])
    assert result.exit_code == 1
    data = json.loads(result.output)["data"]
    assert data["address"] == ADDRESS
    assert data["state_status"] == "unavailable"


def test_missing_analytics_remain_unmeasured():
    state = {**STATE, "tvl_usd": "", "volume_24h_usd": "", "fee_apr": ""}
    with patch("almanak.framework.cli.ax._run_tool") as run:
        run.side_effect = [ToolResponse(status="success", data=IDENTITY), ToolResponse(status="success", data=state)]
        result = CliRunner().invoke(almanak, ["ax", "--json", "pool", ADDRESS])
    assert result.exit_code == 0
    data = json.loads(result.output)["data"]
    assert data["tvl_usd"] == ""
    assert data["analytics_unavailable_reason"]


def test_external_gateway_stays_connected_across_identity_and_state_reads():
    from unittest.mock import AsyncMock, MagicMock

    client = MagicMock()
    connected = True
    calls = []

    def disconnect():
        nonlocal connected
        connected = False

    async def execute(name, arguments):
        assert connected, "Gateway client not connected"
        calls.append(name)
        return ToolResponse(status="success", data=IDENTITY if name == "resolve_pool_address" else STATE)

    client.disconnect.side_effect = disconnect
    executor = MagicMock()
    executor.execute = AsyncMock(side_effect=execute)
    with patch("almanak.framework.agent_tools.cli_executor.create_cli_executor", return_value=(executor, client)):
        result = CliRunner().invoke(almanak, ["ax", "--json", "pool", ADDRESS])
    assert result.exit_code == 0, result.output
    assert calls == ["resolve_pool_address", "get_pool_state"]
    assert not connected
    client.disconnect.assert_called_once()


def test_pool_id_explains_missing_token_pair():
    from almanak.framework.cli.ax_pool import enrich_pool_identity

    identity = ToolResponse(status="success", data={"kind": "pool_id", "factory_verified": "verified"})
    result = enrich_pool_identity(
        identity, address="0x" + "1" * 64, chain="arbitrum", run_tool=lambda *_: pytest.fail("Unexpected state read")
    )
    assert "token pair" in result.data["state_reason"]


def test_managed_gateway_client_disconnects_after_composite_read():
    from unittest.mock import AsyncMock, MagicMock

    import click

    from almanak.framework.cli.ax import _run_tool

    client = MagicMock()
    executor = MagicMock()
    executor.execute = AsyncMock(return_value=ToolResponse(status="success", data=STATE))
    ctx = click.Context(click.Command("test"), obj={"managed_gateway": MagicMock()})
    with patch("almanak.framework.cli.ax._get_executor", return_value=(executor, client)):
        with ctx:
            _run_tool(ctx, "resolve_pool_address", {})
            _run_tool(ctx, "get_pool_state", {})
            client.disconnect.assert_not_called()
    client.disconnect.assert_called_once()


@pytest.mark.parametrize("liquidity", [None, 0, 42])
def test_connector_exact_pool_fetches_analytics_without_inventing_liquidity(liquidity):
    from decimal import Decimal
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from almanak.framework.agent_tools.executor import ToolExecutor
    from almanak.framework.agent_tools.policy import AgentPolicy

    client = MagicMock()
    executor = ToolExecutor(client, policy=AgentPolicy(), wallet_address="0x" + "1" * 40, deployment_id="test")
    spec = MagicMock()
    spec.reader.load.return_value.return_value.read_pool_price.return_value.value = SimpleNamespace(
        price=Decimal("1"),
        tick=None,
        liquidity=liquidity,
        fee_tier=None,
        token0_decimals=18,
        token1_decimals=6,
    )
    analytics = {"tvl_usd": "123", "volume_24h_usd": "456", "fee_apr": "0.1"}
    with patch.object(executor, "_fetch_pool_analytics", return_value=analytics) as fetch:
        result = executor._read_pool_via_connector_reader("base", "aerodrome", spec, ADDRESS)
    assert result.status.value == "success"
    assert result.data["pool_address"] == ADDRESS
    assert all(result.data[key] == value for key, value in analytics.items())
    assert result.data["liquidity"] == ("" if liquidity is None else str(liquidity))
    fetch.assert_called_once_with(chain="base", pool_address=ADDRESS, protocol="aerodrome")
