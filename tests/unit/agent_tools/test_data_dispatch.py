from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.agent_tools.bundle_cache import BundleCache
from almanak.framework.agent_tools.catalog import ToolCategory
from almanak.framework.agent_tools.errors import AgentErrorCode, ToolValidationError
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponse, ToolResponseStatus

_DATA_ROUTES = (
    ("get_price", "_execute_get_price", False),
    ("get_balance", "_execute_get_balance", False),
    ("batch_get_balances", "_execute_batch_get_balances", False),
    ("get_indicator", "_execute_get_indicator", False),
    ("get_pool_state", "_execute_get_pool_state", True),
    ("resolve_pool_address", "_execute_resolve_pool_address", True),
    ("get_lp_position", "_execute_get_lp_position", True),
    ("list_lp_positions", "_execute_list_lp_positions", True),
    ("list_lending_positions", "_execute_list_lending_positions", True),
    ("list_lending_reserves", "_execute_list_lending_reserves", True),
    ("list_token_pools", "_execute_list_token_pools", True),
    ("get_portfolio", "_execute_get_portfolio", True),
    ("resolve_token", "_execute_resolve_token", False),
    ("get_risk_metrics", "_execute_get_risk_metrics", True),
    ("get_vault_state", "_execute_get_vault_state", True),
    ("get_wallet_overview", "_execute_get_wallet_overview", False),
    ("check_protocol_support", "_execute_check_protocol_support", False),
)


@pytest.fixture
def gateway() -> MagicMock:
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def executor(gateway: MagicMock, tmp_path) -> ToolExecutor:
    policy = AgentPolicy(
        allowed_chains={"arbitrum", "base", "bsc"},
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


def test_data_route_table_matches_catalog(executor: ToolExecutor) -> None:
    catalog_names = {tool.name for tool in executor._catalog.list_tools(category=ToolCategory.DATA)}

    assert set(executor._DATA_TOOL_HANDLERS) == catalog_names
    assert executor._DATA_TOOL_HANDLERS == {name: handler for name, handler, _ in _DATA_ROUTES}


@pytest.mark.parametrize(("tool_name", "handler_name", "is_async"), _DATA_ROUTES)
@pytest.mark.asyncio
async def test_dispatch_data_routes_every_tool(
    executor: ToolExecutor,
    tool_name: str,
    handler_name: str,
    is_async: bool,
) -> None:
    args = {"route": tool_name}
    expected = ToolResponse(status=ToolResponseStatus.SUCCESS, data={"route": tool_name})
    handler = AsyncMock(return_value=expected) if is_async else MagicMock(return_value=expected)

    with patch.object(executor, handler_name, handler):
        result = await executor._dispatch_data(tool_name, args)

    assert result is expected
    handler.assert_called_once_with(args)
    if is_async:
        handler.assert_awaited_once_with(args)


@pytest.mark.asyncio
async def test_unknown_data_tool_preserves_validation_error(executor: ToolExecutor) -> None:
    with pytest.raises(ToolValidationError) as exc_info:
        await executor._dispatch_data("not_a_data_tool", {})

    assert exc_info.value.message == "Unknown data tool: not_a_data_tool"
    assert exc_info.value.tool_name == "not_a_data_tool"
    assert exc_info.value.recoverable is True


@pytest.mark.asyncio
async def test_malformed_public_request_fails_before_dispatch(executor: ToolExecutor) -> None:
    with patch.object(executor, "_dispatch_data", AsyncMock()) as dispatch:
        result = await executor.execute("get_price", {})

    assert result.status == ToolResponseStatus.ERROR
    assert result.error is not None
    assert result.error.error_code == AgentErrorCode.VALIDATION_ERROR
    dispatch.assert_not_awaited()


@pytest.mark.parametrize(
    ("arguments", "expected_token_a", "expected_token_b"),
    [
        ({"token0": "WETH", "token1": "USDC"}, "WETH", "USDC"),
        (
            {"token_a": "weth", "token_b": "usdc", "token0": "WETH", "token1": "USDC"},
            "WETH",
            "USDC",
        ),
    ],
)
@pytest.mark.asyncio
async def test_pool_aliases_preserve_alias_precedence(
    executor: ToolExecutor,
    arguments: dict,
    expected_token_a: str,
    expected_token_b: str,
) -> None:
    expected = ToolResponse(status=ToolResponseStatus.SUCCESS, data={})
    handler = AsyncMock(return_value=expected)

    with patch.object(executor, "_execute_get_pool_state", handler):
        result = await executor.execute("get_pool_state", {**arguments, "chain": "arbitrum"})

    assert result is expected
    dispatched = handler.await_args.args[0]
    assert dispatched["token_a"] == expected_token_a
    assert dispatched["token_b"] == expected_token_b
    assert "token0" not in dispatched
    assert "token1" not in dispatched


@pytest.mark.asyncio
async def test_conflicting_pool_aliases_fail_before_handler(executor: ToolExecutor) -> None:
    handler = AsyncMock()

    with patch.object(executor, "_execute_get_pool_state", handler):
        result = await executor.execute(
            "get_pool_state",
            {
                "token_a": "WBTC",
                "token_b": "USDC",
                "token0": "WETH",
                "token1": "USDC",
                "chain": "arbitrum",
            },
        )

    assert result.status == ToolResponseStatus.ERROR
    assert result.error is not None
    assert result.error.error_code == AgentErrorCode.VALIDATION_ERROR
    assert "Conflicting values for token_a" in result.error.message
    handler.assert_not_awaited()


@pytest.mark.asyncio
async def test_chain_alias_is_canonicalized_before_handler(executor: ToolExecutor) -> None:
    expected = ToolResponse(status=ToolResponseStatus.SUCCESS, data={})
    handler = MagicMock(return_value=expected)

    with patch.object(executor, "_execute_get_price", handler):
        result = await executor.execute("get_price", {"token": "WBNB", "chain": "bnb"})

    assert result is expected
    handler.assert_called_once_with({"token": "WBNB", "chain": "bsc"})


@pytest.mark.asyncio
async def test_batch_balance_response_count_mismatch_is_recoverable_gateway_error(
    executor: ToolExecutor,
    gateway: MagicMock,
) -> None:
    gateway.market.BatchGetBalances.return_value.responses = [
        MagicMock(balance="1", balance_usd="1"),
    ]

    result = await executor.execute(
        "batch_get_balances",
        {"tokens": ["ETH", "USDC"], "chain": "arbitrum"},
    )

    assert result.status == ToolResponseStatus.ERROR
    assert result.error is not None
    assert result.error.error_code == AgentErrorCode.GATEWAY_ERROR
    assert result.error.recoverable is True
    assert result.error.message == "Gateway returned 1 balance responses for 2 requested tokens."


@pytest.mark.asyncio
async def test_get_indicator_preserves_chainless_gateway_contract(
    executor: ToolExecutor,
    gateway: MagicMock,
) -> None:
    gateway.market.GetIndicator.return_value = MagicMock(value="42", metadata={})

    result = await executor.execute(
        "get_indicator",
        {"token": "WETH", "indicator": "rsi", "period": 14, "chain": "base"},
    )

    assert result.status == ToolResponseStatus.SUCCESS
    request = gateway.market.GetIndicator.call_args.args[0]
    assert "chain" not in request.DESCRIPTOR.fields_by_name
    assert request.indicator_type == "RSI"
    assert request.token == "WETH"
    assert request.params == {"period": "14"}


@pytest.mark.asyncio
async def test_policy_denial_stays_before_data_dispatch(gateway: MagicMock, tmp_path) -> None:
    executor = ToolExecutor(
        gateway,
        policy=AgentPolicy(allowed_chains={"arbitrum"}, max_tool_calls_per_minute=100),
        bundle_cache=BundleCache(cache_dir=tmp_path),
    )

    with patch.object(executor, "_dispatch_data", AsyncMock()) as dispatch:
        result = await executor.execute("get_price", {"token": "ETH", "chain": "base"})

    assert result.status == ToolResponseStatus.ERROR
    assert result.error is not None
    assert result.error.error_code == AgentErrorCode.RISK_BLOCKED
    dispatch.assert_not_awaited()
