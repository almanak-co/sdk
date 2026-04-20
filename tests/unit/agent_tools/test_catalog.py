"""Tests for agent tool catalog."""

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.agent_tools.catalog import (
    LatencyClass,
    RiskTier,
    ToolCatalog,
    ToolCategory,
    ToolDefinition,
    get_default_catalog,
)
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import GetPriceRequest, GetPriceResponse


class TestToolDefinition:
    def test_immutable(self):
        td = ToolDefinition(
            name="test_tool",
            description="A test",
            category=ToolCategory.DATA,
            risk_tier=RiskTier.NONE,
            request_schema=GetPriceRequest,
            response_schema=GetPriceResponse,
        )
        assert td.name == "test_tool"
        assert td.idempotent is True
        assert td.latency_class == LatencyClass.FAST

    def test_mcp_schema(self):
        td = ToolDefinition(
            name="get_price",
            description="Get price",
            category=ToolCategory.DATA,
            risk_tier=RiskTier.NONE,
            request_schema=GetPriceRequest,
            response_schema=GetPriceResponse,
        )
        schema = td.to_mcp_schema()
        assert schema["name"] == "get_price"
        assert schema["description"] == "Get price"
        assert "inputSchema" in schema
        assert "properties" in schema["inputSchema"]

    def test_openai_schema(self):
        td = ToolDefinition(
            name="get_price",
            description="Get price",
            category=ToolCategory.DATA,
            risk_tier=RiskTier.NONE,
            request_schema=GetPriceRequest,
            response_schema=GetPriceResponse,
        )
        schema = td.to_openai_schema()
        assert schema["type"] == "function"
        assert schema["function"]["name"] == "get_price"
        assert "parameters" in schema["function"]


class TestToolCatalog:
    def test_default_catalog_has_tools(self):
        catalog = get_default_catalog()
        assert len(catalog) == 38

    def test_get_existing_tool(self):
        catalog = get_default_catalog()
        tool = catalog.get("get_price")
        assert tool is not None
        assert tool.name == "get_price"
        assert tool.category == ToolCategory.DATA

    def test_withdraw_lending_registered(self):
        """withdraw_lending must be exposed alongside supply/borrow/repay (VIB-2992)."""
        catalog = get_default_catalog()
        tool = catalog.get("withdraw_lending")
        assert tool is not None
        assert tool.name == "withdraw_lending"
        assert tool.category == ToolCategory.ACTION
        assert tool.risk_tier == RiskTier.MEDIUM
        assert tool.request_schema is not None
        assert tool.response_schema is not None

    def test_list_read_tools_registered(self):
        """list_lp_positions / list_lending_positions / get_portfolio — VIB-2995."""
        catalog = get_default_catalog()
        for name in ("list_lp_positions", "list_lending_positions", "get_portfolio"):
            tool = catalog.get(name)
            assert tool is not None, f"{name} not registered"
            assert tool.category == ToolCategory.DATA
            assert tool.risk_tier == RiskTier.NONE
            assert tool.request_schema is not None
            assert tool.response_schema is not None

    def test_get_nonexistent_tool(self):
        catalog = get_default_catalog()
        assert catalog.get("nonexistent") is None

    def test_contains(self):
        catalog = get_default_catalog()
        assert "get_price" in catalog
        assert "nonexistent" not in catalog

    def test_list_names(self):
        catalog = get_default_catalog()
        names = catalog.list_names()
        assert "get_price" in names
        assert "swap_tokens" in names
        assert "save_agent_state" in names
        assert "wrap_native" in names
        assert "get_wallet_overview" in names
        assert "check_protocol_support" in names
        assert "withdraw_lending" in names
        assert "list_lp_positions" in names
        assert "list_lending_positions" in names
        assert "get_portfolio" in names
        assert len(names) == 38

    def test_filter_by_category(self):
        catalog = get_default_catalog()

        data_tools = catalog.list_tools(category=ToolCategory.DATA)
        assert len(data_tools) == 14
        assert all(t.category == ToolCategory.DATA for t in data_tools)
        data_names = {t.name for t in data_tools}
        assert "get_wallet_overview" in data_names
        assert "check_protocol_support" in data_names

        action_tools = catalog.list_tools(category=ToolCategory.ACTION)
        assert len(action_tools) == 16
        assert all(t.category == ToolCategory.ACTION for t in action_tools)
        assert "wrap_native" in {t.name for t in action_tools}

        planning_tools = catalog.list_tools(category=ToolCategory.PLANNING)
        assert len(planning_tools) == 5

        state_tools = catalog.list_tools(category=ToolCategory.STATE)
        assert len(state_tools) == 3

    def test_mcp_tools_output(self):
        catalog = get_default_catalog()
        mcp_tools = catalog.to_mcp_tools()
        assert len(mcp_tools) == 38
        assert all("name" in t and "description" in t and "inputSchema" in t for t in mcp_tools)

    def test_openai_tools_output(self):
        catalog = get_default_catalog()
        openai_tools = catalog.to_openai_tools()
        assert len(openai_tools) == 38
        assert all(t["type"] == "function" for t in openai_tools)

    def test_custom_tool_registration(self):
        catalog = get_default_catalog()
        custom = ToolDefinition(
            name="custom_tool",
            description="Custom",
            category=ToolCategory.DATA,
            risk_tier=RiskTier.NONE,
            request_schema=GetPriceRequest,
            response_schema=GetPriceResponse,
        )
        catalog.register(custom)
        assert "custom_tool" in catalog
        assert len(catalog) == 39

    def test_risk_tiers_assigned(self):
        catalog = get_default_catalog()
        # Data tools should be NONE risk
        for tool in catalog.list_tools(category=ToolCategory.DATA):
            assert tool.risk_tier == RiskTier.NONE

        # Action tools should be MEDIUM or HIGH
        for tool in catalog.list_tools(category=ToolCategory.ACTION):
            assert tool.risk_tier in (RiskTier.MEDIUM, RiskTier.HIGH)

    def test_action_tools_not_idempotent(self):
        catalog = get_default_catalog()
        for tool in catalog.list_tools(category=ToolCategory.ACTION):
            assert tool.idempotent is False

    # -- Filtered tool generation (VIB-2810) --------------------------------

    def test_openai_tools_filtered_by_allowed(self):
        catalog = get_default_catalog()
        allowed = {"get_price", "get_balance"}
        filtered = catalog.to_openai_tools(allowed=allowed)
        assert len(filtered) == 2
        names = {t["function"]["name"] for t in filtered}
        assert names == {"get_price", "get_balance"}

    def test_openai_tools_allowed_none_returns_all(self):
        catalog = get_default_catalog()
        all_tools = catalog.to_openai_tools(allowed=None)
        assert len(all_tools) == len(catalog)

    def test_openai_tools_allowed_empty_set_returns_empty(self):
        catalog = get_default_catalog()
        filtered = catalog.to_openai_tools(allowed=set())
        assert len(filtered) == 0

    def test_openai_tools_allowed_nonexistent_returns_empty(self):
        catalog = get_default_catalog()
        filtered = catalog.to_openai_tools(allowed={"nonexistent_tool"})
        assert len(filtered) == 0

    def test_openai_tools_allowed_partial_match(self):
        catalog = get_default_catalog()
        allowed = {"get_price", "nonexistent_tool", "swap_tokens"}
        filtered = catalog.to_openai_tools(allowed=allowed)
        assert len(filtered) == 2
        names = {t["function"]["name"] for t in filtered}
        assert names == {"get_price", "swap_tokens"}

    def test_mcp_tools_filtered_by_allowed(self):
        catalog = get_default_catalog()
        allowed = {"get_price", "save_agent_state"}
        filtered = catalog.to_mcp_tools(allowed=allowed)
        assert len(filtered) == 2
        names = {t["name"] for t in filtered}
        assert names == {"get_price", "save_agent_state"}

    def test_mcp_tools_allowed_none_returns_all(self):
        catalog = get_default_catalog()
        all_tools = catalog.to_mcp_tools(allowed=None)
        assert len(all_tools) == len(catalog)


class TestToolExecutorFiltering:
    """Tests for ToolExecutor.get_filtered_openai_tools() wrapper."""

    @staticmethod
    def _make_executor(allowed_tools=None):
        mock_client = MagicMock()
        mock_client.market.GetPrice.return_value = MagicMock(price=1.0, source="mock", timestamp="now")
        return ToolExecutor(
            mock_client,
            policy=AgentPolicy(
                allowed_tools=allowed_tools,
                max_single_trade_usd=Decimal("1000"),
                cooldown_seconds=0,
            ),
            wallet_address="0x" + "ab" * 20,
            strategy_id="test-filter",
        )

    def test_executor_none_returns_all(self):
        executor = self._make_executor(allowed_tools=None)
        tools = executor.get_filtered_openai_tools()
        assert len(tools) == len(get_default_catalog())

    def test_executor_empty_set_returns_empty(self):
        executor = self._make_executor(allowed_tools=set())
        tools = executor.get_filtered_openai_tools()
        assert len(tools) == 0

    def test_executor_specific_set_filters(self):
        executor = self._make_executor(allowed_tools={"get_price", "swap_tokens"})
        tools = executor.get_filtered_openai_tools()
        assert len(tools) == 2
        names = {t["function"]["name"] for t in tools}
        assert names == {"get_price", "swap_tokens"}
