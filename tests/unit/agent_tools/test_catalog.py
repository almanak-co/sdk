"""Tests for agent tool catalog."""

from almanak.framework.agent_tools.catalog import (
    LatencyClass,
    RiskTier,
    ToolCatalog,
    ToolCategory,
    ToolDefinition,
    get_default_catalog,
)
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
        assert len(catalog) == 34

    def test_get_existing_tool(self):
        catalog = get_default_catalog()
        tool = catalog.get("get_price")
        assert tool is not None
        assert tool.name == "get_price"
        assert tool.category == ToolCategory.DATA

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
        assert len(names) == 34

    def test_filter_by_category(self):
        catalog = get_default_catalog()

        data_tools = catalog.list_tools(category=ToolCategory.DATA)
        assert len(data_tools) == 11
        assert all(t.category == ToolCategory.DATA for t in data_tools)
        data_names = {t.name for t in data_tools}
        assert "get_wallet_overview" in data_names
        assert "check_protocol_support" in data_names

        action_tools = catalog.list_tools(category=ToolCategory.ACTION)
        assert len(action_tools) == 15
        assert all(t.category == ToolCategory.ACTION for t in action_tools)
        assert "wrap_native" in {t.name for t in action_tools}

        planning_tools = catalog.list_tools(category=ToolCategory.PLANNING)
        assert len(planning_tools) == 5

        state_tools = catalog.list_tools(category=ToolCategory.STATE)
        assert len(state_tools) == 3

    def test_mcp_tools_output(self):
        catalog = get_default_catalog()
        mcp_tools = catalog.to_mcp_tools()
        assert len(mcp_tools) == 34
        assert all("name" in t and "description" in t and "inputSchema" in t for t in mcp_tools)

    def test_openai_tools_output(self):
        catalog = get_default_catalog()
        openai_tools = catalog.to_openai_tools()
        assert len(openai_tools) == 34
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
        assert len(catalog) == 35

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
