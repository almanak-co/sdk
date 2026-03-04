# Framework Adapters

The Almanak agent tools package includes adapters for three popular LLM frameworks. All adapters read from the same `ToolCatalog` -- they are pure format transforms with no business logic.

```
almanak/framework/agent_tools/adapters/
├── openai_adapter.py      # OpenAI function calling
├── mcp_adapter.py         # Model Context Protocol
└── langchain_adapter.py   # LangChain / LangGraph
```

## OpenAI Function Calling

The default adapter, used by all bundled examples. Converts the catalog to OpenAI's `tools` format for the Chat Completions API.

```python
from almanak.framework.agent_tools import get_default_catalog

catalog = get_default_catalog()

# Generate OpenAI-compatible tool definitions
tools = catalog.to_openai_tools()
# [{"type": "function", "function": {"name": "get_price", "description": "...", "parameters": {...}}}, ...]

# Pass to OpenAI Chat Completions
response = client.chat.completions.create(
    model="gpt-4o",
    messages=messages,
    tools=tools,
)
```

Or use the explicit adapter:

```python
from almanak.framework.agent_tools.adapters.openai_adapter import get_openai_tools

tools = get_openai_tools(catalog)
```

This is the simplest integration path. See `examples/agentic/shared/agent_loop.py` for a complete agent loop implementation.

## MCP (Model Context Protocol)

The MCP adapter wraps the tool catalog and executor as an MCP server, compatible with Claude Desktop, Cursor, and any MCP client.

```python
from almanak.framework.agent_tools.adapters.mcp_adapter import AlmanakMCPServer

server = AlmanakMCPServer(executor)

# List available tools (MCP tools/list)
tools = server.tools_list()

# Execute a tool call (MCP tools/call)
result = await server.tools_call("get_price", {"token": "ETH", "chain": "arbitrum"})
```

### MCP Resources

The server exposes four read-only resources for agent context:

| URI | Description |
|-----|-------------|
| `almanak://chains` | Supported blockchain networks |
| `almanak://protocols` | Available DeFi protocols per chain |
| `almanak://risk-policy/current` | Active agent policy constraints |
| `almanak://wallet/capabilities` | Wallet address and available tools |

```python
# List resources (MCP resources/list)
resources = server.resources_list()

# Read a resource (MCP resources/read)
chains = server.resources_read("almanak://chains")
# {"contents": [{"uri": "almanak://chains", "text": "{\"chains\": [\"ethereum\", \"arbitrum\", ...]}"}]}
```

### Claude Desktop Configuration

To connect Claude Desktop to an Almanak MCP server, add to your Claude Desktop config:

```json
{
  "mcpServers": {
    "almanak": {
      "command": "python",
      "args": ["-m", "almanak.framework.agent_tools.adapters.mcp_adapter"],
      "env": {
        "GATEWAY_HOST": "localhost",
        "GATEWAY_PORT": "50051",
        "ALMANAK_PRIVATE_KEY": "your-key"
      }
    }
  }
}
```

!!! note
    The `almanak mcp serve` CLI command is not yet shipped. Use the Python module path or build a wrapper script.

## LangChain / LangGraph

The LangChain adapter converts Almanak tools into `StructuredTool` instances, compatible with LangChain agents and LangGraph graphs.

**Requires:** `pip install langchain-core`

```python
from almanak.framework.agent_tools.adapters.langchain_adapter import get_langchain_tools

tools = get_langchain_tools(catalog, executor)
# Returns list[StructuredTool] -- pass directly to a LangChain agent
```

Each tool wraps the executor's `execute()` method with full schema validation and policy enforcement. Both sync and async execution are supported.

### LangGraph Example

```python
from langgraph.prebuilt import create_react_agent

tools = get_langchain_tools(catalog, executor)
agent = create_react_agent(model, tools)

result = await agent.ainvoke({
    "messages": [{"role": "user", "content": "Buy $10 of ETH on Arbitrum"}]
})
```

## Choosing an Adapter

| Framework | Adapter | Best For |
|-----------|---------|----------|
| OpenAI Chat Completions | `catalog.to_openai_tools()` | Simplest path, any OpenAI-compatible API |
| MCP | `AlmanakMCPServer` | Claude Desktop, Cursor, MCP-native clients |
| LangChain / LangGraph | `get_langchain_tools()` | LangChain agents, LangGraph graphs, complex orchestration |

All adapters enforce the same `AgentPolicy` -- switching frameworks does not change the safety model.
