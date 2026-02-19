"""Framework adapters for exposing Almanak tools to different agent runtimes.

Each adapter transforms the same ``ToolCatalog`` into the format expected
by a specific framework (MCP, OpenAI Agents SDK, LangChain). No adapter
forks business logic -- they are pure format transforms.
"""
