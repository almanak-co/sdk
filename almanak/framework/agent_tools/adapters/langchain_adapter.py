"""LangChain / LangGraph adapter for Almanak agent tools.

Converts the Almanak tool catalog into LangChain ``StructuredTool`` instances
so agents built with LangChain or LangGraph can use Almanak tools natively.

Usage::

    from almanak.framework.agent_tools.adapters.langchain_adapter import get_langchain_tools

    tools = get_langchain_tools(catalog, executor)
    # Pass `tools` to a LangChain agent or LangGraph graph
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.agent_tools.catalog import ToolCatalog
    from almanak.framework.agent_tools.executor import ToolExecutor


def get_langchain_tools(catalog: ToolCatalog, executor: ToolExecutor) -> list:
    """Convert Almanak tools to LangChain StructuredTool instances.

    Each tool wraps the executor's ``execute()`` method so LangChain
    agents can call Almanak tools with full schema validation and
    policy enforcement.

    Requires ``langchain-core`` to be installed.
    """
    try:
        from langchain_core.tools import StructuredTool
    except ImportError as e:
        raise ImportError(
            "langchain-core is required for the LangChain adapter. Install it with: pip install langchain-core"
        ) from e

    tools = []
    for tool_def in catalog.list_tools():

        def _make_func(name: str):
            """Create a closure that captures the tool name."""

            def _run(**kwargs) -> str:
                try:
                    asyncio.get_running_loop()
                    is_running = True
                except RuntimeError:
                    is_running = False

                if is_running:
                    import concurrent.futures

                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        result = pool.submit(asyncio.run, executor.execute(name, kwargs)).result()
                else:
                    result = asyncio.run(executor.execute(name, kwargs))
                return json.dumps(result.model_dump(exclude_none=True), indent=2)

            async def _arun(**kwargs) -> str:
                result = await executor.execute(name, kwargs)
                return json.dumps(result.model_dump(exclude_none=True), indent=2)

            return _run, _arun

        sync_fn, async_fn = _make_func(tool_def.name)

        tools.append(
            StructuredTool(
                name=tool_def.name,
                description=tool_def.description,
                func=sync_fn,
                coroutine=async_fn,
                args_schema=tool_def.request_schema,
            )
        )

    return tools
