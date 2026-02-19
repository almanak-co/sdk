"""OpenAI Agents SDK adapter for Almanak agent tools.

Converts the Almanak tool catalog into OpenAI ``@function_tool`` format
so agents built with the OpenAI Agents SDK can use Almanak tools directly.

Usage::

    from almanak.framework.agent_tools.adapters.openai_adapter import get_openai_tools

    tools = get_openai_tools(catalog)
    # Pass `tools` to the OpenAI Agents SDK agent definition
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.agent_tools.catalog import ToolCatalog


def get_openai_tools(catalog: ToolCatalog) -> list[dict]:
    """Convert Almanak tool catalog to OpenAI function-calling format.

    Returns a list of dicts compatible with OpenAI's ``tools`` parameter
    in the Chat Completions API and the Agents SDK.
    """
    return catalog.to_openai_tools()
