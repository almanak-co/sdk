"""Natural language interpretation for ``almanak ax --natural``.

Single-shot LLM call that parses free-form text into a structured tool call,
then feeds it through the same ToolExecutor pipeline as structured commands.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from almanak.framework.agent_tools.catalog import get_default_catalog
from almanak.framework.agent_tools.llm_client import LLMClient, LLMConfig, LLMConfigError

logger = logging.getLogger(__name__)


@dataclass
class InterpretedAction:
    """Result of natural language interpretation."""

    tool_name: str
    arguments: dict[str, Any]
    explanation: str = ""


class NaturalLanguageError(Exception):
    """Raised when NL interpretation fails."""


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are a DeFi action parser for the Almanak CLI. Your job is to interpret
the user's natural language request and return EXACTLY ONE tool call.

Rules:
- Return exactly one tool_call. Do not return text content instead of a tool call.
- Pick the most appropriate tool from the available tools.
- If the user's request is ambiguous, pick the most conservative interpretation.
- If you cannot map the request to any tool, do NOT make up a tool call.
  Instead, return a text response explaining what tools are available.
- Default chain is "{chain}" unless the user specifies otherwise.
- For token symbols, normalize to uppercase (e.g. "eth" -> "ETH").
- For amounts, extract the numeric value as a string.

Examples of natural language -> tool call mappings:
- "what's the price of ETH?" -> get_price(token="ETH", chain="{chain}")
- "check my USDC balance" -> get_balance(token="USDC", chain="{chain}")
- "swap 5 USDC to WETH on base" -> swap_tokens(token_in="USDC", token_out="WETH", amount="5", chain="base")
- "swap about 100 bucks of USDC to ETH" -> swap_tokens(token_in="USDC", token_out="ETH", amount="100", chain="{chain}")
- "open LP with 1000 USDC and 0.5 ETH" -> open_lp_position(token_a="USDC", token_b="ETH", amount_a="1000", amount_b="0.5", chain="{chain}")
- "how much WETH do I have on base?" -> get_balance(token="WETH", chain="base")
- "list my tools" -> this is NOT a tool call, respond with text
"""


def _build_system_prompt(chain: str) -> str:
    return _SYSTEM_PROMPT_TEMPLATE.format(chain=chain)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def interpret_natural_language(
    text: str,
    chain: str,
    llm_config: LLMConfig,
) -> InterpretedAction:
    """Parse natural language into a structured tool call using the LLM.

    Args:
        text: User's natural language request.
        chain: Default chain context.
        llm_config: LLM configuration (from env vars).

    Returns:
        InterpretedAction with tool_name, arguments, and explanation.

    Raises:
        LLMConfigError: If LLM is misconfigured or unreachable.
        NaturalLanguageError: If the LLM cannot interpret the request.
    """
    catalog = get_default_catalog()
    openai_tools = catalog.to_openai_tools()

    client = LLMClient(llm_config)
    try:
        response = await client.chat(
            messages=[
                {"role": "system", "content": _build_system_prompt(chain)},
                {"role": "user", "content": text},
            ],
            tools=openai_tools,
        )
    except Exception as e:
        if isinstance(e, LLMConfigError):
            raise
        raise LLMConfigError(
            f"LLM request failed ({type(e).__name__}: {e}).\n"
            f"Use structured syntax instead: almanak ax swap USDC ETH 100"
        ) from e
    finally:
        await client.close()

    return _parse_llm_response(response, catalog_names=set(catalog.list_names()))


def _parse_llm_response(
    response: dict,
    catalog_names: set[str],
) -> InterpretedAction:
    """Extract a tool call from the LLM response."""
    choices = response.get("choices", [])
    if not choices:
        raise NaturalLanguageError("LLM returned empty response.\nTry structured syntax: almanak ax swap USDC ETH 100")

    message = choices[0].get("message", {})
    tool_calls = message.get("tool_calls", [])

    if not tool_calls:
        # LLM responded with text instead of a tool call
        content = message.get("content", "")
        if content:
            raise NaturalLanguageError(
                f"Could not interpret as a DeFi action.\n"
                f"LLM response: {content[:300]}\n\n"
                f"Try structured syntax: almanak ax swap USDC ETH 100"
            )
        raise NaturalLanguageError(
            "Could not interpret your request as a DeFi action.\nTry structured syntax: almanak ax swap USDC ETH 100"
        )

    # Take the first tool call (warn if multiple)
    if len(tool_calls) > 1:
        logger.warning("LLM returned %d tool calls; using only the first one.", len(tool_calls))

    tc = tool_calls[0]
    func = tc.get("function", {})
    tool_name = func.get("name", "")
    raw_args = func.get("arguments", "{}")

    # Parse arguments
    try:
        arguments = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
    except json.JSONDecodeError as exc:
        raise NaturalLanguageError(
            f"LLM returned malformed arguments for {tool_name}.\nTry structured syntax: almanak ax swap USDC ETH 100"
        ) from exc

    # Validate tool exists in catalog
    if tool_name not in catalog_names:
        raise NaturalLanguageError(f"LLM suggested unknown tool '{tool_name}'.\nAvailable tools: almanak ax tools")

    # Extract explanation from content (if LLM included reasoning)
    explanation = message.get("content", "") or ""

    return InterpretedAction(
        tool_name=tool_name,
        arguments=arguments,
        explanation=explanation,
    )
