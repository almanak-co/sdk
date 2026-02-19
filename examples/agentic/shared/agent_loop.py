"""Reusable agent loop for agentic trading examples.

This is the core "glue" code a consumer would write to connect an LLM to the
Almanak agent_tools framework. The loop:

1. Sends messages + tool definitions to the LLM
2. If the LLM returns tool_calls, executes them via ToolExecutor
3. Feeds results back as tool messages
4. Repeats until the LLM returns a text response (no tool calls) or max_rounds
"""

from __future__ import annotations

import json
import logging
from typing import Any

from almanak.framework.agent_tools import ToolExecutor

from .llm_client import LLMClientProtocol

logger = logging.getLogger(__name__)


async def run_agent_loop(
    llm_client: LLMClientProtocol,
    executor: ToolExecutor,
    tools_openai: list[dict[str, Any]],
    system_prompt: str,
    user_prompt: str,
    *,
    max_rounds: int = 10,
) -> str:
    """Run one iteration of the agent loop.

    Args:
        llm_client: LLM client (real or mock) implementing chat().
        executor: ToolExecutor wired to the gateway.
        tools_openai: Tool definitions in OpenAI function-calling format.
        system_prompt: System prompt with agent identity and rules.
        user_prompt: User prompt with current market context / instructions.
        max_rounds: Maximum LLM round-trips before forcing a hold.

    Returns:
        The LLM's final text response (after all tool calls complete).
    """
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    for round_num in range(max_rounds):
        response = await llm_client.chat(messages, tools=tools_openai)
        choice = response["choices"][0]["message"]
        tool_calls = choice.get("tool_calls", [])

        if not tool_calls:
            # LLM is done -- return final text
            content = choice.get("content", "")
            logger.info("Agent finished in %d rounds", round_num + 1)
            return content

        # Append assistant message with tool calls
        messages.append(choice)

        # Execute each tool call and append results
        for tc in tool_calls:
            name = tc["function"]["name"]
            try:
                args = json.loads(tc["function"]["arguments"])
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning("Round %d: malformed tool call from LLM: %s", round_num + 1, e)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": json.dumps({"status": "error", "error": f"Malformed arguments: {e}"}),
                })
                continue

            logger.info("Round %d: agent calling %s(%s)", round_num + 1, name, args)

            try:
                result = await executor.execute(name, args)
            except Exception as e:
                logger.error("Round %d: tool %s raised: %s", round_num + 1, name, e)
                result_json = json.dumps({"status": "error", "error": {"message": str(e)}})
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": result_json,
                })
                continue
            result_json = json.dumps(result.model_dump(exclude_none=True))

            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result_json,
            })

    logger.warning("Agent hit max_rounds=%d, forcing hold", max_rounds)
    return '{"action": "hold", "reason": "max tool rounds exceeded"}'
