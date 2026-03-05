"""Reusable agent loop for agentic trading examples.

This is the core "glue" code a consumer would write to connect an LLM to the
Almanak agent_tools framework. The loop:

1. Sends messages + tool definitions to the LLM
2. If the LLM returns tool_calls, executes them via ToolExecutor
3. Feeds results back as tool messages
4. Repeats until the LLM returns a text response (no tool calls) or max_rounds

Features demonstrated:
- Decision tracing: optional trace_sink callback for audit logging
- Error handling: structured errors with recoverability info for retry/abort decisions
- Tool call accounting: tracks rounds, tool names, and outcomes per iteration
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any

from almanak.framework.agent_tools import ToolExecutor

from .llm_client import LLMClientProtocol

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Decision tracing
# ---------------------------------------------------------------------------
# A trace sink receives a structured dict for every tool call and every LLM
# response. Use it to build an audit trail, write to a file, or forward to
# an observability platform.
#
# Example: file-based trace sink for local development
#
#   import json, pathlib
#
#   def file_trace_sink(event: dict) -> None:
#       path = pathlib.Path("traces/agent_decisions.jsonl")
#       path.parent.mkdir(parents=True, exist_ok=True)
#       with open(path, "a") as f:
#           f.write(json.dumps(event, default=str) + "\n")
#
#   result = await run_agent_loop(..., trace_sink=file_trace_sink)
#
# Each trace event has the shape:
#   {
#     "type": "tool_call" | "tool_result" | "loop_end",
#     "timestamp": <float>,
#     "round": <int>,
#     "tool_name": <str>,            # for tool_call/tool_result
#     "arguments": <dict>,           # for tool_call
#     "status": <str>,               # for tool_result ("success", "error", etc.)
#     "error_code": <str>,           # for tool_result errors
#     "recoverable": <bool>,         # for tool_result errors
#   }
#
# TODO: Once the framework ships a built-in FileTraceSink class, you can
# replace the manual callback with:
#
#   from almanak.framework.agent_tools.tracing import FileTraceSink
#   trace_sink = FileTraceSink("traces/agent_decisions.jsonl")
#

TraceSink = Callable[[dict[str, Any]], None]


async def run_agent_loop(
    llm_client: LLMClientProtocol,
    executor: ToolExecutor,
    tools_openai: list[dict[str, Any]],
    system_prompt: str,
    user_prompt: str,
    *,
    max_rounds: int = 10,
    trace_sink: TraceSink | None = None,
) -> str:
    """Run one iteration of the agent loop.

    Args:
        llm_client: LLM client (real or mock) implementing chat().
        executor: ToolExecutor wired to the gateway.
        tools_openai: Tool definitions in OpenAI function-calling format.
        system_prompt: System prompt with agent identity and rules.
        user_prompt: User prompt with current market context / instructions.
        max_rounds: Maximum LLM round-trips before forcing a hold.
        trace_sink: Optional callback for decision tracing / audit logging.
            Receives a structured dict for every tool call, tool result,
            and loop end. See module docstring for event schema.

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
            _emit_trace(trace_sink, {
                "type": "loop_end",
                "round": round_num + 1,
                "reason": "llm_done",
                "total_rounds": round_num + 1,
            })
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
                _emit_trace(trace_sink, {
                    "type": "tool_call",
                    "round": round_num + 1,
                    "tool_name": name,
                    "status": "parse_error",
                    "error_code": "malformed_arguments",
                    "recoverable": True,
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": json.dumps({"status": "error", "error": f"Malformed arguments: {e}"}),
                })
                continue

            logger.info("Round %d: agent calling %s(%s)", round_num + 1, name, args)
            _emit_trace(trace_sink, {
                "type": "tool_call",
                "round": round_num + 1,
                "tool_name": name,
                "arguments": args,
            })

            # ToolExecutor.execute() never raises -- errors are returned as
            # structured ToolResponse envelopes with status="error".
            result = await executor.execute(name, args)

            # -----------------------------------------------------------
            # Error handling: use structured error info for retry/abort
            # -----------------------------------------------------------
            # The agent_tools error taxonomy provides machine-readable
            # error codes and recoverability flags. The LLM agent can
            # inspect these to decide:
            #
            #   - recoverable=True  -> retry with adjusted args
            #   - recoverable=False -> abort or try a different approach
            #
            # Error codes and their typical handling:
            #   validation_error     -> fix arguments and retry
            #   risk_blocked         -> reduce trade size or abort
            #   simulation_failed    -> adjust slippage/amounts
            #   timeout              -> retry after brief delay
            #   upstream_unavailable -> retry later
            #   permission_denied    -> abort (not allowed)
            #   execution_failed     -> check on-chain state
            #
            if result.status == "error":
                error_info = result.error or {"message": "Unknown error"}
                recoverable = error_info.get("recoverable", False)
                logger.error(
                    "Round %d: tool %s failed: %s",
                    round_num + 1, name, error_info.get("message", "unknown"),
                )
                _emit_trace(trace_sink, {
                    "type": "tool_result",
                    "round": round_num + 1,
                    "tool_name": name,
                    "status": "error",
                    "error_code": error_info.get("error_code", "unknown"),
                    "recoverable": recoverable,
                })
            else:
                _emit_trace(trace_sink, {
                    "type": "tool_result",
                    "round": round_num + 1,
                    "tool_name": name,
                    "status": result.status,
                })

            result_json = json.dumps(result.model_dump(exclude_none=True))

            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result_json,
            })

    logger.warning("Agent hit max_rounds=%d, forcing hold", max_rounds)
    _emit_trace(trace_sink, {
        "type": "loop_end",
        "round": max_rounds,
        "reason": "max_rounds_exceeded",
        "total_rounds": max_rounds,
    })
    return '{"action": "hold", "reason": "max tool rounds exceeded"}'


def _emit_trace(sink: TraceSink | None, event: dict[str, Any]) -> None:
    """Emit a trace event if a sink is configured."""
    if sink is None:
        return
    event.setdefault("timestamp", time.time())
    try:
        sink(event)
    except Exception:
        logger.debug("Trace sink error (non-fatal)", exc_info=True)
