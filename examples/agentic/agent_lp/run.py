#!/usr/bin/env python3
"""AgentLP -- Autonomous LP agent on Trader Joe V2.

Demonstrates how a third-party consumer uses the Almanak agent_tools framework
to build an autonomous LP management agent. The LLM reads market data, decides
whether to open/close/rebalance an LP position, and executes via the gateway.

Usage:
    # Start gateway first (separate terminal):
    almanak gateway --network anvil

    # Run the agent (real LLM):
    AGENT_LLM_API_KEY=sk-... python examples/agentic/agent_lp/run.py --once

    # Run smoke test with mock LLM (no API key needed):
    python examples/agentic/agent_lp/run.py --once --mock
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from decimal import Decimal
from pathlib import Path

# Almanak SDK imports -- these are the only framework dependencies
from almanak.framework.agent_tools import AgentPolicy, ToolExecutor, get_default_catalog
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

# Local shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared.agent_loop import run_agent_loop  # noqa: E402
from shared.llm_client import LLMClient, LLMConfig, MockLLMClient  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("agent_lp")


def load_config() -> dict:
    """Load agent config from config.json alongside this script."""
    config_path = Path(__file__).parent / "config.json"
    with open(config_path) as f:
        return json.load(f)


def create_policy() -> AgentPolicy:
    """Create a tightly scoped policy for the LP agent."""
    return AgentPolicy(
        allowed_chains={"avalanche"},
        allowed_tokens={"WAVAX", "USDC", "AVAX"},
        max_single_trade_usd=Decimal("100"),
        max_daily_spend_usd=Decimal("500"),
        cooldown_seconds=30,
        max_trades_per_hour=5,
        allowed_tools={
            "get_price",
            "get_balance",
            "get_indicator",
            "open_lp_position",
            "close_lp_position",
            "swap_tokens",
            "save_agent_state",
            "load_agent_state",
            "record_agent_decision",
        },
    )


def _mock_tool_call(name: str, args: dict) -> dict:
    """Helper to build an OpenAI tool_call response."""
    import uuid

    return {
        "id": f"call_{uuid.uuid4().hex[:8]}",
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(args)},
    }


def _mock_response(*tool_calls, content: str | None = None) -> dict:
    """Build a mock OpenAI chat completion response."""
    msg: dict = {"role": "assistant", "content": content}
    if tool_calls:
        msg["tool_calls"] = list(tool_calls)
    return {"choices": [{"message": msg, "finish_reason": "stop"}]}


def create_mock_llm(config: dict) -> MockLLMClient:
    """Create a MockLLMClient with realistic scripted responses for LP agent smoke test.

    Sequence: load_state -> get_price -> get_balance x2 -> open_lp_position ->
    save_agent_state -> record_agent_decision -> final text
    """
    pool = config["pool"]
    token_a, token_b, fee = pool.split("/")

    return MockLLMClient([
        # Round 1: load state + get price + get balances (parallel)
        _mock_response(
            _mock_tool_call("load_agent_state", {"strategy_id": config.get("strategy_id", "agent-lp")}),
            _mock_tool_call("get_price", {"token": token_a}),
            _mock_tool_call("get_balance", {"token": token_a}),
            _mock_tool_call("get_balance", {"token": token_b}),
        ),
        # Round 2: open LP position
        _mock_response(
            _mock_tool_call("open_lp_position", {
                "token_a": token_a,
                "token_b": token_b,
                "amount_a": config["amount_x"],
                "amount_b": config["amount_y"],
                "price_lower": "8.5",
                "price_upper": "10.0",
                "fee_tier": int(fee),
                "protocol": "traderjoe_v2",
            }),
        ),
        # Round 3: save state + record decision
        _mock_response(
            _mock_tool_call("save_agent_state", {
                "strategy_id": config.get("strategy_id", "agent-lp"),
                "state": {"position": "open", "range": [8.5, 10.0], "pool": pool},
            }),
            _mock_tool_call("record_agent_decision", {
                "strategy_id": config.get("strategy_id", "agent-lp"),
                "decision_summary": "Opened LP position on WAVAX/USDC with range 8.5-10.0. No existing position; price is 9.23 within range.",
            }),
        ),
        # Round 4: final text response
        _mock_response(content="Opened LP position on WAVAX/USDC/20. Range: $8.50 - $10.00. Will monitor."),
    ])


async def run_once(config: dict, *, use_mock: bool = False) -> None:
    """Run a single iteration of the LP agent."""
    # 1. Connect to gateway
    gw_config = GatewayClientConfig.from_env()
    gateway = GatewayClient(gw_config)
    gateway.connect()

    try:
        if not gateway.wait_for_ready(timeout=15.0):
            logger.error("Gateway not ready -- is it running? (almanak gateway --network anvil)")
            return

        logger.info("Gateway connected at %s:%d", gw_config.host, gw_config.port)

        # 2. Create executor with policy
        policy = create_policy()
        catalog = get_default_catalog()
        executor = ToolExecutor(
            gateway,
            policy=policy,
            catalog=catalog,
            wallet_address=config.get("wallet_address", ""),
            strategy_id=config.get("strategy_id", "agent-lp"),
            default_chain=config.get("chain", "avalanche"),
        )

        # 3. Get OpenAI tool definitions from catalog
        tools_openai = catalog.to_openai_tools()

        # 4. Create LLM client
        if use_mock:
            llm = create_mock_llm(config)
            logger.info("LLM: MockLLMClient (scripted smoke test)")
        else:
            llm_config = LLMConfig.from_env()
            if not llm_config.api_key:
                logger.error("Set AGENT_LLM_API_KEY environment variable")
                return
            llm = LLMClient(llm_config)
            logger.info("LLM: %s via %s", llm_config.model, llm_config.base_url)

        # 5. Build prompt
        from agent_lp.prompts import USER_PROMPT, build_system_prompt  # noqa: E402

        system_prompt = build_system_prompt(config)

        # 6. Run agent loop
        try:
            result = await run_agent_loop(
                llm_client=llm,
                executor=executor,
                tools_openai=tools_openai,
                system_prompt=system_prompt,
                user_prompt=USER_PROMPT,
                max_rounds=config.get("max_tool_rounds", 10),
            )
            logger.info("Agent result: %s", result)
        finally:
            if hasattr(llm, "close"):
                await llm.close()
    finally:
        gateway.disconnect()


async def run_loop(config: dict, *, use_mock: bool = False) -> None:
    """Run the agent in a loop with configurable interval."""
    interval = config.get("interval_seconds", 60)
    logger.info("Starting agent loop (interval=%ds, Ctrl+C to stop)", interval)

    while True:
        try:
            await run_once(config, use_mock=use_mock)
        except Exception:
            logger.exception("Agent iteration failed")
        logger.info("Sleeping %ds until next iteration...", interval)
        await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="AgentLP -- Autonomous LP agent")
    parser.add_argument("--once", action="store_true", help="Run a single iteration then exit")
    parser.add_argument("--mock", action="store_true", help="Use mock LLM (no API key needed)")
    args = parser.parse_args()

    config = load_config()
    logger.info("AgentLP starting: pool=%s chain=%s", config["pool"], config["chain"])

    if args.once:
        asyncio.run(run_once(config, use_mock=args.mock))
    else:
        asyncio.run(run_loop(config, use_mock=args.mock))


if __name__ == "__main__":
    main()
