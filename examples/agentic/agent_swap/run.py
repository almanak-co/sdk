#!/usr/bin/env python3
"""AgentSwap -- Autonomous buy-the-dip RSI agent on Arbitrum.

Demonstrates the simplest agentic trading pattern: read price + RSI,
swap when oversold/overbought, hold otherwise.

Usage:
    # Start gateway first (separate terminal):
    almanak gateway --network anvil

    # Run the agent (real LLM):
    AGENT_LLM_API_KEY=sk-... python examples/agentic/agent_swap/run.py --once

    # Run smoke test with mock LLM (no API key needed):
    python examples/agentic/agent_swap/run.py --once --mock
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
from shared.llm_client import LLMClient, LLMConfig, LLMConfigError, MockLLMClient, validate_llm_config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("agent_swap")


def load_config() -> dict:
    """Load agent config from config.json alongside this script."""
    config_path = Path(__file__).parent / "config.json"
    with open(config_path) as f:
        return json.load(f)


def create_policy() -> AgentPolicy:
    """Create a tightly scoped policy for the swap agent."""
    return AgentPolicy(
        allowed_chains={"arbitrum"},
        allowed_tokens={"WETH", "USDC", "ETH"},
        max_single_trade_usd=Decimal("50"),
        max_daily_spend_usd=Decimal("200"),
        cooldown_seconds=30,
        max_trades_per_hour=5,
        allowed_tools={
            "get_price",
            "get_balance",
            "get_indicator",
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
    """Create a MockLLMClient with scripted responses for swap agent smoke test.

    Sequence: load_state + get_price + get_balance x2 + get_indicator ->
    swap_tokens -> save_agent_state + record_agent_decision -> final text
    """
    buy_token = config["buy_token"]
    sell_token = config["sell_token"]
    strategy_id = config.get("strategy_id", "agent-swap-eth-usdc")

    return MockLLMClient([
        # Round 1: load state + get price + get balances + get RSI (parallel)
        _mock_response(
            _mock_tool_call("load_agent_state", {"strategy_id": strategy_id}),
            _mock_tool_call("get_price", {"token": buy_token, "chain": "arbitrum"}),
            _mock_tool_call("get_balance", {"token": buy_token, "chain": "arbitrum"}),
            _mock_tool_call("get_balance", {"token": sell_token, "chain": "arbitrum"}),
            _mock_tool_call("get_indicator", {
                "token": buy_token,
                "indicator": "RSI",
                "period": config["rsi_period"],
                "chain": "arbitrum",
            }),
        ),
        # Round 2: swap (RSI is low in mock scenario -> buy the dip)
        _mock_response(
            _mock_tool_call("swap_tokens", {
                "token_in": sell_token,
                "token_out": buy_token,
                "amount": config["trade_size_usd"],
                "chain": "arbitrum",
            }),
        ),
        # Round 3: save state + record decision
        _mock_response(
            _mock_tool_call("save_agent_state", {
                "strategy_id": strategy_id,
                "state": {
                    "last_action": "buy",
                    "rsi_at_trade": 25,
                    "price_at_trade": 1850.0,
                    "pair": f"{buy_token}/{sell_token}",
                },
            }),
            _mock_tool_call("record_agent_decision", {
                "strategy_id": strategy_id,
                "decision_summary": f"RSI=25 (oversold). Bought {config['trade_size_usd']} USD of {buy_token} at $1850.",
            }),
        ),
        # Round 4: final text response
        _mock_response(
            content=f"Executed buy-the-dip: swapped {config['trade_size_usd']} {sell_token} -> {buy_token}. RSI was 25 (oversold). Will check again next cycle.",
        ),
    ])


async def run_once(config: dict, *, use_mock: bool = False) -> None:
    """Run a single iteration of the swap agent."""
    # 0. Validate LLM config before anything else (fail-fast)
    if not use_mock:
        llm_config = LLMConfig.from_env()
        await validate_llm_config(llm_config)
        logger.info("LLM verified: %s via %s", llm_config.model, llm_config.base_url)

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
            strategy_id=config.get("strategy_id", "agent-swap-eth-usdc"),
            default_chain=config.get("chain", "arbitrum"),
        )

        # 3. Get OpenAI tool definitions from catalog
        tools_openai = catalog.to_openai_tools()

        # 4. Create LLM client
        if use_mock:
            llm = create_mock_llm(config)
            logger.info("LLM: MockLLMClient (scripted smoke test)")
        else:
            llm = LLMClient(llm_config)
            logger.info("LLM: %s via %s", llm_config.model, llm_config.base_url)

        # 5. Build prompt
        from agent_swap.prompts import USER_PROMPT, build_system_prompt  # noqa: E402

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
    parser = argparse.ArgumentParser(description="AgentSwap -- Buy-the-dip RSI agent")
    parser.add_argument("--once", action="store_true", help="Run a single iteration then exit")
    parser.add_argument("--mock", action="store_true", help="Use mock LLM (no API key needed)")
    args = parser.parse_args()

    config = load_config()
    logger.info("AgentSwap starting: pair=%s/%s chain=%s", config["buy_token"], config["sell_token"], config["chain"])

    try:
        if args.once:
            asyncio.run(run_once(config, use_mock=args.mock))
        else:
            asyncio.run(run_loop(config, use_mock=args.mock))
    except LLMConfigError as e:
        logger.error("ERROR: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
