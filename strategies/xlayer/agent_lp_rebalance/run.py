#!/usr/bin/env python3
"""Agentic LP rebalance on X-Layer (Uniswap V3 WOKB/USDT).

LLM-driven counterpart of the deterministic
`almanak/demo_strategies/xlayer_lp_rebalance` demo. Instead of a hand-rolled
state machine, an LLM reads market data through the Almanak gateway and
decides whether to open, hold, or rebalance a Uniswap V3 LP position on
the X-Layer WOKB/USDT pool.

Usage::

    # Terminal 1: start the gateway against an X-Layer Anvil fork
    almanak gateway --network anvil

    # Terminal 2: run the agent (real LLM)
    AGENT_LLM_API_KEY=sk-... python strategies/xlayer/agent_lp_rebalance/run.py --once

    # Or smoke test with a scripted mock LLM (no API key needed)
    python strategies/xlayer/agent_lp_rebalance/run.py --once --mock
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from decimal import Decimal
from pathlib import Path

# Almanak SDK imports -- the only framework dependencies
from almanak.framework.agent_tools import AgentPolicy, ToolExecutor, get_default_catalog
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

# Reuse the shared agent loop / LLM client utilities from examples/agentic
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "examples" / "agentic"))
from shared.agent_loop import run_agent_loop  # noqa: E402
from shared.llm_client import LLMClient, LLMConfig, LLMConfigError, MockLLMClient, validate_llm_config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("agent_xlayer_lp_rebalance")


def load_config() -> dict:
    """Load agent config from config.json alongside this script."""
    config_path = Path(__file__).parent / "config.json"
    with open(config_path) as f:
        return json.load(f)


def create_policy() -> AgentPolicy:
    """Create a tightly scoped policy for the X-Layer LP agent."""
    return AgentPolicy(
        allowed_chains={"xlayer"},
        allowed_tokens={"WOKB", "OKB", "USDT", "USDC"},
        max_single_trade_usd=Decimal("500"),
        max_daily_spend_usd=Decimal("2000"),
        cooldown_seconds=5,
        max_trades_per_hour=20,
        allowed_tools={
            "get_price",
            "get_balance",
            "get_lp_position",
            "compute_rebalance_candidate",
            "open_lp_position",
            "close_lp_position",
            "swap_tokens",
            "save_agent_state",
            "load_agent_state",
            "record_agent_decision",
        },
    )


def _mock_tool_call(name: str, args: dict) -> dict:
    """Helper to build an OpenAI-style tool_call response."""
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
    """Scripted mock LLM that opens an LP position once and reports status."""
    pool = config["pool"]
    token0, token1, fee = pool.split("/")
    strategy_id = config.get("strategy_id", "agent-xlayer-lp-rebalance")

    return MockLLMClient([
        # Round 1: load state + get prices + get balances (parallel)
        _mock_response(
            _mock_tool_call("load_agent_state", {"strategy_id": strategy_id}),
            _mock_tool_call("get_price", {"token": token0, "chain": config["chain"]}),
            _mock_tool_call("get_price", {"token": token1, "chain": config["chain"]}),
            _mock_tool_call("get_balance", {"token": token0, "chain": config["chain"]}),
            _mock_tool_call("get_balance", {"token": token1, "chain": config["chain"]}),
        ),
        # Round 2: open LP position around the current pair price
        _mock_response(
            _mock_tool_call("open_lp_position", {
                "token_a": token0,
                "token_b": token1,
                "amount_a": config["amount_token0"],
                "amount_b": config["amount_token1"],
                "price_lower": "0.0190",
                "price_upper": "0.0210",
                "fee_tier": int(fee),
                "protocol": "uniswap_v3",
                "chain": config["chain"],
            }),
        ),
        # Round 3: persist state + record decision
        _mock_response(
            _mock_tool_call("save_agent_state", {
                "strategy_id": strategy_id,
                "state": {
                    "position": "open",
                    "pool": pool,
                    "range_lower": "0.0190",
                    "range_upper": "0.0210",
                },
            }),
            _mock_tool_call("record_agent_decision", {
                "strategy_id": strategy_id,
                "decision_summary": (
                    f"Opened concentrated LP on {pool} centred on the current "
                    "pair price with a +/-5% range. No prior state."
                ),
            }),
        ),
        # Round 4: final text response
        _mock_response(content=(
            f"Opened LP position on {pool}. Range: 0.0190 - 0.0210 USDT/WOKB. Will monitor."
        )),
    ])


async def run_once(config: dict, *, use_mock: bool = False) -> None:
    """Run a single iteration of the agentic LP rebalancer."""
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
            strategy_id=config.get("strategy_id", "agent-xlayer-lp-rebalance"),
            default_chain=config.get("chain", "xlayer"),
        )

        # 3. Get OpenAI tool definitions filtered by policy's allowed_tools
        tools_openai = executor.get_filtered_openai_tools()
        logger.info("Tool catalog: %d/%d tools (filtered by policy)", len(tools_openai), len(catalog))

        # 4. Create LLM client
        if use_mock:
            llm = create_mock_llm(config)
            logger.info("LLM: MockLLMClient (scripted smoke test)")
        else:
            llm = LLMClient(llm_config)
            logger.info("LLM: %s via %s", llm_config.model, llm_config.base_url)

        # 5. Build prompt (local to this strategy directory)
        sys.path.insert(0, str(Path(__file__).parent))
        from prompts import USER_PROMPT, build_system_prompt  # noqa: E402

        system_prompt = build_system_prompt(config)

        # 6. Run agent loop
        try:
            result = await run_agent_loop(
                llm_client=llm,
                executor=executor,
                tools_openai=tools_openai,
                system_prompt=system_prompt,
                user_prompt=USER_PROMPT,
                max_rounds=config.get("max_tool_rounds", 12),
                strategy_id=config.get("strategy_id", "agent-xlayer-lp-rebalance"),
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
    parser = argparse.ArgumentParser(description="Agentic X-Layer LP rebalance")
    parser.add_argument("--once", action="store_true", help="Run a single iteration then exit")
    parser.add_argument("--mock", action="store_true", help="Use mock LLM (no API key needed)")
    args = parser.parse_args()

    config = load_config()
    logger.info("AgentXLayerLPRebalance starting: pool=%s chain=%s", config["pool"], config["chain"])

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
