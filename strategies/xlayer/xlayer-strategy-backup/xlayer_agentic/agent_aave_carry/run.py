#!/usr/bin/env python3
"""Agentic Aave V3.6 supply/borrow carry on X-Layer.

LLM-driven counterpart of the deterministic
`almanak/demo_strategies/xlayer_aave_carry` demo. Instead of a hand-rolled
state machine, an LLM reads market data through the Almanak gateway and
decides whether to supply collateral, open a borrow leg, or deleverage
the carry on X-Layer's Aave V3.6 deployment.

Usage::

    # Terminal 1: start the gateway against an X-Layer Anvil fork
    almanak gateway --network anvil

    # Terminal 2: run the agent (real LLM)
    AGENT_LLM_API_KEY=sk-... python strategies/xlayer/agent_aave_carry/run.py --once

    # Or smoke test with a scripted mock LLM (no API key needed)
    python strategies/xlayer/agent_aave_carry/run.py --once --mock
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from decimal import Decimal
from pathlib import Path

# Almanak SDK imports -- the only framework dependencies
from almanak.framework.agent_tools import AgentPolicy, ToolExecutor, get_default_catalog
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

# Reuse the shared agent loop / LLM client utilities from examples/agentic
_REPO_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(_REPO_ROOT / "examples" / "agentic"))
from shared.agent_loop import run_agent_loop  # noqa: E402
from shared.llm_client import LLMClient, LLMConfig, LLMConfigError, MockLLMClient, validate_llm_config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("agent_xlayer_aave_carry")


def load_config() -> dict:
    """Load agent config from config.json alongside this script."""
    config_path = Path(__file__).parent / "config.json"
    with open(config_path) as f:
        return json.load(f)


def create_policy() -> AgentPolicy:
    """Create a tightly scoped policy for the X-Layer Aave carry agent."""
    return AgentPolicy(
        allowed_chains={"xlayer"},
        allowed_tokens={"USDT0", "USDG", "USDT", "USDC", "WOKB", "OKB"},
        max_single_trade_usd=Decimal("500"),
        max_daily_spend_usd=Decimal("2000"),
        cooldown_seconds=5,
        max_trades_per_hour=20,
        allowed_tools={
            "get_price",
            "get_balance",
            "get_indicator",
            "supply_lending",
            "borrow_lending",
            "repay_lending",
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
    """Scripted mock LLM that opens an Aave carry once and reports status."""
    supply_token = config.get("supply_token", "USDT0")
    borrow_token = config.get("borrow_token", "USDG")
    supply_amount = config.get("initial_supply_amount", "4.0")
    strategy_id = config.get("strategy_id", "agent-xlayer-aave-carry")
    chain = config.get("chain", "xlayer")
    # 50% LTV on $4 stablecoin collateral -> ~$2 borrow
    ltv_target = Decimal(str(config.get("ltv_target", "0.5")))
    borrow_amount = (Decimal(supply_amount) * ltv_target).quantize(Decimal("0.000001"))

    return MockLLMClient([
        # Round 1: load state + balances + prices (parallel)
        _mock_response(
            _mock_tool_call("load_agent_state", {"strategy_id": strategy_id}),
            _mock_tool_call("get_balance", {"token": supply_token, "chain": chain}),
            _mock_tool_call("get_price", {"token": supply_token, "chain": chain}),
            _mock_tool_call("get_price", {"token": borrow_token, "chain": chain}),
        ),
        # Round 2: supply collateral
        _mock_response(
            _mock_tool_call("supply_lending", {
                "token": supply_token,
                "amount": supply_amount,
                "protocol": "aave_v3",
                "use_as_collateral": True,
                "chain": chain,
            }),
        ),
        # Round 3: borrow against the freshly supplied collateral
        _mock_response(
            _mock_tool_call("borrow_lending", {
                "token": borrow_token,
                "amount": str(borrow_amount),
                "collateral_token": supply_token,
                "collateral_amount": "all",
                "protocol": "aave_v3",
                "chain": chain,
            }),
        ),
        # Round 4: persist state + record decision
        _mock_response(
            _mock_tool_call("save_agent_state", {
                "strategy_id": strategy_id,
                "state": {
                    "phase": "carry_open",
                    "supplied": supply_amount,
                    "supplied_token": supply_token,
                    "borrowed": str(borrow_amount),
                    "borrowed_token": borrow_token,
                    "ltv_target": str(ltv_target),
                },
            }),
            _mock_tool_call("record_agent_decision", {
                "strategy_id": strategy_id,
                "decision_summary": (
                    f"Supplied {supply_amount} {supply_token} to Aave V3.6 X-Layer "
                    f"and borrowed {borrow_amount} {borrow_token} at {int(ltv_target * 100)}% LTV."
                ),
            }),
        ),
        # Round 5: final text response
        _mock_response(content=(
            f"Carry open: {supply_amount} {supply_token} -> {borrow_amount} {borrow_token}. "
            "Will monitor health factor on next iteration."
        )),
    ])


# ---------------------------------------------------------------------------
# P3: Pre-LLM health-factor guard (VIB-2812)
# ---------------------------------------------------------------------------
# Deterministic check that skips the LLM call when the Aave carry position
# has a comfortable health factor. Fail-open: any error -> call the LLM.

def should_skip_llm(gateway: GatewayClient, config: dict) -> tuple[bool, str]:
    """Check if the LLM call can be skipped because health factor is safe.

    WARNING: This guard computes hf_proxy from saved state (supplied/borrowed
    principal amounts), NOT live on-chain balances. Borrow debt accrues
    continuously, and the position can change out-of-band (e.g. partial
    liquidation). The proxy is therefore optimistic -- it will overestimate
    health factor as debt grows. The fail-open design (any error -> call LLM)
    mitigates hard failures, but stale-but-valid state is a silent risk.

    TODO: Query actual Aave position data from the gateway when
    a lending-position-query API is available.

    Returns:
        (skip, reason): skip=True means the LLM call is unnecessary this
        iteration. reason is a human-readable string for logging.
    """
    skip_enabled = config.get("skip_llm_enabled", True)
    if not skip_enabled:
        return False, "skip_llm disabled in config"

    threshold = Decimal(str(config.get("skip_llm_when_hf_above", "2.0")))
    strategy_id = config.get("strategy_id", "agent-xlayer-aave-carry")
    chain = config.get("chain", "xlayer")
    supply_token = config.get("supply_token", "USDT0")
    borrow_token = config.get("borrow_token", "USDG")
    # HARDCODE_RISK: collateral factor should ideally be fetched from on-chain Aave
    # data via the gateway. Using config as interim -- operators MUST update if Aave
    # governance changes the LTV for the supply token.
    collateral_factor = Decimal(str(config.get("collateral_factor", "0.70")))

    try:
        from almanak.gateway.proto import gateway_pb2

        # 1. Load saved state -- if no carry is open, we need the LLM
        state_resp = gateway.state.LoadState(
            gateway_pb2.LoadStateRequest(strategy_id=strategy_id)
        )
        state = json.loads(state_resp.data) if state_resp.data else {}
        if state.get("phase") != "carry_open":
            return False, f"no open carry (phase={state.get('phase', 'none')})"

        supplied = Decimal(str(state.get("supplied", "0")))
        borrowed = Decimal(str(state.get("borrowed", "0")))
        if supplied <= 0 or borrowed <= 0:
            return False, "missing supply/borrow amounts in state"

        # 2. Fetch current prices
        supply_price_resp = gateway.market.GetPrice(
            gateway_pb2.PriceRequest(token=supply_token, quote="USD")
        )
        borrow_price_resp = gateway.market.GetPrice(
            gateway_pb2.PriceRequest(token=borrow_token, quote="USD")
        )
        supply_price = Decimal(str(supply_price_resp.price))
        borrow_price = Decimal(str(borrow_price_resp.price))

        if supply_price <= 0 or borrow_price <= 0:
            return False, "price data unavailable or zero"

        # 3. Compute hf_proxy
        hf_proxy = (supplied * supply_price * collateral_factor) / (borrowed * borrow_price)

        min_hf = Decimal(str(config.get("min_health_factor", "1.5")))
        if hf_proxy < min_hf:
            # HF is critical -- LLM MUST be called to decide deleverage action
            return False, f"hf_proxy={hf_proxy:.4f} below min_health_factor={min_hf}"

        if hf_proxy >= threshold:
            return True, f"hf_proxy={hf_proxy:.4f} >= threshold={threshold}"

        # HF is between min and threshold -- still safe but not comfortable enough to skip
        return False, f"hf_proxy={hf_proxy:.4f} between min={min_hf} and threshold={threshold}"

    except Exception as e:
        # Fail-open: any error means we call the LLM
        logger.warning("HF guard error (fail-open, calling LLM): %s", e)
        return False, f"error: {e}"


async def run_once(config: dict, *, use_mock: bool = False) -> None:
    """Run a single iteration of the agentic Aave carry."""
    strategy_id = config.get("strategy_id", "agent-xlayer-aave-carry")

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

        # 1.5. Pre-LLM health-factor guard (P3 / VIB-2812)
        if not use_mock:
            skip, reason = should_skip_llm(gateway, config)
            if skip:
                logger.info("Skipping LLM call: %s", reason)
                # Emit structured telemetry matching agent_loop's iteration_telemetry shape
                logger.info(
                    "Telemetry: strategy=%s rounds=0 input_tokens=0 output_tokens=0 "
                    "tool_calls=0 tools=0 decision=hold_skip skip_reason=%s",
                    strategy_id, reason,
                )
                return

        # 2. Create executor with policy
        policy = create_policy()
        catalog = get_default_catalog()
        executor = ToolExecutor(
            gateway,
            policy=policy,
            catalog=catalog,
            wallet_address=config.get("wallet_address", ""),
            strategy_id=strategy_id,
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
                strategy_id=strategy_id,
            )
            logger.info("Agent result: %s", result)
        finally:
            if hasattr(llm, "close"):
                await llm.close()
    finally:
        gateway.disconnect()


async def run_loop(config: dict, *, use_mock: bool = False) -> None:
    """Run the agent in a loop with configurable interval."""
    interval = config.get("interval_seconds", 120)
    logger.info("Starting agent loop (interval=%ds, Ctrl+C to stop)", interval)

    while True:
        try:
            await run_once(config, use_mock=use_mock)
        except Exception:
            logger.exception("Agent iteration failed")
        logger.info("Sleeping %ds until next iteration...", interval)
        await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Agentic X-Layer Aave V3.6 carry")
    parser.add_argument("--once", action="store_true", help="Run a single iteration then exit")
    parser.add_argument("--mock", action="store_true", help="Use mock LLM (no API key needed)")
    args = parser.parse_args()

    config = load_config()
    logger.info(
        "AgentXLayerAaveCarry starting: chain=%s supply=%s borrow=%s",
        config["chain"],
        config["supply_token"],
        config["borrow_token"],
    )

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
