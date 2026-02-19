#!/usr/bin/env python3
"""DeFAI Vault + LP Agent -- Deploy a Lagoon vault and open Uniswap V3 LP on Base.

Demonstrates the full DeFAI vision: an AI agent that autonomously deploys a vault,
initializes it, funds it with USDC, opens a concentrated LP position using vault
funds (via Safe), and settles the vault -- all through the Almanak agent_tools framework.

Two modes:
  INIT (first boot, no vault exists):
    11-phase lifecycle: market assessment -> deploy vault -> approve -> settle ->
    deposit -> settle -> discover pool price -> open LP -> NAV settle -> save state -> summary

  RUNNING (24/7, vault exists):
    P0 Teardown -> P1 Settle -> P2 LP Health / Rebalance -> P3 Deploy Idle -> P4 Hold

Usage:
    # Start gateway first (separate terminal):
    ALMANAK_GATEWAY_SAFE_ADDRESS=0x98aE... ALMANAK_GATEWAY_SAFE_MODE=direct almanak gateway --network anvil

    # Run the agent (real LLM):
    AGENT_LLM_API_KEY=sk-... python examples/agentic/defai_vault_lp/run.py --once

    # Run smoke test with mock LLM (no API key needed):
    python examples/agentic/defai_vault_lp/run.py --once --mock
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import uuid
from decimal import Decimal
from pathlib import Path

# Almanak SDK imports
from almanak.framework.agent_tools import AgentPolicy, ToolExecutor, get_default_catalog
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

# Local shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared.agent_loop import run_agent_loop  # noqa: E402
from shared.llm_client import DynamicMockLLMClient, LLMClient, LLMConfig  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("defai_vault_lp")


def load_config() -> dict:
    """Load agent config from config.json alongside this script, with env overrides."""
    import os

    config_path = Path(__file__).parent / "config.json"
    with open(config_path) as f:
        config = json.load(f)

    # Environment variable overrides for sensitive addresses
    if os.environ.get("VAULT_WALLET_ADDRESS"):
        config["wallet_address"] = os.environ["VAULT_WALLET_ADDRESS"]
    if os.environ.get("VAULT_SAFE_ADDRESS"):
        config["safe_address"] = os.environ["VAULT_SAFE_ADDRESS"]

    return config


def create_policy(config: dict) -> AgentPolicy:
    """Create a policy scoped for the DeFAI vault + LP agent."""
    policy_cfg = config.get("policy", {})
    wallet = config.get("wallet_address", "")
    safe = config.get("safe_address", wallet)
    return AgentPolicy(
        allowed_chains={"base"},
        allowed_tokens={"ALMANAK", "USDC", "ETH"},
        allowed_execution_wallets={wallet, safe} if wallet else None,
        max_single_trade_usd=Decimal(policy_cfg.get("max_single_trade_usd", "10000")),
        max_daily_spend_usd=Decimal(policy_cfg.get("max_daily_spend_usd", "50000")),
        cooldown_seconds=policy_cfg.get("cooldown_seconds", 0),
        max_trades_per_hour=policy_cfg.get("max_trades_per_hour", 30),
        require_rebalance_check=False,  # Vault LP uses its own rebalance logic
        allowed_tools={
            # Data
            "get_price",
            "get_balance",
            "get_vault_state",
            "get_pool_state",
            "get_lp_position",
            "get_indicator",
            "resolve_token",
            # Planning
            "compute_rebalance_candidate",
            "simulate_intent",
            # Vault lifecycle
            "deploy_vault",
            "approve_vault_underlying",
            "deposit_vault",
            "settle_vault",
            "teardown_vault",
            # LP
            "open_lp_position",
            "close_lp_position",
            "swap_tokens",
            # State
            "save_agent_state",
            "load_agent_state",
            "record_agent_decision",
        },
    )


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _tc(name: str, args: dict) -> dict:
    """Build an OpenAI tool_call dict."""
    return {
        "id": f"call_{uuid.uuid4().hex[:8]}",
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(args)},
    }


def _resp(*tool_calls, content: str | None = None) -> dict:
    """Build a mock OpenAI chat completion response."""
    msg: dict = {"role": "assistant", "content": content}
    if tool_calls:
        msg["tool_calls"] = list(tool_calls)
    return {"choices": [{"message": msg, "finish_reason": "stop"}]}


def create_dynamic_mock_llm(config: dict) -> DynamicMockLLMClient:
    """Create a DynamicMockLLMClient with 11-round vault lifecycle flow.

    Each round function receives a context dict populated from prior tool results.
    Key context values:
        - vault_address: set after round 2 (deploy_vault)
        - balance_USDC: set after round 1 (get_balance)
        - price_ALMANAK: set after round 1 (get_price)
        - current_price: set after round 7 (get_pool_state)
    """
    vault_cfg = config.get("vault", {})
    lp_cfg = config.get("lp", {})
    deposit_cfg = config.get("deposit", {})
    wallet = config["wallet_address"]
    safe = config.get("safe_address", wallet)
    strategy_id = config.get("strategy_id", "defai-vault-lp")
    usdc_token = config["usdc_token"]
    almanak_token = config["almanak_token"]

    def round_1(ctx):
        """Phase 1: Market assessment -- load state, get price, get balances."""
        return _resp(
            _tc("load_agent_state", {"strategy_id": strategy_id}),
            _tc("get_price", {"token": "ALMANAK", "chain": "base"}),
            _tc("get_balance", {"token": "USDC", "chain": "base"}),
            _tc("get_balance", {"token": "ALMANAK", "chain": "base"}),
        )

    def round_2(ctx):
        """Phase 2: Deploy vault (EOA signs factory call).

        admin_address MUST be the Safe so settleDeposit() (which checks
        msg.sender == owner) can be called from the Safe.  The valuator
        role is set separately so propose still works from the EOA.
        """
        return _resp(
            _tc("deploy_vault", {
                "chain": "base",
                "name": vault_cfg.get("name", "Almanak DeFAI Vault"),
                "symbol": vault_cfg.get("symbol", "aALM"),
                "underlying_token_address": vault_cfg.get("underlying_token", usdc_token),
                "safe_address": safe,
                "admin_address": safe,
                "fee_receiver_address": wallet,
                "deployer_address": wallet,
                "valuation_manager_address": wallet,
            }),
        )

    def round_3(ctx):
        """Phase 3: Approve vault underlying (Safe signs ERC20 approve)."""
        va = ctx.get("vault_address", "0x0000000000000000000000000000000000000001")
        return _resp(
            _tc("approve_vault_underlying", {
                "vault_address": va,
                "underlying_token": usdc_token,
                "safe_address": safe,
                "chain": "base",
            }),
        )

    def round_4(ctx):
        """Phase 4: Initial settlement (V0.5.0 init -- total_assets=0)."""
        va = ctx.get("vault_address", "0x0000000000000000000000000000000000000001")
        return _resp(
            _tc("settle_vault", {
                "vault_address": va,
                "chain": "base",
                "safe_address": safe,
                "valuator_address": wallet,
                "new_total_assets": "0",
            }),
        )

    def round_5(ctx):
        """Phase 5: Deposit USDC into vault (EOA: approve + requestDeposit)."""
        va = ctx.get("vault_address", "0x0000000000000000000000000000000000000001")
        deposit_amount = deposit_cfg.get("amount_usdc_raw", "10000000")
        return _resp(
            _tc("deposit_vault", {
                "vault_address": va,
                "underlying_token": usdc_token,
                "amount": deposit_amount,
                "chain": "base",
                "depositor_address": wallet,
            }),
        )

    def round_6(ctx):
        """Phase 6: Process deposits -- settle to mint shares and move USDC to Safe."""
        va = ctx.get("vault_address", "0x0000000000000000000000000000000000000001")
        return _resp(
            _tc("settle_vault", {
                "vault_address": va,
                "chain": "base",
                "safe_address": safe,
                "valuator_address": wallet,
            }),
        )

    def round_7(ctx):
        """Phase 7: Discover pool price for LP range calculation."""
        return _resp(
            _tc("get_pool_state", {
                "token_a": almanak_token,
                "token_b": usdc_token,
                "fee_tier": 3000,
                "chain": "base",
                "protocol": "uniswap_v3",
                "pool_address": config.get("pool_address", ""),
            }),
        )

    def round_8(ctx):
        """Phase 8: Open LP position using Safe's funds (execution_wallet=Safe).

        Uses current_price from get_pool_state to compute a centered range.
        """
        range_width = float(lp_cfg.get("range_width_pct", "0.50"))
        current_price = ctx.get("current_price", 0.002)
        price_lower = str(current_price * (1 - range_width / 2))
        price_upper = str(current_price * (1 + range_width / 2))
        return _resp(
            _tc("open_lp_position", {
                "token_a": almanak_token,
                "token_b": usdc_token,
                "amount_a": lp_cfg.get("amount_almanak", "1000"),
                "amount_b": lp_cfg.get("amount_usdc", "10"),
                "price_lower": price_lower,
                "price_upper": price_upper,
                "fee_tier": 3000,
                "protocol": "uniswap_v3",
                "chain": "base",
                "execution_wallet": safe,
            }),
        )

    def round_9(ctx):
        """Phase 9: NAV settlement -- report LP value as new total assets."""
        va = ctx.get("vault_address", "0x0000000000000000000000000000000000000001")
        return _resp(
            _tc("settle_vault", {
                "vault_address": va,
                "chain": "base",
                "safe_address": safe,
                "valuator_address": wallet,
                "new_total_assets": "10000000",
            }),
        )

    def round_10(ctx):
        """Phase 10: Save state + record agent decision."""
        va = ctx.get("vault_address", "0x0000000000000000000000000000000000000001")
        return _resp(
            _tc("save_agent_state", {
                "strategy_id": strategy_id,
                "state": {
                    "vault_address": va,
                    "lp_position_id": ctx.get("position_id"),
                    "pool": config["pool"],
                    "phase": "complete",
                },
            }),
            _tc("record_agent_decision", {
                "strategy_id": strategy_id,
                "decision_summary": (
                    f"DeFAI lifecycle complete: deployed vault at {va}, "
                    "approved underlying, initialized, deposited USDC, "
                    "opened ALMANAK/USDC LP via Safe, settled NAV."
                ),
            }),
        )

    def round_11(ctx):
        """Phase 11: Final text summary."""
        va = ctx.get("vault_address", "unknown")
        return _resp(
            content=(
                f"DeFAI vault lifecycle complete.\n"
                f"- Vault: {va}\n"
                f"- Safe: {safe}\n"
                f"- LP opened on ALMANAK/USDC Uniswap V3 (Base)\n"
                f"- All settlements processed, state persisted."
            ),
        )

    return DynamicMockLLMClient([
        round_1, round_2, round_3, round_4, round_5,
        round_6, round_7, round_8, round_9, round_10, round_11,
    ])


def create_rebalance_mock_llm(config: dict, state: dict) -> DynamicMockLLMClient:
    """Create a mock LLM for the RUNNING mode rebalance scenario (7 rounds).

    Simulates: load state -> assess position (out of range) -> viability check ->
    close LP -> open LP with new range -> settle + save state -> summary.
    """
    wallet = config["wallet_address"]
    safe = config.get("safe_address", wallet)
    strategy_id = config.get("strategy_id", "defai-vault-lp")
    usdc_token = config["usdc_token"]
    almanak_token = config["almanak_token"]
    lp_cfg = config.get("lp", {})
    vault_address = state.get("vault_address", "0xVAULT")
    position_id = state.get("position_id", 12345)

    def round_1(ctx):
        """Load state + assess position and pool."""
        return _resp(
            _tc("load_agent_state", {"strategy_id": strategy_id}),
            _tc("get_vault_state", {"vault_address": vault_address, "chain": "base"}),
            _tc("get_pool_state", {
                "token_a": almanak_token,
                "token_b": usdc_token,
                "fee_tier": 3000,
                "chain": "base",
                "protocol": "uniswap_v3",
                "pool_address": config.get("pool_address", ""),
            }),
            _tc("get_lp_position", {
                "position_id": str(position_id),
                "chain": "base",
                "protocol": "uniswap_v3",
            }),
        )

    def round_2(ctx):
        """Viability check -- compute_rebalance_candidate."""
        return _resp(
            _tc("compute_rebalance_candidate", {
                "position_id": str(position_id),
                "chain": "base",
                "fee_tier": 3000,
            }),
        )

    def round_3(ctx):
        """Close existing LP position."""
        return _resp(
            _tc("close_lp_position", {
                "position_id": str(position_id),
                "chain": "base",
                "protocol": "uniswap_v3",
                "execution_wallet": safe,
            }),
        )

    def round_4(ctx):
        """Open new LP with recentered range."""
        # Use a reasonable default price if pool state wasn't captured
        current_price = float(ctx.get("current_price", 500))
        range_width = float(lp_cfg.get("range_width_pct", "0.50"))
        price_lower = str(round(current_price * (1 - range_width / 2), 10))
        price_upper = str(round(current_price * (1 + range_width / 2), 10))
        return _resp(
            _tc("open_lp_position", {
                "token_a": almanak_token,
                "token_b": usdc_token,
                "amount_a": lp_cfg.get("amount_almanak", "100"),
                "amount_b": lp_cfg.get("amount_usdc", "10"),
                "price_lower": price_lower,
                "price_upper": price_upper,
                "fee_tier": 3000,
                "protocol": "uniswap_v3",
                "chain": "base",
                "execution_wallet": safe,
            }),
        )

    def round_5(ctx):
        """Settle vault with updated NAV."""
        return _resp(
            _tc("settle_vault", {
                "vault_address": vault_address,
                "chain": "base",
                "safe_address": safe,
                "valuator_address": wallet,
                "new_total_assets": "10000000",
            }),
        )

    def round_6(ctx):
        """Save state + record decision."""
        new_pos = ctx.get("position_id", position_id)
        return _resp(
            _tc("save_agent_state", {
                "strategy_id": strategy_id,
                "state": {
                    "vault_address": vault_address,
                    "lp_position_id": new_pos,
                    "pool": config["pool"],
                    "phase": "running",
                    "last_rebalance_timestamp": "now",
                },
            }),
            _tc("record_agent_decision", {
                "strategy_id": strategy_id,
                "decision_summary": (
                    f"Rebalanced LP position: closed #{position_id}, "
                    f"opened new position centered on current price."
                ),
            }),
        )

    def round_7(ctx):
        """Final text summary."""
        return _resp(
            content=(
                f"Rebalance complete.\n"
                f"- Closed out-of-range position #{position_id}\n"
                f"- Opened new position centered on current pool price\n"
                f"- Vault NAV settled, state persisted."
            ),
        )

    return DynamicMockLLMClient([
        round_1, round_2, round_3, round_4, round_5, round_6, round_7,
    ])


def create_teardown_mock_llm(config: dict, state: dict) -> DynamicMockLLMClient:
    """Create a mock LLM for the RUNNING mode teardown scenario (8 rounds).

    Simulates: load state (teardown_requested) -> close LP -> swap to USDC ->
    settle -> check pending -> settle again (loop) -> save state -> summary.
    """
    wallet = config["wallet_address"]
    safe = config.get("safe_address", wallet)
    strategy_id = config.get("strategy_id", "defai-vault-lp")
    usdc_token = config["usdc_token"]
    almanak_token = config["almanak_token"]
    vault_address = state.get("vault_address", "0xVAULT")
    position_id = state.get("position_id", 12345)

    def round_1(ctx):
        """Load state (teardown_requested=true) + get vault state."""
        return _resp(
            _tc("load_agent_state", {"strategy_id": strategy_id}),
            _tc("get_vault_state", {"vault_address": vault_address, "chain": "base"}),
        )

    def round_2(ctx):
        """Close LP position."""
        return _resp(
            _tc("close_lp_position", {
                "position_id": str(position_id),
                "chain": "base",
                "protocol": "uniswap_v3",
                "execution_wallet": safe,
            }),
        )

    def round_3(ctx):
        """Swap non-USDC tokens to USDC."""
        # Use balance from close_lp result or a reasonable default
        almanak_amount = ctx.get("balance_ALMANAK", "100")
        return _resp(
            _tc("swap_tokens", {
                "token_in": almanak_token,
                "token_out": usdc_token,
                "amount": str(almanak_amount),
                "chain": "base",
                "protocol": "uniswap_v3",
                "execution_wallet": safe,
            }),
        )

    def round_4(ctx):
        """First settle_vault."""
        return _resp(
            _tc("settle_vault", {
                "vault_address": vault_address,
                "chain": "base",
                "safe_address": safe,
                "valuator_address": wallet,
                "new_total_assets": "10000000",
            }),
        )

    def round_5(ctx):
        """Check vault state for pending redeems."""
        return _resp(
            _tc("get_vault_state", {"vault_address": vault_address, "chain": "base"}),
        )

    def round_6(ctx):
        """Second settle_vault (settle loop -- clear remaining pending_redeems)."""
        return _resp(
            _tc("settle_vault", {
                "vault_address": vault_address,
                "chain": "base",
                "safe_address": safe,
                "valuator_address": wallet,
            }),
        )

    def round_7(ctx):
        """Save state + record decision."""
        return _resp(
            _tc("save_agent_state", {
                "strategy_id": strategy_id,
                "state": {
                    "vault_address": vault_address,
                    "phase": "torn_down",
                    "teardown_requested": False,
                },
            }),
            _tc("record_agent_decision", {
                "strategy_id": strategy_id,
                "decision_summary": (
                    f"Teardown complete: closed LP #{position_id}, "
                    "swapped all to USDC, settled vault twice to clear pending redeems."
                ),
            }),
        )

    def round_8(ctx):
        """Final text summary."""
        return _resp(
            content=(
                f"Teardown complete.\n"
                f"- Closed LP position #{position_id}\n"
                f"- Swapped all tokens to USDC\n"
                f"- Vault settled (2 rounds to clear pending redeems)\n"
                f"- State saved with phase=torn_down."
            ),
        )

    return DynamicMockLLMClient([
        round_1, round_2, round_3, round_4, round_5, round_6, round_7, round_8,
    ])


async def run_once(config: dict, *, use_mock: bool = False, scenario: str = "init") -> None:
    """Run a single iteration of the DeFAI agent."""
    # 1. Connect to gateway
    gw_config = GatewayClientConfig.from_env()
    gateway = GatewayClient(gw_config)
    gateway.connect()

    try:
        if not gateway.wait_for_ready(timeout=15.0):
            logger.error("Gateway not ready -- is it running? (almanak gateway --network anvil)")
            return

        logger.info("Gateway connected at %s:%d", gw_config.host, gw_config.port)

        # 2. Create executor with policy and alerting
        from almanak.framework.alerting.gateway_alert_manager import GatewayAlertManager

        policy = create_policy(config)
        catalog = get_default_catalog()
        strategy_id = config.get("strategy_id", "defai-vault-lp")
        alert_manager = GatewayAlertManager(gateway, strategy_id=strategy_id)
        executor = ToolExecutor(
            gateway,
            policy=policy,
            catalog=catalog,
            wallet_address=config.get("wallet_address", ""),
            strategy_id=strategy_id,
            default_chain=config.get("chain", "base"),
            alert_manager=alert_manager,
        )

        # 3. Get OpenAI tool definitions from catalog
        tools_openai = catalog.to_openai_tools()

        # 4. Create LLM client
        if use_mock:
            if scenario == "rebalance":
                mock_state = {
                    "vault_address": "0xMOCK_VAULT",
                    "position_id": 99999,
                    "pool": config["pool"],
                    "phase": "running",
                }
                llm = create_rebalance_mock_llm(config, mock_state)
                logger.info("LLM: DynamicMockLLMClient (7-round rebalance)")
            elif scenario == "teardown":
                mock_state = {
                    "vault_address": "0xMOCK_VAULT",
                    "position_id": 99999,
                    "pool": config["pool"],
                    "phase": "running",
                    "teardown_requested": True,
                }
                llm = create_teardown_mock_llm(config, mock_state)
                logger.info("LLM: DynamicMockLLMClient (8-round teardown)")
            else:
                llm = create_dynamic_mock_llm(config)
                logger.info("LLM: DynamicMockLLMClient (11-round vault lifecycle)")
        else:
            llm_config = LLMConfig.from_env()
            if not llm_config.api_key:
                logger.error("Set AGENT_LLM_API_KEY environment variable")
                return
            llm = LLMClient(llm_config)
            logger.info("LLM: %s via %s", llm_config.model, llm_config.base_url)

        # 5. Determine mode: init vs running
        from defai_vault_lp.prompts import (  # noqa: E402
            INIT_USER_PROMPT,
            RUNNING_USER_PROMPT,
            build_system_prompt,
        )

        mode = "init"
        agent_state = None

        # Mock scenarios override mode
        if use_mock and scenario in ("rebalance", "teardown"):
            mode = "running"
            agent_state = mock_state
            logger.info("Mock scenario '%s' -> RUNNING mode", scenario)
        elif not use_mock:
            # For real LLM: try loading persisted state to detect mode
            try:
                state_resp = await executor.execute("load_agent_state", {
                    "strategy_id": config.get("strategy_id", "defai-vault-lp"),
                })
                if state_resp.status == "success" and state_resp.data:
                    saved = state_resp.data.get("state", {})
                    va = saved.get("vault_address", "")
                    if va and not va.startswith("0x000000000000"):
                        # Verify vault exists on-chain before trusting saved state
                        vault_check = await executor.execute(
                            "get_vault_state",
                            {"vault_address": va, "chain": config.get("chain", "base")},
                        )
                        if vault_check.status == "success":
                            mode = "running"
                            agent_state = saved
                            logger.info("Resuming in RUNNING mode (vault=%s)", va)
                        else:
                            logger.warning(
                                "Saved vault %s not found on-chain; starting in INIT mode", va
                            )
            except Exception:
                logger.debug("No persisted state found, starting in INIT mode")

        system_prompt = build_system_prompt(config, mode=mode, state=agent_state)
        user_prompt = RUNNING_USER_PROMPT if mode == "running" else INIT_USER_PROMPT

        # 6. Run agent loop with iteration timeout
        iteration_timeout = config.get("iteration_timeout_seconds", 300)
        try:
            result = await asyncio.wait_for(
                run_agent_loop(
                    llm_client=llm,
                    executor=executor,
                    tools_openai=tools_openai,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_rounds=config.get("max_tool_rounds", 15),
                ),
                timeout=iteration_timeout,
            )
            logger.info("Agent result: %s", result)
        except TimeoutError:
            logger.error("Agent iteration timed out after %ds", iteration_timeout)
        finally:
            if hasattr(llm, "close"):
                await llm.close()
    finally:
        gateway.disconnect()


async def run_loop(config: dict, *, use_mock: bool = False, scenario: str = "init") -> None:
    """Run the agent in a loop with configurable interval."""
    interval = config.get("interval_seconds", 120)
    logger.info("Starting agent loop (interval=%ds, Ctrl+C to stop)", interval)

    while True:
        try:
            await run_once(config, use_mock=use_mock, scenario=scenario)
        except Exception:
            logger.exception("Agent iteration failed")
        logger.info("Sleeping %ds until next iteration...", interval)
        await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="DeFAI Vault + LP Agent")
    parser.add_argument("--once", action="store_true", help="Run a single iteration then exit")
    parser.add_argument("--mock", action="store_true", help="Use mock LLM (no API key needed)")
    parser.add_argument(
        "--scenario",
        choices=["init", "rebalance", "teardown"],
        default="init",
        help="Mock scenario: init (default), rebalance, or teardown",
    )
    args = parser.parse_args()

    config = load_config()
    logger.info(
        "DeFAI agent starting: pool=%s chain=%s vault=%s scenario=%s",
        config["pool"],
        config["chain"],
        config.get("vault", {}).get("name", "?"),
        args.scenario,
    )

    if args.once:
        asyncio.run(run_once(config, use_mock=args.mock, scenario=args.scenario))
    else:
        asyncio.run(run_loop(config, use_mock=args.mock, scenario=args.scenario))


if __name__ == "__main__":
    main()
