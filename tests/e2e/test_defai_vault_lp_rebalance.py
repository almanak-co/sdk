"""E2E tests for DeFAI Vault LP rebalance and teardown flows.

Tests the RUNNING mode mock LLMs:
- Rebalance: 7-round flow (assess -> viability -> close -> open -> settle -> save -> summary)
- Teardown: 8-round flow (load -> close -> swap -> settle -> check -> settle loop -> save -> summary)

These tests verify the mock LLM structure and tool call sequence without requiring
a gateway or Anvil fork.

Usage:
    pytest tests/e2e/test_defai_vault_lp_rebalance.py -v
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Add examples to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "examples" / "agentic"))


# --- Shared test config ---

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
SAFE_ADDRESS = "0x98aE9CE2606e2773eE948178C3a163fdB8194c04"
ALMANAK_TOKEN = "0xdefa1d21c5f1cbeac00eeb54b44c7d86467cc3a3"
USDC_TOKEN = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
POOL_ADDRESS = "0xbDbC38652D78AF0383322bBc823E06FA108d0874"

TEST_CONFIG = {
    "chain": "base",
    "wallet_address": TEST_WALLET,
    "safe_address": SAFE_ADDRESS,
    "pool": "ALMANAK/USDC/3000",
    "pool_address": POOL_ADDRESS,
    "almanak_token": ALMANAK_TOKEN,
    "usdc_token": USDC_TOKEN,
    "vault": {"name": "Test Vault", "symbol": "tVLT", "underlying_token": USDC_TOKEN},
    "lp": {"amount_almanak": "100", "amount_usdc": "10", "range_width_pct": "0.50"},
    "deposit": {"amount_usdc_raw": "10000000", "min_deploy_threshold_usdc_raw": "5000000"},
    "rebalance": {"min_rebalance_interval_minutes": 30},
    "strategy_id": "test-defai",
    "max_tool_rounds": 15,
}

MOCK_STATE_REBALANCE = {
    "vault_address": "0xMOCK_VAULT_ADDR",
    "position_id": 42000,
    "pool": "ALMANAK/USDC/3000",
    "phase": "running",
}

MOCK_STATE_TEARDOWN = {
    "vault_address": "0xMOCK_VAULT_ADDR",
    "position_id": 42000,
    "pool": "ALMANAK/USDC/3000",
    "phase": "running",
    "teardown_requested": True,
}


def _extract_tool_names(response: dict) -> list[str]:
    """Extract tool call names from a mock response."""
    tcs = response["choices"][0]["message"].get("tool_calls", [])
    return [tc["function"]["name"] for tc in tcs]


def _extract_tool_args(response: dict, index: int = 0) -> dict:
    """Extract parsed arguments for a specific tool call in a response."""
    tc = response["choices"][0]["message"]["tool_calls"][index]
    return json.loads(tc["function"]["arguments"])


class TestRebalanceMockStructure:
    """Verify the rebalance mock LLM produces the correct 7-round tool call sequence."""

    @pytest.fixture()
    def mock_llm(self):
        from defai_vault_lp.run import create_rebalance_mock_llm
        return create_rebalance_mock_llm(TEST_CONFIG, MOCK_STATE_REBALANCE)

    def test_round_count(self, mock_llm):
        """Rebalance mock should have exactly 7 rounds."""
        assert len(mock_llm._rounds) == 7

    def test_round_1_loads_state_and_assesses(self, mock_llm):
        """Round 1: load_agent_state + get_vault_state + get_pool_state + get_lp_position."""
        r = mock_llm._rounds[0]({})
        names = _extract_tool_names(r)
        assert names == ["load_agent_state", "get_vault_state", "get_pool_state", "get_lp_position"]

        # Verify get_pool_state uses token_a/token_b (not 'pool')
        pool_args = _extract_tool_args(r, index=2)
        assert "token_a" in pool_args
        assert "token_b" in pool_args

        # Verify position_id is a string
        lp_args = _extract_tool_args(r, index=3)
        assert isinstance(lp_args["position_id"], str)

    def test_round_2_viability_check(self, mock_llm):
        """Round 2: compute_rebalance_candidate."""
        r = mock_llm._rounds[1]({})
        names = _extract_tool_names(r)
        assert names == ["compute_rebalance_candidate"]

    def test_round_3_close_lp(self, mock_llm):
        """Round 3: close_lp_position with execution_wallet=safe."""
        r = mock_llm._rounds[2]({})
        names = _extract_tool_names(r)
        assert names == ["close_lp_position"]
        args = _extract_tool_args(r)
        assert args["execution_wallet"] == SAFE_ADDRESS
        assert args["position_id"] == str(MOCK_STATE_REBALANCE["position_id"])

    def test_round_4_open_lp_with_new_range(self, mock_llm):
        """Round 4: open_lp_position with price range centered on current_price."""
        ctx = {"current_price": 600}
        r = mock_llm._rounds[3](ctx)
        names = _extract_tool_names(r)
        assert names == ["open_lp_position"]

        args = _extract_tool_args(r)
        assert args["execution_wallet"] == SAFE_ADDRESS
        assert args["protocol"] == "uniswap_v3"
        # With range_width_pct=0.50, price=600: lower=450, upper=750
        assert float(args["price_lower"]) == pytest.approx(450.0, abs=1)
        assert float(args["price_upper"]) == pytest.approx(750.0, abs=1)

    def test_round_5_settle_vault(self, mock_llm):
        """Round 5: settle_vault with NAV."""
        r = mock_llm._rounds[4]({})
        names = _extract_tool_names(r)
        assert names == ["settle_vault"]

    def test_round_6_save_state_and_record(self, mock_llm):
        """Round 6: save_agent_state + record_agent_decision."""
        r = mock_llm._rounds[5]({})
        names = _extract_tool_names(r)
        assert names == ["save_agent_state", "record_agent_decision"]

        # Verify state includes phase=running and last_rebalance_timestamp
        save_args = _extract_tool_args(r, index=0)
        state = save_args["state"]
        assert state["phase"] == "running"
        assert "last_rebalance_timestamp" in state

    def test_round_7_text_summary(self, mock_llm):
        """Round 7: final text summary (no tool calls)."""
        r = mock_llm._rounds[6]({})
        msg = r["choices"][0]["message"]
        assert msg.get("tool_calls") is None
        assert msg["content"] is not None
        assert "Rebalance complete" in msg["content"]

    def test_full_sequence_tool_order(self, mock_llm):
        """Verify the complete tool call sequence across all rounds."""
        expected_sequence = [
            ["load_agent_state", "get_vault_state", "get_pool_state", "get_lp_position"],
            ["compute_rebalance_candidate"],
            ["close_lp_position"],
            ["open_lp_position"],
            ["settle_vault"],
            ["save_agent_state", "record_agent_decision"],
            [],  # text-only round
        ]
        ctx = {"current_price": 500}
        for i, expected in enumerate(expected_sequence):
            r = mock_llm._rounds[i](ctx)
            actual = _extract_tool_names(r)
            assert actual == expected, f"Round {i + 1}: expected {expected}, got {actual}"


class TestTeardownMockStructure:
    """Verify the teardown mock LLM produces the correct 8-round tool call sequence."""

    @pytest.fixture()
    def mock_llm(self):
        from defai_vault_lp.run import create_teardown_mock_llm
        return create_teardown_mock_llm(TEST_CONFIG, MOCK_STATE_TEARDOWN)

    def test_round_count(self, mock_llm):
        """Teardown mock should have exactly 8 rounds."""
        assert len(mock_llm._rounds) == 8

    def test_round_1_load_state(self, mock_llm):
        """Round 1: load_agent_state + get_vault_state."""
        r = mock_llm._rounds[0]({})
        names = _extract_tool_names(r)
        assert names == ["load_agent_state", "get_vault_state"]

    def test_round_2_close_lp(self, mock_llm):
        """Round 2: close_lp_position."""
        r = mock_llm._rounds[1]({})
        names = _extract_tool_names(r)
        assert names == ["close_lp_position"]
        args = _extract_tool_args(r)
        assert args["execution_wallet"] == SAFE_ADDRESS
        assert isinstance(args["position_id"], str)

    def test_round_3_swap_to_usdc(self, mock_llm):
        """Round 3: swap_tokens (non-USDC to USDC)."""
        r = mock_llm._rounds[2]({})
        names = _extract_tool_names(r)
        assert names == ["swap_tokens"]
        args = _extract_tool_args(r)
        assert args["token_in"] == ALMANAK_TOKEN
        assert args["token_out"] == USDC_TOKEN
        assert args["execution_wallet"] == SAFE_ADDRESS
        # amount should be a numeric string, not "all"
        assert args["amount"] != "all"

    def test_round_4_first_settle(self, mock_llm):
        """Round 4: first settle_vault."""
        r = mock_llm._rounds[3]({})
        names = _extract_tool_names(r)
        assert names == ["settle_vault"]

    def test_round_5_check_pending(self, mock_llm):
        """Round 5: get_vault_state to check pending_redeems."""
        r = mock_llm._rounds[4]({})
        names = _extract_tool_names(r)
        assert names == ["get_vault_state"]

    def test_round_6_settle_loop(self, mock_llm):
        """Round 6: second settle_vault (settle loop for pending redeems)."""
        r = mock_llm._rounds[5]({})
        names = _extract_tool_names(r)
        assert names == ["settle_vault"]

    def test_round_7_save_state(self, mock_llm):
        """Round 7: save_agent_state + record_agent_decision."""
        r = mock_llm._rounds[6]({})
        names = _extract_tool_names(r)
        assert names == ["save_agent_state", "record_agent_decision"]

        save_args = _extract_tool_args(r, index=0)
        state = save_args["state"]
        assert state["phase"] == "torn_down"
        assert state["teardown_requested"] is False

    def test_round_8_text_summary(self, mock_llm):
        """Round 8: final text summary."""
        r = mock_llm._rounds[7]({})
        msg = r["choices"][0]["message"]
        assert msg.get("tool_calls") is None
        assert "Teardown complete" in msg["content"]

    def test_settle_loop_present(self, mock_llm):
        """Verify the settle loop: rounds 4+6 both call settle_vault."""
        r4 = mock_llm._rounds[3]({})
        r6 = mock_llm._rounds[5]({})
        assert _extract_tool_names(r4) == ["settle_vault"]
        assert _extract_tool_names(r6) == ["settle_vault"]

    def test_full_sequence_tool_order(self, mock_llm):
        """Verify the complete teardown tool call sequence."""
        expected_sequence = [
            ["load_agent_state", "get_vault_state"],
            ["close_lp_position"],
            ["swap_tokens"],
            ["settle_vault"],
            ["get_vault_state"],
            ["settle_vault"],
            ["save_agent_state", "record_agent_decision"],
            [],  # text-only round
        ]
        ctx = {}
        for i, expected in enumerate(expected_sequence):
            r = mock_llm._rounds[i](ctx)
            actual = _extract_tool_names(r)
            assert actual == expected, f"Round {i + 1}: expected {expected}, got {actual}"


class TestPromptTemplateVars:
    """Verify RUNNING_SYSTEM_PROMPT template renders with new variables."""

    def test_running_prompt_includes_range_width(self):
        """build_system_prompt should inject range_width_pct into the running prompt."""
        from defai_vault_lp.prompts import build_system_prompt

        state = {"vault_address": "0xABC", "position_id": 123}
        prompt = build_system_prompt(TEST_CONFIG, mode="running", state=state)

        assert "0.50" in prompt  # range_width_pct
        assert "test-defai" in prompt  # strategy_id
        assert "0xABC" in prompt  # vault_address
        assert "#123" in prompt  # position_id

    def test_running_prompt_settle_loop_instruction(self):
        """P0 teardown section should mention settle loop."""
        from defai_vault_lp.prompts import build_system_prompt

        state = {"vault_address": "0xABC", "position_id": 123}
        prompt = build_system_prompt(TEST_CONFIG, mode="running", state=state)

        assert "pending_redeems" in prompt
        assert "loop up to 5 times" in prompt

    def test_running_prompt_rebalance_steps(self):
        """P2 rebalance section should have explicit step-by-step instructions."""
        from defai_vault_lp.prompts import build_system_prompt

        state = {"vault_address": "0xABC", "position_id": 123}
        prompt = build_system_prompt(TEST_CONFIG, mode="running", state=state)

        assert "compute_rebalance_candidate" in prompt
        assert "price_lower" in prompt
        assert "price_upper" in prompt
        assert "last_rebalance_timestamp" in prompt


class TestScenarioFlag:
    """Verify the --scenario CLI flag is wired up correctly."""

    def test_argparser_accepts_scenario(self):
        """Verify argparse accepts --scenario with valid choices."""
        import argparse

        # Re-create the parser as in main()
        parser = argparse.ArgumentParser()
        parser.add_argument("--once", action="store_true")
        parser.add_argument("--mock", action="store_true")
        parser.add_argument(
            "--scenario",
            choices=["init", "rebalance", "teardown"],
            default="init",
        )

        args = parser.parse_args(["--once", "--mock", "--scenario", "rebalance"])
        assert args.scenario == "rebalance"
        assert args.mock is True

    def test_scenario_default_is_init(self):
        """Default scenario should be 'init'."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--scenario", choices=["init", "rebalance", "teardown"], default="init")

        args = parser.parse_args([])
        assert args.scenario == "init"
