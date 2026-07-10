"""VIB-5681 — one settlement implementation (single-writer invariant).

The runner's ``VaultLifecycleManager`` (``almanak/framework/vault/lifecycle.py``)
is the *only* vault settlement implementation. The executor-private
``settle_vault`` / ``_vault_settlement`` state machine that used to live in
``almanak/framework/agent_tools/executor.py`` was deleted: two implementations
driving one on-chain vault nonce/phase is the bug class the ratified vault
design eliminates (ship gate #2).

This module pins that outcome three ways:

* ``TestSettleVaultRefusal`` — the ``settle_vault`` agent-tool surface still
  EXISTS (so an LLM/tool call does not get an unknown-tool failure) but returns
  a typed, stable refusal directing the operator to the runner-owned path.
* ``TestSettlementStateMachineNotReintroduced`` — a static source guard, in the
  house style of ``tests/unit/teardown/test_teardown_accounting_anti_bypass.py``:
  the deleted settlement machinery must not reappear anywhere under
  ``almanak/framework/agent_tools/``.
* ``TestPolicyWiringUnchanged`` — deleting settlement must not weaken the
  PolicyEngine gate on the remaining mutating tools.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.agent_tools.errors import AgentErrorCode, ErrorCategory
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy

AGENT_TOOLS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "framework" / "agent_tools"


@pytest.fixture
def mock_gateway():
    client = MagicMock()
    client.is_connected = True
    return client


def _permissive_executor(mock_gateway) -> ToolExecutor:
    policy = AgentPolicy(
        allowed_chains={"base"},
        max_tool_calls_per_minute=100,
        cooldown_seconds=0,
        max_single_trade_usd=Decimal("999999999"),
        max_daily_spend_usd=Decimal("999999999"),
    )
    return ToolExecutor(
        mock_gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-strategy",
        default_chain="base",
    )


class TestSettleVaultRefusal:
    """The tool surface stays; it returns a typed refusal, not settlement."""

    @pytest.mark.asyncio
    async def test_settle_vault_returns_typed_refusal(self, mock_gateway):
        executor = _permissive_executor(mock_gateway)
        result = await executor.execute(
            "settle_vault",
            {
                "vault_address": "0x" + "a" * 40,
                "safe_address": "0x" + "b" * 40,
                "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
            },
        )
        assert result.status == "error"
        assert result.error["error_code"] == AgentErrorCode.VAULT_SETTLEMENT_UNSUPPORTED.value
        # Requires a human/runner action — not a retry, not a reconfig.
        assert result.error["error_category"] == ErrorCategory.REQUIRES_HUMAN.value
        assert result.error["recoverable"] is False
        # The message must name the canonical owner so operators know where to go.
        assert "VaultLifecycleManager" in result.error["message"]
        assert "ax vault" in result.error["message"]

    @pytest.mark.asyncio
    async def test_settle_vault_does_not_touch_gateway_execution(self, mock_gateway):
        """The refusal must short-circuit before any on-chain submission."""
        executor = _permissive_executor(mock_gateway)
        await executor.execute(
            "settle_vault",
            {
                "vault_address": "0x" + "a" * 40,
                "safe_address": "0x" + "b" * 40,
                "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
            },
        )
        mock_gateway.execution.Execute.assert_not_called()
        mock_gateway.state.SaveState.assert_not_called()

    def test_settle_vault_still_registered_in_catalog(self):
        """The tool must remain in the catalog so tool-calls resolve it."""
        from almanak.framework.agent_tools.catalog import get_default_catalog

        assert get_default_catalog().get("settle_vault") is not None


class TestSettlementStateMachineNotReintroduced:
    """Static guard: the deleted settlement machine must not come back.

    Reads the ``agent_tools`` source directly (lint-speed, no fixtures) and
    fails if any token unique to the executor-private settlement state machine
    reappears. Re-introducing a second settlement writer must trip this before
    it ships.
    """

    # Tokens that only existed as part of the deleted settlement machine.
    # (The public tool name ``settle_vault`` is intentionally NOT here — the
    # tool surface legitimately survives as the typed refusal.)
    FORBIDDEN_TOKENS = (
        # deleted methods
        "_execute_settle_vault",
        "_compute_vault_nav",
        "_do_settle_deposit_and_redeem",
        "_finalize_executor_settlement",
        "_save_settlement_state",
        "_load_settlement_state",
        "_check_settlement_liquidity",
        "_vault_preflight_checks",
        "_determine_nav",
        # deleted crash-recovery instance state + agent-state key
        '"_vault_settlement"',
        "_settlement_phase",
        "_settlement_proposed_assets",
        "_settlement_nonce",
        "_vault_epoch_counter",
        # deleted settlement bundle builders / params (adapter-driven machine)
        "build_propose_valuation_bundle",
        "build_settle_deposit_bundle",
        "build_settle_redeem_bundle",
        "UpdateTotalAssetsParams",
        "SettleDepositParams",
        "SettleRedeemParams",
        # raw Lagoon settlement selectors (direct-call machinery)
        "updateNewTotalAssets",
        "settleDeposit",
        "settleRedeem",
        # teardown_vault (or anything else) sub-calling the settlement tool
        'execute("settle_vault"',
        "execute('settle_vault'",
    )

    def test_no_settlement_machine_tokens_under_agent_tools(self):
        offenders: dict[str, list[str]] = {}
        for path in AGENT_TOOLS_DIR.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            for token in self.FORBIDDEN_TOKENS:
                if token in text:
                    offenders.setdefault(token, []).append(path.name)
        assert not offenders, (
            "Executor-private vault settlement machinery reintroduced under "
            f"almanak/framework/agent_tools/ (VIB-5681 single-writer invariant): {offenders}. "
            "Settlement is owned solely by the runner's VaultLifecycleManager."
        )

    def test_settle_vault_dispatch_routes_to_refusal(self):
        """The dispatch branch must call the refusal, not an executor state machine."""
        src = (AGENT_TOOLS_DIR / "executor.py").read_text(encoding="utf-8")
        assert 'if tool_name == "settle_vault":' in src
        assert "_settlement_owned_by_runner" in src


class TestPolicyWiringUnchanged:
    """Deleting settlement must not weaken PolicyEngine gating elsewhere."""

    @pytest.mark.asyncio
    async def test_mutating_tool_still_policy_gated(self, mock_gateway):
        """A mutating tool on a disallowed chain is still blocked by policy."""
        policy = AgentPolicy(
            allowed_chains={"base"},
            cooldown_seconds=0,
            max_tool_calls_per_minute=100,
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            deployment_id="test-strategy",
            default_chain="base",
        )
        result = await executor.execute(
            "swap_tokens",
            {
                "token_in": "USDC",
                "token_out": "WETH",
                "amount": "100",
                "chain": "ethereum",  # not in allowed_chains
            },
        )
        assert result.status == "error"
        assert result.error["error_code"] == AgentErrorCode.RISK_BLOCKED.value
        # Policy blocked before any execution submission.
        mock_gateway.execution.Execute.assert_not_called()
