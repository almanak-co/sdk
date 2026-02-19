"""Tests for vault teardown tool."""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponse


@pytest.fixture
def mock_gateway():
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def executor(mock_gateway):
    policy = AgentPolicy(
        allowed_chains={"base"},
        max_tool_calls_per_minute=200,
        cooldown_seconds=0,
        max_single_trade_usd=Decimal("999999999"),
        max_daily_spend_usd=Decimal("999999999"),
        max_position_size_usd=Decimal("999999999"),
        require_human_approval_above_usd=Decimal("999999999"),
        require_rebalance_check=False,
    )
    return ToolExecutor(
        mock_gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        strategy_id="test-strategy",
        default_chain="base",
    )


def _mock_lagoon_sdk():
    """Create a mock LagoonVaultSDK instance."""
    mock_sdk = MagicMock()
    mock_sdk.get_underlying_token_address.return_value = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    mock_sdk.get_total_assets.return_value = 1000000
    return mock_sdk


class TestTeardownSubToolValidation:
    """VIB-101: teardown_vault pre-validates that required sub-tools are allowed."""

    @pytest.mark.asyncio
    async def test_teardown_fails_when_sub_tools_missing_from_allowed_tools(self, mock_gateway):
        """When allowed_tools is set but missing required sub-tools, teardown fails upfront."""
        policy = AgentPolicy(
            allowed_chains={"base"},
            max_tool_calls_per_minute=200,
            cooldown_seconds=0,
            # Only allow teardown_vault itself, not its required sub-tools
            allowed_tools={"teardown_vault", "get_balance"},
            require_rebalance_check=False,
        )
        exec_ = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            default_chain="base",
        )
        result = await exec_._execute_teardown_vault({
            "vault_address": "0x" + "a" * 40,
            "safe_address": "0x" + "b" * 40,
            "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
            "chain": "base",
        })
        assert result.status == "error"
        assert result.error["error_code"] == "teardown_missing_sub_tools"
        assert "close_lp_position" in result.error["message"]
        assert "swap_tokens" in result.error["message"]
        assert "settle_vault" in result.error["message"]

    @pytest.mark.asyncio
    async def test_teardown_succeeds_when_all_sub_tools_in_allowed_tools(self, mock_gateway):
        """When allowed_tools includes all required sub-tools, teardown proceeds."""
        from almanak.framework.agent_tools.policy import TEARDOWN_REQUIRED_TOOLS

        policy = AgentPolicy(
            allowed_chains={"base"},
            max_tool_calls_per_minute=200,
            cooldown_seconds=0,
            allowed_tools={"teardown_vault"} | TEARDOWN_REQUIRED_TOOLS,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
            require_rebalance_check=False,
        )
        exec_ = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            default_chain="base",
        )

        state_resp = MagicMock()
        state_resp.data = json.dumps({"phase": "running"}).encode()
        mock_gateway.state.LoadState.return_value = state_resp
        mock_gateway.state.SaveState.return_value = MagicMock(success=True)

        with (
            patch.object(
                exec_,
                "execute",
                return_value=ToolResponse(
                    status="success",
                    data={"tx_hash": "0x1", "balance": "0", "balance_usd": "0", "new_total_assets": "0"},
                ),
            ),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_mock_lagoon_sdk()),
        ):
            result = await exec_._execute_teardown_vault({
                "vault_address": "0x" + "a" * 40,
                "safe_address": "0x" + "b" * 40,
                "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
            })
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_teardown_skips_sub_tool_check_when_allowed_tools_is_none(self, executor, mock_gateway):
        """When allowed_tools is None (all tools allowed), no pre-flight check needed."""
        assert executor._policy_engine.policy.allowed_tools is None

        state_resp = MagicMock()
        state_resp.data = json.dumps({"phase": "running"}).encode()
        mock_gateway.state.LoadState.return_value = state_resp
        mock_gateway.state.SaveState.return_value = MagicMock(success=True)

        with (
            patch.object(
                executor,
                "execute",
                return_value=ToolResponse(
                    status="success",
                    data={"tx_hash": "0x1", "balance": "0", "balance_usd": "0", "new_total_assets": "0"},
                ),
            ),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_mock_lagoon_sdk()),
        ):
            result = await executor._execute_teardown_vault({
                "vault_address": "0x" + "a" * 40,
                "safe_address": "0x" + "b" * 40,
                "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
            })
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_teardown_error_lists_only_missing_tools(self, mock_gateway):
        """Error message should only list the tools that are actually missing."""
        policy = AgentPolicy(
            allowed_chains={"base"},
            max_tool_calls_per_minute=200,
            cooldown_seconds=0,
            # Include some but not all required sub-tools
            allowed_tools={"teardown_vault", "get_balance", "settle_vault"},
            require_rebalance_check=False,
        )
        exec_ = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            default_chain="base",
        )
        result = await exec_._execute_teardown_vault({
            "vault_address": "0x" + "a" * 40,
            "safe_address": "0x" + "b" * 40,
            "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
            "chain": "base",
        })
        assert result.status == "error"
        # Only close_lp_position and swap_tokens are missing
        assert "close_lp_position" in result.error["message"]
        assert "swap_tokens" in result.error["message"]
        # These are present, so should NOT appear in error
        assert "get_balance" not in result.error["message"]
        assert "settle_vault" not in result.error["message"]


class TestTeardownVault:
    @pytest.mark.asyncio
    async def test_teardown_no_positions_just_settles(self, executor, mock_gateway):
        """Teardown with no LP positions just runs final settlement."""
        # Mock state: no positions
        state_resp = MagicMock()
        state_resp.data = json.dumps({"phase": "running"}).encode()
        mock_gateway.state.LoadState.return_value = state_resp
        mock_gateway.state.SaveState.return_value = MagicMock(success=True)

        # We need to mock the nested execute calls
        async def mock_execute(name, args):
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle", "new_total_assets": "1000000"})
            if name == "get_balance":
                return ToolResponse(status="success", data={"balance": "0", "balance_usd": "0"})
            return ToolResponse(status="error", error={"message": "not mocked"})

        # Mock SDK for underlying token
        with (
            patch.object(executor, "execute", side_effect=mock_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_mock_lagoon_sdk()),
        ):
            result = await executor._execute_teardown_vault({
                "vault_address": "0x" + "a" * 40,
                "safe_address": "0x" + "b" * 40,
                "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
            })

        assert result.status == "success"
        assert result.data["positions_closed"] == 0

    @pytest.mark.asyncio
    async def test_teardown_saves_torn_down_state(self, executor, mock_gateway):
        """Teardown saves state with phase='torn_down'."""
        state_resp = MagicMock()
        state_resp.data = json.dumps({"phase": "running"}).encode()
        mock_gateway.state.LoadState.return_value = state_resp
        mock_gateway.state.SaveState.return_value = MagicMock(success=True)

        with (
            patch.object(
                executor,
                "execute",
                return_value=ToolResponse(
                    status="success",
                    data={"tx_hash": "0x1", "balance": "0", "balance_usd": "0", "new_total_assets": "0"},
                ),
            ),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_mock_lagoon_sdk()),
        ):
            await executor._execute_teardown_vault({
                "vault_address": "0x" + "a" * 40,
                "safe_address": "0x" + "b" * 40,
                "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
            })

        # Verify state was saved with torn_down phase
        save_calls = mock_gateway.state.SaveState.call_args_list
        assert len(save_calls) > 0
        saved_data = json.loads(save_calls[-1][0][0].data)
        assert saved_data["phase"] == "torn_down"
        assert saved_data["lp_position_id"] is None

    @pytest.mark.asyncio
    async def test_teardown_dry_run(self, executor, mock_gateway):
        """Dry run teardown returns simulated status."""
        state_resp = MagicMock()
        state_resp.data = json.dumps({}).encode()
        mock_gateway.state.LoadState.return_value = state_resp
        mock_gateway.state.SaveState.return_value = MagicMock(success=True)

        with (
            patch.object(
                executor,
                "execute",
                return_value=ToolResponse(
                    status="simulated", data={"tx_hash": None, "balance": "0", "balance_usd": "0"}
                ),
            ),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_mock_lagoon_sdk()),
        ):
            result = await executor._execute_teardown_vault({
                "vault_address": "0x" + "a" * 40,
                "safe_address": "0x" + "b" * 40,
                "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
                "dry_run": True,
            })

        # Dry run should still save teardown state
        assert result.status == "simulated" or result.status == "success"
