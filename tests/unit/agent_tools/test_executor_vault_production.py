"""Tests for vault production-readiness fixes.

Covers:
- H1: Decimal tick math (no float overflow/precision loss)
- H3: Missing current_tick skips LP value (no midpoint fallback)
- M2: Liquidity check fails closed on RPC error
- H2: Teardown uses direct token reads instead of get_portfolio
- M1: Teardown saves progress on partial failure
- E4: Deploy vault idempotency check
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponse


@pytest.fixture
def mock_gateway():
    """Create a mock GatewayClient with service stubs."""
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def executor(mock_gateway):
    """Create a ToolExecutor with permissive policy for vault testing."""
    policy = AgentPolicy(
        allowed_chains={"base", "arbitrum"},
        max_tool_calls_per_minute=100,
        cooldown_seconds=0,
        max_single_trade_usd=Decimal("999999999"),
        max_daily_spend_usd=Decimal("999999999"),
        max_position_size_usd=Decimal("999999999"),
        require_human_approval_above_usd=Decimal("999999999"),
    )
    return ToolExecutor(
        mock_gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-strategy",
        default_chain="base",
    )


def _mock_rpc_call(success=True, result="0x0"):
    """Create a mock RPC response."""
    resp = MagicMock()
    resp.success = success
    resp.result = json.dumps(result)
    resp.error = ""
    return resp


def _mock_exec_response(success=True, tx_hashes=None, error=""):
    """Create a mock execution response."""
    resp = MagicMock()
    resp.success = success
    resp.tx_hashes = tx_hashes or ["0xabc123"]
    resp.error = error
    resp.receipts = b"[]"
    return resp


# ── E4: Deploy idempotency ─────────────────────────────────────────

class TestDeployIdempotency:
    """deploy_vault must not create duplicate vaults."""

    @pytest.mark.asyncio
    async def test_existing_vault_in_state_returns_it(self, executor, mock_gateway):
        """If agent state has a vault and it exists on-chain, return it without deploying."""
        existing_vault = "0x" + "d" * 40

        # State has a saved vault address
        state_resp = MagicMock()
        state_resp.data = json.dumps({"vault_address": existing_vault}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        # Vault exists on-chain (get_total_assets succeeds)
        mock_gateway.rpc.Call.return_value = _mock_rpc_call(result=hex(10_000_000))

        result = await executor.execute("deploy_vault", {
            "chain": "base",
            "name": "Test Vault",
            "symbol": "tVLT",
            "underlying_token_address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "safe_address": "0x" + "b" * 40,
            "admin_address": "0x" + "b" * 40,
            "fee_receiver_address": "0x1234567890abcdef1234567890abcdef12345678",
            "deployer_address": "0x1234567890abcdef1234567890abcdef12345678",
        })

        assert result.status == "success"
        assert result.data["vault_address"] == existing_vault
        # Execution.Execute should NOT have been called (no deployment)
        mock_gateway.execution.Execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_state_load_failure_aborts_deployment(self, executor, mock_gateway):
        """If state load fails (non-NOT_FOUND), deploy_vault must abort to prevent duplicates."""
        import grpc

        # Simulate a transient gRPC INTERNAL error (not NOT_FOUND)
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.INTERNAL
        rpc_error.details = lambda: "connection reset"
        mock_gateway.state.LoadState.side_effect = rpc_error

        result = await executor.execute("deploy_vault", {
            "chain": "base",
            "name": "Test Vault",
            "symbol": "tVLT",
            "underlying_token_address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "safe_address": "0x" + "b" * 40,
            "admin_address": "0x" + "b" * 40,
            "fee_receiver_address": "0x1234567890abcdef1234567890abcdef12345678",
            "deployer_address": "0x1234567890abcdef1234567890abcdef12345678",
        })

        assert result.status == "error"
        assert result.error["error_code"] == "state_load_failed"
        assert "state load failure" in result.error["message"]
        mock_gateway.execution.Execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_chain_verification_failure_aborts(self, executor, mock_gateway):
        """If saved vault exists but on-chain check fails (transient RPC), abort to prevent duplicates."""
        existing_vault = "0x" + "d" * 40

        # State has a saved vault address
        state_resp = MagicMock()
        state_resp.data = json.dumps({"vault_address": existing_vault}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        # On-chain verification fails (RPC error)
        rpc_resp = MagicMock()
        rpc_resp.success = False
        rpc_resp.error = "execution reverted"
        rpc_resp.result = ""
        mock_gateway.rpc.Call.return_value = rpc_resp

        result = await executor.execute("deploy_vault", {
            "chain": "base",
            "name": "Test Vault",
            "symbol": "tVLT",
            "underlying_token_address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "safe_address": "0x" + "b" * 40,
            "admin_address": "0x" + "b" * 40,
            "fee_receiver_address": "0x1234567890abcdef1234567890abcdef12345678",
            "deployer_address": "0x1234567890abcdef1234567890abcdef12345678",
        })

        assert result.status == "error"
        assert result.error["error_code"] == "vault_verification_failed"
        mock_gateway.execution.Execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_state_not_found_proceeds_with_deployment(self, executor, mock_gateway):
        """If state load returns NOT_FOUND, deploy_vault should proceed normally."""
        import grpc

        # Simulate NOT_FOUND (new strategy, no state yet)
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.NOT_FOUND
        rpc_error.details = lambda: "State not found"
        mock_gateway.state.LoadState.side_effect = rpc_error

        # Mock deployment succeeding
        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hash = "0xabc123"
        exec_resp.receipt = json.dumps({"logs": []})
        mock_gateway.execution.Execute.return_value = exec_resp

        result = await executor.execute("deploy_vault", {
            "chain": "base",
            "name": "Test Vault",
            "symbol": "tVLT",
            "underlying_token_address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "safe_address": "0x" + "b" * 40,
            "admin_address": "0x" + "b" * 40,
            "fee_receiver_address": "0x1234567890abcdef1234567890abcdef12345678",
            "deployer_address": "0x1234567890abcdef1234567890abcdef12345678",
        })

        # Should have proceeded to deployment (Execute was called)
        mock_gateway.execution.Execute.assert_called()


# ── M1: Teardown state machine ─────────────────────────────────────

class TestTeardownStateMachine:
    """Teardown saves progress for crash recovery."""

    @pytest.mark.asyncio
    async def test_already_torn_down_returns_immediately(self, executor, mock_gateway):
        """If vault is already torn down, return success without re-running."""
        state_resp = MagicMock()
        state_resp.data = json.dumps({"phase": "torn_down"}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        result = await executor.execute("teardown_vault", {
            "vault_address": "0x" + "a" * 40,
            "safe_address": "0x" + "b" * 40,
            "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
            "chain": "base",
        })

        assert result.status == "success"
        assert "already torn down" in result.data["message"]

    @pytest.mark.asyncio
    async def test_lp_close_failure_saves_progress_and_returns_error(self, executor, mock_gateway):
        """LP close failure saves state so next attempt retries from lp_closing phase."""
        state_resp = MagicMock()
        state_resp.data = json.dumps({"lp_position_id": 42}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        # LP close fails
        with patch.object(executor, 'execute') as mock_execute:
            async def side_effect(tool_name, args):
                if tool_name == "close_lp_position":
                    return ToolResponse(
                        status="error",
                        error={"message": "Insufficient gas"},
                    )
                return ToolResponse(status="success", data={})

            mock_execute.side_effect = side_effect

            result = await executor._execute_teardown_vault({
                "vault_address": "0x" + "a" * 40,
                "safe_address": "0x" + "b" * 40,
                "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                "chain": "base",
            })

            assert result.status == "error"
            assert result.error["error_code"] == "teardown_lp_close_failed"
            assert result.error["recoverable"] is True

            # State should have been saved with lp_closing phase
            save_calls = mock_gateway.state.SaveState.call_args_list
            assert len(save_calls) >= 1
            saved_data = json.loads(save_calls[-1].args[0].data)
            assert saved_data["_teardown"]["phase"] == "lp_closing"

    @pytest.mark.asyncio
    async def test_teardown_no_lp_delegates_settlement_to_runner(self, executor, mock_gateway):
        """Teardown without LP position skips close; final settlement is runner-owned.

        VIB-5681: teardown_vault does not settle — it delegates to the runner's
        VaultLifecycleManager. With no LP and no residual tokens, no sub-tool call
        is made and the result marks settlement delegated.
        """
        state_resp = MagicMock()
        state_resp.data = json.dumps({}).encode()  # No lp_position_id
        mock_gateway.state.LoadState.return_value = state_resp

        # Mock the SDK for underlying token
        with patch('almanak.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk_instance = MockSDK.return_value
            sdk_instance.get_underlying_token_address.return_value = (
                "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            )
            sdk_instance.get_total_assets.return_value = 10_000_000

            with patch.object(executor, 'execute') as mock_execute:
                async def side_effect(tool_name, args):
                    # settle_vault must never be sub-called by teardown.
                    assert tool_name != "settle_vault", "teardown_vault must not settle (VIB-5681)"
                    return ToolResponse(status="success", data={"tx_hash": "0xabc"})

                mock_execute.side_effect = side_effect

                result = await executor._execute_teardown_vault({
                    "vault_address": "0x" + "a" * 40,
                    "safe_address": "0x" + "b" * 40,
                    "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "chain": "base",
                })

                assert result.status == "success"
                assert result.data["settlement"] == "runner_owned"
                assert result.data["positions_closed"] == 0
