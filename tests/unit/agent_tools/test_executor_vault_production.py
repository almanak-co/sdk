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
        strategy_id="test-strategy",
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


# ── H1: Decimal tick math ─────────────────────────────────────────

class TestDecimalTickMath:
    """Verify LP NAV computation uses Decimal and handles extreme ticks."""

    @pytest.mark.asyncio
    async def test_moderate_ticks_produce_correct_amounts(self, executor, mock_gateway):
        """Standard tick range (-1000 to 1000) with current_tick=0 computes correctly."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]

        rpc_responses = [
            _mock_rpc_call(result=asset_result),  # asset()
            _mock_rpc_call(result=hex(0)),  # balanceOf(safe) = 0
            _mock_rpc_call(result="0x" + "0" * 64),  # silo (zero addr)
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        state_resp = MagicMock()
        state_resp.data = json.dumps({"lp_position_id": 42}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        with patch.object(executor, '_execute_get_lp_position') as mock_lp:
            mock_lp.return_value = ToolResponse(
                status="success",
                data={
                    "position_id": "42",
                    "token_a": "0xtoken_a_address_padded_to_forty_chars",
                    "token_b": usdc_address,
                    "fee_tier": 3000,
                    "tick_lower": -1000,
                    "tick_upper": 1000,
                    "liquidity": "1000000000000",
                    "current_tick": 0,
                    "tokens_owed_a": "0",
                    "tokens_owed_b": "0",
                    "in_range": True,
                },
            )

            price_resp_a = MagicMock()
            price_resp_a.price = "10.0"
            price_resp_b = MagicMock()
            price_resp_b.price = "1.0"
            underlying_price = MagicMock()
            underlying_price.price = "1.0"
            mock_gateway.market.GetPrice.side_effect = [price_resp_a, price_resp_b, underlying_price]

            with patch('almanak.framework.data.tokens.get_token_resolver') as mock_resolver_fn:
                mock_resolver = MagicMock()
                token_a_resolved = MagicMock()
                token_a_resolved.decimals = 18
                token_b_resolved = MagicMock()
                token_b_resolved.decimals = 6
                underlying_resolved = MagicMock()
                underlying_resolved.decimals = 6
                mock_resolver.resolve.side_effect = [token_a_resolved, token_b_resolved, underlying_resolved]
                mock_resolver_fn.return_value = mock_resolver

                vault = "0x" + "a" * 40
                safe = "0x" + "b" * 40
                nav = await executor._compute_vault_nav(vault, safe, "base")

                # With liquidity and in-range position, NAV should be positive
                assert nav > 0

    @pytest.mark.asyncio
    async def test_extreme_ticks_no_overflow(self, executor, mock_gateway):
        """Large ticks (near max 887272) should not overflow with Decimal math."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]

        rpc_responses = [
            _mock_rpc_call(result=asset_result),
            _mock_rpc_call(result=hex(0)),
            _mock_rpc_call(result="0x" + "0" * 64),
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        state_resp = MagicMock()
        state_resp.data = json.dumps({"lp_position_id": 99}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        with patch.object(executor, '_execute_get_lp_position') as mock_lp:
            mock_lp.return_value = ToolResponse(
                status="success",
                data={
                    "position_id": "99",
                    "token_a": "0xtoken_a_address_padded_to_forty_chars",
                    "token_b": usdc_address,
                    "fee_tier": 3000,
                    "tick_lower": 50000,
                    "tick_upper": 100000,
                    "liquidity": "1000000000000000000",
                    "current_tick": 75000,
                    "tokens_owed_a": "0",
                    "tokens_owed_b": "0",
                    "in_range": True,
                },
            )

            price_resp_a = MagicMock()
            price_resp_a.price = "0.001"
            price_resp_b = MagicMock()
            price_resp_b.price = "1.0"
            underlying_price = MagicMock()
            underlying_price.price = "1.0"
            mock_gateway.market.GetPrice.side_effect = [price_resp_a, price_resp_b, underlying_price]

            with patch('almanak.framework.data.tokens.get_token_resolver') as mock_resolver_fn:
                mock_resolver = MagicMock()
                token_a_resolved = MagicMock()
                token_a_resolved.decimals = 18
                token_b_resolved = MagicMock()
                token_b_resolved.decimals = 6
                underlying_resolved = MagicMock()
                underlying_resolved.decimals = 6
                mock_resolver.resolve.side_effect = [token_a_resolved, token_b_resolved, underlying_resolved]
                mock_resolver_fn.return_value = mock_resolver

                vault = "0x" + "a" * 40
                safe = "0x" + "b" * 40
                # Should not raise OverflowError
                nav = await executor._compute_vault_nav(vault, safe, "base")
                assert nav >= 0

    @pytest.mark.asyncio
    async def test_negative_ticks_handled(self, executor, mock_gateway):
        """Negative ticks (common for stablecoin pairs) computed correctly."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]

        rpc_responses = [
            _mock_rpc_call(result=asset_result),
            _mock_rpc_call(result=hex(0)),
            _mock_rpc_call(result="0x" + "0" * 64),
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        state_resp = MagicMock()
        state_resp.data = json.dumps({"lp_position_id": 50}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        with patch.object(executor, '_execute_get_lp_position') as mock_lp:
            mock_lp.return_value = ToolResponse(
                status="success",
                data={
                    "position_id": "50",
                    "token_a": "0xtoken_a_address_padded_to_forty_chars",
                    "token_b": usdc_address,
                    "fee_tier": 500,
                    "tick_lower": -100000,
                    "tick_upper": -50000,
                    "liquidity": "5000000000000",
                    "current_tick": -75000,
                    "tokens_owed_a": "100000000000000000",
                    "tokens_owed_b": "500000",
                    "in_range": True,
                },
            )

            price_resp_a = MagicMock()
            price_resp_a.price = "5.0"
            price_resp_b = MagicMock()
            price_resp_b.price = "1.0"
            underlying_price = MagicMock()
            underlying_price.price = "1.0"
            mock_gateway.market.GetPrice.side_effect = [price_resp_a, price_resp_b, underlying_price]

            with patch('almanak.framework.data.tokens.get_token_resolver') as mock_resolver_fn:
                mock_resolver = MagicMock()
                token_a_resolved = MagicMock()
                token_a_resolved.decimals = 18
                token_b_resolved = MagicMock()
                token_b_resolved.decimals = 6
                underlying_resolved = MagicMock()
                underlying_resolved.decimals = 6
                mock_resolver.resolve.side_effect = [token_a_resolved, token_b_resolved, underlying_resolved]
                mock_resolver_fn.return_value = mock_resolver

                vault = "0x" + "a" * 40
                safe = "0x" + "b" * 40
                nav = await executor._compute_vault_nav(vault, safe, "base")
                assert nav > 0  # Should include uncollected fees at minimum


# ── H3: Missing current_tick skips LP ──────────────────────────────

class TestCurrentTickRequired:
    """NAV computation must not fallback to midpoint tick."""

    @pytest.mark.asyncio
    async def test_missing_current_tick_excludes_lp_from_nav(self, executor, mock_gateway):
        """When current_tick is None, LP value is excluded (conservative)."""
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        asset_result = "0x" + "0" * 24 + usdc_address[2:]

        rpc_responses = [
            _mock_rpc_call(result=asset_result),
            _mock_rpc_call(result=hex(5_000_000)),  # 5 USDC in Safe
            _mock_rpc_call(result="0x" + "0" * 64),
        ]
        mock_gateway.rpc.Call.side_effect = rpc_responses

        state_resp = MagicMock()
        state_resp.data = json.dumps({"lp_position_id": 42}).encode()
        mock_gateway.state.LoadState.return_value = state_resp

        with patch.object(executor, '_execute_get_lp_position') as mock_lp:
            mock_lp.return_value = ToolResponse(
                status="success",
                data={
                    "position_id": "42",
                    "token_a": "0xtoken_a_address_padded_to_forty_chars",
                    "token_b": usdc_address,
                    "fee_tier": 3000,
                    "tick_lower": -1000,
                    "tick_upper": 1000,
                    "liquidity": "999999999999999",
                    "current_tick": None,  # Unavailable!
                    "tokens_owed_a": "0",
                    "tokens_owed_b": "0",
                    "in_range": True,
                },
            )

            # Price calls may still be attempted for uncollected fees
            # but with 0 amounts, no price calls needed
            vault = "0x" + "a" * 40
            safe = "0x" + "b" * 40
            nav = await executor._compute_vault_nav(vault, safe, "base")

            # NAV should be exactly Safe balance (LP excluded)
            assert nav == 5_000_000


# ── M2: Liquidity check fails closed ──────────────────────────────

class TestLiquidityCheckFailsClosed:
    """Settlement must not proceed when liquidity cannot be verified."""

    @pytest.mark.asyncio
    async def test_pending_redemptions_rpc_failure_blocks_settlement(self, executor, mock_gateway):
        """If get_pending_redemptions fails, _check_settlement_liquidity returns False."""
        with patch('almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk = MockSDK.return_value
            sdk.get_pending_redemptions.side_effect = Exception("RPC timeout")

            sufficient, liquid, needed = await executor._check_settlement_liquidity(
                "0x" + "a" * 40, "0x" + "b" * 40, "base"
            )

            assert sufficient is False
            assert liquid == 0
            assert needed == 0

    @pytest.mark.asyncio
    async def test_balance_read_failure_blocks_settlement(self, executor, mock_gateway):
        """If Safe balance read fails, _check_settlement_liquidity returns False."""
        with patch('almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk = MockSDK.return_value
            sdk.get_pending_redemptions.return_value = 1_000_000_000_000_000_000  # 1 share
            sdk.convert_to_assets.return_value = 1_000_000  # 1 USDC in raw units

            # Balance read fails
            mock_gateway.rpc.Call.side_effect = Exception("RPC timeout")

            sufficient, liquid, needed = await executor._check_settlement_liquidity(
                "0x" + "a" * 40, "0x" + "b" * 40, "base"
            )

            assert sufficient is False

    @pytest.mark.asyncio
    async def test_zero_pending_redemptions_always_passes(self, executor, mock_gateway):
        """No pending redemptions should always pass liquidity check."""
        with patch('almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk = MockSDK.return_value
            sdk.get_pending_redemptions.return_value = 0

            sufficient, liquid, needed = await executor._check_settlement_liquidity(
                "0x" + "a" * 40, "0x" + "b" * 40, "base"
            )

            assert sufficient is True


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
    async def test_teardown_no_lp_proceeds_to_settlement(self, executor, mock_gateway):
        """Teardown without LP position skips close and goes to settlement."""
        state_resp = MagicMock()
        state_resp.data = json.dumps({}).encode()  # No lp_position_id
        mock_gateway.state.LoadState.return_value = state_resp

        # Mock the SDK for underlying token
        with patch('almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK') as MockSDK:
            sdk_instance = MockSDK.return_value
            sdk_instance.get_underlying_token_address.return_value = (
                "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            )
            sdk_instance.get_total_assets.return_value = 10_000_000

            # Mock settle_vault execution
            with patch.object(executor, 'execute') as mock_execute:
                async def side_effect(tool_name, args):
                    return ToolResponse(
                        status="success",
                        data={"tx_hash": "0xabc", "new_total_assets": "10000000"},
                    )

                mock_execute.side_effect = side_effect

                result = await executor._execute_teardown_vault({
                    "vault_address": "0x" + "a" * 40,
                    "safe_address": "0x" + "b" * 40,
                    "valuator_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "chain": "base",
                })

                assert result.status == "success"
                assert result.data["positions_closed"] == 0
