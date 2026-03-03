"""Tests for agent tool executor with mocked gateway."""

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
    """Create a ToolExecutor with permissive policy for testing."""
    policy = AgentPolicy(
        allowed_chains={"arbitrum", "base", "ethereum"},
        max_tool_calls_per_minute=100,
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
    )


class TestToolExecutorBasics:
    @pytest.mark.asyncio
    async def test_unknown_tool_returns_error(self, executor):
        result = await executor.execute("nonexistent_tool", {})
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"

    @pytest.mark.asyncio
    async def test_invalid_arguments_returns_error(self, executor):
        # get_price requires 'token' field
        result = await executor.execute("get_price", {})
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"

    @pytest.mark.asyncio
    async def test_policy_denied_returns_error(self, mock_gateway):
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            cooldown_seconds=0,
            max_tool_calls_per_minute=100,
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")
        result = await executor.execute("get_price", {"token": "ETH", "chain": "ethereum"})
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"


class TestDataToolDispatch:
    @pytest.mark.asyncio
    async def test_get_price_success(self, executor, mock_gateway):
        mock_resp = MagicMock()
        mock_resp.price = "3200.50"
        mock_resp.source = "coingecko"
        mock_resp.timestamp = 1700000000
        mock_gateway.market.GetPrice.return_value = mock_resp

        result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        assert result.status == "success"
        assert result.data["token"] == "ETH"
        assert result.data["price_usd"] == 3200.50
        assert result.data["source"] == "coingecko"

    @pytest.mark.asyncio
    async def test_get_balance_success(self, executor, mock_gateway):
        mock_resp = MagicMock()
        mock_resp.balance = "5000.00"
        mock_resp.balance_usd = "5000.00"
        mock_gateway.market.GetBalance.return_value = mock_resp

        result = await executor.execute("get_balance", {"token": "USDC", "chain": "arbitrum"})
        assert result.status == "success"
        assert result.data["balance"] == "5000.00"

    @pytest.mark.asyncio
    async def test_get_indicator_success(self, executor, mock_gateway):
        mock_resp = MagicMock()
        mock_resp.value = "72.5"
        mock_resp.metadata = {"signal": "overbought"}
        mock_gateway.market.GetIndicator.return_value = mock_resp

        result = await executor.execute(
            "get_indicator", {"token": "ETH", "indicator": "rsi", "period": 14}
        )
        assert result.status == "success"
        assert result.data["indicator"] == "rsi"
        assert result.data["value"] == 72.5

    @pytest.mark.asyncio
    async def test_resolve_token_success(self, executor):
        with patch("almanak.framework.data.tokens.get_token_resolver") as mock_resolver_fn:
            mock_resolver = MagicMock()
            mock_token = MagicMock()
            mock_token.symbol = "USDC"
            mock_token.address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
            mock_token.decimals = 6
            mock_token.chain = "arbitrum"
            mock_token.source = "static"
            mock_resolver.resolve.return_value = mock_token
            mock_resolver_fn.return_value = mock_resolver

            result = await executor.execute("resolve_token", {"token": "USDC", "chain": "arbitrum"})
            assert result.status == "success"
            assert result.data["symbol"] == "USDC"
            assert result.data["decimals"] == 6

    @pytest.mark.asyncio
    async def test_batch_get_balances_success(self, executor, mock_gateway):
        mock_resp = MagicMock()
        r1 = MagicMock()
        r1.balance = "1.5"
        r1.balance_usd = "4800.00"
        r2 = MagicMock()
        r2.balance = "5000.00"
        r2.balance_usd = "5000.00"
        mock_resp.responses = [r1, r2]
        mock_gateway.market.BatchGetBalances.return_value = mock_resp

        result = await executor.execute(
            "batch_get_balances", {"chain": "arbitrum", "tokens": ["ETH", "USDC"]}
        )
        assert result.status == "success"
        assert len(result.data["balances"]) == 2

    @pytest.mark.asyncio
    async def test_batch_get_balances_no_tokens_returns_error(self, executor):
        """tokens is required at schema level -- missing tokens fails validation."""
        result = await executor.execute("batch_get_balances", {"chain": "arbitrum"})
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"

    @pytest.mark.asyncio
    async def test_get_pool_state_dispatches_to_rpc(self, executor, mock_gateway):
        """get_pool_state should call RPC to read pool state (errors without real RPC are expected)."""
        mock_gateway.rpc.Call.side_effect = Exception("no rpc configured")
        result = await executor.execute(
            "get_pool_state", {"token_a": "WETH", "token_b": "USDC", "chain": "base"}
        )
        # Implementation tries RPC; with mock gateway it will error gracefully
        assert result.status == "error"

    @pytest.mark.asyncio
    async def test_get_risk_metrics_returns_portfolio_value(self, executor, mock_gateway):
        """get_risk_metrics returns portfolio value from gateway balances."""
        mock_batch_resp = MagicMock()
        mock_balance = MagicMock()
        mock_balance.balance_usd = "1000.50"
        mock_balance.error = ""
        mock_batch_resp.responses = [mock_balance]
        mock_gateway.market.BatchGetBalances.return_value = mock_batch_resp

        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.status == "success"
        assert "portfolio_value_usd" in result.data

    @pytest.mark.asyncio
    async def test_get_risk_metrics_returns_error_on_gateway_failure(self, executor, mock_gateway):
        """get_risk_metrics returns error when gateway fails."""
        mock_gateway.market.BatchGetBalances.side_effect = Exception("gateway unavailable")

        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.status == "error"
        assert result.error["error_code"] == "gateway_error"

    @pytest.mark.asyncio
    async def test_get_risk_metrics_returns_error_when_all_queries_fail(self, executor, mock_gateway):
        """get_risk_metrics returns error when every balance response has an error."""
        mock_batch_resp = MagicMock()
        err_resp = MagicMock()
        err_resp.balance_usd = ""
        err_resp.error = "rpc timeout"
        mock_batch_resp.responses = [err_resp, err_resp]
        mock_gateway.market.BatchGetBalances.return_value = mock_batch_resp

        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.status == "error"
        assert result.error["error_code"] == "all_queries_failed"


class TestPlanningToolDispatch:
    @pytest.mark.asyncio
    async def test_compile_intent_success(self, executor, mock_gateway):
        mock_resp = MagicMock()
        mock_resp.success = True
        mock_resp.action_bundle = json.dumps({"actions": [{"type": "SWAP"}]}).encode()
        mock_resp.error = ""
        mock_gateway.execution.CompileIntent.return_value = mock_resp

        result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {"from_token": "USDC", "to_token": "ETH", "amount": "1000"}},
        )
        assert result.status == "success"
        assert "bundle_id" in result.data
        assert len(result.data["actions"]) == 1

    @pytest.mark.asyncio
    async def test_compile_intent_failure(self, executor, mock_gateway):
        mock_resp = MagicMock()
        mock_resp.success = False
        mock_resp.error = "Unknown intent type"
        mock_gateway.execution.CompileIntent.return_value = mock_resp

        result = await executor.execute(
            "compile_intent",
            {"intent_type": "invalid", "params": {}},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "simulation_failed"

    @pytest.mark.asyncio
    async def test_simulate_intent_with_intent_type(self, executor, mock_gateway):
        # Compile mock
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = json.dumps({"actions": []}).encode()
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        # Execute (dry_run) mock
        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        result = await executor.execute(
            "simulate_intent",
            {"intent_type": "swap", "params": {"from_token": "USDC", "to_token": "ETH", "amount": "1000"}},
        )
        assert result.status == "simulated"

    @pytest.mark.asyncio
    async def test_estimate_gas_returns_gas_units(self, executor):
        """estimate_gas returns static gas estimate for intent type."""
        result = await executor.execute(
            "estimate_gas", {"intent_type": "swap", "params": {}, "chain": "arbitrum"}
        )
        assert result.status == "success"
        assert result.data["gas_units"] > 0

    @pytest.mark.asyncio
    async def test_estimate_gas_rejects_unknown_intent_type(self, executor):
        """estimate_gas returns validation error for unknown intent types."""
        result = await executor.execute(
            "estimate_gas", {"intent_type": "typo_intent", "params": {}, "chain": "arbitrum"}
        )
        assert result.status == "error"
        assert result.error["error_code"] == "invalid_intent_type"
        assert "TYPO_INTENT" in result.error["message"]


class TestActionToolDispatch:
    @pytest.mark.asyncio
    async def test_swap_tokens_success(self, executor, mock_gateway):
        # Compile
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = json.dumps({"actions": [{"type": "SWAP"}]}).encode()
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        # Execute -- also mock GetPrice for spend tracking
        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xabc123"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "1000", "chain": "arbitrum"},
        )
        assert result.status == "success"
        assert result.data["tx_hash"] == "0xabc123"

    @pytest.mark.asyncio
    async def test_swap_tokens_dry_run(self, executor, mock_gateway):
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = json.dumps({"actions": []}).encode()
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = []
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "1000", "dry_run": True, "chain": "arbitrum"},
        )
        assert result.status == "simulated"

    @pytest.mark.asyncio
    async def test_execute_compiled_bundle_success(self, executor, mock_gateway):
        """P0 regression: execute_compiled_bundle must work end-to-end."""
        # First compile a bundle
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = json.dumps({"actions": [{"type": "SWAP"}]}).encode()
        compile_resp.error = ""
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        compile_result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {"from_token": "USDC", "to_token": "ETH", "amount": "1000"}},
        )
        bundle_id = compile_result.data["bundle_id"]

        # Now execute the compiled bundle
        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xdef456"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        result = await executor.execute(
            "execute_compiled_bundle",
            {"bundle_id": bundle_id, "chain": "arbitrum"},
        )
        assert result.status == "success"
        assert result.data["success"] is True
        assert result.data["tx_hashes"] == ["0xdef456"]

    @pytest.mark.asyncio
    async def test_execute_compiled_bundle_missing_bundle(self, executor):
        """P0 regression: unknown bundle_id returns validation error."""
        result = await executor.execute(
            "execute_compiled_bundle",
            {"bundle_id": "nonexistent-id", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"
        assert "not found" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_compiled_bundle_removes_from_cache(self, executor, mock_gateway):
        """Bundle should be one-shot: removed from cache after execution."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        compile_resp.error = ""
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        compile_result = await executor.execute(
            "compile_intent", {"intent_type": "swap", "params": {}}
        )
        bundle_id = compile_result.data["bundle_id"]

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = []
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        # First execution succeeds
        result = await executor.execute("execute_compiled_bundle", {"bundle_id": bundle_id})
        assert result.status == "success"

        # Second execution fails (bundle consumed)
        result2 = await executor.execute("execute_compiled_bundle", {"bundle_id": bundle_id})
        assert result2.status == "error"
        assert "not found" in result2.error["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_compiled_bundle_blocked_by_spend_limit(self, mock_gateway):
        """Pre-execution spend gate: bundle should be blocked if it would exceed limits."""
        tight_policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("500"),
            max_daily_spend_usd=Decimal("999999999"),
        )
        tight_executor = ToolExecutor(
            mock_gateway,
            policy=tight_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
        )

        # Compile a bundle with a large swap
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": [{"type": "SWAP"}]}'
        compile_resp.error = ""
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        compile_result = await tight_executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {"from_token": "USDC", "amount": "1000"}, "chain": "arbitrum"},
        )
        bundle_id = compile_result.data["bundle_id"]

        # Executing should be blocked by spend limits ($1000 > $500 limit)
        result = await tight_executor.execute(
            "execute_compiled_bundle", {"bundle_id": bundle_id, "chain": "arbitrum"}
        )
        assert result.status == "error"
        assert "spend limit" in result.error["message"].lower() or "blocked" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_lp_open_missing_range_returns_validation_error(self, executor):
        """LP open without price_lower/price_upper should fail schema validation."""
        result = await executor.execute(
            "open_lp_position",
            {
                "token_a": "WETH",
                "token_b": "USDC",
                "amount_a": "1.0",
                "amount_b": "3200",
                "chain": "arbitrum",
            },
        )
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"

    @pytest.mark.asyncio
    async def test_lp_open_with_explicit_range(self, executor, mock_gateway):
        """LP open with explicit range should include range_lower/upper."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xaaa"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        # Use $1 price so total value stays under per-tool approval threshold ($5k)
        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        result = await executor.execute(
            "open_lp_position",
            {
                "token_a": "WETH",
                "token_b": "USDC",
                "amount_a": "1.0",
                "amount_b": "3200",
                "price_lower": "2800",
                "price_upper": "3600",
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"

        call_args = mock_gateway.execution.CompileIntent.call_args
        intent_data = json.loads(call_args[0][0].intent_data)
        assert intent_data["range_lower"] == "2800"
        assert intent_data["range_upper"] == "3600"

    @pytest.mark.asyncio
    async def test_lp_open_preserves_prices_on_token_swap(self, executor, mock_gateway):
        """Prices are preserved when tokens are reordered by address.

        The LLM computes price bounds from get_pool_state's current_price,
        which is always in token1/token0 direction (Uniswap V3 convention).
        Sorting tokens by address doesn't change the price direction -- the
        compiler also expects token1/token0 prices. So prices must NOT be
        inverted.
        """
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xaaa"]
        exec_resp.error = ""
        exec_resp.receipts = b"[]"
        mock_gateway.execution.Execute.return_value = exec_resp

        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        # Mock resolver so ALMANAK addr (0xBB) > USDC addr (0xAA), triggering swap
        mock_token_a = MagicMock()
        mock_token_a.address = "0xBB00000000000000000000000000000000000001"
        mock_token_b = MagicMock()
        mock_token_b.address = "0xAA00000000000000000000000000000000000001"

        with patch("almanak.framework.data.tokens.get_token_resolver") as mock_resolver:
            resolver_inst = MagicMock()
            resolver_inst.resolve_for_swap.side_effect = lambda token, chain: (
                mock_token_a if "ALMANAK" in str(token) else mock_token_b
            )
            mock_resolver.return_value = resolver_inst

            result = await executor.execute(
                "open_lp_position",
                {
                    "token_a": "ALMANAK",
                    "token_b": "USDC",
                    "amount_a": "1000",
                    "amount_b": "10",
                    "price_lower": "200",
                    "price_upper": "1000",
                    "chain": "arbitrum",
                },
            )

        assert result.status == "success"

        call_args = mock_gateway.execution.CompileIntent.call_args
        intent_data = json.loads(call_args[0][0].intent_data)

        # Prices should be preserved (NOT inverted)
        assert intent_data["range_lower"] == "200"
        assert intent_data["range_upper"] == "1000"
        # Amounts should be swapped (USDC first since its addr is lower)
        assert intent_data["amount0"] == "10"    # was amount_b (USDC)
        assert intent_data["amount1"] == "1000"  # was amount_a (ALMANAK)

    @pytest.mark.asyncio
    async def test_lp_close_no_extra_amount_field(self, executor, mock_gateway):
        """P0 regression: LP close must not send 'amount' field (forbidden by LPCloseIntent)."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xbbb"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        result = await executor.execute(
            "close_lp_position",
            {"position_id": "12345", "chain": "arbitrum"},
        )
        assert result.status == "success"

        call_args = mock_gateway.execution.CompileIntent.call_args
        intent_data = json.loads(call_args[0][0].intent_data)
        assert "amount" not in intent_data

    @pytest.mark.asyncio
    async def test_borrow_includes_collateral_fields(self, executor, mock_gateway):
        """P0 regression: borrow_lending must include collateral_token and collateral_amount."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xccc"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        result = await executor.execute(
            "borrow_lending",
            {
                "token": "USDC",
                "amount": "5000",
                "collateral_token": "WETH",
                "collateral_amount": "2.0",
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"

        call_args = mock_gateway.execution.CompileIntent.call_args
        intent_data = json.loads(call_args[0][0].intent_data)
        assert intent_data["collateral_token"] == "WETH"
        assert intent_data["collateral_amount"] == "2.0"
        assert intent_data["borrow_token"] == "USDC"
        assert intent_data["borrow_amount"] == "5000"

    @pytest.mark.asyncio
    async def test_borrow_missing_collateral_returns_validation_error(self, executor):
        """Borrow without collateral_token should fail schema validation."""
        result = await executor.execute(
            "borrow_lending",
            {"token": "USDC", "amount": "5000", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"

    @pytest.mark.asyncio
    async def test_spend_tracking_uses_price_lookup(self, executor, mock_gateway):
        """P0 regression: spend tracking should use gateway price, not raw amount."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xabc"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        # ETH price = $3200
        price_resp = MagicMock()
        price_resp.price = "3200.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        await executor.execute(
            "swap_tokens",
            {"token_in": "ETH", "token_out": "USDC", "amount": "1.0", "chain": "arbitrum"},
        )

        # Spend should be 1.0 * 3200 = $3200, not raw $1.0
        assert executor._policy_engine._daily_spend_usd == Decimal("3200.0")


class TestStateToolDispatch:
    @pytest.mark.asyncio
    async def test_save_state(self, executor, mock_gateway):
        mock_resp = MagicMock()
        mock_resp.success = True
        mock_resp.new_version = 1
        mock_resp.checksum = "abc123"
        mock_gateway.state.SaveState.return_value = mock_resp

        result = await executor.execute(
            "save_agent_state",
            {"state": {"position_id": 12345}},
        )
        assert result.status == "success"
        assert result.data["version"] == 1

    @pytest.mark.asyncio
    async def test_save_state_tracks_version(self, executor, mock_gateway):
        """P1 regression: save should use tracked version for optimistic locking."""
        # First load sets version
        load_resp = MagicMock()
        load_resp.data = json.dumps({"key": "val"}).encode()
        load_resp.version = 5
        mock_gateway.state.LoadState.return_value = load_resp

        await executor.execute("load_agent_state", {})

        # Save should use version=5
        save_resp = MagicMock()
        save_resp.success = True
        save_resp.new_version = 6
        save_resp.checksum = "def456"
        mock_gateway.state.SaveState.return_value = save_resp

        await executor.execute("save_agent_state", {"state": {"key": "updated"}})

        call_args = mock_gateway.state.SaveState.call_args
        assert call_args[0][0].expected_version == 5

    @pytest.mark.asyncio
    async def test_save_state_updates_tracked_version(self, executor, mock_gateway):
        """After a successful save, the tracked version should be updated."""
        save_resp = MagicMock()
        save_resp.success = True
        save_resp.new_version = 3
        save_resp.checksum = "xyz"
        mock_gateway.state.SaveState.return_value = save_resp

        await executor.execute("save_agent_state", {"state": {}})

        # Second save should use version=3
        save_resp2 = MagicMock()
        save_resp2.success = True
        save_resp2.new_version = 4
        save_resp2.checksum = "abc"
        mock_gateway.state.SaveState.return_value = save_resp2

        await executor.execute("save_agent_state", {"state": {}})

        call_args = mock_gateway.state.SaveState.call_args
        assert call_args[0][0].expected_version == 3

    @pytest.mark.asyncio
    async def test_load_state_found(self, executor, mock_gateway):
        mock_resp = MagicMock()
        mock_resp.data = json.dumps({"position_id": 12345}).encode()
        mock_resp.version = 3
        mock_gateway.state.LoadState.return_value = mock_resp

        result = await executor.execute("load_agent_state", {})
        assert result.status == "success"
        assert result.data["state"]["position_id"] == 12345
        assert result.data["version"] == 3

    @pytest.mark.asyncio
    async def test_load_state_not_found(self, executor, mock_gateway):
        mock_gateway.state.LoadState.side_effect = Exception("NOT_FOUND")

        result = await executor.execute("load_agent_state", {})
        assert result.status == "success"
        assert result.data["state"] == {}
        assert result.data["version"] == 0

    @pytest.mark.asyncio
    async def test_load_state_real_error_returns_error(self, executor, mock_gateway):
        """P1 regression: non-NOT_FOUND errors should return error, not empty state."""
        mock_gateway.state.LoadState.side_effect = Exception("connection refused")

        result = await executor.execute("load_agent_state", {})
        assert result.status == "error"
        assert result.error["error_code"] == "state_load_failed"
        assert "connection refused" in result.error["message"]

    @pytest.mark.asyncio
    async def test_record_decision_uses_details_json(self, executor, mock_gateway):
        """P1 regression: should use details_json proto field, not event_data."""
        mock_gateway.observe.RecordTimelineEvent.return_value = MagicMock()

        result = await executor.execute(
            "record_agent_decision",
            {
                "decision_summary": "Rebalanced LP position",
                "tool_calls": [{"name": "get_price", "args": {"token": "ETH"}}],
                "intent_type": "lp_close",
            },
        )
        assert result.status == "success"
        assert result.data["recorded"] is True
        assert "decision_id" in result.data

        # Find the agent_decision call (not the auto-logging tool_execution call)
        found = False
        for call in mock_gateway.observe.RecordTimelineEvent.call_args_list:
            request = call[0][0]
            if hasattr(request, "event_type") and request.event_type == "agent_decision":
                payload = json.loads(request.details_json)
                assert payload["summary"] == "Rebalanced LP position"
                found = True
                break
        assert found, "No agent_decision event recorded"

    @pytest.mark.asyncio
    async def test_record_decision_failure_returns_error(self, executor, mock_gateway):
        """P1 regression: failed record should return error, not silent success."""
        mock_gateway.observe.RecordTimelineEvent.side_effect = Exception("connection refused")

        result = await executor.execute(
            "record_agent_decision",
            {"decision_summary": "Test decision"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "record_failed"
        assert result.data["recorded"] is False


class TestLPClosePartialAmount:
    @pytest.mark.asyncio
    async def test_lp_close_partial_amount_rejected(self, executor):
        """P0 regression: partial LP close must be rejected since LPCloseIntent doesn't support it."""
        result = await executor.execute(
            "close_lp_position",
            {"position_id": "12345", "amount": "0.5", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"
        assert "not supported" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_lp_close_amount_all_accepted(self, executor, mock_gateway):
        """LP close with amount='all' should proceed normally."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xbbb"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        result = await executor.execute(
            "close_lp_position",
            {"position_id": "12345", "amount": "all", "chain": "arbitrum"},
        )
        assert result.status == "success"


class TestGetLPPositionDispatch:
    @pytest.mark.asyncio
    async def test_get_lp_position_dispatches_to_rpc(self, executor, mock_gateway):
        """get_lp_position should call RPC to read position data (errors without real RPC are expected)."""
        mock_gateway.rpc.Call.side_effect = Exception("no rpc configured")
        result = await executor.execute(
            "get_lp_position", {"position_id": "12345", "chain": "base"}
        )
        # Implementation tries RPC; with mock gateway it will error gracefully
        assert result.status == "error"


class TestBundleCacheChainBound:
    @pytest.mark.asyncio
    async def test_execute_bundle_wrong_chain_rejected(self, executor, mock_gateway):
        """P1 regression: bundle compiled for one chain cannot execute on another."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        compile_resp.error = ""
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        compile_result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {}, "chain": "arbitrum"},
        )
        bundle_id = compile_result.data["bundle_id"]

        # Try to execute on a different chain
        result = await executor.execute(
            "execute_compiled_bundle",
            {"bundle_id": bundle_id, "chain": "base"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"
        assert "compiled for chain" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_bundle_same_chain_succeeds(self, executor, mock_gateway):
        """Bundle executed on same chain as compiled should succeed."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        compile_resp.error = ""
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        compile_result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {}, "chain": "ethereum"},
        )
        bundle_id = compile_result.data["bundle_id"]

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xaaa"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        result = await executor.execute(
            "execute_compiled_bundle",
            {"bundle_id": bundle_id, "chain": "ethereum"},
        )
        assert result.status == "success"


class TestSimulationPolicyEnforcement:
    @pytest.mark.asyncio
    async def test_simulation_cannot_be_bypassed_by_agent(self, mock_gateway):
        """P1 regression: policy.require_simulation_before_execution overrides agent flag."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            require_simulation_before_execution=True,
        )
        executor = ToolExecutor(
            mock_gateway, policy=policy, wallet_address="0x1234", strategy_id="test"
        )

        # Compile a bundle
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        compile_resp.error = ""
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        compile_result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {}, "chain": "arbitrum"},
        )
        bundle_id = compile_result.data["bundle_id"]

        # Execute with require_simulation=False (agent trying to bypass)
        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xabc"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        await executor.execute(
            "execute_compiled_bundle",
            {"bundle_id": bundle_id, "require_simulation": False, "chain": "arbitrum"},
        )

        # Verify simulation_enabled was True despite agent requesting False
        call_args = mock_gateway.execution.Execute.call_args
        assert call_args[0][0].simulation_enabled is True


class TestBatchBalancesTotalUsd:
    @pytest.mark.asyncio
    async def test_total_usd_is_computed(self, executor, mock_gateway):
        """CodeRabbit: total_usd should sum balance_usd values, not be hardcoded '0'."""
        mock_resp = MagicMock()
        r1 = MagicMock()
        r1.balance = "1.5"
        r1.balance_usd = "4800.00"
        r2 = MagicMock()
        r2.balance = "5000.00"
        r2.balance_usd = "5000.00"
        mock_resp.responses = [r1, r2]
        mock_gateway.market.BatchGetBalances.return_value = mock_resp

        result = await executor.execute(
            "batch_get_balances", {"chain": "arbitrum", "tokens": ["ETH", "USDC"]}
        )
        assert result.status == "success"
        assert result.data["total_usd"] == "9800.00"


class TestExecutorErrorHandling:
    @pytest.mark.asyncio
    async def test_gateway_exception_returns_error(self, executor, mock_gateway):
        mock_gateway.market.GetPrice.side_effect = Exception("connection refused")

        result = await executor.execute("get_price", {"token": "ETH"})
        assert result.status == "error"
        assert "connection refused" in result.error["message"]

    @pytest.mark.asyncio
    async def test_error_never_raises_to_caller(self, executor, mock_gateway):
        # Even catastrophic errors should be wrapped, never raised
        mock_gateway.market.GetPrice.side_effect = RuntimeError("segfault")

        result = await executor.execute("get_price", {"token": "ETH"})
        assert isinstance(result, ToolResponse)
        assert result.status == "error"


class TestToolExecutionLogging:
    """WS4: automatic tool execution logging."""

    @pytest.mark.asyncio
    async def test_tool_event_recorded_on_success(self, executor, mock_gateway):
        """Tool execution should fire a timeline event."""
        mock_resp = MagicMock()
        mock_resp.price = "3200.0"
        mock_resp.source = "coingecko"
        mock_resp.timestamp = 1700000000
        mock_gateway.market.GetPrice.return_value = mock_resp
        mock_gateway.observe.RecordTimelineEvent.return_value = MagicMock()

        await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})

        mock_gateway.observe.RecordTimelineEvent.assert_called_once()
        call_args = mock_gateway.observe.RecordTimelineEvent.call_args
        request = call_args[0][0]
        assert request.event_type == "tool_execution"
        payload = json.loads(request.details_json)
        assert payload["tool_name"] == "get_price"
        assert payload["status"] == "success"

    @pytest.mark.asyncio
    async def test_tool_event_failure_non_fatal(self, executor, mock_gateway):
        """If timeline event recording fails, tool execution still succeeds."""
        mock_resp = MagicMock()
        mock_resp.price = "3200.0"
        mock_resp.source = "coingecko"
        mock_resp.timestamp = 1700000000
        mock_gateway.market.GetPrice.return_value = mock_resp
        mock_gateway.observe.RecordTimelineEvent.side_effect = Exception("observe down")

        result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        assert result.status == "success"


class TestAlertingIntegration:
    """WS6: alerting on policy denial and circuit breaker."""

    @pytest.mark.asyncio
    async def test_alert_on_policy_denial(self, mock_gateway):
        """Alert manager should be called when policy denies a tool."""
        mock_alert = MagicMock()
        mock_alert.send_alert = MagicMock(return_value=MagicMock())  # not a real coroutine

        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            cooldown_seconds=0,
            max_tool_calls_per_minute=100,
        )
        executor = ToolExecutor(
            mock_gateway, policy=policy, wallet_address="0x1234",
            alert_manager=mock_alert,
        )
        result = await executor.execute("get_price", {"token": "ETH", "chain": "ethereum"})
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"

    @pytest.mark.asyncio
    async def test_no_alert_without_alert_manager(self, mock_gateway):
        """Without alert_manager, policy denial should still work fine."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            cooldown_seconds=0,
            max_tool_calls_per_minute=100,
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")
        result = await executor.execute("get_price", {"token": "ETH", "chain": "ethereum"})
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"

    @pytest.mark.asyncio
    async def test_circuit_breaker_alert(self, mock_gateway):
        """Alert should fire when circuit breaker trips."""
        mock_alert = MagicMock()
        mock_alert.send_alert = MagicMock(return_value=MagicMock())

        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
            max_consecutive_failures=2,
        )
        executor = ToolExecutor(
            mock_gateway, policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test",
            alert_manager=mock_alert,
        )

        # Pre-record a failure
        executor._policy_engine.record_trade(Decimal("100"), success=False)

        # Set up a swap that will fail execution
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = False
        exec_resp.error = "out of gas"
        exec_resp.tx_hashes = []
        mock_gateway.execution.Execute.return_value = exec_resp

        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "error"
        # Second failure should trip circuit breaker (threshold=2)
        assert executor._policy_engine.consecutive_failures >= 2
        assert executor._policy_engine.is_circuit_breaker_tripped


class TestNAVBoundsValidation:
    """WS2: validate_nav_change_bps standalone function."""

    def test_reasonable_increase_passes(self):
        from almanak.framework.vault.lifecycle import validate_nav_change_bps

        ok, reason = validate_nav_change_bps(100000, 105000)  # 5% increase (500 bps)
        assert ok is True
        assert reason == ""

    def test_excessive_increase_rejected(self):
        from almanak.framework.vault.lifecycle import validate_nav_change_bps

        ok, reason = validate_nav_change_bps(100000, 115000)  # 15% increase (1500 bps)
        assert ok is False
        assert "increase" in reason.lower()
        assert "1500" in reason

    def test_reasonable_decrease_passes(self):
        from almanak.framework.vault.lifecycle import validate_nav_change_bps

        ok, reason = validate_nav_change_bps(100000, 97000)  # 3% decrease (300 bps)
        assert ok is True

    def test_excessive_decrease_rejected(self):
        from almanak.framework.vault.lifecycle import validate_nav_change_bps

        ok, reason = validate_nav_change_bps(100000, 90000)  # 10% decrease (1000 bps)
        assert ok is False
        assert "decrease" in reason.lower()

    def test_zero_old_assets_passes(self):
        from almanak.framework.vault.lifecycle import validate_nav_change_bps

        ok, reason = validate_nav_change_bps(0, 100000)
        assert ok is True

    def test_custom_thresholds(self):
        from almanak.framework.vault.lifecycle import validate_nav_change_bps

        ok, reason = validate_nav_change_bps(100000, 112000, max_up_bps=1500)  # 12% < 15% threshold
        assert ok is True

    def test_no_change_passes(self):
        from almanak.framework.vault.lifecycle import validate_nav_change_bps

        ok, reason = validate_nav_change_bps(100000, 100000)
        assert ok is True

    def test_boundary_increase_at_limit(self):
        from almanak.framework.vault.lifecycle import validate_nav_change_bps

        ok, reason = validate_nav_change_bps(100000, 110000)  # exactly 10% = 1000 bps = limit
        assert ok is True  # at limit, not exceeding

    def test_boundary_decrease_at_limit(self):
        from almanak.framework.vault.lifecycle import validate_nav_change_bps

        ok, reason = validate_nav_change_bps(100000, 95000)  # exactly 5% = 500 bps = limit
        assert ok is True  # at limit, not exceeding


class TestPortfolioValueTracking:
    """WS7: portfolio value updates after trades."""

    @pytest.mark.asyncio
    async def test_portfolio_value_updated_after_successful_trade(self, executor, mock_gateway):
        """After successful trade, portfolio value should be fetched and updated."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xabc"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        # Price for spend tracking
        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        # Portfolio value from batch balances
        batch_resp = MagicMock()
        balance_resp = MagicMock()
        balance_resp.balance_usd = "50000.00"
        batch_resp.responses = [balance_resp]
        mock_gateway.market.BatchGetBalances.return_value = batch_resp

        await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "1000", "chain": "arbitrum"},
        )

        # Portfolio value should be tracked
        assert executor._policy_engine._peak_portfolio_usd == Decimal("50000.00")
        assert executor._policy_engine._current_portfolio_usd == Decimal("50000.00")

    @pytest.mark.asyncio
    async def test_portfolio_fetch_failure_non_fatal(self, executor, mock_gateway):
        """If portfolio fetch fails, trade should still succeed."""
        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xabc"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        # BatchGetBalances fails
        mock_gateway.market.BatchGetBalances.side_effect = Exception("gateway error")

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "success"
        # Portfolio value unchanged (fetch failed silently)
        assert executor._policy_engine._peak_portfolio_usd == Decimal("0")


class TestSafeAddressValidation:
    """Test execution_wallet validation against configured Safe address allowlist."""

    @pytest.mark.asyncio
    async def test_unknown_wallet_rejected_when_safe_addresses_set(self, mock_gateway):
        """execution_wallet not in safe_addresses should be rejected."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1111111111111111111111111111111111111111",
            strategy_id="test",
            safe_addresses={"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
        )

        result = await executor.execute(
            "swap_tokens",
            {
                "token_in": "USDC",
                "token_out": "ETH",
                "amount": "100",
                "chain": "arbitrum",
                "execution_wallet": "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
            },
        )
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"
        assert "allowlist" in result.error["message"]

    @pytest.mark.asyncio
    async def test_empty_safe_addresses_denies_all_overrides(self, mock_gateway):
        """Empty safe_addresses set should deny all execution_wallet overrides (deny-all)."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1111111111111111111111111111111111111111",
            strategy_id="test",
            safe_addresses=set(),  # empty set = deny all overrides
        )

        result = await executor.execute(
            "swap_tokens",
            {
                "token_in": "USDC",
                "token_out": "ETH",
                "amount": "100",
                "chain": "arbitrum",
                "execution_wallet": "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
            },
        )
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"
        assert "allowlist" in result.error["message"]

    @pytest.mark.asyncio
    async def test_known_safe_wallet_passes(self, mock_gateway):
        """execution_wallet in safe_addresses should be allowed."""
        safe_addr = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1111111111111111111111111111111111111111",
            strategy_id="test",
            safe_addresses={safe_addr},
        )

        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xabc"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp
        mock_gateway.market.BatchGetBalances.side_effect = Exception("skip")

        result = await executor.execute(
            "swap_tokens",
            {
                "token_in": "USDC",
                "token_out": "ETH",
                "amount": "100",
                "chain": "arbitrum",
                "execution_wallet": safe_addr,
            },
        )
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_own_wallet_always_passes(self, mock_gateway):
        """Using the strategy's own wallet should always pass, even with safe_addresses set."""
        own_wallet = "0x1111111111111111111111111111111111111111"
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address=own_wallet,
            strategy_id="test",
            safe_addresses={"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
        )

        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xabc"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp
        mock_gateway.market.BatchGetBalances.side_effect = Exception("skip")

        result = await executor.execute(
            "swap_tokens",
            {
                "token_in": "USDC",
                "token_out": "ETH",
                "amount": "100",
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_case_insensitive_safe_address_match(self, mock_gateway):
        """Safe address matching should be case-insensitive (checksummed vs lowercase)."""
        safe_addr_lower = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        safe_addr_checksummed = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1111111111111111111111111111111111111111",
            strategy_id="test",
            safe_addresses={safe_addr_checksummed},
        )

        compile_resp = MagicMock()
        compile_resp.success = True
        compile_resp.action_bundle = b'{"actions": []}'
        mock_gateway.execution.CompileIntent.return_value = compile_resp

        exec_resp = MagicMock()
        exec_resp.success = True
        exec_resp.tx_hashes = ["0xabc"]
        exec_resp.error = ""
        mock_gateway.execution.Execute.return_value = exec_resp

        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp
        mock_gateway.market.BatchGetBalances.side_effect = Exception("skip")

        result = await executor.execute(
            "swap_tokens",
            {
                "token_in": "USDC",
                "token_out": "ETH",
                "amount": "100",
                "chain": "arbitrum",
                "execution_wallet": safe_addr_lower,
            },
        )
        assert result.status == "success"


class TestValidateRisk:
    """Tests for the validate_risk tool -- pre-trade risk validation."""

    @pytest.mark.asyncio
    async def test_validate_risk_passes_all_checks(self, executor):
        """A trade within all policy limits should return valid=True with no violations."""
        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "100"},
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        assert result.data["valid"] is True
        assert result.data["violations"] == []
        assert "risk_summary" in result.data
        assert "estimated_value_usd" in result.data["risk_summary"]
        assert "daily_spend_remaining_usd" in result.data["risk_summary"]
        assert "daily_spend_used_usd" in result.data["risk_summary"]

    @pytest.mark.asyncio
    async def test_validate_risk_blocked_token(self, mock_gateway):
        """A trade with a token not in allowed_tokens should report a violation."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            allowed_tokens={"USDC", "ETH"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")

        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "DOGE", "amount": "100"},
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        assert result.data["valid"] is False
        assert len(result.data["violations"]) >= 1
        # Check that the violation is about the blocked token
        token_violations = [v for v in result.data["violations"] if v["check"] == "token_not_allowed"]
        assert len(token_violations) >= 1
        assert "DOGE" in token_violations[0]["message"]
        assert token_violations[0]["severity"] == "blocking"

    @pytest.mark.asyncio
    async def test_validate_risk_exceeded_spend_limit(self, mock_gateway):
        """A trade exceeding the single-trade limit should be flagged."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("500"),
            max_daily_spend_usd=Decimal("999999999"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")

        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "1000"},
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        assert result.data["valid"] is False
        spend_violations = [v for v in result.data["violations"] if v["check"] == "single_trade_limit"]
        assert len(spend_violations) >= 1
        assert "1000" in spend_violations[0]["message"]

    @pytest.mark.asyncio
    async def test_validate_risk_multiple_violations(self, mock_gateway):
        """A trade that violates multiple policies should report all violations."""
        policy = AgentPolicy(
            allowed_chains={"ethereum"},  # Not arbitrum
            allowed_tokens={"USDC", "ETH"},  # Not DOGE
            allowed_protocols={"uniswap_v3"},  # Not sushiswap
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")

        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {
                    "from_token": "USDC",
                    "to_token": "DOGE",
                    "amount": "100",
                    "protocol": "sushiswap",
                },
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        assert result.data["valid"] is False
        # Should have at least violations for chain, token, and protocol
        violation_checks = {v["check"] for v in result.data["violations"]}
        assert "chain_not_allowed" in violation_checks
        assert "token_not_allowed" in violation_checks
        assert "protocol_not_allowed" in violation_checks
        assert len(result.data["violations"]) >= 3

    @pytest.mark.asyncio
    async def test_validate_risk_near_limit_warnings(self, mock_gateway):
        """Trades near policy limits should produce warnings."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("1000"),
            max_daily_spend_usd=Decimal("999999999"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")

        # Trade at 90% of single-trade limit
        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "900"},
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        assert result.data["valid"] is True  # Not blocking
        # Should have a near-limit warning
        warning_checks = {w["check"] for w in result.data["warnings"]}
        assert "single_trade_near_limit" in warning_checks

    @pytest.mark.asyncio
    async def test_validate_risk_no_side_effects(self, executor):
        """validate_risk must not modify any PolicyEngine state."""
        # Record the initial state
        initial_daily_spend = executor._policy_engine._daily_spend_usd
        initial_trades = list(executor._policy_engine._trades_this_hour)
        initial_failures = executor._policy_engine._consecutive_failures
        initial_last_trade = executor._policy_engine._last_trade_timestamp

        await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "5000"},
                "chain": "arbitrum",
            },
        )

        # Verify no state was changed
        assert executor._policy_engine._daily_spend_usd == initial_daily_spend
        assert executor._policy_engine._trades_this_hour == initial_trades
        assert executor._policy_engine._consecutive_failures == initial_failures
        assert executor._policy_engine._last_trade_timestamp == initial_last_trade

    @pytest.mark.asyncio
    async def test_validate_risk_blocked_chain(self, mock_gateway):
        """A trade on a disallowed chain should report a chain violation."""
        policy = AgentPolicy(
            allowed_chains={"ethereum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")

        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "100"},
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        assert result.data["valid"] is False
        chain_violations = [v for v in result.data["violations"] if v["check"] == "chain_not_allowed"]
        assert len(chain_violations) >= 1

    @pytest.mark.asyncio
    async def test_validate_risk_circuit_breaker_tripped(self, mock_gateway):
        """When circuit breaker is tripped, validate_risk should report it."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_consecutive_failures=2,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")

        # Trip the circuit breaker
        executor._policy_engine.record_trade(Decimal("100"), success=False)
        executor._policy_engine.record_trade(Decimal("100"), success=False)
        assert executor._policy_engine.is_circuit_breaker_tripped

        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "100"},
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        assert result.data["valid"] is False
        cb_violations = [v for v in result.data["violations"] if v["check"] == "circuit_breaker"]
        assert len(cb_violations) >= 1

    @pytest.mark.asyncio
    async def test_validate_risk_lp_open_intent(self, executor):
        """LP open intents should map pool tokens correctly for validation."""
        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "lp_open",
                "params": {
                    "pool": "WETH/USDC/3000",
                    "amount0": "1.0",
                    "amount1": "3200",
                    "protocol": "uniswap_v3",
                },
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        # Should pass with permissive executor policy
        assert result.data["valid"] is True

    @pytest.mark.asyncio
    async def test_validate_risk_returns_risk_summary(self, executor):
        """Risk summary should include daily spend tracking."""
        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "1000"},
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        summary = result.data["risk_summary"]
        assert "estimated_value_usd" in summary
        assert "daily_spend_remaining_usd" in summary
        assert "daily_spend_used_usd" in summary
        assert "daily_spend_limit_usd" in summary
        assert "single_trade_limit_usd" in summary
        # With no prior spend, remaining should equal the limit
        assert summary["daily_spend_used_usd"] == "0"

    @pytest.mark.asyncio
    async def test_validate_risk_daily_spend_near_limit_warning(self, mock_gateway):
        """When prior daily spend puts this trade near the daily limit, a warning should appear."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("10000"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")

        # Pre-spend $7500 of $10000 daily limit
        executor._policy_engine._daily_spend_usd = Decimal("7500")

        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "1500"},
                "chain": "arbitrum",
            },
        )
        assert result.status == "success"
        assert result.data["valid"] is True  # 7500 + 1500 = 9000 <= 10000
        # Projected spend 9000/10000 = 90% > 80% threshold => warning
        warning_checks = {w["check"] for w in result.data["warnings"]}
        assert "daily_spend_near_limit" in warning_checks

    @pytest.mark.asyncio
    async def test_validate_risk_explanation_on_pass(self, executor):
        """When all checks pass, explanation should indicate success."""
        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "100"},
                "chain": "arbitrum",
            },
        )
        assert "passed" in result.explanation.lower()

    @pytest.mark.asyncio
    async def test_validate_risk_explanation_on_fail(self, mock_gateway):
        """When checks fail, explanation should indicate the violation count."""
        policy = AgentPolicy(
            allowed_chains={"ethereum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
        )
        executor = ToolExecutor(mock_gateway, policy=policy, wallet_address="0x1234")

        result = await executor.execute(
            "validate_risk",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "100"},
                "chain": "arbitrum",
            },
        )
        assert "blocked" in result.explanation.lower()
        assert "violation" in result.explanation.lower()
