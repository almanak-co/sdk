"""Tests for ToolExecutor action tools producing enriched results.

Verifies that action tools (swap, LP open/close) now go through the
IntentExecutionService pipeline and produce enriched response data
(position_id, swap_amounts, lp_close_data) instead of empty strings.
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
    """Create a ToolExecutor with permissive policy and no retries for fast tests."""
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
        max_retries=0,  # No retries for fast tests
    )


def _make_compile_resp(success=True, error=""):
    resp = MagicMock()
    resp.success = success
    resp.error = error
    resp.action_bundle = json.dumps({"actions": [{"type": "SWAP"}]}).encode()
    return resp


def _make_exec_resp(success=True, error="", tx_hashes=None, receipts=None):
    resp = MagicMock()
    resp.success = success
    resp.error = error
    resp.tx_hashes = tx_hashes or (["0xabc123"] if success else [])
    if receipts is not None:
        resp.receipts = json.dumps(receipts).encode()
    else:
        resp.receipts = b"[]"
    return resp


class TestToolExecutorEnrichedSwap:
    """Swap action tools should produce enriched response data."""

    @pytest.mark.asyncio
    async def test_swap_goes_through_inner_runner(self, executor, mock_gateway):
        """Swap tool execution routes through IntentExecutionService."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp()

        # Mock GetPrice for spend tracking
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
    async def test_swap_dry_run_still_works(self, executor, mock_gateway):
        """Dry run should work through the new pipeline."""
        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(success=True, tx_hashes=[])

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "1000", "dry_run": True, "chain": "arbitrum"},
        )

        assert result.status == "simulated"


class TestToolExecutorEnrichedLPOpen:
    """LP open action tools should extract position_id from enrichment."""

    @pytest.mark.asyncio
    async def test_lp_open_extracts_position_id(self, executor, mock_gateway):
        """LP open should extract position_id via ResultEnricher."""
        # ERC-721 Transfer(address from, address to, uint256 tokenId)
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        zero_address_padded = "0x" + "0" * 64
        token_id = 12345
        token_id_padded = "0x" + hex(token_id)[2:].zfill(64)
        recipient_padded = "0x" + "1234567890abcdef1234567890abcdef12345678".zfill(64)

        receipt = {
            "status": "0x1",
            "transactionHash": "0xdef456",
            "gasUsed": "0x5208",
            "logs": [
                {
                    "topics": [transfer_topic, zero_address_padded, recipient_padded, token_id_padded],
                    "data": "0x",
                    "address": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
                }
            ],
        }

        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=True,
            tx_hashes=["0xdef456"],
            receipts=[receipt],
        )

        # Mock GetPrice for spend tracking
        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        result = await executor.execute(
            "open_lp_position",
            {
                "token_a": "ETH",
                "token_b": "USDC",
                "amount_a": "1.0",
                "amount_b": "3000",
                "price_lower": "2500",
                "price_upper": "3500",
                "chain": "arbitrum",
                "protocol": "uniswap_v3",
            },
        )

        assert result.status == "success"
        assert result.data["position_id"] == token_id
        assert result.data["tx_hash"] == "0xdef456"


class TestToolExecutorRetryIntegration:
    """Verify that retry logic is active for action tools."""

    @pytest.mark.asyncio
    async def test_executor_with_retries_recovers(self, mock_gateway):
        """Action tool with retries enabled should recover from transient failures."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
            require_rebalance_check=False,
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            max_retries=2,
            initial_retry_delay=0.01,  # Fast for tests
        )

        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.side_effect = [
            _make_exec_resp(success=False, error="gateway timeout"),
            _make_exec_resp(success=True, tx_hashes=["0xrecovered"]),
        ]

        # Mock GetPrice for spend tracking
        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )

        # Should succeed after retry
        assert result.status == "success"
        assert result.data["tx_hash"] == "0xrecovered"

    @pytest.mark.asyncio
    async def test_executor_non_retryable_fails_fast(self, mock_gateway):
        """Non-retryable errors should fail immediately without retries."""
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
            require_rebalance_check=False,
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            max_retries=5,  # Many retries, but should not use them
            initial_retry_delay=0.01,
        )

        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=False, error="execution reverted: INSUFFICIENT_INPUT_AMOUNT"
        )

        # Mock GetPrice for spend tracking
        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )

        # Should fail without retries (only 1 call to Execute)
        assert result.status == "error"
        assert mock_gateway.execution.Execute.call_count == 1


class TestToolExecutorSadflowIntegration:
    """Verify that sadflow hooks fire for action tool failures."""

    @pytest.mark.asyncio
    async def test_sadflow_fires_alert_on_failure(self, mock_gateway):
        """Sadflow should fire an alert via alert_manager on failure."""
        alert_manager = MagicMock()

        # Make send_alert return an awaitable (async function)
        async def _async_noop(**kwargs):
            pass

        alert_manager.send_alert = MagicMock(side_effect=_async_noop)

        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            max_tool_calls_per_minute=100,
            cooldown_seconds=0,
            max_single_trade_usd=Decimal("999999999"),
            max_daily_spend_usd=Decimal("999999999"),
            max_position_size_usd=Decimal("999999999"),
            require_human_approval_above_usd=Decimal("999999999"),
            require_rebalance_check=False,
            max_consecutive_failures=999,  # Don't trip circuit breaker
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            alert_manager=alert_manager,
            max_retries=0,
        )

        mock_gateway.execution.CompileIntent.return_value = _make_compile_resp()
        mock_gateway.execution.Execute.return_value = _make_exec_resp(
            success=False, error="some transient error"
        )

        # Mock GetPrice for spend tracking
        price_resp = MagicMock()
        price_resp.price = "1.0"
        mock_gateway.market.GetPrice.return_value = price_resp

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )

        assert result.status == "error"
        # Alert should have been attempted (via _fire_alert in _on_sadflow_event)
        # Note: _fire_alert is fire-and-forget so we check the call was made
        assert alert_manager.send_alert.called
