"""Comprehensive integration tests exercising the full agent tool stack.

These tests simulate realistic agent workflows end-to-end:
- Full agent loop (data -> planning -> action -> state)
- Policy enforcement across multi-step sequences
- Multi-tool workflows (LP rebalancing)
- Error recovery patterns
- Sequential tool call safety
- State consistency after trades

Each test uses a mocked GatewayClient following the established patterns
from test_executor.py and test_policy.py.
"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.agent_tools.catalog import RiskTier, ToolCategory, ToolDefinition, get_default_catalog
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy, PolicyEngine
from almanak.framework.agent_tools.schemas import (
    SwapTokensRequest,
    SwapTokensResponse,
    ToolResponse,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_mock_gateway():
    """Create a mock GatewayClient with service stubs for all gRPC services."""
    client = MagicMock()
    client.is_connected = True
    return client


def _setup_price_response(mock_gateway, price="3200.50", source="coingecko"):
    """Configure mock gateway to return a price response."""
    mock_resp = MagicMock()
    mock_resp.price = price
    mock_resp.source = source
    mock_resp.timestamp = 1700000000
    mock_gateway.market.GetPrice.return_value = mock_resp


def _setup_balance_response(mock_gateway, balance="5000.00", balance_usd="5000.00"):
    """Configure mock gateway to return a balance response."""
    mock_resp = MagicMock()
    mock_resp.balance = balance
    mock_resp.balance_usd = balance_usd
    mock_gateway.market.GetBalance.return_value = mock_resp


def _setup_batch_balance_response(mock_gateway, balances):
    """Configure mock gateway to return batch balance responses.

    Args:
        balances: list of (balance, balance_usd) tuples
    """
    mock_resp = MagicMock()
    responses = []
    for bal, bal_usd in balances:
        r = MagicMock()
        r.balance = bal
        r.balance_usd = bal_usd
        r.error = ""
        responses.append(r)
    mock_resp.responses = responses
    mock_gateway.market.BatchGetBalances.return_value = mock_resp


def _setup_compile_response(mock_gateway, success=True, actions=None, error=""):
    """Configure mock gateway to return a compile response."""
    mock_resp = MagicMock()
    mock_resp.success = success
    mock_resp.action_bundle = json.dumps({"actions": actions if actions is not None else [{"type": "SWAP"}]}).encode()
    mock_resp.error = error
    mock_gateway.execution.CompileIntent.return_value = mock_resp


def _setup_execute_response(mock_gateway, success=True, tx_hashes=None, error=""):
    """Configure mock gateway to return an execution response."""
    mock_resp = MagicMock()
    mock_resp.success = success
    mock_resp.tx_hashes = tx_hashes if tx_hashes is not None else (["0xabc123"] if success else [])
    mock_resp.error = error
    mock_resp.receipts = None
    mock_gateway.execution.Execute.return_value = mock_resp


def _setup_indicator_response(mock_gateway, value="45.0", signal="neutral"):
    """Configure mock gateway to return an indicator response."""
    mock_resp = MagicMock()
    mock_resp.value = value
    mock_resp.metadata = {"signal": signal}
    mock_gateway.market.GetIndicator.return_value = mock_resp


def _setup_state_save_response(mock_gateway, success=True, new_version=1, checksum="abc123"):
    """Configure mock gateway to return a state save response."""
    mock_resp = MagicMock()
    mock_resp.success = success
    mock_resp.new_version = new_version
    mock_resp.checksum = checksum
    mock_gateway.state.SaveState.return_value = mock_resp


def _setup_state_load_response(mock_gateway, state=None, version=0):
    """Configure mock gateway to return a state load response."""
    mock_resp = MagicMock()
    mock_resp.data = json.dumps(state or {}).encode()
    mock_resp.version = version
    mock_gateway.state.LoadState.return_value = mock_resp


def _setup_observe_response(mock_gateway):
    """Configure mock gateway's observe service to succeed."""
    mock_resp = MagicMock()
    mock_gateway.observe.RecordTimelineEvent.return_value = mock_resp


def _make_executor(
    mock_gateway,
    *,
    allowed_tokens=None,
    allowed_protocols=None,
    allowed_chains=None,
    max_single_trade_usd=Decimal("999999999"),
    max_daily_spend_usd=Decimal("999999999"),
    max_position_size_usd=Decimal("999999999"),
    max_trades_per_hour=100,
    max_tool_calls_per_minute=100,
    cooldown_seconds=0,
    require_rebalance_check=False,
    require_human_approval_above_usd=Decimal("999999999"),
    require_simulation_before_execution=True,
    max_consecutive_failures=3,
):
    """Create a ToolExecutor with configurable policy for integration testing."""
    policy = AgentPolicy(
        allowed_tokens=set(allowed_tokens) if allowed_tokens is not None else None,
        allowed_protocols=set(allowed_protocols) if allowed_protocols is not None else None,
        allowed_chains=set(allowed_chains) if allowed_chains is not None else {"arbitrum"},
        max_single_trade_usd=max_single_trade_usd,
        max_daily_spend_usd=max_daily_spend_usd,
        max_position_size_usd=max_position_size_usd,
        max_trades_per_hour=max_trades_per_hour,
        max_tool_calls_per_minute=max_tool_calls_per_minute,
        cooldown_seconds=cooldown_seconds,
        require_rebalance_check=require_rebalance_check,
        require_human_approval_above_usd=require_human_approval_above_usd,
        require_simulation_before_execution=require_simulation_before_execution,
        max_consecutive_failures=max_consecutive_failures,
    )
    return ToolExecutor(
        mock_gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        strategy_id="test-strategy-integration",
    )


# ===========================================================================
# Test 1: Full Agent Loop Simulation
# ===========================================================================


class TestFullAgentLoopSimulation:
    """Simulate a complete agent loop: data -> planning -> action -> state.

    This exercises the core workflow an LLM agent would perform:
    1. Get portfolio data
    2. Check market indicators
    3. Execute a trade
    4. Record the decision for audit trail
    """

    @pytest.fixture
    def mock_gw(self):
        gw = _make_mock_gateway()
        _setup_observe_response(gw)
        return gw

    @pytest.mark.asyncio
    async def test_full_loop_get_balance_then_swap_then_record(self, mock_gw):
        """Full agent loop: get_balance -> swap_tokens -> record_agent_decision."""
        executor = _make_executor(mock_gw, allowed_tokens=["USDC", "ETH", "WETH"])

        # Step 1: Agent checks balance
        _setup_balance_response(mock_gw, balance="5000.00", balance_usd="5000.00")
        result = await executor.execute("get_balance", {"token": "USDC", "chain": "arbitrum"})
        assert result.status == "success"
        assert result.data["balance"] == "5000.00"

        # Step 2: Agent checks ETH price
        _setup_price_response(mock_gw, price="3200.50")
        result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        assert result.status == "success"
        assert result.data["price_usd"] == 3200.50

        # Step 3: Agent checks RSI indicator
        _setup_indicator_response(mock_gw, value="35.0", signal="oversold")
        result = await executor.execute(
            "get_indicator", {"token": "ETH", "indicator": "rsi", "period": 14}
        )
        assert result.status == "success"
        assert result.data["signal"] == "oversold"

        # Step 4: Agent executes a swap
        _setup_compile_response(mock_gw, actions=[{"type": "APPROVE"}, {"type": "SWAP"}])
        _setup_execute_response(mock_gw, tx_hashes=["0xswap001"])
        _setup_price_response(mock_gw, price="1.0")  # USDC price for spend tracking

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "1000", "chain": "arbitrum"},
        )
        assert result.status == "success"
        assert result.data["tx_hash"] == "0xswap001"

        # Step 5: Agent records the decision
        result = await executor.execute(
            "record_agent_decision",
            {
                "decision_summary": "RSI oversold at 35. Bought ETH with 1000 USDC.",
                "tool_calls": [
                    {"tool": "get_balance", "result": "5000 USDC"},
                    {"tool": "get_price", "result": "ETH $3200.50"},
                    {"tool": "swap_tokens", "result": "tx 0xswap001"},
                ],
                "intent_type": "swap",
            },
        )
        assert result.status == "success"
        assert result.data["recorded"] is True
        assert result.data["decision_id"] != ""

    @pytest.mark.asyncio
    async def test_full_loop_policy_state_updated_after_trades(self, mock_gw):
        """Verify PolicyEngine state (daily spend) is correctly incremented after trades."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
            max_daily_spend_usd=Decimal("10000"),
        )
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw, tx_hashes=["0xtx1"])
        _setup_price_response(mock_gw, price="1.0")

        # Verify initial state
        assert executor._policy_engine._daily_spend_usd == Decimal("0")

        # Execute first trade
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "2000", "chain": "arbitrum"},
        )
        assert result.status == "success"

        # Policy engine should have recorded the spend
        assert executor._policy_engine._daily_spend_usd > Decimal("0")

        # Execute second trade
        _setup_execute_response(mock_gw, tx_hashes=["0xtx2"])
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "3000", "chain": "arbitrum"},
        )
        assert result.status == "success"

        # Trades this hour should have 2 entries
        assert len(executor._policy_engine._trades_this_hour) == 2

    @pytest.mark.asyncio
    async def test_batch_get_balances_in_full_loop(self, mock_gw):
        """Agent uses batch_get_balances to get portfolio overview."""
        executor = _make_executor(mock_gw)
        _setup_batch_balance_response(mock_gw, [
            ("1.5", "4800.00"),
            ("5000.00", "5000.00"),
        ])

        result = await executor.execute(
            "batch_get_balances", {"chain": "arbitrum", "tokens": ["ETH", "USDC"]}
        )
        assert result.status == "success"
        assert len(result.data["balances"]) == 2
        assert Decimal(result.data["total_usd"]) == Decimal("9800.00")


# ===========================================================================
# Test 2: Policy Enforcement Chain
# ===========================================================================


class TestPolicyEnforcementChain:
    """Test strict policy enforcement across a sequence of trades.

    Configure a tight policy and verify:
    - Trades within limits succeed
    - Trades approaching limits eventually get blocked
    - Error responses include correct error codes and suggestions
    """

    @pytest.fixture
    def mock_gw(self):
        gw = _make_mock_gateway()
        _setup_observe_response(gw)
        return gw

    @pytest.mark.asyncio
    async def test_daily_spend_limit_eventually_hit(self, mock_gw):
        """Sequence of trades approaches and then exceeds the daily spend limit."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
            max_single_trade_usd=Decimal("3000"),
            max_daily_spend_usd=Decimal("5000"),
        )
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw)
        _setup_price_response(mock_gw, price="1.0")  # USDC = $1

        # Trade 1: $2000 -- should succeed
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "2000", "chain": "arbitrum"},
        )
        assert result.status == "success"

        # Trade 2: $2000 -- should succeed (total $4000 < $5000)
        _setup_execute_response(mock_gw, tx_hashes=["0xtx2"])
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "2000", "chain": "arbitrum"},
        )
        assert result.status == "success"

        # Trade 3: $2000 -- should be BLOCKED (total would be $6000 > $5000)
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "2000", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        assert "daily limit" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_single_trade_limit_enforced(self, mock_gw):
        """Trade exceeding single-trade limit is blocked."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
            max_single_trade_usd=Decimal("1000"),
        )
        _setup_price_response(mock_gw, price="1.0")  # USDC = $1 for spend estimation

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "1500", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        assert "single-trade limit" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_blocked_token_returns_informative_error(self, mock_gw):
        """Using a token not in the allowed set returns an informative error."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
        )

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "WBTC", "token_out": "USDC", "amount": "1", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        assert "WBTC" in result.error["message"]

    @pytest.mark.asyncio
    async def test_blocked_chain_returns_informative_error(self, mock_gw):
        """Using a chain not in the allowed set returns an informative error."""
        executor = _make_executor(
            mock_gw,
            allowed_chains=["arbitrum"],
        )

        result = await executor.execute(
            "get_price", {"token": "ETH", "chain": "ethereum"}
        )
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        assert "ethereum" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_blocked_protocol_returns_informative_error(self, mock_gw):
        """Using a protocol not in the allowed set returns an informative error."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
            allowed_protocols=["uniswap_v3"],
        )

        result = await executor.execute(
            "swap_tokens",
            {
                "token_in": "USDC",
                "token_out": "ETH",
                "amount": "100",
                "chain": "arbitrum",
                "protocol": "sushiswap_v3",
            },
        )
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        assert "sushiswap_v3" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_circuit_breaker_trips_after_consecutive_failures(self, mock_gw):
        """Circuit breaker blocks trades after N consecutive failures."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
            max_consecutive_failures=2,
        )
        _setup_compile_response(mock_gw)
        _setup_price_response(mock_gw, price="1.0")

        # Simulate 2 failed trades by recording failures directly on the policy engine
        executor._policy_engine.record_trade(Decimal("100"), success=False)
        executor._policy_engine.record_trade(Decimal("100"), success=False)

        # Next trade should be blocked by circuit breaker
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        assert "circuit breaker" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_trade_rate_limit_enforced(self, mock_gw):
        """Trade rate limit blocks after max_trades_per_hour."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
            max_trades_per_hour=2,
        )
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw)
        _setup_price_response(mock_gw, price="1.0")

        # Execute 2 trades (at limit)
        for i in range(2):
            _setup_execute_response(mock_gw, tx_hashes=[f"0xtx{i}"])
            result = await executor.execute(
                "swap_tokens",
                {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
            )
            assert result.status == "success", f"Trade {i} should succeed"

        # 3rd trade should be rate-limited
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        assert "rate limit" in result.error["message"].lower()


# ===========================================================================
# Test 3: Multi-Tool Workflow (LP Rebalancing)
# ===========================================================================


class TestMultiToolWorkflow:
    """Simulate an LP rebalancing workflow.

    Workflow: compile_intent -> execute_compiled_bundle
    This tests the compile-then-execute two-step pattern.
    """

    @pytest.fixture
    def mock_gw(self):
        gw = _make_mock_gateway()
        _setup_observe_response(gw)
        return gw

    @pytest.mark.asyncio
    async def test_compile_then_execute_workflow(self, mock_gw):
        """Full compile -> execute workflow for a swap intent."""
        executor = _make_executor(mock_gw)

        # Step 1: Compile a swap intent
        _setup_compile_response(mock_gw, actions=[{"type": "APPROVE"}, {"type": "SWAP"}])
        compile_result = await executor.execute(
            "compile_intent",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "1000"},
                "chain": "arbitrum",
            },
        )
        assert compile_result.status == "success"
        bundle_id = compile_result.data["bundle_id"]
        assert len(compile_result.data["actions"]) == 2

        # Step 2: Execute the compiled bundle
        _setup_execute_response(mock_gw, tx_hashes=["0xexec001"])
        _setup_price_response(mock_gw, price="1.0")
        exec_result = await executor.execute(
            "execute_compiled_bundle",
            {"bundle_id": bundle_id, "chain": "arbitrum"},
        )
        assert exec_result.status == "success"
        assert exec_result.data["success"] is True
        assert exec_result.data["tx_hashes"] == ["0xexec001"]

    @pytest.mark.asyncio
    async def test_compiled_bundle_is_one_shot(self, mock_gw):
        """Compiled bundles are consumed after execution (one-shot)."""
        executor = _make_executor(mock_gw)
        _setup_compile_response(mock_gw)

        compile_result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {"from_token": "USDC", "to_token": "ETH", "amount": "500"}},
        )
        bundle_id = compile_result.data["bundle_id"]

        # First execution succeeds
        _setup_execute_response(mock_gw)
        _setup_price_response(mock_gw, price="1.0")
        result = await executor.execute("execute_compiled_bundle", {"bundle_id": bundle_id})
        assert result.status == "success"

        # Second attempt fails -- bundle consumed
        result = await executor.execute("execute_compiled_bundle", {"bundle_id": bundle_id})
        assert result.status == "error"
        assert "not found" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_compile_execute_cross_chain_blocked(self, mock_gw):
        """Bundle compiled for one chain cannot be executed on another."""
        executor = _make_executor(mock_gw, allowed_chains=["arbitrum", "base"])
        _setup_compile_response(mock_gw)

        compile_result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {}, "chain": "arbitrum"},
        )
        bundle_id = compile_result.data["bundle_id"]

        # Attempt to execute on a different chain
        result = await executor.execute(
            "execute_compiled_bundle",
            {"bundle_id": bundle_id, "chain": "base"},
        )
        assert result.status == "error"
        assert "compiled for chain" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_simulate_then_execute_workflow(self, mock_gw):
        """Simulate intent, then compile and execute."""
        executor = _make_executor(mock_gw)

        # Step 1: Simulate
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw, tx_hashes=[])
        sim_result = await executor.execute(
            "simulate_intent",
            {
                "intent_type": "swap",
                "params": {"from_token": "USDC", "to_token": "ETH", "amount": "1000"},
                "chain": "arbitrum",
            },
        )
        assert sim_result.status == "simulated"
        assert sim_result.data["success"] is True

        # Step 2: Now compile
        _setup_compile_response(mock_gw, actions=[{"type": "SWAP"}])
        compile_result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {"from_token": "USDC", "to_token": "ETH", "amount": "1000"}},
        )
        assert compile_result.status == "success"
        bundle_id = compile_result.data["bundle_id"]

        # Step 3: Execute
        _setup_execute_response(mock_gw, tx_hashes=["0xfinal"])
        _setup_price_response(mock_gw, price="1.0")
        exec_result = await executor.execute(
            "execute_compiled_bundle",
            {"bundle_id": bundle_id, "chain": "arbitrum"},
        )
        assert exec_result.status == "success"


# ===========================================================================
# Test 4: Error Recovery Pattern
# ===========================================================================


class TestErrorRecoveryPattern:
    """Test agent error recovery: policy denial -> adapt -> succeed.

    Verifies that error responses from policy denials contain enough
    information for the agent to adjust its approach.
    """

    @pytest.fixture
    def mock_gw(self):
        gw = _make_mock_gateway()
        _setup_observe_response(gw)
        return gw

    @pytest.mark.asyncio
    async def test_blocked_token_then_adapt_with_allowed_token(self, mock_gw):
        """Agent tries blocked token, gets informative error, then uses allowed token."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
        )

        # Attempt with blocked token
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "WBTC", "token_out": "USDC", "amount": "1", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        # Error should mention the blocked token and suggest allowed alternatives
        assert "WBTC" in result.error["message"]
        assert result.error["recoverable"] is False
        # The suggestion should list allowed tokens
        assert "suggestion" in result.error

        # Agent adapts: use an allowed token instead
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw, tx_hashes=["0xrecovery"])
        _setup_price_response(mock_gw, price="1.0")
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "success"
        assert result.data["tx_hash"] == "0xrecovery"

    @pytest.mark.asyncio
    async def test_over_limit_then_reduce_amount(self, mock_gw):
        """Agent tries trade over limit, gets error with limit info, reduces amount."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
            max_single_trade_usd=Decimal("5000"),
        )
        _setup_price_response(mock_gw, price="1.0")  # USDC = $1 for spend estimation

        # Attempt over the limit
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "8000", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert "single-trade limit" in result.error["message"].lower()
        assert "suggestion" in result.error
        assert "5000" in result.error["suggestion"]

        # Agent reduces amount
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw, tx_hashes=["0xreduced"])
        _setup_price_response(mock_gw, price="1.0")
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "4000", "chain": "arbitrum"},
        )
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_circuit_breaker_reset_after_success(self, mock_gw):
        """Circuit breaker resets after a successful trade."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
            max_consecutive_failures=3,
        )

        # Record 2 failures (just below circuit breaker threshold of 3)
        executor._policy_engine.record_trade(Decimal("100"), success=False)
        executor._policy_engine.record_trade(Decimal("100"), success=False)
        assert executor._policy_engine._consecutive_failures == 2

        # A successful trade resets the counter
        executor._policy_engine.record_trade(Decimal("100"), success=True)
        assert executor._policy_engine._consecutive_failures == 0

        # So the next trade should pass the circuit breaker check
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw, tx_hashes=["0xok"])
        _setup_price_response(mock_gw, price="1.0")
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_unknown_tool_error_is_recoverable(self, mock_gw):
        """Unknown tool returns a validation error with recoverable=True."""
        executor = _make_executor(mock_gw)

        result = await executor.execute("nonexistent_tool", {})
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"
        assert result.error["recoverable"] is True

    @pytest.mark.asyncio
    async def test_invalid_args_error_is_recoverable(self, mock_gw):
        """Invalid arguments return a validation error with recoverable=True."""
        executor = _make_executor(mock_gw)

        # Missing required 'token' field
        result = await executor.execute("get_price", {})
        assert result.status == "error"
        assert result.error["error_code"] == "validation_error"
        assert result.error["recoverable"] is True


# ===========================================================================
# Test 5: Sequential Tool Call Safety
# ===========================================================================


class TestSequentialToolCallSafety:
    """Verify no state leakage between sequential tool calls.

    Call multiple data tools in sequence and verify:
    - Each returns independent results
    - No cross-contamination of state
    - Tool call rate limiting is respected
    """

    @pytest.fixture
    def mock_gw(self):
        gw = _make_mock_gateway()
        _setup_observe_response(gw)
        return gw

    @pytest.mark.asyncio
    async def test_sequential_data_calls_independent(self, mock_gw):
        """Sequential get_price calls return independent results."""
        executor = _make_executor(mock_gw)

        # First call: ETH price
        _setup_price_response(mock_gw, price="3200.50")
        result1 = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        assert result1.status == "success"
        assert result1.data["token"] == "ETH"
        assert result1.data["price_usd"] == 3200.50

        # Second call: BTC price (different response)
        _setup_price_response(mock_gw, price="62000.00")
        result2 = await executor.execute("get_price", {"token": "BTC", "chain": "arbitrum"})
        assert result2.status == "success"
        assert result2.data["token"] == "BTC"
        assert result2.data["price_usd"] == 62000.00

        # Results are independent
        assert result1.data["price_usd"] != result2.data["price_usd"]

    @pytest.mark.asyncio
    async def test_data_tools_dont_affect_action_tool_state(self, mock_gw):
        """Data tool calls should not affect policy state for action tools."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
        )

        # Multiple data calls
        _setup_price_response(mock_gw, price="3200.50")
        for _ in range(5):
            result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
            assert result.status == "success"

        # Data calls should not increment daily spend
        assert executor._policy_engine._daily_spend_usd == Decimal("0")

        # Data calls should not add to trades_this_hour
        assert len(executor._policy_engine._trades_this_hour) == 0

    @pytest.mark.asyncio
    async def test_tool_call_rate_limit_across_calls(self, mock_gw):
        """Tool call rate limit is enforced across all tool types.

        Note: record_tool_call() is invoked BEFORE the policy check on each
        call, so with max_tool_calls_per_minute=N the Nth call already sees
        N recorded entries and is blocked.  We use limit=6 so that 5 calls
        succeed and the 6th is rejected.
        """
        executor = _make_executor(
            mock_gw,
            max_tool_calls_per_minute=6,
        )
        _setup_price_response(mock_gw, price="3200.50")

        # Make 5 calls (under the limit)
        for i in range(5):
            result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
            assert result.status == "success", f"Call {i} should succeed"

        # 6th call hits the limit (6 recorded >= 6)
        result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        assert "rate limit" in result.error["message"].lower()

    @pytest.mark.asyncio
    async def test_balance_and_price_calls_interleaved(self, mock_gw):
        """Interleaved balance and price calls work independently."""
        executor = _make_executor(mock_gw)

        _setup_price_response(mock_gw, price="3200.50")
        price_result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        assert price_result.status == "success"

        _setup_balance_response(mock_gw, balance="10.5", balance_usd="33600.00")
        balance_result = await executor.execute("get_balance", {"token": "ETH", "chain": "arbitrum"})
        assert balance_result.status == "success"
        assert balance_result.data["balance"] == "10.5"

        _setup_price_response(mock_gw, price="1.00")
        price_result2 = await executor.execute("get_price", {"token": "USDC", "chain": "arbitrum"})
        assert price_result2.status == "success"
        assert price_result2.data["price_usd"] == 1.00


# ===========================================================================
# Test 6: State Consistency
# ===========================================================================


class TestStateConsistency:
    """Verify that state (risk metrics, portfolio) reflects actual trades.

    Execute trades and verify:
    - Risk metrics reflect the actual portfolio after trades
    - Saved state can be loaded back consistently
    - State version tracking works for optimistic locking
    """

    @pytest.fixture
    def mock_gw(self):
        gw = _make_mock_gateway()
        _setup_observe_response(gw)
        return gw

    @pytest.mark.asyncio
    async def test_risk_metrics_reflect_portfolio(self, mock_gw):
        """get_risk_metrics returns correct portfolio value from balances."""
        executor = _make_executor(mock_gw)

        _setup_batch_balance_response(mock_gw, [
            ("1.5", "4800.00"),
            ("5000.00", "5000.00"),
        ])

        result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert result.status == "success"
        assert "portfolio_value_usd" in result.data
        # Portfolio value should reflect the batch balance total
        portfolio_value = Decimal(result.data["portfolio_value_usd"])
        assert portfolio_value == Decimal("9800.00")

    @pytest.mark.asyncio
    async def test_state_save_and_load_roundtrip(self, mock_gw):
        """Save agent state and load it back -- roundtrip consistency."""
        executor = _make_executor(mock_gw)

        # Save state
        test_state = {"last_action": "swap", "position_id": "12345", "eth_balance": "1.5"}
        _setup_state_save_response(mock_gw, new_version=1, checksum="abc123")
        save_result = await executor.execute(
            "save_agent_state", {"state": test_state}
        )
        assert save_result.status == "success"
        assert save_result.data["version"] == 1

        # Load state back
        _setup_state_load_response(mock_gw, state=test_state, version=1)
        load_result = await executor.execute("load_agent_state", {})
        assert load_result.status == "success"
        assert load_result.data["state"] == test_state
        assert load_result.data["version"] == 1

    @pytest.mark.asyncio
    async def test_state_version_tracking(self, mock_gw):
        """State versions are tracked for optimistic locking."""
        executor = _make_executor(mock_gw)

        # First save: version 0 -> 1
        _setup_state_save_response(mock_gw, new_version=1)
        await executor.execute("save_agent_state", {"state": {"v": 1}})

        # The executor should now track version 1
        assert executor._state_versions.get("test-strategy-integration") == 1

        # Load also updates version tracking
        _setup_state_load_response(mock_gw, state={"v": 1}, version=1)
        await executor.execute("load_agent_state", {})
        assert executor._state_versions.get("test-strategy-integration") == 1

        # Second save: version 1 -> 2
        _setup_state_save_response(mock_gw, new_version=2)
        await executor.execute("save_agent_state", {"state": {"v": 2}})
        assert executor._state_versions.get("test-strategy-integration") == 2

    @pytest.mark.asyncio
    async def test_trades_followed_by_risk_metrics_consistency(self, mock_gw):
        """Execute trades, then verify risk metrics reflect updated portfolio."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
        )

        # Execute a trade
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw, tx_hashes=["0xtrade1"])
        _setup_price_response(mock_gw, price="1.0")
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "1000", "chain": "arbitrum"},
        )
        assert result.status == "success"

        # Now check risk metrics -- should reflect portfolio after trade
        _setup_batch_balance_response(mock_gw, [
            ("0.3125", "1000.00"),  # ~1000 USDC worth of ETH
            ("4000.00", "4000.00"),  # Remaining USDC
        ])
        risk_result = await executor.execute("get_risk_metrics", {"chain": "arbitrum"})
        assert risk_result.status == "success"
        portfolio_value = Decimal(risk_result.data["portfolio_value_usd"])
        assert portfolio_value == Decimal("5000.00")

    @pytest.mark.asyncio
    async def test_daily_spend_resets_after_24h(self):
        """PolicyEngine resets daily spend after 24 hours."""
        policy = AgentPolicy(
            max_daily_spend_usd=Decimal("5000"),
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)

        # Record some spending
        engine.record_trade(Decimal("3000"), success=True)
        assert engine._daily_spend_usd == Decimal("3000")

        # Simulate 24h passing by backdating the day_start
        engine._day_start = time.time() - 86401

        # The next _check_spend_limits call should auto-reset
        swap_tool = ToolDefinition(
            name="swap_tokens",
            description="test",
            category=ToolCategory.ACTION,
            risk_tier=RiskTier.MEDIUM,
            request_schema=SwapTokensRequest,
            response_schema=SwapTokensResponse,
        )
        decision = engine.check(swap_tool, {"amount": "4000", "chain": "arbitrum"})
        # After reset, daily spend is 0, so 4000 < 5000 should be allowed
        assert decision.allowed is True


# ===========================================================================
# Test 7: Catalog & ToolResponse Envelope Consistency
# ===========================================================================


class TestCatalogAndEnvelopeConsistency:
    """Verify that the tool catalog and response envelopes are consistent."""

    def test_all_builtin_tools_have_valid_schemas(self):
        """Every built-in tool has non-null request/response schemas."""
        catalog = get_default_catalog()
        tools = catalog.list_tools()
        assert len(tools) > 10, "Expected at least 10 built-in tools"

        for tool in tools:
            assert tool.request_schema is not None, f"Tool {tool.name} missing request_schema"
            assert tool.response_schema is not None, f"Tool {tool.name} missing response_schema"
            assert tool.name, "Tool has empty name"
            assert tool.description, f"Tool {tool.name} has empty description"
            # JSON schema generation should not raise
            input_schema = tool.input_json_schema()
            assert "properties" in input_schema or "type" in input_schema

    def test_mcp_schema_generation(self):
        """MCP schema generation works for all tools."""
        catalog = get_default_catalog()
        mcp_tools = catalog.to_mcp_tools()
        assert len(mcp_tools) > 0
        for mcp_tool in mcp_tools:
            assert "name" in mcp_tool
            assert "description" in mcp_tool
            assert "inputSchema" in mcp_tool

    def test_openai_schema_generation(self):
        """OpenAI schema generation works for all tools."""
        catalog = get_default_catalog()
        openai_tools = catalog.to_openai_tools()
        assert len(openai_tools) > 0
        for oa_tool in openai_tools:
            assert oa_tool["type"] == "function"
            assert "function" in oa_tool
            assert "name" in oa_tool["function"]
            assert "parameters" in oa_tool["function"]

    def test_tool_categories_have_expected_distribution(self):
        """Verify category distribution: DATA > PLANNING > ACTION is roughly right."""
        catalog = get_default_catalog()
        data_tools = catalog.list_tools(category=ToolCategory.DATA)
        planning_tools = catalog.list_tools(category=ToolCategory.PLANNING)
        action_tools = catalog.list_tools(category=ToolCategory.ACTION)
        state_tools = catalog.list_tools(category=ToolCategory.STATE)

        assert len(data_tools) >= 5, "Expected at least 5 data tools"
        assert len(planning_tools) >= 3, "Expected at least 3 planning tools"
        assert len(action_tools) >= 5, "Expected at least 5 action tools"
        assert len(state_tools) >= 2, "Expected at least 2 state tools"

    def test_tool_response_envelope_fields(self):
        """ToolResponse has all expected fields."""
        # Success envelope
        success_resp = ToolResponse(
            status="success",
            data={"token": "ETH", "price_usd": 3200.50},
            decision_hints={"action": "hold"},
            explanation="ETH price is high",
        )
        assert success_resp.status == "success"
        assert success_resp.data is not None
        assert success_resp.error is None
        assert success_resp.decision_hints is not None

        # Error envelope
        error_resp = ToolResponse(
            status="error",
            error={"error_code": "risk_blocked", "message": "blocked", "recoverable": False},
        )
        assert error_resp.status == "error"
        assert error_resp.data is None
        assert error_resp.error["error_code"] == "risk_blocked"


# ===========================================================================
# Test 8: Policy Engine Direct Tests (integration-focused)
# ===========================================================================


class TestPolicyEngineIntegration:
    """Integration-level policy engine tests that exercise multiple checks together."""

    def test_stop_loss_blocks_after_drawdown(self):
        """Stop-loss triggers when portfolio drops below threshold."""
        policy = AgentPolicy(
            stop_loss_pct=Decimal("5.0"),
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)

        # Set up portfolio tracking: peak $10000, current $9400 (6% drawdown)
        engine.update_portfolio_value(Decimal("10000"))
        engine._current_portfolio_usd = Decimal("9400")

        swap_tool = ToolDefinition(
            name="swap_tokens",
            description="test",
            category=ToolCategory.ACTION,
            risk_tier=RiskTier.MEDIUM,
            request_schema=SwapTokensRequest,
            response_schema=SwapTokensResponse,
        )
        decision = engine.check(swap_tool, {"amount": "100", "chain": "arbitrum"})
        assert decision.allowed is False
        assert any("stop-loss" in v.lower() for v in decision.violations)

    def test_cooldown_blocks_rapid_trades(self):
        """Cooldown prevents rapid trading."""
        policy = AgentPolicy(cooldown_seconds=300)
        engine = PolicyEngine(policy)

        # Record a trade
        engine.record_trade(Decimal("100"), success=True)

        swap_tool = ToolDefinition(
            name="swap_tokens",
            description="test",
            category=ToolCategory.ACTION,
            risk_tier=RiskTier.MEDIUM,
            request_schema=SwapTokensRequest,
            response_schema=SwapTokensResponse,
        )
        decision = engine.check(swap_tool, {"amount": "100", "chain": "arbitrum"})
        assert decision.allowed is False
        assert any("cooldown" in v.lower() for v in decision.violations)

    def test_rebalance_gate_blocks_lp_without_check(self):
        """LP actions are blocked if compute_rebalance_candidate was not called first."""
        policy = AgentPolicy(
            require_rebalance_check=True,
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)

        lp_tool = ToolDefinition(
            name="open_lp_position",
            description="test",
            category=ToolCategory.ACTION,
            risk_tier=RiskTier.HIGH,
            request_schema=SwapTokensRequest,  # Reusing for simplicity
            response_schema=SwapTokensResponse,
        )
        decision = engine.check(lp_tool, {"amount": "100", "chain": "arbitrum"})
        assert decision.allowed is False
        assert any("compute_rebalance_candidate" in v for v in decision.violations)

    def test_rebalance_gate_opens_after_approval(self):
        """LP actions succeed after compute_rebalance_candidate sets approval."""
        policy = AgentPolicy(
            require_rebalance_check=True,
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)

        # Approve rebalance
        engine.set_rebalance_approved(True)

        lp_tool = ToolDefinition(
            name="open_lp_position",
            description="test",
            category=ToolCategory.ACTION,
            risk_tier=RiskTier.HIGH,
            request_schema=SwapTokensRequest,
            response_schema=SwapTokensResponse,
        )
        decision = engine.check(lp_tool, {"amount": "100", "chain": "arbitrum"})
        # Should pass the rebalance gate (may fail on other checks, but not this one)
        rebalance_violations = [v for v in decision.violations if "compute_rebalance_candidate" in v]
        assert len(rebalance_violations) == 0

    def test_multiple_policy_violations_reported_together(self):
        """Multiple violations are reported in a single decision."""
        policy = AgentPolicy(
            allowed_tokens={"USDC", "ETH"},
            allowed_chains={"arbitrum"},
            max_single_trade_usd=Decimal("100"),
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)

        swap_tool = ToolDefinition(
            name="swap_tokens",
            description="test",
            category=ToolCategory.ACTION,
            risk_tier=RiskTier.MEDIUM,
            request_schema=SwapTokensRequest,
            response_schema=SwapTokensResponse,
        )
        # Violates: token not allowed AND over spend limit AND wrong chain
        decision = engine.check(swap_tool, {
            "token_in": "WBTC",
            "token_out": "DOGE",
            "amount": "200",
            "chain": "polygon",
        })
        assert decision.allowed is False
        # Should have at least 2 violations (chain + tokens)
        assert len(decision.violations) >= 2


# ===========================================================================
# Test 9: Default Chain Injection
# ===========================================================================


class TestDefaultChainInjection:
    """Verify default chain is injected when not provided in arguments."""

    @pytest.fixture
    def mock_gw(self):
        gw = _make_mock_gateway()
        _setup_observe_response(gw)
        return gw

    @pytest.mark.asyncio
    async def test_default_chain_injected_for_data_tools(self, mock_gw):
        """get_price without chain uses the executor's default chain."""
        executor = _make_executor(mock_gw)
        _setup_price_response(mock_gw, price="3200.50")

        # Call without 'chain' argument
        result = await executor.execute("get_price", {"token": "ETH"})
        assert result.status == "success"

        # Verify the gateway was called (price request goes through)
        mock_gw.market.GetPrice.assert_called()

    @pytest.mark.asyncio
    async def test_explicit_chain_overrides_default(self, mock_gw):
        """Explicit chain parameter overrides the default."""
        executor = _make_executor(mock_gw, allowed_chains=["arbitrum", "base"])
        _setup_price_response(mock_gw, price="3200.50")

        result = await executor.execute("get_price", {"token": "ETH", "chain": "base"})
        assert result.status == "success"


# ===========================================================================
# Test 10: Dry Run Isolation
# ===========================================================================


class TestDryRunIsolation:
    """Verify that dry_run trades do not affect policy state."""

    @pytest.fixture
    def mock_gw(self):
        gw = _make_mock_gateway()
        _setup_observe_response(gw)
        return gw

    @pytest.mark.asyncio
    async def test_dry_run_does_not_increment_daily_spend(self, mock_gw):
        """Dry-run swaps should not count toward daily spend limits."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
        )
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw, tx_hashes=[])

        # Execute dry run
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "5000", "dry_run": True, "chain": "arbitrum"},
        )
        assert result.status == "simulated"

        # Daily spend should still be 0
        assert executor._policy_engine._daily_spend_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_dry_run_does_not_count_as_trade(self, mock_gw):
        """Dry-run swaps should not increment the trade count."""
        executor = _make_executor(
            mock_gw,
            allowed_tokens=["USDC", "ETH"],
            max_trades_per_hour=2,
        )
        _setup_compile_response(mock_gw)
        _setup_execute_response(mock_gw, tx_hashes=[])

        # Execute 5 dry runs (should not count toward trade limit)
        for _ in range(5):
            result = await executor.execute(
                "swap_tokens",
                {"token_in": "USDC", "token_out": "ETH", "amount": "100", "dry_run": True, "chain": "arbitrum"},
            )
            assert result.status == "simulated"

        # Real trade should still work (trade counter not incremented by dry runs)
        _setup_execute_response(mock_gw, tx_hashes=["0xreal"])
        _setup_price_response(mock_gw, price="1.0")
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "success"
