"""Tests for MockGatewayClient -- validates the mock fixture and demonstrates
how to write agent E2E tests without real infrastructure.

These tests exercise the full ToolExecutor -> PolicyEngine -> MockGateway path
with REAL policy checks (not mocked).
"""

from datetime import datetime
from decimal import Decimal

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.testing import MockCall, MockGatewayClient, MockGatewayConfig


# ---------------------------------------------------------------------------
# Fixtures (self-contained; also available via tests/fixtures/agent_test_fixtures.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_gw():
    """Fresh MockGatewayClient with common token setup."""
    gw = MockGatewayClient()
    gw.set_price("ETH", Decimal("2000"))
    gw.set_price("USDC", Decimal("1"))
    gw.set_price("WETH", Decimal("2000"))
    gw.set_balance("USDC", "arbitrum", Decimal("10000"))
    gw.set_balance("ETH", "arbitrum", Decimal("5"))
    return gw


def _make_executor(
    gw: MockGatewayClient,
    *,
    max_daily_spend_usd: Decimal = Decimal("100000"),
    max_single_trade_usd: Decimal = Decimal("100000"),
    allowed_tokens: set[str] | None = None,
    cooldown_seconds: int = 0,
) -> ToolExecutor:
    """Helper to build a ToolExecutor with the mock gateway."""
    policy = AgentPolicy(
        allowed_tokens=allowed_tokens or {"USDC", "ETH", "WETH"},
        allowed_protocols={"uniswap_v3"},
        allowed_chains={"arbitrum"},
        max_single_trade_usd=max_single_trade_usd,
        max_daily_spend_usd=max_daily_spend_usd,
        max_position_size_usd=Decimal("999999"),
        require_human_approval_above_usd=Decimal("999999"),
        max_tool_calls_per_minute=200,
        cooldown_seconds=cooldown_seconds,
        require_rebalance_check=False,
        require_simulation_before_execution=False,
    )
    return ToolExecutor(
        gw,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        strategy_id="test-strategy",
    )


# ---------------------------------------------------------------------------
# MockGatewayClient unit tests
# ---------------------------------------------------------------------------


class TestMockGatewayClientBasics:
    def test_is_connected(self):
        gw = MockGatewayClient()
        assert gw.is_connected is True

    def test_set_and_get_price(self):
        gw = MockGatewayClient()
        gw.set_price("ETH", Decimal("3000"))
        assert gw._prices["ETH"] == Decimal("3000")

    def test_set_and_get_balance(self):
        gw = MockGatewayClient()
        gw.set_balance("USDC", "arbitrum", Decimal("5000"))
        assert gw._balances[("USDC", "arbitrum")] == Decimal("5000")

    def test_call_log_starts_empty(self):
        gw = MockGatewayClient()
        assert gw.call_log == []

    def test_reset_clears_calls(self):
        gw = MockGatewayClient()
        gw.set_price("ETH", Decimal("2000"))
        gw.market.GetPrice(type("R", (), {"token": "ETH"})())
        assert len(gw.call_log) == 1
        gw.reset()
        assert len(gw.call_log) == 0
        # Prices should be preserved after reset()
        assert gw._prices["ETH"] == Decimal("2000")

    def test_reset_all_clears_everything(self):
        gw = MockGatewayClient()
        gw.set_price("ETH", Decimal("2000"))
        gw.market.GetPrice(type("R", (), {"token": "ETH"})())
        gw.reset_all()
        assert len(gw.call_log) == 0
        assert len(gw._prices) == 0

    def test_assert_called_passes(self):
        gw = MockGatewayClient()
        gw.set_price("ETH", Decimal("2000"))
        gw.market.GetPrice(type("R", (), {"token": "ETH"})())
        gw.assert_called("GetPrice")
        gw.assert_called("GetPrice", times=1)

    def test_assert_called_fails(self):
        gw = MockGatewayClient()
        with pytest.raises(AssertionError, match="Expected GetPrice"):
            gw.assert_called("GetPrice")

    def test_assert_not_called_passes(self):
        gw = MockGatewayClient()
        gw.assert_not_called("GetPrice")

    def test_assert_not_called_fails(self):
        gw = MockGatewayClient()
        gw.market.GetPrice(type("R", (), {"token": "ETH"})())
        with pytest.raises(AssertionError, match="never be called"):
            gw.assert_not_called("GetPrice")

    def test_mock_call_dataclass(self):
        call = MockCall(tool_name="get_price", method="GetPrice", args={"token": "ETH"})
        assert call.tool_name == "get_price"
        assert call.method == "GetPrice"
        assert isinstance(call.timestamp, datetime)


class TestMockMarketService:
    def test_get_price_returns_configured_price(self):
        gw = MockGatewayClient()
        gw.set_price("ETH", Decimal("2500"))
        resp = gw.market.GetPrice(type("R", (), {"token": "ETH"})())
        assert resp.price == "2500"
        assert resp.source == "mock"

    def test_get_price_returns_zero_for_unknown(self):
        gw = MockGatewayClient()
        resp = gw.market.GetPrice(type("R", (), {"token": "UNKNOWN"})())
        assert resp.price == "0"

    def test_get_balance_returns_configured_balance(self):
        gw = MockGatewayClient()
        gw.set_balance("USDC", "arbitrum", Decimal("5000"))
        gw.set_price("USDC", Decimal("1"))
        resp = gw.market.GetBalance(type("R", (), {"token": "USDC", "chain": "arbitrum"})())
        assert resp.balance == "5000"
        assert resp.balance_usd == "5000"

    def test_batch_get_balances(self):
        gw = MockGatewayClient()
        gw.set_balance("ETH", "arbitrum", Decimal("2"))
        gw.set_price("ETH", Decimal("2000"))
        req = type("R", (), {"token": "ETH", "chain": "arbitrum"})()
        batch = type("B", (), {"requests": [req]})()
        resp = gw.market.BatchGetBalances(batch)
        assert len(resp.responses) == 1
        assert resp.responses[0].balance == "2"


class TestMockExecutionService:
    def test_compile_success_by_default(self):
        gw = MockGatewayClient()
        req = type("R", (), {"intent_type": "swap", "chain": "arbitrum", "intent_data": b"{}",
                             "wallet_address": "0x"})()
        resp = gw.execution.CompileIntent(req)
        assert resp.success is True
        assert resp.action_bundle != b""

    def test_compile_custom_failure(self):
        gw = MockGatewayClient()
        gw.set_compile_result("swap", success=False, error="bad params")
        req = type("R", (), {"intent_type": "swap", "chain": "arbitrum"})()
        resp = gw.execution.CompileIntent(req)
        assert resp.success is False
        assert resp.error == "bad params"

    def test_execute_success_by_default(self):
        gw = MockGatewayClient()
        req = type("R", (), {"dry_run": False, "action_bundle": b"{}"})()
        resp = gw.execution.Execute(req)
        assert resp.success is True
        assert resp.tx_hashes == ["0xmock_tx_hash"]

    def test_execute_custom_failure(self):
        gw = MockGatewayClient()
        gw.set_execute_result(success=False, error="revert")
        req = type("R", (), {"dry_run": False, "action_bundle": b"{}"})()
        resp = gw.execution.Execute(req)
        assert resp.success is False


class TestMockStateService:
    def test_save_and_load_state(self):
        gw = MockGatewayClient()
        save_req = type("R", (), {"strategy_id": "test", "data": b'{"key":"value"}',
                                  "expected_version": 0, "schema_version": 1})()
        save_resp = gw.state.SaveState(save_req)
        assert save_resp.success is True
        assert save_resp.new_version == 1

        load_req = type("R", (), {"strategy_id": "test"})()
        load_resp = gw.state.LoadState(load_req)
        assert load_resp.data == b'{"key":"value"}'
        assert load_resp.version == 1

    def test_load_missing_state_raises(self):
        gw = MockGatewayClient()
        load_req = type("R", (), {"strategy_id": "nonexistent"})()
        with pytest.raises(Exception, match="state not found"):
            gw.state.LoadState(load_req)


# ---------------------------------------------------------------------------
# Integration tests: ToolExecutor + MockGateway + REAL PolicyEngine
# ---------------------------------------------------------------------------


class TestAgentDailySpendLimit:
    """Test: agent respects daily spend limit via real PolicyEngine."""

    @pytest.mark.asyncio
    async def test_daily_spend_limit_blocks_excess(self, mock_gw):
        """Execute swaps totaling > daily limit; last one should be rejected."""
        executor = _make_executor(
            mock_gw,
            max_daily_spend_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("10000"),
        )

        # First swap: $5000 USDC -> ETH (within limit)
        result1 = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "5000", "chain": "arbitrum"},
        )
        assert result1.status == "success"

        # Second swap: $6000 USDC -> ETH (projected total $11000 > $10000 limit)
        result2 = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "6000", "chain": "arbitrum"},
        )
        assert result2.status == "error"
        assert result2.error["error_code"] == "risk_blocked"
        assert "daily" in result2.error["message"].lower() or "spend" in result2.error["message"].lower()

        # Verify the mock recorded both attempts
        compile_calls = mock_gw.get_calls("CompileIntent")
        # First swap compiled successfully; second was blocked before compilation
        assert len(compile_calls) == 1


class TestAgentTokenAllowlist:
    """Test: agent respects token allowlist via real PolicyEngine."""

    @pytest.mark.asyncio
    async def test_disallowed_token_blocked(self, mock_gw):
        """Attempt to swap to a token not in the allowlist."""
        executor = _make_executor(mock_gw, allowed_tokens={"USDC", "ETH"})

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "BADTOKEN", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert result.error["error_code"] == "risk_blocked"
        assert "BADTOKEN" in result.error["message"]

        # Gateway should never be called for a blocked token
        mock_gw.assert_not_called("CompileIntent")
        mock_gw.assert_not_called("Execute")

    @pytest.mark.asyncio
    async def test_allowed_token_passes(self, mock_gw):
        """Swap between allowed tokens should succeed."""
        executor = _make_executor(mock_gw, allowed_tokens={"USDC", "ETH", "WETH"})

        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "success"


class TestDataToolsWithMock:
    """Test: data tools work through the full ToolExecutor path."""

    @pytest.mark.asyncio
    async def test_get_price_through_executor(self, mock_gw):
        executor = _make_executor(mock_gw)
        result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        assert result.status == "success"
        assert result.data["price_usd"] == 2000.0
        mock_gw.assert_called("GetPrice", times=1)

    @pytest.mark.asyncio
    async def test_get_balance_through_executor(self, mock_gw):
        executor = _make_executor(mock_gw)
        result = await executor.execute("get_balance", {"token": "USDC", "chain": "arbitrum"})
        assert result.status == "success"
        assert result.data["balance"] == "10000"

    @pytest.mark.asyncio
    async def test_unknown_tool_returns_error(self, mock_gw):
        executor = _make_executor(mock_gw)
        result = await executor.execute("nonexistent_tool", {})
        assert result.status == "error"


class TestCompileAndExecuteWithMock:
    """Test: the compile -> execute flow with mock gateway."""

    @pytest.mark.asyncio
    async def test_compile_intent_returns_bundle_id(self, mock_gw):
        executor = _make_executor(mock_gw)
        result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {"from_token": "USDC", "to_token": "ETH", "amount": "100"}},
        )
        assert result.status == "success"
        assert "bundle_id" in result.data
        mock_gw.assert_called("CompileIntent", times=1)

    @pytest.mark.asyncio
    async def test_compile_failure_returns_error(self, mock_gw):
        mock_gw.set_compile_result("swap", success=False, error="insufficient liquidity")
        executor = _make_executor(mock_gw)
        result = await executor.execute(
            "compile_intent",
            {"intent_type": "swap", "params": {"from_token": "USDC", "to_token": "ETH", "amount": "100"}},
        )
        assert result.status == "error"


class TestCircuitBreakerWithMock:
    """Test: circuit breaker triggers after consecutive failures."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_trips_after_failures(self, mock_gw):
        mock_gw.set_execute_result(success=False, error="execution reverted")
        executor = _make_executor(mock_gw)
        policy = executor._policy_engine

        # Simulate 3 consecutive failures
        for _ in range(3):
            policy.record_trade(Decimal("100"), success=False)

        assert policy.is_circuit_breaker_tripped is True

        # Next action tool should be blocked
        result = await executor.execute(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "100", "chain": "arbitrum"},
        )
        assert result.status == "error"
        assert "circuit breaker" in result.error["message"].lower()


class TestMockCallInspection:
    """Test: call log provides detailed inspection of gateway interactions."""

    @pytest.mark.asyncio
    async def test_call_log_captures_args(self, mock_gw):
        executor = _make_executor(mock_gw)
        await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})

        calls = mock_gw.get_calls("GetPrice")
        assert len(calls) == 1
        assert calls[0].args["token"] == "ETH"

    @pytest.mark.asyncio
    async def test_filter_calls_by_method(self, mock_gw):
        executor = _make_executor(mock_gw)
        await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        await executor.execute("get_balance", {"token": "USDC", "chain": "arbitrum"})

        price_calls = mock_gw.get_calls("GetPrice")
        balance_calls = mock_gw.get_calls("GetBalance")
        assert len(price_calls) >= 1
        assert len(balance_calls) >= 1

    @pytest.mark.asyncio
    async def test_all_calls_returns_everything(self, mock_gw):
        executor = _make_executor(mock_gw)
        await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        all_calls = mock_gw.get_calls()
        # At least 1 GetPrice call + 1 RecordTimelineEvent
        assert len(all_calls) >= 2
