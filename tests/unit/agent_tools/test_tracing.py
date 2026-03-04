"""Tests for structured decision tracing."""

import json
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.tracing import (
    CallbackTraceSink,
    DecisionTracer,
    FileTraceSink,
    InMemoryTraceSink,
    TraceEntry,
    TraceSink,
    sanitize_args,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tracer():
    return DecisionTracer()


@pytest.fixture
def mock_gateway():
    """Create a mock GatewayClient with service stubs."""
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def permissive_policy():
    return AgentPolicy(
        allowed_chains={"arbitrum", "base", "ethereum"},
        max_tool_calls_per_minute=100,
        cooldown_seconds=0,
        max_single_trade_usd=Decimal("999999999"),
        max_daily_spend_usd=Decimal("999999999"),
        max_position_size_usd=Decimal("999999999"),
        require_human_approval_above_usd=Decimal("999999999"),
        require_rebalance_check=False,
    )


# ---------------------------------------------------------------------------
# TraceEntry tests
# ---------------------------------------------------------------------------


class TestTraceEntry:
    def test_creation_with_all_fields(self):
        now = datetime.now(UTC)
        entry = TraceEntry(
            trace_id="trace-1",
            correlation_id="corr-1",
            timestamp=now,
            tool_name="get_price",
            args={"token": "ETH"},
            policy_result={"allowed": True, "violations": []},
            execution_result={"status": "success"},
            error=None,
            duration_ms=12.5,
            state_delta={"spend_usd": "100"},
        )
        assert entry.trace_id == "trace-1"
        assert entry.correlation_id == "corr-1"
        assert entry.timestamp == now
        assert entry.tool_name == "get_price"
        assert entry.args == {"token": "ETH"}
        assert entry.policy_result["allowed"] is True
        assert entry.execution_result["status"] == "success"
        assert entry.error is None
        assert entry.duration_ms == 12.5
        assert entry.state_delta == {"spend_usd": "100"}

    def test_creation_with_minimal_fields(self):
        entry = TraceEntry(
            trace_id="t1",
            correlation_id="c1",
            timestamp=datetime.now(UTC),
            tool_name="swap_tokens",
            args={},
            policy_result=None,
            execution_result=None,
            error="something went wrong",
            duration_ms=0.0,
        )
        assert entry.state_delta is None
        assert entry.error == "something went wrong"

    def test_state_delta_defaults_to_none(self):
        entry = TraceEntry(
            trace_id="t1",
            correlation_id="c1",
            timestamp=datetime.now(UTC),
            tool_name="get_price",
            args={},
            policy_result=None,
            execution_result=None,
            error=None,
            duration_ms=0.0,
        )
        assert entry.state_delta is None


# ---------------------------------------------------------------------------
# DecisionTracer tests
# ---------------------------------------------------------------------------


class TestDecisionTracer:
    def test_records_entries(self, tracer):
        entry = tracer.trace_tool_call(
            tool_name="get_price",
            args={"token": "ETH"},
            policy_result={"allowed": True},
            execution_result={"status": "success"},
            error=None,
            duration_ms=5.0,
        )
        assert len(tracer.get_entries()) == 1
        assert tracer.get_entries()[0] is entry
        assert entry.tool_name == "get_price"

    def test_multiple_entries(self, tracer):
        tracer.trace_tool_call(
            tool_name="get_price",
            args={},
            policy_result=None,
            execution_result={"status": "success"},
            error=None,
            duration_ms=1.0,
        )
        tracer.trace_tool_call(
            tool_name="swap_tokens",
            args={},
            policy_result=None,
            execution_result={"status": "success"},
            error=None,
            duration_ms=2.0,
        )
        assert len(tracer.get_entries()) == 2

    def test_correlation_id_grouping(self, tracer):
        corr1 = tracer.correlation_id
        tracer.trace_tool_call(
            tool_name="get_price",
            args={},
            policy_result=None,
            execution_result=None,
            error=None,
            duration_ms=1.0,
        )

        corr2 = tracer.new_correlation()
        assert corr2 != corr1
        tracer.trace_tool_call(
            tool_name="swap_tokens",
            args={},
            policy_result=None,
            execution_result=None,
            error=None,
            duration_ms=2.0,
        )

        # Filter by first correlation
        group1 = tracer.get_entries(correlation_id=corr1)
        assert len(group1) == 1
        assert group1[0].tool_name == "get_price"

        # Filter by second correlation
        group2 = tracer.get_entries(correlation_id=corr2)
        assert len(group2) == 1
        assert group2[0].tool_name == "swap_tokens"

        # All entries returned without filter
        assert len(tracer.get_entries()) == 2

    def test_new_correlation_creates_new_group(self, tracer):
        original = tracer.correlation_id
        new_id = tracer.new_correlation()
        assert new_id != original
        assert tracer.correlation_id == new_id

    def test_custom_correlation_id(self):
        tracer = DecisionTracer(correlation_id="my-custom-id")
        assert tracer.correlation_id == "my-custom-id"
        entry = tracer.trace_tool_call(
            tool_name="test",
            args={},
            policy_result=None,
            execution_result=None,
            error=None,
            duration_ms=0.0,
        )
        assert entry.correlation_id == "my-custom-id"

    def test_trace_ids_are_unique(self, tracer):
        e1 = tracer.trace_tool_call(
            tool_name="a", args={}, policy_result=None,
            execution_result=None, error=None, duration_ms=0.0,
        )
        e2 = tracer.trace_tool_call(
            tool_name="b", args={}, policy_result=None,
            execution_result=None, error=None, duration_ms=0.0,
        )
        assert e1.trace_id != e2.trace_id

    def test_entries_have_timestamp(self, tracer):
        before = datetime.now(UTC)
        entry = tracer.trace_tool_call(
            tool_name="get_price",
            args={},
            policy_result=None,
            execution_result=None,
            error=None,
            duration_ms=0.0,
        )
        after = datetime.now(UTC)
        assert before <= entry.timestamp <= after


# ---------------------------------------------------------------------------
# get_summary tests
# ---------------------------------------------------------------------------


class TestGetSummary:
    def test_empty_summary(self):
        tracer = DecisionTracer()
        summary = tracer.get_summary()
        assert summary["total_calls"] == 0
        assert summary["successful"] == 0
        assert summary["failed"] == 0
        assert summary["policy_denied"] == 0
        assert summary["unique_tools"] == 0
        assert summary["total_duration_ms"] == 0.0
        assert summary["avg_duration_ms"] == 0.0
        assert summary["correlation_groups"] == 0

    def test_summary_with_mixed_calls(self):
        tracer = DecisionTracer()
        # Successful call
        tracer.trace_tool_call(
            tool_name="get_price",
            args={},
            policy_result={"allowed": True},
            execution_result={"status": "success"},
            error=None,
            duration_ms=10.0,
        )
        # Failed call
        tracer.trace_tool_call(
            tool_name="swap_tokens",
            args={},
            policy_result={"allowed": True},
            execution_result=None,
            error="execution failed",
            duration_ms=20.0,
        )
        # Policy denied
        tracer.trace_tool_call(
            tool_name="swap_tokens",
            args={},
            policy_result={"allowed": False, "violations": ["limit exceeded"]},
            execution_result=None,
            error="risk_blocked",
            duration_ms=5.0,
        )

        summary = tracer.get_summary()
        assert summary["total_calls"] == 3
        assert summary["successful"] == 1
        assert summary["failed"] == 2
        assert summary["policy_denied"] == 1
        assert summary["unique_tools"] == 2
        assert summary["total_duration_ms"] == 35.0
        assert summary["avg_duration_ms"] == pytest.approx(35.0 / 3)
        assert summary["correlation_groups"] == 1

    def test_summary_multiple_correlation_groups(self):
        tracer = DecisionTracer()
        tracer.trace_tool_call(
            tool_name="a", args={}, policy_result=None,
            execution_result={"status": "success"}, error=None, duration_ms=1.0,
        )
        tracer.new_correlation()
        tracer.trace_tool_call(
            tool_name="b", args={}, policy_result=None,
            execution_result={"status": "success"}, error=None, duration_ms=2.0,
        )
        summary = tracer.get_summary()
        assert summary["correlation_groups"] == 2


# ---------------------------------------------------------------------------
# InMemoryTraceSink tests
# ---------------------------------------------------------------------------


class TestInMemoryTraceSink:
    def test_stores_entries(self):
        sink = InMemoryTraceSink()
        entry = TraceEntry(
            trace_id="t1",
            correlation_id="c1",
            timestamp=datetime.now(UTC),
            tool_name="test",
            args={},
            policy_result=None,
            execution_result=None,
            error=None,
            duration_ms=0.0,
        )
        sink.write(entry)
        assert len(sink.entries) == 1
        assert sink.entries[0] is entry

    def test_flush_is_noop(self):
        sink = InMemoryTraceSink()
        sink.flush()  # Should not raise


# ---------------------------------------------------------------------------
# FileTraceSink tests
# ---------------------------------------------------------------------------


class TestFileTraceSink:
    def test_writes_json_lines(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            sink = FileTraceSink(path)
            entry = TraceEntry(
                trace_id="t1",
                correlation_id="c1",
                timestamp=datetime(2026, 1, 15, 10, 30, 0, tzinfo=UTC),
                tool_name="get_price",
                args={"token": "ETH"},
                policy_result={"allowed": True},
                execution_result={"status": "success"},
                error=None,
                duration_ms=12.5,
            )
            sink.write(entry)
            sink.flush()
            sink.close()

            content = Path(path).read_text()
            lines = [line for line in content.strip().split("\n") if line]
            assert len(lines) == 1

            parsed = json.loads(lines[0])
            assert parsed["trace_id"] == "t1"
            assert parsed["tool_name"] == "get_price"
            assert parsed["args"]["token"] == "ETH"
            assert parsed["duration_ms"] == 12.5
            assert parsed["error"] is None
        finally:
            Path(path).unlink(missing_ok=True)

    def test_appends_multiple_entries(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            sink = FileTraceSink(path)
            for i in range(3):
                entry = TraceEntry(
                    trace_id=f"t{i}",
                    correlation_id="c1",
                    timestamp=datetime.now(UTC),
                    tool_name=f"tool_{i}",
                    args={},
                    policy_result=None,
                    execution_result=None,
                    error=None,
                    duration_ms=float(i),
                )
                sink.write(entry)
            sink.flush()
            sink.close()

            content = Path(path).read_text()
            lines = [line for line in content.strip().split("\n") if line]
            assert len(lines) == 3

            for i, line in enumerate(lines):
                parsed = json.loads(line)
                assert parsed["trace_id"] == f"t{i}"
                assert parsed["tool_name"] == f"tool_{i}"
        finally:
            Path(path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# CallbackTraceSink tests
# ---------------------------------------------------------------------------


class TestCallbackTraceSink:
    def test_invokes_callback(self):
        received = []
        sink = CallbackTraceSink(received.append)
        entry = TraceEntry(
            trace_id="t1",
            correlation_id="c1",
            timestamp=datetime.now(UTC),
            tool_name="test",
            args={},
            policy_result=None,
            execution_result=None,
            error=None,
            duration_ms=0.0,
        )
        sink.write(entry)
        assert len(received) == 1
        assert received[0] is entry

    def test_callback_receives_multiple(self):
        received = []
        sink = CallbackTraceSink(received.append)
        for i in range(5):
            entry = TraceEntry(
                trace_id=f"t{i}",
                correlation_id="c1",
                timestamp=datetime.now(UTC),
                tool_name="test",
                args={},
                policy_result=None,
                execution_result=None,
                error=None,
                duration_ms=0.0,
            )
            sink.write(entry)
        assert len(received) == 5


# ---------------------------------------------------------------------------
# Argument sanitization tests
# ---------------------------------------------------------------------------


class TestSanitizeArgs:
    def test_redacts_sensitive_fields(self):
        args = {
            "token": "ETH",
            "private_key": "0xdeadbeef",
            "api_key": "sk-secret123",
            "chain": "arbitrum",
        }
        sanitized = sanitize_args(args)
        assert sanitized["token"] == "ETH"
        assert sanitized["chain"] == "arbitrum"
        assert sanitized["private_key"] == "***REDACTED***"
        assert sanitized["api_key"] == "***REDACTED***"

    def test_case_insensitive_field_matching(self):
        args = {"Private_Key": "secret", "API_KEY": "key123", "token": "ETH"}
        sanitized = sanitize_args(args)
        assert sanitized["Private_Key"] == "***REDACTED***"
        assert sanitized["API_KEY"] == "***REDACTED***"
        assert sanitized["token"] == "ETH"

    def test_nested_dict_sanitization(self):
        args = {
            "config": {
                "password": "hunter2",
                "host": "localhost",
            },
            "chain": "arbitrum",
        }
        sanitized = sanitize_args(args)
        assert sanitized["config"]["password"] == "***REDACTED***"
        assert sanitized["config"]["host"] == "localhost"
        assert sanitized["chain"] == "arbitrum"

    def test_non_sensitive_fields_preserved(self):
        args = {
            "token": "ETH",
            "amount": "100",
            "chain": "arbitrum",
            "slippage_bps": 50,
        }
        sanitized = sanitize_args(args)
        assert sanitized == args

    def test_sanitizes_dicts_inside_lists(self):
        args = {
            "actions": [
                {"type": "swap", "api_key": "secret123"},
                {"type": "transfer", "password": "hunter2"},
            ],
            "chain": "arbitrum",
        }
        sanitized = sanitize_args(args)
        assert sanitized["chain"] == "arbitrum"
        assert sanitized["actions"][0]["type"] == "swap"
        assert sanitized["actions"][0]["api_key"] == "***REDACTED***"
        assert sanitized["actions"][1]["type"] == "transfer"
        assert sanitized["actions"][1]["password"] == "***REDACTED***"

    def test_non_dict_list_items_preserved(self):
        args = {"tokens": ["ETH", "USDC"], "amounts": [100, 200]}
        sanitized = sanitize_args(args)
        assert sanitized == args

    def test_empty_args(self):
        assert sanitize_args({}) == {}

    def test_all_sensitive_fields(self):
        from almanak.framework.agent_tools.tracing import SENSITIVE_FIELDS

        args = {field: f"value_{field}" for field in SENSITIVE_FIELDS}
        sanitized = sanitize_args(args)
        for field in SENSITIVE_FIELDS:
            assert sanitized[field] == "***REDACTED***"


# ---------------------------------------------------------------------------
# TraceSink base class tests
# ---------------------------------------------------------------------------


class TestTraceSinkBase:
    def test_base_write_is_noop(self):
        sink = TraceSink()
        entry = TraceEntry(
            trace_id="t1",
            correlation_id="c1",
            timestamp=datetime.now(UTC),
            tool_name="test",
            args={},
            policy_result=None,
            execution_result=None,
            error=None,
            duration_ms=0.0,
        )
        sink.write(entry)  # Should not raise

    def test_base_flush_is_noop(self):
        sink = TraceSink()
        sink.flush()  # Should not raise


# ---------------------------------------------------------------------------
# Tracer with custom sink tests
# ---------------------------------------------------------------------------


class TestTracerWithSinks:
    def test_tracer_writes_to_custom_sink(self):
        received = []
        sink = CallbackTraceSink(received.append)
        tracer = DecisionTracer(sink=sink)
        tracer.trace_tool_call(
            tool_name="get_price",
            args={"token": "ETH"},
            policy_result=None,
            execution_result={"status": "success"},
            error=None,
            duration_ms=5.0,
        )
        assert len(received) == 1
        assert received[0].tool_name == "get_price"

    def test_tracer_writes_to_in_memory_sink_by_default(self):
        tracer = DecisionTracer()
        assert isinstance(tracer.sink, InMemoryTraceSink)
        tracer.trace_tool_call(
            tool_name="test",
            args={},
            policy_result=None,
            execution_result=None,
            error=None,
            duration_ms=0.0,
        )
        assert len(tracer.sink.entries) == 1

    def test_tracer_writes_to_file_sink(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            sink = FileTraceSink(path)
            tracer = DecisionTracer(sink=sink)
            tracer.trace_tool_call(
                tool_name="get_price",
                args={"token": "BTC"},
                policy_result=None,
                execution_result={"status": "success"},
                error=None,
                duration_ms=3.0,
            )
            sink.flush()
            sink.close()

            content = Path(path).read_text()
            parsed = json.loads(content.strip())
            assert parsed["tool_name"] == "get_price"
            assert parsed["args"]["token"] == "BTC"
        finally:
            Path(path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# ToolExecutor integration tests
# ---------------------------------------------------------------------------


class TestToolExecutorTracing:
    @pytest.mark.asyncio
    async def test_successful_call_produces_trace(self, mock_gateway, permissive_policy):
        tracer = DecisionTracer()
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            tracer=tracer,
        )

        mock_resp = MagicMock()
        mock_resp.price = "3200.50"
        mock_resp.source = "coingecko"
        mock_resp.timestamp = 1700000000
        mock_gateway.market.GetPrice.return_value = mock_resp

        result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        assert result.status == "success"

        entries = tracer.get_entries()
        assert len(entries) == 1
        entry = entries[0]
        assert entry.tool_name == "get_price"
        assert entry.args["token"] == "ETH"
        assert entry.error is None
        assert entry.execution_result is not None
        assert entry.execution_result["status"] == "success"
        assert entry.duration_ms >= 0

    @pytest.mark.asyncio
    async def test_trace_includes_policy_result_on_success(self, mock_gateway, permissive_policy):
        tracer = DecisionTracer()
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            tracer=tracer,
        )

        mock_resp = MagicMock()
        mock_resp.price = "3200.50"
        mock_resp.source = "coingecko"
        mock_resp.timestamp = 1700000000
        mock_gateway.market.GetPrice.return_value = mock_resp

        await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})

        entries = tracer.get_entries()
        assert len(entries) == 1
        entry = entries[0]
        assert entry.policy_result is not None
        assert entry.policy_result["allowed"] is True

    @pytest.mark.asyncio
    async def test_trace_includes_policy_result_on_denial(self, mock_gateway):
        tracer = DecisionTracer()
        policy = AgentPolicy(
            allowed_chains={"arbitrum"},
            cooldown_seconds=0,
            max_tool_calls_per_minute=100,
        )
        executor = ToolExecutor(
            mock_gateway,
            policy=policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            tracer=tracer,
        )

        result = await executor.execute("get_price", {"token": "ETH", "chain": "ethereum"})
        assert result.status == "error"

        entries = tracer.get_entries()
        assert len(entries) == 1
        entry = entries[0]
        assert entry.policy_result is not None
        assert entry.policy_result["allowed"] is False
        assert len(entry.policy_result["violations"]) > 0
        assert entry.error is not None

    @pytest.mark.asyncio
    async def test_trace_includes_error_on_failure(self, mock_gateway, permissive_policy):
        tracer = DecisionTracer()
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            tracer=tracer,
        )

        # Trigger an unknown tool error
        result = await executor.execute("nonexistent_tool", {})
        assert result.status == "error"

        entries = tracer.get_entries()
        assert len(entries) == 1
        entry = entries[0]
        assert entry.tool_name == "nonexistent_tool"
        assert entry.error is not None
        assert "validation_error" in entry.error

    @pytest.mark.asyncio
    async def test_multiple_calls_all_traced(self, mock_gateway, permissive_policy):
        tracer = DecisionTracer()
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            tracer=tracer,
        )

        mock_resp = MagicMock()
        mock_resp.price = "3200.50"
        mock_resp.source = "coingecko"
        mock_resp.timestamp = 1700000000
        mock_gateway.market.GetPrice.return_value = mock_resp

        await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        await executor.execute("get_price", {"token": "BTC", "chain": "arbitrum"})
        await executor.execute("nonexistent_tool", {})

        entries = tracer.get_entries()
        assert len(entries) == 3
        assert entries[0].tool_name == "get_price"
        assert entries[1].tool_name == "get_price"
        assert entries[2].tool_name == "nonexistent_tool"

    @pytest.mark.asyncio
    async def test_trace_sanitizes_sensitive_args(self, mock_gateway, permissive_policy):
        tracer = DecisionTracer()
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            tracer=tracer,
        )

        mock_resp = MagicMock()
        mock_resp.price = "3200.50"
        mock_resp.source = "coingecko"
        mock_resp.timestamp = 1700000000
        mock_gateway.market.GetPrice.return_value = mock_resp

        # Pass a sensitive field alongside normal args
        await executor.execute(
            "get_price",
            {"token": "ETH", "chain": "arbitrum", "private_key": "0xdeadbeef"},
        )

        entries = tracer.get_entries()
        assert len(entries) == 1
        assert entries[0].args["token"] == "ETH"
        assert entries[0].args["private_key"] == "***REDACTED***"

    @pytest.mark.asyncio
    async def test_executor_tracer_property(self, mock_gateway, permissive_policy):
        tracer = DecisionTracer()
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            tracer=tracer,
        )
        assert executor.tracer is tracer

    @pytest.mark.asyncio
    async def test_executor_default_tracer(self, mock_gateway, permissive_policy):
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        )
        assert executor.tracer is not None
        assert isinstance(executor.tracer, DecisionTracer)

    @pytest.mark.asyncio
    async def test_trace_has_duration(self, mock_gateway, permissive_policy):
        tracer = DecisionTracer()
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            tracer=tracer,
        )

        mock_resp = MagicMock()
        mock_resp.price = "3200.50"
        mock_resp.source = "coingecko"
        mock_resp.timestamp = 1700000000
        mock_gateway.market.GetPrice.return_value = mock_resp

        await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})

        entries = tracer.get_entries()
        assert entries[0].duration_ms > 0

    @pytest.mark.asyncio
    async def test_trace_with_callback_sink(self, mock_gateway, permissive_policy):
        received = []
        sink = CallbackTraceSink(received.append)
        tracer = DecisionTracer(sink=sink)
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            tracer=tracer,
        )

        mock_resp = MagicMock()
        mock_resp.price = "3200.50"
        mock_resp.source = "coingecko"
        mock_resp.timestamp = 1700000000
        mock_gateway.market.GetPrice.return_value = mock_resp

        await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})

        assert len(received) == 1
        assert received[0].tool_name == "get_price"

    @pytest.mark.asyncio
    async def test_trace_on_gateway_error_caught_by_dispatch(self, mock_gateway, permissive_policy):
        """When the gateway raises inside a dispatch method that catches it
        (e.g. get_price), the error is returned as an error ToolResponse rather
        than propagating as an exception. The trace still records the call with
        execution_result status='error'."""
        tracer = DecisionTracer()
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            tracer=tracer,
        )

        # get_price catches exceptions internally and returns error ToolResponse
        mock_gateway.market.GetPrice.side_effect = RuntimeError("connection lost")

        result = await executor.execute("get_price", {"token": "ETH", "chain": "arbitrum"})
        assert result.status == "error"

        entries = tracer.get_entries()
        assert len(entries) == 1
        # The dispatch method caught the error, so no exception propagated
        assert entries[0].execution_result is not None
        assert entries[0].execution_result["status"] == "error"

    @pytest.mark.asyncio
    async def test_trace_on_uncaught_exception(self, mock_gateway, permissive_policy):
        """When an unexpected error propagates out of _execute_inner (not
        caught by dispatch), the trace captures it in the error field."""
        tracer = DecisionTracer()
        executor = ToolExecutor(
            mock_gateway,
            policy=permissive_policy,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            tracer=tracer,
        )

        # Make the observe service fail (called in _record_tool_event after dispatch)
        # But we need something that truly propagates -- let's make _execute_inner
        # raise by breaking validation at a point that bypasses normal error handling.
        # Simplest: pass invalid arguments that Pydantic doesn't catch but the
        # inner dispatch does not handle.
        # Actually, let's cause an error in the _record_tool_event path.
        # The simplest way: use a tool that doesn't have special exception handling
        # in its dispatch.

        # get_balance does NOT have a try/except in its dispatch, so a gateway
        # error will propagate as an uncaught Exception
        mock_gateway.market.GetBalance.side_effect = RuntimeError("unexpected crash")

        result = await executor.execute("get_balance", {"token": "USDC", "chain": "arbitrum"})
        assert result.status == "error"

        entries = tracer.get_entries()
        assert len(entries) == 1
        assert entries[0].error is not None
        assert "unexpected crash" in entries[0].error
