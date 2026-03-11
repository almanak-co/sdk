"""Tests for the human-approval actuator for high-value agent trades."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.agent_tools.approval import (
    ApprovalChannel,
    ApprovalConfig,
    ApprovalDecision,
    ApprovalNotifier,
    ApprovalRequest,
    ApprovalStatus,
    ConsoleApprovalNotifier,
    FileApprovalNotifier,
    HumanApprovalActuator,
    WebhookApprovalNotifier,
)
from almanak.framework.agent_tools.catalog import RiskTier, ToolCategory, ToolDefinition
from almanak.framework.agent_tools.errors import RiskBlockedError
from almanak.framework.agent_tools.policy import AgentPolicy, PolicyEngine
from almanak.framework.agent_tools.schemas import GetPriceRequest, GetPriceResponse, SwapTokensRequest, SwapTokensResponse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tool(
    name: str = "swap_tokens",
    category: ToolCategory = ToolCategory.ACTION,
    risk_tier: RiskTier = RiskTier.MEDIUM,
    requires_approval_above_usd: float | None = None,
):
    return ToolDefinition(
        name=name,
        description="test",
        category=category,
        risk_tier=risk_tier,
        request_schema=SwapTokensRequest,
        response_schema=SwapTokensResponse,
        requires_approval_above_usd=requires_approval_above_usd,
    )


def _make_request(
    tool_name: str = "swap_tokens",
    estimated_value_usd: Decimal = Decimal("15000"),
    threshold_usd: Decimal = Decimal("10000"),
    timeout_seconds: int = 300,
) -> ApprovalRequest:
    now = datetime.now(UTC)
    return ApprovalRequest(
        request_id="test-request-001",
        tool_name=tool_name,
        args={"token_in": "USDC", "token_out": "ETH", "amount": "15000"},
        estimated_value_usd=estimated_value_usd,
        threshold_usd=threshold_usd,
        timestamp=now,
        expires_at=now + timedelta(seconds=timeout_seconds),
    )


class StubNotifier(ApprovalNotifier):
    """Test notifier that records calls and returns preconfigured decisions."""

    def __init__(self, decision: ApprovalDecision | None = None):
        self.notified: list[ApprovalRequest] = []
        self.polled: list[ApprovalRequest] = []
        self._decision = decision

    def notify(self, request: ApprovalRequest) -> None:
        self.notified.append(request)

    def poll(self, request: ApprovalRequest) -> ApprovalDecision | None:
        self.polled.append(request)
        return self._decision


# ---------------------------------------------------------------------------
# ApprovalRequest model tests
# ---------------------------------------------------------------------------


class TestApprovalRequest:
    def test_creation(self):
        req = _make_request()
        assert req.request_id == "test-request-001"
        assert req.tool_name == "swap_tokens"
        assert req.estimated_value_usd == Decimal("15000")
        assert req.threshold_usd == Decimal("10000")
        assert req.status == ApprovalStatus.PENDING

    def test_is_expired_false(self):
        req = _make_request(timeout_seconds=300)
        assert req.is_expired() is False

    def test_is_expired_true(self):
        now = datetime.now(UTC)
        req = ApprovalRequest(
            request_id="expired-1",
            tool_name="swap_tokens",
            args={},
            estimated_value_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
            timestamp=now - timedelta(seconds=600),
            expires_at=now - timedelta(seconds=1),
        )
        assert req.is_expired() is True

    def test_to_dict(self):
        req = _make_request()
        d = req.to_dict()
        assert d["request_id"] == "test-request-001"
        assert d["tool_name"] == "swap_tokens"
        assert d["estimated_value_usd"] == "15000"
        assert d["threshold_usd"] == "10000"
        assert d["status"] == "pending"
        # Timestamps should be ISO format strings
        assert "T" in d["timestamp"]
        assert "T" in d["expires_at"]


class TestApprovalStatus:
    def test_values(self):
        assert ApprovalStatus.PENDING == "pending"
        assert ApprovalStatus.APPROVED == "approved"
        assert ApprovalStatus.REJECTED == "rejected"
        assert ApprovalStatus.EXPIRED == "expired"


# ---------------------------------------------------------------------------
# FileApprovalNotifier tests
# ---------------------------------------------------------------------------


class TestFileApprovalNotifier:
    def test_notify_creates_request_file(self, tmp_path):
        notifier = FileApprovalNotifier(str(tmp_path))
        req = _make_request()
        notifier.notify(req)

        request_file = tmp_path / f"{req.request_id}.request.json"
        assert request_file.exists()

        data = json.loads(request_file.read_text())
        assert data["request_id"] == req.request_id
        assert data["tool_name"] == "swap_tokens"
        assert data["estimated_value_usd"] == "15000"

    def test_poll_no_response(self, tmp_path):
        notifier = FileApprovalNotifier(str(tmp_path))
        req = _make_request()
        assert notifier.poll(req) is None

    def test_poll_approved(self, tmp_path):
        notifier = FileApprovalNotifier(str(tmp_path))
        req = _make_request()

        # Create the approval marker file
        (tmp_path / f"{req.request_id}.approved").touch()

        decision = notifier.poll(req)
        assert decision is not None
        assert decision.status == ApprovalStatus.APPROVED
        assert decision.decided_by == "file"

    def test_poll_rejected_with_reason(self, tmp_path):
        notifier = FileApprovalNotifier(str(tmp_path))
        req = _make_request()

        # Create rejection file with a reason
        (tmp_path / f"{req.request_id}.rejected").write_text("Too risky")

        decision = notifier.poll(req)
        assert decision is not None
        assert decision.status == ApprovalStatus.REJECTED
        assert decision.reason == "Too risky"

    def test_poll_rejected_empty_reason(self, tmp_path):
        notifier = FileApprovalNotifier(str(tmp_path))
        req = _make_request()

        (tmp_path / f"{req.request_id}.rejected").touch()

        decision = notifier.poll(req)
        assert decision is not None
        assert decision.status == ApprovalStatus.REJECTED
        assert decision.reason == ""

    def test_creates_queue_dir_if_missing(self, tmp_path):
        queue_dir = tmp_path / "nested" / "approvals"
        notifier = FileApprovalNotifier(str(queue_dir))
        assert queue_dir.exists()


# ---------------------------------------------------------------------------
# WebhookApprovalNotifier tests
# ---------------------------------------------------------------------------


class TestWebhookApprovalNotifier:
    def test_notify_posts_to_webhook(self):
        notifier = WebhookApprovalNotifier("https://example.com/approve")
        req = _make_request()

        with patch("almanak.framework.agent_tools.approval.httpx.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = MagicMock()
            mock_post.return_value = mock_resp

            notifier.notify(req)

            mock_post.assert_called_once()
            call_kwargs = mock_post.call_args
            assert call_kwargs.args[0] == "https://example.com/approve"
            payload = call_kwargs.kwargs["json"]
            assert payload["request_id"] == req.request_id
            assert payload["tool_name"] == "swap_tokens"

    def test_poll_returns_approved(self):
        notifier = WebhookApprovalNotifier(
            "https://example.com/approve",
            poll_url="https://example.com/approve/status",
        )
        req = _make_request()

        with patch("almanak.framework.agent_tools.approval.httpx.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"status": "approved", "decided_by": "operator"}
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            decision = notifier.poll(req)

            assert decision is not None
            assert decision.status == ApprovalStatus.APPROVED
            assert decision.decided_by == "operator"

    def test_poll_returns_rejected(self):
        notifier = WebhookApprovalNotifier("https://example.com/approve")
        req = _make_request()

        with patch("almanak.framework.agent_tools.approval.httpx.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {
                "status": "rejected",
                "decided_by": "risk-team",
                "reason": "Market conditions unfavorable",
            }
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            decision = notifier.poll(req)

            assert decision is not None
            assert decision.status == ApprovalStatus.REJECTED
            assert decision.reason == "Market conditions unfavorable"

    def test_poll_returns_none_when_pending(self):
        notifier = WebhookApprovalNotifier("https://example.com/approve")
        req = _make_request()

        with patch("almanak.framework.agent_tools.approval.httpx.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"status": "pending"}
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            decision = notifier.poll(req)
            assert decision is None

    def test_poll_returns_none_on_http_error(self):
        import httpx as httpx_mod

        notifier = WebhookApprovalNotifier("https://example.com/approve")
        req = _make_request()

        with patch("almanak.framework.agent_tools.approval.httpx.get") as mock_get:
            mock_get.side_effect = httpx_mod.HTTPError("Connection refused")
            decision = notifier.poll(req)
            assert decision is None

    def test_default_poll_url(self):
        notifier = WebhookApprovalNotifier("https://example.com/approve")
        assert notifier._poll_url == "https://example.com/approve/status"


# ---------------------------------------------------------------------------
# ConsoleApprovalNotifier tests
# ---------------------------------------------------------------------------


class TestConsoleApprovalNotifier:
    def test_notify_prints_to_stdout(self, capsys):
        notifier = ConsoleApprovalNotifier()
        req = _make_request()

        with patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            notifier.notify(req)

        captured = capsys.readouterr()
        assert "HUMAN APPROVAL REQUIRED" in captured.out
        assert req.request_id in captured.out
        assert "swap_tokens" in captured.out

    def test_approve_via_input(self):
        notifier = ConsoleApprovalNotifier()
        req = _make_request()

        with patch("sys.stdin") as mock_stdin, patch("builtins.input", return_value="y"):
            mock_stdin.isatty.return_value = True
            notifier.notify(req)

        decision = notifier.poll(req)
        assert decision is not None
        assert decision.status == ApprovalStatus.APPROVED

    def test_reject_via_input(self):
        notifier = ConsoleApprovalNotifier()
        req = _make_request()

        with patch("sys.stdin") as mock_stdin, patch("builtins.input", return_value="n"):
            mock_stdin.isatty.return_value = True
            notifier.notify(req)

        decision = notifier.poll(req)
        assert decision is not None
        assert decision.status == ApprovalStatus.REJECTED


# ---------------------------------------------------------------------------
# HumanApprovalActuator tests
# ---------------------------------------------------------------------------


class TestHumanApprovalActuator:
    def test_request_approval_creates_request(self):
        stub = StubNotifier()
        config = ApprovalConfig(channel=ApprovalChannel.FILE, timeout_seconds=60)
        actuator = HumanApprovalActuator(config, notifier=stub)

        req = actuator.request_approval(
            tool_name="swap_tokens",
            args={"amount": "15000"},
            estimated_value_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
        )

        assert req.status == ApprovalStatus.PENDING
        assert req.tool_name == "swap_tokens"
        assert req.estimated_value_usd == Decimal("15000")
        assert len(stub.notified) == 1
        assert stub.notified[0].request_id == req.request_id

    def test_poll_decision_approved(self):
        decision = ApprovalDecision(
            request_id="test-1",
            status=ApprovalStatus.APPROVED,
            decided_by="tester",
        )
        stub = StubNotifier(decision=decision)
        config = ApprovalConfig(timeout_seconds=60)
        actuator = HumanApprovalActuator(config, notifier=stub)

        req = actuator.request_approval(
            tool_name="swap_tokens",
            args={},
            estimated_value_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
        )

        # Override the stub decision's request_id to match
        stub._decision = ApprovalDecision(
            request_id=req.request_id,
            status=ApprovalStatus.APPROVED,
            decided_by="tester",
        )

        result = actuator.poll_decision(req)
        assert result is not None
        assert result.status == ApprovalStatus.APPROVED

    def test_poll_decision_expired(self):
        stub = StubNotifier(decision=None)  # Never responds
        config = ApprovalConfig(timeout_seconds=60)
        actuator = HumanApprovalActuator(config, notifier=stub)

        # Create a request that is already expired
        now = datetime.now(UTC)
        req = ApprovalRequest(
            request_id="expired-1",
            tool_name="swap_tokens",
            args={},
            estimated_value_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
            timestamp=now - timedelta(seconds=120),
            expires_at=now - timedelta(seconds=1),
        )
        actuator._requests[req.request_id] = req

        result = actuator.poll_decision(req)
        assert result is not None
        assert result.status == ApprovalStatus.EXPIRED

    @pytest.mark.asyncio
    async def test_wait_for_decision_immediate_approval(self):
        decision = ApprovalDecision(
            request_id="immediate-1",
            status=ApprovalStatus.APPROVED,
            decided_by="tester",
        )
        stub = StubNotifier(decision=decision)
        config = ApprovalConfig(timeout_seconds=60)
        actuator = HumanApprovalActuator(config, notifier=stub)

        req = actuator.request_approval(
            tool_name="swap_tokens",
            args={},
            estimated_value_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
        )

        stub._decision = ApprovalDecision(
            request_id=req.request_id,
            status=ApprovalStatus.APPROVED,
            decided_by="tester",
        )

        result = await actuator.wait_for_decision(req, poll_interval=0.01)
        assert result.status == ApprovalStatus.APPROVED

    def test_get_request_and_decision(self):
        stub = StubNotifier(decision=None)
        config = ApprovalConfig(timeout_seconds=60)
        actuator = HumanApprovalActuator(config, notifier=stub)

        req = actuator.request_approval(
            tool_name="swap_tokens",
            args={},
            estimated_value_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
        )

        assert actuator.get_request(req.request_id) is req
        assert actuator.get_request("nonexistent") is None
        assert actuator.get_decision(req.request_id) is None

    def test_file_channel_full_flow(self, tmp_path):
        """End-to-end test: file-based approval from request to approval."""
        config = ApprovalConfig(
            channel=ApprovalChannel.FILE,
            queue_dir=str(tmp_path),
            timeout_seconds=60,
        )
        actuator = HumanApprovalActuator(config)

        # 1. Request approval
        req = actuator.request_approval(
            tool_name="swap_tokens",
            args={"token_in": "USDC", "token_out": "ETH", "amount": "15000"},
            estimated_value_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
        )

        # Verify request file was created
        request_file = tmp_path / f"{req.request_id}.request.json"
        assert request_file.exists()

        # 2. Poll -- should be pending
        result = actuator.poll_decision(req)
        assert result is None

        # 3. Simulate operator approval
        (tmp_path / f"{req.request_id}.approved").touch()

        # 4. Poll -- should be approved
        result = actuator.poll_decision(req)
        assert result is not None
        assert result.status == ApprovalStatus.APPROVED

    def test_file_channel_rejection_flow(self, tmp_path):
        """End-to-end test: file-based rejection with reason."""
        config = ApprovalConfig(
            channel=ApprovalChannel.FILE,
            queue_dir=str(tmp_path),
            timeout_seconds=60,
        )
        actuator = HumanApprovalActuator(config)

        req = actuator.request_approval(
            tool_name="swap_tokens",
            args={"amount": "50000"},
            estimated_value_usd=Decimal("50000"),
            threshold_usd=Decimal("10000"),
        )

        # Simulate operator rejection with reason
        (tmp_path / f"{req.request_id}.rejected").write_text("Market too volatile")

        result = actuator.poll_decision(req)
        assert result is not None
        assert result.status == ApprovalStatus.REJECTED
        assert result.reason == "Market too volatile"

    def test_create_notifier_file(self, tmp_path):
        config = ApprovalConfig(
            channel=ApprovalChannel.FILE,
            queue_dir=str(tmp_path / "q"),
        )
        actuator = HumanApprovalActuator(config)
        assert isinstance(actuator._notifier, FileApprovalNotifier)

    def test_create_notifier_webhook(self):
        config = ApprovalConfig(
            channel=ApprovalChannel.WEBHOOK,
            webhook_url="https://example.com/approve",
        )
        actuator = HumanApprovalActuator(config)
        assert isinstance(actuator._notifier, WebhookApprovalNotifier)

    def test_create_notifier_webhook_requires_url(self):
        config = ApprovalConfig(channel=ApprovalChannel.WEBHOOK)
        with pytest.raises(ValueError, match="approval_webhook_url is required"):
            HumanApprovalActuator(config)

    def test_create_notifier_console(self):
        config = ApprovalConfig(channel=ApprovalChannel.CONSOLE)
        actuator = HumanApprovalActuator(config)
        assert isinstance(actuator._notifier, ConsoleApprovalNotifier)


# ---------------------------------------------------------------------------
# PolicyEngine integration tests
# ---------------------------------------------------------------------------


class TestPolicyEngineApprovalGate:
    """Test that PolicyEngine correctly flags trades requiring approval."""

    def test_below_threshold_no_approval(self):
        """Trades below the threshold should not require approval."""
        policy = AgentPolicy(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)
        tool = _make_tool()

        decision = engine.check(tool, {"amount": "5000", "token_in": "USDC", "chain": "arbitrum"})
        assert decision.allowed is True
        assert decision.requires_approval is False

    def test_above_threshold_requires_approval(self):
        """Trades above the threshold should flag requires_approval."""
        policy = AgentPolicy(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
            max_position_size_usd=Decimal("200000"),
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)
        tool = _make_tool()

        decision = engine.check(tool, {"amount": "15000", "token_in": "USDC", "chain": "arbitrum"})
        assert decision.allowed is True  # Not a hard block
        assert decision.requires_approval is True
        assert decision.approval_estimated_usd == Decimal("15000")
        assert decision.approval_threshold_usd == Decimal("10000")

    def test_per_tool_threshold_more_restrictive(self):
        """Per-tool threshold should override when more restrictive."""
        policy = AgentPolicy(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
            max_position_size_usd=Decimal("200000"),
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)
        tool = _make_tool(requires_approval_above_usd=5000.0)

        decision = engine.check(tool, {"amount": "7000", "token_in": "USDC", "chain": "arbitrum"})
        assert decision.requires_approval is True
        assert decision.approval_threshold_usd == Decimal("5000")

    def test_no_approval_for_low_risk_tools(self):
        """Low risk tools should never trigger approval even with high amounts."""
        policy = AgentPolicy(
            require_human_approval_above_usd=Decimal("100"),
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)
        tool = _make_tool(risk_tier=RiskTier.NONE, category=ToolCategory.DATA)

        # Data tools skip the MEDIUM/HIGH risk checks entirely
        decision = engine.check(tool, {"amount": "50000", "chain": "arbitrum"})
        assert decision.requires_approval is False

    def test_hard_violations_override_approval(self):
        """If there are hard policy violations, approval gate is skipped."""
        policy = AgentPolicy(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
            max_position_size_usd=Decimal("200000"),
            allowed_chains={"ethereum"},  # Will fail chain check
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)
        tool = _make_tool()

        decision = engine.check(tool, {"amount": "15000", "token_in": "USDC", "chain": "arbitrum"})
        assert decision.allowed is False  # Hard violation: wrong chain
        # Approval gate is skipped when hard violations already exist
        assert decision.requires_approval is False

    def test_vault_lifecycle_tools_skip_approval(self):
        """Vault lifecycle tools should not trigger approval gate."""
        policy = AgentPolicy(
            require_human_approval_above_usd=Decimal("100"),
            max_single_trade_usd=Decimal("100000"),
            max_position_size_usd=Decimal("200000"),
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)
        tool = _make_tool(name="deploy_vault", risk_tier=RiskTier.MEDIUM)

        decision = engine.check(tool, {"amount": "50000", "token_in": "USDC", "chain": "arbitrum"})
        assert decision.requires_approval is False  # vault lifecycle tools skip spend checks


# ---------------------------------------------------------------------------
# ToolExecutor integration tests (approval flow)
# ---------------------------------------------------------------------------


class TestToolExecutorApprovalFlow:
    """Test the executor's handling of the approval flow."""

    def _make_executor(
        self,
        approval_config=None,
        approval_notifier=None,
        policy=None,
    ):
        """Create a ToolExecutor with a mock gateway client."""
        mock_client = MagicMock()
        mock_client.market.GetPrice.return_value = MagicMock(price=1.0, source="mock", timestamp="now")
        return __import__(
            "almanak.framework.agent_tools.executor", fromlist=["ToolExecutor"]
        ).ToolExecutor(
            mock_client,
            policy=policy or AgentPolicy(
                require_human_approval_above_usd=Decimal("10000"),
                max_single_trade_usd=Decimal("100000"),
                max_position_size_usd=Decimal("200000"),
                cooldown_seconds=0,
            ),
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            strategy_id="test-strategy",
            approval_config=approval_config,
            approval_notifier=approval_notifier,
        )

    @pytest.mark.asyncio
    async def test_no_approval_config_raises_risk_blocked(self):
        """Without approval config, exceeding threshold raises RiskBlockedError."""
        from almanak.framework.agent_tools.executor import ToolExecutor

        executor = self._make_executor(approval_config=None)

        with pytest.raises(RiskBlockedError, match="exceeds approval threshold"):
            await executor._handle_approval(
                tool_name="swap_tokens",
                arguments={"amount": "15000", "token_in": "USDC"},
                estimated_usd=Decimal("15000"),
                threshold_usd=Decimal("10000"),
            )

    @pytest.mark.asyncio
    async def test_approval_config_approved_returns_none(self):
        """When approved, _handle_approval returns None (proceed with execution)."""
        decision = ApprovalDecision(
            request_id="test-1",
            status=ApprovalStatus.APPROVED,
            decided_by="tester",
        )
        stub = StubNotifier(decision=decision)
        config = ApprovalConfig(timeout_seconds=5)
        executor = self._make_executor(approval_config=config, approval_notifier=stub)

        result = await executor._handle_approval(
            tool_name="swap_tokens",
            arguments={"amount": "15000"},
            estimated_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
        )

        assert result is None  # Proceed
        assert len(stub.notified) == 1

    @pytest.mark.asyncio
    async def test_approval_config_rejected_returns_response(self):
        """When rejected, _handle_approval returns a rejection ToolResponse."""
        decision = ApprovalDecision(
            request_id="test-1",
            status=ApprovalStatus.REJECTED,
            decided_by="operator",
            reason="Too risky",
        )
        stub = StubNotifier(decision=decision)
        config = ApprovalConfig(timeout_seconds=5)
        executor = self._make_executor(approval_config=config, approval_notifier=stub)

        result = await executor._handle_approval(
            tool_name="swap_tokens",
            arguments={"amount": "15000"},
            estimated_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
        )

        assert result is not None
        assert result.status == "rejected"
        assert result.data["approval_status"] == "rejected"
        assert result.data["reason"] == "Too risky"
        assert result.data["estimated_value_usd"] == "15000"

    @pytest.mark.asyncio
    async def test_approval_config_expired_returns_response(self):
        """When expired, _handle_approval returns an expired ToolResponse."""
        # Use a notifier that never responds, but make the request already expired
        stub = StubNotifier(decision=None)
        config = ApprovalConfig(timeout_seconds=0)  # Immediate expiry
        executor = self._make_executor(approval_config=config, approval_notifier=stub)

        result = await executor._handle_approval(
            tool_name="swap_tokens",
            arguments={"amount": "15000"},
            estimated_usd=Decimal("15000"),
            threshold_usd=Decimal("10000"),
        )

        assert result is not None
        assert result.status == "rejected"
        assert result.data["approval_status"] == "expired"

    def test_below_threshold_bypasses_approval(self):
        """Trades below threshold should not trigger approval at all."""
        stub = StubNotifier()
        config = ApprovalConfig(timeout_seconds=60)
        executor = self._make_executor(approval_config=config, approval_notifier=stub)

        # Directly check the policy engine
        tool = _make_tool()
        decision = executor._policy_engine.check(
            tool, {"amount": "5000", "token_in": "USDC", "chain": "arbitrum"}
        )
        assert decision.requires_approval is False
        assert len(stub.notified) == 0  # Notifier was never called


# ---------------------------------------------------------------------------
# ApprovalConfig tests
# ---------------------------------------------------------------------------


class TestApprovalConfig:
    def test_defaults(self):
        config = ApprovalConfig()
        assert config.channel == ApprovalChannel.FILE
        assert config.timeout_seconds == 300
        assert config.webhook_url is None
        assert config.queue_dir is None

    def test_custom_values(self):
        config = ApprovalConfig(
            channel=ApprovalChannel.WEBHOOK,
            timeout_seconds=600,
            webhook_url="https://example.com/approve",
            webhook_poll_url="https://example.com/approve/check",
            webhook_poll_interval_seconds=10,
            queue_dir="/tmp/approvals",
        )
        assert config.channel == ApprovalChannel.WEBHOOK
        assert config.timeout_seconds == 600
        assert config.webhook_url == "https://example.com/approve"
        assert config.webhook_poll_url == "https://example.com/approve/check"
        assert config.webhook_poll_interval_seconds == 10
