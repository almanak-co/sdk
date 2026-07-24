"""Tests for ``almanak mcp serve`` (``almanak.cli.cli.mcp_serve``).

Covers both serve modes and their exit paths with the MCP server, gateway
client, and tool executor faked — no sockets, no stdio protocol traffic:

- schema-only mode (no gateway client constructed, executor=None),
- schema-only KeyboardInterrupt swallow,
- gateway mode happy path (policy assembled from CLI options),
- gateway-not-ready exit 1,
- server error exit 1 + disconnect,
- gateway-mode KeyboardInterrupt swallow + disconnect.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.cli.cli import almanak
from almanak.core.chains import DEFAULT_CHAIN


class _FakeServer:
    """Stand-in for AlmanakMCPStdioServer with an awaitable ``run``."""

    instances: list["_FakeServer"] = []
    run_error: BaseException | None = None

    def __init__(self, executor=None):
        self.executor = executor
        self.ran = False
        type(self).instances.append(self)

    async def run(self):
        if type(self).run_error is not None:
            raise type(self).run_error
        self.ran = True


@pytest.fixture
def fake_server(monkeypatch):
    _FakeServer.instances = []
    _FakeServer.run_error = None
    monkeypatch.setattr(
        "almanak.framework.agent_tools.adapters.mcp_server.AlmanakMCPStdioServer",
        _FakeServer,
    )
    return _FakeServer


def _mock_gateway(monkeypatch, *, ready: bool = True):
    """Patch GatewayClient/GatewayClientConfig; return the client mock."""
    client = MagicMock()
    client.wait_for_ready.return_value = ready

    config_cls = MagicMock()
    config_cls.from_env.return_value = MagicMock()

    monkeypatch.setattr(
        "almanak.framework.gateway_client.GatewayClient",
        MagicMock(return_value=client),
    )
    monkeypatch.setattr(
        "almanak.framework.gateway_client.GatewayClientConfig",
        config_cls,
    )
    return client


class TestMcpServeSchemaOnly:
    def test_schema_only_serves_without_gateway(self, fake_server, monkeypatch):
        gateway_client_cls = MagicMock()
        monkeypatch.setattr(
            "almanak.framework.gateway_client.GatewayClient", gateway_client_cls
        )

        runner = CliRunner()
        result = runner.invoke(almanak, ["mcp", "serve", "--schema-only"])

        assert result.exit_code == 0
        assert len(fake_server.instances) == 1
        server = fake_server.instances[0]
        assert server.executor is None
        assert server.ran is True
        gateway_client_cls.assert_not_called()

    def test_schema_only_keyboard_interrupt_exits_cleanly(self, fake_server):
        fake_server.run_error = KeyboardInterrupt()

        runner = CliRunner()
        result = runner.invoke(almanak, ["mcp", "serve", "--schema-only"])

        assert result.exit_code == 0

    def test_schema_only_accepts_log_level_option(self, fake_server):
        runner = CliRunner()
        result = runner.invoke(
            almanak, ["mcp", "serve", "--schema-only", "--log-level", "debug"]
        )

        assert result.exit_code == 0
        assert fake_server.instances[0].ran is True

    def test_rejects_unknown_log_level(self, fake_server):
        runner = CliRunner()
        result = runner.invoke(
            almanak, ["mcp", "serve", "--schema-only", "--log-level", "loud"]
        )

        assert result.exit_code != 0
        assert "Invalid value" in result.output


class TestMcpServeGatewayMode:
    def test_gateway_not_ready_exits_1_and_disconnects(self, fake_server, monkeypatch):
        client = _mock_gateway(monkeypatch, ready=False)

        runner = CliRunner()
        result = runner.invoke(
            almanak, ["mcp", "serve", "--gateway-host", "10.0.0.5", "--gateway-port", "50099"]
        )

        assert result.exit_code == 1
        assert "Cannot connect to gateway at 10.0.0.5:50099" in result.output
        assert "almanak gateway" in result.output
        client.connect.assert_called_once_with()
        client.wait_for_ready.assert_called_once_with(timeout=10.0)
        client.disconnect.assert_called_once_with()
        # No server should have been constructed.
        assert fake_server.instances == []

    @patch("almanak.framework.agent_tools.executor.ToolExecutor")
    def test_happy_path_builds_policy_from_options(
        self, mock_executor_cls, fake_server, monkeypatch
    ):
        client = _mock_gateway(monkeypatch, ready=True)
        executor = MagicMock()
        mock_executor_cls.return_value = executor

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "mcp",
                "serve",
                "--max-single-trade-usd",
                "5000",
                "--max-daily-spend-usd",
                "20000",
                "--allowed-tokens",
                "USDC",
                "--allowed-tokens",
                "WETH",
                "--allowed-protocols",
                "uniswap_v3",
                "--allowed-chains",
                "arbitrum",
                "--allowed-chains",
                "base",
            ],
        )

        assert result.exit_code == 0
        kwargs = mock_executor_cls.call_args.kwargs
        assert kwargs["gateway_client"] is client
        policy = kwargs["policy"]
        assert policy.max_single_trade_usd == Decimal("5000")
        assert policy.max_daily_spend_usd == Decimal("20000")
        assert policy.allowed_tokens == {"USDC", "WETH"}
        assert policy.allowed_protocols == {"uniswap_v3"}
        assert policy.allowed_chains == {"arbitrum", "base"}
        server = fake_server.instances[0]
        assert server.executor is executor
        assert server.ran is True
        client.disconnect.assert_called_once_with()

    @patch("almanak.framework.agent_tools.executor.ToolExecutor")
    def test_default_scope_options_map_to_open_policy(
        self, mock_executor_cls, fake_server, monkeypatch
    ):
        _mock_gateway(monkeypatch, ready=True)

        runner = CliRunner()
        result = runner.invoke(almanak, ["mcp", "serve"])

        assert result.exit_code == 0
        policy = mock_executor_cls.call_args.kwargs["policy"]
        # No --allowed-tokens/--allowed-protocols means "all allowed" (None),
        # never an empty set (which would deny everything).
        assert policy.allowed_tokens is None
        assert policy.allowed_protocols is None
        assert policy.allowed_chains == {DEFAULT_CHAIN}
        assert policy.max_single_trade_usd == Decimal("10000")
        assert policy.max_daily_spend_usd == Decimal("50000")

    @patch("almanak.framework.agent_tools.executor.ToolExecutor")
    def test_server_error_exits_1_and_disconnects(
        self, mock_executor_cls, fake_server, monkeypatch
    ):
        client = _mock_gateway(monkeypatch, ready=True)
        fake_server.run_error = RuntimeError("stdio pipe broke")

        runner = CliRunner()
        result = runner.invoke(almanak, ["mcp", "serve"])

        assert result.exit_code == 1
        assert "Error: stdio pipe broke" in result.output
        client.disconnect.assert_called_once_with()

    @patch("almanak.framework.agent_tools.executor.ToolExecutor")
    def test_keyboard_interrupt_exits_cleanly_and_disconnects(
        self, mock_executor_cls, fake_server, monkeypatch
    ):
        client = _mock_gateway(monkeypatch, ready=True)
        fake_server.run_error = KeyboardInterrupt()

        runner = CliRunner()
        result = runner.invoke(almanak, ["mcp", "serve"])

        assert result.exit_code == 0
        client.disconnect.assert_called_once_with()

    def test_negative_trade_limit_rejected(self, fake_server, monkeypatch):
        _mock_gateway(monkeypatch, ready=True)

        runner = CliRunner()
        result = runner.invoke(
            almanak, ["mcp", "serve", "--max-single-trade-usd", "-1"]
        )

        assert result.exit_code != 0
        assert "Invalid value" in result.output
