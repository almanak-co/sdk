"""Tests for the ``almanak ax`` CLI command group and create_cli_executor factory."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.cli.cli import almanak
from almanak.framework.agent_tools.cli_executor import (
    GatewayConnectionError,
    _resolve_wallet_address,
    create_cli_executor,
)


class TestAxCliGroup:
    """Test that the ax command group is registered and shows help."""

    def test_ax_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--help"])
        assert result.exit_code == 0
        assert "Execute DeFi actions directly" in result.output

    def test_ax_no_subcommand(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax"])
        # Should show help/usage when no subcommand is given
        assert result.exit_code == 0

    def test_ax_options_parsed(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "base", "--help"])
        assert result.exit_code == 0

    def test_ax_dry_run_flag(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "--help"])
        assert result.exit_code == 0

    def test_ax_json_flag(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--json", "--help"])
        assert result.exit_code == 0


class TestGatewayConnectionError:
    def test_error_message(self):
        err = GatewayConnectionError("localhost", 50051)
        assert "localhost:50051" in err.format_message()
        assert "almanak gateway" in err.format_message()


class TestResolveWalletAddress:
    def test_no_key_returns_empty(self):
        with patch.dict("os.environ", {}, clear=True):
            assert _resolve_wallet_address() == ""

    def test_valid_key_returns_address(self):
        # Well-known test private key (Anvil default #0)
        test_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        with patch.dict("os.environ", {"ALMANAK_PRIVATE_KEY": test_key}):
            addr = _resolve_wallet_address()
            assert addr.startswith("0x")
            assert len(addr) == 42

    def test_invalid_key_returns_empty(self):
        with patch.dict("os.environ", {"ALMANAK_PRIVATE_KEY": "not-a-key"}):
            assert _resolve_wallet_address() == ""


class TestCreateCliExecutor:
    def test_gateway_unreachable_raises(self):
        """When gateway is not running, should raise GatewayConnectionError."""
        with pytest.raises(GatewayConnectionError):
            create_cli_executor(
                gateway_host="localhost",
                gateway_port=19999,  # unlikely to be in use
                connect_timeout=1.0,
            )

    @patch("almanak.framework.agent_tools.cli_executor.GatewayClient")
    def test_successful_connection(self, mock_client_cls):
        """When gateway is reachable, should return (executor, client)."""
        mock_client = MagicMock()
        mock_client.wait_for_ready.return_value = True
        mock_client_cls.return_value = mock_client

        executor, client = create_cli_executor(
            gateway_host="localhost",
            gateway_port=50051,
            chain="base",
        )

        assert executor is not None
        assert client is mock_client
        mock_client.connect.assert_called_once()
        mock_client.wait_for_ready.assert_called_once()

    @patch("almanak.framework.agent_tools.cli_executor.GatewayClient")
    def test_policy_constraints_applied(self, mock_client_cls):
        """Policy settings should be passed through to the executor."""
        mock_client = MagicMock()
        mock_client.wait_for_ready.return_value = True
        mock_client_cls.return_value = mock_client

        executor, _ = create_cli_executor(
            chain="base",
            max_single_trade_usd=5000,
            max_daily_spend_usd=20000,
            allowed_chains=("base", "arbitrum"),
        )

        from decimal import Decimal

        policy = executor._policy_engine.policy
        assert policy.max_single_trade_usd == Decimal("5000")
        assert policy.max_daily_spend_usd == Decimal("20000")
        assert policy.allowed_chains == {"base", "arbitrum"}
        # CLI-specific: no cooldown or rebalance gate for one-shot commands
        assert policy.cooldown_seconds == 0
        assert policy.require_rebalance_check is False

    @patch("almanak.framework.agent_tools.cli_executor.GatewayClient")
    def test_default_allowed_chains_is_none(self, mock_client_cls):
        """Default allowed_chains should be None (all chains) so bridge works out of the box."""
        mock_client = MagicMock()
        mock_client.wait_for_ready.return_value = True
        mock_client_cls.return_value = mock_client

        executor, _ = create_cli_executor(chain="arbitrum")

        policy = executor._policy_engine.policy
        assert policy.allowed_chains is None, (
            "CLI default should allow all chains so cross-chain operations like bridge work"
        )

    @patch("almanak.framework.agent_tools.cli_executor.GatewayClient")
    def test_wallet_from_env(self, mock_client_cls):
        """Wallet address should be derived from ALMANAK_PRIVATE_KEY."""
        mock_client = MagicMock()
        mock_client.wait_for_ready.return_value = True
        mock_client_cls.return_value = mock_client

        test_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        with patch.dict("os.environ", {"ALMANAK_PRIVATE_KEY": test_key}):
            executor, _ = create_cli_executor()

        assert executor._wallet_address.startswith("0x")
        assert len(executor._wallet_address) == 42
