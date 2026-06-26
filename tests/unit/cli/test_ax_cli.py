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


class TestLpInfoNetwork:
    """Test that lp-info passes --network to the tool executor."""

    def test_lp_info_default_network_is_mainnet(self):
        """lp-info should default to mainnet network."""
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lp-info", "--help"])
        assert result.exit_code == 0
        assert "mainnet" in result.output
        assert "anvil" in result.output

    @patch("almanak.framework.cli.ax._run_tool")
    def test_lp_info_passes_network_mainnet(self, mock_run_tool):
        """lp-info should pass network='mainnet' by default."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--chain", "arbitrum", "lp-info", "123456"],
        )
        # Should have called _run_tool with network="mainnet"
        mock_run_tool.assert_called_once()
        call_args = mock_run_tool.call_args
        tool_args = call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("args", {})
        assert tool_args["network"] == "mainnet"
        assert tool_args["position_id"] == "123456"

    @patch("almanak.framework.cli.ax._run_tool")
    def test_lp_info_passes_network_anvil(self, mock_run_tool):
        """lp-info --network anvil should pass network='anvil'."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--chain", "arbitrum", "lp-info", "123456", "--network", "anvil"],
        )
        mock_run_tool.assert_called_once()
        call_args = mock_run_tool.call_args
        tool_args = call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("args", {})
        assert tool_args["network"] == "anvil"


class TestLpClose:
    """`ax lp-close` argument plumbing, incl. V4 close-by-id (VIB-5361)."""

    @patch("almanak.framework.cli.ax._run_tool")
    def test_lp_close_dry_run_passes_dry_run_flag(self, mock_run_tool):
        mock_run_tool.return_value = MagicMock(status="success")
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "arbitrum", "lp-close", "123456", "--dry-run"])
        assert result.exit_code == 0
        args = mock_run_tool.call_args[0][2]
        assert args["dry_run"] is True
        assert args["position_id"] == "123456"
        assert args["protocol"] == "uniswap_v3"
        assert "pool" not in args

    @patch("almanak.framework.cli.ax._run_tool")
    def test_lp_close_execute_with_yes(self, mock_run_tool):
        mock_run_tool.return_value = MagicMock(status="success")
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "arbitrum", "lp-close", "123456", "--yes"])
        assert result.exit_code == 0
        args = mock_run_tool.call_args[0][2]
        assert args["amount"] == "all"
        assert args["collect_fees"] is True

    @patch("almanak.framework.cli.ax._run_tool")
    def test_lp_close_v4_by_id_omits_pool(self, mock_run_tool):
        """V4 close-by-id: no --pool → args carry no pool, so the V4 compiler
        resolves currencies from the position id on-chain."""
        mock_run_tool.return_value = MagicMock(status="success")
        runner = CliRunner()
        result = runner.invoke(
            almanak, ["ax", "--chain", "base", "lp-close", "654321", "--protocol", "uniswap_v4", "--yes"]
        )
        assert result.exit_code == 0
        args = mock_run_tool.call_args[0][2]
        assert args["protocol"] == "uniswap_v4"
        assert "pool" not in args

    @patch("almanak.framework.cli.ax._run_tool")
    def test_lp_close_v4_with_pool_hint(self, mock_run_tool):
        mock_run_tool.return_value = MagicMock(status="success")
        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--chain", "base", "lp-close", "654321", "--protocol", "uniswap_v4", "--pool", "WETH/USDC/3000", "--yes"],
        )
        assert result.exit_code == 0
        args = mock_run_tool.call_args[0][2]
        assert args["pool"] == "WETH/USDC/3000"

    @patch("almanak.framework.cli.ax._run_tool")
    def test_lp_close_no_collect_fees(self, mock_run_tool):
        mock_run_tool.return_value = MagicMock(status="success")
        runner = CliRunner()
        result = runner.invoke(
            almanak, ["ax", "--chain", "arbitrum", "lp-close", "123456", "--no-collect-fees", "--yes"]
        )
        assert result.exit_code == 0
        args = mock_run_tool.call_args[0][2]
        assert args["collect_fees"] is False

    @patch("almanak.framework.cli.ax._run_tool")
    def test_lp_close_error_status_exits_nonzero(self, mock_run_tool):
        mock_run_tool.return_value = MagicMock(status="error")
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "arbitrum", "lp-close", "123456", "--yes"])
        assert result.exit_code == 1


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


class TestAxSignerKeyResolution:
    """VIB-5457: ax's managed gateway must sign with the EFFECTIVE inline
    ALMANAK_PRIVATE_KEY (the same key the ``from_address`` is derived from), not a
    .env master / gateway-prefixed key. The bug was an asymmetry: ``from_address``
    came from ALMANAK_PRIVATE_KEY but the signer came from
    ``load_config().gateway.private_key`` (which prefers ALMANAK_GATEWAY_PRIVATE_KEY)
    → from_address(pool) != signer(master) → every mutating ax op reverts.
    """

    # Two well-known Anvil keys with distinct addresses.
    POOL = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"  # anvil #0
    MASTER = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"  # anvil #1

    @staticmethod
    def _addr(key: str) -> str:
        from eth_account import Account

        return Account.from_key(key).address

    def test_inline_key_wins_over_gateway_prefixed_for_signer(self):
        """The effective signer key (ax's primary resolution rung) is the inline
        ALMANAK_PRIVATE_KEY, and it matches the wallet ``from_address`` — even when
        a gateway-prefixed master key is also present."""
        from almanak.config.runtime import private_key_from_env

        with patch.dict(
            "os.environ",
            {"ALMANAK_PRIVATE_KEY": self.POOL, "ALMANAK_GATEWAY_PRIVATE_KEY": self.MASTER},
            clear=True,
        ):
            # This is the primary rung of ax._start_managed_gateway's resolution:
            # ``private_key_from_env() or (load_config().gateway.private_key or "")``.
            effective = private_key_from_env() or ""
            assert self._addr(effective) == self._addr(self.POOL)
            assert self._addr(effective) != self._addr(self.MASTER)
            # Symmetry restored: signer address == the from_address ax derives.
            assert self._addr(effective) == _resolve_wallet_address()

    def test_assert_signer_matches_intended_wallet_raises_on_mismatch(self):
        import click

        from almanak.framework.cli.ax import _assert_signer_matches_intended_wallet

        with pytest.raises(click.ClickException, match="does not match"):
            _assert_signer_matches_intended_wallet(self.MASTER, self._addr(self.POOL))

    def test_assert_signer_matches_intended_wallet_noop_on_match(self):
        from almanak.framework.cli.ax import _assert_signer_matches_intended_wallet

        # No exception when the signer derives the intended wallet.
        _assert_signer_matches_intended_wallet(self.POOL, self._addr(self.POOL))

    def test_assert_signer_matches_intended_wallet_noop_when_wallet_unset(self):
        from almanak.framework.cli.ax import _assert_signer_matches_intended_wallet

        # No intended wallet pinned (auto-derive path) → nothing to assert.
        _assert_signer_matches_intended_wallet(self.POOL, "")

    def test_assert_signer_matches_intended_wallet_noop_on_non_evm_key(self):
        from almanak.framework.cli.ax import _assert_signer_matches_intended_wallet

        # Unparseable / non-EVM key: skip silently (Solana etc.) rather than crash.
        _assert_signer_matches_intended_wallet("not-a-hex-key", self._addr(self.POOL))


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
