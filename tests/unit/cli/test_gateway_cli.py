"""Tests for the standalone `almanak gateway` CLI command."""

import os
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner

from almanak.cli.cli import almanak as cli


class TestGatewayCliAuth:
    """Test that the gateway CLI handles auth correctly for different networks."""

    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_mainnet_auto_generates_auth_token(self, mock_serve):
        """On mainnet without explicit auth token, a session token is auto-generated."""
        runner = CliRunner()
        env = {k: v for k, v in os.environ.items()}
        env.pop("ALMANAK_GATEWAY_AUTH_TOKEN", None)
        env.pop("ALMANAK_GATEWAY_ALLOW_INSECURE", None)

        result = runner.invoke(cli, ["gateway", "--network", "mainnet"], env=env)

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        # serve() should have been called with settings that have an auth_token
        mock_serve.assert_called_once()
        settings = mock_serve.call_args[0][0]
        assert settings.auth_token is not None, "Expected auto-generated auth token for mainnet"
        assert len(settings.auth_token) == 32  # uuid4().hex is 32 chars
        assert settings.allow_insecure is False
        # Session token should be displayed in output
        assert "GATEWAY_AUTH_TOKEN" in result.output

    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_anvil_uses_allow_insecure(self, mock_serve):
        """On anvil, allow_insecure is True and no auth token is generated."""
        runner = CliRunner()
        env = {k: v for k, v in os.environ.items()}
        env.pop("ALMANAK_GATEWAY_AUTH_TOKEN", None)
        env.pop("ALMANAK_GATEWAY_ALLOW_INSECURE", None)

        result = runner.invoke(cli, ["gateway", "--network", "anvil"], env=env)

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        mock_serve.assert_called_once()
        settings = mock_serve.call_args[0][0]
        assert settings.allow_insecure is True
        # No session token output for test networks
        assert "GATEWAY_AUTH_TOKEN" not in result.output

    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_mainnet_with_chains_does_not_crash(self, mock_serve):
        """Regression test: mainnet with --chains should not crash."""
        runner = CliRunner()
        env = {k: v for k, v in os.environ.items()}
        env.pop("ALMANAK_GATEWAY_AUTH_TOKEN", None)
        env.pop("ALMANAK_GATEWAY_ALLOW_INSECURE", None)

        result = runner.invoke(cli, ["gateway", "--network", "mainnet", "--chains", "arbitrum"], env=env)

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        mock_serve.assert_called_once()
        settings = mock_serve.call_args[0][0]
        assert settings.chains == ["arbitrum"]
        assert settings.auth_token is not None

    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_explicit_env_auth_token_is_used(self, mock_serve):
        """When ALMANAK_GATEWAY_AUTH_TOKEN is set, no session token is generated."""
        runner = CliRunner()
        env = {k: v for k, v in os.environ.items()}
        env["ALMANAK_GATEWAY_AUTH_TOKEN"] = "my-explicit-token"
        env.pop("ALMANAK_GATEWAY_ALLOW_INSECURE", None)

        result = runner.invoke(cli, ["gateway", "--network", "mainnet"], env=env)

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        mock_serve.assert_called_once()
        settings = mock_serve.call_args[0][0]
        # The env var is picked up by GatewaySettings via pydantic-settings
        # No session token should be displayed since explicit token is set
        assert settings.auth_token == "my-explicit-token"
        assert "Session auth token" not in result.output
