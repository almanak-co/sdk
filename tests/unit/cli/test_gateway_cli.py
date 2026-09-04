"""Tests for the standalone `almanak gateway` CLI command."""

import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from almanak.cli.cli import _emit_gateway_startup
from almanak.cli.cli import almanak as cli

_GATEWAY_ENV_VARS = (
    "ALMANAK_GATEWAY_ALLOW_INSECURE",
    "ALMANAK_GATEWAY_AUTH_TOKEN",
    "ALMANAK_GATEWAY_DATABASE_URL",
    "ALMANAK_GATEWAY_GRPC_PORT",
    "ALMANAK_GATEWAY_NETWORK",
    "ALMANAK_IS_HOSTED",
    "ALMANAK_DEPLOYMENT_ID",
    "ALMANAK_STATE_DB",
    "ALMANAK_STRATEGY_FOLDER",
    "GATEWAY_AUTH_TOKEN",
    "GATEWAY_METRICS_ENABLED",
    "GATEWAY_METRICS_PORT",
    "GATEWAY_PORT",
)


def _gateway_env(**overrides: str) -> dict[str, str | None]:
    env: dict[str, str | None] = {name: None for name in _GATEWAY_ENV_VARS}
    env.update(overrides)
    return env


class TestGatewayCliAuth:
    """Test that the gateway CLI handles auth correctly for different networks."""

    @patch("almanak.framework.local_paths.clear_gateway_session_token")
    @patch("almanak.framework.local_paths.write_gateway_session_token", return_value=Path("/tmp/token"))
    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_mainnet_auto_generates_auth_token(self, mock_serve, mock_write_token, mock_clear_token):
        """Local mainnet uses an ephemeral token and cleans up its handoff file."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway", "--network", "mainnet", "--standalone"],
            env=_gateway_env(),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        mock_serve.assert_called_once()
        settings = mock_serve.call_args[0][0]
        assert settings.auth_token is not None
        assert len(settings.auth_token) == 32
        assert settings.allow_insecure is False
        mock_write_token.assert_called_once_with(settings.auth_token)
        mock_clear_token.assert_called_once_with()
        assert "DB Anchor         : STANDALONE (utility DB)" in result.output
        assert "Auth              : auto-generated session token (see below)" in result.output
        assert f"export ALMANAK_GATEWAY_AUTH_TOKEN={settings.auth_token}" in result.output
        assert "also written to a 0600 session file" in result.output

    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_anvil_uses_allow_insecure(self, mock_serve):
        """On anvil, allow_insecure is True and no auth token is generated."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway", "--network", "anvil", "--standalone"],
            env=_gateway_env(),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        mock_serve.assert_called_once()
        settings = mock_serve.call_args[0][0]
        assert settings.allow_insecure is True
        assert "GATEWAY_AUTH_TOKEN" not in result.output
        assert "Chains            : (on-demand)" in result.output

    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_test_network_with_explicit_token_does_not_generate_session_token(self, mock_serve):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway", "--network", "anvil", "--standalone"],
            env=_gateway_env(ALMANAK_GATEWAY_AUTH_TOKEN="test-token"),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        settings = mock_serve.call_args[0][0]
        assert settings.auth_token == "test-token"
        assert settings.allow_insecure is True
        assert "Session auth token" not in result.output

    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_mainnet_with_chains_does_not_crash(self, mock_serve):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway", "--network", "mainnet", "--chains", "arbitrum", "--standalone"],
            env=_gateway_env(),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        settings = mock_serve.call_args[0][0]
        assert settings.chains == ["arbitrum"]
        assert settings.auth_token is not None

    @patch("almanak.framework.local_paths.clear_gateway_session_token")
    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_explicit_env_auth_token_is_used(self, mock_serve, mock_clear_token):
        """When ALMANAK_GATEWAY_AUTH_TOKEN is set, no session token is generated."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway", "--network", "mainnet", "--standalone"],
            env=_gateway_env(ALMANAK_GATEWAY_AUTH_TOKEN="my-explicit-token"),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        mock_serve.assert_called_once()
        settings = mock_serve.call_args[0][0]
        assert settings.auth_token == "my-explicit-token"
        mock_clear_token.assert_not_called()
        assert "Session auth token" not in result.output

    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_mainnet_insecure_warns_without_generating_token(self, mock_serve):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway", "--network", "mainnet", "--insecure", "--standalone"],
            env=_gateway_env(),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        settings = mock_serve.call_args[0][0]
        assert settings.auth_token is None
        assert settings.allow_insecure is True
        assert "SECURITY WARNING: Insecure mode is active on network 'mainnet'." in result.output
        assert "Session auth token" not in result.output

    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_hosted_missing_auth_token_reaches_server_invariant(self, mock_serve):
        """Hosted startup must not replace a missing provisioned token with a local token."""
        from almanak.gateway._server_start_helpers import validate_deployment_invariants

        async def validate_settings(settings):
            validate_deployment_invariants(settings)

        mock_serve.side_effect = validate_settings
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway", "--network", "mainnet"],
            env=_gateway_env(
                ALMANAK_IS_HOSTED="true",
                ALMANAK_DEPLOYMENT_ID="deployment-1",
                ALMANAK_GATEWAY_DATABASE_URL="postgresql://gateway/db",
            ),
        )

        assert result.exit_code == 1
        assert isinstance(result.exception, RuntimeError)
        assert "ALMANAK_GATEWAY_AUTH_TOKEN is unset" in str(result.exception)
        settings = mock_serve.call_args[0][0]
        assert settings.auth_token is None
        assert "Session auth token" not in result.output


class TestGatewayCliContext:
    @patch("almanak.framework.local_paths.auto_detect_strategy_folder", return_value=None)
    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_local_gateway_refuses_to_start_without_db_anchor(self, mock_serve, mock_detect):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway"],
            env=_gateway_env(ALMANAK_GATEWAY_AUTH_TOKEN="explicit-token"),
        )

        assert result.exit_code == 2
        assert "Refusing to start gateway: no strategy folder resolved." in result.output
        assert "or pass --standalone" in result.output
        mock_detect.assert_called_once_with()
        mock_serve.assert_not_called()

    @patch("almanak.framework.local_paths.auto_detect_strategy_folder", return_value=None)
    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_explicit_state_db_allows_strategy_mode(self, mock_serve, _mock_detect, tmp_path):
        db_path = tmp_path / "state.db"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway"],
            env=_gateway_env(
                ALMANAK_GATEWAY_AUTH_TOKEN="explicit-token",
                ALMANAK_STATE_DB=str(db_path),
            ),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        assert mock_serve.call_args[0][0].standalone is False
        assert f"DB Anchor         : explicit (ALMANAK_STATE_DB={db_path})" in result.output

    @patch("almanak.framework.local_paths.auto_detect_strategy_folder")
    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_detected_strategy_folder_is_reported(self, mock_serve, mock_detect, tmp_path):
        mock_detect.return_value = tmp_path
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway"],
            env=_gateway_env(ALMANAK_GATEWAY_AUTH_TOKEN="explicit-token"),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        assert f"DB Anchor         : STRATEGY-PINNED ({tmp_path})" in result.output

    @patch("almanak.framework.local_paths.auto_detect_strategy_folder")
    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_hosted_gateway_skips_local_detection(self, mock_serve, mock_detect):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway"],
            env=_gateway_env(
                ALMANAK_IS_HOSTED="true",
                ALMANAK_DEPLOYMENT_ID="deployment-1",
                ALMANAK_GATEWAY_DATABASE_URL="postgresql://gateway/db",
                ALMANAK_GATEWAY_AUTH_TOKEN="hosted-token",
            ),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        mock_detect.assert_not_called()
        assert mock_serve.call_args[0][0].auth_token == "hosted-token"
        assert "DB Anchor         : HOSTED (Postgres)" in result.output

    @pytest.mark.parametrize(
        ("explicit_strategy_folder", "expected_anchor"),
        [
            ("/configured/strategy", "STRATEGY-PINNED (/configured/strategy)"),
            (None, "STRATEGY-PINNED (env)"),
        ],
    )
    def test_db_anchor_fallback_labels(self, capsys, explicit_strategy_folder, expected_anchor):
        _emit_gateway_startup(
            settings=SimpleNamespace(network="mainnet"),
            port=50051,
            metrics=True,
            metrics_port=9090,
            log_level="info",
            parsed_chains=[],
            standalone=False,
            hosted=False,
            explicit_state_db=None,
            explicit_strategy_folder=explicit_strategy_folder,
            detected_folder=None,
            session_auth_token=None,
        )

        assert f"DB Anchor         : {expected_anchor}" in capsys.readouterr().out


class TestGatewayCliOptions:
    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_options_are_normalized_and_rendered(self, mock_serve):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "gateway",
                "--network",
                "anvil",
                "--port",
                "51003",
                "--chains",
                " Arbitrum, BASE, ,",
                "--no-metrics",
                "--metrics-port",
                "9191",
                "--log-level",
                "debug",
                "--standalone",
            ],
            env=_gateway_env(),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        settings = mock_serve.call_args[0][0]
        assert settings.grpc_port == 51003
        assert settings.chains == ["arbitrum", "base"]
        assert settings.metrics_enabled is False
        assert settings.metrics_port == 9191
        assert "Chains            : arbitrum, base" in result.output
        assert "Metrics           : disabled" in result.output
        assert "Metrics Port      : N/A" in result.output
        assert "Log Level         : debug" in result.output

    @pytest.mark.parametrize(
        ("args", "expected_port"),
        [
            (["gateway", "--standalone"], 51002),
            (["gateway", "--standalone", "--port", "51003"], 51003),
        ],
    )
    @patch("almanak.gateway.server.serve", new_callable=AsyncMock)
    def test_legacy_port_env_and_cli_precedence(self, mock_serve, args, expected_port):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            args,
            env=_gateway_env(
                ALMANAK_GATEWAY_AUTH_TOKEN="explicit-token",
                ALMANAK_GATEWAY_GRPC_PORT="51001",
                GATEWAY_PORT="51002",
            ),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        assert mock_serve.call_args[0][0].grpc_port == expected_port
        assert f"gRPC Port         : {expected_port}" in result.output


class TestGatewayCliLifecycle:
    @patch("almanak.framework.local_paths.clear_gateway_session_token")
    @patch("almanak.framework.local_paths.write_gateway_session_token", return_value=None)
    @patch("almanak.gateway.server.serve", new_callable=AsyncMock, side_effect=KeyboardInterrupt)
    def test_keyboard_interrupt_reports_stop_and_clears_session_token(
        self, mock_serve, mock_write_token, mock_clear_token
    ):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["gateway", "--standalone"],
            env=_gateway_env(),
        )

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        settings = mock_serve.call_args[0][0]
        mock_write_token.assert_called_once_with(settings.auth_token)
        mock_clear_token.assert_called_once_with()
        assert "also written to a 0600 session file" not in result.output
        assert "Gateway stopped." in result.output


class TestStrategyRunNoGatewayAuth:
    """Test --no-gateway auth token resolution in strat run CLI."""

    @patch("almanak.framework.gateway_client.GatewayClient")
    def test_strat_run_no_gateway_with_almanak_auth_token(self, mock_client_class):
        """strat run --no-gateway picks ALMANAK_GATEWAY_AUTH_TOKEN over GATEWAY_AUTH_TOKEN."""
        runner = CliRunner()
        env = os.environ.copy()
        # Clean up any existing auth tokens
        env.pop("ALMANAK_GATEWAY_AUTH_TOKEN", None)
        env.pop("GATEWAY_AUTH_TOKEN", None)
        env["ALMANAK_GATEWAY_AUTH_TOKEN"] = "almanak-token"
        env["GATEWAY_AUTH_TOKEN"] = "legacy-token"

        # Mock the gateway client to avoid actual connection
        mock_client = mock_client_class.return_value
        mock_client.wait_for_ready.return_value = True
        mock_client.health_check.return_value = True

        with patch("almanak.framework.strategies.intent_strategy.IntentStrategy"):
            runner.invoke(cli, ["strat", "run", "-d", ".", "--no-gateway", "--once"], env=env)

            # Check that GatewayClient was created with the ALMANAK_ token
            assert mock_client_class.called
            call_args = mock_client_class.call_args
            if call_args:
                gateway_config = call_args[0][0]
                assert gateway_config.auth_token == "almanak-token"

    @patch("almanak.framework.gateway_client.GatewayClient")
    def test_strat_run_no_gateway_with_legacy_auth_token(self, mock_client_class):
        """strat run --no-gateway falls back to GATEWAY_AUTH_TOKEN when ALMANAK_ not set."""
        runner = CliRunner()
        env = os.environ.copy()
        # Clean up any existing auth tokens
        env.pop("ALMANAK_GATEWAY_AUTH_TOKEN", None)
        env.pop("GATEWAY_AUTH_TOKEN", None)
        env["GATEWAY_AUTH_TOKEN"] = "legacy-token"

        # Mock the gateway client
        mock_client = mock_client_class.return_value
        mock_client.wait_for_ready.return_value = True
        mock_client.health_check.return_value = True

        with patch("almanak.framework.strategies.intent_strategy.IntentStrategy"):
            runner.invoke(cli, ["strat", "run", "-d", ".", "--no-gateway", "--once"], env=env)

            # Check that GatewayClient was created with the legacy token
            if mock_client_class.called:
                call_args = mock_client_class.call_args
                if call_args:
                    gateway_config = call_args[0][0]
                    assert gateway_config.auth_token == "legacy-token"
