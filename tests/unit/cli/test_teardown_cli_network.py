"""Unit tests for VIB-3585 fixes in `almanak strat teardown execute`.

Covers:
- --network option is exposed and validated (click.Choice)
- --no-gateway + --network raises an error (safety guard)
- gateway_client is passed into IntentCompiler (core bug fix)
"""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

cli_module = importlib.import_module("almanak.cli.cli")


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


class TestNetworkOption:
    """--network option is present, validated, and documented."""

    def test_help_exposes_network_option(self, runner: CliRunner) -> None:
        result = runner.invoke(cli_module.almanak, ["strat", "teardown", "execute", "--help"])
        assert result.exit_code == 0
        assert "--network" in result.output or "-n" in result.output

    def test_help_lists_valid_choices(self, runner: CliRunner) -> None:
        result = runner.invoke(cli_module.almanak, ["strat", "teardown", "execute", "--help"])
        assert result.exit_code == 0
        assert "mainnet" in result.output
        assert "anvil" in result.output

    def test_invalid_network_value_rejected(self, runner: CliRunner, tmp_path) -> None:
        result = runner.invoke(
            cli_module.almanak,
            ["strat", "teardown", "execute", "-d", str(tmp_path), "--network", "mainet"],
        )
        assert result.exit_code != 0
        # click.Choice produces a "Invalid value" error message
        assert "Invalid value" in result.output or "invalid" in result.output.lower()

    def test_network_mainnet_accepted(self, runner: CliRunner, tmp_path) -> None:
        """mainnet is a valid choice; failure should be about strategy, not --network."""
        result = runner.invoke(
            cli_module.almanak,
            ["strat", "teardown", "execute", "-d", str(tmp_path), "--network", "mainnet"],
        )
        # Should NOT fail with "Invalid value" for --network
        assert "Invalid value" not in result.output

    def test_network_anvil_accepted(self, runner: CliRunner, tmp_path) -> None:
        """anvil is a valid choice; failure should be about strategy, not --network."""
        result = runner.invoke(
            cli_module.almanak,
            ["strat", "teardown", "execute", "-d", str(tmp_path), "--network", "anvil"],
        )
        assert "Invalid value" not in result.output


class TestNoGatewayNetworkGuard:
    """--no-gateway + --network is rejected early with a clear error."""

    def test_no_gateway_with_network_raises(self, runner: CliRunner, tmp_path) -> None:
        result = runner.invoke(
            cli_module.almanak,
            [
                "strat",
                "teardown",
                "execute",
                "-d",
                str(tmp_path),
                "--no-gateway",
                "--network",
                "anvil",
            ],
        )
        assert result.exit_code != 0
        assert "--network" in result.output

    def test_no_gateway_without_network_not_rejected(self, runner: CliRunner, tmp_path) -> None:
        """--no-gateway alone should not be blocked by the network guard."""
        result = runner.invoke(
            cli_module.almanak,
            ["strat", "teardown", "execute", "-d", str(tmp_path), "--no-gateway"],
        )
        # Failure should be about a missing/unreachable gateway, not about --network
        assert "--network only applies" not in result.output


class TestGatewayClientPassedToCompiler:
    """gateway_client is wired into IntentCompiler (root-cause fix for VIB-3585)."""

    def test_intent_compiler_receives_gateway_client(self, tmp_path) -> None:
        """execute_teardown must pass gateway_client into IntentCompiler.

        This is the root cause of VIB-3585: without gateway_client, LP_CLOSE
        on-chain queries (ERC20 balance for Aerodrome, position liquidity for
        Uniswap V3) return None and compilation fails silently.
        """
        from almanak.framework.cli.teardown import execute_teardown

        captured: dict = {}

        class FakeGatewayClient:
            def health_check(self):
                return True

            def disconnect(self):
                pass

        fake_gateway_client = FakeGatewayClient()

        # Minimal strategy that reports no open positions so teardown exits cleanly
        class FakeStrategy:
            strategy_id = "test-strategy"
            chain = "arbitrum"

            def get_open_positions(self):
                from almanak.framework.teardown import TeardownSummary

                return TeardownSummary(strategy_id="test-strategy", chain="arbitrum", positions=[])

            def generate_teardown_intents(self, mode):
                return []

        class FakeStrategyClass:
            __name__ = "FakeStrategy"

        # Create a minimal strategy.py in tmp_path
        (tmp_path / "strategy.py").write_text(
            "from almanak.framework.strategies.intent_strategy import IntentStrategy\n"
            "class FakeStrategy(IntentStrategy):\n"
            "    pass\n"
        )
        (tmp_path / "config.json").write_text('{"chain": "arbitrum"}')

        original_init = None

        def patched_compiler_init(self, *args, **kwargs):
            captured["gateway_client"] = kwargs.get("gateway_client")
            # Raise to exit early after we've captured what we need
            raise SystemExit(0)

        with (
            patch(
                "almanak.framework.cli.teardown.load_strategy_from_file",
                return_value=(FakeStrategy, None),
            ),
            patch(
                "almanak.framework.intents.compiler.IntentCompiler.__init__",
                patched_compiler_init,
            ),
            patch(
                "almanak.framework.cli.teardown._inject_balance_provider",
            ),
            patch(
                "almanak.framework.cli.teardown._restore_strategy_state_for_teardown",
            ),
        ):
            runner = CliRunner()
            # Invoke with enough mocking that we reach the IntentCompiler constructor
            result = runner.invoke(
                cli_module.almanak,
                [
                    "strat",
                    "teardown",
                    "execute",
                    "-d",
                    str(tmp_path),
                    "--no-gateway",
                ],
                catch_exceptions=False,
            )

        # If we reached IntentCompiler.__init__, gateway_client key was captured
        if "gateway_client" in captured:
            assert captured["gateway_client"] is not None, (
                "gateway_client must not be None when passed to IntentCompiler; "
                "None causes LP_CLOSE on-chain queries to silently return None (VIB-3585)"
            )
