"""Tests for ``almanak ax`` subcommands (price, balance, swap, tools, run)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from almanak.cli.cli import almanak
from almanak.framework.agent_tools.schemas import ToolResponse


def _mock_executor_and_client():
    """Create a mock executor/client pair for testing."""
    mock_executor = MagicMock()
    mock_client = MagicMock()
    mock_client.wait_for_ready.return_value = True
    return mock_executor, mock_client


class TestAxPrice:
    @patch("almanak.framework.cli.ax._get_executor")
    def test_price_success(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        import asyncio

        response = ToolResponse(
            status="success",
            data={"token": "ETH", "price_usd": 2500.0, "source": "coingecko"},
        )

        async def mock_execute(tool_name, args):
            assert tool_name == "get_price"
            assert args["token"] == "ETH"
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "price", "ETH"])
        assert result.exit_code == 0
        assert "2500" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_price_json_output(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="success",
            data={"token": "ETH", "price_usd": 2500.0},
        )

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--json", "price", "ETH"])
        assert result.exit_code == 0
        assert '"price_usd": 2500.0' in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_price_with_chain(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="success", data={"token": "ETH", "price_usd": 2500.0})

        captured_args = {}

        async def mock_execute(tool_name, args):
            captured_args.update(args)
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "base", "price", "ETH"])
        assert result.exit_code == 0
        assert captured_args["chain"] == "base"


class TestAxBalance:
    @patch("almanak.framework.cli.ax._get_executor")
    def test_balance_success(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="success",
            data={"token": "USDC", "balance": "1000.50", "balance_usd": "1000.50"},
        )

        async def mock_execute(tool_name, args):
            assert tool_name == "get_balance"
            assert args["token"] == "USDC"
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "balance", "USDC"])
        assert result.exit_code == 0
        assert "1000.50" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_balance_error(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="error",
            error={"message": "Token not found"},
        )

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "balance", "FAKE"])
        assert result.exit_code != 0


class TestAxSwap:
    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_dry_run(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="simulated",
            data={"estimated_output": "0.04 ETH", "gas_estimate": "150000"},
        )

        captured_args = {}

        async def mock_execute(tool_name, args):
            assert tool_name == "swap_tokens"
            captured_args.update(args)
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "swap", "USDC", "ETH", "100"])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert captured_args["dry_run"] is True

    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_with_yes(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="success",
            data={"tx_hash": "0xabc123", "amount_out": "0.04"},
        )

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--yes", "swap", "USDC", "ETH", "100"])
        assert result.exit_code == 0
        assert "0xabc123" in result.output

    @patch("almanak.framework.cli.ax_render.is_interactive", return_value=True)
    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_interactive_confirm(self, mock_get_exec, _mock_tty):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="success",
            data={"tx_hash": "0xdef456"},
        )

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        # Simulate user typing "y" to confirm
        result = runner.invoke(almanak, ["ax", "swap", "USDC", "ETH", "100"], input="y\n")
        assert result.exit_code == 0

    @patch("almanak.framework.cli.ax_render.is_interactive", return_value=True)
    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_interactive_cancel(self, mock_get_exec, _mock_tty):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        runner = CliRunner()
        # Simulate user typing "n" to cancel
        result = runner.invoke(almanak, ["ax", "swap", "USDC", "ETH", "100"], input="n\n")
        assert result.exit_code == 0
        assert "Cancelled" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_slippage_option(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="simulated", data={})
        captured_args = {}

        async def mock_execute(tool_name, args):
            captured_args.update(args)
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "swap", "USDC", "ETH", "100", "--slippage", "100"])
        assert result.exit_code == 0
        assert captured_args["slippage_bps"] == 100

    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_protocol_option(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="simulated", data={})
        captured_args = {}

        async def mock_execute(tool_name, args):
            captured_args.update(args)
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "swap", "USDC", "ETH", "100", "--protocol", "uniswap_v3"])
        assert result.exit_code == 0
        assert captured_args["protocol"] == "uniswap_v3"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_dry_run_eth_shows_weth_note(self, mock_get_exec):
        """When swapping to ETH in dry-run, output should note that DEX produces WETH."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="simulated",
            data={"estimated_output": "0.04 ETH"},
        )

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "swap", "USDC", "ETH", "100"])
        assert result.exit_code == 0
        assert "WETH" in result.output
        assert "unwrap" in result.output.lower()

    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_yes_eth_shows_unwrap_tip(self, mock_get_exec):
        """When swapping to ETH with --yes, output should show unwrap tip."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="success",
            data={"tx_hash": "0xabc123", "amount_out": "0.04"},
        )

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--yes", "swap", "USDC", "ETH", "100"])
        assert result.exit_code == 0
        assert "WETH" in result.output
        assert "unwrap" in result.output.lower()

    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_usdc_no_weth_note(self, mock_get_exec):
        """When swapping to USDC (not native), no WETH note should appear."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="simulated",
            data={"estimated_output": "100 USDC"},
        )

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "swap", "ETH", "USDC", "0.1"])
        assert result.exit_code == 0
        assert "unwrap" not in result.output.lower()

    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_avax_shows_wavax_note(self, mock_get_exec):
        """Swapping to AVAX should note WAVAX output."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="simulated", data={})

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "--chain", "avalanche", "swap", "USDC", "AVAX", "10"])
        assert result.exit_code == 0
        assert "WAVAX" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_swap_bnb_shows_wbnb_note(self, mock_get_exec):
        """Swapping to BNB on BSC should note WBNB output."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="simulated", data={})

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "--chain", "bsc", "swap", "USDC", "BNB", "10"])
        assert result.exit_code == 0
        assert "WBNB" in result.output

    def test_swap_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "swap", "--help"])
        assert result.exit_code == 0
        assert "Swap tokens" in result.output
        assert "--slippage" in result.output


class TestAxTools:
    def test_tools_list(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "tools"])
        assert result.exit_code == 0
        assert "Available tools" in result.output
        assert "get_price" in result.output
        assert "swap_tokens" in result.output

    def test_tools_list_json(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--json", "tools"])
        assert result.exit_code == 0
        import json

        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) > 0
        names = [t["name"] for t in data]
        assert "get_price" in names

    def test_tools_filter_category(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "tools", "--category", "action"])
        assert result.exit_code == 0
        assert "swap_tokens" in result.output

    def test_tools_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "tools", "--help"])
        assert result.exit_code == 0
        assert "--category" in result.output

    def test_tools_describe_supply_lending(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "tools", "--describe", "supply_lending"])
        assert result.exit_code == 0
        assert "supply_lending" in result.output
        assert "Required:" in result.output
        assert "token" in result.output
        assert "amount" in result.output
        assert "Optional:" in result.output
        assert "protocol" in result.output

    def test_tools_describe_shows_defaults(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "tools", "--describe", "swap_tokens"])
        assert result.exit_code == 0
        assert "default:" in result.output
        assert "slippage_bps" in result.output

    def test_tools_describe_unknown_tool(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "tools", "--describe", "nonexistent_tool"])
        assert result.exit_code != 0
        assert "Unknown tool" in result.output

    def test_tools_describe_json_mode(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--json", "tools", "--describe", "get_price"])
        assert result.exit_code == 0
        import json

        data = json.loads(result.output)
        assert "properties" in data
        assert "_meta" in data
        assert data["_meta"]["name"] == "get_price"


class TestAxRun:
    @patch("almanak.framework.cli.ax._get_executor")
    def test_run_read_tool(self, mock_get_exec):
        """Read-only tools (NONE risk) should execute without confirmation."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="success", data={"token": "ETH", "price_usd": 2500.0})

        async def mock_execute(tool_name, args):
            assert tool_name == "get_price"
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "run", "get_price", '{"token": "ETH"}'])
        assert result.exit_code == 0
        assert "2500" in result.output

    def test_run_unknown_tool(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "run", "nonexistent_tool"])
        assert result.exit_code != 0

    def test_run_invalid_json(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "run", "get_price", "not-json"])
        assert result.exit_code != 0

    @patch("almanak.framework.cli.ax._get_executor")
    def test_run_write_tool_dry_run(self, mock_get_exec):
        """Write tools with --dry-run should simulate."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="simulated", data={"estimated": "0.04 ETH"})

        async def mock_execute(tool_name, args):
            assert args.get("dry_run") is True
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--dry-run", "run", "swap_tokens", '{"token_in": "USDC", "token_out": "ETH", "amount": "100"}'],
        )
        assert result.exit_code == 0
        assert "DRY RUN" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_run_write_tool_with_yes(self, mock_get_exec):
        """Write tools with --yes should execute without confirmation."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="success", data={"tx_hash": "0x123"})

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--yes", "run", "swap_tokens", '{"token_in": "USDC", "token_out": "ETH", "amount": "100"}'],
        )
        assert result.exit_code == 0
        assert "0x123" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_run_json_output(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="success", data={"token": "ETH", "price_usd": 2500.0})

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--json", "run", "get_price", '{"token": "ETH"}'])
        assert result.exit_code == 0
        assert '"status": "success"' in result.output

    def test_run_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "run", "--help"])
        assert result.exit_code == 0
        assert "Run any tool" in result.output


class TestAxLending:
    """Tests for the ax lending-* CLI shortcuts (supply/borrow/repay/withdraw)."""

    def test_withdraw_lending_registered_in_catalog(self):
        """withdraw_lending must be exposed via ax tools (VIB-2992)."""
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "tools", "--describe", "withdraw_lending"])
        assert result.exit_code == 0
        assert "withdraw_lending" in result.output
        assert "Required:" in result.output
        assert "token" in result.output
        assert "amount" in result.output

    def test_lending_supply_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-supply", "--help"])
        assert result.exit_code == 0
        assert "Supply tokens" in result.output

    def test_lending_borrow_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-borrow", "--help"])
        assert result.exit_code == 0
        assert "Borrow tokens" in result.output
        assert "--collateral" in result.output

    def test_lending_repay_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-repay", "--help"])
        assert result.exit_code == 0
        assert "--full" in result.output

    def test_lending_withdraw_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-withdraw", "--help"])
        assert result.exit_code == 0
        assert "--all" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_supply_dry_run(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}
        response = ToolResponse(status="simulated", data={"amount_supplied": "100"})

        async def mock_execute(tool_name, args):
            captured["tool_name"] = tool_name
            captured["args"] = args
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--dry-run", "lending-supply", "USDC", "100"],
        )
        assert result.exit_code == 0
        assert captured["tool_name"] == "supply_lending"
        assert captured["args"]["token"] == "USDC"
        assert captured["args"]["amount"] == "100"
        assert captured["args"]["dry_run"] is True
        assert captured["args"]["use_as_collateral"] is True

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_supply_no_collateral_flag(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--dry-run", "lending-supply", "USDC", "100", "--no-collateral"],
        )
        assert result.exit_code == 0
        assert captured["args"]["use_as_collateral"] is False

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_borrow_passes_collateral_args(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["tool_name"] = tool_name
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "ax",
                "--dry-run",
                "lending-borrow",
                "USDC",
                "100",
                "--collateral",
                "WETH",
                "--collateral-amount",
                "0.1",
            ],
        )
        assert result.exit_code == 0
        assert captured["tool_name"] == "borrow_lending"
        assert captured["args"]["collateral_token"] == "WETH"
        assert captured["args"]["collateral_amount"] == "0.1"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_repay_full_flag(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "lending-repay", "USDC", "--full"])
        assert result.exit_code == 0
        assert captured["args"]["amount"] == "all"

    def test_lending_repay_requires_amount_or_full(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-repay", "USDC"])
        assert result.exit_code != 0
        assert "Pass an amount, or use --full for full repayment" in result.output

    def test_lending_repay_full_conflicts_with_amount(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-repay", "USDC", "50", "--full"])
        assert result.exit_code != 0
        assert "--full conflicts" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_withdraw_all_flag(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["tool_name"] = tool_name
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "lending-withdraw", "USDC", "--all"])
        assert result.exit_code == 0
        assert captured["tool_name"] == "withdraw_lending"
        assert captured["args"]["amount"] == "all"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_withdraw_partial_amount(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", "lending-withdraw", "USDC", "50"])
        assert result.exit_code == 0
        assert captured["args"]["amount"] == "50"

    def test_lending_withdraw_requires_amount_or_all(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-withdraw", "USDC"])
        assert result.exit_code != 0
        assert "Pass an amount, or use --all" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_withdraw_market_id_passed_for_morpho(self, mock_get_exec):
        """--market-id should be forwarded for Morpho Blue withdrawals."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "ax",
                "--dry-run",
                "lending-withdraw",
                "USDC",
                "--all",
                "--protocol",
                "morpho_blue",
                "--market-id",
                "0xabc",
            ],
        )
        assert result.exit_code == 0
        assert captured["args"]["market_id"] == "0xabc"
        assert captured["args"]["protocol"] == "morpho_blue"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_supply_market_id_forwarded(self, mock_get_exec):
        """--market-id should also be forwarded for supply."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "ax",
                "--dry-run",
                "lending-supply",
                "USDC",
                "100",
                "--protocol",
                "morpho_blue",
                "--market-id",
                "0xdef",
            ],
        )
        assert result.exit_code == 0
        assert captured["args"]["market_id"] == "0xdef"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_borrow_market_id_forwarded(self, mock_get_exec):
        """--market-id should be forwarded for borrow on isolated markets."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "ax",
                "--dry-run",
                "lending-borrow",
                "USDC",
                "100",
                "--collateral",
                "WETH",
                "--collateral-amount",
                "0.1",
                "--protocol",
                "morpho_blue",
                "--market-id",
                "0xfeed",
            ],
        )
        assert result.exit_code == 0
        assert captured["args"]["market_id"] == "0xfeed"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_repay_market_id_forwarded(self, mock_get_exec):
        """--market-id should be forwarded for repay on isolated markets."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "ax",
                "--dry-run",
                "lending-repay",
                "USDC",
                "--full",
                "--protocol",
                "morpho_blue",
                "--market-id",
                "0xcafe",
            ],
        )
        assert result.exit_code == 0
        assert captured["args"]["market_id"] == "0xcafe"
        assert captured["args"]["amount"] == "all"

    def test_lending_withdraw_loan_token_rejected_on_non_morpho(self):
        """--loan-token must raise UsageError on non-Morpho protocols.

        Downstream WithdrawIntent would silently ignore the flag, leaving the
        operator wondering why their withdraw didn't behave as expected. Fail
        fast at the CLI instead (CodeRabbit PR #1535 review).
        """
        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--dry-run", "lending-withdraw", "USDC", "100", "--loan-token", "--protocol", "aave_v3"],
        )
        assert result.exit_code != 0
        assert "--loan-token is only supported on Morpho Blue" in result.output

    def test_lending_withdraw_market_id_rejected_on_non_morpho(self):
        """--market-id on non-Morpho protocols must raise UsageError."""
        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "ax",
                "--dry-run",
                "lending-withdraw",
                "USDC",
                "100",
                "--protocol",
                "aave_v3",
                "--market-id",
                "0xabc",
            ],
        )
        assert result.exit_code != 0
        assert "--market-id is only supported on isolated-market protocols" in result.output

    def test_lending_supply_market_id_rejected_on_non_morpho(self):
        """--market-id on non-Morpho supply must raise UsageError for parity."""
        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "ax",
                "--dry-run",
                "lending-supply",
                "USDC",
                "100",
                "--protocol",
                "aave_v3",
                "--market-id",
                "0xabc",
            ],
        )
        assert result.exit_code != 0
        assert "--market-id is only supported on isolated-market protocols" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_withdraw_loan_token_on_morpho_sets_is_collateral_false(self, mock_get_exec):
        """--loan-token on Morpho Blue must flip is_collateral to False."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "ax",
                "--dry-run",
                "lending-withdraw",
                "USDC",
                "--all",
                "--protocol",
                "morpho_blue",
                "--market-id",
                "0xabc",
                "--loan-token",
            ],
        )
        assert result.exit_code == 0
        assert captured["args"]["is_collateral"] is False
        assert captured["args"]["market_id"] == "0xabc"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_withdraw_morpho_blue_hyphen_alias_accepted(self, mock_get_exec):
        """`--protocol morpho-blue` should behave identically to `morpho_blue`."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            [
                "ax",
                "--dry-run",
                "lending-withdraw",
                "USDC",
                "--all",
                "--protocol",
                "morpho-blue",
                "--market-id",
                "0xabc",
                "--loan-token",
            ],
        )
        assert result.exit_code == 0
        assert captured["args"]["is_collateral"] is False
        assert captured["args"]["market_id"] == "0xabc"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_withdraw_non_morpho_does_not_forward_is_collateral(self, mock_get_exec):
        """For non-Morpho protocols, is_collateral must not be forwarded."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="simulated", data={})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--dry-run", "lending-withdraw", "USDC", "--all", "--protocol", "aave_v3"],
        )
        assert result.exit_code == 0
        assert "is_collateral" not in captured["args"]
        assert "market_id" not in captured["args"]


class TestAxReadCommands:
    """Tests for `ax lp-list`, `ax lending-list`, `ax portfolio` (VIB-2995)."""

    def test_lp_list_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lp-list", "--help"])
        assert result.exit_code == 0
        assert "LP positions" in result.output
        assert "--include-empty" in result.output

    def test_lending_list_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-list", "--help"])
        assert result.exit_code == 0
        assert "lending" in result.output.lower()

    def test_portfolio_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "portfolio", "--help"])
        assert result.exit_code == 0
        assert "--tokens" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lp_list_forwards_args(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["tool_name"] = tool_name
            captured["args"] = args
            return ToolResponse(
                status="success",
                data={"chain": "arbitrum", "protocol": "uniswap_v3", "count": 0, "positions": []},
            )

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "base", "lp-list", "--include-empty"])
        assert result.exit_code == 0
        assert captured["tool_name"] == "list_lp_positions"
        assert captured["args"]["chain"] == "base"
        assert captured["args"]["include_empty"] is True
        assert captured["args"]["protocol"] == "uniswap_v3"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_list_forwards_args(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["tool_name"] = tool_name
            captured["args"] = args
            return ToolResponse(
                status="success",
                data={
                    "chain": "arbitrum",
                    "protocol": "aave_v3",
                    "total_collateral_usd": "1000",
                    "total_debt_usd": "500",
                    "health_factor": "2.0",
                },
            )

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-list"])
        assert result.exit_code == 0
        assert captured["tool_name"] == "list_lending_positions"
        assert captured["args"]["protocol"] == "aave_v3"

    def test_lending_reserves_help(self):
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-reserves", "--help"])
        assert result.exit_code == 0
        assert "borrowable" in result.output.lower()
        assert "--asset" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_reserves_forwards_args(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["tool_name"] = tool_name
            captured["args"] = args
            return ToolResponse(
                status="success",
                data={
                    "schema_version": 1,
                    "chain": "polygon",
                    "protocol": "aave_v3",
                    "count": 0,
                    "reserves": [],
                },
            )

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "polygon", "lending-reserves", "--asset", "WMATIC"])
        assert result.exit_code == 0
        assert captured["tool_name"] == "list_lending_reserves"
        assert captured["args"]["chain"] == "polygon"
        assert captured["args"]["protocol"] == "aave_v3"
        assert captured["args"]["asset"] == "WMATIC"
        # network is forwarded from the group-level --network (default mainnet)
        assert captured["args"]["network"] == "mainnet"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_reserves_sub_level_chain_flag(self, mock_get_exec):
        """-c/--chain after the subcommand works on commands that historically lacked it (_chain_option)."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["tool_name"] = tool_name
            captured["args"] = args
            return ToolResponse(
                status="success",
                data={"schema_version": 1, "chain": "base", "protocol": "aave_v3", "count": 0, "reserves": []},
            )

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "lending-reserves", "-c", "base"])
        assert result.exit_code == 0
        assert captured["args"]["chain"] == "base"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_pool_sub_level_chain_flag(self, mock_get_exec):
        """pool accepts -c after the subcommand and forwards the override to the tool args."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["tool_name"] = tool_name
            captured["args"] = args
            return ToolResponse(status="success", data={"chain": "base"})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "pool", "WETH", "USDC", "-c", "base", "--protocol", "aerodrome"])
        assert result.exit_code == 0
        assert captured["tool_name"] == "get_pool_state"
        assert captured["args"]["chain"] == "base"
        assert captured["args"]["protocol"] == "aerodrome"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_pool_title_renders_swept_fee_tier(self, mock_get_exec):
        """No --fee-tier -> the CLI forwards fee_tier=None (the executor sweeps
        the protocol's native tiers) and the title renders the tier the sweep
        actually selected — not a fabricated 3000 default."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(
                status="success",
                data={"pool_address": "0xabc", "fee_tier": 2500, "fee_tier_source": "sweep"},
            )

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "pool", "WBNB", "USDT", "--protocol", "pancakeswap_v3"])
        assert result.exit_code == 0
        assert captured["args"]["fee_tier"] is None
        # 2500 (Pancake-native tier) rendered as 0.25% in the title.
        assert "0.25%" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_pool_error_without_tier_omits_title_suffix(self, mock_get_exec):
        """An error response with no explicit --fee-tier has no measured tier;
        the title must omit the suffix rather than fabricate one."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        async def mock_execute(tool_name, args):
            return ToolResponse(
                status="error",
                error={"error_code": "empty_pool", "message": "Pool not found", "recoverable": False},
            )

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "pool", "WETH", "USDC"])
        assert result.exit_code == 1
        assert "WETH/USDC (" not in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_reserves_human_table(self, mock_get_exec):
        """Human (non-JSON) output renders a scannable column table, not a flat repr."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        async def mock_execute(tool_name, args):
            return ToolResponse(
                status="success",
                data={
                    "schema_version": 1,
                    "chain": "polygon",
                    "protocol": "aave_v3",
                    "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
                    "count": 3,
                    "total_matched": 3,
                    "truncated": False,
                    "reserves": [
                        {
                            "symbol": "WPOL",
                            "address": "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
                            "borrowing_enabled": False,
                            "usage_as_collateral_enabled": True,
                            "is_active": True,
                            "is_frozen": False,
                            "ltv_bps": 6800,
                            "error": "",
                        },
                        {
                            "symbol": "LINK",
                            "address": "0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39",
                            "borrowing_enabled": False,
                            "usage_as_collateral_enabled": True,
                            "is_active": True,
                            "is_frozen": False,
                            "ltv_bps": 0,  # real "no borrowing power" — must render 0.0%, not —
                            "error": "",
                        },
                        {
                            "symbol": "DAI",
                            "address": "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063",
                            "borrowing_enabled": None,
                            "usage_as_collateral_enabled": None,
                            "is_active": None,
                            "is_frozen": None,
                            "ltv_bps": None,
                            "error": "execution reverted",
                        },
                    ],
                },
            )

        mock_executor.execute = mock_execute
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "polygon", "lending-reserves"])
        assert result.exit_code == 0
        # Column headers + the WPOL row rendered as a table (not a Python list repr).
        assert "SYMBOL" in result.output and "BORROW" in result.output and "LTV" in result.output
        assert "WPOL" in result.output
        assert "68.0%" in result.output  # 6800 bps formatted
        assert "0.0%" in result.output  # ltv_bps == 0 is a real value, not "—" (Empty != Zero)
        assert "[{" not in result.output  # not a flat list-of-dicts repr
        # Failed reserve surfaced, not hidden.
        assert "DAI" in result.output and "execution reverted" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_lending_reserves_table_risk_context(self, mock_get_exec):
        """Risk-context columns: caps (whole tokens; measured 0 renders as
        'uncapped', unmeasured as '—'), eMode category, paused flag — and the
        risk_note detail renders as a trailing annotation line so a
        collateral-enabled / LTV-0 row is not misleading in the human table."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        async def mock_execute(tool_name, args):
            return ToolResponse(
                status="success",
                data={
                    "schema_version": 1,
                    "chain": "polygon",
                    "protocol": "aave_v3",
                    "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
                    "count": 2,
                    "total_matched": 2,
                    "truncated": False,
                    "reserves": [
                        {
                            "symbol": "ezETH",
                            "address": "0x" + "e1" * 20,
                            "borrowing_enabled": False,
                            "usage_as_collateral_enabled": True,
                            "is_active": True,
                            "is_frozen": False,
                            "ltv_bps": 0,
                            "liquidation_threshold_bps": 10,
                            "supply_cap": 450000,
                            "borrow_cap": 0,  # measured 0 = no cap (Aave semantics)
                            "emode_category": 3,
                            "is_paused": False,
                            "detail": {"risk_note": "base LTV zero — collateral counts only inside eMode category 3"},
                            "error": "",
                        },
                        {
                            "symbol": "USDC",
                            "address": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
                            "borrowing_enabled": True,
                            "usage_as_collateral_enabled": True,
                            "is_active": True,
                            "is_frozen": False,
                            "ltv_bps": 7500,
                            "liquidation_threshold_bps": 7800,
                            "supply_cap": None,  # unmeasured — must render "—", never 0
                            "borrow_cap": None,
                            "emode_category": None,
                            "is_paused": None,
                            "error": "",
                        },
                    ],
                },
            )

        mock_executor.execute = mock_execute
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "polygon", "lending-reserves"])
        assert result.exit_code == 0
        assert "SUPPLY-CAP" in result.output and "EMODE" in result.output and "PAUSED" in result.output
        assert "450,000" in result.output  # whole-token supply cap, formatted
        assert "uncapped" in result.output  # measured 0 = no cap, not a bare 0
        assert "—" in result.output  # unmeasured caps/eMode stay em-dash
        # The misleading row carries its risk note as a trailing line.
        assert "ezETH: base LTV zero — collateral counts only inside eMode category 3" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_portfolio_parses_token_list(self, mock_get_exec):
        """--tokens 'USDC,WETH' should split into a list before dispatch."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="success", data={"chain": "arbitrum", "wallet_address": "0x0"})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "portfolio", "--tokens", "USDC,WETH,ARB"])
        assert result.exit_code == 0
        assert captured["args"]["tokens"] == ["USDC", "WETH", "ARB"]

    @patch("almanak.framework.cli.ax._get_executor")
    def test_portfolio_empty_tokens_sends_empty_list(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="success", data={"chain": "arbitrum", "wallet_address": "0x0"})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "portfolio"])
        assert result.exit_code == 0
        assert captured["args"]["tokens"] == []

    @patch("almanak.framework.cli.ax._get_executor")
    def test_portfolio_filters_empty_token_entries(self, mock_get_exec):
        """--tokens "USDC,,WETH," drops empty entries (CodeRabbit PR #1536)."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: dict = {}

        async def mock_execute(tool_name, args):
            captured["args"] = args
            return ToolResponse(status="success", data={"chain": "arbitrum", "wallet_address": "0x0"})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "portfolio", "--tokens", "USDC,,WETH,"])
        assert result.exit_code == 0
        assert captured["args"]["tokens"] == ["USDC", "WETH"]

    @patch("almanak.framework.cli.ax._get_executor")
    def test_group_level_network_cascades_to_read_commands(self, mock_get_exec):
        """`ax --network anvil lp-list` forwards network='anvil' (CodeRabbit Major).

        Previously the subcommand default='mainnet' overrode the group
        value, silently returning mainnet snapshots from an Anvil gateway.
        """
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: list = []

        async def mock_execute(tool_name, args):
            captured.append({"tool": tool_name, "network": args.get("network")})
            return ToolResponse(status="success", data={"chain": "arbitrum", "count": 0, "positions": []})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        for cmd in (["lp-list"], ["lending-list"], ["portfolio"]):
            captured.clear()
            result = runner.invoke(almanak, ["ax", "--network", "anvil", *cmd])
            assert result.exit_code == 0, result.output
            assert captured[0]["network"] == "anvil", f"{cmd} dropped group-level network=anvil"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_subcommand_network_overrides_group_level(self, mock_get_exec):
        """Explicit subcommand --network takes precedence over group-level."""
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        captured: list = []

        async def mock_execute(tool_name, args):
            captured.append(args.get("network"))
            return ToolResponse(status="success", data={"chain": "arbitrum", "count": 0, "positions": []})

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--network", "anvil", "lp-list", "--network", "mainnet"])
        assert result.exit_code == 0
        assert captured[0] == "mainnet"


class TestAxBundleCommands:
    """Tests for ``ax bundle-list`` and ``ax bundle-clear`` admin commands."""

    def test_bundle_list_empty(self, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "bundle-list"])
        assert result.exit_code == 0
        assert "No cached bundles" in result.output

    def test_bundle_list_renders_entries(self, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        from almanak.framework.agent_tools.bundle_cache import BundleCache

        cache = BundleCache()
        cache.put("deadbeef-0000-4000-8000-000000000001", "arbitrum", b"x", {"intent_type": "swap"})

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "bundle-list"])
        assert result.exit_code == 0
        assert "deadbeef-0000-4000-8000-000000000001" in result.output
        assert "arbitrum" in result.output
        assert "swap" in result.output
        assert "live" in result.output

    def test_bundle_list_json_output(self, tmp_path, monkeypatch):
        import json as json_mod

        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        from almanak.framework.agent_tools.bundle_cache import BundleCache

        cache = BundleCache()
        cache.put("bundle-json-1", "base", b"y", {"intent_type": "lp_open"})

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--json", "bundle-list"])
        assert result.exit_code == 0
        payload = json_mod.loads(result.output)
        assert len(payload["entries"]) == 1
        entry = payload["entries"][0]
        assert entry["bundle_id"] == "bundle-json-1"
        assert entry["chain"] == "base"
        assert entry["intent_type"] == "lp_open"
        assert entry["expired"] is False

    def test_bundle_clear_all_requires_confirm(self, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        from almanak.framework.agent_tools.bundle_cache import BundleCache

        cache = BundleCache()
        cache.put("bundle-clear-1", "arbitrum", b"x", {})

        runner = CliRunner()
        # Simulate user saying "no" to the prompt.
        result = runner.invoke(almanak, ["ax", "bundle-clear"], input="n\n")
        assert result.exit_code == 0
        assert "Cancelled" in result.output
        # Bundle still present.
        assert BundleCache().get("bundle-clear-1") is not None

    def test_bundle_clear_all_with_yes_flag(self, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        from almanak.framework.agent_tools.bundle_cache import BundleCache

        cache = BundleCache()
        cache.put("bundle-yes-1", "arbitrum", b"x", {})
        cache.put("bundle-yes-2", "base", b"y", {})

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "bundle-clear", "--yes"])
        assert result.exit_code == 0
        assert "Removed" in result.output
        # Both gone.
        assert BundleCache().get("bundle-yes-1") is None
        assert BundleCache().get("bundle-yes-2") is None

    def test_bundle_clear_expired_only(self, tmp_path, monkeypatch):
        import time as _time

        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        from almanak.framework.agent_tools.bundle_cache import BundleCache

        # Two caches with different TTLs so the "fresh" control entry cannot
        # inherit the 1s clock and become flaky on a slow CI runner.
        # (CodeRabbit round 2.)
        short_ttl_cache = BundleCache(default_ttl_seconds=1)
        short_ttl_cache.put("bundle-expired", "arbitrum", b"x", {})
        _time.sleep(1.2)
        BundleCache(default_ttl_seconds=900).put("bundle-fresh", "base", b"y", {})

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "bundle-clear", "--expired", "--yes"])
        assert result.exit_code == 0
        # Fresh bundle survives.
        fresh = BundleCache()
        assert fresh.get("bundle-fresh") is not None
        # Expired bundle gone (must not raise expired error).
        assert fresh.get("bundle-expired") is None


class TestUsesIsolatedMarkets:
    """Capability-derived --market-id gate (VIB-4851 B3).

    ``_uses_isolated_markets`` derives from the connector-owned
    ``requires_market_id`` capability instead of a hardcoded protocol set.
    Tested against the REAL capabilities registry (pure data modules) so the
    truth table pins what connectors actually declare — mocking the registry
    would only test the mock.
    """

    def test_isolated_market_protocols_accept_format_variants(self):
        from almanak.framework.cli.ax import _uses_isolated_markets

        for spelling in (
            "morpho_blue",
            "morpho-blue",
            "morpho blue",
            "Morpho_Blue",
            "morpho",  # legacy alias key the morpho_blue capabilities module declares
            "curvance",
        ):
            assert _uses_isolated_markets(spelling), spelling

    def test_unified_pool_protocols_rejected(self):
        from almanak.framework.cli.ax import _uses_isolated_markets

        # Unified-pool lenders must NOT accept --market-id routing — including
        # the market-scoped read-seam protocols (Compound/Silo/Euler/BENQI),
        # whose CLI market selection deliberately does not use --market-id.
        for protocol in ("aave_v3", "compound_v3", "spark", "silo_v2", "euler_v2", "benqi"):
            assert not _uses_isolated_markets(protocol), protocol

    def test_unknown_protocol_fails_closed(self):
        from almanak.framework.cli.ax import _uses_isolated_markets

        assert not _uses_isolated_markets("definitely_not_a_protocol")
        assert not _uses_isolated_markets("")


class TestAxJsonStreamRouting:
    """Under ``--json`` the payload — success OR structured error — must land
    on STDOUT so scripted callers can pipe it; stderr carries only human/log
    noise. Regression guard: ``ax --json`` error documents were emitted to
    stderr, leaving stdout empty on failure.

    ``CliRunner(mix_stderr=False)`` separates the two streams so each
    assertion pins the exact stream, not just the merged output.
    """

    @patch("almanak.framework.cli.ax._get_executor")
    def test_json_success_payload_on_stdout_only(self, mock_get_exec):
        import json

        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="success", data={"token": "ETH", "price_usd": 2500.0})

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(almanak, ["ax", "--json", "price", "ETH"])
        assert result.exit_code == 0
        payload = json.loads(result.stdout)  # stdout is exactly one JSON document
        assert payload["status"] == "success"
        assert payload["data"]["price_usd"] == 2500.0
        assert "price_usd" not in result.stderr

    def test_json_error_payload_on_stdout_not_stderr(self):
        """``ax run <unknown tool>`` reaches the shared render_error seam
        offline (catalog lookup, no gateway) — the structured error document
        must be parseable from stdout with a non-zero exit code."""
        import json

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(almanak, ["ax", "--json", "run", "definitely_not_a_tool", "{}"])
        assert result.exit_code == 1
        payload = json.loads(result.stdout)
        assert payload["status"] == "error"
        assert "definitely_not_a_tool" in payload["message"]
        assert '"status"' not in result.stderr

    @patch("almanak.framework.cli.ax._get_executor")
    def test_json_exception_error_on_stdout(self, mock_get_exec):
        """A runtime failure inside a subcommand (executor raises) routes
        through the same render_error seam — JSON error on stdout, exit 1."""
        import json

        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        async def mock_execute(tool_name, args):
            raise RuntimeError("gateway exploded")

        mock_executor.execute = mock_execute

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(almanak, ["ax", "--json", "price", "ETH"])
        assert result.exit_code == 1
        payload = json.loads(result.stdout)
        assert payload == {"status": "error", "message": "gateway exploded"}
        assert "gateway exploded" not in result.stderr


class TestAxBridge:
    _BASE_ARGS = ["bridge", "USDC", "100", "--from-chain", "arbitrum", "--to-chain", "base"]

    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_dry_run(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="simulated",
            data={"estimated_output": "99.8 USDC", "bridge": "across"},
        )
        captured_args = {}

        async def mock_execute(tool_name, args):
            assert tool_name == "bridge_tokens"
            captured_args.update(args)
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", *self._BASE_ARGS])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert captured_args["token"] == "USDC"
        assert captured_args["amount"] == "100"
        assert captured_args["from_chain"] == "arbitrum"
        assert captured_args["to_chain"] == "base"
        assert captured_args["slippage_bps"] == 50
        assert captured_args["dry_run"] is True
        assert "preferred_bridge" not in captured_args

    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_dry_run_error_exits_nonzero(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="error", error={"message": "no route"})

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", *self._BASE_ARGS])
        assert result.exit_code == 1

    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_preferred_bridge_and_slippage_options(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="simulated", data={})
        captured_args = {}

        async def mock_execute(tool_name, args):
            captured_args.update(args)
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--dry-run", *self._BASE_ARGS, "--bridge", "across", "--slippage", "100"],
        )
        assert result.exit_code == 0
        assert captured_args["preferred_bridge"] == "across"
        assert captured_args["slippage_bps"] == 100

    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_with_yes_executes(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(
            status="success",
            data={"tx_hash": "0xbr1d6e", "amount_out": "99.8"},
        )
        captured_args = {}

        async def mock_execute(tool_name, args):
            assert tool_name == "bridge_tokens"
            captured_args.update(args)
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--yes", *self._BASE_ARGS])
        assert result.exit_code == 0
        assert "0xbr1d6e" in result.output
        # Execution path must not silently flip into simulation.
        assert "dry_run" not in captured_args

    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_execute_error_exits_nonzero(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="error", error={"message": "insufficient balance"})

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--yes", *self._BASE_ARGS])
        assert result.exit_code == 1

    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_non_interactive_without_yes_requires_flag(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)
        executed = []

        async def mock_execute(tool_name, args):
            executed.append(tool_name)

        mock_executor.execute = mock_execute

        runner = CliRunner()
        # CliRunner streams are not TTYs -> the safety gate raises a
        # ClickException instead of prompting.
        result = runner.invoke(almanak, ["ax", *self._BASE_ARGS])
        assert result.exit_code != 0
        assert "requires --yes" in result.output
        assert executed == []

    @patch("almanak.framework.cli.ax_render.is_interactive", return_value=True)
    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_interactive_confirm(self, mock_get_exec, _mock_tty):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="success", data={"tx_hash": "0xok"})

        async def mock_execute(tool_name, args):
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", *self._BASE_ARGS], input="y\n")
        assert result.exit_code == 0
        assert "0xok" in result.output

    @patch("almanak.framework.cli.ax_render.is_interactive", return_value=True)
    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_interactive_cancel(self, mock_get_exec, _mock_tty):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)
        executed = []

        async def mock_execute(tool_name, args):
            executed.append(tool_name)

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", *self._BASE_ARGS], input="n\n")
        assert result.exit_code == 0
        assert "Cancelled" in result.output
        assert executed == []

    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_unexpected_error_renders_and_exits(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        async def mock_execute(tool_name, args):
            raise RuntimeError("bridge adapter offline")

        mock_executor.execute = mock_execute

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--dry-run", *self._BASE_ARGS])
        assert result.exit_code == 1
        assert "bridge adapter offline" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_bridge_sub_flags_merge_with_group_flags(self, mock_get_exec):
        mock_executor, mock_client = _mock_executor_and_client()
        mock_get_exec.return_value = (mock_executor, mock_client)

        response = ToolResponse(status="simulated", data={})
        captured_args = {}

        async def mock_execute(tool_name, args):
            captured_args.update(args)
            return response

        mock_executor.execute = mock_execute

        runner = CliRunner()
        # --dry-run given at the subcommand level rather than the group level.
        result = runner.invoke(almanak, ["ax", *self._BASE_ARGS, "--dry-run"])
        assert result.exit_code == 0
        assert captured_args["dry_run"] is True
