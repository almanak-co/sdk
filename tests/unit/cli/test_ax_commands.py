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
        result = runner.invoke(
            almanak, ["ax", "--dry-run", "swap", "USDC", "ETH", "100", "--protocol", "uniswap_v3"]
        )
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
        result = runner.invoke(
            almanak, ["ax", "--dry-run", "--chain", "avalanche", "swap", "USDC", "AVAX", "10"]
        )
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
        result = runner.invoke(
            almanak, ["ax", "--dry-run", "--chain", "bsc", "swap", "USDC", "BNB", "10"]
        )
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
        assert "--market-id is only supported on Morpho Blue" in result.output

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
        assert "--market-id is only supported on Morpho Blue" in result.output

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
