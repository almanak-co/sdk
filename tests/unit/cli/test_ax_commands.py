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
