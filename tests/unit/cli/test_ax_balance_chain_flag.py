"""Tests for ``almanak ax balance --chain`` flag handling (VIB-3142).

Before VIB-3142, ``ax balance ETH --chain base`` was rejected by Click
because only the ``ax`` group declared ``--chain``. The balance help
text advertised the subcommand-level placement, so the rejection was a
UX bug. This test pins:

1. ``ax balance ETH --chain base`` is accepted (regression test).
2. ``ax --chain base balance ETH`` (group placement) still works.
3. When both are provided, the subcommand value wins (more specific).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from almanak.cli.cli import almanak


def _extract_chain(mock_run_tool: MagicMock) -> str:
    """Pull the ``chain`` value out of the mocked _run_tool invocation.

    Also asserts that ``ctx.obj["chain"]`` was updated to match the tool arg --
    this verifies the override propagates to downstream infrastructure
    (executor, gateway client, managed gateway) rather than just the tool args.
    """
    mock_run_tool.assert_called_once()
    call_args = mock_run_tool.call_args
    # _run_tool signature: (ctx, tool_name, arguments). `arguments` is the
    # dict we build inside balance(). Support both positional + kwarg calls.
    ctx = call_args[0][0]
    tool_args = call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("arguments", {})
    assert ctx.obj["chain"] == tool_args["chain"], (
        f"ctx.obj['chain']={ctx.obj['chain']} does not match tool arg chain={tool_args['chain']}; "
        "the override did not propagate to the context (executor/gateway would use the wrong chain)."
    )
    return tool_args["chain"]


class TestAxBalanceChainFlag:
    """Verify --chain works on the ax balance subcommand in all placements."""

    @patch("almanak.framework.cli.ax._run_tool")
    def test_subcommand_chain_flag_is_accepted(self, mock_run_tool: MagicMock):
        """Regression: ``ax balance ETH --chain base`` must not be rejected."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "balance", "ETH", "--chain", "base"])

        assert result.exit_code == 0, f"Click rejected the subcommand --chain: {result.output}"
        assert _extract_chain(mock_run_tool) == "base"

    @patch("almanak.framework.cli.ax._run_tool")
    def test_group_chain_flag_still_works(self, mock_run_tool: MagicMock):
        """Backcompat: the documented workaround ``ax --chain base balance ETH`` still works."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "base", "balance", "ETH"])

        assert result.exit_code == 0, result.output
        assert _extract_chain(mock_run_tool) == "base"

    @patch("almanak.framework.cli.ax._run_tool")
    def test_subcommand_chain_wins_over_group(self, mock_run_tool: MagicMock):
        """When both are set, the more-specific (subcommand) value wins."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "--chain", "base", "balance", "ETH", "--chain", "arbitrum"],
        )

        assert result.exit_code == 0, result.output
        assert _extract_chain(mock_run_tool) == "arbitrum"

    @patch("almanak.framework.cli.ax._run_tool")
    def test_short_flag_c_also_works(self, mock_run_tool: MagicMock):
        """``-c`` short flag works at the subcommand level (matches group)."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "balance", "ETH", "-c", "base"])

        assert result.exit_code == 0, result.output
        assert _extract_chain(mock_run_tool) == "base"

    @patch("almanak.framework.cli.ax._run_tool")
    def test_no_chain_flag_uses_default(self, mock_run_tool: MagicMock):
        """Without any --chain, the group default (arbitrum) is used."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        # Clear ALMANAK_CHAIN so the fallback is the hard-coded default.
        result = runner.invoke(
            almanak,
            ["ax", "balance", "ETH"],
            env={"ALMANAK_CHAIN": ""},
        )

        assert result.exit_code == 0, result.output
        assert _extract_chain(mock_run_tool) == "arbitrum"
