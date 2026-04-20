"""Tests for ``almanak ax price --chain`` flag handling (VIB-3182).

Follow-on to VIB-3142 / PR #1583, which fixed the same bug on
``ax balance``. Before VIB-3182, ``ax price USDC --chain base`` was
rejected by Click because only the ``ax`` group declared ``--chain``.
The price help text advertised the subcommand-level placement, so the
rejection was a UX bug. This test pins:

1. ``ax price USDC --chain base`` is accepted (regression test).
2. ``ax --chain base price USDC`` (group placement) still works.
3. When both are provided, the subcommand value wins (more specific).
4. The override propagates to ``ctx.obj["chain"]`` so downstream
   infrastructure (executor, gateway client, managed gateway) sees
   the resolved chain (Gemini's #1583 fix).
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
    ctx = call_args[0][0]
    tool_args = call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("arguments", {})
    assert ctx.obj["chain"] == tool_args["chain"], (
        f"ctx.obj['chain']={ctx.obj['chain']} does not match tool arg chain={tool_args['chain']}; "
        "the override did not propagate to the context (executor/gateway would use the wrong chain)."
    )
    return tool_args["chain"]


class TestAxPriceChainFlag:
    """Verify --chain works on the ax price subcommand in all placements."""

    @patch("almanak.framework.cli.ax._run_tool")
    def test_subcommand_chain_flag_is_accepted(self, mock_run_tool: MagicMock):
        """Regression: ``ax price USDC --chain base`` must not be rejected."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "price", "USDC", "--chain", "base"])

        assert result.exit_code == 0, f"Click rejected the subcommand --chain: {result.output}"
        assert _extract_chain(mock_run_tool) == "base"

    @patch("almanak.framework.cli.ax._run_tool")
    def test_group_chain_flag_still_works(self, mock_run_tool: MagicMock):
        """Backcompat: ``ax --chain base price USDC`` still works."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        result = runner.invoke(almanak, ["ax", "--chain", "base", "price", "USDC"])

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
            ["ax", "--chain", "base", "price", "USDC", "--chain", "arbitrum"],
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
        result = runner.invoke(almanak, ["ax", "price", "USDC", "-c", "base"])

        assert result.exit_code == 0, result.output
        assert _extract_chain(mock_run_tool) == "base"

    @patch("almanak.framework.cli.ax._run_tool")
    def test_no_chain_flag_uses_default(self, mock_run_tool: MagicMock):
        """Without any --chain, the group default (arbitrum) is used."""
        mock_response = MagicMock()
        mock_response.status = "success"
        mock_run_tool.return_value = mock_response

        runner = CliRunner()
        result = runner.invoke(
            almanak,
            ["ax", "price", "USDC"],
            env={"ALMANAK_CHAIN": ""},
        )

        assert result.exit_code == 0, result.output
        assert _extract_chain(mock_run_tool) == "arbitrum"
