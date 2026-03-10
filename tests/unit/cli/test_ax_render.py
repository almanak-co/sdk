"""Tests for the ax CLI output renderer and TTY safety matrix."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from almanak.framework.agent_tools.schemas import ToolResponse
from almanak.framework.cli.ax_render import (
    check_safety_gate,
    is_interactive,
    render_error,
    render_result,
    render_simulation,
)


class TestIsInteractive:
    def test_returns_bool(self):
        result = is_interactive()
        assert isinstance(result, bool)


class TestCheckSafetyGate:
    def test_dry_run_always_returns_false(self):
        assert check_safety_gate(dry_run=True, yes=False, action_description="test") is False
        assert check_safety_gate(dry_run=True, yes=True, action_description="test") is False

    def test_non_interactive_without_yes_raises(self):
        with patch("almanak.framework.cli.ax_render.is_interactive", return_value=False):
            from click import ClickException

            with pytest.raises(ClickException, match="--yes"):
                check_safety_gate(dry_run=False, yes=False, action_description="test")

    def test_non_interactive_with_yes_returns_true(self):
        with patch("almanak.framework.cli.ax_render.is_interactive", return_value=False):
            assert check_safety_gate(dry_run=False, yes=True, action_description="test") is True

    def test_interactive_with_yes_skips_prompt(self):
        with patch("almanak.framework.cli.ax_render.is_interactive", return_value=True):
            assert check_safety_gate(dry_run=False, yes=True, action_description="test") is True

    def test_interactive_confirm_yes(self):
        with (
            patch("almanak.framework.cli.ax_render.is_interactive", return_value=True),
            patch("click.confirm", return_value=True),
        ):
            assert check_safety_gate(dry_run=False, yes=False, action_description="test") is True

    def test_interactive_confirm_no(self):
        with (
            patch("almanak.framework.cli.ax_render.is_interactive", return_value=True),
            patch("click.confirm", return_value=False),
        ):
            assert check_safety_gate(dry_run=False, yes=False, action_description="test") is False


class TestRenderResult:
    def test_json_output(self, capsys):
        response = ToolResponse(status="success", data={"balance": "100.5"})
        render_result(response, json_output=True)
        output = capsys.readouterr().out
        assert '"status": "success"' in output
        assert '"balance": "100.5"' in output

    def test_human_output_success(self, capsys):
        response = ToolResponse(status="success", data={"price": "2500.00"})
        render_result(response, json_output=False, title="Price")
        output = capsys.readouterr().out
        assert "Price:" in output
        assert "2500.00" in output

    def test_human_output_error(self, capsys):
        response = ToolResponse(
            status="error",
            error={"message": "Insufficient balance", "recoverable": True},
        )
        render_result(response, json_output=False)
        output = capsys.readouterr().out
        assert "Insufficient balance" in output


class TestRenderError:
    def test_json_error(self, capsys):
        render_error("Something failed", json_output=True)
        output = capsys.readouterr().err
        assert '"status": "error"' in output
        assert "Something failed" in output

    def test_human_error(self, capsys):
        render_error("Something failed", json_output=False)
        output = capsys.readouterr().err
        assert "Something failed" in output


class TestRenderSimulation:
    def test_json_simulation(self, capsys):
        response = ToolResponse(status="simulated", data={"estimated_output": "0.04 ETH"})
        render_simulation(response, json_output=True)
        output = capsys.readouterr().out
        assert '"status": "simulated"' in output

    def test_human_simulation(self, capsys):
        response = ToolResponse(status="simulated", data={"estimated_output": "0.04 ETH"})
        render_simulation(response, json_output=False)
        output = capsys.readouterr().out
        assert "DRY RUN" in output
        assert "Simulation" in output
