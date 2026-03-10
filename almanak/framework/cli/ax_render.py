"""Output renderer and TTY safety matrix for ``almanak ax`` commands.

TTY Safety Matrix:
  - Interactive terminal (TTY): simulate -> preview -> confirm -> execute
  - Non-interactive + --yes: simulate -> execute (no confirmation)
  - Non-interactive without --yes: fail with error message
  - --dry-run: simulate only, never submit (in any mode)
"""

from __future__ import annotations

import json
import re
import sys

import click

from almanak.framework.agent_tools.schemas import ToolResponse

# Strip ANSI escape sequences and control characters from untrusted strings
# to prevent terminal manipulation via malicious LLM or gateway responses.
_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]|\x1b\].*?\x07|\x1b[^[\]].?")


def _sanitize(value: str) -> str:
    """Remove ANSI escape sequences and non-printable control characters."""
    cleaned = _ANSI_ESCAPE_RE.sub("", str(value))
    return "".join(c for c in cleaned if c == "\n" or c == "\t" or (c >= " " and ord(c) < 0x7F) or ord(c) >= 0xA0)


def is_interactive() -> bool:
    """Check if stdout is connected to a TTY (interactive terminal)."""
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def check_safety_gate(*, dry_run: bool, yes: bool, action_description: str) -> bool:
    """Enforce the TTY safety matrix before executing a write action.

    Returns True if execution should proceed, False if it should be skipped.
    Raises click.ClickException if non-interactive without --yes.

    Args:
        dry_run: If True, always returns False (simulate only).
        yes: If True, skip confirmation prompt.
        action_description: Human-readable description of the action (e.g. "Swap 100 USDC -> ETH").
    """
    if dry_run:
        return False

    if not is_interactive() and not yes:
        raise click.ClickException(
            "Non-interactive mode requires --yes to execute transactions.\n"
            "Add --yes to confirm, or use --dry-run to simulate only."
        )

    if yes:
        return True

    # Interactive TTY: prompt for confirmation
    click.echo()
    click.echo(click.style("Action: ", bold=True) + action_description)
    return click.confirm("Execute this transaction?", default=False)


def render_result(response: ToolResponse, *, json_output: bool = False, title: str = "Result") -> None:
    """Render a ToolResponse to stdout.

    Args:
        response: The tool execution result.
        json_output: If True, output raw JSON. Otherwise, human-readable table.
        title: Title for the human-readable output.
    """
    if json_output:
        _render_json(response)
    else:
        _render_human(response, title=title)


def render_error(message: str, *, json_output: bool = False) -> None:
    """Render an error message to stderr.

    Args:
        message: Error message.
        json_output: If True, output as JSON object.
    """
    if json_output:
        click.echo(json.dumps({"status": "error", "message": message}), err=True)
    else:
        click.echo(click.style(f"Error: {message}", fg="red"), err=True)


def render_simulation(response: ToolResponse, *, json_output: bool = False) -> None:
    """Render a simulation/dry-run result.

    Args:
        response: The simulation result.
        json_output: If True, output raw JSON.
    """
    if json_output:
        _render_json(response)
    else:
        click.echo(click.style("[DRY RUN] ", fg="yellow", bold=True) + "Simulation result:")
        _render_human(response, title="Simulation")


def render_interpretation(
    tool_name: str,
    arguments: dict,
    *,
    json_output: bool = False,
) -> None:
    """Show what the LLM interpreted from natural language input.

    Always displayed (even with --yes) so the user sees what the LLM understood.
    """
    if json_output:
        click.echo(json.dumps({"interpreted": {"tool": tool_name, "arguments": arguments}}, indent=2, default=str))
        return

    click.echo()
    click.echo(click.style("Interpreted as:", bold=True))

    # Show tool name (sanitize LLM output)
    click.echo(f"  Action:   {_sanitize(tool_name)}")

    # Show key arguments with readable formatting (sanitize LLM output)
    for key, value in arguments.items():
        label = _sanitize(key.replace("_", " ").title())
        click.echo(f"  {label + ':':<12} {_sanitize(str(value))}")

    click.echo()


def _render_json(response: ToolResponse) -> None:
    """Output ToolResponse as pretty-printed JSON."""
    output: dict[str, object] = {"status": response.status}
    if response.data:
        output["data"] = response.data
    if response.error:
        output["error"] = response.error
    if response.explanation:
        output["explanation"] = response.explanation
    click.echo(json.dumps(output, indent=2, default=str))


def _render_human(response: ToolResponse, *, title: str = "Result") -> None:
    """Output ToolResponse as a human-readable table."""
    # Status line
    status = response.status
    if status == "success":
        status_str = click.style("SUCCESS", fg="green", bold=True)
    elif status == "simulated":
        status_str = click.style("SIMULATED", fg="yellow", bold=True)
    elif status == "error":
        status_str = click.style("ERROR", fg="red", bold=True)
    else:
        status_str = click.style(status.upper(), bold=True)

    click.echo(f"\n{title}: {status_str}")
    click.echo("-" * 40)

    # Data fields
    if response.data:
        max_key_len = max((len(str(k)) for k in response.data), default=0)
        for key, value in response.data.items():
            click.echo(f"  {_sanitize(str(key)):<{max_key_len + 2}} {_sanitize(str(value))}")

    # Error details
    if response.error:
        click.echo()
        if isinstance(response.error, dict):
            msg = response.error.get("message", str(response.error))
            recoverable = response.error.get("recoverable")
        else:
            msg = str(response.error)
            recoverable = False
        click.echo(click.style(f"  Error: {_sanitize(msg)}", fg="red"))
        if recoverable:
            click.echo(click.style("  (recoverable)", fg="yellow"))

    # Explanation
    if response.explanation:
        click.echo()
        click.echo(f"  {_sanitize(response.explanation)}")

    click.echo()
