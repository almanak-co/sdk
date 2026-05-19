"""Unit test for the ``--no-gateway`` flag on ``almanak strat test``.

The flag mirrors the equivalent option on ``strat run``: when set, the SDK
should pass ``no_gateway=True`` through to ``framework_run_cmd`` so the run
path attaches to an existing gateway (sidecar) instead of auto-starting a
managed one.

We don't drive a real gateway here — that's an integration concern. The
guarantee this test enforces is just the wiring: ``--no-gateway`` flips
``no_gateway`` in the kwargs ``strategy_test`` forwards, and its absence
leaves it ``False``.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from click.testing import CliRunner

from almanak.cli.cli import strat


def _scaffold_minimal_workspace(tmp_path: Path) -> Path:
    """Create the smallest workspace ``strat test`` will accept past arg-validation."""
    (tmp_path / "config.json").write_text(json.dumps({"chain": "arbitrum"}))
    (tmp_path / "strategy.py").write_text("# placeholder\n")
    return tmp_path


def _capture_framework_run_kwargs(monkeypatch) -> dict:
    """Replace ``framework_run_cmd`` with a no-op that records its kwargs.

    Returns a dict the caller can read after invocation. Patches enough of the
    pre-invoke setup (private-key prime, redaction, skip-reason scan) so the
    command reaches the ``ctx.invoke(framework_run_cmd, ...)`` line.
    """
    captured: dict = {}

    def fake_framework_run(**kwargs):
        captured.update(kwargs)

    # Patch framework_run_cmd to a plain function. Click's ``ctx.invoke()``
    # dispatches via ``isinstance(callback, Command)`` — a plain callable falls
    # through to direct invocation with the forwarded kwargs, which is exactly
    # the call shape we want to inspect.
    import almanak.cli.cli as cli_mod

    monkeypatch.setattr(cli_mod, "framework_run_cmd", fake_framework_run)
    monkeypatch.setattr(cli_mod, "install_redaction", lambda: None)
    monkeypatch.setattr(cli_mod, "_strat_test_skip_reason", lambda *_: None)

    # _prime_strategy_command_config reads ctx.obj and resolves the gateway/runtime
    # config; return a minimal stub with a non-empty private_key so strategy_test
    # doesn't try to set the ANVIL_DEFAULT_PRIVATE_KEY contextvar (which has its
    # own indirect ladders we don't want to exercise in this wiring test).
    fake_boot = MagicMock()
    fake_boot.gateway.private_key = "0x" + "1" * 64
    monkeypatch.setattr(cli_mod, "_prime_strategy_command_config", lambda _: fake_boot)

    return captured


def test_no_gateway_flag_sets_kwarg_true(monkeypatch, tmp_path):
    """``strat test --no-gateway --teardown`` forwards ``no_gateway=True``."""
    workspace = _scaffold_minimal_workspace(tmp_path)
    captured = _capture_framework_run_kwargs(monkeypatch)

    runner = CliRunner()
    result = runner.invoke(strat, ["test", "--no-gateway", "--teardown", "-d", str(workspace)])

    assert result.exit_code == 0, result.output
    assert captured.get("no_gateway") is True


def test_no_gateway_default_is_false(monkeypatch, tmp_path):
    """Without ``--no-gateway``, ``strategy_test`` keeps ``no_gateway=False``."""
    workspace = _scaffold_minimal_workspace(tmp_path)
    captured = _capture_framework_run_kwargs(monkeypatch)

    runner = CliRunner()
    result = runner.invoke(strat, ["test", "--teardown", "-d", str(workspace)])

    assert result.exit_code == 0, result.output
    assert captured.get("no_gateway") is False


def test_no_gateway_appears_in_help_output():
    """The flag is discoverable via ``--help``.

    Catches accidental removal of the click decorator separately from the
    plumbing tests above, which run with the framework_run_cmd reference
    monkeypatched out.
    """
    runner = CliRunner()
    result = runner.invoke(strat, ["test", "--help"])

    assert result.exit_code == 0
    assert "--no-gateway" in result.output
    assert "existing gateway" in result.output
