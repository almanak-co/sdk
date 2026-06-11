"""Behavioral guards for the plan-007 optional-extras split.

Two contracts:

1. A default install (no extras) can import the CLI bootstrap and the pnl
   backtesting package. Simulated by blocking the moved packages via
   ``sys.modules[name] = None`` (which makes ``import name`` raise
   ImportError) in a fresh subprocess.
2. Reaching an optuna-backed symbol without optuna installed raises an
   ImportError that names the ``almanak[backtest]`` extra.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

_BLOCKED = ("streamlit", "plotly", "matplotlib", "optuna")


def _run_blocked(script_body: str) -> subprocess.CompletedProcess[str]:
    script = textwrap.dedent(
        f"""
        import sys
        for _name in {_BLOCKED!r}:
            sys.modules[_name] = None
        """
    ) + textwrap.dedent(script_body)
    env = os.environ.copy()
    env["ALMANAK_STRATEGIES_DIR"] = "/nonexistent_strategies_dir_for_lean_import_test"
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )


def test_cli_bootstrap_works_without_optional_extras() -> None:
    result = _run_blocked(
        """
        from almanak.cli.cli import almanak  # noqa: F401
        import almanak.framework.backtesting.pnl  # noqa: F401
        import almanak.framework.backtesting  # noqa: F401
        print("OK")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_actionable_optuna_import_error_returns_actionable_for_optuna_names() -> None:
    """ImportError with name='optuna' or 'optuna.*' returns the actionable error."""
    from almanak.framework.backtesting.pnl.optuna_tuner import _actionable_optuna_import_error

    for name in ("optuna", "optuna.samplers"):
        exc = ImportError("no module named optuna")
        exc.name = name  # type: ignore[attr-defined]
        result = _actionable_optuna_import_error(exc)
        assert "almanak[backtest]" in str(result), f"Expected actionable message for name={name!r}"


def test_actionable_optuna_import_error_reraises_for_non_optuna() -> None:
    """ImportError with name='scipy' or name=None propagates the original exception."""
    import pytest
    from almanak.framework.backtesting.pnl.optuna_tuner import _actionable_optuna_import_error

    for name in ("scipy", None):
        original = ImportError("no module named scipy")
        original.name = name  # type: ignore[attr-defined]
        with pytest.raises(ImportError) as exc_info:
            _actionable_optuna_import_error(original)
        assert exc_info.value is original, f"Expected original exception to propagate for name={name!r}"


def test_optuna_symbol_raises_actionable_error_without_extra() -> None:
    result = _run_blocked(
        """
        import almanak.framework.backtesting.pnl as pnl
        try:
            pnl.OptunaTuner
        except ImportError as e:
            assert "almanak[backtest]" in str(e), str(e)
            print("ACTIONABLE")
        else:
            raise SystemExit("expected ImportError")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "ACTIONABLE" in result.stdout
