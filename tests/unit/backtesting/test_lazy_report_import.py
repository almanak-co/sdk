"""report_generator must stay lazy in the backtesting package (VIB-5620).

jinja2 ships in the ``backtest`` extra, not the base install. An eager
``from .report_generator import ...`` in ``backtesting/__init__.py`` made
every ``almanak strat backtest`` subcommand crash at import time on a base
install. These tests pin the lazy re-export in-process and prove the CLI
sweep module imports without pulling report_generator (subprocess, so the
dev environment's own imports cannot mask a regression).
"""

from __future__ import annotations

import subprocess
import sys

import pytest


def test_backtesting_import_does_not_load_report_generator() -> None:
    code = (
        "import sys\n"
        "import almanak.framework.cli.backtest.sweep\n"
        "import almanak.framework.backtesting\n"
        "mod = 'almanak.framework.backtesting.report_generator'\n"
        "assert mod not in sys.modules, f'{mod} imported eagerly (VIB-5620)'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


def test_report_symbols_resolve_lazily() -> None:
    import almanak.framework.backtesting as backtesting

    # jinja2 is present in the dev environment, so the lazy path must
    # resolve the real symbols on attribute access.
    assert callable(backtesting.generate_report)
    assert callable(backtesting.generate_report_from_json)
    assert backtesting.ReportResult is not None


def test_unknown_attribute_still_raises() -> None:
    import almanak.framework.backtesting as backtesting

    with pytest.raises(AttributeError, match="no_such_symbol"):
        _ = backtesting.no_such_symbol
