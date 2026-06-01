"""Unit: ``repair-teardown-lp-close`` is registered under the ``strat`` group.

Guards the wiring in ``almanak/cli/cli.py`` (``strat.add_command(...,
name="repair-teardown-lp-close")``). The e2e suite exercises the command's
behaviour through the registered path; this fast unit test pins the
registration itself so it can't silently regress.
"""

from __future__ import annotations

from almanak.cli.cli import strat
from almanak.framework.cli.repair_teardown_lp_close import repair_teardown_lp_close_cmd


def test_repair_teardown_lp_close_registered_under_strat():
    assert "repair-teardown-lp-close" in strat.commands
    assert strat.commands["repair-teardown-lp-close"] is repair_teardown_lp_close_cmd


def test_repair_teardown_lp_close_exposes_expected_options():
    cmd = strat.commands["repair-teardown-lp-close"]
    flags = {opt for param in cmd.params for opt in getattr(param, "opts", [])}
    assert {"--db", "--deployment-id", "-s", "--dry-run", "--prices-source"} <= flags
