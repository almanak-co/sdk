"""E2E: ``almanak strat repair-teardown-lp-close`` against the frozen fixture.

RED -> repair -> GREEN, plus negative control + dry-run + idempotence, driven
through the real click command (``CliRunner``) and the committed fixture DB
(``tests/fixtures/accounting/lp_triple_teardown_bug/state.db``).

The fixture reproduces the pre-VIB-4839 silent-cache bug: LP1 + LP2 teardown
CLOSE rows with token0/token1/value_usd all blank; LP3 is a healthy rebalance
CLOSE (negative control). See the fixture's ``generate.py`` for the exact
shape + deterministic pricing (value_usd == $2000 per repaired position).
"""

from __future__ import annotations

import glob
import json
import shutil
import sqlite3
from decimal import Decimal
from pathlib import Path

from click.testing import CliRunner

from almanak.cli.cli import strat as strat_group
from almanak.framework.cli.repair_teardown_lp_close import repair_teardown_lp_close_cmd
from almanak.framework.cli.strat_pnl import strat_pnl
from almanak.framework.observability.pnl_attributor import CURRENT_VERSION

DEPLOYMENT_ID = "LpTriple:fixture4896"
_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "accounting"
    / "lp_triple_teardown_bug"
    / "state.db"
)
EXPECTED_VALUE = Decimal("2000")


def _copy_fixture(tmp_path) -> str:
    """Copy the frozen fixture into tmp_path so the test never mutates it."""
    assert _FIXTURE.is_file(), f"fixture missing: {_FIXTURE}"
    dst = tmp_path / "almanak_state.db"
    shutil.copy2(_FIXTURE, dst)
    return str(dst)


def _row(db_path: str, event_id: str) -> tuple:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT token0, token1, tick_lower, tick_upper, liquidity, value_usd, "
            "attribution_json, attribution_version FROM position_events WHERE id=?",
            (event_id,),
        ).fetchone()
    finally:
        conn.close()


def _full_row(db_path: str, event_id: str) -> tuple:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT * FROM position_events WHERE id=?", (event_id,)).fetchone()
    finally:
        conn.close()


def _pnl_exit_values(db_path: str) -> dict:
    runner = CliRunner()
    result = runner.invoke(strat_pnl, ["-s", DEPLOYMENT_ID, "--db", db_path, "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    return {p["position_id"]: p.get("exit_value_usd") for p in payload.get("lp", {}).get("positions", [])}


def test_repair_red_to_green_e2e(tmp_path):
    db = _copy_fixture(tmp_path)

    # (1) RED — strat pnl shows no exit value for the two broken positions.
    exits_before = _pnl_exit_values(db)
    assert exits_before["LP1"] is None
    assert exits_before["LP2"] is None
    assert Decimal(exits_before["LP3"]) == Decimal("1950")  # healthy control

    # Capture LP3's full row for the byte-identical negative control.
    lp3_before = _full_row(db, "lp3-close")

    # (2) Run repair (default mode) via the CLI.
    runner = CliRunner()
    result = runner.invoke(repair_teardown_lp_close_cmd, ["--db", db])
    assert result.exit_code == 0, result.output
    assert "detected: 2" in result.output
    assert "repaired: 2" in result.output

    # A .bak-* backup must exist.
    backups = glob.glob(db + ".bak-*")
    assert backups, "expected a .bak-<ts> backup to be created"

    # (3) Post-assert rows: tokens, bracket, exact value_usd + principal.
    for cid in ("lp1-close", "lp2-close"):
        token0, token1, tl, tu, liq, value_usd, attr_json, attr_ver = _row(db, cid)
        assert token0 == "WETH" and token1 == "USDC"
        assert tl is not None and tu is not None and liq
        assert Decimal(value_usd) == EXPECTED_VALUE
        attr = json.loads(attr_json)
        assert Decimal(attr["principal_recovered_usd"]) == EXPECTED_VALUE
        assert attr_ver == CURRENT_VERSION

    # (4) NEGATIVE CONTROL — healthy rebalance CLOSE byte-identical.
    assert _full_row(db, "lp3-close") == lp3_before

    # (5) Post-assert render — strat pnl now shows the exit value.
    exits_after = _pnl_exit_values(db)
    assert Decimal(exits_after["LP1"]) == EXPECTED_VALUE
    assert Decimal(exits_after["LP2"]) == EXPECTED_VALUE
    assert Decimal(exits_after["LP3"]) == Decimal("1950")


def test_dry_run_changes_nothing_and_no_backup(tmp_path):
    db = _copy_fixture(tmp_path)
    runner = CliRunner()
    result = runner.invoke(repair_teardown_lp_close_cmd, ["--db", db, "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "DRY RUN" in result.output
    assert "Would repair" in result.output

    # No backup created in dry-run.
    assert not glob.glob(db + ".bak-*")
    # Broken rows still blank.
    for cid in ("lp1-close", "lp2-close"):
        token0, token1, _, _, _, value_usd, _, _ = _row(db, cid)
        assert token0 == "" and token1 == "" and value_usd == ""


def test_idempotent_second_run(tmp_path):
    db = _copy_fixture(tmp_path)
    runner = CliRunner()
    first = runner.invoke(repair_teardown_lp_close_cmd, ["--db", db])
    assert first.exit_code == 0
    assert "repaired: 2" in first.output

    second = runner.invoke(repair_teardown_lp_close_cmd, ["--db", db])
    assert second.exit_code == 0
    assert "detected: 0" in second.output
    assert "Nothing to repair" in second.output


def test_missing_db_exits_nonzero():
    runner = CliRunner()
    result = runner.invoke(repair_teardown_lp_close_cmd, ["--db", "/nonexistent/state.db"])
    assert result.exit_code == 1
    assert "State DB not found" in result.output


# --- Registered-entrypoint coverage -----------------------------------------
# The tests above invoke the click callback (``repair_teardown_lp_close_cmd``)
# directly. The tests below drive the FULL registered path
# ``almanak strat repair-teardown-lp-close`` through the ``strat`` group, so the
# registration in ``almanak/cli/cli.py`` (strat.add_command(..., name=...)) is
# exercised end-to-end and cannot silently regress.


def test_root_path_dry_run_detects_broken_rows(tmp_path):
    """``almanak strat repair-teardown-lp-close --dry-run`` detects but writes nothing."""
    db = _copy_fixture(tmp_path)
    runner = CliRunner()
    result = runner.invoke(strat_group, ["repair-teardown-lp-close", "--db", db, "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "detected: 2" in result.output
    assert "DRY RUN" in result.output
    assert "Would repair" in result.output

    # Dry-run: no backup, broken rows untouched.
    assert not glob.glob(db + ".bak-*")
    for cid in ("lp1-close", "lp2-close"):
        token0, token1, _, _, _, value_usd, _, _ = _row(db, cid)
        assert token0 == "" and token1 == "" and value_usd == ""


def test_root_path_repair_applies_through_registered_command(tmp_path):
    """Real (write) repair through the registered root path mirrors the callback test."""
    db = _copy_fixture(tmp_path)

    # RED — broken positions have no exit value.
    exits_before = _pnl_exit_values(db)
    assert exits_before["LP1"] is None
    assert exits_before["LP2"] is None

    runner = CliRunner()
    result = runner.invoke(strat_group, ["repair-teardown-lp-close", "--db", db])
    assert result.exit_code == 0, result.output
    assert "detected: 2" in result.output
    assert "repaired: 2" in result.output

    # Backup written, rows repaired with the same values as the callback path.
    assert glob.glob(db + ".bak-*"), "expected a .bak-<ts> backup to be created"
    for cid in ("lp1-close", "lp2-close"):
        token0, token1, tl, tu, liq, value_usd, attr_json, attr_ver = _row(db, cid)
        assert token0 == "WETH" and token1 == "USDC"
        assert tl is not None and tu is not None and liq
        assert Decimal(value_usd) == EXPECTED_VALUE
        attr = json.loads(attr_json)
        assert Decimal(attr["principal_recovered_usd"]) == EXPECTED_VALUE
        assert attr_ver == CURRENT_VERSION

    # GREEN — strat pnl now renders the repaired exit value.
    exits_after = _pnl_exit_values(db)
    assert Decimal(exits_after["LP1"]) == EXPECTED_VALUE
    assert Decimal(exits_after["LP2"]) == EXPECTED_VALUE
