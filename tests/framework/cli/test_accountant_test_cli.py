"""Unit tests for the accountant_test CLI's gating modes (VIB-3870)."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from almanak.framework.cli.accountant_test_cli import main


def _make_minimal_passing_lp_db() -> Path:
    """Build a synthetic LP DB that passes the event-driven generic cells
    + LP1. This is the same shape used by ``test_accountant_test`` —
    duplicated here so the CLI tests stay self-contained.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE transaction_ledger (
            id TEXT PRIMARY KEY, cycle_id TEXT, strategy_id TEXT,
            deployment_id TEXT, execution_mode TEXT, timestamp TEXT,
            intent_type TEXT, token_in TEXT, amount_in TEXT, token_out TEXT,
            amount_out TEXT, gas_used INTEGER, gas_usd TEXT, tx_hash TEXT,
            chain TEXT, protocol TEXT, success INTEGER,
            extracted_data_json TEXT, price_inputs_json TEXT,
            pre_state_json TEXT, post_state_json TEXT,
            schema_version INTEGER, formula_version INTEGER,
            matching_policy_version INTEGER
        );
        CREATE TABLE position_events (
            id TEXT, cycle_id TEXT, deployment_id TEXT, position_id TEXT,
            position_type TEXT, event_type TEXT, timestamp TEXT,
            protocol TEXT, chain TEXT, tick_lower INTEGER, tick_upper INTEGER,
            liquidity TEXT, fees_token0 TEXT, fees_token1 TEXT,
            tx_hash TEXT, ledger_entry_id TEXT
        );
        CREATE TABLE accounting_events (
            id TEXT, cycle_id TEXT, deployment_id TEXT, strategy_id TEXT,
            timestamp TEXT, chain TEXT, protocol TEXT, event_type TEXT,
            position_key TEXT, ledger_entry_id TEXT, tx_hash TEXT,
            confidence TEXT, payload_json TEXT
        );
        CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY, strategy_id TEXT, deployment_id TEXT,
            cycle_id TEXT, total_value_usd TEXT, available_cash_usd TEXT,
            deployed_capital_usd TEXT, value_confidence TEXT,
            iteration_number INTEGER, timestamp TEXT, chain TEXT
        );
        CREATE TABLE portfolio_metrics (
            strategy_id TEXT, initial_value_usd TEXT, total_value_usd TEXT,
            cycle_id TEXT, deployment_id TEXT, is_complete INTEGER
        );
        """
    )
    cur.execute(
        "INSERT INTO transaction_ledger VALUES ('led-1', 'cyc-1', 'lp-test', "
        "'lp-test', 'live', '2026-05-01T00:00:00Z', 'LP_OPEN', 'WETH', '0.001', "
        "'USDC', '3.0', 500000, '0.05', '0xdeadbeef', 'arbitrum', 'uniswap_v3', 1, "
        "'{}', "
        "'{\"WETH\": {\"price_usd\": \"3000\", \"oracle_source\": \"chainlink\"}, "
        "\"USDC\": {\"price_usd\": \"1.0\", \"oracle_source\": \"chainlink\"}}', "
        "'{}', '{}', 1, 1, 1)"
    )
    cur.execute(
        "INSERT INTO position_events VALUES ('pe-1', 'cyc-1', 'lp-test', 'pos-1', "
        "'LP', 'LP_OPEN', '2026-05-01T00:00:00Z', 'uniswap_v3', 'arbitrum', "
        "100, 200, '1234567', '0', '0', '0xdeadbeef', 'led-1')"
    )
    cur.execute(
        "INSERT INTO accounting_events VALUES ('ae-1', 'cyc-1', 'lp-test', 'lp-test', "
        "'2026-05-01T00:00:00Z', 'arbitrum', 'uniswap_v3', 'LP_OPEN', 'pos-1', "
        "'led-1', '0xdeadbeef', 'HIGH', "
        "'{\"event_type\": \"LP_OPEN\", \"protocol\": \"uniswap_v3\", "
        "\"position_key\": \"pos-1\", \"pool_address\": \"weth-usdc-500\", "
        "\"token0\": \"WETH\", \"token1\": \"USDC\", \"amount0\": \"0.001\", "
        "\"amount1\": \"3.0\", \"amount0_usd\": \"3.0\", \"amount1_usd\": \"3.0\", "
        "\"cost_basis_usd\": \"6.0\", \"tick_lower\": 100, \"tick_upper\": 200, "
        "\"liquidity\": 1234567, \"confidence\": \"HIGH\", \"matching_policy_version\": 1}')"
    )
    for i, val in enumerate(["10.0", "10.0001", "10.0002"]):
        cur.execute(
            "INSERT INTO portfolio_snapshots (strategy_id, deployment_id, cycle_id, "
            "total_value_usd, available_cash_usd, deployed_capital_usd, "
            "value_confidence, iteration_number, timestamp, chain) "
            "VALUES ('lp-test', 'lp-test', 'cyc-1', ?, '0', ?, 'HIGH', ?, ?, 'arbitrum')",
            (val, val, i, f"2026-05-01T00:0{i}:00Z"),
        )
    cur.execute(
        "INSERT INTO portfolio_metrics VALUES ('lp-test', '10.0', '10.0002', "
        "'cyc-1', 'lp-test', 0)"
    )
    conn.commit()
    conn.close()
    return path


def _argv(*extra: str, db: Path) -> list[str]:
    return ["--db", str(db), "--primitive", "lp", *extra]


def test_default_mode_passes_when_no_cells_fail():
    """Default behaviour (progress scorecard): exit 0 unless any cell FAILs."""
    db = _make_minimal_passing_lp_db()
    try:
        rc = main(_argv(db=db))
        assert rc == 0
    finally:
        db.unlink(missing_ok=True)


def test_strict_mode_fails_when_cells_xfail(capsys):
    """VIB-3870 acceptance: --strict exits non-zero on any non-PASS cell.
    The minimal LP DB has Track-C-dependent cells (G14, G15, LP2, LP6,
    etc.) at XFAIL — under --strict that fails the gate."""
    db = _make_minimal_passing_lp_db()
    try:
        rc = main(_argv("--strict", db=db))
        assert rc == 1
        err = capsys.readouterr().err
        assert "--strict gate" in err
        assert "XFAIL" in err
    finally:
        db.unlink(missing_ok=True)


def test_require_cells_passes_when_listed_cells_all_pass():
    """VIB-3870 acceptance: --require-cells G2,G7 — both PASS in fixture."""
    db = _make_minimal_passing_lp_db()
    try:
        rc = main(_argv("--require-cells", "G2,G7,G9", db=db))
        assert rc == 0
    finally:
        db.unlink(missing_ok=True)


def test_require_cells_fails_when_listed_cell_is_not_pass(capsys):
    """G14 is XFAIL in the minimal fixture — listing it must fail the gate."""
    db = _make_minimal_passing_lp_db()
    try:
        rc = main(_argv("--require-cells", "G2,G14", db=db))
        assert rc == 1
        err = capsys.readouterr().err
        assert "--require-cells gate" in err
        assert "G14" in err
    finally:
        db.unlink(missing_ok=True)


def test_require_cells_rejects_unknown_cell_id(capsys):
    """An unknown cell ID is a usage error (return code 2), not a gate failure."""
    db = _make_minimal_passing_lp_db()
    try:
        rc = main(_argv("--require-cells", "G42,G99", db=db))
        assert rc == 2
        err = capsys.readouterr().err
        assert "unknown cell IDs" in err
    finally:
        db.unlink(missing_ok=True)


def test_strict_and_require_cells_are_mutually_exclusive(capsys):
    db = _make_minimal_passing_lp_db()
    try:
        rc = main(_argv("--strict", "--require-cells", "G2", db=db))
        assert rc == 2
        err = capsys.readouterr().err
        assert "mutually exclusive" in err
    finally:
        db.unlink(missing_ok=True)


def test_default_mode_fails_when_cells_fail():
    """Sanity check: a malformed payload makes G6 FAIL, which the default
    progress-scorecard mode also fails on."""
    # Reuse the test_accountant_test fixture pattern — but simpler: build
    # a DB where G2 FAILs because gas_used > 0 and gas_usd is empty.
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    path = Path(tmp.name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE transaction_ledger (
            id TEXT, cycle_id TEXT, strategy_id TEXT, deployment_id TEXT,
            execution_mode TEXT, timestamp TEXT, intent_type TEXT,
            gas_used INTEGER, gas_usd TEXT, tx_hash TEXT, chain TEXT,
            protocol TEXT, success INTEGER, extracted_data_json TEXT,
            price_inputs_json TEXT, pre_state_json TEXT,
            post_state_json TEXT
        );
        CREATE TABLE position_events (id TEXT, cycle_id TEXT, deployment_id TEXT, event_type TEXT, position_id TEXT);
        CREATE TABLE accounting_events (id TEXT, cycle_id TEXT, deployment_id TEXT, event_type TEXT, payload_json TEXT, confidence TEXT);
        CREATE TABLE portfolio_snapshots (id INTEGER, total_value_usd TEXT, value_confidence TEXT, iteration_number INTEGER, timestamp TEXT);
        CREATE TABLE portfolio_metrics (strategy_id TEXT, initial_value_usd TEXT);
        """
    )
    cur.execute(
        "INSERT INTO transaction_ledger (id, cycle_id, intent_type, gas_used, gas_usd, "
        "tx_hash, success, price_inputs_json) "
        "VALUES ('l1', 'cyc', 'SWAP', 100000, '', '0xabc', 1, '')"
    )
    conn.commit()
    conn.close()
    try:
        rc = main(_argv(db=path))
        # G2 + G12 + G13 FAIL on this fixture → default mode also returns 1.
        assert rc == 1
    finally:
        path.unlink(missing_ok=True)


@pytest.mark.parametrize("flag", ["--strict", "--require-cells"])
def test_unknown_flag_combos_help_smoke(flag, capsys):
    """Smoke: the CLI's argparse usage line still works with the new flags
    (i.e. we didn't accidentally break --help formatting)."""
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert flag in out
