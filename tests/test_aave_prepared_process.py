"""Multi-chain prepared state remains exact across real process replacement."""

import sqlite3
import subprocess
import sys

import pytest

from almanak.backtest.cli import write_json
from almanak.backtest.economic_state import EconomicState
from almanak.backtest.engine import Journal
from tests.aave_economic_support import prepared, provision_archive, strategy_name
from tests.test_backtest_process import FAULT_DRIVER
from tests.test_uniswap_economic_process import PREPARE_FAULT
from tests.test_uniswap_process import stop_gateway
from tests.uniswap_support import ROOT


@pytest.mark.parametrize("stage", ["pending_commit", "effects_commit", "outcome"])
def test_both_chain_positions_survive_runtime_death_with_network_disabled(tmp_path, stage):
    manifest, expected, model = prepared(tmp_path)
    model.book.close()
    directory = tmp_path / "run"
    directory.mkdir()
    write_json(directory / "manifest.json", manifest)
    Journal(directory / "state.sqlite", EconomicState).create(manifest.initial)
    script = (
        "import socket\n"
        "def forbid(*args, **kwargs): raise AssertionError('network forbidden')\n"
        "socket.socket.connect = forbid\n"
        + FAULT_DRIVER.replace("len(state.completed) >= 1", "len(state.completed) >= 2")
    )
    arguments = ["backtest", "resume", "--run", str(directory), "--progress", "none"]
    failed = subprocess.run(
        [sys.executable, "-c", script, stage, str(tmp_path / "gateway.pid"), *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert failed.returncode == 74, failed.stderr
    partial = Journal(directory / "state.sqlite", EconomicState).load()
    assert (
        len({p.payload.target.fund.chain for p in partial.portfolio.positions if p.payload.shares})
        == 2
    )
    assert not (tmp_path / "gateway.pid").exists()
    for operation in ("resume", "reproduce"):
        args = ["backtest", operation, "--run", str(directory), "--progress", "none"]
        result = subprocess.run(
            [sys.executable, "-c", script, "never", str(tmp_path / "gateway.pid"), *args],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 0, result.stderr
    assert Journal(directory / "state.sqlite", EconomicState).load() == expected


def test_multichain_preparation_resumes_after_committing_one_chain_inputs(tmp_path):
    from almanak.backtest.prepared import load_dataset

    directory, manifest, _ = provision_archive(tmp_path)
    name = strategy_name("multichain")
    script = PREPARE_FAULT.replace(
        "almanak.connectors.uniswap_v4.economic_prepare",
        "almanak.backtest.economic_prepare",
    )
    pidfile = tmp_path / "gateway.pid"
    common = [str(pidfile), name, str(directory)]
    try:
        stopped = subprocess.run(
            [sys.executable, "-c", script, *common, "exit"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert stopped.returncode == 74, stopped.stderr
    finally:
        stop_gateway(pidfile)
    assert not (directory / "dataset.json").exists()
    with sqlite3.connect(directory / "data.sqlite") as db:
        count = db.execute("SELECT COUNT(*) FROM inputs").fetchone()[0]
        assert count > 0
    result = subprocess.run(
        [sys.executable, "-c", script, *common, "continue"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stderr
    _, actual = load_dataset(directory)
    _, original = load_dataset(manifest.dataset)
    try:
        assert (
            actual.db.execute("SELECT body FROM inputs ORDER BY source,time").fetchall()
            == original.db.execute("SELECT body FROM inputs ORDER BY source,time").fetchall()
        )
    finally:
        actual.close()
        original.close()
