"""Process death with retained economic identities and replaceable read-only acquisition."""

import json
import sqlite3
import subprocess
import sys

import pytest

from almanak.backtest.cli import write_json
from almanak.backtest.engine import Journal
from almanak.backtest.prepared import load_dataset
from almanak.connectors.uniswap_v4.economic_prepare import identity
from almanak.connectors.uniswap_v4.economic_state import State
from tests.test_backtest_process import FAULT_DRIVER
from tests.test_uniswap_process import stop_gateway
from tests.uniswap_economic_support import prepared, provision_archive
from tests.uniswap_support import ROOT


@pytest.mark.parametrize("chain,kind", [("ethereum", "trading"), ("avalanche", "lp")])
@pytest.mark.parametrize("stage", ["pending_commit", "effects_commit", "outcome"])
def test_economic_process_death_and_networkless_exact_replay(tmp_path, chain, kind, stage):
    manifest, expected, model = prepared(tmp_path, chain, kind)
    model.book.close()
    directory = tmp_path / "run"
    directory.mkdir()
    write_json(directory / "manifest.json", manifest)
    Journal(directory / "state.sqlite", State).create(manifest.initial)
    # An economic runtime must not start a gateway. Deny socket connections in
    # the process itself, in addition to strategy sandbox network isolation.
    script = (
        "import socket\n"
        "def forbid(*args, **kwargs): raise AssertionError('network forbidden')\n"
        "socket.socket.connect = forbid\n" + FAULT_DRIVER
    )
    failed = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            stage,
            str(tmp_path / "gateway.pid"),
            "backtest",
            "resume",
            "--run",
            str(directory),
            "--progress",
            "none",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert failed.returncode == 74, failed.stderr
    assert not (tmp_path / "gateway.pid").exists()
    for operation in ("resume", "reproduce"):
        arguments = ["backtest", operation, "--run", str(directory), "--progress", "none"]
        result = subprocess.run(
            [sys.executable, "-c", script, "never", str(tmp_path / "gateway.pid"), *arguments],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 0, result.stderr
    from tests.dx_replay import assert_economics

    assert_economics(Journal(directory / "state.sqlite", State).load(), expected)


PREPARE_FAULT = """
import json, os, sys
from pathlib import Path
from types import SimpleNamespace
from almanak.backtest.cli import GatewayProcess
from almanak.backtest.inputs import HarnessSelection
from almanak.backtest.prepared import DatasetWriter
from almanak.connectors.uniswap_v4.economic_prepare import prepare
from almanak.strategy import StrategyDefinition
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory
pidfile = Path(sys.argv[1])
name, output, fault = sys.argv[2:]
root = harness_directory(name)
author = read_author(root / 'config.yaml')
settings = read_backtest(root / 'backtest-economic.yaml', author)
strategy = StrategyDefinition.read(Path.cwd() / 'strategies' / name / 'strategy.py')
enter = GatewayProcess.__enter__
def entered(self):
    value = enter(self)
    pidfile.write_text(str(self.process.pid))
    return value
GatewayProcess.__enter__ = entered
commit = DatasetWriter.commit_range
def committed(self, span, records, evidence):
    commit(self, span, records, evidence)
    if fault == 'exit' and self.db.execute('SELECT COUNT(*) FROM inputs').fetchone()[0] >= 3:
        os._exit(74)
DatasetWriter.commit_range = committed
args = SimpleNamespace(
    strategy=strategy.filename,
    output=Path(output),
    env_file=None,
    _backtest_inputs=HarnessSelection(author, settings, strategy),
)
print(json.dumps(prepare(args), default=str))
"""


@pytest.mark.parametrize("chain,kind", [("ethereum", "trading"), ("avalanche", "lp")])
def test_preparation_death_replaces_gateway_and_preserves_published_inputs(tmp_path, chain, kind):
    directory, manifest, _ = provision_archive(tmp_path, chain, kind)
    name = f"uniswap_v4_{'avalanche_' if chain == 'avalanche' else ''}{kind}"
    common = [str(pidfile := tmp_path / "gateway.pid"), name, str(directory)]
    try:
        result = subprocess.run(
            [sys.executable, "-c", PREPARE_FAULT, *common, "exit"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 74, result.stderr
    finally:
        stop_gateway(pidfile)
    assert not (directory / "dataset.json").exists()
    db = sqlite3.connect(directory / "data.sqlite")
    initial = db.execute("SELECT body FROM inputs ORDER BY time").fetchall()
    db.close()
    assert len(initial) == 3
    resumed = subprocess.run(
        [sys.executable, "-c", PREPARE_FAULT, *common, "continue"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert resumed.returncode == 0, resumed.stderr
    published, reader = load_dataset(directory)
    try:
        assert published.preparation.policy_digest == identity(manifest.config, manifest.settings)
        expected, original = load_dataset(manifest.dataset)
        try:
            from tests.uniswap_economic_support import assert_archive_payloads

            assert_archive_payloads(reader, original)

        finally:
            original.close()
        assert published.records == expected.records
    finally:
        reader.close()
    cached = subprocess.run(
        [sys.executable, "-c", PREPARE_FAULT, *common, "continue"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert cached.returncode == 0, cached.stderr
    assert json.loads(cached.stdout)["cache_state"] == "completed_dataset"
