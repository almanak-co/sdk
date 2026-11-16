"""Operational options must not overwrite inputs or blur incompatible timing evidence."""

import json
import os
import signal
import subprocess
import sys
from types import SimpleNamespace

import pytest

from almanak.backtest import cli, progress_cli
from almanak.backtest.inputs import HarnessSelection
from almanak.backtest.progress import ProgressJob
from almanak.backtest.progress_types import ProgressSnapshot
from almanak.strategy import StrategyDefinition
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.backtest_support import LEGACY, REFERENCE, ROOT


@pytest.fixture
def arguments(tmp_path):
    # The harness selection path reads the frozen unconverted reference files.
    author = read_author(LEGACY / "config.yaml")
    settings = read_backtest(LEGACY / "backtest.yaml", author)
    strategy = StrategyDefinition.read(REFERENCE / "strategy.py")
    return SimpleNamespace(
        product="backtest",
        strategy=REFERENCE / "strategy.py",
        config=LEGACY / "config.yaml",
        backtest_config=LEGACY / "backtest.yaml",
        output=tmp_path / "run",
        env_file=None,
        progress="none",
        _backtest_inputs=HarnessSelection(author, settings, strategy),
    )


@pytest.mark.parametrize("option", ["progress_file", "timing_history"])
def test_paths_preserve_run_state_and_inputs(arguments, option):
    setattr(arguments, option, arguments.config)
    with pytest.raises(ValueError, match="command inputs"):
        progress_cli.paths(arguments)
    setattr(arguments, option, arguments.output / "state.sqlite")
    with pytest.raises(ValueError, match="outside the run"):
        progress_cli.paths(arguments)


def test_same_operational_files_are_rejected(arguments, tmp_path):
    arguments.progress_file = arguments.timing_history = tmp_path / "one.json"
    with pytest.raises(ValueError, match="differ from each other"):
        progress_cli.paths(arguments)


def test_existing_run_preserves_its_progress(arguments):
    arguments.output.mkdir()
    path = arguments.output.with_name("run.progress.json")
    path.write_text("retained original progress")
    with pytest.raises(FileExistsError, match="backtest-resume"):
        cli.run_with_progress(arguments)
    assert path.read_text() == "retained original progress"


def test_timing_identity_uses_window_length_and_operational_context(arguments):
    settings, config, strategy_hash = progress_cli.inputs(arguments)
    before = progress_cli.timing_key(arguments, settings, config, strategy_hash, "cold")
    shifted = json.loads(json.dumps(settings))
    shifted["schedule"]["start"] += 86400
    shifted["schedule"]["end"] += 86400
    assert progress_cli.timing_key(arguments, shifted, config, strategy_hash, "cold") == before
    changed = json.loads(json.dumps(settings))
    changed["history"][0]["collection"]["max_in_flight"] = 1
    assert progress_cli.timing_key(arguments, changed, config, strategy_hash, "cold") != before
    assert (
        progress_cli.timing_key(arguments, settings, config, strategy_hash, "selected_dataset")
        != before
    )
    arguments.timing_scope = "different-worker-class"
    assert progress_cli.timing_key(arguments, settings, config, strategy_hash, "cold") != before


def test_public_selection_views_keep_the_same_timing_identity(tmp_path):
    from tests.backtest_support import converted_config

    config, operator, _ = converted_config(
        tmp_path, SimpleNamespace(start=1787183999), SimpleNamespace(server_port=1)
    )
    arguments = SimpleNamespace(
        product="backtest",
        strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
        config=config,
        backtest_config=config.with_name("backtest.yaml"),
        gateway_config=operator,
        output=tmp_path / "run",
        env_file=None,
        progress="none",
    )
    settings, authored, strategy_hash = progress_cli.inputs(arguments)
    assert settings["market_model"] == "historical_path"
    assert settings["schedule"]["decision_interval_override_seconds"] == 60
    assert "history" not in settings and "execution" not in authored
    assert progress_cli.cache_mode(arguments) == "cold"
    before = progress_cli.timing_key(arguments, settings, authored, strategy_hash, "cold")
    shifted = json.loads(json.dumps(settings))
    shifted["schedule"]["start"] += 86400
    shifted["schedule"]["end"] += 86400
    assert progress_cli.timing_key(arguments, shifted, authored, strategy_hash, "cold") == before
    changed = json.loads(json.dumps(settings))
    changed["operator"] = "0" * 64
    assert progress_cli.timing_key(arguments, changed, authored, strategy_hash, "cold") != before
    with ProgressJob(tmp_path / "progress.json", "key", interval=3600) as job:
        progress_cli.initial_plan(job, arguments, settings, "cold")
        assert {"simulation", "teardown"} <= set(job.phases)


def test_prepare_plan_and_terminal_modes(arguments, tmp_path, monkeypatch):
    settings, _, _ = progress_cli.inputs(arguments)
    arguments.product = "backtest-prepare"
    with ProgressJob(tmp_path / "progress.json", "key", interval=3600) as job:
        progress_cli.initial_plan(job, arguments, settings, "cold")
        assert "preparation" in job.phases
        assert "simulation" not in job.phases
    assert progress_cli.progress_stream(arguments) is None
    arguments.progress = "auto"
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)
    assert progress_cli.progress_stream(arguments) is sys.stderr


def test_sigterm_records_interruption_and_retains_checkpoint_identity(tmp_path):
    path = tmp_path / "progress.json"
    code = """
import signal,sys,time
from pathlib import Path
from almanak.backtest.progress import ProgressJob
from almanak.backtest.progress_cli import termination_signal
with termination_signal(), ProgressJob(Path(sys.argv[1]), "signal-test") as job:
    job.begin("waiting", "Waiting", 10.0)
    print("ready", flush=True)
    time.sleep(30)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", code, str(path)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        os.kill(process.pid, signal.SIGTERM)
        _, error = process.communicate(timeout=5)
        assert process.returncode == 143, error
        snapshot = ProgressSnapshot.model_validate_json(path.read_text())
        assert snapshot.status == "interrupted"
        assert snapshot.eta.remaining_seconds is None
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
