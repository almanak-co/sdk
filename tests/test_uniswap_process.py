"""Runtime and gateway replacement over both chains' retained history, offline."""

import json
import os
import signal
import subprocess
import sys

import pytest

from almanak.backtest.engine import Journal
from almanak.connectors.uniswap_v4.backtest import State
from tests.test_backtest_process import FAULT_DRIVER, run_cli
from tests.uniswap_support import ROOT, prepare_historical


def stop_gateway(pidfile):
    if not pidfile.exists():
        return
    try:
        os.kill(int(pidfile.read_text()), signal.SIGTERM)
    except ProcessLookupError:
        pass


@pytest.mark.parametrize("name", ["trading", "lp", "avalanche_trading", "avalanche_lp"])
@pytest.mark.parametrize("stage", ["pending_commit", "effects_commit", "outcome"])
def test_actual_process_death_gateway_replacement_and_exact_reproduction(tmp_path, name, stage):
    directory = tmp_path / "run"
    _, expected = prepare_historical(directory, name)
    pidfile = tmp_path / "gateway.pid"
    try:
        failed = subprocess.run(
            [
                sys.executable,
                "-c",
                FAULT_DRIVER,
                stage,
                str(pidfile),
                "backtest",
                "resume",
                "--run",
                str(directory),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert failed.returncode == 74, failed.stderr
    finally:
        stop_gateway(pidfile)
    pending = Journal(directory / "state.sqlite", State).load()
    assert (
        pending.pending is not None
        if stage == "pending_commit"
        else pending.delivered < len(pending.completed)
    )
    resumed = run_cli(["backtest", "resume", "--run", str(directory)])
    assert resumed.returncode == 0, resumed.stderr
    state = Journal(directory / "state.sqlite", State).load()
    assert state.portfolio == expected.portfolio and state.memory == expected.memory
    assert state.mode == "finished" and state.delivered == len(state.completed)
    assert len({c.pending.identity for c in state.completed}) == len(state.completed)
    reproduced = run_cli(["backtest", "reproduce", "--run", str(directory)])
    assert reproduced.returncode == 0, reproduced.stderr
    assert json.loads(reproduced.stdout)["reproduced"]
