"""Live execution needs explicit opt-in even through imported entrypoints."""

from __future__ import annotations

import argparse
import asyncio
import sys

import pytest

from qa_lab import mainnet_intent_coordinator as coordinator
from qa_lab import qa_mainnet_lane as lane
from qa_lab import run_mainnet_intent as runner


@pytest.fixture(autouse=True)
def no_ambient_authorization(monkeypatch):
    monkeypatch.delenv("ALMANAK_QA_FORK_CONTEXT", raising=False)
    monkeypatch.delenv(lane.MAINNET_LANE_ENV, raising=False)


@pytest.mark.parametrize("command", sorted(lane.MAINNET_LANE_GUARDED_COMMANDS))
@pytest.mark.parametrize("enabled,authorized", [(False, False), (False, True), (True, False), (True, True)])
def test_both_operator_acts_are_required(command, enabled, authorized):
    env = {lane.MAINNET_LANE_ENV: "enabled"} if enabled else {}
    if enabled and authorized:
        lane.assert_mainnet_lane_enabled(command, operator_authorized=authorized, env=env)
    else:
        with pytest.raises(lane.MainnetLaneDisabledError):
            lane.assert_mainnet_lane_enabled(command, operator_authorized=authorized, env=env)


@pytest.mark.parametrize("enabled", [False, True])
def test_imported_runner_refuses_before_reading_plan(enabled, monkeypatch, tmp_path):
    if enabled:
        monkeypatch.setenv(lane.MAINNET_LANE_ENV, "enabled")
    with pytest.raises(lane.MainnetLaneDisabledError):
        asyncio.run(
            runner.execute_plan(plan_path=tmp_path / "absent", approval_path=tmp_path / "absent", output=tmp_path)
        )


@pytest.mark.parametrize(
    "entry",
    [
        coordinator.prepare,
        coordinator.approve,
        coordinator.run,
        coordinator._execute_owned,
        runner.plan_command,
        runner.approve_command,
    ],
)
def test_imported_control_plane_refuses_before_state_access(entry):
    with pytest.raises(lane.MainnetLaneDisabledError):
        entry(argparse.Namespace())


def test_cli_passes_explicit_authorization_to_runner(monkeypatch, tmp_path):
    reached = []

    async def execute(**kwargs):
        reached.append(kwargs["operator_authorized"])
        return {"overall": "PASS"}

    monkeypatch.setattr(runner, "execute_plan", execute)
    argv = [
        "runner",
        "run",
        "--plan",
        str(tmp_path / "p"),
        "--approval",
        str(tmp_path / "a"),
        "--output",
        str(tmp_path),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(lane.MainnetLaneDisabledError):
        runner.main()
    monkeypatch.setenv(lane.MAINNET_LANE_ENV, "enabled")
    with pytest.raises(lane.MainnetLaneDisabledError):
        runner.main()
    assert not reached
    monkeypatch.setattr(sys, "argv", [*argv, lane.MAINNET_LANE_FLAG])
    runner.main()
    assert reached == [True]


def test_fork_exemption_requires_verified_rpc_identity(monkeypatch):
    class Context:
        def assert_rpc_identity(self):
            raise ValueError("wrong instance")

    monkeypatch.setattr(lane, "active_context", lambda: Context())
    with pytest.raises(ValueError, match="wrong instance"):
        lane.assert_mainnet_lane_enabled("run")


def test_read_only_recovery_is_exempt():
    assert "recover-seal" not in lane.MAINNET_LANE_GUARDED_COMMANDS
    assert "reconcile" not in lane.MAINNET_LANE_GUARDED_COMMANDS
