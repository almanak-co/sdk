"""The paused mainnet QA lane must be mechanically closed, not documented shut.

A sentence in a runbook is not a control: an agent that can read the runbook can
also invoke ``run_mainnet_intent.py``. Reproduced at 21256be6f -- ``main()``
dispatched ``run`` straight into ``execute_plan`` with no environment check and
no operator acknowledgement, on a runner that funds a live wallet and signs
mainnet transactions.

Re-enabling therefore takes two independent, deliberate acts: the environment
variable AND the explicit flag. One alone is an accident; two are a decision.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
RUNNER = REPO_ROOT / "scripts" / "quant-test" / "run_mainnet_intent.py"


@pytest.fixture(scope="module")
def runner():
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    spec = importlib.util.spec_from_file_location("run_mainnet_intent_guard_test", RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ENABLED = {"ALMANAK_QA_MAINNET_LANE": "enabled"}


def test_every_guarded_command_refuses_with_no_environment_re_enable(runner) -> None:
    for command in sorted(runner.MAINNET_LANE_GUARDED_COMMANDS):
        with pytest.raises(runner.MainnetLaneDisabledError, match="ALMANAK_QA_MAINNET_LANE=enabled"):
            runner.assert_mainnet_lane_enabled(command, operator_authorized=True, env={})


def test_every_guarded_command_refuses_with_the_environment_but_no_operator_flag(runner) -> None:
    for command in sorted(runner.MAINNET_LANE_GUARDED_COMMANDS):
        with pytest.raises(runner.MainnetLaneDisabledError, match=runner.MAINNET_LANE_FLAG):
            runner.assert_mainnet_lane_enabled(command, operator_authorized=False, env=ENABLED)


@pytest.mark.parametrize("value", ["", "0", "false", "no", "disabled", "true", "yes", "ENABLE", "enabled "])
def test_only_the_exact_enable_value_opens_the_lane(runner, value: str) -> None:
    """Fail closed on anything that merely looks affirmative."""
    if value.strip().lower() == runner.MAINNET_LANE_ENABLED_VALUE:
        runner.assert_mainnet_lane_enabled("run", operator_authorized=True, env={"ALMANAK_QA_MAINNET_LANE": value})
        return
    with pytest.raises(runner.MainnetLaneDisabledError):
        runner.assert_mainnet_lane_enabled("run", operator_authorized=True, env={"ALMANAK_QA_MAINNET_LANE": value})


def test_both_acts_together_open_the_lane(runner) -> None:
    for command in sorted(runner.MAINNET_LANE_GUARDED_COMMANDS):
        runner.assert_mainnet_lane_enabled(command, operator_authorized=True, env=ENABLED)


def test_cli_run_never_reaches_the_money_path_without_a_re_enable(runner, monkeypatch) -> None:
    """The negative control: without the guard this reaches ``execute_plan``."""
    reached: dict[str, bool] = {}

    async def _fake_execute_plan(*, plan_path, approval_path, output, operator_authorized=False):
        reached["money_path"] = True
        return {"overall": "PASS"}

    monkeypatch.setattr(runner, "execute_plan", _fake_execute_plan)
    monkeypatch.delenv("ALMANAK_QA_MAINNET_LANE", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_mainnet_intent.py", "run", "--plan", "/tmp/p.json", "--approval", "/tmp/a.json", "--output", "/tmp/o"],
    )

    with pytest.raises(runner.MainnetLaneDisabledError):
        runner.main()
    assert reached == {}

    # The env var alone is still not enough from the CLI.
    monkeypatch.setenv("ALMANAK_QA_MAINNET_LANE", "enabled")
    with pytest.raises(runner.MainnetLaneDisabledError, match=runner.MAINNET_LANE_FLAG):
        runner.main()
    assert reached == {}

    # Both acts: the guard is liveness-tested, not merely always-raising.
    monkeypatch.setattr(sys, "argv", [*sys.argv, runner.MAINNET_LANE_FLAG])
    runner.main()
    assert reached == {"money_path": True}


def test_the_importable_money_path_is_guarded_too(runner, monkeypatch) -> None:
    """An agent can call ``execute_plan`` directly and never touch argparse."""
    import asyncio

    monkeypatch.delenv("ALMANAK_QA_MAINNET_LANE", raising=False)
    with pytest.raises(runner.MainnetLaneDisabledError):
        asyncio.run(
            runner.execute_plan(plan_path=Path("/tmp/p.json"), approval_path=Path("/tmp/a.json"), output=Path("/tmp/o"))
        )


def test_plan_and_approve_commands_refuse_before_doing_any_work(runner, monkeypatch) -> None:
    monkeypatch.delenv("ALMANAK_QA_MAINNET_LANE", raising=False)
    with pytest.raises(runner.MainnetLaneDisabledError):
        runner.plan_command(argparse.Namespace(cell_id="whatever", pool_index=0, output=Path("/tmp/plan.json")))
    with pytest.raises(runner.MainnetLaneDisabledError):
        runner.approve_command(
            argparse.Namespace(plan=Path("/tmp/plan.json"), approver="agent", output=Path("/tmp/a.json"))
        )


def test_recover_seal_is_deliberately_exempt(runner) -> None:
    """It seals an already-completed bundle: it signs nothing and spends nothing.

    Guarding it would strand evidence for runs that already happened.
    """
    assert "recover-seal" not in runner.MAINNET_LANE_GUARDED_COMMANDS


def test_the_importable_money_path_requires_the_flag_not_just_the_env(runner, monkeypatch) -> None:
    """The env var alone must not open ``execute_plan`` for an importing agent.

    ``execute_plan`` used to hardcode ``operator_authorized=True`` at its own
    guard, so the CLI flag was never a second independent act on the import
    path. The flag now defaults to False and must be threaded in explicitly.
    """
    import asyncio

    monkeypatch.setenv("ALMANAK_QA_MAINNET_LANE", "enabled")
    with pytest.raises(runner.MainnetLaneDisabledError, match=runner.MAINNET_LANE_FLAG):
        asyncio.run(
            runner.execute_plan(plan_path=Path("/tmp/p.json"), approval_path=Path("/tmp/a.json"), output=Path("/tmp/o"))
        )
