"""Actual Aave strategy scheduling across runtime and gateway process death."""

import signal
import sys
import time
from datetime import UTC, datetime

import pytest

from tests.fork.support import launch as launch
from tests.fork.support import request_teardown, resume, wait_state
from tests.fork.test_lifecycle import (
    ROOT,
    STRATEGY,
    assert_exit,
    evidence,
    setup_run,
    wait_closed,
    wait_holding,
)
from tests.fork.test_lifecycle import fork as fork

pytestmark = pytest.mark.fork


def test_admitted_intent_recovers_and_tears_down_before_next_interval(fork, tmp_path, launch):
    _, journal, arguments = setup_run(fork, tmp_path, launch, decision_interval=120)
    crashed = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(STRATEGY),
            *arguments,
            "--crash",
            "decision_committed",
        ]
    )
    assert_exit(crashed, -signal.SIGKILL)
    admitted = journal.load()
    assert admitted.decision is not None and admitted.plan is None
    running = resume(launch, journal, arguments)
    holding = wait_holding(journal, running)
    assert_recovered_intent(admitted, holding)
    running.kill()
    assert_exit(running, -signal.SIGKILL)
    fork.gateway_process.kill()
    assert_exit(fork.gateway_process, -signal.SIGKILL)
    fork.gateway_process = launch(fork.gateway_arguments, ready="gateway_ready")
    running = resume(launch, journal, arguments)
    assert_idle_before_deadline(journal, running, holding)
    request_teardown(arguments)
    closed = wait_closed(journal, running)
    assert_exit(running)
    assert_closed_before_deadline(admitted, closed)
    evidence(journal, "interval-admission-restart-teardown")


def assert_recovered_intent(admitted, holding):
    assert holding.schedule == admitted.schedule
    assert all(outcome.decision == admitted.decision for outcome in holding.outcomes)


def assert_closed_before_deadline(admitted, closed):
    assert closed.schedule == admitted.schedule
    assert datetime.now(UTC) < admitted.schedule.next_due_at
    assert [outcome.operation.kind for outcome in closed.outcomes] == [
        "approve",
        "supply",
        "withdraw",
    ]


def assert_idle_before_deadline(journal, running, holding):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        assert running.poll() is None
        assert datetime.now(UTC) < holding.schedule.next_due_at
        assert journal.load() == holding
        time.sleep(0.1)


@pytest.mark.parametrize("stage", ["evaluated", "decision_committed"])
def test_hold_crash_resumes_schedule_without_duplicate_supply(fork, tmp_path, launch, stage):
    _, journal, arguments = setup_run(fork, tmp_path, launch)
    crashed = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(STRATEGY),
            *arguments,
            "--crash",
            stage,
            "--decision-number",
            "2",
        ]
    )
    assert_exit(crashed, -signal.SIGKILL, timeout=80)
    before = journal.load()
    assert before.decision is None and before.pending is None
    assert [outcome.operation.kind for outcome in before.outcomes] == ["approve", "supply"]
    running = resume(launch, journal, arguments)
    after = wait_state(journal, lambda state: state.schedule != before.schedule, running)
    assert_only_schedule_advanced(before, after)
    request_teardown(arguments)
    wait_closed(journal, running)
    assert_exit(running)
    evidence(journal, "interval-hold-" + stage)


def assert_only_schedule_advanced(before, after):
    assert after.schedule.last_evaluation.due_at == before.schedule.next_due_at
    assert after.memory == before.memory and after.outcomes == before.outcomes
    assert after.owned == before.owned
