"""Fault-injection driver for real process-death recovery tests."""

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

from almanak.connectors.composition import configuration_from_json
from almanak.runtime import Runtime
from almanak.strategy import StrategyDefinition

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=Path, required=True)
parser.add_argument("--strategy", type=Path, required=True)
parser.add_argument("--gateway-url", required=True)
parser.add_argument("--gateway-auth-file", type=Path, required=True)
parser.add_argument(
    "--crash",
    choices=(
        "reserved",
        "signed",
        "prepared",
        "broadcast",
        "receipt",
        "commit",
        "outcome",
        "transition",
        "evaluated",
        "decision_committed",
    ),
)
parser.add_argument(
    "--operation",
    default="supply",
)
parser.add_argument("--allocation")
parser.add_argument("--result-kind")
# A venue action's attempt purpose (HyperCore's `entry`, for instance), whose operations
# all share one kind.
parser.add_argument("--purpose")
parser.add_argument("--resume", action="store_true")
parser.add_argument("--teardown-after-entry", action="store_true")
parser.add_argument("--decision-number", type=int, default=1)
args = parser.parse_args()
decision_events = 0


def crash(stage, pending):
    if stage != args.crash:
        return
    if stage in ("evaluated", "decision_committed"):
        crash_decision()
    elif operation_matches(pending):
        os.kill(os.getpid(), signal.SIGKILL)


def operation_matches(pending):
    operation = pending.operation
    return (
        operation.kind == args.operation
        and unset_or_equal(args.allocation, lambda: operation.allocation)
        and unset_or_equal(args.result_kind, lambda: pending.kind)
        and unset_or_equal(args.purpose, lambda: operation.attempt.purpose)
    )


def unset_or_equal(wanted, read):
    """An unset argument matches any value; a set one must equal the pending field, read
    only then since other operations may not carry it."""
    return wanted is None or read() == wanted


def crash_decision():
    global decision_events
    decision_events += 1
    if decision_events == args.decision_number:
        os.kill(os.getpid(), signal.SIGKILL)


runner = Runtime(
    args.gateway_url,
    args.gateway_auth_file,
    create=None if args.resume else configuration_from_json(args.config.read_text()),
    strategy=None if args.resume else StrategyDefinition.read(args.strategy),
    fault=crash,
)
print(json.dumps({"event": "ready"}), file=sys.stderr, flush=True)
try:
    while not runner.tick():
        state = runner.journal.load()
        if (
            args.teardown_after_entry
            and state.mode == "running"
            and (len(state.memory.values.get("borrowed_at", {})) == len(state.config.debts))
        ):
            runner.journal.teardown()
        time.sleep(0.05)
finally:
    runner.close()
