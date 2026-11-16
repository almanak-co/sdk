"""Bounded child processes; no assertion retries and no surviving process groups."""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from tests.qa.catalog import ROOT
from tests.qa.inputs import write_json


def pytest_workers(value):
    if value == "auto":
        return value
    try:
        count = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "pytest workers must be auto or a positive integer"
        ) from None
    if count <= 0:
        raise argparse.ArgumentTypeError("pytest workers must be auto or a positive integer")
    return count


def networkless(command):
    if sys.platform == "linux":
        return [
            "bwrap",
            "--unshare-net",
            "--die-with-parent",
            "--dev-bind",
            "/",
            "/",
            "--proc",
            "/proc",
            "--",
            *command,
        ]
    if sys.platform == "darwin":
        policy = (
            "(version 1)(allow default)(deny network*)"
            '(allow network* (local ip "localhost:*") (remote ip "localhost:*"))'
        )
        return ["/usr/bin/sandbox-exec", "-p", policy, *command]
    raise RuntimeError("QA offline execution requires OS network isolation")


def stop_group(pid, grace=2):
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    time.sleep(grace)
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def run(command, directory: Path, name, timeout=1800, offline=False, grace=2):
    directory.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    actual = networkless(command) if offline else command
    record = {
        "command": command,
        "network": "isolated" if offline else "explicit_providers",
        "status": "failed",
    }
    write_json(directory / f"{name}.json", record)
    with (directory / f"{name}.log").open("w") as output:
        process = subprocess.Popen(
            actual, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT, start_new_session=True
        )
        try:
            record["exit_code"] = process.wait(timeout=timeout)
            record["status"] = "passed" if process.returncode == 0 else "failed"
        except subprocess.TimeoutExpired:
            record["status"] = "blocked"
            record["category"] = "timeout"
        finally:
            stop_group(process.pid, grace)
            process.wait()
    record["wall_seconds"] = time.monotonic() - started
    write_json(directory / f"{name}.json", record)
    if record["status"] != "passed":
        raise RuntimeError(f"{name}: {record['status']}; inspect {name}.log")
    return record


def cli(arguments):
    return [
        sys.executable,
        "-c",
        "from almanak.cli import main; raise SystemExit(main())",
        *map(str, arguments),
        "--progress",
        "none",
    ]
