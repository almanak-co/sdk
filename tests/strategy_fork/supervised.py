"""One converted reference lifecycle driven only through the public CLI.

Every state change goes through `almanak run/pause/resume/teardown/shutdown` in a
child process, so the proof covers the published commands rather than internal
calls. Retained evidence is the workspace subset that carries no key or token.
"""

import json
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx

from almanak.gateway.fork_fixture import Fork, Rpc
from almanak.gateway.operator_fixture import EoaFixtureFund
from almanak.models import Block, Chain
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from almanak.supervisor_process import running, stop
from almanak.workspace import read as read_workspace
from almanak.workspace import request as control

ROOT = Path(__file__).resolve().parents[2]
# One lifecycle phase over a proxied archive provider can take several minutes.
# A V4 teardown unwinds several sequential intents, each a full observe-sign-
# broadcast-reconcile cycle, so closure alone can exceed a quarter hour.
PHASE_TIMEOUT = 1800
# A control command only reaches the local supervisor; closure work is awaited separately.
CONTROL_TIMEOUT = 180
CLI = [sys.executable, "-c", "from almanak.cli import main; raise SystemExit(main())"]

# Tests monkeypatch this hook to drive a supervised fault-injection reversal. It is
# called as fault(node, held, refuse) while the workspace is paused; refuse() resumes,
# requests teardown and returns the refused checkpoint with its workspace evidence.
# The hook returns the proof dict it populated, which the reversal marks with
# restored_router_completed_reference once the restored router closes the reference.
supervised_fault = None


def revalidated(model, **changes):
    return type(model).model_validate({**model.model_dump(), **changes})


def eoa_inputs(inputs):
    """The same fixture funding, held and submitted by plain EOAs instead of Safes."""
    funds = tuple(
        EoaFixtureFund(
            kind="eoa", account=f.account, native_balance=inputs.native_balance(f), assets=f.assets
        )
        for f in inputs.funds
    )
    return revalidated(inputs, funds=funds, operators=())


def launch_supervisor(root):
    """Start the actual supervisor process exactly as the public workspace command does."""
    with (root / "logs" / "supervisor.log").open("a") as log:
        return subprocess.Popen(
            [sys.executable, "-m", "almanak.supervisor", "--run", str(root)],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            cwd="/",
        )


def reachable(state, stages):
    return state["stage"] in stages and bool(state.get("control_url"))


def ready_status(root, state, stages):
    """The control status once a selected stage is reported and provisioning has finished."""
    if not reachable(state, stages):
        return None
    try:
        status = control(root, "status")
    except (httpx.HTTPError, ValueError):
        return None
    return None if status["provisioning"] else status


def await_stage(root, stages):
    deadline = time.monotonic() + PHASE_TIMEOUT
    while time.monotonic() < deadline:
        state = read_workspace(root)
        if state["stage"] == "failed":
            raise AssertionError(f"supervisor failed: {state.get('diagnostic')}")
        status = ready_status(root, state, stages)
        if status is not None:
            return status
        time.sleep(0.5)
    raise AssertionError(f"supervisor did not reach {stages}; inspect logs/supervisor.log")


def loaded_checkpoint(database):
    """The coordinator creates the schema and its first checkpoint after startup."""
    if not database.is_file():
        return None
    try:
        return Journal(database).load()
    except (RuntimeError, sqlite3.OperationalError):
        return None


def await_checkpoint(root, predicate):
    database = root / "state.sqlite"
    deadline = time.monotonic() + PHASE_TIMEOUT
    while time.monotonic() < deadline:
        state = loaded_checkpoint(database)
        if state is not None and predicate(state):
            return state
        if not running(read_workspace(root)["processes"].get("coordinator")):
            raise AssertionError("coordinator stopped before the required phase; inspect logs")
        time.sleep(0.5)
    raise AssertionError("supervised lifecycle did not reach its required phase")


class SupervisedNode(Fork):
    """A fork node the supervisor already runs, seen through the harness node surface.

    The scenario assertions read the node, select it by chain and re-check its
    origin; nothing here starts, snapshots or stops the process, so the Fork
    constructor is bypassed.
    """

    def __init__(self, entry):
        self.rpc = Rpc(entry["endpoint"])
        self.metadata = entry["metadata"]
        self.origin = Block.model_validate_json(json.dumps(entry["origin"]))
        instance = self.metadata["instanceId"]
        self.execution = Chain(chain_id=self.origin.chain.chain_id, environment="anvil:" + instance)


class ForkHandles:
    """Chain-scoped assertions select a node the way `ForkSet.for_chain` does."""

    def __init__(self, handles):
        self.handles = handles

    def for_chain(self, chain):
        return next(h for h in self.handles if chain in (h.origin.chain, h.execution))


class WorkspaceCoordinator:
    """Process-like view of the coordinator managed behind public control commands."""

    def __init__(self, root):
        self.root = root

    def poll(self):
        return None if running(read_workspace(self.root)["processes"].get("coordinator")) else 1


def fork_handle(root):
    """The fork node of a single-chain workspace, or a chain-keyed set for several chains."""
    processes = read_workspace(root)["processes"]
    handles = [SupervisedNode(e) for name, e in processes.items() if name.startswith("fork-")]
    if not handles:
        raise AssertionError(f"workspace has no fork node: {sorted(processes)}")
    return handles[0] if len(handles) == 1 else ForkHandles(tuple(handles))


def retain_workspace(root, artifact):
    for relative in (
        "workspace.json",
        "preflight.json",
        "private/resolution.json",
        "private/fixture-plan.json",
        "private/parameters.json",
        "private/deployment/preparation.json",
        "private/deployment/deployment.json",
        "private/deployment/fixture/fixture-started.json",
        "private/deployment/fixture/fixture-accounts.json",
        "private/deployment/fixture/fixture-installation.json",
        "private/deployment/fixture/permission-verification.json",
    ):
        path = root / relative
        if path.is_file():
            shutil.copyfile(path, artifact / path.name)
    for name in (
        "lp-burn-minima.json",
        "external-execution.json",
        "permission-budget-observations.json",
        "market-drive.json",
    ):
        path = root / name
        if path.is_file():
            shutil.copyfile(path, artifact / name)
    # The coordinator, gateway and supervisor logs say why a run stopped early.
    for log in sorted((root / "logs").glob("*.log")) if (root / "logs").is_dir() else ():
        (artifact / "logs").mkdir(exist_ok=True)
        shutil.copyfile(log, artifact / "logs" / log.name)
    database = root / "state.sqlite"
    if database.is_file():
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as source:
            with sqlite3.connect(artifact / "state.sqlite") as copy:
                source.backup(copy)
        assert Journal(artifact / "state.sqlite").load() == Journal(database).load()


def end_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=25)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def command(arguments, timeout=CONTROL_TIMEOUT):
    """One public CLI invocation; stdout carries the JSON report when it succeeds."""
    return subprocess.run(
        [*CLI, *map(str, arguments)], capture_output=True, text=True, timeout=timeout, cwd=ROOT
    )


def control_command(root, action, record, *, force=False):
    arguments = [action, "--run", root, *(["--force"] if force else []), "--json"]
    result = command(arguments)
    record.setdefault("commands", []).append({"action": action, "exit_code": result.returncode})
    assert result.returncode == 0, f"almanak {action} failed: {result.stderr}"
    return json.loads(result.stdout)


def resume(root, record):
    """Resume and wait out the provisioning it starts.

    The supervisor refuses control commands while it provisions, and a reference that is
    already holding passes the next hold at once, so a teardown would race that provisioning.
    """
    control_command(root, "resume", record)
    await_stage(root, ("running",))


def start_run(case, operator_path, env_file, root, log):
    """`almanak run` follows its own workspace, so it stays in the background."""
    arguments = [
        "run",
        "--config",
        case.config,
        "--strategy",
        case.strategy,
        "--fork",
        "--gateway-config",
        operator_path,
        "--env-file",
        env_file,
        "--output",
        root,
        "--json",
    ]
    with log.open("a") as stream:
        return subprocess.Popen(
            [*CLI, *map(str, arguments)],
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            cwd=ROOT,
        )


def await_workspace(root, process):
    deadline = time.monotonic() + PHASE_TIMEOUT
    while time.monotonic() < deadline:
        if (root / "workspace.json").is_file():
            return
        if process.poll() is not None:
            raise AssertionError("almanak run exited before creating the workspace")
        time.sleep(0.5)
    raise AssertionError("almanak run did not create the supervised workspace")


def attempt(action):
    """Cleanup and retention must never replace the failure that is being reported."""
    try:
        action()
    except (OSError, RuntimeError, ValueError, AssertionError, subprocess.SubprocessError):
        return


def release(root, process):
    if (root / "workspace.json").is_file():
        attempt(lambda: control_command(root, "shutdown", {}, force=True))
        for entry in read_workspace(root)["processes"].values():
            attempt(lambda entry=entry: stop(entry))
    end_process(process)


def retain_supervised(root, artifact, record):
    from tests.strategy_fork.harness import retain_database

    attempt(lambda: retain_workspace(root, artifact))
    database = root / "state.sqlite"
    if database.is_file():
        attempt(lambda: retain_database(database, artifact, record))
    drive = root / "market-drive.json"
    if drive.is_file():
        # A scenario that moved the fork market says so in the case record itself.
        record["market_drive"] = json.loads(drive.read_text())
    (artifact / "result.json").write_text(json.dumps(record, indent=2) + "\n")


def hold(root, assertions, record, phase):
    state = await_checkpoint(root, assertions.entered)
    assertions.assert_holding(fork_handle(root), state)
    record["phases"].append(phase)
    return state


def drive_external(root, assertions):
    """Settle connector-owned requests against the live supervised workspace."""
    from tests.strategy_fork.harness import drive_external as drive

    # The supervisor can report running before the coordinator has created its
    # schema and initial state. External drivers load that state to find the
    # retained request, so wait for the first readable checkpoint explicitly.
    await_checkpoint(root, lambda state: True)
    drive(
        assertions,
        fork_handle(root),
        Journal(root / "state.sqlite"),
        WorkspaceCoordinator(root),
    )


def await_refusal(root):
    """The coordinator must stop mid-teardown; a completed close means the fault was bypassed."""
    database = root / "state.sqlite"
    deadline = time.monotonic() + PHASE_TIMEOUT
    while time.monotonic() < deadline:
        state = loaded_checkpoint(database)
        if state is not None:
            if state.mode == "closed":
                raise AssertionError("teardown closed; the injected fault was not exercised")
            coordinator = read_workspace(root)["processes"].get("coordinator")
            if state.mode == "teardown" and not running(coordinator):
                return state
        time.sleep(0.5)
    raise AssertionError("supervised teardown was not refused; inspect logs/coordinator.log")


def gateway_refusal(root):
    """Return the last structured gateway refusal emitted by the coordinator, if present."""
    path = root / "logs" / "coordinator.log"
    if not path.is_file():
        return None
    for line in reversed(path.read_text().splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            isinstance(value, dict)
            and set(value) == {"error", "message"}
            and all(isinstance(value[key], str) for key in value)
        ):
            return value
    return None


def supervise_reversal(root, record, follower, assertions, fault):
    """Hold, refuse a torn-down swap against replaced router code, then restore and close."""
    held = hold(root, assertions, record, "entered_and_held")
    control_command(root, "pause", record)
    record["run_exit_code"] = follower.wait(timeout=CONTROL_TIMEOUT)
    assert record["run_exit_code"] == 0, "almanak run reported a failed workspace"
    record["phases"].append("actual_reference_borrowed_and_traded")

    def refuse():
        resume(root, record)
        control_command(root, "teardown", record)
        failed = await_refusal(root)
        status = control(root, "status")
        refusal = {
            "refused_workspace_stage": status["stage"],
            "refused_diagnostic": status["diagnostic"],
        }
        gateway_error = gateway_refusal(root)
        if gateway_error is not None:
            refusal["refused_gateway_error"] = gateway_error
        return failed, refusal

    proof = fault(fork_handle(root), held, refuse)
    record["phases"].append("changed_router_refused_without_repayment_or_collateral_release")
    control_command(root, "resume", record)
    drive_external(root, assertions)
    closed = await_checkpoint(root, lambda state: state.mode == "closed")
    assertions.assert_closed(fork_handle(root), closed)
    proof["restored_router_completed_reference"] = True
    record["phases"].append("restored_exact_router_reversed_repaid_and_withdrew")
    record["phases"].append("closed")
    control_command(root, "shutdown", record)
    record["phases"].append("workspace_shutdown")


def supervise(case, root, record, follower):
    from tests.strategy_fork.harness import align_teardown, load_assertions

    assertions = load_assertions(case)
    await_workspace(root, follower)
    await_stage(root, ("running",))
    record["phases"].append("supervised_resolution_fixture_hooks_and_admission")
    drive_external(root, assertions)
    if (root / "market-drive.json").is_file():
        record["phases"].append("fork_market_driven_by_scenario")
    if supervised_fault is not None:
        supervise_reversal(root, record, follower, assertions, supervised_fault)
        return
    hold(root, assertions, record, "entered_and_held")
    control_command(root, "pause", record)
    record["run_exit_code"] = follower.wait(timeout=CONTROL_TIMEOUT)
    assert record["run_exit_code"] == 0, "almanak run reported a failed workspace"
    resume(root, record)
    hold(root, assertions, record, "paused_and_resumed")
    exercise = getattr(assertions, "exercise_hold", None)
    if exercise is not None:
        exercise(fork_handle(root), Journal(root / "state.sqlite"), WorkspaceCoordinator(root))
        record["phases"].append("timed_strategy_exit_verified")
    align_teardown(
        assertions,
        fork_handle(root),
        Journal(root / "state.sqlite"),
        WorkspaceCoordinator(root),
        record["phases"],
    )
    control_command(root, "teardown", record)
    drive_external(root, assertions)
    closed = await_checkpoint(root, lambda state: state.mode == "closed")
    assertions.assert_closed(fork_handle(root), closed)
    record["phases"].append("closed")
    control_command(root, "shutdown", record)
    record["phases"].append("workspace_shutdown")


def run_supervised_case(
    case, operator_path, env_file, temporary, artifacts, operator_mode="configured"
):
    """Drive one converted reference through the public supervised fork commands."""
    artifact = artifacts / (case.name.replace("/", "--") + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    # Pytest can reuse numbered temporary paths after cleanup or a reboot.
    # Durable database claims require a fresh path for each independent fixture.
    temporary = temporary / artifact.name
    temporary.mkdir(parents=True)
    root = temporary / "run"
    record = {
        "strategy": case.name,
        "operator_mode": operator_mode,
        "driver": "public_cli_supervised_fork",
        "phases": [],
        "status": "failed",
        "sha256": StrategyDefinition.read(case.strategy).sha256,
    }
    follower = start_run(case, operator_path, env_file, root, temporary / "almanak-run.log")
    try:
        supervise(case, root, record, follower)
        record["status"] = "passed"
    except BaseException as exc:
        record["failure_type"] = type(exc).__name__
        raise
    finally:
        release(root, follower)
        retain_supervised(root, artifact, record)
        print("Supervised strategy fork evidence:", artifact)
