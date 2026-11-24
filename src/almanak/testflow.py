"""One supervised fork lifecycle with a JSON verdict: `almanak test run`.

The command drives the same supervised workspace `almanak run --fork` creates:
admission, up to N decisions with the fork's time advanced between them, an early
stop once an entry intent has executed, teardown until closed, then shutdown and the
release of the run's fund claims. With `--through-exit` the decisions continue after
the entry, so the strategy's own later intents (an LP mint after its entry swap,
fee collection, a recentre, its own close or sell) execute before teardown; the
verdict lists every intent kind that executed and those that executed after the
entry. Fork time moves by the author's decision interval
(or `--advance`, raised to the next due decision when shorter) through
`evm_setNextBlockTimestamp` and `evm_mine`; the coordinator's decision clock moves by
the same offset, so the author's interval is never edited.
"""

import json
import math
import signal
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import httpx

from almanak.testflow_evidence import teardown_decisions
from almanak.testflow_keepers import Keepers

ADMISSION_SECONDS = 1200
# With --through-exit, consecutive holds after the entry that end the lifecycle early.
IDLE_DECISIONS = 3
DECISION_SECONDS = 600
TEARDOWN_SECONDS = 900
# Teardown's wait when no decision settled. A coordinator that made no progress rarely
# closes, and DECISION_SECONDS plus the full TEARDOWN_SECONDS outlast a 20-minute
# lifecycle budget, so the verdict would never name the cause. Six STALL_SECONDS
# advances still fit, for a closure that waits on chain time.
NO_PROGRESS_TEARDOWN_SECONDS = 180
SETUP_SETTLE_SECONDS = 300
# Teardown without journal progress this long advances fork time by one step.
STALL_SECONDS = 30
POLL_SECONDS = 1.0
# Teardown ends as `teardown_refused` after this many exposure-increasing intents the
# coordinator refused with no teardown outcome between them.
TEARDOWN_REFUSALS = 3
EXPOSURE_REFUSED = "exposure_refused_in_teardown"
# Coordinator holds that retry without stopping (`runtime.TimeoutStreak` and
# `runtime.TickHolds`), each with the event that ends it: a strategy worker that timed out
# and a provider read the gateway marked transient. The coordinator logs one per attempt.
STALL_HOLDS = {
    "strategy_worker_timeout": "strategy_worker_recovered",
    "transient_read_hold": "transient_read_recovered",
}
# A stall hold the coordinator has kept this long ends the lifecycle with its kind. Its
# back-off doubles from 5 to 60 seconds, so by then it has failed about six consecutive
# attempts, the last ones a minute apart: a briefly starved host recovers well before,
# while waiting out DECISION_SECONDS would leave no lifecycle budget for teardown.
STALL_HOLD_SECONDS = 300
# Failure kinds whose cause is outside the strategy: the provider, the gateway, the local
# fork or its setup. Every other kind, including one not listed here, is a `strategy`
# failure, which running the same strategy again does not change. Only an `infra`
# failure whose source marked it transient is `transient`.
INFRA_KINDS = frozenset(
    {
        "rpc",
        "http",
        "transport",
        "unavailable",
        "gateway_unavailable",
        "timeout",
        "rate_limit",
        "fork_unavailable",
        "unsupported_on_fork",
        "admission_timeout",
        # The harness or a wall clock ended the run: a stop signal or a slow host. The
        # wall-time waits are transient; an interrupt is not.
        "interrupt",
        "decision_timeout",
        "teardown_timeout",
        "worker_timeout",
        *STALL_HOLDS,
    }
)
TRANSIENT_KINDS = frozenset(
    {"decision_timeout", "teardown_timeout", "worker_timeout", *STALL_HOLDS}
)


class Stopped(Exception):
    """The lifecycle cannot continue; the message names why.

    `kind` is its structured cause, the coordinator's refusal kind where it reported
    one, and `transient` and `reverted` whether that refusal was marked so. `details`
    are the refusal's structured facts, such as a teardown's remaining obligations.
    """

    def __init__(
        self,
        message: str,
        kind: str = "stopped",
        transient: bool = False,
        reverted: bool = False,
        details: dict | None = None,
    ):
        super().__init__(message)
        self.kind = kind
        self.transient = transient
        self.reverted = reverted
        self.details = details

    def record(self, verdict, stage) -> None:
        failure(verdict, stage, str(self), self.kind, self.transient, self.reverted, self.details)


def command(args) -> int:
    from almanak.presentation import emit

    started = time.monotonic()
    root = args.output.resolve()
    # Only a directory this run creates receives its verdict; an earlier run's stays intact.
    fresh = not root.exists()
    verdict = empty_verdict()
    process = keepers = None
    interrupted = False
    with interrupt_on_termination():
        try:
            process = start(args, root, verdict)
            if process is not None and admitted(root, process, verdict):
                keepers = Keepers(root, args.env_file)
                drive(root, args, verdict, keepers)
                step = args.advance or interval_or_none(root)
                bound = None if progressed(verdict) else NO_PROGRESS_TEARDOWN_SECONDS
                teardown(root, verdict, step, keepers, bound)
        except KeyboardInterrupt:
            failure(verdict, "interrupt", "interrupted; services are stopped without closure")
            interrupted = True
        except Exception as exc:  # noqa: BLE001 - reported in the verdict; cleanup still runs
            failure(verdict, "internal", f"{type(exc).__name__}: {exc}")
        finally:
            with ignored_interrupts():
                if interrupted and process is not None:
                    record_interrupted(root, verdict)
                if keepers is not None:
                    verdict["keepers"] = keepers.rows
                    for message in keepers.failures:
                        failure(verdict, "keeper", message)
                    keepers.close()
                cleanup(root, process, verdict)
    verdict["duration_seconds"] = round(time.monotonic() - started, 1)
    verdict["passed"] = passed(verdict)
    verdict["status"] = status(verdict)
    verdict["failure_class"] = failure_class(verdict)
    if fresh and root.is_dir():
        (root / "test-verdict.json").write_text(json.dumps(verdict, indent=2))
    emit(verdict, args)
    return 0 if verdict["passed"] else 1


def empty_verdict() -> dict:
    return {
        "schema": "almanak-test-verdict-v1",
        "workspace": None,
        "parameter_overrides": {},
        "parameter_sources": {},
        "admission": {"ok": False, "refusal": None},
        "decisions": [],
        "transactions": [],
        "entered": False,
        "executed": [],
        "after_entry": [],
        "teardown_executed": [],
        "teardown_sequence": [],
        "teardown_decisions": [],
        "in_flight": [],
        "repayment": [],
        "holdings": [],
        "keepers": [],
        "closed": False,
        "evidence_clean": False,
        "passed": False,
        "status": None,
        "failures": [],
        "failure_class": None,
        "duration_seconds": None,
    }


def passed(verdict) -> bool:
    return (
        verdict["admission"]["ok"]
        and verdict["closed"]
        and verdict["evidence_clean"]
        and not verdict["failures"]
    )


def status(verdict) -> str:
    """`entered` or `not_entered` for a passed lifecycle, else `failed`.

    A lifecycle that only held and then closed passes; this tells it apart from one
    whose entry executed, without changing `passed`.
    """
    if not verdict["passed"]:
        return "failed"
    return "entered" if verdict["entered"] else "not_entered"


def failure(
    verdict, stage, message, kind=None, transient=False, reverted=False, details=None
) -> None:
    """Record why the lifecycle stopped; `kind` defaults to the stage that failed."""
    entry: dict = {"stage": stage, "message": message}
    if details:
        entry["details"] = details
    verdict["failures"].append(classified(entry, kind or stage, transient, reverted))


def classified(entry: dict, kind: str, transient: bool = False, reverted: bool = False) -> dict:
    """`entry` with its structured `kind`, its `class` and whether it is `transient`.

    A contract revert keeps the generic `rpc` kind but is the strategy's call failing.
    """
    infra = kind in INFRA_KINDS and not reverted
    entry.update(
        {
            "kind": kind,
            "class": "infra" if infra else "strategy",
            "transient": infra and bool(transient),
        }
    )
    return entry


def failure_class(verdict) -> str | None:
    """The class of the first failure: the admission refusal, else the first failure."""
    refusal = verdict["admission"]["refusal"]
    if refusal is not None:
        return refusal["class"]
    return verdict["failures"][0]["class"] if verdict["failures"] else None


def start(args, root, verdict):
    """Create the workspace and launch its supervisor; a refused input is the admission."""
    from almanak.checks import selected_paths
    from almanak.cli import error_document
    from almanak.configuration.strategy_parameters import overrides
    from almanak.typed_parameters import model_fields
    from almanak.worker_session import worker_timed_out
    from almanak.workspace import launch, prepare

    release_orphaned_claims(root)
    try:
        given = overrides(args.param)
        config, strategy = selected_paths(args)
        fields = model_fields(strategy)
        defaults = {} if args.production_parameters else test_defaults(config, fields)
        requested = {**defaults, **given}
        prepared = prepare(args, test_clock=True, overrides=requested, fields=fields)
    except Exception as exc:
        document = error_document(exc)
        if document is None:
            raise
        slow = worker_timed_out(exc)
        kind = "worker_timeout" if slow else document.get("error", "invalid_input")
        verdict["admission"]["refusal"] = classified(
            document,
            kind,
            slow or document.get("transient") is True,
            document.get("reverted") is True,
        )
        return None
    verdict["workspace"] = str(root)
    verdict["parameter_overrides"] = applied(prepared, requested)
    verdict["parameter_sources"] = {
        key: "--param" if key in given else "test_parameters" for key in requested
    }
    return launch(root)


def test_defaults(config, fields) -> dict:
    """The config's `strategy.test_parameters`; a missing config is left to `prepare`."""
    from almanak.configuration.unresolved import read_inputs

    return read_inputs(config, fields=fields).test_parameters if config.is_file() else {}


def applied(root, requested) -> dict:
    """The overrides as the workspace froze them: the configured type, not the CLI text."""
    if not requested:
        return {}
    path = Path(root) / "private/author-inputs.json"
    strategy = json.loads(path.read_text()).get("strategy") or {}
    parameters = strategy.get("parameters") or {}
    return {key: parameters.get(key, value) for key, value in requested.items()}


def release_orphaned_claims(root) -> int:
    """Release the claims a killed earlier run left on this run's database path.

    `cleanup` releases them, but a run killed before it finishes cannot. A test worker
    reuses one output path and removes it before the next run, and the workspace refuses
    an existing root, so while the root is absent no deployment owns a database under
    it: its claims are orphaned and would refuse this run's journal at admission. Claims
    of any other database, and of a root that still exists, are kept.
    """
    from almanak.state import release_claims

    if root.exists():
        return 0
    return release_claims(root / "state.sqlite")


def admitted(root, process, verdict) -> bool:
    """Wait for provisioning, admission and the coordinator start."""
    deadline = time.monotonic() + ADMISSION_SECONDS
    while time.monotonic() < deadline:
        state = supervisor_status(root)
        if state.get("stage") == "running" and not state.get("provisioning"):
            verdict["admission"]["ok"] = True
            return True
        if state.get("stage") in ("failed", "action_required", "shutdown"):
            message = state.get("diagnostic") or "inspect logs/supervisor.log"
            # A preload worker that timed out on a slow host is not the inputs' failure.
            cause = state.get("cause")
            return refused(verdict, state["stage"], message, failure_kind(state), cause)
        if process.poll() is not None:
            message = "supervisor exited during setup; inspect logs/supervisor.log"
            return refused(verdict, "supervisor", message)
        time.sleep(POLL_SECONDS)
    message = f"admission did not finish within {ADMISSION_SECONDS} seconds"
    return refused(verdict, "timeout", message, kind="admission_timeout")


def failure_kind(state):
    from almanak.workspace import failure

    return failure(state)


def refused(verdict, stage, message, error=None, kind=None) -> bool:
    """Record why admission ended; `error` is the supervisor's structured failure kind.

    The refusal's `kind` is `kind` (a cause the supervisor named, or the condition that
    ended the wait), else that failure kind, else the stage it ended at.
    """
    refusal = {"stage": stage, "message": message}
    if error is not None:
        refusal["error"] = error
    kind = kind or error or stage
    verdict["admission"]["refusal"] = classified(refusal, kind, kind in TRANSIENT_KINDS)
    return False


def supervisor_status(root) -> dict:
    from almanak.workspace import read, request

    try:
        return request(root, "status")
    except (httpx.HTTPError, ValueError):
        return read(root)


def drive(root, args, verdict, keepers=None) -> None:
    previous = wire = None
    for index in range(args.decisions):
        try:
            if index:
                seconds = step(root, wire, args.advance or decision_interval(root))
                advance(root, keepers.step(wire, seconds) if keepers else seconds)
            wire = next_decision(root, previous, keepers)
        except Stopped as stop:
            stop.record(verdict, "decision")
            unsettled(root, previous, verdict)
            return
        previous = evaluated(wire)
        verdict["decisions"].append(with_facts(root, classify(wire, previous), wire))
        verdict["transactions"] = transactions(wire)
        verdict["executed"], verdict["after_entry"] = executed(wire)
        if entered(wire):
            verdict["entered"] = True
            if not getattr(args, "through_exit", False) or idle(verdict, args):
                return


def idle(verdict, args) -> bool:
    """Whether the entered strategy has held for its last `--idle` decisions.

    A strategy may hold while it waits on its own timer (a hold time before its sell, a
    collection interval), so a short run of holds continues; a longer one means it has
    nothing left to do before teardown.
    """
    count = getattr(args, "idle", IDLE_DECISIONS)
    recent = verdict["decisions"][-count:]
    return len(recent) == count and all(d["result"] == "hold" for d in recent)


def unsettled(root, previous, verdict) -> None:
    """Report the decision a stopped lifecycle left unsettled, with what it executed.

    A GMX order, for example, waits for a keeper that does not run on a fork.
    """
    from almanak.reports import checkpoint

    wire = checkpoint(root)
    last = evaluated(wire)
    if last is None or last == previous:
        return
    verdict["decisions"].append({**with_facts(root, classify(wire, last), wire), "settled": False})
    verdict["transactions"] = transactions(wire)
    verdict["executed"], verdict["after_entry"] = executed(wire)
    verdict["entered"] = entered(wire)


def progressed(verdict) -> bool:
    """Whether any decision settled: a coordinator that settled none made no progress."""
    return any(row.get("settled", True) for row in verdict["decisions"])


def interval_or_none(root) -> int | None:
    try:
        return decision_interval(root)
    except Stopped:
        return None


def decision_interval(root) -> int:
    from almanak.reports import checkpoint

    wire = checkpoint(root)
    if wire is None:
        raise Stopped("no execution state to read the decision interval from", "no_state")
    return int(wire["config"]["decision_trigger"]["seconds"])


def step(root, wire, requested: int) -> int:
    """Fork seconds to advance: `requested`, or more when the next decision is due later.

    A step shorter than the decision interval would otherwise leave the next decision
    to the wall clock, which reaches an hourly interval long after the decision wait.
    """
    due = None if wire is None else (wire.get("schedule") or {}).get("next_due_at")
    if due is None:
        return requested
    remaining = datetime.fromisoformat(due) - coordinator_now(root)
    return max(requested, math.ceil(remaining.total_seconds()))


def coordinator_now(root) -> datetime:
    from almanak.supervisor_preparation import CLOCK_OFFSET_FILE
    from almanak.triggers import offset_clock

    return offset_clock(root / CLOCK_OFFSET_FILE)()


def advance(root, seconds: int) -> None:
    """Move every fork node's time and the coordinator's decision clock by `seconds`."""
    from almanak.gateway.fork_fixture import Rpc
    from almanak.workspace import read

    for name, entry in read(root)["processes"].items():
        if name.startswith("fork-"):
            rpc = Rpc(entry["endpoint"])
            latest = int(rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
            rpc("evm_setNextBlockTimestamp", [latest + seconds])
            rpc("evm_mine")
    shift_clock(root, seconds)


def shift_clock(root, seconds: int) -> None:
    """Move the coordinator's decision clock with fork time moved by `seconds`."""
    from almanak.supervisor_preparation import CLOCK_OFFSET_FILE
    from almanak.triggers import read_offset
    from almanak.workspace import save

    path = root / CLOCK_OFFSET_FILE
    save(path, read_offset(path) + seconds)


def next_decision(root, previous, keepers=None):
    """The checkpoint once a decision after `previous` has settled all its work."""
    from almanak.reports import checkpoint

    deadline = time.monotonic() + DECISION_SECONDS
    while time.monotonic() < deadline:
        wire = checkpoint(root)
        last = evaluated(wire)
        if last is not None and last != previous and settled(wire):
            return wire
        require_coordinator(root)
        settle_requests(keepers, wire)
        time.sleep(POLL_SECONDS)
    # A hold the coordinator is still on, however brief, is the timeout's cause.
    require_no_hold(root)
    message = f"no settled decision within {DECISION_SECONDS} seconds"
    raise Stopped(message, "decision_timeout", transient=True)


def settle_requests(keepers, wire, *, stop=True) -> None:
    """Let a connector's fork keeper execute the accepted requests the run is waiting on.

    A decision cannot settle without it, so a failure stops the lifecycle. During
    teardown it is only recorded: closure still cancels the request once it expires.
    """
    from almanak.gateway.ethereum import DataError

    if keepers is None:
        return
    try:
        keepers.settle(wire)
    except (DataError, ValueError, RuntimeError, httpx.HTTPError) as exc:
        message = f"fork keeper could not settle a request: {exc}"
        if stop:
            kind = exc.kind if isinstance(exc, DataError) else "keeper"
            reverted = isinstance(exc, DataError) and exc.reverted
            raise Stopped(
                message, kind, isinstance(exc, DataError) and exc.transient, reverted
            ) from exc
        keepers.failures.append(message)


def evaluated(wire) -> str | None:
    if wire is None:
        return None
    return (wire["schedule"]["last_evaluation"] or {}).get("started_at")


def settled(wire) -> bool:
    # A multi-chain checkpoint keeps each chain's decision, plan and attempt in its work.
    scopes = (wire, *(wire.get("work") or ()))
    return not any(scope.get(name) for scope in scopes for name in ("pending", "plan", "decision"))


def require_coordinator(root, since=None) -> None:
    """Stop unless the coordinator runs and holds, in what it logged after `since`, on no
    fee ceiling and on no stall hold for STALL_HOLD_SECONDS."""
    from almanak.supervisor import ceiling_hold
    from almanak.supervisor_process import running
    from almanak.workspace import read

    state = read(root)
    if running(state["processes"].get("coordinator")):
        # A production coordinator waits out a fee quote above its ceiling, but fork gas
        # need not fall within a test run, so the lifecycle stops with the reason at once.
        offset = log_offset(state, since)
        hold = ceiling_hold(root, offset)
        if hold is not None:
            raise Stopped(f"coordinator holding on {hold['kind']}: {hold['reason']}", hold["kind"])
        require_unstalled(root, offset, STALL_HOLD_SECONDS)
        return
    line = last_error(root)
    refusal = logged_refusal(line)
    kind = refusal.get("error") if isinstance(refusal.get("error"), str) else None
    details = refusal.get("details")
    raise Stopped(
        "coordinator stopped: " + line,
        kind or "coordinator_stopped",
        refusal.get("transient") is True,
        refusal.get("reverted") is True,
        details if isinstance(details, dict) else None,
    )


def log_offset(state, since=None) -> int | None:
    """Where this coordinator's log starts, or `since` when that is later."""
    offset = state.get("coordinator_log_offset")
    if offset is not None and since is not None:
        offset = max(offset, since)
    return offset


def require_no_hold(root, since=None) -> None:
    """At a wait's timeout, stop on the stall hold the coordinator is still on, however
    brief: it names the timeout's cause. Without a workspace there is no evidence."""
    from almanak.workspace import read

    state = read(root) if (Path(root) / "workspace.json").exists() else {}
    require_unstalled(root, log_offset(state, since), 0)


def require_unstalled(root, offset, seconds) -> None:
    """Stop once the coordinator has held on any stall hold for `seconds`; the latest
    such hold names the stop."""
    due = [h for h in stall_holds(root, offset) if (h.get("elapsed_seconds") or 0) >= seconds]
    if not due:
        return
    hold = due[0]
    event = hold["event"]
    reason = hold.get("reason") or f"strategy worker {hold.get('kind')} timeout"
    raise Stopped(
        f"coordinator holding on {event} for {hold.get('elapsed_seconds')} seconds "
        f"({hold.get('attempt')} consecutive attempts): {reason}",
        event,
        transient=True,
        details={key: hold.get(key) for key in ("kind", "attempt", "elapsed_seconds")},
    )


def stall_holds(root, offset) -> list[dict]:
    """The latest hold of each stall kind logged after `offset` that its recovery has not
    ended, newest first."""
    from almanak.supervisor import coordinator_lines, documents

    recovered = {end: hold for hold, end in STALL_HOLDS.items()}
    ended: set[str] = set()
    holds: dict[str, dict] = {}
    for document in documents(coordinator_lines(root, offset) or ()):
        event = document.get("event")
        if event in recovered:
            ended.add(recovered[event])
        elif event in STALL_HOLDS and event not in ended:
            holds.setdefault(event, document)
    return list(holds.values())


def last_error(root) -> str:
    """The last refusal the coordinator logged, which names why it stopped."""
    path = root / "logs" / "coordinator.log"
    lines = path.read_text(errors="replace").splitlines() if path.exists() else []
    errors = (line for line in reversed(lines) if line.startswith('{"error"'))
    return next(errors, "inspect logs/coordinator.log")


def logged_refusal(line: str) -> dict:
    """The coordinator's logged error document, or an empty one for any other line."""
    try:
        document = json.loads(line)
    except ValueError:
        return {}
    return document if isinstance(document, dict) else {}


def classify(wire, started_at) -> dict:
    kinds = []
    decisions = [o.get("decision") for o in wire.get("outcomes", ())]
    decisions.append(wire.get("decision"))
    for decision in decisions:
        if decision and decision["trigger"].get("started_at") == started_at:
            kind = (decision.get("intent") or {}).get("kind")
            if kind and kind not in kinds:
                kinds.append(kind)
    if not kinds:
        return {"time": started_at, "result": "hold"}
    return {"time": started_at, "result": "intent", "intents": kinds}


def with_facts(root, decision, wire=None) -> dict:
    """`decision` with what the coordinator logged it knew when it evaluated it.

    Whether the strategy ran, the allocations observed, each declared price series'
    completeness and the chains not ready: facts a hold's cause can be checked against.
    Where several chains logged the row's time, an intent's own chain selects its facts.
    A coordinator that logged none leaves the row as it was.
    """
    from almanak.decision_facts import logged, matching

    candidates = logged(root / "logs" / "coordinator.log").get(decision["time"], [])
    return {**decision, **matching(candidates, intent_chain(wire, decision["time"]))}


def intent_chain(wire, started_at):
    """The chain of the intent the strategy proposed at `started_at`, if it proposed one."""
    if wire is None:
        return None
    decisions = [o.get("decision") for o in wire.get("outcomes", ())]
    decisions += [wire.get("decision"), *(w.get("decision") for w in wire.get("work") or ())]
    for decision in decisions:
        if decision and decision.get("trigger", {}).get("started_at") == started_at:
            target = (decision.get("intent") or {}).get("target") or {}
            chain = (target.get("fund") or {}).get("chain")
            if chain is not None:
                return chain
    return None


def entered(wire) -> bool:
    """Whether an intent that increases exposure has executed completely.

    The intent's last transaction succeeded and completed it; an approval alone, an
    intent abandoned for teardown, or a request its keeper cancelled, is not entry.
    """
    from almanak.connectors.composition import connector_for

    state = decoded(wire)
    lifecycle = connector_for(state.config).lifecycle
    return any(
        completed(outcome) and lifecycle.increases_exposure(outcome.decision.intent)
        for outcome in state.outcomes
    )


def executed(wire) -> tuple[list[str], list[str]]:
    """Intent kinds the strategy executed completely, in first-execution order, and
    those among them that executed after its first entry. Teardown's are not included."""
    kinds, after, _ = executions(decoded(wire))
    return kinds, after


def executions(state) -> tuple[list[str], list[str], list[str]]:
    """`executed`'s two lists, then the kinds teardown executed completely."""
    from almanak.connectors.composition import connector_for

    kinds: list[str] = []
    after: list[str] = []
    teardown: list[str] = []
    entry = False
    for outcome in state.outcomes:
        if not completed(outcome):
            continue
        intent = outcome.decision.intent
        kind = getattr(intent, "kind", type(intent).__name__)
        if by_teardown(outcome):
            remember(teardown, kind)
            continue
        remember(kinds, kind)
        if entry:
            remember(after, kind)
        entry = entry or connector_for(state.config).lifecycle.increases_exposure(intent)
    return kinds, after, teardown


def remember(kinds: list[str], kind: str) -> None:
    if kind not in kinds:
        kinds.append(kind)


def by_teardown(outcome) -> bool:
    trigger = getattr(outcome.decision, "trigger", None)
    return getattr(trigger, "kind", None) == "teardown"


def decoded(wire):
    from almanak.state import decode_checkpoint

    return decode_checkpoint(json.dumps(wire))


def evidence_clean(state) -> bool:
    if state.integrity_error or state.pending or state.plan:
        return False
    if len(state.outcomes) != state.delivered:
        return False
    if any(getattr(outcome, "accounting_issues", ()) for outcome in state.outcomes):
        return False
    return not any(work.fault or work.pending or work.plan for work in state.work)


def completed(outcome) -> bool:
    """Whether `outcome` finished its intent: its transaction and its result succeeded.

    A connector's result can fail a mined transaction, as GMX does for a request its
    keeper cancelled: the strategy entered nothing, so it is not an execution.
    """
    if getattr(outcome, "decision", None) is None or not getattr(outcome, "success", False):
        return False
    result = outcome.strategy_result()
    return bool(result.success and result.complete)


def transactions(wire) -> list[dict]:
    rows = []
    for outcome in wire.get("outcomes", ()):
        if not outcome.get("tx_hash"):
            continue
        receipt = outcome.get("receipt") or {}
        operation = outcome.get("operation") or {}
        profile = ((outcome.get("execution") or {}).get("binding") or {}).get("profile") or {}
        roles = str(profile.get("roles_module") or "").lower()
        trigger = ((outcome.get("decision") or {}).get("trigger") or {}).get("kind")
        rows.append(
            {
                "trigger": "teardown" if trigger == "teardown" else "decision",
                "action": operation.get("kind"),
                "allocation": (operation.get("target") or {}).get("allocation"),
                "status": "success" if outcome.get("success") else "failed",
                "gas_used": int(receipt["gasUsed"], 16) if receipt.get("gasUsed") else None,
                "via_roles": bool(roles) and str(receipt.get("to") or "").lower() == roles,
                "transaction": outcome["tx_hash"],
            }
        )
    return rows


def teardown(root, verdict, step=None, keepers=None, bound=None) -> None:
    """Request teardown and wait for closure, at most `bound` seconds when given."""
    from almanak.reports import checkpoint
    from almanak.workspace import request

    # Teardown's own requests are created when it starts; let a keeper choose that time.
    seconds = keepers.step(checkpoint(root), 0) if keepers else 0
    if seconds:
        advance(root, seconds)
    # A hold the coordinator logged before teardown is not the closure's.
    since = coordinator_log_end(root)
    try:
        request(root, "teardown")
    except (httpx.HTTPError, ValueError) as exc:
        failure(verdict, "teardown", f"teardown was not accepted: {exc}")
        return
    try:
        await_closure(root, step, keepers, since, bound)
    except Stopped as stop:
        stop.record(verdict, "teardown")
    record_closure(root, verdict)
    verdict["teardown_decisions"] = teardown_decisions(root, verdict)


def coordinator_log_end(root) -> int | None:
    """The coordinator log's current size, or None without a workspace to read it from."""
    from almanak.supervisor import log_size

    if root is None:
        return None
    return log_size(Path(root) / "logs" / "coordinator.log")


def await_closure(root, step, keepers=None, since=None, bound=None) -> None:
    """Wait for closure, advancing fork time whenever the journal stops progressing.

    Some closures wait on chain time: a GMX order can be cancelled only after its
    expiration, and an idle fork's clock does not move. Time never moves while a
    decision awaits its plan: the gateway re-prepares the plan against the chain
    before reserving it, and a mined block in between refuses it as stale. `bound`
    shortens the wait after a lifecycle that settled no decision.
    """
    from almanak.reports import checkpoint

    limit = TEARDOWN_SECONDS if bound is None else bound
    deadline = time.monotonic() + limit
    last, quiet = None, time.monotonic()
    outcomes, settled_at = None, since
    while time.monotonic() < deadline:
        wire = checkpoint(root)
        if wire is not None and wire.get("mode") == "closed":
            return
        require_coordinator(root, since)
        count = len((wire or {}).get("outcomes", ()))
        if outcomes is not None and count != outcomes:
            settled_at = coordinator_log_end(root)
        outcomes = count
        require_no_refusals(root, settled_at)
        settle_requests(keepers, wire, stop=False)
        if progress(wire) != last:
            last, quiet = progress(wire), time.monotonic()
        elif step and time.monotonic() - quiet >= STALL_SECONDS and not planning(wire):
            advance(root, keepers.step(wire, step) if keepers else step)
            quiet = time.monotonic()
        time.sleep(POLL_SECONDS)
    require_no_hold(root, since)
    message = f"not closed within {limit} seconds"
    if bound is not None:
        message += ", the wait bounded because no decision settled"
    raise Stopped(message, "teardown_timeout", True)


def require_no_refusals(root, offset) -> None:
    """Stop teardown once the strategy keeps proposing exposure teardown refuses.

    The coordinator drops each such decision and the strategy decides again; without a
    teardown outcome in between, waiting longer only repeats the refusal.
    """
    from almanak.supervisor import coordinator_lines, documents

    refusals = [
        document
        for document in documents(coordinator_lines(root, offset) or ())
        if document.get("event") == EXPOSURE_REFUSED
    ]
    if len(refusals) < TEARDOWN_REFUSALS:
        return
    latest = refusals[0]
    intent, allocation = latest.get("intent"), latest.get("allocation")
    raise Stopped(
        f"teardown refused {len(refusals)} consecutive {intent} intents for allocation "
        f"{allocation}: teardown admits no intent that increases exposure",
        "teardown_refused",
        details={"intent": intent, "allocation": allocation, "refusals": len(refusals)},
    )


def planning(wire) -> bool:
    """Whether a decision is being planned: retained with no plan, attempt or request yet.

    A decision whose own request is unresolved (a GMX order awaiting its expiration)
    waits on chain time, so it does not count; executed or cancelled requests, and those
    of another decision or chain, exempt nothing.
    """
    if wire is None:
        return False
    awaiting = {
        request.get("decision_identity")
        for request in wire.get("requests") or ()
        if request.get("decision_identity")
        and ((request.get("events") or [{}])[-1]).get("status") in ("accepted", "frozen")
    }
    units = [wire, *(wire.get("work") or ())]
    return any(
        unit.get("decision")
        and not unit.get("plan")
        and not unit.get("pending")
        and unit["decision"].get("identity") not in awaiting
        for unit in units
    )


def progress(wire) -> str:
    if wire is None:
        return ""
    fields = ("mode", "pending", "plan", "decision", "requests", "work")
    return json.dumps(
        [len(wire.get("outcomes", ())), *(wire.get(name) for name in fields)], sort_keys=True
    )


def record_closure(root, verdict, wire=None) -> None:
    from almanak.reports import checkpoint
    from almanak.testflow_evidence import holdings, teardown_sequence

    wire = checkpoint(root) if wire is None else wire
    if wire is None:
        return
    verdict["transactions"] = transactions(wire)
    state = decoded(wire)
    verdict["executed"], verdict["after_entry"], verdict["teardown_executed"] = executions(state)
    verdict["teardown_sequence"] = teardown_sequence(state)
    # The latest measured debt against what the fund can repay it from, per allocation.
    verdict["repayment"] = [row.model_dump(mode="json") for row in getattr(state, "repayment", ())]
    verdict["closed"] = state.mode == "closed"
    verdict["evidence_clean"] = evidence_clean(state)
    verdict["holdings"] = holdings(root, state)


def record_interrupted(root, verdict) -> None:
    """Record what the journal holds when an interrupt cut the lifecycle short.

    An interrupt can land during teardown, after `drive` last wrote the transactions;
    the journal still holds every teardown transaction and the intent left in flight.
    """
    from almanak.reports import checkpoint

    try:
        wire = checkpoint(root)
        if wire is None:
            return
        record_closure(root, verdict, wire)
        verdict["entered"] = verdict["entered"] or entered(wire)
        verdict["in_flight"] = in_flight(wire)
        verdict["teardown_decisions"] = teardown_decisions(root, verdict)
    except Exception as exc:  # noqa: BLE001 - the interrupt stays the recorded failure
        failure(verdict, "interrupt", f"journal unreadable at interrupt: {exc}")


def in_flight(wire) -> list[dict]:
    """Each decision the journal retains unsettled: its intent, trigger and transaction."""
    if wire is None:
        return []
    rows = []
    for unit in (wire, *(wire.get("work") or ())):
        decision = unit.get("decision")
        if not decision:
            continue
        intent = decision.get("intent") or {}
        rows.append(
            {
                "trigger": (decision.get("trigger") or {}).get("kind"),
                "intent": intent.get("kind"),
                "allocation": (intent.get("target") or {}).get("allocation"),
                "transaction": (unit.get("pending") or {}).get("tx_hash"),
            }
        )
    return rows


def cleanup(root, process, verdict) -> None:
    """Stop every service the workspace recorded, the supervisor and the run's claims."""
    from almanak.state import release_claims

    if process is None:
        return
    settle_setup(root, process)
    try:
        shutdown(root, force=not verdict["closed"])
    except (httpx.HTTPError, ValueError) as exc:
        failure(verdict, "shutdown", f"supervisor shutdown failed: {exc}")
    stop_recorded(root, verdict)
    stop_supervisor(process)
    # Fork funds are claimed under their node's own anvil environment; once the node
    # stops no other deployment can contend for them, closed or not.
    release_claims(root / "state.sqlite")


def settle_setup(root, process) -> None:
    """Shutdown is refused while setup runs; a setup may be starting fork nodes."""
    deadline = time.monotonic() + SETUP_SETTLE_SECONDS
    while time.monotonic() < deadline and process.poll() is None:
        if not supervisor_status(root).get("provisioning"):
            return
        time.sleep(POLL_SECONDS)


def shutdown(root, *, force) -> None:
    from almanak.workspace import request

    if not force:
        try:
            request(root, "shutdown")
            return
        except ValueError:
            pass
    request(root, "shutdown", force=True)


def stop_recorded(root, verdict) -> None:
    from almanak.supervisor_process import running, stop
    from almanak.workspace import read

    for name, record in read(root)["processes"].items():
        if running(record):
            try:
                stop(record, interrupt=name == "coordinator")
            except RuntimeError as exc:
                failure(verdict, "shutdown", f"{name}: {exc}")


def stop_supervisor(process) -> None:
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


@contextmanager
def interrupt_on_termination():
    """SIGTERM ends the test as Ctrl-C does, so cleanup still runs."""

    def interrupt(signum, frame):
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGTERM, interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


@contextmanager
def ignored_interrupts():
    """Cleanup itself is not interrupted; a second Ctrl-C would leave services behind."""
    previous = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
