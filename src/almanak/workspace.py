"""Authenticated local supervision; authoritative execution stays in the journal."""

import fcntl
import json
import os
import subprocess
import sys
import time

import httpx

from almanak.gateway.auth import read_token
from almanak.presentation import emit

# A relaunch for teardown waits for the new supervisor's setup, which refuses every
# control but pause; production admission of a workspace without execution state can
# take minutes. The whole command is held to RELAUNCH_BUDGET_SECONDS, so the wait ends
# early enough to leave the control its request timeout and the command's own start.
RELAUNCH_BUDGET_SECONDS = 600
CONTROL_RESERVE_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 35
# A supervisor that answered a shutdown keeps the workspace lock until its server's
# 0.2 second poll ends and the process exits; a repeated shutdown waits that long.
SHUTDOWN_SETTLE_SECONDS = 2
# A starting supervisor retries the lock past a shutdown probe's shared hold.
ACQUIRE_SECONDS = 1
LOCK_POLL_SECONDS = 0.05


def save(path, value):
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def read(root):
    return json.loads((root / "workspace.json").read_text())


def failure(state):
    """Why a failed workspace stopped, or None for any other stage.

    `start` is a setup failure before a coordinator ran; launching again repeats only
    what a restart does. `execution` is a coordinator that stopped, which needs an
    explicit resume. `teardown_required` is a coordinator that stopped on a recorded
    execution or accounting failure, which blocks new exposure on every resume: only a
    teardown moves it on. `unsupported_on_fork` is a fork setup refused because a contract
    the strategy uses has no EVM bytecode, and `invalid_config` is parameters the
    strategy's `validate_config` refused; neither a retry nor a resume can pass them.
    A failure recorded before the distinction reports None.
    """
    return state.get("failure") if state.get("stage") == "failed" else None


INVALID_CONFIG = "invalid_config"
TEARDOWN_REQUIRED = "teardown_required"
# Failures no resume can clear: the next step is to stop the workspace's services.
FINAL_FAILURES = frozenset({"unsupported_on_fork", INVALID_CONFIG})


def next_commands(root, state, facts=None):
    """The command that moves this workspace on; a resume only where one can help."""
    closed = facts is not None and facts.get("mode") == "closed"
    # A shut-down fork's nodes are gone, so only its recorded evidence remains to read.
    if state.get("stage") == "shutdown" and (closed or state.get("mode") == "fork"):
        return [f"almanak report --run {root}"]
    if closed or failure(state) in FINAL_FAILURES:
        return [f"almanak shutdown --run {root}"]
    if failure(state) == TEARDOWN_REQUIRED:
        return [f"almanak teardown --run {root}"]
    return [f"almanak resume --run {root}"]


def create(args):
    """Freeze the authored choices and operator binding, then launch the supervisor."""
    root = prepare(args)
    launch(root, password=credential_password(root))
    return root


def prepare(args, *, test_clock=False, overrides=None, fields=...):
    """Freeze the authored choices and operator binding before any process starts.

    `test_clock` gives a fork coordinator a clock offset that `almanak test run` advances
    together with the fork's own time; nothing else sets it. `overrides` replace declared
    `strategy.parameters` in the workspace's copy of the inputs; the source file is only read.
    `fields`, when given, are the typed parameter model's fields already read from strategy.py.
    """
    from almanak.checks import author_inputs, checked
    from almanak.configuration.preparation import check_preparation
    from almanak.connectors.support import RunMode
    from almanak.gateway.operator_config import read_operator_config
    from almanak.supervisor_preparation import create_workspace, selected_anchors

    stages: list[dict[str, str]] = []
    document, definition = checked(
        stages,
        "configuration",
        args.config or args.directory,
        lambda: author_inputs(args, overrides=overrides, fields=fields),
    )
    check_preparation(document, definition, stages=stages)
    operator = read_operator_config(args.gateway_config)
    mode = RunMode.PRODUCTION if args.production else RunMode.FORK
    anchors = selected_anchors(operator, mode, document.inputs)
    context = document.inputs.context(operator, mode, anchors)
    return create_workspace(
        args.output,
        document,
        definition,
        context,
        args.gateway_config,
        args.env_file,
        test_clock=test_clock,
    )


def launch(root, *, password=None, resume=False, teardown=False) -> subprocess.Popen:
    with (root / "logs" / "supervisor.log").open("a") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "almanak.supervisor",
                "--run",
                str(root),
                *(["--password-stdin"] if password is not None else []),
                *(["--resume"] if resume else []),
                *(["--teardown"] if teardown else []),
            ],
            stdin=subprocess.PIPE if password is not None else subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            cwd="/",
        )

    if password is not None:
        assert process.stdin is not None
        process.stdin.write((json.dumps(password) + "\n").encode())
        process.stdin.close()
    return process


class Unanswered(RuntimeError):
    """A control the workspace's supervisor did not answer, as one CLI error document.

    A supervisor refuses a control with HTTP 200 and an `error` body, which `request`
    raises as ValueError. This is a transport failure or any other HTTP status: an
    exited supervisor, a stale control URL or a rejected bearer token.
    """

    def __init__(self, root, action, cause: httpx.HTTPError):
        answered = cause.response if isinstance(cause, httpx.HTTPStatusError) else None
        # Timeouts and some connection failures carry an empty message.
        reason = (
            f"HTTP {answered.status_code}"
            if answered is not None
            else str(cause) or type(cause).__name__
        )
        self.document = {
            "error": "supervisor_unavailable",
            "workspace": str(root),
            "action": action,
            "message": f"the workspace supervisor is unavailable for {action} ({reason}); "
            f"inspect `almanak status --run {root}` and logs/supervisor.log",
        }
        # The supervisor's own 400 and 403 errors carry an HTML page and its refusals
        # arrive with HTTP 200, so a body here says nothing the status does not; it may
        # also be from whatever now listens on the port, so none is repeated.
        if answered is not None:
            self.document["status_code"] = answered.status_code
        super().__init__(self.document["message"])


def request(root, action, *, force=False, password=None):
    state = read(root)
    endpoint = state.get("control_url")
    if endpoint is None:
        if state.get("stage") == "shutdown":
            raise ValueError("workspace is shut down and has no ready supervisor")
        raise ValueError("supervisor is not ready; inspect status and logs")
    with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS, trust_env=False) as client:
        response = client.post(
            endpoint + "/control",
            headers={"Authorization": "Bearer " + read_token(root / "control.token")},
            json={
                "identity": state["identity"],
                "action": action,
                "force": force,
                "password": password,
            },
        )
        response.raise_for_status()
        result = response.json()
    if "error" in result:
        raise ValueError(result["error"])
    return result


def status(root):
    try:
        return request(root, "status")
    except (httpx.HTTPError, ValueError):
        return recorded_status(root)


def recorded_status(root):
    """The status a workspace's own files report when no supervisor answers."""
    from almanak.reports import retained

    state = read(root)
    facts = retained(root)
    return {
        "workspace": str(root),
        "stage": state["stage"],
        "failure": failure(state),
        # The reason a failed run stopped survives the supervisor that recorded it.
        "diagnostic": state.get("diagnostic"),
        "supervisor": "unavailable",
        "evidence": "recorded; no fresh chain audit",
        "recorded": facts,
        "next_commands": next_commands(root, state, facts),
    }


def wait_ready(root, *, replaced=None, launched=None):
    """Wait for the launched supervisor to answer, or for a stage it persisted to end setup.

    `replaced` is the workspace state read before relaunching a supervisor that no
    longer answered. Until the new one publishes its own control URL, the stage and
    URL on disk are that supervisor's and say nothing about this launch. The wait
    also ends once the `launched` process has exited.
    """
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if launched is not None and launched.poll() is not None:
            break
        state = read(root)
        if replaced is None or state.get("control_url") != replaced.get("control_url"):
            if state["stage"] in ("failed", "action_required"):
                return status(root)
            if state.get("control_url"):
                try:
                    return request(root, "status")
                except httpx.HTTPError:
                    pass
        time.sleep(0.2)
    return status(root)


def follow(root, args):
    try:
        emit(wait_ready(root), args)
        while True:
            result = followed(root, "status")
            if follow_finished(result):
                emit(result, args)
                return 1 if result.get("stage") == "failed" else 0
            time.sleep(1)
    except KeyboardInterrupt:
        emit(followed(root, "pause"), args)
        return 130


def followed(root, action):
    """The followed supervisor's answer, or the recorded status once it has shut down.

    Shutdown withdraws the control URL before the server stops, so a concurrent
    shutdown ends the follow as a refused or unanswered control.
    """
    try:
        return request(root, action)
    except (httpx.HTTPError, ValueError):
        if read(root).get("stage") != "shutdown":
            raise
        return recorded_status(root)


def follow_finished(result):
    return (
        result.get("coordinator") != "running"
        and not result.get("provisioning")
        and result.get("stage") not in ("created", "provisioning")
    )


def command(args):
    """Send one control and print the supervisor's status.

    `resume` returns once the supervisor accepts it, reporting `provisioning: true`
    while admission and service starts run in its background worker; `status`
    reports the outcome. Waiting here would hold the caller for the whole admission,
    which can take minutes.
    """
    root = create(args) if args.product == "run" else args.run.resolve()
    try:
        return send_control(root, args)
    except httpx.HTTPError as exc:
        raise Unanswered(root, args.product, exc) from exc


def send_control(root, args):
    if args.product == "run":
        return follow(root, args)
    if args.product == "status":
        emit(status(root), args)
        return 0
    if args.product == "shutdown" and shut_down(root):
        # A control URL left by an older supervisor may now belong to another process,
        # so the repeated shutdown reports the files without sending the token there.
        emit(recorded_status(root), args)
        return 0
    password, resumed = ensure_supervisor(root, args.product)
    if resumed is not None:
        emit(resumed, args)
        return 0
    result = request(root, args.product, force=getattr(args, "force", False), password=password)
    emit(result, args)
    return 0


def ensure_supervisor(root, action):
    if action not in ("resume", "teardown"):
        return None, None
    password = credential_password(root)
    try:
        request(root, "status")
    except (httpx.HTTPError, ValueError):
        deadline = time.monotonic() + RELAUNCH_BUDGET_SECONDS - CONTROL_RESERVE_SECONDS
        replaced = read(root)
        # A relaunch for resume carries the resume itself: the supervisor refuses every
        # other control while its setup runs, so none could follow here.
        process = launch(
            root,
            password=password,
            resume=action == "resume",
            teardown=action == "teardown",
        )
        result = wait_ready(root, replaced=replaced, launched=process)
        if process.poll() is not None:
            return surviving_supervisor(root, password)
        if action == "resume":
            return None, accepted_resume(root, result, replaced)
        wait_settled(root, process, deadline)
        return None, None
    return password, None


def surviving_supervisor(root, password):
    """Hand the control to the supervisor that kept the workspace after a relaunch exited.

    A replacement exits when it cannot take the workspace lock, which happens when the
    original supervisor only answered late. That supervisor then receives the control
    itself; a relaunch that exited while none answers is an error.
    """
    try:
        request(root, "status")
    except (httpx.HTTPError, ValueError):
        raise RuntimeError("relaunched supervisor exited; inspect logs/supervisor.log") from None
    return password, None


def accepted_resume(root, result, replaced):
    """The status of a relaunch that carried the resume, or the reason it was not accepted.

    Only the replacement's own status counts, which it answers from the control URL it
    published. action_required is an accepted resume waiting for the owner.
    """
    published = read(root).get("control_url") != replaced.get("control_url")
    if result.get("supervisor") == "unavailable" or not published:
        raise RuntimeError("relaunched supervisor did not answer; inspect logs/supervisor.log")
    if result.get("stage") == "failed":
        raise RuntimeError(
            "relaunched supervisor setup failed: "
            + (result.get("diagnostic") or "inspect logs/supervisor.log")
        )
    return result


def wait_settled(root, process, deadline):
    """Wait for a relaunched supervisor's setup to end so the following control is accepted.

    A failed status poll is retried while the launched supervisor lives. Once it has
    exited or the deadline passes, the control is sent and reports the outcome.
    """
    while time.monotonic() < deadline and process.poll() is None:
        try:
            if not request(root, "status").get("provisioning"):
                return
        except (httpx.HTTPError, ValueError):
            pass
        time.sleep(0.5)


def shut_down(root):
    """Whether the workspace is shut down with no supervisor holding it.

    A supervisor relaunched for a teardown holds the lock before it replaces the
    shutdown stage with its own, so the stage alone is not enough. Every supervisor
    holds the workspace lock for its whole life. Without a control URL the lock is
    awaited briefly, since the supervisor that shut down holds it until it exits. When
    this user cannot probe the lock, only a workspace with no URL to send to counts as
    shut down.
    """
    state = read(root)
    if state.get("stage") != "shutdown":
        return False
    unpublished = state.get("control_url") is None
    deadline = time.monotonic() + (SHUTDOWN_SETTLE_SECONDS if unpublished else 0)
    while (held := lock_held(root)) and time.monotonic() < deadline:
        time.sleep(LOCK_POLL_SECONDS)
    return unpublished if held is None else not held


def lock_held(root):
    """Whether a supervisor holds the workspace lock, or None when it cannot be probed.

    The probe takes a shared lock and releases it at once; a supervisor starting
    meanwhile retries its exclusive lock in `acquire`.
    """
    try:
        descriptor = (root / "supervisor.lock").open("a+")
    except OSError:
        return None
    with descriptor:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        except OSError:
            return None
    return False


def acquire(root):
    descriptor = (root / "supervisor.lock").open("a")
    deadline = time.monotonic() + ACQUIRE_SECONDS
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return descriptor
        except BlockingIOError:
            if time.monotonic() < deadline:
                time.sleep(LOCK_POLL_SECONDS / 2)
                continue
        except OSError:
            pass
        descriptor.close()
        raise RuntimeError("this workspace already has a supervisor") from None


def credential_password(root):
    from almanak.supervisor_process import running

    state = read(root)
    if (
        state["mode"] != "production"
        or not needs_keystore_password(root, state)
        or running(state["processes"].get("gateway"))
    ):
        return None
    import getpass

    password = getpass.getpass("Keystore password (not saved): ")
    if not password:
        raise ValueError("keystore password must not be empty")
    return password


def needs_keystore_password(root, state):
    """Only a keystore without an operator password file needs an ephemeral password.

    A remote signer holds its own key material and takes no password, so runs
    without a terminal must never reach the prompt.
    """
    from almanak.supervisor_preparation import operator_for

    operator, _ = operator_for(root)
    signing = operator.production.signing if operator.production is not None else None
    if signing is None:
        raise ValueError("operator has no production signing configuration")
    return signing.key_file is not None and signing.password_file is None
