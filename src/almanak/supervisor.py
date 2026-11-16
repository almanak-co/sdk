"""One local workspace supervisor. It never signs or coordinates economic state."""

import argparse
import hmac
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from almanak.gateway.auth import read_token
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_compatibility import UNSUPPORTED_ON_FORK
from almanak.supervisor_process import (
    SERVICE_READY_SECONDS,
    loopback_listener,
    running,
    spawn,
    stop,
    timing,
)
from almanak.worker_session import worker_timed_out
from almanak.workspace import (
    INVALID_CONFIG,
    TEARDOWN_REQUIRED,
    acquire,
    failure,
    needs_keystore_password,
    next_commands,
    read,
    save,
)

COORDINATOR_STOPPED = "Coordinator stopped; inspect its log and explicitly resume."
NOTHING_TO_TEAR_DOWN = (
    "workspace has no execution state (state.sqlite); nothing ran, so there is nothing to tear down"
)
# Refusals `Runtime.decide` raises in running mode from the recorded outcomes alone, so
# every resume repeats them; teardown mode passes them and closes what remains.
LATCHED_REFUSALS = frozenset({"execution_failed", "accounting"})
# A refused coordinator prints its error last; reading a bounded tail keeps status cheap.
FAILURE_TAIL_BYTES = 64 * 1024
FAILURE_MESSAGE_CHARS = 1000


def log_size(path):
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def coordinator_lines(root, offset):
    """The coordinator's log lines since `offset`, within a bounded tail, or None."""
    if offset is None:
        return None
    try:
        with (root / "logs" / "coordinator.log").open("rb") as log:
            end = log.seek(0, 2)
            log.seek(max(offset, end - FAILURE_TAIL_BYTES))
            return log.read().decode(errors="replace").splitlines()
    except OSError:
        return None


def documents(lines):
    """The JSON objects among `lines`, newest first."""
    for line in reversed(lines):
        try:
            document = json.loads(line)
        except ValueError:
            continue
        if isinstance(document, dict):
            yield document


def coordinator_failure(root, offset):
    """The failure kind and diagnostic for a stopped coordinator.

    A refused coordinator prints one `{"error": kind, "message": ...}` line before it
    exits (`cli.error_document`), and that refusal, such as an inventory shortfall or
    a gas policy limit, is what the author must act on. Only output written since this
    supervisor spawned the coordinator at `offset` is read, so an earlier run's error is
    never reported. Without one, the generic instruction to inspect the log remains.
    A refusal in `LATCHED_REFUSALS` repeats on every resume, so it is reported as
    `teardown_required` with teardown as the way on; every other stop is `execution`.
    """
    for document in documents(coordinator_lines(root, offset) or ()):
        if (
            "event" not in document
            and isinstance(document.get("error"), str)
            and isinstance(document.get("message"), str)
        ):
            kind, message = (
                document["error"],
                document["message"][:FAILURE_MESSAGE_CHARS].rstrip("."),
            )
            where = "logs/gateway.log" if kind == "invalid_request" else "logs/coordinator.log"
            if kind in LATCHED_REFUSALS:
                return TEARDOWN_REQUIRED, (
                    f"Coordinator stopped on {kind}: {message}. Details are in {where}; "
                    "a resume repeats this refusal, so teardown is required to close the "
                    "recorded positions."
                )
            return "execution", (
                f"Coordinator stopped on {kind}: {message}. "
                f"Details are in {where}; resolve the cause, then explicitly resume."
            )
    return "execution", COORDINATOR_STOPPED


# The policy field each fee-ceiling hold kind waits on (`runtime.CEILING_HOLDS`).
CEILING_FIELDS = {"gas_price": "max_gas_price_wei", "gas_fee_quote": "max_transaction_fee"}
# Coordinator events that end or supersede a fee-ceiling hold: it recovered, its decision
# was dropped, or the coordinator is now held on something else.
NOT_CEILING = (
    "gas_ceiling_recovered",
    "gas_ceiling_dropped",
    "transient_read_hold",
    "strategy_worker_timeout",
)


def ceiling_hold(root, offset):
    """The latest `gas_ceiling_hold` event, while nothing has ended or superseded it."""
    for document in documents(coordinator_lines(root, offset) or ()):
        event = document.get("event")
        if event in NOT_CEILING:
            return None
        if event == "gas_ceiling_hold" and document.get("kind") in CEILING_FIELDS:
            return document
    return None


def coordinator_hold(root, offset):
    """Why a running single-chain coordinator is holding on a fee ceiling, or None.

    The text names only the kind and the policy field, so it changes when the hold
    does, not with every quote; each quote is in the coordinator log.
    """
    hold = ceiling_hold(root, offset)
    if hold is None:
        return None
    return (
        f"Coordinator holding on {hold['kind']}: the market fee quote is above the gas "
        f"policy's {CEILING_FIELDS[hold['kind']]}. Nothing is signed while it holds; it "
        "quotes again on every attempt and proceeds once the quote is within the ceiling. "
        "Each quote is in logs/coordinator.log."
    )


class Supervisor:
    def __init__(self, root, password=None):
        self.password = password
        self.state_lock = threading.RLock()
        self.setup_active = False
        self.root = root
        self.lock = acquire(root)
        self.state = read(root)
        self.nodes = None
        self.server = None

    def persist(self, **updates):
        with self.state_lock:
            self.state.update(updates)
            save(self.root / "workspace.json", self.state)

    @property
    def deployment(self):
        return self.root / "private" / "deployment"

    @property
    def strategy_source(self):
        return self.root / self.state.get("strategy_file", "strategy.py")

    def start_setup(self, work=None):
        """Run provisioning, admission and service starts off the control request thread.

        The control server answers one request at a time, so this work must never run
        inside a request: production admission reads the full Roles history and can
        take minutes, and status polls queued behind it read as an unavailable
        supervisor. While `setup_active` holds, every control except status and pause
        is refused. A failure is persisted as the failed stage with its diagnostic.
        Setup never runs the coordinator, so the failure is marked `start`: launching
        again repeats only what any restart repeats, and a host may retry it unasked.
        A failure no retry can pass is marked final instead (`workspace.FINAL_FAILURES`).
        """
        with self.state_lock:
            if self.setup_active:
                raise ValueError("provisioning is already active")
            self.setup_active = True
        threading.Thread(target=self.setup_worker, args=(work or self.setup,), daemon=True).start()

    def setup_worker(self, work):
        started = time.monotonic()
        try:
            work()
        except Exception as exc:
            import traceback

            from almanak.runtime import config_refusal

            traceback.print_exc()
            # Hosts retry only a `start` failure; a contract the fork cannot execute,
            # or parameters the strategy's validate_config refused, refuse every retry
            # and resume the same way.
            diagnostic = str(exc)
            kind = "start"
            if isinstance(exc, DataError) and exc.kind == UNSUPPORTED_ON_FORK:
                kind = UNSUPPORTED_ON_FORK
            elif (refusal := config_refusal(exc)) is not None:
                kind, diagnostic = INVALID_CONFIG, refusal.summary()
            # Hosts keep retrying a `start` failure; `cause` only names a slow host.
            cause = "worker_timeout" if worker_timed_out(exc) else None
            self.persist(stage="failed", failure=kind, diagnostic=diagnostic, cause=cause)
        finally:
            self.password = None
            self.setup_active = False
            timing("setup_finished", started, stage=self.state["stage"])

    def prepared(self):
        from almanak.supervisor_preparation import PREPARED

        return self.state.get("configuration") in PREPARED

    def setup(self, *, resume=False, teardown=False):
        """Provision, admit and start services for a newly launched supervisor.

        Existing execution state stays paused unless this launch is itself a resume:
        `almanak resume` launches the supervisor when none answers, and no later
        resume control follows while this setup is active. A launch for teardown
        leaves it provisioned for the teardown control that follows, and never creates
        execution state: a teardown with none is refused before any service starts.
        """
        from almanak.supervisor_preparation import provision

        if teardown and not self.executed():
            raise ValueError(NOTHING_TO_TEAR_DOWN)
        if not self.prepared():
            raise ValueError("workspace has no prepared configuration; create it with almanak run")
        provision(self)
        if not (self.deployment / "deployment.json").exists():
            raise RuntimeError("deployment provisioning has not completed")
        if not self.admission():
            return
        gateway_url = self.state.get("gateway_url")
        self.start_gateway()
        if teardown:
            return
        if not self.executed():
            self.start_coordinator(create=True, admitted=True)
        elif not resume:
            self.persist(stage="paused")
        else:
            self.resume_execution(adoptable=self.state.get("gateway_url") == gateway_url)

    def resume_execution(self, *, adoptable):
        """Resume existing execution after a launch, adopting a coordinator that outlived it.

        A coordinator keeps the gateway URL it was started with, so it is adopted only
        while that gateway still serves. Otherwise it is stopped as a pause stops it and
        resumed against the current gateway.
        """
        coordinator = self.state["processes"].get("coordinator")
        if running(coordinator) and adoptable:
            # Under the state lock a pause either lands first and is honored here, or
            # finds the running stage recorded and stops the adopted coordinator.
            with self.state_lock:
                if not self.honor_pause():
                    self.persist(stage="running", diagnostic=None)
            return
        stop(coordinator, interrupt=True)
        self.start_coordinator(admitted=True)

    def start_resumed_setup(self):
        """Accept the resume a launch carries before the first status can be answered.

        Until then the workspace holds the stage the previous supervisor left, such as
        shutdown or failed, and hosts treat both as final. An earlier pause request is
        cleared as a resume control clears it; a pause arriving during setup still holds.
        """
        self.persist(pause_requested=False, stage="provisioned", diagnostic=None)
        self.start_setup(lambda: self.setup(resume=True))

    def executed(self):
        return (self.root / "state.sqlite").exists()

    def start_teardown_setup(self):
        """Report provisioned while the setup of a launch for teardown runs.

        `almanak teardown` sends its control only once this setup ends. Until then the
        workspace would hold the stage the previous supervisor left, such as failed or
        shutdown, which hosts treat as final, so a container started to tear a failed
        deployment down would end before the teardown is applied.
        """
        self.persist(stage="provisioned", diagnostic=None)
        self.start_setup(lambda: self.setup(teardown=True))

    def client(self):
        return GatewayClient(self.state["gateway_url"], self.deployment / "gateway-auth.token")

    def admission(self):
        from almanak.supervisor_preparation import checked_preparation

        if self.state["mode"] != "production":
            from almanak.supervisor_forks import fork_admission

            return fork_admission(self)
        prepared = checked_preparation(self.root).preparation.prepared
        if (self.root / "state.sqlite").exists():
            return True
        from almanak.connectors.composition import configuration_from_json
        from almanak.production_setup import admit

        config = configuration_from_json((self.deployment / "deployment.json").read_text())
        result = admit(
            config,
            Path(self.state["credentials"]["env_file"]),
            self.strategy_source,
            self.deployment / "permissions",
            prepared=prepared,
        )
        save(self.root / "preflight.json", result)
        if not result["ready"]:
            self.persist(stage="action_required", diagnostic=result["reason"])
        return result["ready"]

    def gateway_arguments(self, listen_fd):
        from almanak.supervisor_preparation import gateway_arguments

        return gateway_arguments(self, listen_fd)

    def start_gateway(self):
        from almanak.supervisor_preparation import checked_preparation

        checked_preparation(self.root)
        if running(self.state["processes"].get("gateway")):
            self.check_gateway()
            return
        if self.state["mode"] == "fork":
            self.verify_forks()
        with loopback_listener() as listener:
            port = listener.getsockname()[1]
            arguments = self.gateway_arguments(listener.fileno())
            self.require_password(arguments)
            try:
                process = spawn(
                    self.root, "gateway", arguments, password=self.password, listener=listener
                )
            finally:
                self.password = None
        self.persist(
            gateway_url=f"http://127.0.0.1:{port}",
            processes={**self.state["processes"], "gateway": process},
        )
        self.wait_gateway(process)

    def require_password(self, arguments):
        if self.state["mode"] == "production" and needs_keystore_password(self.root, self.state):
            if self.password is None:
                raise ValueError("gateway restart requires an interactive keystore password")
            arguments.append("--password-stdin")

    def check_gateway(self):
        from almanak.supervisor_preparation import checked_preparation

        client = self.client()
        try:
            config = client.configuration()
            record = checked_preparation(self.root)
            record.require_configuration(config, record.preparation.prepared.strategy)
        finally:
            client.close()

    def wait_gateway(self, process):
        started = time.monotonic()
        deadline = started + SERVICE_READY_SECONDS
        while time.monotonic() < deadline:
            try:
                self.check_gateway()
                timing("gateway_ready", started)
                return
            except DataError as error:
                # Only an unanswered gateway is awaited; a configuration it serves but
                # the workspace refuses fails the start at once.
                if error.kind != "gateway_unavailable":
                    raise
                if not running(process):
                    raise RuntimeError(
                        "gateway exited during start; inspect logs/gateway.log"
                    ) from None
                time.sleep(0.1)
        raise RuntimeError(
            f"gateway did not answer within {SERVICE_READY_SECONDS} seconds; "
            "inspect logs/gateway.log"
        )

    def verify_forks(self):
        """Read-only node identity; fork keys stay with the execution gateway."""
        from almanak.gateway.fixture_accounts import require_fixture_nodes
        from almanak.supervisor_preparation import retained

        fixture = self.deployment / "fixture"
        require_fixture_nodes(retained(self.root).fixture, fixture / "fork-routes.json")

    def honor_pause(self):
        if self.state.get("pause_requested", False):
            self.persist(stage="paused")
            return True
        return False

    def start_coordinator(self, *, create=False, admitted=False):
        """Start the coordinator after one admission.

        `admitted` is true only when the caller admitted in the same worker just
        before; production admission walks the full Roles history, so running it
        twice back to back doubled a first start. Every other path admits here.
        """
        if self.honor_pause():
            return
        if running(self.state["processes"].get("coordinator")):
            raise ValueError("coordinator is already running")
        if not admitted and not self.admission():
            return
        self.start_gateway()
        # Admission and the gateway start run in the setup worker, and a pause may
        # arrive meanwhile. The state lock makes the pause either land before this
        # check or find the spawned coordinator recorded, so it is never lost.
        with self.state_lock:
            if self.honor_pause():
                return
            self.spawn_coordinator(create)

    def spawn_coordinator(self, create):
        args = [
            "service",
            "start" if create else "resume",
            "--gateway-url",
            self.state["gateway_url"],
            "--gateway-auth-file",
            str(self.deployment / "gateway-auth.token"),
            "--export",
            str(self.root / "dashboard"),
        ]
        if self.state.get("clock_offset_file"):
            args.extend(["--clock-offset-file", str(self.root / self.state["clock_offset_file"])])
        if self.state.get("decision_facts") is True:
            args.append("--decision-facts")
        if create:
            args.extend(
                [
                    "--config",
                    str(self.deployment / "deployment.json"),
                    "--strategy",
                    str(self.strategy_source),
                ]
            )
        # The log is appended across runs; its failure is read only from this run's output.
        offset = log_size(self.root / "logs" / "coordinator.log")
        process = spawn(self.root, "coordinator", args)
        self.persist(
            stage="running",
            diagnostic=None,
            coordinator_log_offset=offset,
            processes={**self.state["processes"], "coordinator": process},
        )

    def status(self):
        from almanak.reports import retained

        facts = retained(self.root)
        active = running(self.state["processes"].get("coordinator"))
        if self.state["stage"] == "running" and not active:
            if facts.get("mode") == "closed":
                self.persist(stage="closed", failure=None, diagnostic=None)
            else:
                kind, diagnostic = coordinator_failure(
                    self.root, self.state.get("coordinator_log_offset")
                )
                self.persist(stage="failed", failure=kind, diagnostic=diagnostic)
        return {
            "workspace": str(self.root),
            "mode": self.state["mode"],
            "stage": self.state["stage"],
            "failure": failure(self.state),
            # Why a setup failed when it was a slow host, not the inputs.
            "cause": self.state.get("cause") if failure(self.state) == "start" else None,
            "supervisor": "available",
            "provisioning": self.setup_active,
            "coordinator": "running" if active else "stopped",
            "recorded": facts,
            "diagnostic": self.state.get("diagnostic") or self.hold(active),
            "next_commands": next_commands(self.root, self.state, facts),
        }

    def hold(self, active):
        # A hold is live state, never persisted: it can end on the coordinator's next tick.
        if not active or self.state["stage"] != "running":
            return None
        return coordinator_hold(self.root, self.state.get("coordinator_log_offset"))

    def control(self, action, force, password=None):
        if action == "status":
            return self.status()
        if password is not None:
            self.password = password
        if self.setup_active and action != "pause":
            raise ValueError("provisioning is active; wait for completion before this command")
        if action == "pause":
            self.persist(pause_requested=True)
            stop(self.state["processes"].get("coordinator"), interrupt=True)
            self.persist(stage="paused")
        elif action == "resume":
            self.resume()
        elif action == "teardown":
            if not self.executed():
                raise ValueError(NOTHING_TO_TEAR_DOWN)
            self.persist(pause_requested=False)
            self.start_gateway()
            client = self.client()
            try:
                client.teardown()
            finally:
                client.close()
            if not running(self.state["processes"].get("coordinator")):
                self.start_coordinator()
        elif action == "shutdown":
            self.shutdown(force)
        else:
            raise ValueError("unknown workspace control")
        return self.status()

    def resume(self):
        """Accept a resume and return; admission and service starts run in the setup worker.

        A refused resume leaves the pause request as it was. An accepted one reports the
        provisioned stage until the worker persists running, action_required or failed:
        a stale stage would misreport it, and status turns a leftover running stage
        without a live coordinator into failed, which a host treats as final.
        """
        if running(self.state["processes"].get("coordinator")):
            raise ValueError("coordinator is already running")
        self.persist(pause_requested=False)
        if not self.state.get("provisioned", False):
            self.start_setup()
            return
        create = not (self.root / "state.sqlite").exists()
        self.persist(stage="provisioned", diagnostic=None)
        self.start_setup(lambda: self.start_coordinator(create=create))

    def shutdown(self, force):
        from almanak.reports import retained

        facts = retained(self.root)
        if not force and facts.get("mode") != "closed" and not facts.get("closed", False):
            raise ValueError(
                "remaining or unverified obligations; use teardown before shutdown, or --force"
            )
        stop(self.state["processes"].get("coordinator"), interrupt=True)
        stop(self.state["processes"].get("gateway"))
        stop(self.state["processes"].get("read-gateway"))
        for name, process in self.state["processes"].items():
            # Fork nodes and the connector venues beside them (`fork_venues`).
            if name.startswith(("fork-", "venue-")):
                stop(process)
        # The server stops below and its port can be reused by another process, so the
        # recorded URL is withdrawn with the stage.
        self.persist(
            stage="shutdown",
            control_url=None,
            diagnostic="Fork nodes stopped; this fork cannot be resumed."
            if self.state["mode"] == "fork"
            else "Services stopped; obligations remain recorded.",
        )
        if self.server is None:
            raise RuntimeError("supervisor server is unavailable")
        threading.Thread(target=self.server.shutdown, daemon=True).start()


def handler(supervisor):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            try:
                expected = "Bearer " + read_token(supervisor.root / "control.token")
                if not hmac.compare_digest(self.headers.get("Authorization", ""), expected):
                    self.send_error(403)
                    return
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 4096 or self.path != "/control":
                    self.send_error(400)
                    return
                request = json.loads(self.rfile.read(length))
                if request.get("identity") != supervisor.state["identity"]:
                    raise ValueError("workspace identity mismatch")
                result = supervisor.control(
                    request["action"], request.get("force", False), request.get("password")
                )
            except Exception as exc:
                result = {"error": str(exc)}
            body = json.dumps(result).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--password-stdin", action="store_true")
    launched = parser.add_mutually_exclusive_group()
    launched.add_argument("--resume", action="store_true", help="resume existing execution state")
    launched.add_argument(
        "--teardown", action="store_true", help="set up for the teardown control that follows"
    )
    args = parser.parse_args()
    password = json.loads(__import__("sys").stdin.readline(65537)) if args.password_stdin else None
    root = args.run.resolve()
    supervisor = Supervisor(root, password)
    with HTTPServer(("127.0.0.1", 0), handler(supervisor)) as server:
        supervisor.server = server
        # Setup starts before the control URL is published, so the first status a
        # launcher reads already reports provisioning and, for a resume, its stage.
        if args.resume:
            supervisor.start_resumed_setup()
        elif args.teardown:
            supervisor.start_teardown_setup()
        else:
            supervisor.start_setup()
        supervisor.persist(control_url=f"http://127.0.0.1:{server.server_port}")
        server.serve_forever(poll_interval=0.2)


if __name__ == "__main__":
    main()
