"""Trusted runtime client. No provider credentials, private keys or local fallback."""

import json
import threading
import time
from contextlib import nullcontext
from pathlib import Path
from uuid import uuid4

import httpx

from almanak.connectors.composition import (
    checkpoint_from_json,
    configuration_from_json,
    connector_for,
)
from almanak.gateway.auth import loopback_endpoint, read_token
from almanak.gateway.ethereum import DataError
from almanak.models import Model
from almanak.state import lease_extended, mark_lease
from almanak.strategy import StrategyDefinition

# Creation generates and verifies Roles permissions inside one request. On a new workspace
# or after a lost checkpoint that is a full history walk, which grows with the Safe's age.
CREATE_TIMEOUT = 600.0
# Coordinator requests the gateway answers without the fenced lease commit.
UNFENCED = frozenset({"release", "teardown"})
# Read-only requests whose overrun fails only the decision that asked, as a hold.
TIMEOUT_HOLDS = frozenset({"prices"})
# Requests that only read, so an overrun leaves nothing half done. It stays a gateway
# failure, which never holds a decision, but is transient: the coordinator holds the whole
# tick, committing nothing, and asks again, as after a transient provider read.
READ_ONLY = frozenset({"configuration", "state", "observe", "audit", "receipt", "reconcile"})


def _decode(model, value):
    # Preserve Pydantic strict JSON semantics (JSON arrays represent typed tuples).
    return model.model_validate_json(json.dumps(value))


class Fenced:
    """This coordinator's own lease-fenced requests, as its lease renewal sees them.

    A broadcast sends while the gateway holds the journal's write lock, and a renewal
    cannot be written until that lock is released. The renewal therefore waits for such a
    request instead of counting the time it takes against the lease. A request the
    gateway could not serve at all is not an answer, so an unreachable gateway still
    ends the renewal once the lease has run out.

    Every fenced request counts, not only one that holds the lock: a running `observe`
    or `plan` also keeps the renewal asking, and a refusal that never reached the lock
    counts as an answer. The gateway still refuses an expired lease, so this only
    delays the report of a lease lost for another reason, by up to that request's
    length. A request this client stopped waiting for (its HTTP timeout) is not an
    answer either, although the gateway may still be sending under the lock; a strategy
    run ends on that failure itself, and resume recovers the recorded step.
    """

    def __init__(self):
        self.lock = threading.Lock()
        # When each running request started, by entry. Each thread keeps its own entries,
        # innermost last, so its exit removes the request it started.
        self.running: dict[int, float] = {}
        self.entries = 0
        self.local = threading.local()
        self.answered = 0
        # `answered` as the renewal last saw it.
        self.seen = 0

    def __enter__(self):
        with self.lock:
            self.entries += 1
            self.local.__dict__.setdefault("entries", []).append(self.entries)
            self.running[self.entries] = time.monotonic()

    def __exit__(self, kind, exc, traceback):
        with self.lock:
            del self.running[self.local.entries.pop()]
            if not (isinstance(exc, DataError) and exc.kind == "gateway_unavailable"):
                self.answered += 1

    def count(self) -> int:
        """Requests answered so far, read before a renewal is sent."""
        with self.lock:
            return self.answered

    def covered(self, answered: int) -> None:
        """A renewal served after `answered` requests covers them, and no later one.

        A request answered while the renewal was in flight may have extended the lease
        in a later commit than the renewal's own, so it still counts at the next failure.
        """
        with self.lock:
            self.seen = max(self.seen, answered)

    def progressed(self, deadline: float) -> bool:
        """Whether a request that started before `deadline` is running, or one was
        answered since this was last asked.

        The renewal asks when a failed attempt finds the lease run out, so a request
        answered while it waited for that attempt counts there, and counts once. Only a
        request already running at the deadline keeps the renewal waiting for it: one
        started later, against a gateway that answers nothing, does not prolong it.
        """
        with self.lock:
            progressed = (
                any(started < deadline for started in self.running.values())
                or self.answered != self.seen
            )
            self.seen = self.answered
            return progressed


class GatewayClient:
    def __init__(self, endpoint: str, token_file: Path):
        self.endpoint = loopback_endpoint(endpoint)
        self.token = read_token(token_file)
        self.http = httpx.Client(timeout=60, trust_env=False)
        self.coordinator: str | None = None
        self.deployment_id: str | None = None
        self.stopped = threading.Event()
        self.heartbeat: threading.Thread | None = None
        self.lease_error: Exception | None = None
        # When the gateway last answered a lease-fenced request successfully.
        self.lease_confirmed: float | None = None
        self.fenced = Fenced()

    def request(self, method: str, *, timeout: float | None = None, **fields):
        body = {
            key: value.model_dump(mode="json") if isinstance(value, Model) else value
            for key, value in fields.items()
        }
        try:
            response = self.http.post(
                self.endpoint + "/" + method,
                headers={"Authorization": "Bearer " + self.token},
                json=body,
                timeout=httpx.USE_CLIENT_DEFAULT if timeout is None else timeout,
            )
            result = response.json()
        except httpx.TimeoutException:
            if method in TIMEOUT_HOLDS:
                # A read-only request the gateway is still answering: it retains what it
                # reads, so the next decision finds it. Only this decision holds.
                raise DataError(
                    "timeout", f"{method}: gateway answer exceeded its budget"
                ) from None
            raise DataError(
                "gateway_unavailable",
                "gateway request failed; durable work is retained",
                transient=method in READ_ONLY,
            ) from None
        except (httpx.HTTPError, ValueError):
            raise DataError(
                "gateway_unavailable", "gateway request failed; durable work is retained"
            ) from None
        if response.status_code == 503:
            raise DataError(
                "gateway_unavailable", "gateway temporarily unavailable; durable work is retained"
            )
        if response.status_code != 200:
            refusal = DataError(
                result.get("error", "gateway_error"),
                result.get("message", "gateway refused request"),
                transient=result.get("transient") is True,
                details=result["details"] if isinstance(result.get("details"), dict) else None,
            )
            refusal.reverted = result.get("reverted") is True
            mark_lease(refusal, extended=result.get("lease_extended") is True)
            raise refusal
        return result["result"]

    def scoped(self, method, **fields):
        if self.lease_error is not None:
            cause = self.lease_error
            raise RuntimeError(
                "gateway coordinator heartbeat failed: the lease was not renewed and is "
                f"expired or lost ({type(cause).__name__}: {cause}); {self.lease_recovery()}"
            ) from cause
        try:
            with nullcontext() if method == "renew" or method in UNFENCED else self.fenced:
                result = self.request(
                    method, coordinator=self.coordinator, deployment_id=self.deployment_id, **fields
                )
        except Exception as exc:
            # A refusal counts only when the gateway says the commit that ended the request
            # extended the lease. A request that failed before any commit, a commit that
            # storage refused and a request that never reached the gateway extended nothing.
            if method not in UNFENCED and lease_extended(exc):
                self.confirm_lease()
            raise
        if method not in UNFENCED:
            self.confirm_lease()
        return result

    def confirm_lease(self) -> None:
        # The gateway extends the lease in the commit of every fenced request it
        # completes, for its full duration from the end of the request. The answer
        # follows the commit that releases the journal lock, so the lease is counted
        # from the answer, not from the start of a request that may have held the
        # lock for a whole send.
        # That keeps the lease counted if a renewal then fails transiently. It can
        # overstate the lease by the time the answer took to arrive, which only
        # decides how long a renewal is asked again: the gateway refuses an expired
        # lease whatever this clock says.
        answered = time.monotonic()
        self.lease_confirmed = max(self.lease_confirmed or answered, answered)

    def lease_recovery(self) -> str:
        """What the operator does about a lost lease."""
        return "resume to recover"

    def configuration(self):
        config = configuration_from_json(json.dumps(self.request("configuration")))
        self.connector = connector_for(config)
        return config

    def load(self):
        state = checkpoint_from_json(json.dumps(self.request("state")))
        self.connector = connector_for(state.config)
        if self.deployment_id is not None and self.deployment_id != state.deployment_id:
            raise ValueError("gateway changed deployment identity")
        return state

    def create(self, config, strategy: StrategyDefinition | None):
        # Creation uses server-provisioned configuration; callers cannot provision authority.
        if config != self.configuration():
            raise ValueError(
                "configuration changed; gateway requires its provisioned configuration"
            )

        # An operator deployment sends no strategy; the gateway's own start decides
        # whether it accepts that shape.
        fields = {} if strategy is None else {"strategy": strategy}
        # A create that outlives this wait still runs to completion in the gateway, and the
        # journal's exclusive database creation refuses any second create.
        state = checkpoint_from_json(
            json.dumps(self.request("create", timeout=CREATE_TIMEOUT, **fields))
        )
        if config != state.config:
            raise ValueError(
                "configuration changed; gateway requires its provisioned configuration"
            )

    def generate_permissions(self, config, strategy: StrategyDefinition):
        if config != self.configuration():
            raise ValueError("permission configuration differs from the dedicated gateway")
        self.connector = connector_for(config)
        return _decode(
            self.connector.permissions.manifest_type,
            self.request("permissions-generate", strategy=strategy),
        )

    def verify_permissions(self, manifest):
        return self.request("permissions-verify", manifest=manifest)

    def acquire(self):
        state = self.load()
        self.deployment_id = state.deployment_id
        self.coordinator = uuid4().hex
        admitted_at = time.monotonic()
        lease = self.scoped("acquire")
        # The admission itself is not a request the renewal waits for.
        self.fenced.covered(self.fenced.count())

        def renew():
            period = lease["lease_seconds"] / 3
            deadline = admitted_at + lease["lease_seconds"]
            delay = period

            def confirmed(deadline):
                # Read every time: a fenced request can confirm the lease while a renewal waits.
                if self.lease_confirmed is None:
                    return deadline
                return max(deadline, self.lease_confirmed + lease["lease_seconds"])

            while not self.stopped.wait(delay):
                started = time.monotonic()
                answered = self.fenced.count()
                deadline = confirmed(deadline)
                try:
                    # Asked even past the deadline: after a stall this clock cannot tell
                    # whether another coordinator was admitted. The gateway can, and it
                    # refuses a lease that was released or superseded.
                    self.scoped("renew")
                    deadline = started + lease["lease_seconds"]
                    delay = period
                    # Requests answered before this renewal was sent are covered by it.
                    self.fenced.covered(answered)
                except Exception as exc:
                    # Retry only temporary unavailability for this same lease, never admission
                    # under a new identity or signing/broadcasting an economic operation.
                    if isinstance(exc, DataError) and exc.kind == "gateway_unavailable":
                        deadline = confirmed(deadline)
                        remaining = deadline - time.monotonic()
                        if remaining > 0:
                            delay = min(period / 2, remaining)
                            continue
                        if self.fenced.progressed(deadline):
                            # This coordinator's own request was running when the lease ran
                            # out, or was answered while the renewal waited. It may hold the
                            # journal lock for a whole send, and the gateway extends the
                            # lease when it ends, also when it fails under the lock, so ask
                            # again.
                            delay = min(period / 2, 1.0)
                            continue
                    self.lease_error = exc
                    return

        self.heartbeat = threading.Thread(target=renew, daemon=True)
        self.heartbeat.start()

    def close(self):
        self.stopped.set()
        if self.heartbeat is not None:
            self.heartbeat.join(timeout=65)
        if self.coordinator is not None:
            try:
                self.request(
                    "release", coordinator=self.coordinator, deployment_id=self.deployment_id
                )
            except DataError:
                pass
        self.http.close()

    def update(self, change, *, chain=None):
        while True:
            state = self.load()
            updated = change(state)
            if updated == state:
                return state
            updated = self.connector.codec.checkpoint.model_validate_json(updated.model_dump_json())
            try:
                return _decode(
                    self.connector.codec.checkpoint,
                    self.scoped(
                        "update",
                        before=state,
                        after=updated,
                        **({"chain": chain} if chain is not None else {}),
                    ),
                )
            except DataError as exc:
                if exc.kind != "conflict":
                    raise

    def teardown(self):
        state = self.load()
        return _decode(
            self.connector.codec.checkpoint,
            self.request("teardown", deployment_id=state.deployment_id),
        )

    def observe(self):
        return tuple(
            _decode(self.connector.codec.snapshot, item) for item in self.scoped("observe")
        )

    def prices(self, bindings, cutoff):
        from almanak.gateway.strategy_prices import request_budget
        from almanak.price_series import PriceSeries

        if not bindings:
            return ()
        wire = [json.loads(binding.model_dump_json()) for binding in bindings]
        result = self.scoped(
            "prices",
            price_bindings=wire,
            cutoff=int(cutoff),
            timeout=request_budget(bindings),
        )
        return tuple(_decode(PriceSeries, item) for item in result)

    def audit_history(self, state):
        self.scoped("audit")

    def prepare(self, state, intent):
        return _decode(
            self.connector.codec.plan, self.scoped("plan", intent=intent, attempt_id=uuid4().hex)
        )

    def sign(self):
        return _decode(self.connector.codec.pending, self.scoped("sign"))

    def receipt(self, pending):
        return self.scoped("receipt")

    def broadcast(self, pending):
        return self.scoped("broadcast")

    def reconcile(self, pending, receipt):
        return _decode(self.connector.codec.outcome, self.scoped("reconcile"))

    def superseded(self, pending):
        from almanak.operator_deployment import ConsumedNonce

        return _decode(ConsumedNonce, self.scoped("superseded"))

    def family(self, pending, action, index):
        return _decode(
            self.connector.codec.checkpoint,
            self.scoped(
                "family", family_id=pending.family.identity, candidate_index=index, action=action
            ),
        )

    def request_reconcile(self):
        result = self.scoped("request-reconcile")
        return None if result is None else _decode(self.connector.codec.outcome, result)

    def external_reconcile(self):
        binding = self.connector.external_effects
        if binding is None:
            raise ValueError("connector has no external-effect binding")
        result = self.scoped("external-reconcile")
        return None if result is None else _decode(binding.page_type, result)

    def request_cancel_plan(self):
        result = self.scoped("request-cancel-plan", attempt_id=uuid4().hex)
        return None if result is None else _decode(self.connector.codec.plan, result)
