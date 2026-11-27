"""A deployment-scoped HTTP gateway. Workflow progression stays in the runtime."""

import hmac
import json
import socket
import sqlite3
import sys
import time
from contextlib import nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field, ValidationError

from almanak.backtest.pricing import PriceBinding
from almanak.backtest.types import UnixTime
from almanak.chains import ChainAdapter, configured_chains
from almanak.connectors.composition import connector_for
from almanak.gateway.auth import read_token
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.gateway.fenced import Writes
from almanak.gateway.request_scope import request_scope
from almanak.gateway.routing import ChainRouter, backend_for
from almanak.gateway.scoped_service import ROUTED, dispatch_chain, validate_global_update
from almanak.gateway.strategy_prices import StrategyPriceSource
from almanak.models import Chain, Model, SafeRoles
from almanak.operator_deployment import (
    accounting_blocked,
    accounting_refusal,
    adopts,
    is_operator,
    request_executed,
)
from almanak.state import Journal, Lease, decode_checkpoint, lease_extended, mark_lease
from almanak.strategy import StrategyDefinition
from almanak.transaction_family import is_unsent

MAX_REQUEST_BYTES = 8 * 1024 * 1024


class Request[Intent, Checkpoint, Manifest](Model):
    chain: Chain | None = None
    coordinator: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")] | None = None
    deployment_id: str | None = None
    strategy: StrategyDefinition | None = None
    intent: Intent | None = None
    attempt_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")] | None = None
    before: Checkpoint | None = None
    after: Checkpoint | None = None
    manifest: Manifest | None = None
    family_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")] | None = None
    candidate_index: Annotated[int, Field(ge=0)] | None = None
    action: Literal["inspect", "reserve", "sign", "attempt", "abandon"] | None = None
    price_bindings: tuple[PriceBinding, ...] | None = None
    cutoff: UnixTime | None = None


FIELDS = {
    "configuration": set(),
    "permissions-generate": {"strategy"},
    "permissions-verify": {"manifest"},
    "create": {"strategy"},
    "state": set(),
    "teardown": {"deployment_id"},
    "acquire": {"coordinator", "deployment_id"},
    "release": {"coordinator", "deployment_id"},
    "renew": {"coordinator", "deployment_id"},
    "update": {"coordinator", "deployment_id", "before", "after"},
    "observe": {"coordinator", "deployment_id"},
    "prices": {"coordinator", "deployment_id", "price_bindings", "cutoff"},
    "audit": {"coordinator", "deployment_id"},
    "plan": {"coordinator", "deployment_id", "intent", "attempt_id"},
    "sign": {"coordinator", "deployment_id"},
    "receipt": {"coordinator", "deployment_id"},
    "broadcast": {"coordinator", "deployment_id"},
    "reconcile": {"coordinator", "deployment_id"},
    "request-reconcile": {"coordinator", "deployment_id"},
    "request-cancel-plan": {"coordinator", "deployment_id", "attempt_id"},
    "external-reconcile": {"coordinator", "deployment_id"},
    "family": {"coordinator", "deployment_id", "family_id", "candidate_index", "action"},
    "superseded": {"coordinator", "deployment_id"},
}


def admit(request, deployment_id: str) -> None:
    if request.deployment_id != deployment_id:
        raise ValueError("request deployment does not match this gateway")


def coordinator(request) -> str:
    if request.coordinator is None:
        raise ValueError("a coordinator identity is required")
    return request.coordinator


class GatewayService:
    # The last durable authority `check` accepted, held only in this process's memory.
    verified: tuple | None = None
    # The last checkpoint body decoded and its state.
    last_decoded: tuple[str, Any] | None = None
    # The last stored checkpoint body `check` accepted, and its deployment.
    accepted: tuple[str, str] | None = None

    def __init__(
        self,
        config,
        database: Path,
        backend: EvmGateway | ChainRouter,
        *,
        lease_seconds: float,
        preparation=None,
        operator=None,
        operator_actions: bool = False,
        prices: StrategyPriceSource | None = None,
    ):
        if not 2 <= lease_seconds <= 120:
            raise ValueError("lease policy must be between 2 and 120 seconds")
        self.route_chain: Chain | None = None
        # The trusted gateway start, not a caller, chooses whether this deployment
        # takes operator requests or runs a strategy.
        self.operator_actions = operator_actions
        self.config = config
        self.connector = connector_for(config)
        self.request_type = Request.__class_getitem__(
            (
                self.connector.codec.intent,
                self.connector.codec.checkpoint,
                self.connector.permissions.manifest_type,
            )
        )
        self.journal = Journal(database)
        self.preparation = preparation
        self.prices = prices
        self.check_preparation(operator)
        chains = configured_chains(config)
        self.backend: ChainAdapter | ChainRouter = (
            backend_for(backend, chains[0])
            if isinstance(backend, ChainRouter) and len(chains) == 1
            else backend
        )
        self.lease_seconds = lease_seconds
        if database.exists():
            self.check(self.journal.load())

    def check_preparation(self, operator):
        require_preparation(self.config, self.preparation, operator, self.journal)

    @property
    def adapter(self) -> ChainAdapter:
        if isinstance(self.backend, ChainRouter):
            raise ValueError("execution requires an explicit chain adapter route")
        return self.backend

    def check(self, state):
        # Every input the check reads, the gateway's own and the checkpoint's. An unchanged
        # tuple was already accepted, so re-deriving its Safe grants on every checkpoint read
        # would only repeat the same verdict.
        authority = (
            self.config,
            self.preparation,
            self.operator_actions,
            state.config,
            state.schema_version,
            is_operator(state),
            state.capabilities,
            state.strategy,
            getattr(state, "permissions", None),
        )
        if authority == self.verified:
            return
        self.check_authority(state)
        self.verified = authority

    def check_authority(self, state):
        if state.config != self.config:
            raise ValueError("gateway configuration differs from the durable deployment")
        if state.schema_version != self.connector.codec.schema_version:
            raise ValueError("gateway execution requires its connector checkpoint schema")
        if is_operator(state) != self.operator_actions:
            raise ValueError("gateway operator mode differs from the durable deployment")
        if state.capabilities is None:
            raise ValueError("execution requires retained strategy capabilities")
        self.connector.codec.capability.validate(self.config, state.capabilities)
        if self.preparation is not None:
            prepared = self.preparation.preparation.prepared
            if state.strategy != prepared.strategy or state.capabilities != prepared.capabilities:
                raise ValueError("durable strategy authority differs from frozen preparation")
        if any(isinstance(b.profile, SafeRoles) for b in self.config.accounts.bindings):
            self.check_retained_permissions(state)

    def check_retained_permissions(self, state):
        from almanak.gateway.permissions import validate_manifest

        if state.permissions is None or state.strategy is None:
            raise ValueError("Safe execution requires retained permissions and strategy identity")
        validate_manifest(self.config, state.permissions, connector=self.connector.permissions)
        if (state.permissions.source_hash, state.permissions.capabilities) != (
            state.strategy.sha256,
            state.capabilities,
        ):
            raise ValueError("retained permissions differ from the sandboxed strategy authority")

    def load_state(self):
        return self.decoded(self.load_body())

    def load_body(self) -> str:
        try:
            self.journal.path.stat()
        except FileNotFoundError:
            raise DataError(
                "deployment_not_found",
                "Deployment database not found. A first start creates it; "
                "an existing deployment requires its original database.",
            ) from None
        return self.journal.body()

    def decoded(self, body: str):
        """The immutable checkpoint `body` decodes to, decoded once while it is unchanged."""
        last = self.last_decoded
        if last is not None and last[0] == body:
            return last[1]
        state = decode_checkpoint(body)
        self.last_decoded = (body, state)
        return state

    def dispatch(self, method: str, request: Request):
        fields = request.model_fields_set - ({"chain"} if method in ROUTED else set())
        if method not in FIELDS or fields != self.request_fields(method):
            raise ValueError("unsupported gateway request shape")
        if method == "configuration":
            return self.config
        if method.startswith("permissions-"):
            return self.permissions(method, request)
        if method == "create":
            return self.create(request)
        if method == "state":
            return self.serve_state()
        if method == "renew":
            return self.renew(request)
        state = self.load_state()
        self.check(state)
        admit(request, state.deployment_id)
        if method == "teardown":
            return self.journal.teardown()
        identity = coordinator(request)
        if method == "acquire":
            claim = Lease(self.journal, state)
            claim.close()
            self.journal.acquire(identity, self.lease_seconds)
            return {"lease_seconds": self.lease_seconds}
        if method == "release":
            self.journal.release(identity)
            return None
        return self.dispatch_fenced(method, request)

    def checked(self, body: str) -> str:
        """The deployment of a stored checkpoint `body`, checked unless last accepted."""
        accepted = self.accepted
        if accepted is not None and accepted[0] == body:
            return accepted[1]
        state = self.decoded(body)
        self.check(state)
        self.accepted = (body, state.deployment_id)
        return state.deployment_id

    def serve_state(self):
        """The stored checkpoint JSON, which its own model wrote, once `checked`."""
        body = self.load_body()
        self.checked(body)
        return json.loads(body)

    def renew(self, request):
        """Extend the lease, writing only the lease row, over a `checked` checkpoint."""
        admit(request, self.checked(self.load_body()))
        self.journal.renew(coordinator(request), self.lease_seconds)

    def request_fields(self, method):
        # An operator deployment is created without a strategy; supplying one is refused
        # by shape, as omitting one is for a strategy deployment.
        if method == "create" and self.operator_actions:
            return set()
        return FIELDS[method]

    def create(self, request):
        if self.operator_actions:
            if any(isinstance(b.profile, SafeRoles) for b in self.config.accounts.bindings):
                raise ValueError("operator deployments support EOA accounts only")
            if len(configured_chains(self.config)) != 1:
                raise ValueError("operator deployments support one chain only")
            self.journal.create(self.config, None, None, preparation=self.preparation)
        else:
            if request.strategy is None:
                raise ValueError("an explicit strategy is required")
            manifest = self.creation_permissions(request.strategy)
            self.journal.create(
                self.config, request.strategy, manifest, preparation=self.preparation
            )
        state = self.journal.load()
        # Claims outlive the service. The runtime's coordinator lease is separate.
        claim = Lease(self.journal, state)
        claim.close()
        return state

    def dispatch_fenced(self, method, request):
        """Run one lease-fenced request, holding the journal lock only to commit.

        Provider reads and signing run against the checkpoint read under a current lease.
        The commit re-checks the lease, refuses a replacement of a checkpoint that changed
        meanwhile and submits only under the lock, so a superseded coordinator can neither
        commit nor broadcast, and renewals do not wait behind slow external calls.
        """
        body = self.journal.read_fenced(request.coordinator)
        if not keeps_lock(method, request, self.decoded(body)):
            commit = self.fenced_commit(request)
            result, failure = self.run_fenced(method, request, body, commit, durable=commit)
        else:
            with self.journal.authorized(request.coordinator, self.lease_seconds) as (db, body):
                result, failure = self.run_fenced(
                    method, request, body, lambda: nullcontext((db, body))
                )
        # Raised only after the commit, so a retained fault survives its own failure.
        if failure is not None:
            # The commit that retained it also extended the lease.
            mark_lease(failure, extended=True)
            raise failure
        return result

    def fenced_commit(self, request):
        return lambda: self.journal.authorized(request.coordinator, self.lease_seconds)

    def run_fenced(self, method, request, body, commit, durable=None):
        """Compute from `body`, then commit; returns the result and any committed failure.

        `durable` opens a separate locked commit, so a change it writes ahead is durable
        before an irreversible effect; it is absent while the request holds the lock.
        """
        state = self.decoded(body)
        self.check(state)
        writes, failure, result = Writes(body, durable), None, None
        try:
            if request.chain is not None:
                result = dispatch_chain(self, method, request, writes, state)
            else:
                result = self.dispatch_unscoped(method, request, writes, state)
        except DataError as exc:
            failure, writes = exc, failure_writes(request, state, body, exc)
        with commit() as (db, current):
            if failure is None:
                try:
                    result = writes.commit(db, current, result)
                except DataError as exc:
                    failure, writes = exc, failure_writes(request, state, body, exc)
            if failure is not None:
                writes.commit(db, current, None)
        return result, failure

    def dispatch_unscoped(self, method, request, db, state):
        if state.work and method in ROUTED - {"update"}:
            raise ValueError("multi-chain execution requires an explicit chain route")
        self.check_execution_authority(method, state)
        return self.dispatch_authorized(method, request, db, state)

    def check_execution_authority(self, method, state):
        if method in (
            "observe",
            "plan",
            "sign",
            "broadcast",
            "family",
            "request-reconcile",
            "request-cancel-plan",
            "external-reconcile",
        ):
            if state.integrity_error:
                raise DataError(
                    "integrity", "invalidated execution history requires operator investigation"
                )
            self.adapter.audit_history(state)
        from almanak.gateway.production_config import execution_gas

        if method == "broadcast" and execution_gas(self.config) is not None:
            self.adapter.authorize_submission(state)

    def permissions(self, method, request):
        from almanak.gateway.permissions import generate, verify

        if method == "permissions-generate":
            return generate(
                self.config,
                request.strategy,
                self.backend,
                connector=self.connector.permissions,
                prepared=self.preparation.preparation.prepared if self.preparation else None,
            )
        return verify(
            self.config, request.manifest, self.backend, connector=self.connector.permissions
        )

    def creation_permissions(self, strategy):
        if any(isinstance(b.profile, SafeRoles) for b in self.config.accounts.bindings):
            from almanak.gateway.permissions import generate, verify

            manifest = generate(
                self.config,
                strategy,
                self.backend,
                connector=self.connector.permissions,
                prepared=self.preparation.preparation.prepared if self.preparation else None,
            )
            verify(
                self.config,
                manifest,
                self.backend,
                connector=self.connector.permissions,
            )
            return manifest
        return None

    def serve_prices(self, request, state):
        """The declared strategy series as of the decision cutoff, retained as evidence."""
        from almanak.gateway.strategy_prices import require_declared, series_json, strategy_bindings

        prepared = self.preparation.preparation.prepared if self.preparation else None
        require_declared(request.price_bindings, strategy_bindings(prepared))
        if self.prices is None:
            raise DataError("missing_provider", "this gateway serves no strategy price series")
        return series_json(
            self.prices.series(state.deployment_id, request.price_bindings, request.cutoff)
        )

    def dispatch_authorized(self, method, request, db, state):
        if method in ("family", "broadcast"):
            return self.dispatch_admission(method, request, db, state, state)
        if method == "update":
            return self.record_update(request, db, state)
        if method == "observe":
            return self.adapter.observe(state)
        if method == "prices":
            return self.serve_prices(request, state)
        if method == "audit":
            self.adapter.audit_history(state)
            return None
        if method == "plan":
            return self.prepare_plan(request, state)
        if method in ("request-reconcile", "request-cancel-plan"):
            from almanak.gateway.requests import dispatch

            return dispatch(self.adapter, state, method, request.attempt_id)
        if method == "external-reconcile":
            from almanak.gateway.external_effects import reconcile

            return reconcile(self.adapter, state)
        if method == "sign":
            return self.adapter.sign(state)
        return self.dispatch_pending(method, state)

    def dispatch_admission(self, method, request, db, state, local):
        from almanak.work import merge

        if (
            method == "broadcast"
            and local.pending is not None
            and getattr(local.pending, "family", None) is None
        ):
            # Authority, simulation and budget checks run now; identity, nonce and the send
            # repeat under the lock against an unchanged pending transaction.
            self.adapter.prepare_submission(local)
            if is_unsent(local.pending):
                local = self.record_send(db, state, local)
            return db.submit(lambda current: self.submit_prepared(db, current, local))
        if method == "family":
            if None in (request.action, request.family_id, request.candidate_index):
                raise ValueError("family action and candidate identity are required")
            changed = self.adapter.advance_family(
                local, request.action, request.family_id, request.candidate_index
            )
            sent = False
        else:
            if local.pending is None:
                raise ValueError("execution requires a durably recorded signed transaction")
            changed, sent = self.adapter.submit(local)
        after = merge(state, self.route_chain, changed) if self.route_chain else changed
        after = self.connector.codec.checkpoint.model_validate_json(after.model_dump_json())
        db.replace(after)
        return after if method == "family" else sent

    def record_send(self, db, state, local):
        """Durably clear the attempt's never-sent proof before its bytes can leave.

        The mark is cleared before the locked re-checks of identity, nonce and lease, so a
        refusal there, like a crash before the send, leaves the attempt maybe sent. That is
        conservative: it is never released as unsent again.
        """
        from almanak.work import merge, project

        sent = local.model_copy(
            update={"pending": local.pending.model_copy(update={"unsent": False})}
        )

        def mark(current):
            now = project(current, self.route_chain) if self.route_chain else current
            if now.pending != local.pending:
                raise DataError("conflict", "pending execution changed; reload before broadcasting")
            if not self.route_chain:
                return sent
            return merge(
                current, self.route_chain, now.model_copy(update={"pending": sent.pending})
            )

        db.write_ahead(mark)
        return sent

    def submit_prepared(self, db, current, local):
        from almanak.work import merge, project

        now = project(current, self.route_chain) if self.route_chain else current
        if now.integrity_error:
            raise DataError(
                "integrity", "invalidated execution history requires operator investigation"
            )
        if now.pending != local.pending:
            raise DataError("conflict", "pending execution changed; reload before broadcasting")
        changed, sent = self.adapter.submit(local)
        if changed.pending != local.pending:
            # The adapter recorded in the pending attempt what its send returned, such as a
            # venue's answer that no receipt can reproduce later. Only that record commits,
            # under this lock with the send.
            recorded = now.model_copy(update={"pending": changed.pending})
            after = merge(current, self.route_chain, recorded) if self.route_chain else recorded
            db.replace(self.connector.codec.checkpoint.model_validate_json(after.model_dump_json()))
        return sent

    def dispatch_pending(self, method, state):
        if state.pending is None:
            raise ValueError("execution requires a durably recorded signed transaction")
        if method == "receipt":
            return self.adapter.receipt(state.pending)
        if method == "reconcile":
            receipt = self.adapter.receipt(state.pending)
            if receipt is None:
                raise DataError("unavailable", "the recorded transaction has no receipt")
            return self.adapter.reconcile(state.pending, receipt)
        if method == "superseded":
            if not is_operator(state):
                raise ValueError("only an operator deployment drops a superseded signed step")
            return self.proven_superseded(state)

        raise ValueError("unsupported gateway operation")

    def record_update(self, request, db, state):
        if request.before != state:
            raise DataError("conflict", "checkpoint changed; reload before recording")
        after = request.after
        if after is None:
            raise ValueError("updated checkpoint is required")
        if state.work:
            validate_global_update(self, state, after)
        else:
            self.validate_update(state, after)
        db.replace(after)
        return after

    def prepare_plan(self, request, state):
        from almanak.request_runtime import unresolved

        if unresolved(state):
            raise ValueError("resolve the retained request before ordinary preparation")
        if state.pending is not None or state.plan is not None:
            raise ValueError("resolve the admitted attempt before preparing another")
        if state.delivered != len(state.outcomes):
            raise ValueError("deliver recorded outcomes before continuing execution")
        if request.intent is None or request.attempt_id is None:
            raise ValueError("intent and attempt identity are required")
        return self.prepare_observed_plan(request, state)

    def prepare_observed_plan(self, request, state):
        observed = {s.allocation: s for s in self.adapter.observe(state)}
        if request.intent.target.allocation not in observed:
            raise ValueError("intent position is not bound to this gateway")
        return self.adapter.prepare(
            state,
            request.intent,
            observed[request.intent.target.allocation],
            request.attempt_id,
        )

    def validate_update(self, before, after):
        self.validate_recorded_evidence(before, after)
        self.validate_schedule_update(before, after)
        self.validate_decision_admission(before, after)
        self.validate_pending_resolution(before, after)
        self.validate_released_record(before, after)
        if after.plan is not None and after.plan != before.plan:
            self.validate_new_plan(before, after.plan)
        if before.pending is None and after.pending is not None:
            self.validate_signed_admission(before, after.pending)
        self.validate_owned_evidence(before, after)
        from almanak.gateway.admission import validate_execution_update

        validate_execution_update(self.backend, before, after)

    def validate_schedule_update(self, before, after):
        if before.schedule == after.schedule:
            return
        if is_operator(before):
            raise ValueError("an operator deployment has no decision schedule")
        if (
            before.mode != "running"
            or not before.initialized
            or before.decision is not None
            or before.plan is not None
            or before.pending is not None
            or before.delivered != len(before.outcomes)
            or after.schedule.last_evaluation is None
        ):
            raise ValueError("interval progress requires an eligible new decision")
        expected = before.schedule.after(
            before.config.decision_trigger, after.schedule.last_evaluation
        )
        if after.schedule != expected:
            raise ValueError("recording cannot reset decision scheduling")
        if after.decision is not None and after.decision.trigger != after.schedule.last_evaluation:
            raise ValueError("decision must retain its interval event")

    def validate_decision_admission(self, before, after):
        decision = after.decision
        if decision is None or decision == before.decision:
            return
        if before.decision is not None:
            raise ValueError("an active decision cannot be replaced")
        if is_operator(before) or decision.trigger.kind == "operator":
            self.validate_operator_admission(before, after)
            return
        if decision.trigger.kind == "teardown":
            if before.mode != "teardown":
                raise ValueError("teardown decisions require a teardown request")
            return
        if after.schedule == before.schedule or decision.trigger != after.schedule.last_evaluation:
            raise ValueError("a new interval decision requires atomic scheduling progress")

    def validate_operator_admission(self, before, after):
        """An operator request is admitted only on an idle, initialized operator deployment."""
        decision = after.decision
        if not is_operator(before) or decision.trigger.kind != "operator":
            raise ValueError("operator requests and operator deployments admit only each other")
        if (
            not before.initialized
            or before.plan is not None
            or before.pending is not None
            or before.delivered != len(before.outcomes)
        ):
            raise ValueError("an operator request requires an idle initialized deployment")
        increases = self.connector.lifecycle.increases_exposure(decision.intent)
        if before.mode == "closed" or (before.mode == "teardown" and increases):
            raise ValueError("teardown admits only operator requests that reduce exposure")
        if request_executed(before.outcomes, decision.trigger):
            raise ValueError("operator request was already executed")
        if increases and accounting_blocked(before.outcomes):
            raise ValueError(accounting_refusal(before.outcomes))
        self.require_direct(decision.intent)

    def require_direct(self, intent):
        """An operator request is admitted only for an action its chain declares direct."""
        from almanak.connectors.target_routing import connector_for_target
        from almanak.gateway.environments import origin_chain

        chain = origin_chain(self.config, intent.target.fund.chain).chain_id
        connector_for_target(intent.target).support_for(chain).require_direct(intent.kind)

    def validate_recorded_evidence(self, before, after):
        immutable = (
            "config",
            "strategy",
            "deployment_id",
            "schema_version",
            "capabilities",
            "permissions",
            "integrity_error",
        )
        if any(getattr(before, field) != getattr(after, field) for field in immutable):
            raise ValueError("recording cannot change immutable deployment identity")
        if is_operator(before) != is_operator(after):
            raise ValueError("recording cannot change immutable deployment identity")
        if is_operator(before) and after.memory != before.memory:
            raise ValueError("an operator deployment has no strategy memory")
        if after.decision is not None:
            from almanak.work import validate_dependencies

            validate_dependencies(after.decision)
            if (
                getattr(self, "route_chain", None) is not None
                and after.decision.intent.target.fund.chain != self.route_chain
            ):
                raise ValueError("decision target differs from the authorized chain route")
            self.connector.codec.capability.admit(before.capabilities, after.decision.intent)
            target = after.decision.intent.target
            if target != self.config.target(target.allocation):
                raise ValueError("decision target is not authorized by configuration")
            if after.decision != before.decision:
                self.verify_observation_set(after.decision.observations)
        if after.plan is not None and after.plan.decision != after.decision:
            raise ValueError("plan must retain the active decision")
        if after.pending is not None and after.pending.decision != after.decision:
            raise ValueError("pending transaction must retain the active decision")
        self.validate_evidence_extension(before, after)
        self.validate_mode_transition(before, after)

    def validate_evidence_extension(self, before, after):
        if (
            after.outcomes[: len(before.outcomes)] != before.outcomes
            or after.delivered < before.delivered
        ):
            raise ValueError("recording cannot erase execution evidence or reopen a deployment")

    def validate_mode_transition(self, before, after):
        if (before.mode != "running" and after.mode == "running") or (
            before.mode == "closed" and after != before
        ):
            raise ValueError("recording cannot erase execution evidence or reopen a deployment")

    def validate_pending_resolution(self, before, after):
        if after.plan is not None and after.pending is not None:
            raise ValueError("a deployment admits only one transaction attempt")
        if recorded_superseded(before) != recorded_superseded(after):
            if before.pending is None or superseded_change(before, after) is None:
                raise ValueError("a proof is recorded only with the drop of its signed step")
        if before.pending is not None and after.pending != before.pending:
            if after.pending is None and after.outcomes == before.outcomes:
                self.validate_released_attempt(before, after)
            else:
                self.validate_resolved_outcome(before, after)

    def validate_released_attempt(self, before, after):
        from almanak.request_runtime import release_pending

        if self.superseded_drop(before, after):
            return
        if after != release_pending(before):
            raise ValueError("a transaction is released only with its decision")
        self.adapter.require_releasable(before.pending)

    def superseded_drop(self, before, after) -> bool:
        """Whether an operator deployment drops a signed step a final transaction superseded.

        The update records the proof it rests on, which sets it apart from every release.
        It is admitted only when that record is exactly what the gateway proves now.
        """
        from almanak.request_runtime import drop_superseded

        recorded = superseded_change(before, after)
        if recorded is None:
            return False
        if not is_operator(before):
            raise ValueError("only an operator deployment drops a superseded signed step")
        if after != drop_superseded(before, recorded):
            raise ValueError("a superseded step is dropped only with its proof and its decision")
        if recorded != self.proven_superseded(before):
            raise ValueError("recorded proof differs from the gateway's own")
        return True

    def proven_superseded(self, state):
        """The adapter's proof for the signed step, unless a released attempt took the nonce.

        The transaction the proof names must be foreign to the deployment. When it is
        the transaction of an attempt released earlier at that nonce, that attempt was
        mined after all: its execution has to be reconciled from its own receipt, so
        the drop, which would settle it without an outcome, is refused.
        """
        proof = self.adapter.superseded(state.pending)
        if any(item.tx_hash == proof.consumed_by for item in getattr(state, "released", ())):
            raise DataError(
                "nonce_conflict",
                f"the released attempt {proof.consumed_by} was mined at the recorded nonce;"
                " reconcile it before continuing",
            )
        return proof

    def validate_released_record(self, before, after):
        """Released attempts change only by a release or by reconciling their nonce.

        A new signed attempt at a released nonce leaves the record in place: until an
        attempt at that nonce is reconciled, a released copy may still land, and its
        identity is the evidence for that reconciliation.
        """
        from almanak.request_runtime import settle_released

        retained, changed = getattr(before, "released", ()), getattr(after, "released", ())
        if before.pending is not None and after.pending is None:
            if after.outcomes == before.outcomes:
                # Exactness: validate_released_attempt. A never-sent attempt is not retained,
                # nor is an operator deployment's step that a final transaction superseded.
                expected = self.released_after_release(before, after)
            else:
                expected = settle_released(before, before.pending).get("released", retained)
            if changed == expected:
                return
        elif retained == changed:
            return
        raise ValueError("released attempts are retained until an attempt at their nonce settles")

    @staticmethod
    def released_after_release(before, after):
        """What `released` holds once a signed attempt leaves without an outcome.

        A never-sent attempt is not retained. An operator deployment's step that a final
        transaction superseded is not retained either, and settles the attempts released
        earlier at its nonce, which that transaction took as well. Any other release
        retains the attempt.
        """
        from almanak.request_runtime import settle_released

        retained = getattr(before, "released", ())
        if superseded_change(before, after) is not None:
            return settle_released(before, before.pending).get("released", retained)
        return retained if is_unsent(before.pending) else (*retained, before.pending)

    def validate_resolved_outcome(self, before, after):
        if after.pending is not None or len(after.outcomes) != len(before.outcomes) + 1:
            raise ValueError("pending execution must be resolved with its recorded outcome")
        receipt = self.adapter.receipt(before.pending)
        if receipt is None or after.outcomes[-1] != self.adapter.reconcile(before.pending, receipt):
            raise ValueError("economic commit differs from independently reconciled chain evidence")

    def validate_owned_evidence(self, before, after):
        from almanak.external_effects import changed

        expected = self.connector.lifecycle.ownership(before)
        if adopts(before, after):
            # An operator deployment adopts the position its baseline states; the
            # baseline itself is verified against the chain below.
            adopted = self.connector.lifecycle.adopt(before.owned, after.baseline)
            expected = {"owned": adopted}
        elif changed(before, after):
            from almanak.gateway.external_effects import verified_commit

            verified = verified_commit(self.adapter, before, after)
            expected = self.connector.lifecycle.ownership(verified)
        elif (
            before.pending is not None
            and after.pending is None
            and after.outcomes != before.outcomes
        ):
            expected = self.connector.lifecycle.ownership(before, after.outcomes[-1])
        elif after.outcomes != before.outcomes:
            from almanak.gateway.requests import verified_commit

            verified_commit(self.adapter, before, after)
            expected = self.connector.lifecycle.ownership(before, after.outcomes[-1])
        if any(getattr(after, field) != value for field, value in expected.items()):
            raise ValueError("ownership must follow the independently verified economic commit")
        self.validate_observation_records(before, after)

    def validate_observation_records(self, before, after):
        if after.baseline != before.baseline:
            if before.initialized or not after.initialized:
                raise ValueError("baseline cannot replace existing capital evidence")
            self.verify_observation_set(after.baseline)
        if after.closure != before.closure:
            if after.mode != "closed":
                raise ValueError("closure evidence requires a closed deployment")
            self.verify_observation_set(after.closure)

    def verify_observation_set(self, observations):
        targets = tuple(
            self.config.target(a.name)
            for a in self.config.positions
            if getattr(self, "route_chain", None) in (None, a.chain)
        )
        if tuple(o.target for o in observations) != targets:
            raise ValueError("observations must cover each retained position exactly once")
        for observation in observations:
            self.verify_observation(observation)

    def verify_observation(self, observation):
        if self.adapter.verify_snapshot(observation) != observation:
            raise ValueError("recorded financial evidence differs from its pinned chain reads")

    def validate_new_plan(self, before, plan):
        if before.pending is not None or before.plan is not None or not plan.attempt_id:
            raise ValueError("an admitted plan cannot be replaced")
        if before.delivered != len(before.outcomes):
            raise ValueError("deliver recorded outcomes before continuing execution")
        self.validate_preparation(before, plan)

    def validate_preparation(self, before, plan):
        observed = self.adapter.snapshot(plan.operation.allocation, plan.before.block, plan.before)
        from almanak.request_runtime import unresolved

        if unresolved(before):
            from almanak.gateway.requests import validate_cancellation_plan

            if observed != plan.before:
                raise ValueError("cancellation snapshot differs from verified observations")
            validate_cancellation_plan(self.adapter, before, plan, observed)
            return
        if observed != plan.before:
            raise plan_differs(["snapshot"])
        prepared = self.adapter.prepare(before, plan.decision.intent, observed, plan.attempt_id)
        if prepared != plan:
            raise plan_differs(differing(plan, prepared))

    def validate_signed_admission(self, before, pending):
        if before.plan is None or self.adapter.unsigned_plan(pending) != before.plan:
            raise ValueError("signed transaction differs from the durably reserved plan")
        if not self.adapter.verify_signed_admission(before, pending):
            raise ValueError("signed transaction differs from the authorized signature")


def plan_differs(parts) -> ValueError:
    """The refusal of a plan the gateway did not prepare as reserved, naming the parts
    that differ (never their values) so the cause can be told from the log."""
    return ValueError(
        "transaction plan differs from verified gateway preparation: " + ", ".join(parts)
    )


def differing(plan, prepared) -> list[str]:
    """Names of the fields of the gateway's own preparation that `plan` does not match."""
    names = getattr(type(prepared), "model_fields", None) or vars(prepared)
    parts = [name for name in names if getattr(plan, name, None) != getattr(prepared, name)]
    return parts or ["plan"]


def recorded_superseded(state) -> tuple:
    return getattr(state, "superseded", ())


def superseded_change(before, after):
    """The one proof `after` adds while it drops `before`'s signed step, or None."""
    earlier, later = recorded_superseded(before), recorded_superseded(after)
    if after.pending is not None or after.outcomes != before.outcomes:
        return None
    if len(later) != len(earlier) + 1 or later[:-1] != earlier:
        return None
    return later[-1]


def keeps_lock(method, request, state) -> bool:
    """Replacement families record reservations, timing and submission errors from the
    state they sign or send against, so they keep the journal lock throughout."""
    if method == "family":
        return True
    if method != "broadcast":
        return False
    from almanak.work import project

    local = project(state, request.chain) if request.chain is not None else state
    return getattr(local.pending, "family", None) is not None


def failure_writes(request, state, body, exc) -> Writes:
    """What a failed request still commits; raises `exc` when it commits nothing."""
    from almanak.work import retain_fault

    writes = Writes(body)
    chain = request.chain
    if chain is not None and state.work and exc.kind != "conflict":
        writes.apply(lambda current: retain_fault(current, chain, exc))
    elif exc.kind == "reorganization":
        writes.apply(lambda current: current.model_copy(update={"integrity_error": str(exc)}))
    elif chain is None:
        raise exc
    return writes


def require_preparation(config, preparation, operator, journal):
    # Check retained authority before a launch path opens keys or provider clients.
    if preparation is None:
        if operator is not None or getattr(config, "preparation_digest", None) is not None:
            raise ValueError("gateway requires the deployment's frozen preparation and operator")
    else:
        if operator is None:
            raise ValueError("prepared execution requires independent operator configuration")
        preparation.require_configuration(config, preparation.preparation.prepared.strategy)
        preparation.require_operator(operator)
    if journal.path.exists() and journal.configuration_preparation() != preparation:
        raise ValueError("gateway preparation differs from the durable deployment")
    if journal.path.exists() and preparation is not None:
        preparation.require_checkpoint(journal.load())


Listen = int | socket.socket


def listen_address(args) -> Listen:
    """The explicit CLI port, or the loopback socket a supervisor bound and passed down."""
    fd = getattr(args, "listen_fd", None)
    return args.port if fd is None else inherited_listener(fd)


def inherited_listener(fd: int) -> socket.socket:
    sock = socket.socket(fileno=fd)
    try:
        if (
            sock.family != socket.AF_INET
            or sock.type != socket.SOCK_STREAM
            or sock.getsockname()[0] != "127.0.0.1"
        ):
            raise ValueError("gateway listener must be an inherited loopback TCP socket")
    except BaseException:
        sock.close()
        raise
    # Strategy workers and other children must not hold the gateway's port.
    sock.set_inheritable(False)
    return sock


def loopback_server(listen: Listen, handler) -> ThreadingHTTPServer:
    if isinstance(listen, int):
        return ThreadingHTTPServer(("127.0.0.1", listen), handler)
    server = ThreadingHTTPServer(listen.getsockname(), handler, bind_and_activate=False)
    server.socket.close()
    server.socket = listen
    host, port = listen.getsockname()
    server.server_address = (host, port)
    server.server_name, server.server_port = host, port
    try:
        server.server_activate()
    except BaseException:
        server.server_close()
        raise
    return server


def refusal_reason(exc: Exception) -> str:
    """Why a request was refused, for the operator's log only; the caller sees a generic reply.

    Validation errors name only the failing fields, since their text can echo request values.
    """
    if isinstance(exc, ValidationError):
        fields = sorted({".".join(map(str, error["loc"])) or "body" for error in exc.errors()})
        return ("ValidationError: " + ", ".join(fields))[:300]
    if isinstance(exc, KeyError):
        # A missing key can be a value the caller submitted, such as an unretained asset.
        return "KeyError: missing lookup key"
    return f"{type(exc).__name__}: {exc}"[:300]


def http_server(service, token_file: Path, listen: Listen, *, request_type=None, fields=FIELDS):
    read_token(token_file)
    request_type = request_type or service.request_type

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def setup(self):
            super().setup()
            self.connection.settimeout(30)

        def reply(self, status, payload, reason=None):
            body = json.dumps(payload).encode()
            method = self.path.removeprefix("/")
            # Wall time from request receipt to an encoded reply, before the write.
            # It includes waiting for the durable checkpoint lock held by another request.
            duration = time.monotonic() - self.received
            line = {
                "event": "gateway_request",
                "operation": method if method in fields else "unknown",
                "status": status,
                "duration_ms": round(duration * 1000, 1),
            }
            if reason is not None:
                line["reason"] = reason
            line.update(self.scope.log_fields())
            print(
                json.dumps(line),
                file=sys.stderr,
                flush=True,
            )
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # The caller may die after dispatch. Recovery uses the durable record.
                pass

        def do_POST(self):
            self.received = time.monotonic()
            with request_scope() as self.scope:
                self.handle_post()

        def handle_post(self):
            try:
                # Reload trusted authentication on every request so rotation survives restart.
                supplied = self.headers.get("Authorization", "")
                if not hmac.compare_digest(supplied, "Bearer " + read_token(token_file)):
                    self.reply(401, {"error": "unauthorized", "message": "authentication failed"})
                    return
                method = self.path.removeprefix("/")
                if method not in fields or self.headers.get("Transfer-Encoding"):
                    raise ValueError("unsupported gateway operation or transfer encoding")
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    # int() echoes the header value, which must not reach the refusal log.
                    raise ValueError("gateway request has an invalid Content-Length") from None
                if not 0 < length <= MAX_REQUEST_BYTES:
                    raise ValueError("gateway request size is outside the permitted range")
                request = request_type.model_validate_json(self.rfile.read(length))
                result = service.dispatch(method, request)
                if isinstance(result, Model):
                    result = result.model_dump(mode="json")
                elif isinstance(result, tuple):
                    result = [item.model_dump(mode="json") for item in result]
                self.reply(200, {"result": result})
            except (ValueError, ValidationError, KeyError) as exc:
                self.refuse(
                    400,
                    {"error": "invalid_request", "message": "gateway request refused"},
                    exc,
                    reason=refusal_reason(exc),
                )
            except DataError as exc:
                status = 429 if exc.kind == "rate_limit" else 409
                # A decoded revert reason is contract output, bounded and printable, so
                # the gateway log names it; other refusals keep their messages out of it.
                # A stuck attempt's stop carries the revert it wraps the same way.
                decoded = "simulation_reverted" in (exc.kind, getattr(exc, "refusal_kind", None))
                reason = f"{exc.kind}: {exc}" if decoded else None
                self.refuse(status, data_error_reply(exc), exc, reason=reason)
            except RuntimeError as exc:
                self.refuse(409, {"error": "authority", "message": str(exc)}, exc)
            except (OSError, sqlite3.Error):
                # The client reads this as unavailability and counts nothing from it.
                self.reply(503, {"error": "unavailable", "message": "gateway storage unavailable"})
            except Exception as exc:
                self.refuse(500, {"error": "internal", "message": "gateway operation failed"}, exc)

        def refuse(self, status, payload, exc, reason=None):
            # A fenced request whose failure still committed a lease extension says so,
            # so the coordinator counts its lease from this answer.
            if lease_extended(exc):
                payload = {**payload, "lease_extended": True}
            self.reply(status, payload, reason=reason)

    server = loopback_server(listen, Handler)
    # Drain admitted requests before closing their adapters or finalizing native objects.
    server.daemon_threads = False
    return server


def data_error_reply(exc: DataError) -> dict:
    # The JSON-RPC code tells a contract revert from a throttled or failed read.
    reply: dict = {"error": exc.kind, "message": str(exc)}
    if exc.code is not None:
        reply["code"] = exc.code
    # A provider read that outlasted its retries; the coordinator holds instead of stopping.
    if exc.transient:
        reply["transient"] = True
    if exc.reverted:
        reply["reverted"] = True
    if exc.details:
        reply["details"] = exc.details
    return reply


def serve(service: GatewayService, token_file: Path, listen: Listen):
    server = http_server(service, token_file, listen)
    try:
        print(json.dumps({"event": "gateway_ready", "port": server.server_port}), flush=True)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        service.backend.close()
