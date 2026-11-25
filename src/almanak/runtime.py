"""One execution lifecycle coordinator, including recovery and requested teardown."""

import json
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from almanak import decision_facts, external_effects, request_runtime, transaction_facts
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.composition import connector_for
from almanak.connectors.repayment import latest_coverage
from almanak.connectors.repayment import transitions as coverage_transitions
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
from almanak.gateway.routing import ScopedGateway
from almanak.operator_deployment import accounting_refusal, is_operator
from almanak.reporting import report as report
from almanak.sandbox import worker_command
from almanak.strategy import (
    DataDependencyRequest,
    DecisionRequest,
    EconomicDataDependencyRequest,
    EconomicDecisionRequest,
    EconomicOutcomeRequest,
    EconomicValidationRequest,
    EventRequest,
    OutcomeRequest,
    StrategyDefinition,
    ValidationRequest,
)
from almanak.strategy_api import Memory
from almanak.transaction_family import CURRENT_REFUSALS, is_unsent
from almanak.triggers import utc_now
from almanak.work import ChainAvailability, availability
from almanak.worker_process import WorkerStartupTimeout, WorkerWallTimeout, run_worker

# An idle coordinator reads its checkpoint at least this often (seconds), so a teardown or
# external effect recorded while it waits for the next decision is seen within this bound.
IDLE_POLL_SECONDS = 5
# A starved host can delay a worker that is well inside its CPU budget, at startup or while
# it evaluates. Retrying is safe because nothing an evaluation produces is recorded before
# its compare-and-set commit. After each consecutive timeout the coordinator backs off for
# IDLE_POLL_SECONDS, doubling up to this cap, so it still tries at least once a minute and
# evaluates a teardown requested meanwhile within that bound.
TIMEOUT_BACKOFF_CAP_SECONDS = 60
# A running deployment stops only after its evaluations have timed out without one
# success for this long, measured on the monotonic clock. A deployment in teardown never
# stops for timeouts: stopping could only leave its positions unmanaged, while a retry
# exits them as soon as the host has CPU again.
WORKER_TIMEOUT_HORIZON_SECONDS = 30 * 60
# A provider read the gateway marked transient (a throttle, transport failure, HTTP 5xx or 408,
# or a block a lagging backend does not yet serve) holds the tick and backs off like a worker
# timeout. A running deployment stops only after every tick for this long was held; one in
# teardown never stops for it.
TRANSIENT_HOLD_HORIZON_SECONDS = 30 * 60
# A market fee quote above a per-transaction ceiling of the gas policy is a market
# condition, not a fault: `gas_price` is a gas price above `max_gas_price_wei` and
# `gas_fee_quote` a quoted maximum fee (with any OP Stack L1 data and operator fees) above
# `max_transaction_fee`. Preparing an unsigned decision holds on them with the transient
# back-off, without a horizon in any mode, and proceeds once the quote is within the
# ceilings. A refused preparation commits nothing, so the retained decision is prepared
# again from fresh observations. Only preparation holds: signing and submission re-check
# the fees it pinned, and a refusal there still stops. Gas limit, payer balance, exit
# reserve and lifetime budget refusals stop the coordinator too.
CEILING_HOLDS = ("gas_price", "gas_fee_quote")
# Refusals of an unsigned plan after which the coordinator discards it and decides afresh.
# Nothing is signed or broadcast when the gateway refuses at preparation or signing, so the
# plan's nonce was never used. Expired plans retry without bound. A market that moved under
# the plan (its simulation reverts, or the pool left the retained price tolerance), a
# hooked pool whose terms the snapshot could not measure for an entry, or a plan whose
# protocol deadline window was mostly spent before signing, is retried at most
# REPLAN_ATTEMPTS consecutive times per chain route, waiting
# attempt * REPLAN_BACKOFF_SECONDS between them, and then stops with the refusal's reason.
MARKET_REFUSALS = ("simulation_reverted", "price_movement", "hook_terms", "deadline_margin")
REPLANNED = ("stale_plan", *MARKET_REFUSALS)
REPLAN_ATTEMPTS = 5
REPLAN_BACKOFF_SECONDS = 15


class CeilingHold(DataError):
    """Preparation refused on a fee ceiling (`CEILING_HOLDS`); only `Runtime.run` holds on it.

    A single tick still raises it, so an operator request is refused at once.
    """


class ReplansExhausted(DataError):
    """A market refusal past REPLAN_ATTEMPTS; it stops the coordinator on every route.

    It keeps the refusal's kind and reason. Only this capped refusal leaves a chain-scoped
    tick: the same kinds raised elsewhere, such as at broadcast, stay chain-local faults.
    """


@dataclass
class Replanning:
    """Consecutive market refusals per chain route, and the wait the last one requested."""

    attempts: dict = field(default_factory=dict)
    # Per chain route, the stages ("prepare", "sign") whose refusals the count includes.
    stages: dict = field(default_factory=dict)

    def reset(self, route) -> None:
        self.attempts.pop(route, None)
        self.stages.pop(route, None)

    delay: float = 0.0


def strategy_worker(
    request: DecisionRequest
    | ValidationRequest
    | OutcomeRequest
    | EventRequest
    | EconomicDecisionRequest
    | EconomicOutcomeRequest
    | EconomicValidationRequest
    | DataDependencyRequest,
) -> str:
    arguments = []
    if isinstance(request, ValidationRequest):
        arguments = ["--validate-config"]
    if isinstance(request, OutcomeRequest):
        arguments = ["--outcome"]
    if isinstance(request, EventRequest):
        arguments = ["--event"]
    if isinstance(request, DataDependencyRequest):
        arguments = ["--data-dependencies"]
    if isinstance(request, EconomicDataDependencyRequest):
        arguments = ["--economic-data-dependencies"]
    if isinstance(request, EconomicValidationRequest):
        arguments = ["--economic-validate"]
    if isinstance(request, EconomicDecisionRequest):
        arguments = ["--economic"]
    if isinstance(request, EconomicOutcomeRequest):
        arguments = ["--economic-outcome"]
    return sandboxed_evaluation(request, arguments)


def sandboxed_evaluation(request, arguments: list[str]) -> str:
    """One strategy worker evaluation, in the active preloaded session when there is one."""
    from almanak.worker_session import ACTIVE

    if (session := ACTIVE.get()) is not None:
        output = session.evaluate(request, arguments)
    else:
        output = run_worker("almanak.strategy_worker", request.model_dump_json(), arguments)
    output = output.strip()
    refused(output, request.strategy)
    return output


def refused(output: str, definition: StrategyDefinition) -> None:
    """Every mode that types the parameters answers a refused value with the refusal."""
    if output.startswith('{"refusal"'):
        validation_result(output, definition)


class StrategyConfigRefused(ConfigurationError):
    """The typed model or `validate_config` refused the parameters: final, never retried."""

    def __init__(self, message, file, line, field=None):
        super().__init__(
            message,
            file=file,
            field=field or "strategy.parameters",
            line=line,
            code="strategy_config_refused",
            remedy="Change the parameters the strategy refused, then run again.",
        )

    def summary(self) -> str:
        where = self.file if self.line is None else f"{self.file}:{self.line}"
        return f"the strategy refused the configuration ({where}): {self}"


def config_refusal(exc: BaseException | None) -> "StrategyConfigRefused | None":
    """The refusal `exc` is or was raised from, such as a check stage that wrapped it."""
    while exc is not None:
        if isinstance(exc, StrategyConfigRefused):
            return exc
        exc = exc.__cause__
    return None


def validate_strategy_config(definition: StrategyDefinition, config, *, economic=False) -> None:
    from almanak.configuration.parameter_codec import hook_parameters

    parameters = hook_parameters(config)
    request_type = EconomicValidationRequest if economic else ValidationRequest
    validation_result(
        strategy_worker(request_type(strategy=definition, config=parameters)), definition
    )


def validation_result(output: str, definition: StrategyDefinition) -> None:
    """Accept `null`, raise the author's refusal as `StrategyConfigRefused`, else refuse."""
    if output == "null":
        return
    try:
        document = json.loads(output)
    except ValueError:
        document = None
    if (
        not isinstance(document, dict)
        or document.get("refusal") != "invalid_config"
        or not isinstance(document.get("message"), str)
        or not isinstance(document.get("line"), (int, type(None)))
        or not isinstance(document.get("field", ""), str)
    ):
        raise RuntimeError("strategy validation worker returned an invalid response")
    raise StrategyConfigRefused(
        document["message"], definition.filename, document["line"], document.get("field")
    )


def strategy_capabilities(definition: StrategyDefinition, config):
    return connector_for(config).codec.capability.evaluate(definition, config)


def strategy_decision(definition: StrategyDefinition, context):
    output = strategy_worker(DecisionRequest(strategy=definition, context=context))
    return connector_for(context.config).codec.decision.model_validate_json(output)


class Runtime:
    # A strategy coordinator; operator_runtime.OperatorRuntime is the other kind.
    operator = False
    # Set by a tick that found nothing to do before the next scheduled decision: the
    # committed checkpoint it read (the tick wrote nothing) and when that decision is due.
    idle: tuple[Any, datetime] | None = None
    # A multi-chain coordinator advances one chain per tick. Each chain found idle since a
    # chain last had work maps to its next decision time (None for a closed chain); once a
    # whole cycle was idle the coordinator waits like an idle single-chain one.
    idle_chains: dict[str, datetime | None] | None = None
    # The run loop's worker timeout streak. A chain-scoped copy shares it, so an evaluation
    # on any chain ends the streak.
    streak: "TimeoutStreak | None" = None
    # The run loop's hold on a fee ceiling. It ends only when preparation admits a plan,
    # or when the decision waiting on it is gone; other ticks in between leave it as it is,
    # back-off included, so a decision dropped and decided again mid-hold keeps waiting
    # at the held pace instead of re-quoting every second.
    ceiling: "TickHolds | None" = None
    # `almanak test run` opts its coordinator in to logging what each decision saw
    # (`decision_facts`); a production coordinator never logs them.
    decision_facts = False

    def __init__(
        self,
        endpoint: str | None = None,
        auth_file: Path | None = None,
        *,
        gateway: GatewayClient | None = None,
        create=None,
        strategy: StrategyDefinition | None = None,
        expected=None,
        fault: Callable | None = None,
        clock: Callable[[], datetime] = utc_now,
        export: Path | None = None,
    ):
        self.export = export
        # A caller that holds the gateway service in its own process passes its client;
        # every other coordinator reaches the gateway over authenticated loopback.
        if gateway is None:
            if endpoint is None or auth_file is None:
                raise ValueError("gateway URL and authentication file are required")
            worker_command(key_file=auth_file)
            gateway = GatewayClient(endpoint, auth_file)
        self.gateway: GatewayClient | ScopedGateway = gateway
        self.chain_availability: tuple[ChainAvailability, ...] = ()
        self.live_bindings: tuple | None = None
        # Per-chain copies keep this pointer, so the declared series are read once.
        self.price_owner: Runtime = self
        self.journal: GatewayClient | ScopedGateway = self.gateway
        self.closed = False
        self.clock = clock
        self.fault = fault or (lambda stage, pending: None)
        try:
            if create is None:
                stored = self.journal.load()
                config = stored.config
            else:
                stored = None
                config = create
            self.connector = connector_for(config)
            if expected is not None and expected != config:
                raise ValueError(
                    "configuration changed; resume requires the stored validated configuration"
                )
            if stored is not None:
                if strategy is not None:
                    raise ValueError(
                        "resume uses its recorded strategy; code replacement is unsupported"
                    )
                if is_operator(stored) != self.operator:
                    raise ValueError("coordinator kind differs from the durable deployment")
                strategy = stored.strategy
            if (strategy is None) != self.operator:
                raise ValueError(
                    "an explicit recorded strategy is required; "
                    "only an operator coordinator executes without strategy code"
                )
            if create is not None:
                if strategy is not None and getattr(config, "preparation_digest", None) is None:
                    validate_strategy_config(strategy, config)
                self.journal.create(config, strategy)
            self.gateway.acquire()
            if not self.journal.load().work:
                self.gateway.audit_history(self.journal.load())
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.gateway.close()

    def tick(self) -> bool:
        if self.closed:
            raise RuntimeError("coordinator lease has been released")
        self.idle = None
        self.replanning().delay = 0.0
        state = self.journal.load()
        if state.work:
            return self.tick_chains(state)
        return self.tick_state(state)

    def tick_state(self, state):
        from almanak import external_effects

        if state.integrity_error is not None:
            raise RuntimeError("execution history invalidated: " + state.integrity_error)
        if state.delivered < len(state.outcomes):
            return self.deliver_outcome(state)
        if state.mode == "closed":
            return True
        if external_effects.progress(self, state):
            return False
        if state.plan is not None:
            return self.admit_signed_plan(state)
        if state.pending is not None:
            return self.resolve_pending(state.pending)
        if request_runtime.unresolved(state):
            return request_runtime.progress(self, state)
        if state.decision is not None:
            return self.prepare_decision(state)
        return self.decide(state)

    def tick_chains(self, state):
        if state.delivered < len(state.outcomes):
            return self.deliver_outcome(state)
        if state.mode == "closed":
            return True
        work = state.work[state.scheduler_cursor % len(state.work)]
        # A tick that raises (a worker timeout, a gateway failure) ends the idle cycle.
        idle, self.idle_chains = self.idle_chains or {}, None
        skipped = work.closed or bool(work.fault and work.fault.invalidated)
        due = None if skipped else self.advance_chain(state, work.chain)
        self.journal.update(
            lambda current: (
                current.model_copy(
                    update={"scheduler_cursor": (state.scheduler_cursor + 1) % len(state.work)}
                )
                if current.mode != "closed"
                else current
            )
        )
        current = self.journal.load()
        if not skipped and due is None:
            self.idle_chains = {}
        elif {**idle, work.chain: due}.keys() >= {item.chain for item in state.work}:
            deadlines = [at for at in (*idle.values(), due) if at is not None]
            fallback = self.clock() + timedelta(seconds=IDLE_POLL_SECONDS)
            # The cursor write is committed, so this read is the checkpoint the export reuses.
            self.idle = (current, min(deadlines, default=fallback))
            self.idle_chains = {}
        else:
            self.idle_chains = {**idle, work.chain: due}
        return current.mode == "closed"

    def advance_chain(self, state, chain) -> datetime | None:
        """Advance one chain; return its next decision time when it had nothing to do."""
        from copy import copy

        from almanak.gateway.routing import ScopedGateway
        from almanak.work import availability, project

        # Created before the copy, so every chain-scoped copy counts into the same record.
        self.replanning()
        scoped = copy(self)
        # Display only: a chain reports only what it observes itself, never the parent's readings.
        for name in ("observed", "prices"):
            setattr(scoped, name, None)
        scoped.gateway = scoped.journal = ScopedGateway(self.gateway, chain)
        scoped.chain_availability = availability(state)
        try:
            # Recovery audits committed receipts before touching retained attempts.
            scoped.gateway.audit_history(state)
            scoped.tick_state(project(state, chain))
        except DataError as exc:
            if isinstance(exc, ReplansExhausted) or exc.kind in GATEWAY_FAILURES:
                raise
            # The gateway has durably recorded this chain-local fault. No pending
            # identity is discarded, and the scheduler advances the next chain.
            return None
        finally:
            # Display only: the run loop exports the dashboard from this runtime, not the copy.
            # Each chain observes only its own allocations, so the export reads the latest
            # observation of every chain together.
            observed: tuple | None = getattr(scoped, "observed", None)
            prices = getattr(scoped, "prices", None)
            if observed is not None:
                by_chain: dict = getattr(self, "observed_by_chain", {})
                self.observed_by_chain = {**by_chain, chain: tuple(observed)}
                self.observed = tuple(o for part in self.observed_by_chain.values() for o in part)
            if prices is not None:
                self.prices = prices
        return scoped.idle[1] if scoped.idle is not None else None

    def admit_signed_plan(self, state):
        if (
            state.mode == "teardown"
            and self.connector.lifecycle.increases_exposure(state.plan.decision.intent)
            and not request_runtime.is_cancellation(state, state.plan.operation)
        ):
            # No broadcast can precede durable signed admission, so this unsigned plan
            # may be cancelled even if a signing response was lost.
            self.journal.update(lambda current: request_runtime.discard_plan(current))
            return False
        try:
            pending = self.gateway.sign()
        except DataError as exc:
            if exc.kind not in REPLANNED:
                raise
            self.count_replan(exc, "sign")
            self.journal.update(
                lambda current: (
                    request_runtime.discard_plan(current) if current == state else current
                )
            )
            return False
        # The re-plan budget runs until an attempt is sent: a signed attempt refused before
        # its first send keeps counting, so a sign-then-refuse loop still stops.
        self.fault("signed", pending)
        saved = self.journal.update(
            lambda current: (
                current.model_copy(update={"plan": None, "pending": pending})
                if current == state
                else current
            )
        )
        if saved.pending == pending:
            self.fault("prepared", pending)
        return False

    def resolve_pending(self, pending):
        receipt = self.gateway.receipt(pending)
        if receipt is None:
            if getattr(pending, "family", None) is not None:
                from almanak.replacement_runtime import progress

                return progress(self, pending)
            try:
                sent = self.gateway.broadcast(pending)
            except DataError as exc:
                if exc.kind == "transaction_expired":
                    return self.release_expired(pending)
                if not (is_unsent(pending) and exc.kind in CURRENT_REFUSALS):
                    raise
                return self.release_unsent(pending, exc)
            if sent:
                self.replanning().reset(self.route())
                self.fault("broadcast", pending)
            return False
        self.replanning().reset(self.route())
        outcome = self.gateway.reconcile(pending, receipt)
        self.fault("receipt", pending)
        self.journal.update(lambda current: commit_outcome(current, pending, outcome))
        self.log_transaction(outcome)
        self.fault("commit", pending)
        return False

    def release_unsent(self, pending, exc):
        # Never sent, the attempt is as discardable as an unsigned plan refused at signing:
        # it is released and the next decision plans afresh at its nonce. Every such
        # refusal counts, so a sign-then-refuse loop stops at the re-plan limit.
        self.count_replan(exc, "broadcast", CURRENT_REFUSALS)
        self.release_expired(pending)
        if self.operator:
            # No strategy decides again: the operator learns the request was dropped.
            raise DataError(
                exc.kind,
                f"{exc}; the request's signed step was released before it was sent,"
                " so nothing executed: submit the request again",
                code=exc.code,
            ) from exc
        return False

    def release_expired(self, pending):
        # Replaying it could never succeed. The gateway proves it was never sent or can
        # only revert before the release, and the next decision plans afresh at the nonce.
        saved = self.journal.update(
            lambda current: (
                request_runtime.release_pending(current) if current.pending == pending else current
            )
        )
        if saved.pending is None:
            self.fault("released", pending)
        return False

    def deliver_outcome(self, state):
        outcome = state.outcomes[state.delivered]
        context = self.connector.codec.outcome_input(
            config=state.config, memory=state.memory, result=outcome.strategy_result()
        )
        memory = Memory.model_validate_json(
            strategy_worker(OutcomeRequest(strategy=state.strategy, context=context))
        )
        self.worker_answered()
        self.fault("outcome", outcome)
        saved = self.journal.update(
            lambda current: (
                current.model_copy(update={"memory": memory, "delivered": state.delivered + 1})
                if current == state
                else current
            )
        )
        if saved.delivered == state.delivered + 1:
            self.fault("transition", outcome)
            if (
                not isinstance(outcome, external_effects.ExternalEffect)
                and not outcome.success
                and not state.work
            ):
                raise RuntimeError(
                    f"transaction reverted{revert_suffix(outcome)}; "
                    "outcome delivered, resume for recovery"
                )
        return False

    def decide(self, state):
        failed = [
            item
            for item in state.outcomes
            if not isinstance(item, external_effects.ExternalEffect) and not item.success
        ]
        if state.mode == "running" and failed:
            raise DataError(
                "execution_failed",
                f"recorded execution failure{revert_suffix(failed[-1])} blocks new exposure; "
                "request teardown",
            )
        if state.mode == "running" and any(item.accounting_issues for item in state.outcomes):
            raise DataError(
                "accounting",
                f"{accounting_refusal(state.outcomes)}; teardown remains available",
            )
        if state.initialized and state.mode == "running" and not state.schedule.due(self.clock()):
            self.idle = (state, state.schedule.next_due_at)
            return False
        observed = self.gateway.observe()
        # Display only: the dashboard export keeps each decision's reading for its charts.
        self.observed = observed
        if not state.initialized:
            return self.initialize(state, observed)
        return self.apply_decision(state, observed)

    def initialize(self, state, observed):
        validate_initial_balances(state, observed)
        self.journal.update(
            lambda current: current.model_copy(update={"initialized": True, "baseline": observed})
        )
        return False

    def apply_decision(self, state, observed):
        validate_observed_ownership(state, observed)
        state = self.retain_repayment(state, observed)
        event = state.schedule.event(self.clock(), teardown=state.mode == "teardown")
        if event is None:
            return False
        context = self.connector.codec.decision_input(
            config=state.config,
            memory=state.memory,
            owned=owned_for_decision(self.connector, state),
            observations=observed,
            availability=getattr(self, "chain_availability", ()) or availability(state),
            trigger=event,
        )
        # Teardown never depends on a vendor: its decisions receive no series.
        bindings = self.price_bindings(state) if state.mode == "running" else ()
        if bindings:
            try:
                prices = self.gateway.prices(bindings, decision_cutoff(event, observed))
            except DataError as exc:
                if exc.kind in GATEWAY_FAILURES:
                    raise
                # The series is unavailable for this decision: hold until the next one.
                report_price_hold(exc)
                try:
                    return self.hold_or_close(state, observed, state.memory, event)
                finally:
                    self.log_facts(context, price_error=exc)
            context = context.model_copy(update={"prices": prices})
            # Display only: the dashboard export keeps the series the decision read.
            self.prices = prices
        decision = strategy_decision(state.strategy, context)
        self.worker_answered()
        self.fault("evaluated", event)
        try:
            return self.record_decision(state, observed, decision, event)
        finally:
            self.log_facts(context, decision)

    def log_facts(self, context, decision=None, price_error=None) -> None:
        """Only a test run's coordinator logs decision facts, after the decision is kept."""
        if self.decision_facts:
            decision_facts.log(context, decision, price_error, chain=self.route())

    def log_transaction(self, outcome) -> None:
        """Only a test run's coordinator logs a transaction, after its outcome is committed."""
        if self.decision_facts:
            transaction_facts.log(outcome, getattr(self, "connector", None), self.clock)

    def price_bindings(self, state):
        """The declared series the gateway serves live, read once per coordinator."""
        owner = getattr(self, "price_owner", self)
        if getattr(owner, "live_bindings", None) is None:
            from almanak.backtest.strategy_prices import live_bindings

            owner.live_bindings = live_bindings(state.strategy, state.config)
        return owner.live_bindings

    def retain_repayment(self, state, observed):
        """Keep the latest repayment coverage and log when a shortfall starts or ends.

        A measurement only; it never refuses anything. The repayment itself refuses
        when unfunded.
        """
        measure = getattr(self.connector.lifecycle, "repayment_coverage", None)
        if measure is None:
            return state
        rows = latest_coverage(state.repayment, measure(state, observed), observed)
        if rows == state.repayment:
            return state
        updated = state.model_copy(update={"repayment": rows})
        saved = self.journal.update(lambda current: updated if current == state else current)
        # Another writer may have replaced the checkpoint; its coverage is not this one's.
        if saved == updated:
            for event in coverage_transitions(state.repayment, rows):
                log_event(**event)
        return saved

    def record_decision(self, state, observed, decision, event):
        if decision.intent is None:
            # The strategy no longer asks for an action that waited on a fee ceiling. A
            # price series hold never reaches the strategy, so it ends no such hold.
            self.ceiling_ended("dropped")
            return self.hold_or_close(state, observed, decision.memory, event)
        intent = decision.intent
        self.connector.codec.capability.admit(state.capabilities, intent)
        if intent.target != state.config.target(intent.target.allocation):
            raise ValueError("strategy proposed an intent for an unbound position")
        record = self.connector.codec.decision_record(
            identity=uuid4().hex,
            trigger=event,
            intent=intent,
            observations=observed,
            memory=decision.memory,
            required_observations=decision.required_observations,
        )
        from almanak.work import validate_dependencies

        validate_dependencies(record)
        self.commit_decision(state, {"decision": record, "memory": decision.memory}, event)
        return False

    def commit_decision(self, state, update, event):
        update["schedule"] = state.schedule.after(state.config.decision_trigger, event)
        saved = self.journal.update(
            lambda current: current.model_copy(update=update) if current == state else current
        )
        if saved == state.model_copy(update=update):
            self.fault("decision_committed", event)
        return saved

    def prepare_decision(self, state):
        intent = state.decision.intent
        if state.mode == "teardown" and self.connector.lifecycle.increases_exposure(intent):
            # Teardown never admits more exposure; the refusal is logged so a strategy that
            # keeps proposing it is visible, and the decision is dropped as before.
            log_event(
                "exposure_refused_in_teardown",
                intent=getattr(intent, "kind", type(intent).__name__),
                allocation=intent.target.allocation,
                reason="teardown admits no intent that increases exposure",
            )
            self.journal.update(
                lambda current: (
                    current.model_copy(update={"decision": None}) if current == state else current
                )
            )
            self.ceiling_ended("dropped")
            return False
        try:
            plan = self.gateway.prepare(state, intent)
        except DataError as exc:
            if exc.kind in CEILING_HOLDS:
                # The fee check follows preparation's simulation and market checks, so this
                # attempt passed them: earlier refusals at preparation are not consecutive
                # with a later one. A refusal at signing or broadcast ran checks preparation
                # does not repeat, so a count that includes one is kept until an attempt is
                # sent.
                budget = self.replanning()
                if budget.stages.get(self.route(), set()) <= {"prepare"}:
                    budget.reset(self.route())
                raise CeilingHold(exc.kind, str(exc)) from exc
            if exc.kind not in REPLANNED:
                raise
            self.count_replan(exc, "prepare")
            # Nothing is signed yet, so the expired decision is dropped as it is at signing,
            # and the next decision plans afresh. Retrying it could only be refused again.
            self.journal.update(
                lambda current: (
                    request_runtime.discard_plan(current) if current == state else current
                )
            )
            return False
        saved = self.journal.update(
            lambda current: (
                current.model_copy(update={"plan": plan}) if current == state else current
            )
        )
        if saved.plan == plan:
            self.ceiling_ended("recovered")
            self.fault("reserved", plan)
        return False

    def ceiling_ended(self, outcome: str) -> None:
        """End a fee-ceiling hold: its plan was admitted, or its decision was dropped."""
        if self.ceiling is not None:
            self.ceiling.clear(outcome)

    def replanning(self) -> Replanning:
        # Lazy, so orchestration-only construction in tests needs no setup.
        return vars(self).setdefault("_replanning", Replanning())

    def route(self):
        return getattr(self.gateway, "chain", None)

    def count_replan(self, exc, stage, counted=MARKET_REFUSALS):
        """Allow one more re-plan after a counted refusal, or stop with its reason."""
        if exc.kind not in counted:
            return
        budget = self.replanning()
        attempt = budget.attempts.get(self.route(), 0) + 1
        budget.attempts[self.route()] = attempt
        budget.stages.setdefault(self.route(), set()).add(stage)
        print(
            json.dumps(
                {
                    "event": "unsigned_plan_refused",
                    "kind": exc.kind,
                    "stage": stage,
                    "attempt": attempt,
                    "limit": REPLAN_ATTEMPTS,
                    "reason": str(exc),
                }
            ),
            file=sys.stderr,
            flush=True,
        )
        if attempt >= REPLAN_ATTEMPTS:
            raise ReplansExhausted(exc.kind, str(exc)) from exc
        budget.delay = attempt * REPLAN_BACKOFF_SECONDS

    def hold_or_close(self, state, observed, memory, event):
        update = {"memory": memory}
        if state.mode == "teardown":
            validate_closure(state, observed)
            update.update(mode="closed", closure=observed)
        saved = self.commit_decision(state, update, event)
        return saved.mode == "closed"

    def worker_answered(self) -> None:
        """A strategy worker evaluation completed; only this ends a timeout streak."""
        if self.streak is not None:
            self.streak.clear()

    def run(self, interval: float = 1, *, interrupt: Callable[[], None] = lambda: None):
        # A tick that evaluates nothing (no chain due, a gateway step) leaves the streak as
        # it is, so it cannot reset the no-success horizon or report a false recovery.
        self.streak = streak = TimeoutStreak()
        holds = TickHolds()
        self.ceiling = ceiling = TickHolds("gas_ceiling", horizon=None)
        try:
            while True:
                interrupt()
                try:
                    finished = self.tick()
                    holds.clear()
                except WorkerWallTimeout as error:
                    streak.record(self, error)
                    finished = False
                except DataError as error:
                    if isinstance(error, CeilingHold):
                        # The gateway answered, so a read hold has ended.
                        holds.clear()
                        ceiling.record(self, error)
                    elif error.transient:
                        holds.record(self, error)
                    else:
                        raise
                    finished = False
                finally:
                    retain_dashboard_input(self)
                if finished:
                    return self.journal.load()
                held = streak.attempts or holds.attempts or ceiling.attempts
                wait(next_delay(self, interval, held), interrupt)
        finally:
            self.close()

    def retain_dashboard_input(self):
        retain_dashboard_input(self)


def retain_dashboard_input(runtime):
    """Publish the projected dashboard input; display export never blocks execution."""
    directory = getattr(runtime, "export", None)
    if directory is None:
        return
    from almanak.dashboard.export import export_input

    try:
        # An idle tick wrote nothing, so the checkpoint it read is still the committed one.
        idle = getattr(runtime, "idle", None)
        state = runtime.journal.load() if idle is None else idle[0]
        observed, runtime.observed = getattr(runtime, "observed", None), None
        prices, runtime.prices = getattr(runtime, "prices", None), None
        export_input(directory, state, observed=observed, prices=prices)
    except Exception as exc:  # noqa: BLE001 - reported, never fatal for the run
        print(
            json.dumps({"event": "dashboard_export_failed", "reason": type(exc).__name__}),
            file=sys.stderr,
            flush=True,
        )


def next_delay(runtime, interval: float, timeouts: int) -> float:
    """Seconds until the next tick: back off after a timeout, poll slowly while idle."""
    if timeouts:
        return min(IDLE_POLL_SECONDS * 2 ** (timeouts - 1), TIMEOUT_BACKOFF_CAP_SECONDS)
    replanning = vars(runtime).get("_replanning")
    if replanning is not None and replanning.delay:
        return replanning.delay
    idle = getattr(runtime, "idle", None)
    if idle is None:
        return interval
    until_due = (idle[1] - runtime.clock()).total_seconds()
    return min(max(until_due, 0.0), IDLE_POLL_SECONDS)


def wait(seconds: float, interrupt: Callable[[], None]) -> None:
    """Sleep in slices of at most one second, honoring a deferred Ctrl-C between them."""
    while seconds > 0:
        step = min(seconds, 1.0)
        time.sleep(step)
        seconds -= step
        interrupt()


class TimeoutStreak:
    """Consecutive worker timeouts of one coordinator; a completed evaluation ends it."""

    def __init__(self):
        self.attempts = 0
        # Monotonic time of the streak's first timeout; meaningful while attempts > 0.
        self.since = 0.0

    def record(self, runtime, error: WorkerWallTimeout) -> None:
        """Log one timeout as a degraded coordinator, or stop once the streak is final."""
        now = time.monotonic()
        if not self.attempts:
            self.since = now
        self.attempts += 1
        elapsed = now - self.since
        mode = runtime.journal.load().mode
        final = mode == "running" and elapsed >= WORKER_TIMEOUT_HORIZON_SECONDS
        log_event(
            "strategy_worker_timeout",
            status="stopped" if final else "degraded",
            kind="startup" if isinstance(error, WorkerStartupTimeout) else "wall",
            mode=mode,
            attempt=self.attempts,
            elapsed_seconds=round(elapsed),
            retry_in_seconds=None if final else next_delay(runtime, 0, self.attempts),
            stops_after_seconds=WORKER_TIMEOUT_HORIZON_SECONDS if mode == "running" else None,
        )
        if final:
            raise WorkerWallTimeout(
                f"{error}: every evaluation for {round(elapsed)} seconds "
                f"({self.attempts} attempts) timed out"
            ) from error

    def clear(self) -> None:
        if self.attempts:
            log_event(
                "strategy_worker_recovered",
                attempts=self.attempts,
                elapsed_seconds=round(time.monotonic() - self.since),
            )
        self.attempts = 0


class TickHolds:
    """Consecutive ticks held on a refusal a later tick may pass, logged as `{prefix}_*`.

    By default the refusal is a provider read that outlasted the gateway's retries, and a
    running deployment stops once the hold reaches `horizon` seconds. A fee quote above a
    per-transaction ceiling holds with no horizon (`CEILING_HOLDS`).

    A held tick commits nothing: on a single-chain deployment the failed gateway request
    wrote no state, so the next tick repeats the same step from the same checkpoint. A
    pending transaction is only looked up and reconciled again, or a retained signed
    transaction the provider does not hold is re-sent unchanged, so a hold never creates a
    new economic action. A tick that completes ends a read hold; a fee-ceiling hold ends
    only when its plan is admitted or its decision is dropped (`Runtime.ceiling_ended`).
    """

    def __init__(
        self, prefix: str = "transient_read", horizon: int | None = TRANSIENT_HOLD_HORIZON_SECONDS
    ):
        self.prefix = prefix
        self.horizon = horizon
        self.attempts = 0
        # Monotonic time of the first held tick; meaningful while attempts > 0.
        self.since = 0.0
        # Whether this hold has logged that it outlasted the horizon without stopping.
        self.escalated = False

    def record(self, runtime, error: DataError) -> None:
        """Log one held tick, or re-raise `error` once a running deployment's hold is final.

        Past the horizon a hold that keeps going, as teardown's does, logs one
        `{prefix}_hold_escalated` event so an operator is told it is not recovering.
        """
        now = time.monotonic()
        if not self.attempts:
            self.since = now
        self.attempts += 1
        elapsed = now - self.since
        mode = hold_mode(runtime)
        bounded = self.horizon is not None and mode == "running"
        past = self.horizon is not None and elapsed >= self.horizon
        final = bounded and past
        log_event(
            self.prefix + "_hold",
            status="stopped" if final else "held",
            kind=error.kind,
            reason=str(error),
            mode=mode,
            attempt=self.attempts,
            elapsed_seconds=round(elapsed),
            retry_in_seconds=None if final else next_delay(runtime, 0, self.attempts),
            stops_after_seconds=self.horizon if bounded else None,
        )
        if final:
            raise error
        if past and not self.escalated:
            self.escalated = True
            log_event(
                self.prefix + "_hold_escalated",
                kind=error.kind,
                reason=str(error),
                mode=mode,
                attempts=self.attempts,
                elapsed_seconds=round(elapsed),
            )

    def clear(self, outcome: str = "recovered") -> None:
        if self.attempts:
            log_event(
                f"{self.prefix}_{outcome}",
                attempts=self.attempts,
                elapsed_seconds=round(time.monotonic() - self.since),
            )
        self.attempts = 0
        self.escalated = False


def hold_mode(runtime) -> str | None:
    """The deployment mode, or None while the gateway cannot answer that read either."""
    try:
        return runtime.journal.load().mode
    except DataError as error:
        if not error.transient:
            raise
        return None


def log_event(event: str, **fields) -> None:
    print(json.dumps({"event": event, **fields}), file=sys.stderr, flush=True)


def revert_suffix(outcome) -> str:
    """The decoded inner revert a gateway recorded for a failed transaction, if any."""
    reason = getattr(outcome, "revert_reason", None)
    return f" ({reason})" if reason else ""


def commit_outcome(current, pending, outcome):
    if current.pending != pending:
        raise RuntimeError("pending transaction changed during reconciliation")
    ownership = connector_for(current.config).lifecycle.ownership(current, outcome)
    has_request = request_runtime.unresolved(current.model_copy(update=ownership))
    return current.model_copy(
        update={
            "pending": None,
            "outcomes": (*current.outcomes, outcome),
            **request_runtime.settle_released(current, pending),
            **ownership,
            "decision": pending.decision
            if has_request or (outcome.success and not outcome.strategy_result().complete)
            else None,
        }
    )


def validate_initial_balances(state, observed):
    connector_for(state.config).lifecycle.validate_initial(state, observed)


def validate_observed_ownership(state, observed):
    connector_for(state.config).lifecycle.validate_owned(state, observed)


def validate_closure(state, observed):
    connector_for(state.config).lifecycle.validate_closure(state, observed)


def owned_for_decision(connector, state):
    """The owned records a decision reads; a connector with shared fund cash projects it."""
    project = getattr(connector.lifecycle, "decision_owned", None)
    return state.owned if project is None else project(state)


# Gateway or request failures, as opposed to a refusal or source failure the gateway
# recorded for one chain or one decision.
GATEWAY_FAILURES = ("gateway_unavailable", "internal", "conflict", "invalid_request")


def report_price_hold(error: DataError):
    print(
        json.dumps({"event": "price_series_hold", "kind": error.kind, "reason": str(error)}),
        file=sys.stderr,
        flush=True,
    )


def decision_cutoff(event, observed) -> int:
    """No input later than the trigger or the oldest observed block reaches a decision."""
    from almanak.models import Block

    times = [int(event.started_at.timestamp())]
    for snapshot in observed:
        # Some snapshots nest their block (an Aave V4 position's reserve); every
        # snapshot states the block time it observed as `effective_at`.
        effective = getattr(snapshot, "effective_at", None)
        if effective is not None:
            times.append(int(effective.timestamp()))
        values = [snapshot, *(getattr(snapshot, name) for name in type(snapshot).model_fields)]
        for value in values:
            block = value if isinstance(value, Block) else getattr(value, "block", None)
            if isinstance(block, Block):
                times.append(int(block.timestamp))
    return min(times)
