"""Coordinator for an operator deployment: one admitted operator request at a time.

An operator request replaces the strategy only as the source of a decision. The
decision is retained before planning, the reserved plan can be shown and discarded
before anything is signed, and signing, broadcast, receipt and reconciliation are
the shared runtime path. Interrupted work stays recorded; the next coordinator
resolves it before it admits another request, and never repeats it.
"""

import time
from uuid import uuid4

from almanak import request_runtime
from almanak.gateway.ethereum import DataError
from almanak.operator_deployment import accounting_blocked, accounting_refusal, request_executed
from almanak.runtime import Runtime, validate_closure, validate_observed_ownership
from almanak.triggers import OperatorEvent
from almanak.work import validate_dependencies


class OperatorRuntime(Runtime):
    operator = True
    # Seconds between checks for an unmined signed step.
    poll = 1.0
    # The latest refusal that dropped the active request's plan; nobody re-decides it.
    refused: DataError | None = None

    def count_replan(self, exc, *arguments, **options):
        self.refused = exc
        super().count_replan(exc, *arguments, **options)

    def decide(self, state):
        """No strategy decides; an idle deployment waits for the next operator request."""
        if not state.initialized:
            return self.initialize(state, self.gateway.observe())
        return True

    def initialize(self, state, observed):
        """Adopt the account's existing position instead of requiring a pristine fund.

        An operator acts on what the account holds now. The gateway re-reads the baseline
        and accepts only the ownership that baseline states.
        """
        owned = self.connector.lifecycle.adopt(state.owned, observed)
        self.journal.update(
            lambda current: current.model_copy(
                update={"initialized": True, "baseline": observed, "owned": owned}
            )
        )
        return False

    def deliver_outcome(self, state):
        # There is no strategy transition to apply: delivery only acknowledges the outcome.
        outcome = state.outcomes[state.delivered]
        self.fault("outcome", outcome)
        saved = self.journal.update(
            lambda current: (
                current.model_copy(update={"delivered": state.delivered + 1})
                if current == state
                else current
            )
        )
        if saved.delivered == state.delivered + 1:
            self.fault("transition", outcome)
        return False

    def recover(self):
        """Resolve signed steps and deliver recorded outcomes; never plan or sign a new one.

        A request that was admitted but not confirmed stays retained and unexecuted:
        the caller resumes or discards it explicitly.
        """
        while True:
            state = self.journal.load()
            if state.mode == "closed" or not self.retained(state):
                return state
            self.tick()
            self.wait(state)

    @staticmethod
    def retained(state) -> bool:
        return (
            not state.initialized
            or state.delivered < len(state.outcomes)
            or state.pending is not None
        )

    def wait(self, state):
        # Only an unmined signed step is worth waiting for; everything else is local.
        if state.pending is not None and self.poll:
            time.sleep(self.poll)

    def submit(self, intent, request: str | None = None):
        """Retain one operator request as the active decision, before any planning."""
        state = self.recover()
        if state.mode == "closed":
            raise ValueError("deployment is closed")
        if state.decision is not None:
            raise ValueError("an earlier operator request is unfinished; resume or discard it")
        if state.work:
            raise ValueError("multi-chain operator deployments are not supported")
        increases = self.connector.lifecycle.increases_exposure(intent)
        if state.mode == "teardown" and increases:
            raise ValueError("teardown admits only operator requests that reduce exposure")
        if increases and accounting_blocked(state.outcomes):
            raise DataError(
                "accounting",
                f"{accounting_refusal(state.outcomes)}; reducing requests remain",
            )
        event = OperatorEvent(started_at=self.clock(), request=request or uuid4().hex)
        if request_executed(state.outcomes, event):
            raise ValueError("operator request was already executed")
        observed = self.gateway.observe()
        validate_observed_ownership(state, observed)
        self.connector.codec.capability.admit(state.capabilities, intent)
        if intent.target != state.config.target(intent.target.allocation):
            raise ValueError("operator request names an unbound position")
        record = self.connector.codec.decision_record(
            identity=uuid4().hex,
            trigger=event,
            intent=intent,
            observations=observed,
            memory=state.memory,
        )
        validate_dependencies(record)
        saved = self.journal.update(
            lambda current: (
                current.model_copy(update={"decision": record}) if current == state else current
            )
        )
        if saved.decision != record:
            raise DataError("conflict", "deployment changed; the operator request was not admitted")
        self.fault("decision_committed", event)
        return record

    def preview(self):
        """Plan the active request's next step and return it, unsigned."""
        state = self.journal.load()
        if state.decision is None or state.pending is not None:
            raise ValueError("preview requires an admitted request with no signed step")
        if state.plan is None:
            self.prepare_decision(state)
            state = self.journal.load()
        if state.plan is None:
            # Preparation refused the request and dropped it; a strategy would decide
            # again, an operator has to be told why.
            refused = self.refused
            if refused is None:
                raise DataError("plan_refused", "the request was refused during planning")
            raise DataError(refused.kind, f"the request was refused during planning: {refused}")
        return state.plan

    def discard(self):
        """Drop the active request while nothing is signed; no external effect exists."""
        state = self.journal.load()
        if state.pending is not None:
            raise ValueError("a signed step cannot be discarded; resolve it instead")
        if request_runtime.unresolved(state):
            raise ValueError("an accepted protocol request must be resolved, not discarded")
        self.journal.update(
            lambda current: (
                current.model_copy(update={"plan": None, "decision": None})
                if current == state
                else current
            )
        )

    def execute(self, interrupt=lambda: None):
        """Run the admitted request to its outcome; returns the outcomes it produced.

        `interrupt` is called between steps and may raise to stop; the journal keeps
        whatever was signed.
        """
        state = self.journal.load()
        if state.decision is None and not self.retained(state):
            raise ValueError("no operator request is admitted")
        before = len(state.outcomes)
        while not self.tick():
            interrupt()
            self.wait(self.journal.load())
        return self.journal.load().outcomes[before:]

    def finish(self):
        """Close the deployment once every obligation it owns is verifiably gone."""
        state = self.recover()
        if state.mode == "closed":
            return state
        if state.decision is not None:
            raise ValueError("an earlier operator request is unfinished; resume or discard it")
        observed = self.gateway.observe()
        validate_closure(state, observed)
        return self.journal.update(
            lambda current: (
                current.model_copy(update={"mode": "closed", "closure": observed})
                if current == state
                else current
            )
        )
