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
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
from almanak.operator_deployment import accounting_blocked, accounting_refusal, request_executed
from almanak.runtime import Runtime, validate_closure, validate_observed_ownership
from almanak.triggers import OperatorEvent
from almanak.work import validate_dependencies


class OperatorRuntime(Runtime):
    operator = True
    # An operator deployment runs on one chain, so its client is never chain-scoped.
    gateway: GatewayClient
    # Seconds between checks for an unmined signed step.
    poll = 1.0
    # The latest refusal that dropped the active request's plan; nobody re-decides it.
    refused: DataError | None = None
    # Seconds `execute` spends on ticks that end in a refused consumed-nonce proof, per
    # signed step, before that refusal ends it. Long enough for an endpoint's nodes to
    # agree on a transaction that was just mined. Not tied to a finality margin: where
    # the margin is shorter, as on Avalanche, the proof may succeed within the wait.
    proof_wait = 60.0
    # The signed step whose proof was refused, and the seconds spent on it so far.
    proof_spent: tuple[str, float] | None = None
    # The latest refusal that came out of the consumed-nonce proof or its recording.
    unproven: DataError | None = None
    # Seconds `execute` spends on ticks that end in a passing failure between two steps
    # of one request, once a step of it is mined, before that failure ends it. Long
    # enough for several whole retry budgets of a read and a short throttle window.
    # Short beside a calldata deadline, which the held time still runs down: with the
    # default 300 second window, signing refuses a step with less than 150 seconds left.
    step_wait = 60.0
    # The request whose failure between steps was held, and the seconds spent so far.
    step_spent: tuple[str, float] | None = None
    monotonic = staticmethod(time.monotonic)

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

    def resolve_pending(self, pending):
        try:
            return super().resolve_pending(pending)
        except DataError as exc:
            if exc.kind != "nonce_conflict":
                raise
        try:
            return self.drop_superseded(pending)
        except DataError as exc:
            self.unproven = exc
            raise

    def drop_superseded(self, pending):
        """Drop a signed step whose nonce another, final transaction took.

        Such a step can never be mined, so sending it again or waiting resolves nothing.
        The gateway names the transaction that took the nonce, and refuses with
        `nonce_conflict` while that is not proven. Nothing is planned in its place: the
        request ends and the operator states it again.
        """
        evidence = self.gateway.superseded(pending)
        if evidence.transaction != pending.tx_hash:
            raise DataError("inconsistent", "the gateway's proof names another signed step")
        saved = self.journal.update(
            lambda current: (
                request_runtime.drop_superseded(current, evidence)
                if current.pending == pending
                else current
            )
        )
        if saved.pending is None:
            self.fault("released", pending)
        return False

    def recover(self, interrupt=lambda: None):
        """Resolve signed steps and deliver recorded outcomes; never plan or sign a new one.

        A request that was admitted but not confirmed stays retained and unexecuted:
        the caller resumes or discards it explicitly. `interrupt` is called before every
        step and may raise to stop.
        """
        while True:
            state = self.journal.load()
            if state.mode == "closed" or not self.retained(state):
                # Nothing is left to do, so a stop requested meanwhile ends nothing: the
                # caller still reports what was resolved or dropped.
                return state
            interrupt()
            self.held_tick()
            self.wait(self.journal.load())

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
        """Retain one operator request as the active decision, before any planning.

        `intent` is the intent itself, or a function of the checkpoint and the
        observations the request is admitted with that returns it. An intent whose
        bounds come from an observed quote is built that way, so the gateway checks
        it against the same observations. Every refusal that needs no observation
        comes before the read; a built intent is checked for new exposure after it.
        """
        state = self.recover()
        if state.mode == "closed":
            raise ValueError("deployment is closed")
        if state.decision is not None:
            raise ValueError("an earlier operator request is unfinished; resume or discard it")
        if state.work:
            raise ValueError("multi-chain operator deployments are not supported")
        built = callable(intent)
        if not built:
            self.refuse_new_exposure(state, intent)
        event = OperatorEvent(started_at=self.clock(), request=request or uuid4().hex)
        if request_executed(state.outcomes, event):
            raise ValueError("operator request was already executed")
        observed = self.gateway.observe()
        validate_observed_ownership(state, observed)
        if built:
            intent = intent(state, observed)
            self.refuse_new_exposure(state, intent)
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

    def refuse_new_exposure(self, state, intent) -> None:
        """Refuse a request that adds exposure in teardown or over an accounting issue."""
        if not self.connector.lifecycle.increases_exposure(intent):
            return
        if state.mode == "teardown":
            raise ValueError("teardown admits only operator requests that reduce exposure")
        if accounting_blocked(state.outcomes):
            raise DataError(
                "accounting",
                f"{accounting_refusal(state.outcomes)}; reducing requests remain",
            )

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

        `interrupt` is called before every step and may raise to stop, so a stop
        requested while waiting on a signed step is honoured before anything more is
        planned, signed or sent. The journal keeps whatever was signed.

        A transient failure while a signed step is unresolved is held and asked again:
        giving up there would leave a sent transaction without its outcome over one
        failed read. So is any refusal of the consumed-nonce proof, for `proof_wait`
        seconds per signed step: the step's own transaction, mined but not yet served
        by every node of the endpoint, looks the same as another transaction at its
        nonce, and the next tick reads its receipt first. So is a passing failure
        between two steps of a request that has a mined step, for `step_wait` seconds
        per request (`awaits_step`).
        """
        state = self.journal.load()
        if state.decision is None and not self.retained(state):
            raise ValueError("no operator request is admitted")
        before = len(state.outcomes)
        while True:
            interrupt()
            began = self.monotonic()
            try:
                done = self.held_tick()
            except DataError as exc:
                if not self.asks_again(exc, began):
                    raise
                done = False
            if done:
                break
            self.wait(self.journal.load())
        return self.journal.load().outcomes[before:]

    def asks_again(self, exc: DataError, began: float) -> bool:
        """Whether `execute` runs another tick after the one that ended in `exc`."""
        if exc is self.unproven:
            return self.awaits_proof(began)
        return passing(exc) and self.awaits_step(exc, began)

    def awaits_step(self, exc: DataError, began: float) -> bool:
        """Whether a passing failure between two steps is asked again instead of ending.

        Only for a request with a step already mined: ending there leaves that step
        standing without the rest, over one failed read. Before any step is mined
        nothing is on chain, and the failure ends the command at once.

        Only while no signed step is recorded. With no plan either, the next tick can
        only deliver the recorded outcome, or observe and plan the next step afresh: a
        failed preparation recorded nothing.

        With a plan recorded the failure arose at signing or at the admission of the
        signed bytes, and the next tick signs again. That is a signature of the same
        transaction: the gateway signs the recorded plan's own call, nonce, gas and gas
        price, and the plan cannot change, since replacing it means discarding it, which
        ends an operator request. A signature made before the failure is in no record
        and is never sent, because only a recorded signed step is. A direct command
        signs with the local keystore, whose signature of one transaction is always the
        same bytes; of two different signatures of one transaction only one could be
        mined. The gateway checks the plan at every
        signature as it does at the first: its observation's age, the state it rests
        on, the fee ceilings and the margin before a calldata deadline. A refusal there
        ends the command at once. No fee-replacement candidate exists at this point:
        those are signed only for a recorded signed step.

        Not for a plan that is not a chain transaction, such as a venue action: its
        signing was not examined for this, and no direct action plans one.

        Until the ticks that ended in such a failure for this request, each with the
        pause after it, have taken `step_wait` seconds. A tick that ends another way
        spends none of it. Then the failure ends the command, and says that the
        mined steps stand.
        """
        try:
            state = self.journal.load()
        except DataError:
            # The checkpoint cannot be read either; the failure that was met ends it.
            raise exc from None
        mined = mined_steps(state)
        if not mined or state.pending is not None or not transaction_or_none(state.plan):
            return False
        request = state.decision.identity
        spent = self.monotonic() - began + self.poll
        if self.step_spent is not None and self.step_spent[0] == request:
            spent += self.step_spent[1]
        self.step_spent = (request, spent)
        if spent >= self.step_wait:
            raise DataError(
                exc.kind,
                f"{exc}; the failure did not pass within {self.step_wait:g} seconds."
                f" Steps of the request already mined: {mined}. They stand: state the"
                " command again, and it is planned against what the wallet holds now",
                code=exc.code,
                transient=exc.transient,
                details=exc.details,
            ) from exc
        if self.poll:
            time.sleep(self.poll)
        return True

    def awaits_proof(self, began: float) -> bool:
        """Whether a refused consumed-nonce proof is asked again instead of ending.

        Only while the signed step is still recorded, and until the ticks that ended in
        such a refusal for that step, each with the pause after it, have taken
        `proof_wait` seconds. A tick that ends another way, such as a held transient
        failure, spends none of it, and cannot extend it: every refusal spends its own
        tick. The step stays recorded throughout. The next tick may send the same
        signed bytes again, as every tick without a receipt does; nothing new is
        planned or signed, and only the gateway's full proof drops the step. A step
        with a fee-replacement family is not waited for: its ticks may sign another
        candidate at the same nonce, and the proof refuses every family.
        """
        pending = self.journal.load().pending
        if pending is None or getattr(pending, "family", None) is not None:
            return False
        spent = self.monotonic() - began + self.poll
        if self.proof_spent is not None and self.proof_spent[0] == pending.tx_hash:
            spent += self.proof_spent[1]
        self.proof_spent = (pending.tx_hash, spent)
        return spent < self.proof_wait

    def held_tick(self) -> bool:
        """One tick; a passing failure while a signed step waits is not an ending."""
        try:
            return self.tick()
        except DataError as exc:
            if not self.holds(exc):
                raise
            return False

    def holds(self, exc: DataError) -> bool:
        """Whether a failure is a passing one met while a signed step waits."""
        return passing(exc) and self.journal.load().pending is not None

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


def passing(exc: DataError) -> bool:
    """Whether the same request may pass a failure later: a provider or storage failure."""
    return exc.transient or exc.kind == "gateway_unavailable"


def transaction_or_none(plan) -> bool:
    """Whether nothing is planned, or the plan is a chain transaction with its nonce."""
    return plan is None or getattr(plan, "nonce", None) is not None


def mined_steps(state) -> int:
    """How many recorded outcomes belong to the active request, from the checkpoint.

    An outcome carries the decision its step was signed under, so this needs nothing
    the process remembers. A request whose decision is still active after an outcome
    has more steps to go.
    """
    if state.decision is None:
        return 0
    request = state.decision.identity
    decisions = (getattr(item, "decision", None) for item in state.outcomes)
    return sum(getattr(decision, "identity", None) == request for decision in decisions)
