"""Custody requests as the connector's `Requests` (blueprint 30, sections 4.4 and 13.1).

A custody transaction's receipt shows only that HyperCore was asked; the request it
queued stays unresolved until its own effect is identified (`custody.reconcile`). The
runtime's request step reconciles it as it reconciles a GMX keeper order: the decision
waits, and its next step is planned once the request resolves. Requests are stepwise: an
intermediate one resolves and the intent continues, so `request_runtime` keeps the
decision for it. A queued CoreWriter action cannot be withdrawn, so there is no
cancellation.

A resolution rests on `/info` answers that never repeat (a standard mode or agent answer
carries the time it was received), so the gateway retains the outcome it derived for a
checkpoint and verifies the coordinator's commit of that checkpoint against it, as it
retains a node gas-price reading for the re-preparation of the same attempt.
"""

import hashlib

from almanak.connectors.hyperliquid import planning
from almanak.connectors.hyperliquid.agents import Approval
from almanak.connectors.hyperliquid.custody import CustodyRequests, with_resolution
from almanak.connectors.hyperliquid.custody import reconcile as custody_reconcile
from almanak.connectors.hyperliquid.operations import Withdrawal
from almanak.connectors.hyperliquid.state import CustodyOutcome, TransactionOutcome
from almanak.effects import AssetDelta, WalletAsset
from almanak.gateway.ethereum import DataError


def unresolved(state) -> bool:
    return CustodyRequests.unresolved(state.custody)


def reconcile(backend, state):
    """The outcome of the custody request or agent approval whose effect is now identified;
    None while it may still appear. Past its window it halts with its evidence."""
    if not unresolved(state):
        return None
    key = ("custody", hashlib.sha256(state.model_dump_json().encode()).hexdigest())
    return backend.retained(key, lambda: resolution(backend, state))


def resolution(backend, state):
    (owned,) = state.owned
    custody = custody_reconcile(backend.custody_backend(owned.target.account), state.custody)
    if custody is None:
        return None
    resolved = newly_resolved(state.custody, custody)
    if resolved is None:
        return None
    # The outcome carries only what resolved; the checkpoint's document takes it in.
    found = {"approval" if isinstance(resolved, Approval) else "request": resolved}
    if with_resolution(state.custody, **found) != custody:
        raise DataError("inconsistent", "a custody reconciliation changed more than it resolved")
    queued = queuing(state)
    operation = queued.operation
    before = queued.after.model_copy(update={"owned": owned})
    after, assets = before, ()
    if isinstance(operation, Withdrawal):
        after, assets = paid(backend, resolved, before, operation)
    return CustodyOutcome(
        decision=state.decision,
        before=before,
        after=after,
        success=True,
        complete=planning.completes(state.decision.intent, operation),
        assets=assets,
        operation=operation,
        **found,
    )


def newly_resolved(before, after):
    """The one request or agent approval this reconciliation resolved."""
    settled = {r.key for r in before.requests if r.status == "resolved"}
    found = [r for r in after.requests if r.status == "resolved" and r.key not in settled]
    waiting = {a.address for a in before.agents.approvals if a.status == "requested"}
    found += [a for a in after.agents.approvals if a.address in waiting and a.status != "requested"]
    if len(found) > 1:
        raise DataError("inconsistent", "one custody step resolves at a time")
    return found[0] if found else None


def queuing(state) -> TransactionOutcome:
    """The decision's latest mined custody call: the one whose request is outstanding."""
    for outcome in reversed(state.outcomes):
        if (
            isinstance(outcome, TransactionOutcome)
            and outcome.decision.identity == state.decision.identity
            and outcome.success
        ):
            return outcome
    raise ValueError("an unresolved custody request lacks the decision's mined call")


def paid(backend, request, before, operation):
    """A withdrawal is paid to the Safe on HyperEVM in the block after its own: the
    snapshot at that payout block, with the amount back in the owned inventory."""
    payout = request.evidence[1]
    block = backend.reader.pin_block(payout["block"])
    amount = operation.amount.amount
    (held,) = before.owned.inventory
    owned = before.owned.model_copy(
        update={"inventory": (held.model_copy(update={"amount": held.amount + amount}),)}
    )
    after = backend.snapshot(operation.allocation, block, owned)
    target = operation.target
    key_asset = WalletAsset(
        account=target.account, chain=target.fund.chain, asset=target.margin_asset
    )
    return after, (AssetDelta(key=key_asset, amount=amount),)


class Requests:
    # An intermediate custody request resolves and its intent continues with its next step.
    stepwise = True
    unresolved = staticmethod(unresolved)
    reconcile = staticmethod(reconcile)

    @staticmethod
    def fork_keeper(forks, output):
        """Fork runs only: the keeper face of the workspace's `hypercore-model` process,
        the synthesized HyperCore of blueprint 30, section 9.1."""
        from almanak.connectors.hyperliquid.fork_venue import ModelKeeper

        return ModelKeeper.open(forks, output)

    @staticmethod
    def cancellation_plan(backend, state, attempt_id, *, reference=None):
        # A queued CoreWriter action cannot be withdrawn.
        return None

    @staticmethod
    def is_cancellation(state, operation) -> bool:
        return False

    @staticmethod
    def project(state, chain) -> dict:
        if state.owned[0].target.fund.chain != chain:
            raise ValueError("a HyperCore deployment trades through one Safe on one chain")
        return {"custody": state.custody}

    @staticmethod
    def merge(state, changed, chain) -> dict:
        if state.owned[0].target.fund.chain != chain or changed.custody.safe != state.custody.safe:
            raise ValueError("a HyperCore custody merge keeps its one Safe and chain")
        return {"custody": changed.custody}

    @staticmethod
    def audit(backend, state) -> None:
        """Installed agent authority: every agent HyperCore lists under the deployment's
        names is a key the signer holds for it (D9)."""
        (owned,) = state.owned
        CustodyRequests.audit(backend.custody_backend(owned.target.account), state.custody)
