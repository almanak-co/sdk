"""The Safe's inventory and HyperCore documents commit together through runtime ownership.

`custody`, `trading` and `book` are ownership fields: the gateway lets them change only
through verified evidence (`gateway/admission.py`), as it does the owned inventory. Each
outcome carries its evidence and `documents` applies it to the checkpoint's own
documents, so the coordinator and the gateway derive the same change:

- a custody transaction's receipt retains the request it queued, or submits the agent
  approval its plan named (`custody_changed`);
- a custody request's outcome carries only the request or agent approval its own effect
  resolved, which takes its place in the custody document (`custody.with_resolution`);
- a venue action's planned attempt is retained in the trading document exactly as
  `trading.plan` retains it, and the attempt as its answer or evidence resolved it takes
  its place (`venue_settled`); only never-sent attempts are dropped without an outcome;
- an effects page's fills, funding and ledger entries are applied to the book once each.

What the strategy owns of the HyperCore account (its position, holding and USDC, the
account's activation and the leverage it set) is projected from these documents
(`positions.projected`), so it changes only when a verified outcome changes them.
"""

from uuid import uuid4

from almanak.connectors.hyperliquid import agents as agent_rules
from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid.custody import (
    Custody,
    CustodyRequests,
    record_action,
    record_deposit,
    with_resolution,
)
from almanak.connectors.hyperliquid.effects import Book, reconcile
from almanak.connectors.hyperliquid.operations import AgentApproval, Approve, Deposit, core_call
from almanak.connectors.hyperliquid.positions import initial_owned, projected
from almanak.connectors.hyperliquid.state import (
    Checkpoint,
    CustodyOutcome,
    EffectsOutcome,
    TransactionOutcome,
    VenueOutcome,
    VenuePending,
    Work,
    perp_target,
)
from almanak.connectors.hyperliquid.trading import Trading, TradingPolicy
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_exchange import Accepted
from almanak.triggers import DecisionSchedule
from almanak.work import initial_work

DOCUMENTS = ("custody", "trading", "book")
EXPOSURE = ("deposit", "set_leverage", "increase_position")


def ownership(state, outcome=None):
    fields = {"owned": state.owned, **{name: getattr(state, name) for name in DOCUMENTS}}
    if outcome is None:
        return fields
    changed = {**fields, **documents(state, outcome)}
    return {**changed, "owned": owned_after(state.owned, outcome, changed)}


def documents(state, outcome) -> dict:
    """The HyperCore documents an outcome's evidence changes."""
    if isinstance(outcome, TransactionOutcome):
        return {"custody": transaction_custody(state.custody, outcome)}
    if isinstance(outcome, CustodyOutcome):
        return {"custody": with_resolution(state.custody, outcome.request, outcome.approval)}
    if isinstance(outcome, VenueOutcome):
        return {"trading": venue_settled(state.trading, outcome)}
    if isinstance(outcome, EffectsOutcome):
        return {"book": reconcile(state.book, outcome.pages, identities(state))}
    return {}


def owned_after(owned, outcome, changed: dict):
    """Custody moves the Safe's HyperEVM inventory; every HyperCore figure is projected
    again from the documents the outcome left (`positions.projected`): a resolved deposit
    activates the account, an applied `updateLeverage` sets the leverage, and effects
    move the holding and the USDC. A venue order moves nothing until its fills are
    reconciled."""
    return tuple(
        o.model_copy(update=projected_from(changed, o.target.market.coin))
        for o in commit_owned(owned, outcome)
    )


def projected_from(documents: dict, coin: str) -> dict:
    return projected(documents["custody"], documents["trading"], documents["book"], coin)


def commit_owned(owned, outcome):
    if isinstance(outcome, (TransactionOutcome, CustodyOutcome)):
        inventory = outcome.after.owned.inventory
        return tuple(
            o.model_copy(update={"inventory": inventory}) if o.target == outcome.after.target else o
            for o in owned
        )
    return owned


def transaction_custody(custody: Custody, outcome: TransactionOutcome) -> Custody:
    if not outcome.success:
        return custody
    return custody_changed(custody, outcome.operation, outcome.receipt, outcome.after.block)


def custody_changed(custody: Custody, operation, receipt: dict, block) -> Custody:
    """The custody document after a mined custody call: the request its receipt shows it
    queued, or, for an agent approval, the approval its plan named, retained and submitted.
    A receipt without its call's own evidence is refused."""
    if isinstance(operation, Approve):
        return custody
    if isinstance(operation, Deposit):
        return record_deposit(custody, receipt, block.timestamp, operation.amount.amount)
    if isinstance(operation, AgentApproval):
        agents = prepared(custody.agents, operation.agent, operation.label)
        custody = custody.model_copy(update={"agents": agents})
    return record_action(custody, core_call(operation), receipt, block.timestamp)


def prepared(agents, address: str, label: str):
    """The approval a plan named, retained as `agents.prepare` retained it at the plan's own
    time, which the label's expiry fixes."""
    now = label_expiry(label) - agents.policy.lifetime
    retained, expected = agent_rules.prepare(agents, address, now)
    if expected != label:
        raise ValueError("the agent approval's label is not the one its plan derived")
    return retained


def label_expiry(label: str) -> int:
    """The 13-digit millisecond expiry an approval label ends with."""
    return int(label.rpartition(agent_rules.SUFFIX)[2])


def venue_settled(trading: Trading, outcome: VenueOutcome) -> Trading:
    """The trading document with the outcome's attempt retained and resolved."""
    trading = inserted(trading, outcome.operation.attempt)
    attempt = outcome.attempt
    trading = rules.replaced(trading, attempt)
    if attempt.status == "applied" and isinstance(attempt.answer, Accepted):
        # What an applied answer leaves behind beyond the attempt: an owned resting order,
        # a cancelled one, a scheduled cancel. Phase 3's actions leave nothing.
        trading = rules.took_effect(trading, attempt, attempt.answer)
    return trading


def inserted(trading: Trading, attempt) -> Trading:
    """The trading document with a planned attempt retained as `trading.plan` retains it:
    no other attempt unresolved, within the re-plan budget, the next identity and cloid,
    its nonce strictly above its agent's last reserved one and reserved, and the attempt
    appended unsigned."""
    if trading.pending is not None:
        raise ValueError("an attempt is unresolved; it is reconciled before another")
    if attempt.purpose in rules.BUDGETED and trading.released >= trading.policy.replans:
        raise DataError("authority", "HyperCore attempts exhausted their re-plan budget")
    request = attempt.request
    planned = (
        attempt.id == rules.next_id(trading)
        and attempt.status == "reserved"
        and attempt.signed is None
        and attempt.answer is None
        and not (attempt.oids or attempt.evidence or attempt.unavailable)
        and request.nonce > trading.nonces.get(attempt.agent, 0)
        and request.expires_after <= request.nonce + trading.policy.expiry
        and attempt.cloid in (None, rules.cloid_for(trading.deployment, attempt.id))
    )
    if not planned:
        raise ValueError("the attempt is not the trading rules' next plan")
    nonces = {**trading.nonces, attempt.agent: request.nonce}
    return trading.model_copy(update={"nonces": nonces, "attempts": (*trading.attempts, attempt)})


def identities(state) -> tuple[frozenset[str], frozenset[int]]:
    """The cloids and oids of the strategy's own orders: every retained attempt's and a
    pending one's, which may already have filled before its outcome is committed."""
    cloids, oids = state.trading.identities()
    pending = state.pending
    if isinstance(pending, VenuePending):
        attempt = pending.resolved or pending.operation.attempt
        if attempt.cloid is not None:
            cloids = cloids | {attempt.cloid}
        oids = oids | frozenset(attempt.oids)
    return cloids, oids


def validate_owned(state, observations):
    if tuple(o.target for o in observations) != tuple(o.target for o in state.owned):
        raise ValueError("Hyperliquid observations must cover every retained target exactly once")
    for owned, observed in zip(state.owned, observations, strict=True):
        if observed.owned != owned:
            raise ValueError("Hyperliquid observation differs from retained ownership")
        if any(
            wallet.amount < balance.amount
            for wallet, balance in zip(observed.wallets, owned.inventory, strict=True)
        ):
            raise ValueError("Hyperliquid Safe wallet no longer covers retained owned inventory")


def validate_initial(state, observations):
    """The documents are fresh and the Safe grants CoreDepositWallet nothing; the gateway
    proves at the venue that the Safe's HyperCore account holds nothing either."""
    validate_owned(state, observations)
    if state.custody.requests or state.custody.agents.approvals or state.trading.attempts:
        raise ValueError("Hyperliquid initialization requires fresh HyperCore documents")
    if any(o.needs_cleanup for o in observations):
        raise ValueError(
            "Hyperliquid initialization cannot adopt an existing CoreDepositWallet allowance"
        )


def validate_closure(state, observations):
    """Teardown's order (section 7) is complete by the documents: nothing unresolved, no
    owned order or position, no HyperCore USDC and no allowance. Exposure the book shows
    outside the strategy's own keeps it incomplete with that evidence; the gateway proves
    the rest at the venue, open orders of any origin included."""
    validate_owned(state, observations)
    if CustodyRequests.unresolved(state.custody):
        raise ValueError("Hyperliquid closure cannot discard unresolved custody requests")
    if state.trading.pending is not None or state.trading.live():
        raise ValueError("Hyperliquid closure cannot discard an unresolved attempt or open order")
    if any(o.position or o.holds_usdc for o in state.owned):
        raise ValueError("Hyperliquid closure requires no owned position and no HyperCore USDC")
    if state.book.positions:
        raise DataError(
            "teardown_incomplete",
            "HyperCore positions outside the strategy's own remain; teardown never closes them",
            details={"positions": {c: str(s) for c, s in state.book.positions.items()}},
        )
    if any(o.needs_cleanup for o in observations):
        raise ValueError("Hyperliquid closure requires no CoreDepositWallet allowance")


def initial_documents(config, deployment_id):
    (allocation,) = config.allocations
    safe = config.target(allocation.name).fund.address
    # Forks run against the synthesized model, whose register states A4: an Ioc order the
    # venue does not know past its expiry, with no fill, never ran. On mainnet T4 is open,
    # so production (D7) keeps such an order unresolved.
    policy = TradingPolicy(release_unknown_after_expiry=config.production is None)
    return {
        "custody": Custody(safe=safe),
        "trading": Trading(
            deployment=deployment_id,
            safe=safe,
            targets=(perp_target(allocation),),
            policy=policy,
        ),
        # The Safe is dedicated to the strategy, so the book reconciles from its first
        # effect; the gateway refuses to initialize over an account holding anything.
        "book": Book(safe=safe, cursor=0),
    }


class Lifecycle:
    ownership = staticmethod(ownership)
    commit_owned = staticmethod(commit_owned)
    validate_owned = staticmethod(validate_owned)
    validate_initial = staticmethod(validate_initial)
    validate_closure = staticmethod(validate_closure)

    @staticmethod
    def increases_exposure(intent):
        """A deposit, the leverage that admits an entry, and the entry itself."""
        return intent.kind in EXPOSURE

    @staticmethod
    def initial(config, strategy, capabilities, permissions):
        deployment_id = uuid4().hex
        return Checkpoint(
            schedule=DecisionSchedule(),
            work=initial_work(config, Work),
            deployment_id=deployment_id,
            config=config,
            strategy=strategy,
            capabilities=capabilities,
            permissions=permissions,
            mode="running",
            initialized=False,
            owned=tuple(initial_owned(config, a.name) for a in config.allocations),
            **initial_documents(config, deployment_id),
        )
