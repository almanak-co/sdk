"""The phase 3 trading path: plan, admit, sign, record, send, recover and reconcile.

Each step commits to the durable document (`trading_store.py`) before the external
action it precedes (blueprint 30, section 5.2):

1. the attempt is retained with its reserved nonce, cloid and `expiresAfter`;
2. the gateway admits it (section 5.3) and the signer signs it with the Safe's current
   agent key; the returned signature must recover to that agent over the digest the
   gateway computes;
3. the exact signed body is recorded, never sent;
4. the attempt is marked maybe sent, and only then are those bytes posted;
5. the answer resolves it, or the outcome stays unknown and recovery resolves it from
   the attempt's own evidence. Nothing re-posts a fresh request for an admitted attempt.

Restart releases attempts that were never sent, resolves maybe-sent ones from their
evidence, reconciles the book from its cursor and re-reads every live owned order (D6):
a resting order is owned state, so restart continues past it. Teardown cancels the
strategy's own live orders by their retained oids, clears its scheduled cancel and
closes its positions with reduce-only `Ioc` orders, stopping at any attempt it cannot
resolve. It reports any open order, and any exposure, that is not the strategy's own;
it never closes or cancels one, and it does not revoke the agent (D2).
"""

from decimal import Decimal
from functools import partial
from typing import Protocol

from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid.admission import Authority, admit
from almanak.connectors.hyperliquid.agents import require_signing
from almanak.connectors.hyperliquid.effects import (
    Exposure,
    Pages,
    proven_exposure,
    reconcile,
    require_unchanged,
)
from almanak.connectors.hyperliquid.trading import Attempt, RestingOrder
from almanak.connectors.hyperliquid.trading_store import (
    AccountingFailure,
    HyperCoreState,
    TradingStore,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_exchange import Accepted, Refused, UnknownOutcome
from almanak.gateway.hypercore_info import (
    ActiveAssetData,
    ClearinghouseState,
    ExtraAgents,
    InfoRequest,
    InfoResult,
    L2Book,
    OpenOrders,
    OrderStatus,
    UserAbstraction,
    UserFillsByTime,
    UserFunding,
    UserNonFundingLedgerUpdates,
    UserRole,
    typed,
)
from almanak.gateway.hypercore_shapes import KnownOrder
from almanak.gateway.hypercore_signing import (
    ActionRequest,
    Cancel,
    OrderAction,
    SignedAction,
    Tpsl,
    UpdateLeverage,
)
from almanak.gateway.signers import SignerRefused
from almanak.models import Model

# The observations admission rests on, beyond the agent and the account mode, by purpose.
READS = {
    "leverage": ("account", "orders"),
    "entry": ("active",),
    "exit": ("account",),
    "protect": ("account", "active"),
    "margin": ("account",),
}


class Venue(Protocol):
    """The gateway's HyperCore face: `/info` reads, `/exchange` posts and its clock."""

    def fetch(self, request: InfoRequest) -> InfoResult: ...

    def post(self, body: bytes) -> Accepted | Refused: ...

    def now(self) -> int: ...


class ActionSigner(Protocol):
    def sign_hyperliquid_action(
        self, account: str, agent: str, request: ActionRequest
    ) -> SignedAction: ...


class OutsideOrder(Model):
    coin: str
    side: str
    size: Decimal
    limit_price: Decimal
    oid: int
    cloid: str | None


class TeardownVerdict(Model):
    complete: bool
    # The Safe's venue positions, and the part the strategy's own fills do not hold,
    # which teardown never closes.
    positions: dict[str, Decimal]
    outside_positions: dict[str, Decimal]
    # Open orders that are not the strategy's own attempts; never cancelled by teardown.
    outside: tuple[OutsideOrder, ...]
    # The strategy's own orders still open.
    own: tuple[int, ...]
    unresolved: tuple[str, ...]
    # The strategy's scheduled cancel still ahead, which teardown clears.
    schedule: int | None = None
    # A reconciliation failure an exit went ahead despite, still unresolved.
    accounting: AccountingFailure | None = None
    evidence: tuple[InfoResult, ...]


class Trader:
    def __init__(self, store: TradingStore, venue: Venue, signer: ActionSigner):
        self.store = store
        self.venue = venue
        self.signer = signer

    def observe(self, query) -> InfoResult:
        return self.venue.fetch(InfoRequest(info=query))

    def state(self) -> HyperCoreState:
        return self.store.load()

    def agent(self, state: HyperCoreState) -> str:
        return require_signing(state.custody.agents, self.venue.now()).address

    # Planning: each plan is retained before anything is signed.

    def retain(self, planner) -> Attempt:
        attempt: Attempt | None = None

        def change(state: HyperCoreState) -> HyperCoreState:
            nonlocal attempt
            trading, attempt = planner(state)
            return state.model_copy(update={"trading": trading})

        self.store.update(change)
        assert attempt is not None
        return attempt

    def leverage(self, coin: str) -> Attempt:
        now = self.venue.now()
        return self.act(
            lambda s: rules.plan_leverage(s.trading, coin, self.agent(s), now),
        )

    def enter(self, coin: str, is_buy: bool, size: Decimal) -> Attempt:
        book = self.observe(L2Book(coin=coin))
        now = self.venue.now()
        return self.act(
            lambda s: rules.plan_entry(s.trading, coin, self.agent(s), is_buy, size, book, now)
        )

    def exit(self, coin: str) -> Attempt | None:
        """A reduce-only exit of the strategy's own position in `coin`, which is bounded by
        the Safe's venue position; None when the strategy holds none there. Admission
        establishes the ownership again at its own account snapshot."""
        size = self.owned_exposure().owned.get(coin, Decimal(0))
        if size == 0:
            return None
        book = self.observe(L2Book(coin=coin))
        now = self.venue.now()
        return self.act(lambda s: rules.plan_exit(s.trading, coin, self.agent(s), size, book, now))

    def owned_exposure(self) -> Exposure:
        """What the strategy owns, to size an exit or protective order. Full reconciliation
        proves it; when
        cash accounting fails, the fills since the cursor and the venue position alone do,
        and the accounting failure is recorded until a reconciliation succeeds. Exposure
        the fills cannot prove halts."""
        try:
            book = self.reconcile().book
        except DataError as failure:
            held = self.proven_exposure()
            failed = AccountingFailure(kind=failure.kind, message=str(failure), at=self.venue.now())
            self.store.update(lambda s: s.model_copy(update={"accounting": failed}))
            return held
        return Exposure(positions=book.positions, owned=book.owned)

    def proven_exposure(self) -> Exposure:
        state = self.state()
        account = self.observe(ClearinghouseState(user=state.trading.safe))
        fills = self.fills_through(state, account)
        if fills is None:
            # Nothing new since the cursor: the durable book itself is the proof.
            require_unchanged(state.book, account)
            return Exposure(positions=state.book.positions, owned=state.book.owned)
        return proven_exposure(state.book, account, fills, state.trading.identities())

    def fills_through(self, state: HyperCoreState, account: InfoResult) -> InfoResult | None:
        """The Safe's fills from the book's cursor to an account snapshot's time, read
        after it; None when the snapshot is no later than the cursor."""
        end = typed(account).time
        if end < state.book.cursor:
            return None
        query = UserFillsByTime(user=state.trading.safe, start_time=state.book.cursor, end_time=end)
        return self.observe(query)

    def rest(self, coin: str, order: RestingOrder) -> Attempt:
        """A `Gtc` or `Alo` limit order, which becomes an owned order while it rests."""
        book = self.observe(L2Book(coin=coin))
        now = self.venue.now()
        return self.act(
            lambda s: rules.plan_resting(s.trading, coin, self.agent(s), order, book, now)
        )

    def protect(self, coin: str, tpsl: Tpsl, trigger: Decimal, size=None) -> Attempt | None:
        """A reduce-only limit TP or SL over `size` of the strategy's own position in `coin`,
        the whole of it by default; None when the strategy holds none there. Admission
        bounds it, with every live reduce-only order, by the owned position."""
        owned = self.owned_exposure().owned.get(coin, Decimal(0))
        if owned == 0:
            return None
        amount = owned if size is None else (size if owned > 0 else -size)
        now = self.venue.now()
        return self.act(
            lambda s: rules.plan_trigger(s.trading, coin, self.agent(s), amount, tpsl, trigger, now)
        )

    def cancel(self, oid: int) -> Attempt:
        """Cancel a live owned order by its retained oid."""
        now = self.venue.now()
        return self.act(lambda s: rules.plan_cancel(s.trading, oid, self.agent(s), now))

    def cancel_by_cloid(self, cloid: str) -> Attempt:
        """Cancel the owned order a cloid names, by its retained oid: HyperCore resolves a
        signed cloid only when it executes, to the newest order carrying it (T5)."""
        return self.cancel(self.state().trading.owned_by_cloid(cloid).oid)

    def schedule_cancel(self, time: int | None) -> Attempt:
        """Set the dead man's switch to `time`, or clear it with None."""
        now = self.venue.now()
        return self.act(lambda s: rules.plan_schedule(s.trading, self.agent(s), time, now))

    def margin(self, coin: str, amount: Decimal) -> Attempt:
        """Move `amount` USDC into (positive) or out of (negative) the isolated position."""
        now = self.venue.now()
        return self.act(lambda s: rules.plan_margin(s.trading, coin, self.agent(s), amount, now))

    def retire(self, attempt_id: str) -> Attempt:
        now = self.venue.now()
        return self.act(lambda s: rules.plan_retire(s.trading, attempt_id, now))

    def act(self, planner) -> Attempt:
        attempt = self.retain(planner)
        return self.submit(attempt.id)

    # Admission, signing and the send.

    def authority(self, state: HyperCoreState, attempt: Attempt) -> Authority:
        safe = state.trading.safe
        reads = set(READS.get(attempt.purpose, ()))
        queries = {
            "account": lambda: ClearinghouseState(user=safe),
            "active": lambda: ActiveAssetData(user=safe, coin=attempt.coin or ""),
            "orders": lambda: OpenOrders(user=safe),
        }
        observed: dict[str, InfoResult | None] = {
            name: self.observe(query()) for name, query in queries.items() if name in reads
        }
        account = observed.get("account")
        if attempt.purpose in ("exit", "protect") and account is not None:
            # Ownership is established at the account snapshot that bounds the order.
            observed["fills"] = self.fills_through(state, account)
        return Authority(
            role=self.observe(UserRole(user=attempt.agent)),
            agents=self.observe(ExtraAgents(user=safe)),
            abstraction=self.observe(UserAbstraction(user=safe)),
            **observed,
        )

    def submit(self, attempt_id: str) -> Attempt:
        self.prepare(attempt_id)
        return self.send(attempt_id)

    def prepare(self, attempt_id: str) -> SignedAction:
        """Admit, sign and record the signed body; nothing is sent. Admission derives an
        exit's ownership itself, from the durable book and the Safe's fills up to the
        account snapshot it bounds the exit with.

        A refusal (admission's, the signer's own `SignerRefused`, or a signature that does
        not verify) releases the attempt and counts against the re-plan budget; an
        unavailable read or signer releases it uncounted."""
        state = self.state()
        attempt = state.trading.attempt(attempt_id)
        try:
            authority = self.authority(state, attempt)
        except Exception:
            self.trading(lambda t: rules.unavailable(t, attempt_id))
            raise
        try:
            admit(
                state.trading,
                state.custody.agents,
                attempt,
                authority,
                self.venue.now(),
                state.book,
            )
            signature = self.signer.sign_hyperliquid_action(
                state.trading.safe, attempt.agent, attempt.request
            )
        except (DataError, ValueError, PermissionError, SignerRefused):
            self.trading(lambda t: rules.refused(t, attempt_id))
            raise
        except Exception:
            self.trading(lambda t: rules.unavailable(t, attempt_id))
            raise
        self.trading(lambda t: rules.signed(t, attempt_id, signature))
        return signature

    def send(self, attempt_id: str) -> Attempt:
        """Mark the recorded attempt maybe sent, then post its recorded body once. Only an
        attempt signed and never sent is sent; a lost answer leaves it maybe sent."""
        attempt = self.trading(lambda t: rules.sending(t, attempt_id)).trading.attempt(attempt_id)
        if attempt.signed is None:
            raise ValueError("a sent attempt carries its recorded body")
        try:
            answer = self.venue.post(attempt.signed.body())
        except UnknownOutcome:
            return attempt
        state = self.trading(lambda t: rules.answered(t, attempt_id, answer))
        return state.trading.attempt(attempt_id)

    def resend(self, attempt_id: str) -> Attempt:
        """Post a maybe-sent attempt's recorded bytes again; never a fresh request.

        Only an acceptance resolves it. A refusal proves nothing: HyperCore's nonce set
        refuses exactly the bytes that already applied, so evidence decides."""
        attempt = self.state().trading.attempt(attempt_id)
        if attempt.status != "sent" or attempt.signed is None:
            raise ValueError("only a maybe-sent attempt's recorded bytes are resent")
        try:
            answer = self.venue.post(attempt.signed.body())
        except UnknownOutcome:
            return attempt
        if isinstance(answer, Refused):
            return attempt
        state = self.trading(lambda t: rules.answered(t, attempt_id, answer))
        return state.trading.attempt(attempt_id)

    def trading(self, change) -> HyperCoreState:
        return self.store.update(lambda s: s.model_copy(update={"trading": change(s.trading)}))

    # Recovery.

    def recover(self) -> tuple[Attempt, ...]:
        """Release never-sent attempts and resolve maybe-sent ones from their evidence;
        returns those still unresolved."""
        self.trading(rules.released_unsent)
        for attempt in self.state().trading.attempts:
            if attempt.status == "sent":
                self.resolve(attempt)
        return tuple(a for a in self.state().trading.attempts if not a.resolved)

    def resolve(self, attempt: Attempt) -> None:
        safe = self.state().trading.safe
        now = self.venue.now()
        action = attempt.request.action
        if attempt.purpose == "retire":
            self.trading(lambda t: rules.recovered_retirement(t, attempt))
        elif isinstance(action, OrderAction):
            self.resolve_order(attempt, safe, now)
        elif isinstance(action, UpdateLeverage):
            active = self.observe(ActiveAssetData(user=safe, coin=attempt.coin or ""))
            self.trading(lambda t: rules.recovered_leverage(t, attempt, active, now))
        elif isinstance(action, Cancel) and attempt.cancels is not None:
            status = self.observe(OrderStatus(user=safe, oid=attempt.cancels))
            self.trading(lambda t: rules.recovered_cancel(t, attempt, status, now))
        elif now <= attempt.request.expires_after:
            # `scheduleCancel` and `updateIsolatedMargin` have no read (T9, T10): only an
            # answer, to a resend of the recorded bytes, shows their outcome.
            self.resend(attempt.id)

    def resolve_order(self, attempt: Attempt, safe: str, now: int) -> None:
        """By `orderStatus` of its cloid; its fills are read only when that cannot tell,
        through that read and the venue's latest time for an order that may rest."""
        status = self.observe(OrderStatus(user=safe, oid=attempt.cloid or ""))
        fills = None
        if not isinstance(typed(status), KnownOrder):
            # An order that may rest is read to the venue's own latest time as well.
            through = status.received_at
            if attempt.rests:
                venue = typed(self.observe(ClearinghouseState(user=safe))).time
                through = max(through, venue)
            start, end = rules.fill_range(attempt, through)
            fills = self.observe(UserFillsByTime(user=safe, start_time=start, end_time=end))
        self.trading(lambda t: rules.recovered_order(t, attempt, status, fills, now))

    def require_resolved(self) -> None:
        """Halt with the evidence of any attempt recovery could not resolve."""
        unresolved = self.recover()
        if unresolved:
            raise DataError(
                "unknown_outcome",
                "HyperCore attempts have no evidence of their outcome yet",
                details={"attempts": [a.model_dump(mode="json") for a in unresolved]},
            )

    # External effects.

    def reconcile(self) -> HyperCoreState:
        """Apply fills, funding and ledger entries since the book's cursor, once each."""
        state = self.state()
        safe, cursor = state.trading.safe, state.book.cursor
        account = self.observe(ClearinghouseState(user=safe))
        end = typed(account).time
        if end < cursor:
            # Nothing happened since the last range; the account must still match.
            require_unchanged(state.book, account)
            return state
        pages = Pages(
            account=account,
            fills=self.observe(UserFillsByTime(user=safe, start_time=cursor, end_time=end)),
            funding=self.observe(UserFunding(user=safe, start_time=cursor, end_time=end)),
            ledger=self.observe(
                UserNonFundingLedgerUpdates(user=safe, start_time=cursor, end_time=end)
            ),
        )
        return self.store.update(
            lambda s: s.model_copy(
                update={
                    "book": reconcile(s.book, pages, s.trading.identities()),
                    "accounting": None,
                }
            )
        )

    def refresh(self) -> HyperCoreState:
        """Read each live owned order's state from `orderStatus` of its own oid (D6)."""
        safe = self.state().trading.safe
        for order in self.state().trading.live():
            status = self.observe(OrderStatus(user=safe, oid=order.oid))
            self.trading(partial(rules.refreshed, status=status))
        return self.state()

    def restart(self) -> HyperCoreState:
        """Resolve every attempt, reconcile the book and re-read the owned orders; a
        resting order is owned state, so restart continues past it."""
        self.require_resolved()
        self.reconcile()
        return self.refresh()

    # Teardown.

    def cancel_orders(self) -> tuple[Attempt, ...]:
        """Cancel each live owned order by its retained oid, stopping at a cancel whose
        outcome is not yet known."""
        cancelled: list[Attempt] = []
        for order in self.refresh().trading.live():
            cancelled.append(self.cancel(order.oid))
            if not cancelled[-1].resolved:
                break
        return tuple(cancelled)

    def disarm(self) -> Attempt | None:
        """Clear the strategy's scheduled cancel if one is still ahead."""
        if not self.state().trading.armed(self.venue.now()):
            return None
        return self.schedule_cancel(None)

    def close_positions(self) -> tuple[Attempt, ...]:
        """Reduce-only `Ioc` exits of every configured market the Safe holds."""
        closed = []
        for target in self.state().trading.targets:
            attempt = self.exit(target.coin)
            if attempt is not None:
                closed.append(attempt)
        return tuple(closed)

    def teardown(self) -> "TeardownVerdict":
        """Cancel the strategy's own orders, clear its scheduled cancel and close its
        positions; the verdict says what remains. Each step waits for every attempt to be
        resolved, and the scheduled cancel stays armed, with positions open, until a fresh
        read shows no owned order still live: a refused cancel leaves its order able to
        reopen exposure. Outside orders and exposure are never touched."""
        steps = ((self.cancel_orders, False), (self.disarm, True), (self.close_positions, True))
        for each, after_cancels in steps:
            if self.recover() or (after_cancels and self.refresh().trading.live()):
                break
            each()
        return self.verdict()

    def verdict(self) -> "TeardownVerdict":
        state = self.refresh()
        safe = state.trading.safe
        account = self.observe(ClearinghouseState(user=safe))
        orders = self.observe(OpenOrders(user=safe))
        return teardown_verdict(state, account, orders, self.venue.now())


def teardown_verdict(state: HyperCoreState, account: InfoResult, orders: InfoResult, now=0):
    """What teardown leaves; a scheduled cancel counts while it lies after `now`."""
    attempts = state.trading.attempts
    cloids = {a.cloid for a in attempts if a.cloid is not None}
    oids = {oid for a in attempts for oid in a.oids}
    own, outside = [], []
    for order in typed(orders):
        if order.oid in oids or (order.cloid is not None and order.cloid in cloids):
            own.append(order.oid)
        else:
            outside.append(
                OutsideOrder(
                    coin=order.coin,
                    side=order.side,
                    size=Decimal(order.sz),
                    limit_price=Decimal(order.limitPx),
                    oid=order.oid,
                    cloid=order.cloid,
                )
            )
    positions = {p.position.coin: Decimal(p.position.szi) for p in typed(account).assetPositions}
    unresolved = tuple(a.id for a in attempts if not a.resolved)
    schedule = state.trading.schedule if state.trading.armed(now) else None
    return TeardownVerdict(
        complete=not (positions or own or outside or unresolved or schedule or state.accounting),
        positions=positions,
        outside_positions=state.book.outside(),
        outside=tuple(outside),
        own=tuple(own),
        unresolved=unresolved,
        schedule=schedule,
        accounting=state.accounting,
        evidence=(account, orders),
    )


def require_complete(verdict: TeardownVerdict) -> None:
    """Halt an incomplete teardown with its evidence for the Safe owners to resolve."""
    if not verdict.complete:
        raise DataError(
            "teardown_incomplete",
            "HyperCore teardown left positions, open orders, unresolved attempts or a "
            "scheduled cancel",
            details=verdict.model_dump(mode="json"),
        )
