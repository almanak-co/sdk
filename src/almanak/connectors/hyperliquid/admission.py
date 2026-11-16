"""What the gateway enforces before an agent signs (blueprint 30, section 5.3).

The agent can trade every market of the Safe's HyperCore account at any size, price and
leverage; nothing on HyperCore restricts it (D2). The gateway is the only enforcement,
and it signs an attempt only when:

- its action is one the gateway signs for its purpose, on a configured market with the
  pinned asset index;
- an exit, resting or not, or a protective trigger order is reduce-only, opposite to the
  strategy's owned position and no larger than it or than the Safe's venue position,
  both taken at one `clearinghouseState` snapshot: ownership is replayed from the
  durable book through the Safe's fills up to that snapshot's time, so outside trades
  since the book, or since the order was planned, can never pass for the strategy's
  exposure. With every live reduce-only order the strategy owns in the market it fits
  inside the owned position, so they cannot close outside exposure together;
- a reduce-only order that may rest is admitted only while the Safe holds no outside
  exposure in the market, since outside activity while it rests could otherwise leave it
  closing someone else's position;
- an entry, resting or not, finds the market at the configured leverage and margin mode
  (`activeAssetData`), so a lost setup or an outside change cannot open risk at another
  leverage;
- a limit order's price, of any time in force, crosses the retained book by no more than
  the author's slippage; the book is within its age limit by the venue's book time and
  by when it was received, and its book time is not ahead of the deployment clock by
  more than the declared skew;
- a trigger order's trigger lies on the side of a fresh mark that HyperCore requires for
  its kind (below it for a stop that sells), since HyperCore triggers on the mark, and
  its limit lies within the author's slippage of the trigger, on the side that fills
  once it triggers;
- leverage is the author's configured leverage and margin mode, set while the Safe holds
  no position and no open order in that market;
- a cancel names a live order the strategy owns by its retained oid; `cancelByCloid` is
  never signed, since HyperCore resolves the cloid when it executes the cancel (T5);
- a scheduled cancel lies at least 5 seconds past the attempt's expiry, so HyperCore
  accepts it whenever the attempt applies, and within the connector's horizon;
- a margin change moves an isolated position's margin on a market configured isolated,
  and only under a policy that states assumption A10 (open fact T10);
- `userRole` has reported the signing agent as this Safe's agent since its approval,
  `extraAgents` shows that approval with its expiry, and the expiry is not past;
- the Safe's `userAbstraction` reads `disabled`;
- its nonce is inside HyperCore's window around the latest observation.
"""

from decimal import Decimal

from almanak.connectors.hyperliquid.agents import Agents, require_signing
from almanak.connectors.hyperliquid.effects import Book, proven_exposure, require_unchanged
from almanak.connectors.hyperliquid.trading import (
    BASIS_POINTS,
    SCHEDULE_LEAD_MS,
    Attempt,
    PerpTarget,
    Trading,
    best,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import InfoResult, typed
from almanak.gateway.hypercore_shapes import AgentRole
from almanak.gateway.hypercore_signing import (
    Cancel,
    Noop,
    OrderAction,
    ScheduleCancel,
    TriggerOrder,
    UpdateIsolatedMargin,
    UpdateLeverage,
    require_nonce_window,
)
from almanak.models import Model


class Authority(Model):
    """The observations admission rests on, retained with the decision."""

    role: InfoResult
    agents: InfoResult
    abstraction: InfoResult
    # The Safe's perp state, for an exit, a protective order, a leverage change or a
    # margin change.
    account: InfoResult | None = None
    # For an exit or a protective order: the Safe's fills from the book's cursor to
    # `account`'s time, read after it, which establish what the strategy owns at that same
    # snapshot. None when the snapshot is no later than the cursor.
    fills: InfoResult | None = None
    # The Safe's leverage, mode and the market's mark, for an entry or a trigger order.
    active: InfoResult | None = None
    # The Safe's open orders, for a leverage change.
    orders: InfoResult | None = None


def refuse(message: str, **details) -> DataError:
    return DataError("authority", message, details=details or None)


def asked(result: InfoResult, kind: str, user: str) -> None:
    query = result.request.info
    if query.type != kind or getattr(query, "user", None) != user:
        raise refuse(f"admission needs {kind} of {user}")


def admit(
    trading: Trading,
    agents: Agents,
    attempt: Attempt,
    authority: Authority,
    now: int,
    reconciled: Book,
):
    """Refuse the attempt unless every rule of section 5.3 holds. `reconciled` is the
    durable book, from which an exit's ownership is derived here, never taken from the
    caller."""
    if attempt.status != "reserved":
        raise refuse("only a retained, unsigned attempt is admitted")
    require_account(trading, agents, attempt, authority, now)
    latest = max(r.received_at for r in (authority.role, authority.agents, authority.abstraction))
    try:
        require_nonce_window(attempt.request.nonce, latest)
    except ValueError as error:
        raise refuse(str(error)) from None
    require_action(trading, attempt, authority, now, reconciled)


def require_account(trading, agents: Agents, attempt: Attempt, authority: Authority, now: int):
    """What every action rests on: the Safe's current agent signs it, as HyperCore reports
    it, and the account is in standard mode. A refusal here is no market condition: the
    agent or the account changed, which halts rather than re-plans."""
    require_agent(trading, agents, attempt, authority, now)
    asked(authority.abstraction, "userAbstraction", trading.safe)
    if typed(authority.abstraction) != "disabled":
        raise refuse("the Safe's HyperCore account is not in standard mode")


def require_action(trading, attempt: Attempt, authority: Authority, now: int, reconciled):
    action = attempt.request.action
    if isinstance(action, Noop):
        require_retirement(trading, attempt)
    elif isinstance(action, UpdateLeverage):
        require_leverage(trading, attempt, action, authority)
    elif isinstance(action, OrderAction):
        require_order(trading, attempt, action, authority, now, reconciled)
    elif isinstance(action, Cancel):
        require_cancel(trading, attempt, action)
    elif isinstance(action, ScheduleCancel):
        require_schedule(trading, attempt, action)
    else:
        require_margin(trading, attempt, action, authority.account)


def require_agent(trading, agents: Agents, attempt: Attempt, authority: Authority, now: int):
    approval = require_signing(agents, now)
    if attempt.agent != approval.address or approval.submission is None:
        raise refuse("the attempt is not signed by the Safe's current agent")
    asked(authority.role, "userRole", approval.address)
    role = typed(authority.role)
    if not isinstance(role, AgentRole) or role.data.user != trading.safe:
        raise refuse("userRole does not report the agent as this Safe's agent")
    if authority.role.received_at < approval.submission.time:
        raise refuse("userRole was read before the agent's approval")
    asked(authority.agents, "extraAgents", trading.safe)
    listed = {(a.name, a.address, a.validUntil) for a in typed(authority.agents)}
    if (approval.name, approval.address, approval.valid_until) not in listed:
        raise refuse("extraAgents does not show the agent's approval with its expiry")
    if approval.valid_until <= max(now, authority.agents.received_at):
        raise refuse("the agent's approval has expired")


def require_retirement(trading: Trading, attempt: Attempt) -> None:
    retired = trading.attempt(attempt.retires or "")
    if (attempt.purpose, retired.agent, retired.request.nonce) != (
        "retire",
        attempt.agent,
        attempt.request.nonce,
    ):
        raise refuse("a noop only retires an attempt at its own nonce")


def require_leverage(trading: Trading, attempt: Attempt, action: UpdateLeverage, authority):
    target = trading.target(attempt.coin or "")
    configured = (target.asset, target.margin == "cross", target.leverage)
    if attempt.purpose != "leverage" or (action.asset, action.is_cross, action.leverage) != (
        configured
    ):
        raise refuse("leverage is the configured leverage and mode of a configured market")
    account, orders = authority.account, authority.orders
    if account is None or orders is None:
        raise refuse("leverage is checked against the Safe's observed position and orders")
    asked(account, "clearinghouseState", trading.safe)
    asked(orders, "openOrders", trading.safe)
    if position(account, target.coin) != 0 or any(o.coin == target.coin for o in typed(orders)):
        raise refuse("leverage is set only while the Safe holds no position or order there")


def require_order(trading, attempt: Attempt, action: OrderAction, authority, now, reconciled):
    order = action.order
    if isinstance(order, TriggerOrder):
        require_trigger(trading, attempt, order, authority, now, reconciled)
        return
    target = trading.target(attempt.coin or "")
    if (
        attempt.purpose not in ("entry", "exit")
        or order.asset != target.asset
        or order.reduce_only != (attempt.purpose == "exit")
    ):
        raise refuse("the order is not the configured market's entry or reduce-only exit")
    book = require_fresh_book(trading, attempt, now)
    within(order.price, best(book, order.is_buy), order.is_buy, target)
    if attempt.purpose == "exit":
        held, account = exit_ownership(trading, reconciled, authority, attempt.coin or "")
        require_reducing(attempt, order.is_buy, order.size, account, held)
        require_unreserved(trading, attempt, order.is_buy, order.size, held)
        if attempt.rests:
            require_no_outside(attempt, account, held)
    else:
        require_configured_leverage(trading, target, authority.active)


def within(price: Decimal, reference: Decimal, is_buy: bool, target: PerpTarget) -> None:
    """A buy no higher, or a sell no lower, than the reference moved by the slippage."""
    slip = Decimal(target.slippage_bps) / BASIS_POINTS
    bound = reference * (1 + slip) if is_buy else reference * (1 - slip)
    if (price > bound) if is_buy else (price < bound):
        raise refuse("the limit price is outside the author's slippage of its reference")


def require_trigger(trading, attempt: Attempt, order: TriggerOrder, authority, now, reconciled):
    """A protective TP or SL: reduce-only, triggered on the side of the mark HyperCore
    requires for its kind, and limited within the slippage of its trigger."""
    target = trading.target(attempt.coin or "")
    if attempt.purpose != "protect" or order.asset != target.asset:
        raise refuse("a trigger order is a configured market's protective order")
    mark = require_fresh_mark(trading, attempt, authority.active, now)
    # A stop that sells and a take-profit that buys trigger below the mark.
    below = (order.tpsl == "sl") != order.is_buy
    if (order.trigger_price >= mark) if below else (order.trigger_price <= mark):
        raise refuse("the trigger price is on the wrong side of the mark for its kind")
    within(order.price, order.trigger_price, order.is_buy, target)
    if (order.price < order.trigger_price) if order.is_buy else (order.price > order.trigger_price):
        raise refuse("a trigger order's limit lies beyond its trigger")
    held, account = exit_ownership(trading, reconciled, authority, attempt.coin or "")
    require_reducing(attempt, order.is_buy, order.size, account, held)
    require_unreserved(trading, attempt, order.is_buy, order.size, held)
    require_no_outside(attempt, account, held)


def require_fresh_mark(trading: Trading, attempt: Attempt, active, now: int) -> Decimal:
    """The market's mark from an `activeAssetData` read within the book age limit."""
    if active is None:
        raise refuse("a trigger order is checked against the market's observed mark")
    asked(active, "activeAssetData", trading.safe)
    policy = trading.policy
    if typed(active).coin != attempt.coin or not (
        -policy.clock_skew <= now - active.received_at <= policy.book_age
    ):
        raise refuse("the mark observation is not a fresh read of the order's market")
    return Decimal(typed(active).markPx)


def require_unreserved(trading: Trading, attempt: Attempt, is_buy: bool, size, held) -> None:
    """Every live reduce-only order the strategy owns in the market, with this one, fits
    inside the owned position."""
    live = trading.reducing(attempt.coin or "", is_buy)
    if live + size > abs(held):
        raise refuse(
            "live reduce-only orders and this one exceed the strategy's own position",
            live=str(live),
            owned=str(held),
        )


def require_no_outside(attempt: Attempt, account, held: Decimal) -> None:
    """A reduce-only order that may rest needs the Safe's venue position in the market to
    be the strategy's own: outside exposure could change what it closes while it rests."""
    if account is None or position(account, attempt.coin or "") != held:
        raise refuse("a resting reduce-only order needs no outside exposure in its market")


def require_cancel(trading: Trading, attempt: Attempt, action: Cancel) -> None:
    """A cancel of a live owned order by its own oid."""
    if attempt.purpose != "cancel" or attempt.cancels is None:
        raise refuse("a cancel names the owned order it cancels")
    order = trading.owned(attempt.cancels)
    target = trading.target(order.coin)
    if not order.live or (attempt.coin, action.asset) != (order.coin, target.asset):
        raise refuse("a cancel names a live owned order of its market")
    if action.oid != order.oid:
        raise refuse("a cancel names the owned order's own oid")


def require_schedule(trading: Trading, attempt: Attempt, action: ScheduleCancel) -> None:
    if attempt.purpose != "schedule":
        raise refuse("a scheduled cancel is the strategy's dead man's switch")
    if action.time is None:
        return
    request = attempt.request
    earliest = request.expires_after + SCHEDULE_LEAD_MS
    if not earliest <= action.time <= request.nonce + trading.policy.schedule_horizon:
        raise refuse("a scheduled cancel lies 5 seconds past its expiry, within the horizon")


def require_margin(trading: Trading, attempt: Attempt, action: UpdateIsolatedMargin, account):
    target = trading.target(attempt.coin or "")
    if attempt.purpose != "margin" or action.asset != target.asset or target.margin != "isolated":
        raise refuse("a margin change moves an isolated configured market's margin")
    if not trading.policy.margin_without_record:
        raise refuse("no /info record identifies an applied margin change (T10)")
    if account is None:
        raise refuse("a margin change is checked against the Safe's observed position")
    asked(account, "clearinghouseState", trading.safe)
    held = venue_position(account, target.coin)
    if held is None or held.leverage.type != "isolated":
        raise refuse("a margin change needs the Safe's isolated position in the market")


def exit_ownership(trading: Trading, reconciled: Book, authority, coin: str):
    """The strategy's position an exit or protective order may close, at the account
    snapshot that also bounds it: the durable book replayed through the Safe's fills up
    to that snapshot's time. Anything that cannot be established there is refused."""
    account = authority.account
    if account is None:
        raise refuse("an exit is checked against the Safe's observed position")
    asked(account, "clearinghouseState", trading.safe)
    if typed(account).time < reconciled.cursor:
        require_unchanged(reconciled, account)
        return reconciled.owned.get(coin, Decimal(0)), account
    if authority.fills is None:
        raise refuse("an exit is checked against the Safe's fills up to its account snapshot")
    proven = proven_exposure(reconciled, account, authority.fills, trading.identities())
    return proven.owned.get(coin, Decimal(0)), account


def require_fresh_book(trading, attempt: Attempt, now: int) -> InfoResult:
    book = attempt.book
    if book is None or book.request.info.type != "l2Book" or typed(book).coin != attempt.coin:
        raise refuse("an order is priced from a retained book of its market")
    policy = trading.policy
    for observed in (book.received_at, typed(book).time):
        if not -policy.clock_skew <= now - observed <= policy.book_age:
            raise refuse("the book observation is older than its age limit or ahead of the clock")
    return book


def require_configured_leverage(trading, target, active) -> None:
    if active is None:
        raise refuse("an entry is checked against the market's observed leverage")
    asked(active, "activeAssetData", trading.safe)
    leverage = typed(active).leverage
    if typed(active).coin != target.coin or (leverage.type, leverage.value) != (
        target.margin,
        target.leverage,
    ):
        raise refuse("the market is not at the configured leverage and margin mode")


def require_reducing(attempt, is_buy: bool, size: Decimal, account: InfoResult, owned: Decimal):
    """An exit closes only the strategy's own exposure, within the venue position, both
    at the same account snapshot."""
    venue = position(account, attempt.coin)
    if owned == 0 or is_buy != (owned < 0) or size > abs(owned):
        raise refuse("an exit only reduces the strategy's own position", owned=str(owned))
    if (venue < 0) != (owned < 0) or size > abs(venue):
        raise refuse("an exit is no larger than the Safe's venue position", venue=str(venue))


def venue_position(account: InfoResult, coin: str):
    """The Safe's position in `coin` from a `clearinghouseState` answer, None when flat."""
    for held in typed(account).assetPositions:
        if held.position.coin == coin:
            return held.position
    return None


def position(account: InfoResult, coin: str) -> Decimal:
    """The signed size of `coin` in a `clearinghouseState` answer, zero when flat."""
    held = venue_position(account, coin)
    return Decimal(0) if held is None else Decimal(held.szi)
