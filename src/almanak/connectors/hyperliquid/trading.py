"""HyperCore trading attempts under the agent's venue submission scope (blueprint 30, 5.1-5.2).

Phase 3 trades one way: leverage set while flat (`updateLeverage`), then an `Ioc` limit
entry and a reduce-only `Ioc` exit, each priced from a retained `l2Book` observation and
the author's slippage. `noop` retires an attempt whose answer was lost. Phase 4 adds
resting `Gtc` and post-only `Alo` limit orders, reduce-only limit TP/SL trigger orders,
cancels, the `scheduleCancel` dead man's switch and, on a market configured isolated,
`updateIsolatedMargin`. A cancel is always signed by the order's retained oid: HyperCore
resolves a cloid only when it executes a cancel, to the newest order carrying it (T5),
so a cancel the caller names by cloid is resolved to the owned order's oid here and
`cancelByCloid` is never signed.

Every attempt is retained before it is signed, with its nonce reserved in the agent's
scope (a millisecond time strictly above the agent's last reserved nonce), an
`expiresAfter` the connector declares and, for an order, a cloid derived from the
deployment and the attempt's own identity, so no two attempts ever share one: HyperCore
does not keep cloids unique (T5). The exact signed body is recorded before it is posted,
the attempt is marked maybe sent before the post, and a lost answer never leads to a
fresh request for the same attempt. An attempt is resolved only by evidence of its own
outcome:

- an answer: `filled` or, for an order that may rest, `resting` (its oid retained beside
  its cloid), an order `error`, a cancel's `success` or `error`, `default` for the
  other actions, or a refusal of the whole action such as "Action already expired";
- without an answer, an order by `orderStatus` of its cloid, then by fills carrying its
  cloid or oid. A status miss with no fill is not proof of non-execution on mainnet
  (open fact T4), so the attempt stays unresolved there; only a venue whose policy
  states assumption A4, the synthesized model, releases an `Ioc` order once the venue
  and the deployment clock are both past its `expiresAfter`, and it may be planned
  afresh. An order that may rest can fill long after its placement expires, so its
  fills are read through the latest observation and a miss never releases it;
- without an answer, `updateLeverage` by `activeAssetData` showing the requested
  leverage and mode, which are absolute, so a match resolves it whichever request
  applied it;
- without an answer, a cancel by `orderStatus` of the order it names: cancelled applies
  it, another final state makes it moot, and an order still live once the venue and the
  clock are past the cancel's `expiresAfter` shows that it did not apply;
- `scheduleCancel` and `updateIsolatedMargin` only by an answer, to the attempt or to a
  resend of its recorded bytes: no `/info` query reads either (T9, T10), so without one
  they stay unresolved and halt the deployment. A margin change is never planned
  afresh, since a second request could move the amount twice.

By D6 a resting order is owned state, not pending work. The attempt that places it
resolves once evidence shows the order exists; its retained oid and unique cloid then
make it an owned order of the document, whose life (open, then filled, cancelled,
triggered or rejected) is read from `orderStatus` by oid while further attempts go
ahead. One attempt is unresolved at a time; owned orders do not count against that.

Attempts never signed or never sent are released at once: signers never broadcast.
Consecutive releases of entries and leverage changes count against the connector's
re-plan budget; a reduce-only exit or protective order is never held back by it, so risk
can always be reduced.
"""

from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Annotated, Literal

from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.hyperliquid.markets import find_market, perp_markets
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_exchange import (
    Accepted,
    CancelResponse,
    ErrorStatus,
    FilledStatus,
    OrderResponse,
    Refused,
    RestingStatus,
)
from almanak.gateway.hypercore_info import Coin, InfoResult, TimeRange, typed
from almanak.gateway.hypercore_shapes import Cloid, KnownOrder, Millis, OrderState
from almanak.gateway.hypercore_signing import (
    NONCE_FUTURE_MS,
    ActionRequest,
    Cancel,
    LimitOrder,
    Noop,
    OrderAction,
    ScheduleCancel,
    SignedAction,
    Tif,
    Tpsl,
    TriggerOrder,
    UpdateIsolatedMargin,
    UpdateLeverage,
)
from almanak.models import Address, Model, UInt

AttemptId = Annotated[str, Field(pattern=r"^[1-9][0-9]{0,8}$")]
Purpose = Literal["leverage", "entry", "exit", "protect", "cancel", "schedule", "margin", "retire"]
Margin = Literal["cross", "isolated"]
# reserved: retained with its nonce, not signed. signed: its exact body recorded, never
# sent. sent: maybe sent, outcome not yet known. applied / not_applied: resolved.
Status = Literal["reserved", "signed", "sent", "applied", "not_applied"]
UNRESOLVED = ("reserved", "signed", "sent")
# The purposes the re-plan budget limits.
BUDGETED = ("entry", "leverage")
# The order states in which HyperCore still holds an order.
LIVE = ("open", "triggered")
BASIS_POINTS = Decimal(10_000)
USDC_PLACES = 6
# HyperCore perp prices carry at most five significant figures and 6 - szDecimals
# decimals; an integer price is always admitted.
PRICE_DECIMALS = 6
SIGNIFICANT = 5
# A fills page this long may be truncated (phase 1 recordings and documentation).
TRUNCATED_FILLS = 2000
# HyperCore refuses a scheduled cancel less than 5 seconds ahead (API documentation).
SCHEDULE_LEAD_MS = 5_000


class TradingPolicy(Model):
    """Connector-declared bounds, in milliseconds."""

    # `expiresAfter` is the decision time plus this: the deadline the signed action itself
    # enforces, after which an order without evidence is non-execution (A4).
    expiry: Annotated[int, Field(strict=True, gt=0, le=600_000)] = 30_000
    # The oldest `l2Book` observation an order may be priced from, by the venue's own
    # book time and by when it was received.
    book_age: Annotated[int, Field(strict=True, gt=0)] = 5_000
    # How far the venue's book time may run ahead of the deployment clock.
    clock_skew: Annotated[int, Field(strict=True, ge=0)] = 1_000
    # Consecutive entries or leverage changes released without effect before the next
    # one halts. Exits and protective orders are never counted or held back.
    replans: Annotated[int, Field(strict=True, ge=0)] = 3
    # How far ahead of its nonce a scheduled cancel may lie.
    schedule_horizon: Annotated[int, Field(strict=True, gt=SCHEDULE_LEAD_MS)] = 3_600_000
    # Whether `unknownOid` with no fill past `expiresAfter` counts as non-execution. Only
    # a venue that states assumption A4 sets it: on mainnet T4 is open.
    release_unknown_after_expiry: bool = False
    # Whether `updateIsolatedMargin` is admitted although no `/info` record identifies
    # an applied one (T10), so that a lost answer halts the deployment for good. Only a
    # venue that states assumption A10 sets it: section 5.2 admits it on mainnet only
    # once a record is identified.
    margin_without_record: bool = False


class PerpTarget(Model):
    """A configured first-dex market with its asset index pinned from `meta`."""

    coin: Coin
    asset: Annotated[int, Field(strict=True, ge=0, lt=10_000)]
    size_decimals: Annotated[int, Field(strict=True, ge=0, le=PRICE_DECIMALS)]
    max_leverage: Annotated[int, Field(strict=True, ge=1)]
    # The author's fixed leverage, margin mode and slippage.
    leverage: Annotated[int, Field(strict=True, ge=1)]
    slippage_bps: Annotated[int, Field(strict=True, gt=0, le=1_000)]
    margin: Margin = "cross"
    # The `metaAndAssetCtxs` answer the index was read from. An installed connector's
    # checkpoint leaves it out: its configuration's resolved evidence keeps the answer.
    evidence: InfoResult | None = None

    @model_validator(mode="after")
    def within(self):
        if self.leverage > self.max_leverage:
            raise ValueError("the configured leverage exceeds the market's maximum")
        return self


def resolve_target(
    result: InfoResult, coin: str, leverage: int, slippage_bps: int, margin: Margin = "cross"
) -> PerpTarget:
    """The market `meta` names `coin`, pinned with its index, or a refusal."""
    if result.request.info.type != "metaAndAssetCtxs":
        raise ValueError("a perp target resolves from metaAndAssetCtxs")
    market = find_market(perp_markets(typed(result)), coin)
    meta = market.meta
    if meta.isDelisted:
        raise DataError("unsupported", f"HyperCore market {meta.name} is delisted")
    if margin == "cross" and (meta.onlyIsolated or meta.marginMode is not None):
        raise DataError("unsupported", f"HyperCore market {meta.name} admits no cross position")
    return PerpTarget(
        coin=meta.name,
        asset=market.asset,
        size_decimals=meta.szDecimals,
        max_leverage=meta.maxLeverage,
        leverage=leverage,
        slippage_bps=slippage_bps,
        margin=margin,
        evidence=result,
    )


class Attempt(Model):
    id: AttemptId
    purpose: Purpose
    coin: Coin | None
    agent: Address
    request: ActionRequest
    # The `l2Book` observation an order's limit or trigger price was checked against.
    book: InfoResult | None = None
    status: Status = "reserved"
    signed: SignedAction | None = None
    answer: Annotated[Accepted | Refused, Field(discriminator="status")] | None = None
    oids: tuple[int, ...] = ()
    # The observations that resolved it without an answer.
    evidence: tuple[InfoResult, ...] = ()
    # A `noop` names the attempt whose nonce it retires.
    retires: AttemptId | None = None
    # A cancel names the owned order it cancels.
    cancels: UInt | None = None
    # Released before signing because a read admission rests on, or the signer, was
    # unavailable: not a refusal, so it neither counts against the re-plan budget nor
    # ends a streak of refusals.
    unavailable: bool = False

    @property
    def cloid(self) -> str | None:
        action = self.request.action
        return action.order.cloid if isinstance(action, OrderAction) else None

    @property
    def resolved(self) -> bool:
        return self.status not in UNRESOLVED

    @property
    def rests(self) -> bool:
        """Whether the attempt places an order HyperCore may hold: a `Gtc` or `Alo` limit
        or a trigger order, never an `Ioc` one."""
        action = self.request.action
        if not isinstance(action, OrderAction):
            return False
        order = action.order
        return isinstance(order, TriggerOrder) or order.tif != "Ioc"


class OwnedOrder(Model):
    """A strategy order HyperCore holds or held (D6), known by the attempt that placed it,
    its retained oid and its unique cloid."""

    attempt: AttemptId
    coin: Coin
    oid: UInt
    cloid: Cloid
    status: OrderState = "open"
    # The latest `orderStatus` answer that set its state, absent while only the placing
    # answer is known.
    evidence: InfoResult | None = None

    @property
    def live(self) -> bool:
        return self.status in LIVE

    def remaining(self, placed: Decimal) -> Decimal:
        """The size still open: the latest `orderStatus` read's, else the placed size."""
        if self.evidence is None:
            return placed
        known = typed(self.evidence)
        return Decimal(known.order.order.sz) if isinstance(known, KnownOrder) else placed


class Trading(Model):
    deployment: Annotated[str, Field(min_length=1, max_length=128)]
    safe: Address
    targets: tuple[PerpTarget, ...]
    policy: TradingPolicy = TradingPolicy()
    # The agent submission scope: each agent's last reserved nonce.
    nonces: dict[Address, int] = {}
    attempts: tuple[Attempt, ...] = ()
    orders: tuple[OwnedOrder, ...] = ()
    # The time of the last applied `scheduleCancel`, or None once cleared or never set.
    schedule: Millis | None = None
    # Settled attempts the store moved out of the document (`trading_store.py`). Identities
    # continue after every attempt ever retained, these included.
    archived: Annotated[int, Field(strict=True, ge=0)] = 0

    @model_validator(mode="after")
    def unique(self):
        ids = [a.id for a in self.attempts]
        cloids = [a.cloid for a in self.attempts if a.cloid is not None]
        if len(set(ids)) != len(ids) or len(set(cloids)) != len(cloids):
            raise ValueError("attempt identities and cloids are never reused")
        if any(int(i) > self.archived + len(ids) for i in ids):
            raise ValueError("an attempt identity is above every attempt ever retained")
        if len({o.oid for o in self.orders}) != len(self.orders):
            raise ValueError("an owned order is retained once")
        if len({t.coin for t in self.targets}) != len(self.targets):
            raise ValueError("a market is configured once")
        return self

    def target(self, coin: str) -> PerpTarget:
        for target in self.targets:
            if target.coin == coin:
                return target
        raise DataError("authority", f"HyperCore market {coin} is not a configured target")

    def attempt(self, attempt_id: str) -> Attempt:
        for attempt in self.attempts:
            if attempt.id == attempt_id:
                return attempt
        raise KeyError(attempt_id)

    def owned(self, oid: int) -> OwnedOrder:
        for order in self.orders:
            if order.oid == oid:
                return order
        raise DataError("authority", f"HyperCore order {oid} is not one of the strategy's")

    def live(self) -> tuple[OwnedOrder, ...]:
        return tuple(o for o in self.orders if o.live)

    def owned_by_cloid(self, cloid: str) -> OwnedOrder:
        for order in self.orders:
            if order.cloid == cloid:
                return order
        raise DataError("authority", f"HyperCore cloid {cloid} is not one of the strategy's")

    def reducing(self, coin: str, is_buy: bool) -> Decimal:
        """The remaining size of every live reduce-only order the strategy owns in `coin`
        on this side: what its exits and protective orders may still close."""
        total = Decimal(0)
        for order in self.live():
            placed = self.attempt(order.attempt).request.action
            if not isinstance(placed, OrderAction) or order.coin != coin:
                continue
            if placed.order.reduce_only and placed.order.is_buy == is_buy:
                total += order.remaining(placed.order.size)
        return total

    def armed(self, now: int) -> bool:
        """Whether a scheduled cancel the strategy set is still ahead."""
        return self.schedule is not None and self.schedule > now

    @property
    def pending(self) -> Attempt | None:
        """The one attempt not yet resolved; a new one waits for it."""
        return next((a for a in self.attempts if not a.resolved), None)

    @property
    def released(self) -> int:
        """Entries and leverage changes released without effect since the last one that
        applied: the current re-plan streak."""
        return sum(1 for a in self.streak() if a.status == "not_applied")

    def streak(self) -> tuple[Attempt, ...]:
        """The counted attempts `released` walks back over, newest first: those released
        since the last one that did not release, and that one."""
        walked: list[Attempt] = []
        for attempt in reversed(self.attempts):
            if attempt.purpose not in BUDGETED or attempt.unavailable:
                continue
            walked.append(attempt)
            if attempt.status != "not_applied":
                break
        return tuple(walked)

    def identities(self) -> tuple[frozenset[str], frozenset[int]]:
        """Every cloid and oid of the strategy's own orders, released ones included."""
        cloids = frozenset(a.cloid for a in self.attempts if a.cloid is not None)
        return cloids, frozenset(oid for a in self.attempts for oid in a.oids)


def cloid_for(deployment: str, attempt_id: str) -> str:
    """A cloid unique to this attempt of this deployment, released attempts included."""
    return "0x" + keccak(text=f"almanak-hypercore-cloid:{deployment}:{attempt_id}")[:16].hex()


def reserve(trading: Trading, agent: str, now: int) -> tuple[Trading, int]:
    """The next nonce in the agent's scope: the clock, strictly above its last one."""
    nonce = max(now, trading.nonces.get(agent, 0) + 1)
    return trading.model_copy(update={"nonces": {**trading.nonces, agent: nonce}}), nonce


def plan(trading: Trading, purpose: Purpose, coin, agent: str, action, now: int, **fields):
    """Retain a new attempt with its nonce reserved, before anything is signed."""
    if trading.pending is not None:
        raise ValueError("an attempt is unresolved; it is reconciled before another")
    if purpose in BUDGETED and trading.released >= trading.policy.replans:
        raise DataError("authority", "HyperCore attempts exhausted their re-plan budget")
    trading, nonce = reserve(trading, agent, now)
    request = ActionRequest(action=action, nonce=nonce, expires_after=now + trading.policy.expiry)
    attempt = Attempt(
        id=next_id(trading),
        purpose=purpose,
        coin=coin,
        agent=agent,
        request=request,
        **fields,
    )
    return trading.model_copy(update={"attempts": (*trading.attempts, attempt)}), attempt


def next_id(trading: Trading) -> str:
    """The identity after every attempt ever retained, archived ones included."""
    return str(trading.archived + len(trading.attempts) + 1)


def plan_leverage(trading: Trading, coin: str, agent: str, now: int):
    target = trading.target(coin)
    action = UpdateLeverage(
        asset=target.asset, is_cross=target.margin == "cross", leverage=target.leverage
    )
    return plan(trading, "leverage", coin, agent, action, now)


def best(book: InfoResult, is_buy: bool) -> Decimal:
    """The price an `Ioc` order crosses first: the best ask to buy, the best bid to sell."""
    levels = typed(book).levels[1 if is_buy else 0]
    if not levels:
        raise DataError("unavailable", "the HyperCore book has no level on that side")
    return Decimal(levels[0].px)


def mid(book: InfoResult) -> Decimal:
    """The book's mid price, which HyperCore holds a trigger price against."""
    return (best(book, True) + best(book, False)) / 2


def tick(raw: Decimal, rounding: str, size_decimals: int) -> Decimal:
    """`raw` on the market's price grid: five significant figures and 6 - szDecimals
    places, an integer always admitted."""
    places = PRICE_DECIMALS - size_decimals
    price = raw.quantize(Decimal(1).scaleb(-places), rounding)
    if price != price.to_integral_value():
        whole = price.adjusted() + 1
        places = min(places, max(0, SIGNIFICANT - whole))
        price = raw.quantize(Decimal(1).scaleb(-places), rounding)
    return price


def limit_price(reference: Decimal, is_buy: bool, slippage_bps: int, size_decimals: int):
    """The reference moved by the slippage and rounded toward the passive side, so the
    rounded limit never exceeds the author's slippage."""
    factor = 1 + Decimal(slippage_bps) / BASIS_POINTS * (1 if is_buy else -1)
    return tick(reference * factor, ROUND_DOWN if is_buy else ROUND_UP, size_decimals)


def on_grid(price: Decimal, size_decimals: int) -> Decimal:
    """A strategy-chosen price, refused unless it already lies on the market's grid."""
    if price <= 0 or tick(price, ROUND_DOWN, size_decimals) != price:
        raise ValueError("the price is not on the market's price grid")
    return price


def order_size(size: Decimal, size_decimals: int) -> Decimal:
    rounded = size.quantize(Decimal(1).scaleb(-size_decimals), ROUND_DOWN)
    if rounded <= 0:
        raise ValueError("the order size rounds to nothing at the market's size decimals")
    return rounded


def plan_order(trading, coin, agent, is_buy, size, reduce_only, book, now):
    target = trading.target(coin)
    if typed(book).coin != coin:
        raise ValueError("an order is priced from its own market's book")
    order = LimitOrder(
        asset=target.asset,
        is_buy=is_buy,
        price=limit_price(best(book, is_buy), is_buy, target.slippage_bps, target.size_decimals),
        size=order_size(size, target.size_decimals),
        reduce_only=reduce_only,
        cloid=cloid_for(trading.deployment, next_id(trading)),
    )
    purpose: Purpose = "exit" if reduce_only else "entry"
    return plan(trading, purpose, coin, agent, OrderAction(order=order), now, book=book)


def plan_entry(trading, coin: str, agent: str, is_buy: bool, size: Decimal, book, now: int):
    return plan_order(trading, coin, agent, is_buy, size, False, book, now)


def plan_exit(trading, coin: str, agent: str, position: Decimal, book, now: int):
    """A reduce-only `Ioc` closing the whole owned position."""
    if position == 0:
        raise ValueError("no position to close")
    return plan_order(trading, coin, agent, position < 0, abs(position), True, book, now)


class RestingOrder(Model):
    """What a strategy asks of a resting limit order."""

    is_buy: bool
    size: Decimal
    price: Decimal
    tif: Tif
    reduce_only: bool = False

    @model_validator(mode="after")
    def rests(self):
        if self.tif == "Ioc":
            raise ValueError("a resting order is Gtc or Alo")
        return self


def plan_resting(trading, coin, agent, order: RestingOrder, book, now: int):
    """A `Gtc` or `Alo` limit order at the strategy's own price, which may rest. The
    gateway bounds its price as an `Ioc` order's: it never crosses the retained book by
    more than the author's slippage."""
    target = trading.target(coin)
    if typed(book).coin != coin:
        raise ValueError("an order is checked against its own market's book")
    limit = LimitOrder(
        asset=target.asset,
        is_buy=order.is_buy,
        price=on_grid(order.price, target.size_decimals),
        size=order_size(order.size, target.size_decimals),
        reduce_only=order.reduce_only,
        tif=order.tif,
        cloid=cloid_for(trading.deployment, next_id(trading)),
    )
    purpose: Purpose = "exit" if order.reduce_only else "entry"
    return plan(trading, purpose, coin, agent, OrderAction(order=limit), now, book=book)


def plan_trigger(trading, coin, agent, position, tpsl: Tpsl, trigger, now: int):
    """A reduce-only limit TP or SL over `position` of the owned position, placed once the
    mark reaches `trigger`; its limit is the trigger moved by the author's slippage.
    Admission checks the trigger's side against a fresh mark and establishes ownership
    at its own account snapshot, as an exit's."""
    if position == 0:
        raise ValueError("no position to protect")
    target = trading.target(coin)
    is_buy = position < 0
    trigger = on_grid(trigger, target.size_decimals)
    order = TriggerOrder(
        asset=target.asset,
        is_buy=is_buy,
        price=limit_price(trigger, is_buy, target.slippage_bps, target.size_decimals),
        size=order_size(abs(position), target.size_decimals),
        trigger_price=trigger,
        tpsl=tpsl,
        cloid=cloid_for(trading.deployment, next_id(trading)),
    )
    action = OrderAction(order=order)
    return plan(trading, "protect", coin, agent, action, now)


def plan_cancel(trading: Trading, oid: int, agent: str, now: int):
    """A cancel of a live owned order by its retained oid, never by its cloid: HyperCore
    resolves a cloid when it executes the cancel, to the newest order carrying it (T5)."""
    order = trading.owned(oid)
    if not order.live:
        raise ValueError("only a live owned order is cancelled")
    action = Cancel(asset=trading.target(order.coin).asset, oid=order.oid)
    return plan(trading, "cancel", order.coin, agent, action, now, cancels=oid)


def plan_schedule(trading: Trading, agent: str, time: int | None, now: int):
    """Set the dead man's switch to `time`, or clear it when `time` is None."""
    return plan(trading, "schedule", None, agent, ScheduleCancel(time=time), now)


def plan_margin(trading: Trading, coin: str, agent: str, amount: Decimal, now: int):
    """Add (positive) or remove (negative) USDC of the isolated position's margin."""
    target = trading.target(coin)
    if target.margin != "isolated":
        raise ValueError("margin moves only on a market configured isolated")
    ntli = amount.scaleb(USDC_PLACES)
    if ntli != ntli.to_integral_value():
        raise ValueError("a margin change is a whole number of micro-USDC")
    action = UpdateIsolatedMargin(asset=target.asset, ntli=int(ntli))
    return plan(trading, "margin", coin, agent, action, now)


def plan_retire(trading: Trading, attempt_id: str, now: int):
    """A `noop` at a maybe-sent attempt's own nonce, so its signed bytes can no longer
    apply if they have not already (assumption A4). The attempt still resolves only by
    its own evidence."""
    retired = trading.attempt(attempt_id)
    if retired.status != "sent":
        raise ValueError("only a maybe-sent attempt is retired")
    request = ActionRequest(
        action=Noop(), nonce=retired.request.nonce, expires_after=now + trading.policy.expiry
    )
    attempt = Attempt(
        id=next_id(trading),
        purpose="retire",
        coin=None,
        agent=retired.agent,
        request=request,
        retires=attempt_id,
    )
    return trading.model_copy(update={"attempts": (*trading.attempts, attempt)}), attempt


def replaced(trading: Trading, attempt: Attempt) -> Trading:
    attempts = tuple(attempt if a.id == attempt.id else a for a in trading.attempts)
    return trading.model_copy(update={"attempts": attempts})


def changed(trading: Trading, attempt_id: str, expected: Status, **fields) -> Trading:
    attempt = trading.attempt(attempt_id)
    if attempt.status != expected:
        raise ValueError(f"attempt {attempt_id} is {attempt.status}, not {expected}")
    return replaced(trading, attempt.model_copy(update=fields))


def signed(trading: Trading, attempt_id: str, signature: SignedAction) -> Trading:
    """Record the exact signed body before anything is posted."""
    attempt = trading.attempt(attempt_id)
    if (signature.request, signature.agent) != (attempt.request, attempt.agent):
        raise ValueError("the signed action is not the retained attempt")
    return changed(trading, attempt_id, "reserved", status="signed", signed=signature)


def refused(trading: Trading, attempt_id: str) -> Trading:
    """The gateway refused to admit or sign the attempt; nothing was sent."""
    return changed(trading, attempt_id, "reserved", status="not_applied")


def unavailable(trading: Trading, attempt_id: str) -> Trading:
    """A read admission rests on, or the signer, was unavailable; nothing was signed or
    sent, and the release is not counted against the re-plan budget."""
    return changed(trading, attempt_id, "reserved", status="not_applied", unavailable=True)


def sending(trading: Trading, attempt_id: str) -> Trading:
    """Mark the attempt maybe sent; only then are its recorded bytes posted."""
    return changed(trading, attempt_id, "signed", status="sent")


def answered(trading: Trading, attempt_id: str, answer: Accepted | Refused) -> Trading:
    """Resolve a sent attempt from the venue's answer, and keep what it shows: a resting
    order owned, a cancelled order cancelled, a scheduled cancel's time."""
    attempt = trading.attempt(attempt_id)
    status, oids = outcome(attempt, answer)
    trading = changed(trading, attempt_id, "sent", status=status, answer=answer, oids=oids)
    if status == "applied" and isinstance(answer, Accepted):
        trading = took_effect(trading, trading.attempt(attempt_id), answer)
    return trading


def outcome(attempt: Attempt, answer: Accepted | Refused) -> tuple[Status, tuple[int, ...]]:
    if isinstance(answer, Refused):
        return "not_applied", ()
    response, action = answer.response, attempt.request.action
    if isinstance(action, OrderAction):
        return order_outcome(attempt, response)
    if isinstance(action, Cancel):
        return cancel_outcome(response)
    if isinstance(response, OrderResponse | CancelResponse):
        raise DataError("inconsistent", "the exchange answered another action than the attempt")
    return "applied", ()


def order_outcome(attempt: Attempt, response) -> tuple[Status, tuple[int, ...]]:
    if not isinstance(response, OrderResponse):
        raise DataError("malformed", "the exchange answered an order without its status")
    (status,) = response.data.statuses
    if isinstance(status, ErrorStatus):
        return "not_applied", ()
    # An Ioc order never rests and a trigger order never fills when placed; an answer that
    # does either, or that names another cloid, is not the attempt's own.
    if isinstance(status, RestingStatus) and attempt.rests:
        oid, cloid = status.resting.oid, status.resting.cloid
    elif isinstance(status, FilledStatus) and not is_trigger(attempt):
        oid, cloid = status.filled.oid, status.filled.cloid
    else:
        raise DataError("inconsistent", "the exchange answered an order that is not the attempt")
    if cloid not in (None, attempt.cloid):
        raise DataError("inconsistent", "the exchange answered an order that is not the attempt")
    return "applied", (oid,)


def is_trigger(attempt: Attempt) -> bool:
    action = attempt.request.action
    return isinstance(action, OrderAction) and isinstance(action.order, TriggerOrder)


def cancel_outcome(response) -> tuple[Status, tuple[int, ...]]:
    if not isinstance(response, CancelResponse):
        raise DataError("malformed", "the exchange answered a cancel without its status")
    (status,) = response.data.statuses
    return ("not_applied" if isinstance(status, ErrorStatus) else "applied"), ()


def took_effect(trading: Trading, attempt: Attempt, answer: Accepted) -> Trading:
    action = attempt.request.action
    if attempt.rests:
        state: OrderState = "open"
        if isinstance(answer.response, OrderResponse):
            (status,) = answer.response.data.statuses
            state = "filled" if isinstance(status, FilledStatus) else "open"
        return kept(trading, attempt, state, None)
    if isinstance(action, Cancel) and attempt.cancels is not None:
        return moved(trading, attempt.cancels, "canceled", None)
    if isinstance(action, ScheduleCancel):
        return trading.model_copy(update={"schedule": action.time})
    return trading


def kept(trading: Trading, attempt: Attempt, state: OrderState, evidence) -> Trading:
    """Own the order an applied attempt placed, under its oid and cloid."""
    (oid,) = attempt.oids
    order = OwnedOrder(
        attempt=attempt.id,
        coin=attempt.coin or "",
        oid=oid,
        cloid=attempt.cloid or "",
        status=state,
        evidence=evidence,
    )
    return trading.model_copy(update={"orders": (*trading.orders, order)})


def moved(trading: Trading, oid: int, state: OrderState, evidence) -> Trading:
    order = trading.owned(oid).model_copy(update={"status": state, "evidence": evidence})
    orders = tuple(order if o.oid == oid else o for o in trading.orders)
    return trading.model_copy(update={"orders": orders})


def released_unsent(trading: Trading) -> Trading:
    """On restart: an attempt never signed or never sent is released at once."""
    for attempt in trading.attempts:
        if attempt.status in ("reserved", "signed"):
            trading = replaced(trading, attempt.model_copy(update={"status": "not_applied"}))
    return trading


def past_expiry(attempt: Attempt, observation: InfoResult, now: int) -> bool:
    expiry = attempt.request.expires_after
    return observation.received_at > expiry and now > expiry


def recovered_order(
    trading: Trading, attempt: Attempt, status: InfoResult, fills: InfoResult | None, now: int
) -> Trading:
    """A maybe-sent order resolved by `orderStatus` of its own cloid, then by its fills.

    A known order resolves from its status alone; fills are needed only for a miss."""
    query = status.request.info
    if query.type != "orderStatus" or (query.user, query.oid) != (trading.safe, attempt.cloid):
        raise ValueError("an order recovers from orderStatus of its own cloid")
    known = typed(status)
    if isinstance(known, KnownOrder):
        oid = known.order.order.oid
        trading = changed(
            trading, attempt.id, "sent", status="applied", oids=(oid,), evidence=(status,)
        )
        return owned_after(trading, attempt.id, known.order.status, status)
    if fills is None:
        raise ValueError("an order whose status is unknown recovers from its fills")
    query = fills.request.info
    if attempt.rests and (not isinstance(query, TimeRange) or query.end_time < status.received_at):
        raise ValueError("an order that may rest recovers from fills read to its status")
    own = own_fills(trading, attempt, fills)
    if own:
        oids = tuple(sorted({fill.oid for fill in own}))
        trading = changed(
            trading, attempt.id, "sent", status="applied", oids=oids, evidence=(status, fills)
        )
        # Its state is read by oid at the next reconciliation of owned orders.
        return owned_after(trading, attempt.id, "open", None)
    # A4 states that an Ioc order the venue does not know after its expiry never ran. An
    # order that may rest can still fill after its placement expires, so a miss never
    # releases one.
    if (
        trading.policy.release_unknown_after_expiry
        and not attempt.rests
        and past_expiry(attempt, status, now)
        and past_expiry(attempt, fills, now)
    ):
        return changed(trading, attempt.id, "sent", status="not_applied", evidence=(status, fills))
    return trading


def owned_after(trading: Trading, attempt_id: str, state: OrderState, evidence) -> Trading:
    attempt = trading.attempt(attempt_id)
    return kept(trading, attempt, state, evidence) if attempt.rests else trading


def fill_range(attempt: Attempt, through: int = 0) -> tuple[int, int]:
    """The times an attempt could have filled in: HyperCore refuses a nonce more than a
    day ahead of its time and any action past `expiresAfter`. An order that may rest can
    fill later too, so its range runs to `through`, the latest observation."""
    end = attempt.request.expires_after
    return attempt.request.nonce - NONCE_FUTURE_MS, max(end, through) if attempt.rests else end


def own_fills(trading: Trading, attempt: Attempt, fills: InfoResult) -> list:
    query = fills.request.info
    start, end = fill_range(attempt)
    if query.type != "userFillsByTime" or (query.user, query.start_time) != (trading.safe, start):
        raise ValueError("an order recovers from the Safe's fills over its own range")
    if query.end_time < end:
        raise ValueError("an order's fills are read to its expiry")
    page = typed(fills)
    if len(page) >= TRUNCATED_FILLS:
        raise DataError("incomplete", "the fills page an order recovers from may be truncated")
    return [f for f in page if f.cloid == attempt.cloid or f.oid in attempt.oids]


def recovered_leverage(trading: Trading, attempt: Attempt, active: InfoResult, now: int):
    """A maybe-sent `updateLeverage` resolved by `activeAssetData` of its market."""
    query = active.request.info
    if query.type != "activeAssetData" or (query.user, query.coin) != (trading.safe, attempt.coin):
        raise ValueError("leverage recovers from activeAssetData of its own market")
    leverage = typed(active).leverage
    action = attempt.request.action
    if not isinstance(action, UpdateLeverage):
        raise ValueError("only leverage recovers from activeAssetData")
    mode = "cross" if action.is_cross else "isolated"
    if (leverage.type, leverage.value) == (mode, action.leverage):
        return changed(trading, attempt.id, "sent", status="applied", evidence=(active,))
    if past_expiry(attempt, active, now):
        return changed(trading, attempt.id, "sent", status="not_applied", evidence=(active,))
    return trading


def refreshed(trading: Trading, status: InfoResult) -> Trading:
    """An owned order's state from `orderStatus` of its own oid."""
    query = status.request.info
    if query.type != "orderStatus" or query.user != trading.safe or isinstance(query.oid, str):
        raise ValueError("an owned order is read from orderStatus of its oid at the Safe")
    order = trading.owned(query.oid)
    known = typed(status)
    if not isinstance(known, KnownOrder) or known.order.order.oid != order.oid:
        raise DataError(
            "inconsistent",
            "HyperCore no longer reports an order the strategy owns",
            details={"order": order.model_dump(mode="json"), "status": status.result},
        )
    return moved(trading, order.oid, known.order.status, status)


def recovered_cancel(trading: Trading, attempt: Attempt, status: InfoResult, now: int):
    """A maybe-sent cancel resolved by `orderStatus` of the order it names: cancelled
    applies it, another final state makes it moot, and an order still live once the venue
    and the clock are past the cancel's expiry shows that it did not apply."""
    query = status.request.info
    if query.type != "orderStatus" or attempt.cancels is None or query.oid != attempt.cancels:
        raise ValueError("a cancel recovers from orderStatus of the order it names")
    trading = refreshed(trading, status)
    order = trading.owned(attempt.cancels)
    if order.status == "canceled":
        return changed(trading, attempt.id, "sent", status="applied", evidence=(status,))
    if not order.live or past_expiry(attempt, status, now):
        return changed(trading, attempt.id, "sent", status="not_applied", evidence=(status,))
    return trading


def recovered_retirement(trading: Trading, attempt: Attempt) -> Trading:
    """A maybe-sent `noop` resolves with the attempt it retires: its outcome is that
    attempt's own evidence, and a noop has no effect to account for."""
    retired = trading.attempt(attempt.retires or "")
    if not retired.resolved:
        return trading
    return changed(trading, attempt.id, "sent", status="not_applied", evidence=retired.evidence)
