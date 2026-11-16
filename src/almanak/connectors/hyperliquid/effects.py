"""HyperCore fills, funding and ledger entries as external effects (blueprint 30, section 6).

The Safe's perp account changes through effects no transaction hash identifies one by
one: an order fills against several counterparties, funding is paid every hour, and the
venue closes a position it liquidates. Each effect is applied once, by its own key:

- a fill by market, block time and trade ID (`tid`), read unaggregated;
- a funding payment by market and time (its hash is zero);
- a ledger entry by hash, time, delta type, a digest of its delta and its occurrence
  among identical entries: one backstop liquidation wrote two `liquidation` entries
  sharing hash, time and type (phase 1 recordings), and a page holding a time holds
  every entry at it, so the occurrence is stable across overlapping pages.

Reconciliation advances its cursor only once it proves the range complete. Each page
must cover exactly the cursor to the time of a `clearinghouseState` read before it, and
stay below the size at which it may be truncated (2000 fills, 500 funding payments, 500
ledger entries). The position change of each market must then equal the signed sum of
its new fills, and the perp USDC change (account value less unrealized PnL) must equal
their closed PnL less fees, plus funding and class transfers. Anything else halts with
the pages as evidence and leaves the cursor where it was. A ledger entry of a type the
binding cannot attribute halts too; so does a `liquidation` entry without the liquidation
fills of each position it names. The checks are exact, which the synthesized model
satisfies; whether mainnet's rounded figures need a declared tolerance is open.

The venue position is the Safe's whole position; the strategy owns only what its own
orders filled. A fill is attributed to the strategy by the cloid or oid of one of its
attempts, and anything else, such as an order the Safe owners placed, stays visible as
outside exposure. New fills are replayed in the venue's execution order, each market's
chained by `startPosition` from the reconciled position, and owned exposure is bounded
by the venue position after every fill: a liquidation, or an outside order trading
against it, shrinks it, a flat or opposite position zeroes it, and a later outside fill
never restores it. Fills that do not chain halt, and so do fills of one time whose
possible orders leave different owned exposure: the venue publishes no finer order.

An exit needs only that part: the fills since the cursor and the venue position prove
what the strategy owns (`proven_exposure`), so risk reduction does not wait for cash
accounting, whose failure stays recorded and visible.
"""

import hashlib
import json
from decimal import Decimal
from itertools import groupby
from typing import Literal

from pydantic import JsonValue

from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import InfoResult, TimeRange, typed
from almanak.gateway.hypercore_shapes import Fill, FundingPayment, LedgerUpdate
from almanak.models import Address, Model

# A page this long may be truncated (phase 1 recordings and documentation).
TRUNCATED = {"userFillsByTime": 2000, "userFunding": 500, "userNonFundingLedgerUpdates": 500}
ZERO = Decimal(0)
# The most partial orders of one millisecond's fills followed before a sequence counts
# as ambiguous; fills that chain uniquely explore one per fill.
MAX_ORDERS = 10_000
# Custody's own spot entries, attributed by its requests; they move no perp USDC.
SPOT_ONLY = ("spotTransfer", "send", "accountActivationGas")


class Effect(Model):
    kind: Literal["fill", "funding", "ledger"]
    key: str
    coin: str | None
    time: int
    # The signed change to the market's position and to perp USDC.
    size: Decimal
    cash: Decimal
    # A fill of the Safe's own liquidation: the venue closed the position.
    liquidation: bool = False
    # A fill of one of the strategy's own orders, by its cloid or oid.
    owned: bool = False
    # A fill's position in its market before it (`startPosition`), which orders fills.
    start: Decimal | None = None
    entry: JsonValue


class Book(Model):
    """The reconciled perp account: positions, USDC and every effect applied once."""

    safe: Address
    # The first millisecond not yet reconciled.
    cursor: int
    # The Safe's venue positions, and the part of them the strategy's own fills hold.
    positions: dict[str, Decimal] = {}
    owned: dict[str, Decimal] = {}
    cash: Decimal = Decimal(0)
    effects: tuple[Effect, ...] = ()

    def applied(self) -> frozenset[str]:
        return frozenset(e.key for e in self.effects)

    def outside(self) -> dict[str, Decimal]:
        """Exposure the strategy's own orders do not hold."""
        coins = set(self.positions) | set(self.owned)
        exposure = {c: self.positions.get(c, ZERO) - self.owned.get(c, ZERO) for c in coins}
        return {coin: size for coin, size in exposure.items() if size != 0}


class Pages(Model):
    account: InfoResult
    fills: InfoResult
    funding: InfoResult
    ledger: InfoResult


def fill_effect(fill: Fill, safe: str, ours: tuple[frozenset[str], frozenset[int]]) -> Effect:
    if fill.feeToken != "USDC" or fill.builderFee is not None:
        raise DataError("inconsistent", "a HyperCore fill pays a fee the binding cannot attribute")
    sign = 1 if fill.side == "B" else -1
    liquidated = fill.liquidation is not None and fill.liquidation.liquidatedUser == safe
    return Effect(
        kind="fill",
        key=f"fill:{fill.coin}:{fill.time}:{fill.tid}",
        coin=fill.coin,
        time=fill.time,
        size=sign * Decimal(fill.sz),
        cash=Decimal(fill.closedPnl) - Decimal(fill.fee),
        liquidation=liquidated,
        owned=fill.cloid in ours[0] or fill.oid in ours[1],
        start=Decimal(fill.startPosition),
        entry=fill.model_dump(mode="json", exclude_none=True),
    )


def funding_effect(payment: FundingPayment) -> Effect:
    delta = payment.delta
    return Effect(
        kind="funding",
        key=f"funding:{delta.coin}:{payment.time}",
        coin=delta.coin,
        time=payment.time,
        size=Decimal(0),
        cash=Decimal(delta.usdc),
        entry=payment.model_dump(mode="json"),
    )


def ledger_effects(entries: tuple[LedgerUpdate, ...], fills: list[Effect]) -> list[Effect]:
    effects: list[Effect] = []
    seen: dict[str, int] = {}
    for entry in entries:
        digest = hashlib.sha256(json.dumps(entry.delta, sort_keys=True).encode()).hexdigest()
        base = f"ledger:{entry.hash}:{entry.time}:{entry.delta['type']}:{digest[:16]}"
        seen[base] = seen.get(base, 0) + 1
        effects.append(
            Effect(
                kind="ledger",
                key=f"{base}:{seen[base]}",
                coin=None,
                time=entry.time,
                size=Decimal(0),
                cash=ledger_cash(entry, fills),
                entry=entry.model_dump(mode="json"),
            )
        )
    return effects


def ledger_cash(entry: LedgerUpdate, fills: list[Effect]) -> Decimal:
    delta = entry.delta
    kind = delta["type"]
    if kind == "accountClassTransfer":
        amount = Decimal(str(delta["usdc"]))
        return amount if delta["toPerp"] is True else -amount
    if kind == "liquidation":
        require_liquidation_fills(entry, fills)
        return Decimal(0)
    if kind in SPOT_ONLY:
        return Decimal(0)
    raise DataError(
        "inconsistent",
        f"a HyperCore ledger entry of type {kind} cannot be attributed",
        details={"entry": entry.model_dump(mode="json")},
    )


def require_liquidation_fills(entry: LedgerUpdate, fills: list[Effect]) -> None:
    named = entry.delta.get("liquidatedPositions")
    coins = (
        {p.get("coin") for p in named if isinstance(p, dict)} if isinstance(named, list) else set()
    )
    found = {f.coin for f in fills if f.liquidation and f.time == entry.time}
    if not coins or not coins <= found:
        raise DataError(
            "inconsistent",
            "a HyperCore liquidation entry has no liquidation fill for each position it names",
            details={"entry": entry.model_dump(mode="json")},
        )


def require_range(book: Book, pages: Pages) -> int:
    """The end of the range every page covers: the perp state's own time."""
    end = require_account(book, pages.account)
    for page, kind in zip((pages.fills, pages.funding, pages.ledger), TRUNCATED, strict=True):
        require_page(book, page, kind, pages.account)
    return end


def require_account(book: Book, account: InfoResult) -> int:
    query = account.request.info
    if query.type != "clearinghouseState" or query.user != book.safe:
        raise ValueError("reconciliation reads the Safe's clearinghouseState")
    return typed(account).time


def require_page(book: Book, page: InfoResult, kind: str, account: InfoResult) -> None:
    """One page covering the cursor to the perp state's time, read after it, untruncated."""
    end = typed(account).time
    query = page.request.info
    if not isinstance(query, TimeRange) or query.type != kind:
        raise ValueError(f"reconciliation reads {kind} pages")
    if (query.user, query.start_time, query.end_time) != (book.safe, book.cursor, end):
        raise ValueError("a page covers the cursor to the perp state's time")
    if page.received_at < account.received_at:
        raise ValueError("every page is read after the perp state it is checked against")
    if len(typed(page)) >= TRUNCATED[kind]:
        raise DataError(
            "incomplete",
            f"HyperCore {kind} page may be truncated; its range must be split",
            details={"start": book.cursor, "end": end},
        )


def observed(account: InfoResult) -> tuple[dict[str, Decimal], Decimal]:
    """Each market's position and perp USDC as the venue reports them."""
    state = typed(account)
    positions = {p.position.coin: Decimal(p.position.szi) for p in state.assetPositions}
    unrealized = sum((Decimal(p.position.unrealizedPnl) for p in state.assetPositions), Decimal(0))
    return positions, Decimal(state.marginSummary.accountValue) - unrealized


class Exposure(Model):
    """The positions the new fills leave, and the part of them the strategy owns."""

    positions: dict[str, Decimal]
    owned: dict[str, Decimal]


def exposure(book: Book, fills: list[Effect]) -> Exposure:
    """Replay new fills in execution order from the reconciled positions, bounding owned
    exposure by the venue position after each one."""
    positions, owned = dict(book.positions), dict(book.owned)
    for coin in sorted({f.coin or "" for f in fills}):
        mine = sorted((f for f in fills if f.coin == coin), key=lambda f: f.time)
        for _, group in groupby(mine, key=lambda f: f.time):
            positions[coin], owned[coin] = replayed(
                list(group), positions.get(coin, ZERO), owned.get(coin, ZERO)
            )
    held = {coin: size for coin, size in positions.items() if size != 0}
    return Exposure(positions=held, owned=bounded(owned, held))


def replayed(group: list[Effect], running: Decimal, mine: Decimal) -> tuple[Decimal, Decimal]:
    """One millisecond of one market's fills: the position after them and the part the
    strategy owns. Within one time only `startPosition` orders fills, and it need not
    order them uniquely, so every order that chains is followed: none chaining halts, and
    orders that leave different owned exposure halt as ambiguous."""
    outcomes = chained(group, running, mine)
    if not outcomes:
        raise DataError(
            "inconsistent",
            "HyperCore fills do not chain by startPosition from the reconciled position",
            details={"coin": group[0].coin, "position": str(running)},
        )
    if len(outcomes) > 1:
        raise DataError(
            "ambiguous",
            "HyperCore fills of one time chain in orders that leave different owned exposure",
            details={"coin": group[0].coin, "owned": sorted(str(o) for o in outcomes)},
        )
    return running + sum((f.size for f in group), ZERO), outcomes.pop()


def chained(group: list[Effect], running: Decimal, mine: Decimal) -> set[Decimal]:
    """The owned exposure each order of `group` chaining from `running` leaves."""
    return {owned_after(order, running, mine) for order in orders(group, running)}


def orders(group: list[Effect], running: Decimal) -> list[tuple[Effect, ...]]:
    """Every order of one time's fills that chains by `startPosition` from `running`,
    each complete; the holding replays them as the book proves them (`positions.py`)."""
    found = []
    stack: list[tuple[tuple[int, ...], tuple[int, ...], Decimal]] = [
        ((), tuple(range(len(group))), running)
    ]
    explored = 0
    while stack:
        done, left, position = stack.pop()
        if not left:
            found.append(tuple(group[i] for i in done))
            continue
        explored += 1
        if explored > MAX_ORDERS:
            raise DataError("ambiguous", "HyperCore fills of one time admit too many orders")
        for index in left:
            fill = group[index]
            if fill.start == position:
                rest = tuple(i for i in left if i != index)
                stack.append(((*done, index), rest, position + fill.size))
    return found


def owned_after(order: tuple[Effect, ...], position: Decimal, held: Decimal) -> Decimal:
    for fill in order:
        position += fill.size
        held = within(held + (fill.size if fill.owned else ZERO), position)
    return held


def reconcile(book: Book, pages: Pages, ours: tuple[frozenset[str], frozenset[int]]) -> Book:
    """Apply each new effect once, and advance the cursor only over a proven range.

    `ours` holds the cloids and oids of the strategy's own attempts."""
    end = require_range(book, pages)
    applied = book.applied()
    fills = [fill_effect(f, book.safe, ours) for f in typed(pages.fills)]
    effects = [
        *fills,
        *(funding_effect(p) for p in typed(pages.funding)),
        *ledger_effects(typed(pages.ledger), fills),
    ]
    new = [e for e in effects if e.key not in applied]
    held = exposure(book, [e for e in new if e.kind == "fill"])
    cash = book.cash + sum((e.cash for e in new), Decimal(0))
    require_balance(held.positions, cash, pages.account, pages.model_dump(mode="json"))
    return book.model_copy(
        update={
            "cursor": end + 1,
            "positions": held.positions,
            "owned": held.owned,
            "cash": cash,
            "effects": (*book.effects, *new),
        }
    )


def proven_exposure(
    book: Book, account: InfoResult, fills: InfoResult, ours: tuple[frozenset[str], frozenset[int]]
) -> Exposure:
    """What the strategy owns, from the fills since the cursor and the venue position
    alone, without funding, ledger or cash: enough for a reduce-only exit while cash
    accounting fails. Anything it cannot prove halts."""
    require_account(book, account)
    require_page(book, fills, "userFillsByTime", account)
    applied = book.applied()
    new = [fill_effect(f, book.safe, ours) for f in typed(fills)]
    held = exposure(book, [e for e in new if e.key not in applied])
    venue, _ = observed(account)
    if held.positions != venue:
        raise DataError(
            "inconsistent",
            "HyperCore fills do not account for the venue position",
            details={"expected": str(held.positions), "observed": str(venue)},
        )
    return held


def bounded(owned: dict[str, Decimal], positions: dict[str, Decimal]) -> dict[str, Decimal]:
    """Owned exposure within the venue position: same side, never larger."""
    kept = {coin: within(size, positions.get(coin, ZERO)) for coin, size in owned.items()}
    return {coin: size for coin, size in kept.items() if size != 0}


def within(owned: Decimal, venue: Decimal) -> Decimal:
    if owned == 0 or venue == 0 or (owned > 0) != (venue > 0):
        return ZERO
    return min(owned, venue, key=abs)


def require_balance(positions: dict, cash: Decimal, account: InfoResult, evidence) -> None:
    venue_positions, venue_cash = observed(account)
    if positions != venue_positions or cash != venue_cash:
        raise DataError(
            "inconsistent",
            "HyperCore effects do not account for the perp account's change",
            details={
                "expected": {"positions": str(positions), "cash": str(cash)},
                "observed": {"positions": str(venue_positions), "cash": str(venue_cash)},
                "evidence": evidence,
            },
        )


def require_unchanged(book: Book, account: InfoResult) -> None:
    """An account read no later than the cursor must match the book as it stands."""
    query = account.request.info
    if query.type != "clearinghouseState" or query.user != book.safe:
        raise ValueError("reconciliation reads the Safe's clearinghouseState")
    require_balance(book.positions, book.cash, account, account.model_dump(mode="json"))


def liquidations(book: Book) -> tuple[Effect, ...]:
    return tuple(e for e in book.effects if e.liquidation)
