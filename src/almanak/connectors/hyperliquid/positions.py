"""What the strategy owns of one Safe's HyperEVM wallet and HyperCore account.

The HyperCore side's full record lives in the checkpoint's documents: custody requests
and agents (`custody.Custody`), trading attempts (`trading.Trading`) and the reconciled
perp account (`effects.Book`). Ownership repeats the figures a strategy decides on, each
projected from those documents (`projected`), so they change only when a verified
outcome changes a document (`lifecycle.py`). No HyperCore answer reaches a snapshot,
because `/info` answers at read time and a decision's observations must be re-provable
at their block (section 13.1).

A strategy sees the figures a backtest's account shows (`economic_state`), with the same
names, types and meaning: whether the account is `activated`, the cross `leverage` the
strategy set, the `holding` its own fills hold, and the USDC a withdrawal could return.
Where the documents hold less than the backtest's model, blueprint 30, section 13.6 says
what the projection uses instead.
"""

from decimal import ROUND_DOWN, Decimal
from itertools import groupby
from typing import Annotated

from pydantic import Field, model_validator

from almanak.backtest.types import UnixTime
from almanak.connectors.hyperliquid.effects import orders
from almanak.connectors.hyperliquid.models import Target
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_shapes import DecimalText
from almanak.models import AssetAmount, Model, UInt

ZERO = Decimal(0)
USDC = Decimal("0.000001")
# The ledger entries of the custody path that move spot USDC; the book applies them as
# effects that move no perp USDC (`effects.SPOT_ONLY`), and class transfers as both.
SPOT_TRANSFERS = ("spotTransfer", "send")
# A withdrawal leaves at least a tenth of an open position's notional (section 9.4).
NOTIONAL_SHARE_KEPT = Decimal(10)


class Holding(Model):
    """The account's position in its market: signed coins at an average entry price."""

    szi: DecimalText
    entry_px: DecimalText
    # When the position last changed; candles that opened before it cannot liquidate it.
    changed_at: UnixTime


class Ownership(Model):
    target: Target
    # The Safe's HyperEVM USDC the strategy has yet to deposit, or has withdrawn back.
    inventory: tuple[AssetAmount, ...]
    # The signed size of the market's position the strategy's own fills hold, in coin
    # units: the book's `owned`, never the Safe's outside exposure.
    position: Decimal = Field(default=ZERO, allow_inf_nan=False)
    # USDC on the Safe's HyperCore account, spot and perp, as its reconciled ledger and
    # fills record it (`held_usdc`): what a withdrawal still has to return.
    hypercore: Decimal = Field(default=ZERO, allow_inf_nan=False)
    # HyperCore holds an account for the Safe once a deposit paid its activation fee: a
    # deposit request its own credit resolved.
    activated: bool = False
    # The cross leverage the strategy's last applied `updateLeverage` set for the market;
    # None until one applies.
    leverage: Annotated[int, Field(strict=True, ge=1)] | None = None
    # The position the strategy's own fills hold, at their average entry price.
    holding: Holding | None = None
    # USDC base units a withdrawal of everything could return (`withdrawable`).
    withdrawable: UInt = 0

    @model_validator(mode="after")
    def inventory_scope(self):
        if tuple(value.asset for value in self.inventory) != (self.target.margin_asset,):
            raise ValueError("Hyperliquid owned inventory is exactly its target's USDC")
        return self

    @property
    def holds_usdc(self) -> bool:
        return self.hypercore > 0


def initial_owned(config, name):
    allocation = config.allocation(name)
    return Ownership(target=config.target(name), inventory=(allocation.capital,))


def projected(custody, trading, book, coin: str) -> dict:
    """Every HyperCore figure of `Ownership`, from the checkpoint's documents alone."""
    held = holding(book, coin)
    set_leverage = leverage(trading, coin)
    return {
        "position": book.owned.get(coin, ZERO),
        "hypercore": held_usdc(book),
        "activated": activated(custody),
        "leverage": set_leverage,
        "holding": held,
        "withdrawable": withdrawable(book, held, set_leverage),
    }


def activated(custody) -> bool:
    """A deposit request resolved by its own credit, which paid the activation fee (A7):
    the Safe is dedicated, so its first deposit is the account's first credit."""
    return any(r.kind == "deposit" and r.status == "resolved" for r in custody.requests)


def leverage(trading, coin: str) -> int | None:
    """The cross leverage of the market's last applied `updateLeverage`, or None."""
    for attempt in reversed(trading.attempts):
        if (attempt.purpose, attempt.coin, attempt.status) == ("leverage", coin, "applied"):
            action = attempt.request.action
            return action.leverage if action.is_cross else None
    return None


def holding(book, coin: str) -> Holding | None:
    """The book's owned position in `coin` at the average price of the strategy's own fills.

    The owned fills are replayed in the venue's execution order, owned exposure bounded by
    the venue position after each fill as the book bounds it (`effects.exposure`): a fill
    that adds to the owned side averages its price in, one that reduces it keeps the
    entry, and one that flips it starts at its own price, as `perp_math.trade` and the
    fork model price a position. The size is the book's own figure."""
    size = book.owned.get(coin, ZERO)
    if size == 0:
        return None
    entry, changed = replayed(e for e in book.effects if e.kind == "fill" and e.coin == coin)
    return Holding(szi=text(size), entry_px=text(entry), changed_at=UnixTime(changed // 1000))


def replayed(fills) -> tuple[Decimal, int]:
    """The owned exposure's average entry price after `fills`, and when it last changed.

    Each time's fills follow every complete order that chains by `startPosition`, as the
    book's proof does (`effects.orders`): orders that leave different prices are refused
    as ambiguous, as the book refuses different owned exposure, and none chaining as
    inconsistent; no fill is ever taken out of its chain."""
    replay = (ZERO, ZERO, ZERO, 0)
    for _, group in groupby(sorted(fills, key=time), key=time):
        outcomes = {applied(order, replay) for order in orders(list(group), replay[0])}
        if len(outcomes) != 1:
            raise unpriced(outcomes, replay[0])
        (replay,) = outcomes
    return replay[2], replay[3]


def applied(order, replay: tuple) -> tuple:
    """The venue position, owned exposure, entry price and change time after one order."""
    venue, owned, entry, changed = replay
    for fill in order:
        after = venue + fill.size
        mine = bounded(owned + (fill.size if fill.owned else ZERO), after)
        if mine != owned:
            entry, changed = averaged(owned, entry, mine, Decimal(fill.entry["px"])), fill.time
        venue, owned = after, mine
    return venue, owned, entry, changed


def unpriced(outcomes: set, position: Decimal) -> DataError:
    if not outcomes:
        return DataError(
            "inconsistent",
            "HyperCore fills do not chain by startPosition from the replayed position",
            details={"position": str(position)},
        )
    return DataError(
        "ambiguous",
        "HyperCore fills of one time chain in orders that leave different holding prices",
        details={"prices": sorted(str(o[2]) for o in outcomes)},
    )


def time(effect) -> int:
    return effect.time


def bounded(owned: Decimal, venue: Decimal) -> Decimal:
    if owned == 0 or venue == 0 or (owned > 0) != (venue > 0):
        return ZERO
    return min(owned, venue, key=abs)


def averaged(start: Decimal, entry: Decimal, after: Decimal, price: Decimal) -> Decimal:
    if after == 0:
        return ZERO
    if start == 0 or (start > 0) != (after > 0):
        return price
    if abs(after) <= abs(start):
        return entry
    return (abs(start) * entry + (abs(after) - abs(start)) * price) / abs(after)


def withdrawable(book, held: Holding | None, set_leverage: int | None) -> int:
    """USDC base units a withdrawal of everything could return, by the backtest model's
    rule (`perp_math.withdrawable`): perp USDC less the larger of initial margin and a
    tenth of notional, plus the spot USDC a withdrawal sends as well.

    The documents hold no mark price, so an open position is valued at its entry price:
    its unrealized PnL counts as zero. While flat the figure is exact."""
    perp = book.cash
    if held is not None:
        notional = abs(Decimal(held.szi)) * Decimal(held.entry_px)
        initial = notional / set_leverage if set_leverage else ZERO
        perp -= max(initial, notional / NOTIONAL_SHARE_KEPT)
    total = max(ZERO, perp) + held_usdc(book) - book.cash
    return int((max(ZERO, total) / USDC).to_integral_value(ROUND_DOWN))


def text(value: Decimal) -> str:
    """A decimal as the venue writes it: plain digits, no exponent, no trailing zeros."""
    written = format(value.normalize(), "f")
    return "0" if written in ("-0", "") else written


def held_usdc(book) -> Decimal:
    """The Safe's HyperCore USDC by its reconciled book: perp cash (account value less
    unrealized PnL, `Book.cash`) and spot USDC from the ledger entries the book applied.

    A credit's USDC fee comes out of it, as the activation fee of the first deposit does
    (canary T7); its `accountActivationGas` entry repeats that fee and moves nothing more.
    """
    spot = sum(
        (spot_change(e.entry["delta"], book.safe) for e in book.effects if e.kind == "ledger"),
        ZERO,
    )
    return book.cash + spot


def spot_change(delta: dict, safe: str) -> Decimal:
    kind = delta.get("type")
    if kind == "accountClassTransfer":
        usdc = Decimal(str(delta["usdc"]))
        return -usdc if delta["toPerp"] is True else usdc
    if kind not in SPOT_TRANSFERS or delta.get("token") != "USDC":
        return ZERO
    amount = Decimal(str(delta["amount"]))
    fee = Decimal(str(delta["fee"])) if delta.get("feeToken") == "USDC" else ZERO
    if delta.get("destination") == safe:
        return amount - fee
    if delta.get("user") == safe:
        return -(amount + fee)
    return ZERO
