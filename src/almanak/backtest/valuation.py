"""Quote-currency equity valuation over a run's explicit valuation price bindings.

Token claims accrue exactly in base units; conversion into the declared quote
currency happens afterward, per point in time. A held asset without a valid
price at a point leaves that point incomplete and is listed by identity; a
price is never guessed and a run is never dropped for missing valuation data.
"""

from decimal import Decimal
from typing import NamedTuple, Protocol

from almanak.backtest import metrics
from almanak.backtest.dataset import DataKey, InputUnavailable
from almanak.caip import AssetType
from almanak.effects import Balance

DAY_SECONDS = 86400
# Uniform UTC sampling applies only to sparse curves over longer windows.
GRID_SAMPLE_LIMIT = 370
DENSE_GRID_MIN_POINTS = 25
DENSE_GRID_MIN_SPAN = 2 * DAY_SECONDS


class ValuedAmount(NamedTuple):
    asset: AssetType
    units: int
    value: Decimal


class AccruedUnits(NamedTuple):
    """One exact claim (positive) or obligation (negative) in asset base units.

    The optional market identifies the position's own price market, so a
    market-specific binding is not overridden by a generic one.
    """

    asset: AssetType
    decimals: int
    units: int
    market: DataKey | None = None


class PriceLookup(Protocol):
    """Connector-owned price source; None means no valid price, never a guess."""

    quote: str | None

    def price(self, asset: AssetType, at: int, market=None) -> Decimal | None: ...

    def decimals(self, asset: AssetType) -> int | None: ...


class PositionAccrual(Protocol):
    """Connector-owned position claims and obligations in exact base units."""

    def asset(self, position) -> AssetType: ...

    def accrue(self, position, at: int) -> tuple[AccruedUnits, ...]: ...


class EquityPoint(NamedTuple):
    time: int
    value: Decimal
    complete: bool
    unvalued: tuple[str, ...]


def value_wallets(
    balances: tuple[Balance, ...], at: int, prices: PriceLookup
) -> tuple[list[ValuedAmount], list[str]]:
    valued: list[ValuedAmount] = []
    unvalued: list[str] = []
    for balance in balances:
        if not balance.amount:
            continue
        asset = balance.key.asset
        decimals = prices.decimals(asset)
        price = prices.price(asset, at)
        if decimals is None or price is None:
            unvalued.append(str(asset))
            continue
        value = Decimal(int(balance.amount)) * price / 10**decimals
        valued.append(ValuedAmount(asset, int(balance.amount), value))
    return valued, unvalued


def value_positions(
    positions: tuple, at: int, accrual: PositionAccrual, prices: PriceLookup
) -> tuple[list[ValuedAmount], list[str]]:
    valued: list[ValuedAmount] = []
    unvalued: list[str] = []
    for position in positions:
        try:
            accrued = accrual.accrue(position.payload, at)
        except InputUnavailable:
            # Missing connector inputs at this time leave the position unvalued.
            unvalued.append(str(accrual.asset(position.payload)))
            continue
        for entry in accrued:
            if not entry.units:
                continue
            price = prices.price(entry.asset, at, entry.market)
            if price is None:
                unvalued.append(str(entry.asset))
                continue
            valued.append(
                ValuedAmount(
                    entry.asset, entry.units, Decimal(entry.units) * price / 10**entry.decimals
                )
            )
    return valued, unvalued


def equity_point(portfolio, at: int, prices: PriceLookup, accrual: PositionAccrual) -> EquityPoint:
    valued_wallets, missing_wallets = value_wallets(portfolio.balances, at, prices)
    valued_positions, missing_positions = value_positions(portfolio.positions, at, accrual, prices)
    total = sum((v.value for v in (*valued_wallets, *valued_positions)), Decimal(0))
    unvalued = tuple(sorted({*missing_wallets, *missing_positions}))
    return EquityPoint(at, total, not unvalued, unvalued)


def equity_curve(state, prices: PriceLookup, accrual: PositionAccrual) -> list[EquityPoint]:
    times = grid_times(state)
    snapshots = portfolios_at(state, times)
    return [equity_point(snapshots[time], time, prices, accrual) for time in times]


def grid_times(state) -> tuple[int, ...]:
    start, last = int(state.schedule.start), int(state.time)
    times = {start, last}
    times.update(int(c.pending.eligible_at) for c in state.completed)
    times.update(int(e.time) for e in state.events)
    if int(state.schedule.end) <= last:
        times.add(int(state.schedule.end))
    points = {time for time in times if start <= time <= last}
    if len(points) < DENSE_GRID_MIN_POINTS and last - start > DENSE_GRID_MIN_SPAN:
        points.update(daily_marks(start, last))
    return tuple(sorted(points))


def daily_marks(start: int, last: int) -> range:
    first = -(-start // DAY_SECONDS) * DAY_SECONDS
    days = -(-(last - first) // DAY_SECONDS) if last > first else 0
    step = max(1, -(-days // (GRID_SAMPLE_LIMIT - 1)))
    return range(first, last + 1, step * DAY_SECONDS)


def portfolios_at(state, times: tuple[int, ...]) -> dict[int, object]:
    """Reconstruct the portfolio at each grid time from recorded effects.

    Events apply before a completed step at the same timestamp, matching the
    simulator's advance-then-execute ordering.
    """
    items = [(int(e.time), 0, e.transition.effects) for e in state.events]
    items += [(int(c.pending.eligible_at), 1, c.transition.effects) for c in state.completed]
    items.sort(key=lambda item: (item[0], item[1]))
    initial = state.portfolio
    for _, _, effects in reversed(items):
        initial = unapply(initial, effects)
    result = {}
    portfolio = initial
    index = 0
    for time in times:
        while index < len(items) and items[index][0] <= time:
            portfolio = portfolio.apply(items[index][2])
            index += 1
        result[time] = portfolio
    return result


def unapply(portfolio, effects):
    balances = {b.key: b.amount for b in portfolio.balances}
    for delta in reversed(effects.assets):
        balances[delta.key] -= delta.amount
    positions = {p.key: p for p in portfolio.positions}
    for change in reversed(effects.positions):
        key = (change.after or change.before).key
        if change.before is None:
            del positions[key]
        else:
            positions[key] = change.before
    return type(portfolio)(
        balances=tuple(Balance(key=key, amount=amount) for key, amount in balances.items()),
        positions=tuple(positions.values()),
    )


def curve_point(point: EquityPoint) -> dict:
    result = {
        "time": point.time,
        "value": format(point.value, "f"),
        "complete": point.complete,
    }
    if point.unvalued:
        result["unvalued_assets"] = list(point.unvalued)
    return result


def headline(points: list[EquityPoint]) -> dict:
    curve = [(point.time, point.value) for point in points]
    return {
        "total_return_pct": metrics.total_return_pct(curve),
        "max_drawdown_pct": metrics.max_drawdown_pct(curve),
        "sharpe_ratio": metrics.sharpe_ratio(curve),
    }


def net_return(points: list[EquityPoint], quote: str | None) -> dict | None:
    if not points or quote is None:
        return None
    return {
        "quote": quote,
        "value": format(points[-1].value - points[0].value, "f"),
        "return_pct": metrics.total_return_pct([(p.time, p.value) for p in points]),
    }
