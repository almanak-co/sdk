"""Pool-price valuation of V3 liquidity runs in their funding asset.

A run whose allocations share one funding asset is valued in that asset. The other
pool asset is priced by the prepared pool sample in force at each mark, so no price
outside the run's own dataset is read. Balances of assets outside the configured
pools (gas) stay in asset units and are named, never priced.

The valued curve explains its own result: fees credited at the price they were
collected at, the cost of each swap against the pool price it executed on, and the
remaining price effect on the inventory the run held.
"""

from dataclasses import dataclass, field
from decimal import Context, Decimal, localcontext
from fractions import Fraction
from types import SimpleNamespace
from typing import Any

from almanak.backtest import valuation
from almanak.backtest.dataset import InputUnavailable
from almanak.connectors.concentrated_math import amounts, sqrt_at_tick
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.v3_economic_simulation import fee_bounds, pending_fees
from almanak.connectors.v3_pool_planning import concrete_range, sqrt_bounds

# The curve keeps every action and at most this many evenly spaced marks.
MARKS = 480
# Quote values keep the funding asset's precision up to this many places.
VALUE_PLACES = 8
PRICE_DIGITS = 8
ADDITIONS = ("mint", "increase")
REMOVALS = ("decrease", "collect", "close")


def decimal(value: Fraction) -> Decimal:
    with localcontext() as context:
        context.prec = 60
        return Decimal(value.numerator) / Decimal(value.denominator)


def pool_rate(pool, quote) -> Fraction:
    """Whole quote units per whole unit of the pool's other asset, at the pool's price."""
    ratio = Fraction(pool.sqrt_price_x96**2, 2**192)
    ratio *= Fraction(10**pool.currency0.decimals, 10**pool.currency1.decimals)
    return ratio if quote == pool.currency1.identifier else 1 / ratio


def pair_value(pool, quote, units) -> Decimal:
    """Quote value of base-unit amounts of the pool's two assets, at the pool's price."""
    rate = pool_rate(pool, quote)
    total = Fraction(0)
    for currency, amount in zip((pool.currency0, pool.currency1), units, strict=True):
        whole = Fraction(amount, 10**currency.decimals)
        total += whole if currency.identifier == quote else whole * rate
    return decimal(total)


def position_claims(model, target, position, record):
    """Principal, credited pending fees and the growth bounds of one simulated position."""
    principal = amounts(
        position.liquidity,
        record.payload.pool.sqrt_price_x96,
        *sqrt_bounds(target, position=position),
    )
    selected = concrete_range(target, position=position)
    bounds = fee_bounds(record, target, selected, model.band_growth)
    return principal, pending_fees(position, bounds.lower), bounds


def funding_asset(config):
    """The one funding asset every allocation shares, or None when they differ."""
    assets = {target.funding_asset for target in targets(config)}
    return next(iter(assets)) if len(assets) == 1 else None


class PoolPrices:
    """Funding-asset prices from prepared pool samples; a missing sample is None."""

    def __init__(self, model, asset):
        self.book, self.asset = model.book, asset
        self.targets = targets(model.config)
        self.metadata = model.config.asset_metadata
        known = self.metadata.get(asset)
        self.quote: str | None = known.symbol if known and known.symbol else str(asset)
        self.priced = {a for target in self.targets for a in target.assets}

    def decimals(self, asset):
        known = self.metadata.get(asset)
        return None if known is None or asset not in self.priced else known.decimals

    def price(self, asset, at, market=None):
        if asset == self.asset:
            return Decimal(1)
        sources = (market,) if market is not None else self.targets
        pools = {target.pool: target for target in sources if asset in target.assets}
        # No pool, or two pools that could disagree, is no declared price.
        if len(pools) != 1:
            return None
        (target,) = pools.values()
        try:
            pool = self.book.at(target, at).payload.pool
        except ValueError:
            # No prepared coverage, or the newest sample is stale at this time.
            return None
        return decimal(pool_rate(pool, self.asset))


class LiquidityAccrual:
    """The principal and credited pending fees of one simulated V3 position."""

    def __init__(self, model):
        self.model = model

    def asset(self, position):
        return position.target.funding_asset

    def accrue(self, payload, at):
        position, target = payload.owned.position, payload.target
        if position is None:
            return ()
        try:
            record = self.model.book.at(target, at)
            principal, fees, _ = position_claims(self.model, target, position, record)
        except InputUnavailable:
            raise
        except ValueError as error:
            # No sample for this pool, or one that cannot measure this range: the
            # position stays unvalued at this mark.
            raise InputUnavailable(self.model.data.key(target), at, str(error)) from error
        pool = record.payload.pool
        return tuple(
            valuation.AccruedUnits(currency.identifier, currency.decimals, held + owed, target)
            for currency, held, owed in zip(
                (pool.currency0, pool.currency1), principal, fees, strict=True
            )
        )


def mark_times(state, seconds):
    start, last = int(state.schedule.start), int(state.time)
    times = {start, last, *(int(c.pending.eligible_at) for c in state.completed)}
    count = (last - start) // seconds + 1
    times.update(range(start, last + 1, seconds * max(1, -(-count // MARKS))))
    return tuple(sorted(time for time in times if start <= time <= last))


def money(value: Decimal, places: int) -> Decimal:
    with localcontext() as context:
        context.prec = 60
        return value.quantize(Decimal(1).scaleb(-places))


def equity_curve(state, model, prices, times):
    """Pool assets and simulated liquidity valued at each mark; other balances are left out."""
    accrual = LiquidityAccrual(model)
    places = min(prices.decimals(prices.asset) or 0, VALUE_PLACES)
    snapshots = valuation.portfolios_at(state, times)
    points = []
    for time in times:
        portfolio: Any = snapshots[time]
        scoped = SimpleNamespace(
            balances=tuple(b for b in portfolio.balances if b.key.asset in prices.priced),
            positions=portfolio.positions,
        )
        point = valuation.equity_point(scoped, time, prices, accrual)
        points.append(point._replace(value=money(point.value, places)))
    return points


def price_point(prices, at):
    values = {}
    for asset in sorted(prices.priced - {prices.asset}, key=str):
        price = prices.price(asset, at)
        if price is not None:
            values[str(asset)] = significant(price)
    return {"time": at, "prices": values}


def significant(value: Decimal) -> str:
    return format(Context(prec=PRICE_DIGITS).create_decimal(value), "f")


@dataclass
class Held:
    """One simulated position from its mint to its close, in asset units and quote value."""

    target: object
    identity: str
    opened_at: int
    selected: object
    deposited: list[int] = field(default_factory=lambda: [0, 0])
    withdrawn: list[int] = field(default_factory=lambda: [0, 0])
    fees: list[int] = field(default_factory=lambda: [0, 0])
    value_in: Decimal = Decimal(0)
    value_out: Decimal | None = None
    fees_value: Decimal = Decimal(0)
    closed_at: int | None = None
    price_at_close: str | None = None


class Ledger:
    """What the run's completed actions earned and cost, valued where each executed."""

    def __init__(self, quote):
        self.quote = quote
        self.open: dict[object, Held] = {}
        self.held: list[Held] = []
        self.swap_cost = self.swap_volume = Decimal(0)
        self.swaps = 0

    def record(self, result, at):
        kind, pool, target = result.steps[0], result.before.pool, result.intent.target
        deltas, fees = (result.delta0, result.delta1), (result.fees0, result.fees1)
        if kind == "swap":
            self.swaps += 1
            self.swap_cost -= pair_value(pool, self.quote, deltas)
            self.swap_volume -= pair_value(pool, self.quote, tuple(min(d, 0) for d in deltas))
        elif kind == "mint":
            position = result.after.position
            self.open[target] = Held(
                target, position.identity, at, concrete_range(target, position=position)
            )
            self.held.append(self.open[target])
        if kind in ADDITIONS and target in self.open:
            self.added(self.open[target], pool, deltas)
        if kind in REMOVALS and target in self.open:
            self.removed(self.open[target], pool, deltas, fees)
            if kind == "close":
                self.closed(self.open.pop(target), pool, at)

    def added(self, held, pool, deltas):
        for index, delta in enumerate(deltas):
            held.deposited[index] -= delta
        held.value_in -= pair_value(pool, self.quote, deltas)

    def removed(self, held, pool, deltas, fees):
        for index, (delta, fee) in enumerate(zip(deltas, fees, strict=True)):
            held.withdrawn[index] += delta - fee
            held.fees[index] += fee
        held.fees_value += pair_value(pool, self.quote, fees)
        held.value_out = (held.value_out or Decimal(0)) + pair_value(pool, self.quote, deltas)

    def closed(self, held, pool, at):
        """Where the price stood against the range at the close, in quote-price order."""
        held.closed_at = at
        sides = ("below_range", "above_range")
        if self.quote == held.target.assets[0]:
            sides = sides[::-1]
        held.price_at_close = (
            sides[0]
            if pool.tick < held.selected.lower
            else sides[1]
            if pool.tick >= held.selected.upper
            else "in_range"
        )

    def finish(self, state, model):
        """A position still open at the end is marked at the final sample, fees included."""
        payloads = {p.payload.target: p.payload for p in state.portfolio.positions}
        for target, held in self.open.items():
            position = payloads[target].owned.position
            try:
                record = model.book.at(target, int(state.time))
                principal, fees, _ = position_claims(model, target, position, record)
            except ValueError:
                continue
            pool = record.payload.pool
            for index, fee in enumerate(fees):
                held.fees[index] += fee
            held.fees_value += pair_value(pool, self.quote, fees)
            claim = tuple(p + f for p, f in zip(principal, fees, strict=True))
            held.value_out = (held.value_out or Decimal(0)) + pair_value(pool, self.quote, claim)


def in_range_seconds(book, held, end, origin, seconds):
    """Seconds the position was open while its prepared sample priced the pool inside its range.

    Samples sit `seconds` apart from `origin`; a moment without a usable sample is not counted.
    """
    total, at = 0, held.opened_at
    while at < end:
        following = min(end, origin + ((at - origin) // seconds + 1) * seconds)
        try:
            tick = book.at(held.target, at).payload.pool.tick
        except ValueError:
            tick = None
        if tick is not None and held.selected.lower <= tick < held.selected.upper:
            total += following - at
        at = following
    return total


def merged_seconds(intervals):
    total, reached = 0, None
    for start, end in sorted(intervals):
        start = start if reached is None else max(start, reached)
        if end > start:
            total += end - start
            reached = end
    return total


def range_prices(held, quote, decimals):
    """The range's two prices in quote units per whole unit of the other asset."""
    scale = Fraction(10 ** decimals[0], 10 ** decimals[1])
    edges = [
        Fraction(sqrt_at_tick(tick) ** 2, 2**192) * scale
        for tick in (held.selected.lower, held.selected.upper)
    ]
    if quote == held.target.assets[0]:
        edges = [1 / edge for edge in reversed(edges)]
    return [significant(decimal(edge)) for edge in edges]


def position_row(held, state, model, prices, places):
    target = held.target
    end = int(state.time) if held.closed_at is None else held.closed_at
    decimals = tuple(prices.decimals(asset) for asset in target.assets)
    return {
        "allocation": target.allocation,
        "identity": held.identity,
        "opened_at": held.opened_at,
        "closed_at": held.closed_at,
        "range": {"lower": held.selected.lower, "upper": held.selected.upper},
        "price_range": range_prices(held, prices.asset, decimals),
        "deposited": quantities(target, held.deposited),
        "withdrawn": quantities(target, held.withdrawn),
        "fees": quantities(target, held.fees),
        "deposited_value": text(held.value_in, places),
        "withdrawn_value": None if held.value_out is None else text(held.value_out, places),
        "fees_value": text(held.fees_value, places),
        "price_at_close": held.price_at_close,
        "held_seconds": end - held.opened_at,
        "in_range_seconds": in_range_seconds(
            model.book, held, end, int(state.schedule.start), model.settings.data.sample_seconds
        ),
    }


def quantities(target, values):
    return [
        {"asset": str(asset), "amount": str(value), "unit": "base_units"}
        for asset, value in zip(target.assets, values, strict=True)
    ]


def text(value: Decimal, places: int) -> str:
    return format(money(value, places), "f")


def benchmarks(prices, start, last):
    """Each other pool asset's price change, as holding it or half of it would return."""
    rows = []
    for asset in sorted(prices.priced - {prices.asset}, key=str):
        first, final = prices.price(asset, start), prices.price(asset, last)
        if first and final is not None:
            change = float((final - first) / first * 100)
            rows.append(
                {
                    "asset": str(asset),
                    "hold_return_pct": change,
                    "even_split_return_pct": change / 2,
                }
            )
    return rows


def performance(state, model, prices, points):
    ledger = Ledger(prices.asset)
    for completed in state.completed:
        result = completed.transition.result
        if result.success and result.steps:
            ledger.record(result, int(completed.pending.eligible_at))
    ledger.finish(state, model)
    start, last = int(state.schedule.start), int(state.time)
    places = min(prices.decimals(prices.asset) or 0, VALUE_PLACES)
    rows = [position_row(held, state, model, prices, places) for held in ledger.held]
    net = points[-1].value - points[0].value
    fees = sum((held.fees_value for held in ledger.held), Decimal(0))
    return {
        "initial_value": text(points[0].value, places),
        "final_value": text(points[-1].value, places),
        "net_value": text(net, places),
        "fees_value": text(fees, places),
        "swap_cost_value": text(ledger.swap_cost, places),
        "swap_volume_value": text(ledger.swap_volume, places),
        # What remains after fees and swap costs: the price change of the inventory held.
        "price_effect_value": text(net - fees + ledger.swap_cost, places),
        "swaps": ledger.swaps,
        "positions_opened": len(rows),
        "window_seconds": last - start,
        "time_in_market_seconds": merged_seconds(
            (row["opened_at"], row["opened_at"] + row["held_seconds"]) for row in rows
        ),
        "time_in_range_seconds": sum(row["in_range_seconds"] for row in rows),
        "benchmarks": benchmarks(prices, start, last),
        "positions": rows,
    }


def valuation_layer(state, model) -> dict:
    """Equity curve, prices, headline metrics and the fee, swap and price breakdown."""
    asset = funding_asset(model.config)
    if asset is None:
        return {}
    prices = PoolPrices(model, asset)
    if prices.decimals(asset) is None:
        return {}
    times = mark_times(state, model.settings.data.sample_seconds)
    points = equity_curve(state, model, prices, times)
    return {
        "valuation": "pool_price",
        "valuation_quote": prices.quote,
        "valuation_asset": str(asset),
        "valuation_excludes": sorted(
            {
                str(b.key.asset)
                for b in state.portfolio.balances
                if b.amount and b.key.asset not in prices.priced
            }
        ),
        "equity_curve": [valuation.curve_point(point) for point in points],
        "price_series": [price_point(prices, time) for time in times],
        "metrics": valuation.headline(points),
        "net_quote_return": valuation.net_return(points, prices.quote),
        "performance": performance(state, model, prices, points),
    }
