"""Aave V4 adapter for the quote-valuation layer: book prices and exact accrual.

Position claims use the same supply/borrow index math as the economic report's
final marked and debt values; vendor or oracle prices come from the run's
frozen valuation bindings. Missing inputs surface as unvalued assets, never as
invented prices.
"""

from decimal import Decimal

from almanak.backtest import valuation
from almanak.backtest.economic_data import reserve_key
from almanak.backtest.economic_math import debts
from almanak.backtest.pricing import CoinGeckoPrice


class BookPrices:
    """Valuation-purpose prices from the frozen book; missing data is None."""

    def __init__(self, book, bindings, metadata):
        self.book = book
        self.bindings = bindings
        self.units_decimals = {asset: m.decimals for asset, m in metadata.items()}
        for timeline in getattr(book, "risks", {}).values():
            reserve = timeline.rows[0].payload.reserve
            self.units_decimals.setdefault(reserve.asset.identifier, reserve.asset.decimals)
        quotes = {
            b.source.quote if isinstance(b.source, CoinGeckoPrice) else "USD" for b in bindings
        }
        if len(quotes) > 1:
            raise ValueError("valuation bindings require one shared quote currency")
        self.quote: str | None = quotes.pop() if quotes else None

    def decimals(self, asset):
        return self.units_decimals.get(asset)

    def price(self, asset, at, market=None):
        candidates = [b for b in self.bindings if b.asset == asset]
        exact = [b for b in candidates if b.market is not None and b.market == market]
        selected = exact or [b for b in candidates if b.market is None] or candidates
        # Zero matches or genuine ambiguity means no declared price, not an error.
        if len(selected) != 1:
            return None
        binding = selected[0]
        binding_timeline = getattr(self.book, "binding_timeline", None)
        if binding_timeline is None:
            return None
        try:
            row = binding_timeline(binding, at, at).at(at)
        except ValueError:
            # No prepared coverage, or the newest sample is stale at this time.
            return None
        if isinstance(binding.source, CoinGeckoPrice):
            return row.payload.value
        return Decimal(row.payload.price) / Decimal(10**8)


class AaveAccrual:
    """Exact share-index claims and debt obligations of one Aave position."""

    def __init__(self, book):
        self.book = book

    def asset(self, position):
        return position.target.asset

    def accrue(self, position, at):
        if not (
            position.shares or position.drawn_shares or position.premium_shares or position.deficit
        ):
            return ()
        market = self.book.at(position.target.reserve, at)
        asset = market.risk.reserve.asset
        claim = market.supplied(position.shares)
        drawn, premium = debts(position, market.borrow_index)
        debt = drawn + premium + position.deficit
        # The reserve's price key is the market identity valuation bindings use.
        price_market = reserve_key(position.target.reserve, "price")
        entries = []
        if claim:
            entries.append(
                valuation.AccruedUnits(asset.identifier, asset.decimals, claim, price_market)
            )
        if debt:
            entries.append(
                valuation.AccruedUnits(asset.identifier, asset.decimals, -debt, price_market)
            )
        return tuple(entries)


def valuation_layer(state, driver) -> dict:
    """Equity curve, price series and quote return, only for declared bindings."""
    bindings = tuple(b for b in driver.settings.data.prices if b.purpose == "valuation")
    if not bindings:
        return {}
    prices = BookPrices(driver.book, bindings, driver.config.asset_metadata)
    points = valuation.equity_curve(state, prices, AaveAccrual(driver.book))
    series = [price_point(prices, point.time) for point in points]
    if not any(point["prices"] for point in series):
        # The dataset holds no usable valuation observation; keep asset units only.
        return {}
    return {
        "valuation_quote": prices.quote,
        "equity_curve": [valuation.curve_point(point) for point in points],
        "price_series": series,
        "metrics": valuation.headline(points),
        "net_quote_return": valuation.net_return(points, prices.quote),
    }


def price_point(prices, at):
    values = {}
    # One entry per asset, priced exactly like the equity curve selects it.
    for asset in dict.fromkeys(b.asset for b in prices.bindings):
        price = prices.price(asset, at)
        if price is not None:
            values[str(asset)] = format(price, "f")
    return {"time": at, "prices": values}
