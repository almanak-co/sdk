"""The GMX position's size, collateral, average entry and claimable funding, from the readings the
runtime keeps at each decision (`context.history`). Mark price, PnL, leverage and liquidation
price are not observed; PnL is shown only as an approximation from a declared price series."""

from decimal import Decimal

from almanak import dashboard as ui
from almanak.connectors.gmx_v2.models import Parameters
from almanak.dashboard import build as b
from almanak.dashboard.models import ExchangePosition

SOURCE = "GMX position read at each decision"
LABELS = {"increase_position": "Increase", "decrease_position": "Decrease"}
LABELS |= {"close_position": "Close"}
TABLES = (
    ("positions", "Position"),
    ("requests", "GMX requests"),
    ("submitters", "Gas paid"),
    ("transactions", "Transactions"),
)


def symbol(metadata, asset):
    found = metadata.get(asset)
    return (found.symbol if found else None) or "token"


def reading(latest, field):
    return Decimal(latest[field]) if field in latest else None


def token(metadata, position, id, asset, amount, reason):
    """An amount of `asset`, scoped like the fund's holding of it."""
    held = {str(v.asset): v for v in position.inventory}.get(str(asset), position.inventory[0])
    found = metadata.get(asset)
    return b.token(
        id,
        amount,
        reason,
        asset=asset,
        decimals=found.decimals if found else held.decimals or 0,
        symbol=symbol(metadata, asset),
        scope=held.scope,
        evidence="author_derived",
    )


def usd(id, amount, reason, source=SOURCE):
    return b.usd(id, amount, reason, price_source=source, evidence="author_derived")


def status(side, size, index_symbol):
    if size is None:
        return b.callout("state", "Not read yet", "No decision has read the position yet.")
    if size:
        return b.callout(
            "state",
            f"{side.title()} open",
            f"A {side} on {index_symbol}, holding until the exit condition.",
            "success",
        )
    return b.callout("state", "No open position", "The fund holds no GMX position now.")


def declared_prices(context, index):
    """The declared series for the index token, else the first one declared."""
    return next(
        (row for row in context.prices if str(row.asset) == str(index)),
        context.prices[0] if context.prices else None,
    )


def approximate_pnl(side, tokens, entry, mark):
    if mark is None or not tokens or entry is None:
        return None
    return tokens * (mark - entry) * (1 if side == "long" else -1)


def pnl_metric(approx, pair):
    reason = (
        f"Approximate: index tokens times the move from entry, at the strategy's "
        f"{pair} close; excludes fees, funding, borrowing and price impact."
        if approx is not None
        else "Needs an open position and a declared price series for the index token."
    )
    return b.metric(
        "pnl",
        "Approximate PnL",
        usd("pnl", approx, reason, source=f"Strategy price series ({pair})"),
    )


def funding_metric(metadata, position, latest, leg, asset):
    id = f"funding_{leg}"
    return b.metric(
        id,
        f"Funding in {symbol(metadata, asset)}",
        token(
            metadata,
            position,
            id,
            asset,
            reading(latest, id),
            f"Claimable funding in the {leg} asset.",
        ),
    )


def now_group(metadata, position, allocation, latest, approx, pair):
    market = allocation.market
    return b.group(
        "now",
        "Now",
        b.metric("size", "Size", usd("size", reading(latest, "size_usd"), "Protocol size in USD.")),
        b.metric(
            "collateral",
            "Collateral",
            token(
                metadata,
                position,
                "collateral",
                allocation.capital.asset,
                reading(latest, "collateral"),
                "Posted collateral.",
            ),
        ),
        b.metric(
            "entry",
            "Average entry",
            usd("entry", reading(latest, "entry_price"), "Size in USD over size in index tokens."),
        ),
        funding_metric(metadata, position, latest, "long", market.long_asset),
        funding_metric(metadata, position, latest, "short", market.short_asset),
        pnl_metric(approx, pair),
    )


def size_chart(context):
    sizes = [
        (t, usd(f"s{i}", v, "Reading.")) for i, (t, v) in enumerate(b.history(context, "size_usd"))
    ]
    if not sizes:
        return []
    return [
        b.line_chart(
            "size_chart",
            "Position size",
            [b.series("size", "Size, USD", sizes)],
            caption="Protocol size at each decision; zero when no position is open.",
        )
    ]


def collateral_chart(context, metadata, position, asset):
    posted = [
        (t, token(metadata, position, f"c{i}", asset, v, "Reading."))
        for i, (t, v) in enumerate(b.history(context, "collateral"))
    ]
    if not posted:
        return []
    return [
        b.line_chart(
            "collateral_chart",
            "Collateral",
            [b.series("collateral", f"Collateral, {symbol(metadata, asset)}", posted)],
            caption="Collateral posted to the position at each decision.",
        )
    ]


def price(declared, pair, market, id, amount, reason):
    return b.price(
        id,
        amount,
        reason,
        price_source="The strategy's declared price series",
        symbol=pair[:40],
        quote=declared.quote or market.short_asset,
        asset=declared.asset,
        evidence="author_derived",
    )


def price_chart(context, metadata, position, market, declared, closes, pair, entry):
    if declared is None or not closes:
        return []
    markers = [
        b.marker(t, LABELS.get(action, action.replace("_", " ")[:24]), series="price")
        for t, action, _ in b.trades(context, position.allocation)
        if closes[0][0] <= t
    ]
    references = (
        [
            b.reference(
                "Entry, USD",
                price(declared, pair, market, "entry_ref", entry, "Average entry in USD."),
            )
        ]
        if entry is not None
        else []
    )
    line = [
        (t, price(declared, pair, market, f"p{i}", v, "Close.")) for i, (t, v) in enumerate(closes)
    ]
    return [
        b.line_chart(
            "price_chart",
            "Index price and trades",
            [b.series("price", symbol(metadata, market.index_asset), line)],
            references=references,
            markers=markers,
            caption="The closes the strategy read, its average entry and its transactions.",
        )
    ]


def tables(context):
    return [
        ui.Table(id=key, title=title, data=context.datasets[key])
        for key, title in TABLES
        if key in context.datasets
    ]


def dashboard(context: ui.Context) -> ui.Dashboard:
    config = context.config
    if not isinstance(config, Parameters):
        raise TypeError("this dashboard reads a GMX V2 configuration")
    position = next(p for p in context.positions if isinstance(p, ExchangePosition))
    allocation = config.allocation(position.allocation)
    market, side = allocation.market, allocation.side
    metadata = config.asset_metadata
    latest = context.history[-1].fields if context.history else {}
    index = market.index_asset
    declared = declared_prices(context, index)
    closes = b.prices(context, declared.asset) if declared else []
    pair = (
        declared.label if declared else None
    ) or f"{symbol(metadata, market.short_asset)} per {symbol(metadata, index)}"
    entry = reading(latest, "entry_price")
    approx = approximate_pnl(
        side,
        reading(latest, "size_tokens"),
        entry,
        closes[-1][1] if closes else None,
    )
    blocks = [
        status(side, reading(latest, "size_usd"), symbol(metadata, index)),
        now_group(metadata, position, allocation, latest, approx, pair),
        *size_chart(context),
        *collateral_chart(context, metadata, position, allocation.capital.asset),
        *price_chart(context, metadata, position, market, declared, closes, pair, entry),
        *tables(context),
        ui.Text(
            id="scope",
            text="GMX does not report a mark price, PnL, leverage or liquidation price here; "
            "size, collateral, entry and funding are read from the protocol.",
        ),
    ]
    return b.dashboard(
        f"GMX {symbol(metadata, index)} {side}",
        *blocks,
        description="Size, collateral, entry and funding.",
    )
