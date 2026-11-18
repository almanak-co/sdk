"""The pool price against the owned range, and the position's value against holding the starting
tokens, from the readings the runtime keeps at each decision (`context.history`)."""

from decimal import Decimal
from typing import NamedTuple

from almanak import dashboard as ui
from almanak.dashboard import build as b
from almanak.dashboard.models import ExchangePosition, LineStyle, Value

SOURCE = "Configured Uniswap V3 pool, read at each decision"

VALUE_LINES: tuple[tuple[str, str, LineStyle], ...] = (
    ("value", "Strategy", "solid"),
    ("hold", "If held", "dashed"),
)


class Pair(NamedTuple):
    """The position's two tokens: prices are in `quote` per `base`, values in `quote`."""

    token0: Value
    token1: Value
    base: str
    quote: str


def dashboard(context: ui.Context) -> ui.Dashboard:
    position = next(
        p
        for p in context.positions
        if isinstance(p, ExchangePosition) and p.allocation == "position"
    )
    tokens = pair(position)
    latest = context.history[-1].fields if context.history else {}
    now, lower, upper = (reading(latest, f) for f in ("price", "range_lower", "range_upper"))
    blocks = [
        status(now, lower, upper),
        now_group(tokens, latest),
        # The first decision has no reading yet, and a chart needs at least one point.
        *price_chart(context, tokens, lower, upper),
        *value_chart(context, tokens),
        *(
            ui.Table(id=id, title=title, data=context.datasets[key])
            for id, key, title in (
                ("inventory", "exchange_inventory", "Tokens outside the position"),
                ("gas", "payer_costs", "Gas paid"),
                ("transactions", "transactions", "Transactions"),
            )
            if key in context.datasets
        ),
        ui.Text(
            id="scope",
            text=f"Values are in {tokens.quote} at the pool price, not USD. Uncollected fees are "
            "not measured, so value against holding excludes them; gas is shown separately.",
        ),
    ]
    return b.dashboard(
        f"{tokens.base}/{tokens.quote} liquidity",
        *blocks,
        description="Range and value against holding.",
    )


def pair(position: ExchangePosition) -> Pair:
    token0, token1 = position.inventory
    return Pair(token0, token1, token0.symbol or "currency0", token1.symbol or "currency1")


def reading(latest, field):
    return Decimal(latest[field]) if field in latest else None


def price(tokens: Pair, id, amount, reason):
    return b.price(
        id,
        amount,
        reason,
        price_source=SOURCE,
        symbol=f"{tokens.quote} per {tokens.base}",
        quote=tokens.token1.asset,
        asset=tokens.token0.asset,
        evidence="author_derived",
    )


def worth(tokens: Pair, id, amount, reason):
    return b.token(
        id,
        amount,
        reason,
        asset=tokens.token1.asset,
        decimals=tokens.token1.decimals or 0,
        symbol=tokens.quote,
        scope=tokens.token1.scope,
        evidence="author_derived",
    )


def distance(now, edge):
    """The move from `now` to `edge`, in percent."""
    return None if now is None or edge is None else (edge - now) / now * 100


def status(now, lower, upper):
    if now is None:
        return b.callout(
            "range", "Waiting for a reading", "The next decision reads the pool and its position."
        )
    # A reading without a range is one taken with no position open.
    if lower is None or upper is None:
        return b.callout("range", "No open position", "The strategy holds no liquidity now.")
    if lower <= now < upper:
        return b.callout("range", "In range", "The position earns fees at this price.", "success")
    return b.callout(
        "range", "Out of range", "The position holds one token and earns no fees.", "warning"
    )


def now_group(tokens: Pair, latest):
    now, lower, upper = (reading(latest, f) for f in ("price", "range_lower", "range_upper"))
    value, hold = reading(latest, "value"), reading(latest, "hold")
    gap = None if value is None or hold is None else value - hold
    return b.group(
        "now",
        "Now",
        b.metric("price", "Pool price", price(tokens, "price", now, "Latest reading.")),
        b.metric(
            "lower", "Lower edge", price(tokens, "lower", lower, "The position's lower tick.")
        ),
        b.metric(
            "upper", "Upper edge", price(tokens, "upper", upper, "The position's upper tick.")
        ),
        b.metric(
            "to_lower",
            "Move to lower edge",
            b.percent("to_lower", distance(now, lower), "From the price."),
        ),
        b.metric(
            "to_upper",
            "Move to upper edge",
            b.percent("to_upper", distance(now, upper), "From the price."),
        ),
        b.metric(
            "value",
            "Value",
            worth(tokens, "value", value, "Wallet plus the position's principal."),
        ),
        b.metric(
            "hold", "If held", worth(tokens, "hold", hold, "The starting tokens at this price.")
        ),
        b.metric("vs_hold", "Value minus held", worth(tokens, "vs_hold", gap, "Fees excluded.")),
    )


def price_chart(context, tokens: Pair, lower, upper):
    prices = [
        (t, price(tokens, f"p{i}", v, "Reading."))
        for i, (t, v) in enumerate(b.history(context, "price"))
    ]
    if not prices:
        return []
    bands = (
        []
        if lower is None or upper is None
        else [
            b.band(
                "Owned range",
                low=price(tokens, "band_low", lower, "Lower tick."),
                high=price(tokens, "band_high", upper, "Upper tick."),
                tone="success",
            )
        ]
    )
    return [
        b.line_chart(
            "price_chart",
            "Price against the owned range",
            [b.series("price", "Pool price", prices)],
            bands=bands,
            caption="The pool price at each decision; the shaded band is the owned range.",
        )
    ]


def value_chart(context, tokens: Pair):
    lines = [
        b.series(field, label, points, style=style)
        for field, label, style in VALUE_LINES
        if (
            points := [
                (t, worth(tokens, f"{field}{i}", v, "Reading."))
                for i, (t, v) in enumerate(b.history(context, field))
            ]
        )
    ]
    if not lines:
        return []
    return [
        b.line_chart(
            "value_chart",
            "Value against holding",
            lines,
            caption=f"Wallet plus position against the starting tokens held, in {tokens.quote}.",
        )
    ]
