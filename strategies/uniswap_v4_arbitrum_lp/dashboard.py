"""The pool price against the owned range, the position's uncollected fees, and its value against
holding the starting tokens, from the readings the runtime keeps at each decision
(`context.history`)."""

from decimal import Decimal
from typing import NamedTuple

from almanak import dashboard as ui
from almanak.dashboard import build as b
from almanak.dashboard.models import ExchangePosition, LineStyle, Value

SOURCE = "Configured Uniswap V4 pool, read at each decision"
# Edges this far apart are the full tick range: the position earns fees at any price.
FULL_RANGE = Decimal(10) ** 6

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


class Range(NamedTuple):
    """The latest price and the owned edges; `full` when the edges span every tick."""

    now: Decimal | None
    lower: Decimal | None
    upper: Decimal | None
    opened: bool
    full: bool


def dashboard(context: ui.Context) -> ui.Dashboard:
    tokens = pair(next(p for p in context.positions if isinstance(p, ExchangePosition)))
    latest = context.history[-1].fields if context.history else {}
    owned = owned_range(latest)
    blocks = [
        status(owned),
        now_group(tokens, latest, owned),
        *price_chart(context, tokens, owned),
        *value_chart(context, tokens),
        *tables(context),
        ui.Text(
            id="scope",
            text=f"Values are in {tokens.quote} at the pool price, not USD. Value counts "
            "uncollected fees; gas is shown separately.",
        ),
    ]
    return b.dashboard(
        f"{tokens.base}/{tokens.quote} liquidity",
        *blocks,
        description="Range, fees and value against holding.",
    )


def pair(position: ExchangePosition) -> Pair:
    token0, token1 = position.inventory
    return Pair(token0, token1, token0.symbol or "currency0", token1.symbol or "currency1")


def reading(latest, field):
    return Decimal(latest[field]) if field in latest else None


def owned_range(latest) -> Range:
    now, lower, upper = (reading(latest, f) for f in ("price", "range_lower", "range_upper"))
    opened = lower is not None and upper is not None and now is not None
    full = opened and (not lower or upper / lower > FULL_RANGE)
    return Range(now, lower, upper, opened, full)


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


def status(owned: Range):
    now, lower, upper = owned.now, owned.lower, owned.upper
    if now is None:
        return b.callout(
            "range", "Waiting for a reading", "The next decision reads the pool and its position."
        )
    # A reading without a range is one taken with no position open.
    if lower is None or upper is None:
        return b.callout("range", "No open position", "The strategy holds no liquidity now.")
    if owned.full:
        return b.callout("range", "Full range", "The position earns fees at any price.", "success")
    if lower <= now < upper:
        return b.callout("range", "In range", "The position earns fees at this price.", "success")
    return b.callout(
        "range", "Out of range", "The position holds one token and earns no fees.", "warning"
    )


def fees(latest, now):
    """Both tokens' uncollected fees in the quote token, at `now`."""
    fees0, fees1 = reading(latest, "fees0"), reading(latest, "fees1")
    return None if now is None or fees0 is None or fees1 is None else fees0 * now + fees1


def edges(tokens: Pair, owned: Range):
    if owned.full:
        return []
    return [
        b.metric(
            "lower", "Lower edge", price(tokens, "lower", owned.lower, "The position's lower tick.")
        ),
        b.metric(
            "upper", "Upper edge", price(tokens, "upper", owned.upper, "The position's upper tick.")
        ),
        b.metric(
            "to_lower",
            "Move to lower edge",
            b.percent("to_lower", distance(owned.now, owned.lower), "From now."),
        ),
        b.metric(
            "to_upper",
            "Move to upper edge",
            b.percent("to_upper", distance(owned.now, owned.upper), "From now."),
        ),
    ]


def now_group(tokens: Pair, latest, owned: Range):
    value, hold = reading(latest, "value"), reading(latest, "hold")
    gap = None if value is None or hold is None else value - hold
    return b.group(
        "now",
        "Now",
        b.metric("price", "Pool price", price(tokens, "price", owned.now, "Latest reading.")),
        *edges(tokens, owned),
        b.metric(
            "fees",
            "Uncollected fees",
            worth(tokens, "fees", fees(latest, owned.now), "Both tokens at this price."),
        ),
        b.metric("value", "Value", worth(tokens, "value", value, "Wallet, principal and fees.")),
        b.metric(
            "hold", "If held", worth(tokens, "hold", hold, "The starting tokens at this price.")
        ),
        b.metric("vs_hold", "Value minus held", worth(tokens, "vs_hold", gap, "Fees included.")),
    )


def price_chart(context, tokens: Pair, owned: Range):
    prices = [
        (t, price(tokens, f"p{i}", v, "Reading."))
        for i, (t, v) in enumerate(b.history(context, "price"))
    ]
    if not prices:
        return []
    bands = (
        [
            b.band(
                "Owned range",
                low=price(tokens, "band_low", owned.lower, "Lower tick."),
                high=price(tokens, "band_high", owned.upper, "Upper tick."),
                tone="success",
            )
        ]
        if owned.opened and not owned.full
        else []
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
            caption="Wallet, principal and uncollected fees against the starting tokens "
            f"held, in {tokens.quote}.",
        )
    ]


def tables(context):
    return [
        ui.Table(id=id, title=title, data=context.datasets[id])
        for id, title in (
            ("exchange_inventory", "Tokens outside the position"),
            ("payer_costs", "Gas paid"),
            ("transactions", "Transactions"),
        )
        if id in context.datasets
    ]
