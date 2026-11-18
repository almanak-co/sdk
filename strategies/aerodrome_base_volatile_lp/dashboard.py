"""The pool price, the position's principal and claimable fees, and its value against holding the
starting tokens, from the readings the runtime keeps at each decision (`context.history`)."""

from decimal import Decimal

from almanak import dashboard as ui
from almanak.dashboard import build as b
from almanak.dashboard.models import ExchangePosition

SOURCE = "Configured Aerodrome pool reserves, read at each decision"
TABLES = (
    ("exchange_inventory", "Tokens outside the position"),
    ("pool_shares_and_fees", "Pool shares and fees"),
    ("payer_costs", "Gas paid"),
    ("transactions", "Transactions"),
)


def reading(latest, field):
    return Decimal(latest[field]) if field in latest else None


def price(id, amount, reason, token0, token1):
    return b.price(
        id,
        amount,
        reason,
        price_source=SOURCE,
        symbol=f"{token1.symbol or 'currency1'} per {token0.symbol or 'currency0'}",
        quote=token1.asset,
        asset=token0.asset,
        evidence="author_derived",
    )


def worth(id, amount, reason, token1):
    return b.token(
        id,
        amount,
        reason,
        asset=token1.asset,
        decimals=token1.decimals or 0,
        symbol=token1.symbol or "currency1",
        scope=token1.scope,
        evidence="author_derived",
    )


def marked(field, row):
    """Both tokens of a reading pair (`position0`/`position1`) in the quote token."""
    names = (f"{field}0", f"{field}1")
    if "price" not in row or any(name not in row for name in names):
        return None
    return Decimal(row[names[0]]) * Decimal(row["price"]) + Decimal(row[names[1]])


def status(latest):
    if not latest:
        return b.callout(
            "status", "Waiting for a reading", "The next decision reads the pool and its position."
        )
    if reading(latest, "shares"):
        return b.callout(
            "status",
            "Providing liquidity",
            "The fund holds pool shares and earns swap fees.",
            "success",
        )
    return b.callout("status", "No open position", "The strategy holds no pool shares now.")


def now_group(latest, token0, token1):
    now, value, hold = reading(latest, "price"), reading(latest, "value"), reading(latest, "hold")
    return b.group(
        "now",
        "Now",
        b.metric("price", "Pool price", price("price", now, "Latest reading.", token0, token1)),
        b.metric(
            "principal",
            "Principal",
            worth(
                "principal", marked("position", latest), "The shares' claim on the pool.", token1
            ),
        ),
        b.metric(
            "fees",
            "Claimable fees",
            worth("fees", marked("fees", latest), "Both tokens at this price.", token1),
        ),
        b.metric("value", "Value", worth("value", value, "Wallet, principal and fees.", token1)),
        b.metric(
            "hold", "If held", worth("hold", hold, "The starting tokens at this price.", token1)
        ),
        b.metric(
            "vs_hold",
            "Value minus held",
            worth(
                "vs_hold",
                None if value is None or hold is None else value - hold,
                "Fees included.",
                token1,
            ),
        ),
    )


def line(context, field, label, token1, style="solid"):
    points = [
        (t, worth(f"{field}{i}", v, "Reading.", token1))
        for i, (t, v) in enumerate(b.history(context, field))
    ]
    return b.series(field, label, points, style=style) if points else None


def price_points(context, token0, token1):
    return [
        (t, price(f"p{i}", v, "Reading.", token0, token1))
        for i, (t, v) in enumerate(b.history(context, "price"))
    ]


def fee_points(context, token1):
    return [
        (row.time, worth(f"f{i}", amount, "Reading.", token1))
        for i, row in enumerate(context.history)
        if (amount := marked("fees", row.fields)) is not None
    ]


def charts(context, token0, token1):
    quote = token1.symbol or "currency1"
    prices = price_points(context, token0, token1)
    fees = fee_points(context, token1)
    values = (
        line(context, "value", "Strategy", token1),
        line(context, "hold", "If held", token1, "dashed"),
    )
    drawn = [
        (
            "price_chart",
            "Pool price",
            [b.series("price", "Pool price", prices)] if prices else [],
            "The pool's reserve price at each decision.",
        ),
        (
            "value_chart",
            "Value against holding",
            [s for s in values if s],
            f"Wallet, principal and claimable fees against the starting tokens held, in {quote}.",
        ),
        (
            "fees_chart",
            "Claimable fees",
            [b.series("fees", "Claimable fees", fees)] if fees else [],
            f"Fees the position can claim at each decision, in {quote} at that price.",
        ),
    ]
    return [
        b.line_chart(id, title, lines, caption=text) for id, title, lines, text in drawn if lines
    ]


def tables(context):
    return [
        ui.Table(id=id, title=title, data=context.datasets[id])
        for id, title in TABLES
        if id in context.datasets
    ]


def dashboard(context: ui.Context) -> ui.Dashboard:
    token0, token1 = next(p for p in context.positions if isinstance(p, ExchangePosition)).inventory
    base, quote = token0.symbol or "currency0", token1.symbol or "currency1"
    latest = context.history[-1].fields if context.history else {}
    blocks = [status(latest), now_group(latest, token0, token1)]
    blocks += charts(context, token0, token1)
    blocks += tables(context)
    blocks.append(
        ui.Text(
            id="scope",
            text=f"Values are in {quote} at the pool's reserve price, not USD. Value counts "
            "claimable fees; gas is shown separately.",
        )
    )
    return b.dashboard(
        f"{base}/{quote} liquidity",
        *blocks,
        description="Principal, fees and value against holding.",
    )
