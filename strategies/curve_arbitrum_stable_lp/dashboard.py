"""The first allocation's pool shares and its value against holding the starting coins, from the
readings the runtime keeps at each decision (`context.history`), and every allocation's coins and
shares."""

from decimal import Decimal

from almanak import dashboard as ui
from almanak.dashboard import build as b
from almanak.dashboard.models import ExchangePosition, LineStyle

VALUE_LINES: tuple[tuple[str, str, LineStyle], ...] = (
    ("value", "Strategy", "solid"),
    ("hold", "If held", "dashed"),
)
TABLES = (
    ("exchange_inventory", "Coins outside the pool"),
    ("pool_shares", "Pool shares"),
    ("payer_costs", "Gas paid"),
    ("transactions", "Transactions"),
)


def reading(latest, field):
    return Decimal(latest[field]) if field in latest else None


def worth(coin, id, amount, reason):
    """An amount in the first coin of the pool."""
    return b.token(
        id,
        amount,
        reason,
        asset=coin.asset,
        decimals=coin.decimals or 0,
        symbol=coin.symbol or "coin 0",
        scope=coin.scope,
        evidence="author_derived",
    )


def status(position, shares):
    if shares:
        return b.callout(
            "status",
            "Providing liquidity",
            f"The {position.allocation} allocation holds pool shares.",
            "success",
        )
    return b.callout(
        "status", "No open position", f"The {position.allocation} allocation holds no shares."
    )


def now_group(context, position, latest):
    coin = position.inventory[0]
    shares, value, hold = (reading(latest, f) for f in ("shares", "value", "hold"))
    gap = None if value is None or hold is None else value - hold
    return b.group(
        "now",
        f"Now: {position.allocation}",
        b.metric(
            "shares",
            "Pool shares",
            context.values.get(f"{position.allocation}-shares")
            or b.index("shares", shares, "Latest reading.", places=4),
        ),
        b.metric(
            "value", "Value", worth(coin, "value", value, "Wallet coins plus the shares' claim.")
        ),
        b.metric("hold", "If held", worth(coin, "hold", hold, "The starting coins.")),
        b.metric("vs_hold", "Value minus held", worth(coin, "vs_hold", gap, "Reading.")),
    )


def value_chart(context, coin):
    lines = [
        b.series(field, label, points, style=style)
        for field, label, style in VALUE_LINES
        if (
            points := [
                (t, worth(coin, f"{field}{i}", v, "Reading."))
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
            caption="Wallet coins plus the shares' claim against the starting coins, "
            f"in {coin.symbol or 'coin 0'}.",
        )
    ]


def tables(context):
    return [
        ui.Table(id=id, title=title, data=context.datasets[id])
        for id, title in TABLES
        if id in context.datasets
    ]


def dashboard(context: ui.Context) -> ui.Dashboard:
    position = next(p for p in context.positions if isinstance(p, ExchangePosition))
    coin = position.inventory[0]
    unit = coin.symbol or "coin 0"
    latest = context.history[-1].fields if context.history else {}
    blocks = [
        status(position, reading(latest, "shares")),
        now_group(context, position, latest),
        *value_chart(context, coin),
        *tables(context),
        ui.Text(
            id="scope",
            text=f"Values are in {unit}, converting coins at the pool's peg rates, not a market "
            "price or USD. Fees stay inside the share value; gas is shown separately.",
        ),
    ]
    return b.dashboard("Curve stable liquidity", *blocks, description="Shares and value.")
