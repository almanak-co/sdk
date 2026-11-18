"""The Aave V3 loan and the Uniswap V3 position it funds. The liquidity readings come from the
runtime's history at each decision (`context.history`), keyed by allocation (`liquidity:price`,
`liquidity:range_lower`, ...); the lending side comes from the latest observed supply and debt."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from almanak import dashboard as ui
from almanak.configuration.composed import Parameters
from almanak.dashboard import build as b
from almanak.dashboard.models import ExchangePosition

SOURCE = "Configured Uniswap V3 pool, read at each decision"
LP = "liquidity:"


@dataclass(frozen=True)
class Pair:
    """The pool's two held tokens and their symbols."""

    base: ui.Value
    quote: ui.Value
    base_symbol: str
    quote_symbol: str


def dashboard(context: ui.Context) -> ui.Dashboard:
    config = context.config
    if not isinstance(config, Parameters):
        raise TypeError("this dashboard reads a composed configuration")
    pair = held_pair(context, config)
    latest = context.history[-1].fields if context.history else {}
    now, lower, upper = (reading(latest, f) for f in ("price", "range_lower", "range_upper"))
    memory = context.memory.values if context.memory else {}
    lending = loan_metrics(context, param(config, "unwind_health_factor"))
    blocks = [
        status(memory, now, lower, upper),
        *([b.group("lending", "Loan", *lending)] if lending else []),
    ]
    blocks.append(liquidity_group(pair, latest, now, lower, upper))
    blocks += charts(context, pair, lower, upper)
    blocks += tables(context)
    blocks.append(
        ui.Text(
            id="policy",
            text="The position is funded in the borrowed asset, so closing it and selling "
            "its other asset yields the asset the repay needs. With cover_shortfall, "
            "collateral covers any shortfall; otherwise the fund's own assets do. Position "
            f"value is in {pair.quote_symbol} at the pool price, not net of the debt.",
        )
    )
    return b.dashboard(
        "Aave V3 borrow and Uniswap V3 liquidity",
        *blocks,
        description="One fund supplies WETH, borrows USDC and provides it as banded liquidity.",
    )


def param(config, name):
    """A strategy parameter by name, from the plain mapping or the typed parameters.py model;
    None when it is not set."""
    values = config.parameters
    return values.get(name) if isinstance(values, Mapping) else getattr(values, name, None)


def held_pair(context, config):
    pool = config.allocation("liquidity").pool
    position = next(
        p
        for p in context.positions
        if isinstance(p, ExchangePosition) and p.allocation == "liquidity"
    )
    held = {str(v.asset): v for v in position.inventory}
    base, quote = held[str(pool.currency0)], held[str(pool.currency1)]
    metadata = config.asset_metadata
    return Pair(
        base, quote, symbol(metadata, base, "currency0"), symbol(metadata, quote, "currency1")
    )


def symbol(metadata, value, fallback):
    found = metadata.get(value.asset)
    return (found.symbol if found else None) or value.symbol or fallback


def reading(latest, field):
    return Decimal(latest[LP + field]) if LP + field in latest else None


def price(pair, id, amount, reason):
    return b.price(
        id,
        amount,
        reason,
        price_source=SOURCE,
        symbol=f"{pair.quote_symbol} per {pair.base_symbol}",
        quote=pair.quote.asset,
        asset=pair.base.asset,
        evidence="author_derived",
    )


def worth(pair, id, amount, reason):
    return b.token(
        id,
        amount,
        reason,
        asset=pair.quote.asset,
        decimals=pair.quote.decimals or 0,
        symbol=pair.quote_symbol,
        scope=pair.quote.scope,
        evidence="author_derived",
    )


def principal(fields):
    try:
        return Decimal(fields[LP + "position0"]) * Decimal(fields[LP + "price"]) + Decimal(
            fields[LP + "position1"]
        )
    except KeyError:
        return None


def status(memory, now, lower, upper):
    shortfall = str(memory.get("shortfall") or "")
    if shortfall:
        return b.callout("state", "Unwind stuck", shortfall, "warning")
    if memory.get("unwinding"):
        return b.callout("state", "Unwinding", "Closing liquidity, repaying, withdrawing.")
    if now is None:
        return b.callout(
            "state", "Waiting for a reading", "The next decision reads the pool and its position."
        )
    # A pool reading without a range is one taken with no liquidity open.
    if lower is None or upper is None:
        return b.callout("state", "No liquidity open", "The position is not open now.")
    if lower <= now < upper:
        return b.callout("state", "In range", "The borrowed cash earns pool fees.", "success")
    return b.callout(
        "state", "Out of range", "The position holds one token and earns no fees.", "warning"
    )


def loan_metrics(context, level):
    # Lending readings: once the Aave V3 connector keeps a history, its fields arrive here as
    # `collateral:<field>` and `loan:<field>` (the composed history prefixes each part's
    # readings with its allocation). Add their metrics and charts here; nothing is drawn
    # until those fields exist.
    values = context.values
    lending = [
        b.metric(key, label, values[key])
        for key, label in (("collateral-supply", "Collateral supplied"), ("loan-debt", "Debt"))
        if key in values
    ]
    if level is not None:
        lending.append(
            b.metric(
                "unwind_at",
                "Unwinds below health",
                b.ratio(
                    "unwind_at",
                    level,
                    "The configured unwind_health_factor.",
                    evidence="author_derived",
                ),
            )
        )
    return lending


def liquidity_group(pair, latest, now, lower, upper):
    return b.group(
        "liquidity",
        "Liquidity",
        b.metric("price", "Pool price", price(pair, "price", now, "Latest reading.")),
        b.metric("lower", "Lower edge", price(pair, "lower", lower, "The position's lower tick.")),
        b.metric("upper", "Upper edge", price(pair, "upper", upper, "The position's upper tick.")),
        b.metric(
            "lp_value",
            "Position value",
            worth(pair, "lp_value", principal(latest), "The position's principal at the price."),
        ),
    )


def owned_range(pair, lower, upper):
    if lower is None or upper is None:
        return []
    return [
        b.band(
            "Owned range",
            low=price(pair, "band_low", lower, "Lower tick."),
            high=price(pair, "band_high", upper, "Upper tick."),
            tone="success",
        )
    ]


def charts(context, pair, lower, upper):
    found = []
    prices = [
        (t, price(pair, f"p{i}", v, "Reading."))
        for i, (t, v) in enumerate(b.history(context, LP + "price"))
    ]
    if prices:
        found.append(
            b.line_chart(
                "price_chart",
                "Price against the owned range",
                [b.series("price", "Pool price", prices)],
                bands=owned_range(pair, lower, upper),
                caption="The pool price at each decision; the shaded band is the owned range.",
            )
        )
    principals = [
        (row.time, worth(pair, f"v{i}", amount, "Reading."))
        for i, row in enumerate(context.history)
        if (amount := principal(row.fields)) is not None
    ]
    if principals:
        found.append(
            b.line_chart(
                "value_chart",
                "Position value",
                [b.series("lp_value", "Position principal", principals)],
                caption=f"The position's principal at each decision, in {pair.quote_symbol}; "
                "uncollected fees and the debt are not included.",
            )
        )
    return found


def tables(context):
    return [
        ui.Table(id=key, title=title, data=context.datasets[key])
        for key, title in (
            ("repayment_coverage", "Repayment coverage"),
            ("fund_inventory", "Shared fund cash"),
            ("payer_costs", "Gas paid"),
            ("transactions", "Transactions"),
        )
        if key in context.datasets
    ]
