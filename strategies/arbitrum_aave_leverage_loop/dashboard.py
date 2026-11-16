"""Health, loan-to-value, debt against collateral and rates of the loop, from the readings the
runtime keeps at each decision (`context.history`)."""

from collections.abc import Mapping
from decimal import Decimal

from almanak import dashboard as ui
from almanak.configuration.composed import Parameters
from almanak.dashboard import build as b
from almanak.dashboard.models import CalloutTone

SOURCE = "Aave V3 oracle, read at each decision"
WAD = Decimal(10**18)


def dashboard(context: ui.Context) -> ui.Dashboard:
    config = context.config
    if not isinstance(config, Parameters):
        raise TypeError("this dashboard reads a composed configuration")
    latest = context.history[-1].fields if context.history else {}
    memory = context.memory.values
    floor = Decimal(config.allocation("loan").minimum_health_factor) / WAD
    trigger = Decimal(str(param(config, "unwind_health_factor")))
    rounds = memory.get("rounds", 0)
    if not isinstance(rounds, int):
        raise TypeError("memory.rounds must be a count")
    blocks = [status(memory, latest, trigger), now_group(latest, rounds)]
    blocks += charts(context, trigger, floor)
    tone: CalloutTone = "warning" if memory.get("capped_reason") else "info"
    blocks.append(b.callout("rounds_note", "Borrow rounds", rounds_text(memory), tone))
    blocks += tables(context)
    blocks += [
        ui.Text(
            id="scope",
            text="Values are in USD at the Aave oracle. Swap fees are inside value; gas is "
            "shown separately.",
        ),
    ]
    return b.dashboard(
        "Aave V3 leverage loop", *blocks, description="Health, leverage and rates of the loop."
    )


def param(config, name):
    """A strategy parameter by name, from the plain mapping or the typed parameters.py model."""
    values = config.parameters
    return values[name] if isinstance(values, Mapping) else getattr(values, name)


def reading(latest, field):
    return Decimal(latest[field]) if field in latest else None


def usd(id, amount, reason):
    return b.usd(id, amount, reason, price_source=SOURCE, evidence="author_derived")


def pct(id, amount, reason):
    percent = None if amount is None else amount * 100
    return b.value(id, percent, "percent", reason=reason, places=2)


def ratio(id, amount, reason):
    return b.ratio(id, amount, reason, evidence="author_derived")


def points(context, field, make):
    return [
        (t, make(f"{field}-{i}", v, "Reading."))
        for i, (t, v) in enumerate(b.history(context, field))
    ]


def worth(context, field, asset_field, make):
    """A token reading times its USD price, row by row."""
    found = []
    for i, row in enumerate(context.history):
        if field in row.fields and asset_field in row.fields:
            amount = Decimal(row.fields[field]) * Decimal(row.fields[asset_field])
            found.append((row.time, make(f"{field}-usd-{i}", amount, "Reading.")))
    return found


def priced(latest, field, price_field):
    amount, price = reading(latest, field), reading(latest, price_field)
    return None if amount is None or price is None else amount * price


def chart(id, title, lines, caption, **marks):
    drawn = [
        b.series(key, label, found, style=style) for key, label, found, style in lines if found
    ]
    return [b.line_chart(id, title, drawn, caption=caption, **marks)] if drawn else []


def status(memory, latest, trigger):
    health = reading(latest, "loan:health")
    if memory.get("shortfall"):
        return b.callout("status", "Unwind stuck", str(memory["shortfall"]), "danger")
    if memory.get("unwinding"):
        return b.callout(
            "status", "Unwinding", "Repaying debt and releasing collateral.", "warning"
        )
    if health is None:
        return without_health(latest)
    if health < trigger:
        return b.callout(
            "status",
            "Below the unwind level",
            f"Health {health:.2f} is under {trigger}.",
            "warning",
        )
    return b.callout(
        "status",
        "Healthy",
        f"Health {health:.2f}, above the {trigger} unwind level.",
        "success",
    )


def without_health(latest):
    if not latest:
        return b.callout("status", "Waiting for a reading", "The next decision reads the loop.")
    if reading(latest, "loan:debt"):
        return b.callout("status", "Health unavailable", "The loan owes debt; health not read.")
    return b.callout("status", "No debt", "Nothing is borrowed right now.")


def now_group(latest, rounds):
    health = reading(latest, "loan:health")
    collateral_usd = priced(latest, "collateral:supplied", "collateral:price_usd")
    debt_usd = priced(latest, "loan:debt", "loan:price_usd")
    return b.group(
        "now",
        "Now",
        b.metric(
            "health_now", "Health factor", ratio("health_now", health, "Liquidation below 1.")
        ),
        b.metric(
            "ltv_now",
            "Loan to value",
            pct("ltv_now", reading(latest, "loan:ltv"), "Debt over collateral."),
        ),
        b.metric(
            "max_ltv_now",
            "Max loan to value",
            pct("max_ltv_now", reading(latest, "loan:max_ltv"), "Aave's borrow limit."),
        ),
        b.metric(
            "collateral_now",
            "Collateral",
            usd("collateral_now", collateral_usd, "Supplied collateral."),
        ),
        b.metric("debt_now", "Debt", usd("debt_now", debt_usd, "Borrowed, interest included.")),
        b.metric(
            "value_now",
            "Net value",
            usd(
                "value_now", reading(latest, "lending_value"), "Wallet plus collateral minus debt."
            ),
        ),
        b.metric(
            "hold_now",
            "If held",
            usd(
                "hold_now",
                reading(latest, "lending_hold"),
                "The starting tokens at today's prices.",
            ),
        ),
        b.metric("rounds", "Borrow rounds", b.count("rounds", rounds, "Completed borrows.")),
    )


def charts(context, trigger, floor):
    one = ratio("liquidation", 1, "Aave liquidates below 1.")
    found = chart(
        "health_chart",
        "Health factor",
        [("health", "Health", points(context, "loan:health", ratio), "solid")],
        "Health at each decision. Below 1 the position can be liquidated.",
        references=[
            b.reference("Liquidation", one, "danger"),
            b.reference(
                "Unwind level", ratio("trigger", trigger, "Unwind starts below this."), "warning"
            ),
            b.reference("Floor", ratio("floor", floor, "Borrows and withdrawals stop here.")),
        ],
    )
    found += chart(
        "ltv_chart",
        "Loan to value",
        [
            ("ltv", "Loan to value", points(context, "loan:ltv", pct), "solid"),
            ("max_ltv", "Max", points(context, "loan:max_ltv", pct), "dashed"),
        ],
        "Debt over collateral, against Aave's borrow limit.",
    )
    found += chart(
        "debt_chart",
        "Debt and collateral",
        [
            (
                "collateral",
                "Collateral",
                worth(context, "collateral:supplied", "collateral:price_usd", usd),
                "solid",
            ),
            ("debt", "Debt", worth(context, "loan:debt", "loan:price_usd", usd), "dashed"),
        ],
        "In USD at the Aave oracle.",
    )
    found += chart(
        "value_chart",
        "Net value against holding",
        [
            ("value", "Strategy", points(context, "lending_value", usd), "solid"),
            ("hold", "If held", points(context, "lending_hold", usd), "dashed"),
        ],
        "Wallet plus collateral minus debt, against the starting tokens held, in USD.",
    )
    found += chart(
        "rates_chart",
        "Rates",
        [
            (
                "supply",
                "Collateral supply APY",
                points(context, "collateral:supply_apy", pct),
                "solid",
            ),
            ("borrow", "Loan borrow APY", points(context, "loan:borrow_apy", pct), "dashed"),
        ],
        "What the collateral earns and what the debt costs, per year.",
    )
    return found


def tables(context):
    """A composed strategy's tables depend on its parts: draw each one it has."""
    return [
        ui.Table(id=id, title=title, data=context.datasets[key])
        for id, key, title in (
            ("inventory", "fund_inventory", "Fund cash"),
            ("gas", "payer_costs", "Gas paid"),
            ("transactions", "transactions", "Transactions"),
        )
        if key in context.datasets
    ]


def rounds_text(values) -> str:
    reason = values.get("capped_reason")
    if reason:
        return str(reason)
    return f"Completed borrow rounds: {values.get('rounds', 0)}. No round has been refused."
