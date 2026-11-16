"""Collateral, debt, health and rates from the readings the runtime keeps at each decision
(`context.history`), with the reserve and gas tables."""

from decimal import Decimal

from almanak import dashboard as ui
from almanak.connectors.aave_v3.models import Parameters
from almanak.dashboard import build as b

SOURCE = "Aave V3 oracle, read at each decision"
WAD = Decimal(10**18)


def dashboard(context: ui.Context) -> ui.Dashboard:
    config = context.config
    if not isinstance(config, Parameters):
        raise TypeError("this dashboard reads a Aave V3 configuration")
    latest = context.history[-1].fields if context.history else {}

    def reading(field):
        return Decimal(latest[field]) if field in latest else None

    def token(allocation, id, amount, reason):
        position = next(
            p
            for p in context.positions
            if isinstance(p, ui.Position) and p.allocation == allocation
        )
        meta = config.asset_metadata.get(position.target.asset)
        return b.token(
            id,
            amount,
            reason,
            asset=position.target.asset,
            decimals=meta.decimals if meta else 18,
            symbol=(meta.symbol if meta else None) or allocation,
            scope=position.owned_shares.scope,
            evidence="author_derived",
        )

    def supplied(id, amount, reason):
        return token("collateral", id, amount, reason)

    def usd(id, amount, reason):
        return b.usd(id, amount, reason, price_source=SOURCE, evidence="author_derived")

    def apy(id, amount, reason):
        percent = None if amount is None else amount * 100
        return b.value(id, percent, "percent", reason=reason, places=2)

    def health(id, amount, reason):
        return b.ratio(id, amount, reason, evidence="author_derived")

    def points(field, make):
        return [
            (t, make(f"{field}-{i}", v, "Reading."))
            for i, (t, v) in enumerate(b.history(context, field))
        ]

    def chart(id, title, lines, caption, **marks):
        drawn = [
            b.series(key, label, found, style=style) for key, label, found, style in lines if found
        ]
        return [b.line_chart(id, title, drawn, caption=caption, **marks)] if drawn else []

    minimum = config.allocation("loan").minimum_health_factor
    if minimum is None:
        raise ValueError("the loan allocation needs a minimum_health_factor")
    floor = Decimal(minimum) / WAD
    now = reading("loan:health")
    if now is None:
        status = b.callout("health", "No debt", "Nothing is borrowed right now.")
    elif now < floor:
        status = b.callout(
            "health", "Below the health floor", f"Health {now:.2f} is under {floor}.", "danger"
        )
    else:
        status = b.callout(
            "health", "Healthy", f"Health {now:.2f}, above the {floor} floor.", "success"
        )

    numbers = b.group(
        "now",
        "Now",
        b.metric(
            "supplied",
            "Collateral supplied",
            supplied("supplied", reading("collateral:supplied"), "Latest reading."),
        ),
        b.metric("debt", "Debt", token("loan", "debt", reading("loan:debt"), "Latest reading.")),
        b.metric("health_now", "Health factor", health("health_now", now, "Liquidation below 1.")),
        b.metric(
            "value", "Net value", usd("value", reading("value"), "Wallet plus supplied minus debt.")
        ),
        b.metric(
            "hold",
            "If held",
            usd("hold", reading("hold"), "The starting tokens at today's prices."),
        ),
        b.metric(
            "supply_apy",
            "Supply APY",
            apy("supply_apy", reading("collateral:supply_apy"), "Collateral reserve."),
        ),
        b.metric(
            "borrow_apy",
            "Borrow APY",
            apy("borrow_apy", reading("loan:borrow_apy"), "Loan reserve."),
        ),
    )

    blocks = [status, numbers]
    blocks += chart(
        "value_chart",
        "Net value against holding",
        [
            ("value", "Strategy", points("value", usd), "solid"),
            ("hold", "If held", points("hold", usd), "dashed"),
        ],
        "Wallet plus supplied minus debt, in USD at the Aave oracle.",
    )
    blocks += chart(
        "supplied_chart",
        "Collateral supplied",
        [("supplied", "Supplied", points("collateral:supplied", supplied), "solid")],
        "Collateral in the reserve at each decision, interest included.",
    )
    blocks += chart(
        "rates_chart",
        "Rates",
        [
            ("supply", "Supply APY", points("collateral:supply_apy", apy), "solid"),
            ("borrow", "Borrow APY", points("loan:borrow_apy", apy), "dashed"),
        ],
        "Collateral reserve supply rate and loan reserve borrow rate.",
    )
    blocks += [
        ui.Table(
            id="positions", title="Reserve positions", data=context.datasets["lending_positions"]
        ),
        ui.Table(id="costs", title="Fund and gas payer", data=context.datasets["fund_costs"]),
        *transactions_table(context),
        ui.Text(
            id="scope",
            text="Values are in USD at the Aave oracle. Gas is shown separately and not "
            "subtracted from value.",
        ),
    ]
    return b.dashboard("Aave V3 lending", *blocks, description="Collateral, debt and rates.")


def transactions_table(context):
    """Each committed transaction, when the run has recorded any."""
    if "transactions" not in context.datasets:
        return ()
    return (
        ui.Table(id="transactions", title="Transactions", data=context.datasets["transactions"]),
    )
