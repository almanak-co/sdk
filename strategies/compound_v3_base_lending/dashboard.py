"""The lender's supply and the borrower's health, debt and rates, from the readings the runtime
keeps at each decision (`context.history`), with the position and gas tables."""

from decimal import Decimal

from almanak import dashboard as ui
from almanak.connectors.compound_v3.models import Parameters
from almanak.dashboard import build as b

SOURCE = "Compound V3 Comet price feeds, read at each decision"
WAD = Decimal(10**18)


def dashboard(context: ui.Context) -> ui.Dashboard:
    config = context.config
    if not isinstance(config, Parameters):
        raise TypeError("this dashboard reads a Compound V3 configuration")
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

    def earned(id, amount, reason):
        return token("earn", id, amount, reason)

    def usd(id, amount, reason):
        return b.usd(id, amount, reason, price_source=SOURCE, evidence="author_derived")

    def pct(id, amount, reason):
        percent = None if amount is None else amount * 100
        return b.value(id, percent, "percent", reason=reason, places=2)

    def ratio(id, amount, reason):
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
    health = reading("loan:health")
    if health is None:
        status = b.callout("status", "No debt", "The borrower owes nothing right now.")
    elif health < floor:
        status = b.callout(
            "status", "Below the health floor", f"Health {health:.2f} is under {floor}.", "danger"
        )
    else:
        status = b.callout(
            "status", "Healthy", f"Health {health:.2f}, above the {floor} floor.", "success"
        )

    numbers = b.group(
        "now",
        "Now",
        b.metric(
            "earn_now",
            "Lender supplied",
            token("earn", "earn_now", reading("earn:supplied"), "Interest included."),
        ),
        b.metric(
            "collateral_now",
            "Borrower collateral",
            token("collateral", "collateral_now", reading("collateral:collateral"), "Posted."),
        ),
        b.metric(
            "debt_now",
            "Borrower debt",
            token("loan", "debt_now", reading("loan:debt"), "Interest included."),
        ),
        b.metric("health_now", "Health factor", ratio("health_now", health, "Absorbed below 1.")),
        b.metric(
            "ltv_now", "Loan to value", pct("ltv_now", reading("loan:ltv"), "Debt over collateral.")
        ),
        b.metric(
            "supply_apy", "Supply APY", pct("supply_apy", reading("earn:supply_apy"), "Base asset.")
        ),
        b.metric(
            "borrow_apy", "Borrow APY", pct("borrow_apy", reading("loan:borrow_apy"), "Base asset.")
        ),
        b.metric("value_now", "Net value", usd("value_now", reading("value"), "Both funds.")),
        b.metric(
            "hold_now",
            "If held",
            usd("hold_now", reading("hold"), "The starting tokens at today's prices."),
        ),
    )

    blocks = [status, numbers]
    blocks += chart(
        "health_chart",
        "Borrower health",
        [("health", "Health", points("loan:health", ratio), "solid")],
        "Collateral value at the liquidation factor over debt. Below 1 Comet absorbs it.",
        references=[
            b.reference("Liquidation", ratio("liquidation", 1, "Absorbed below 1."), "danger"),
            b.reference("Floor", ratio("floor", floor, "Borrows and withdrawals stop here.")),
        ],
    )
    blocks += chart(
        "supplied_chart",
        "Lender supplied",
        [("earn", "Supplied", points("earn:supplied", earned), "solid")],
        "The earning fund's balance, interest included.",
    )
    blocks += chart(
        "value_chart",
        "Net value against holding",
        [
            ("value", "Strategy", points("value", usd), "solid"),
            ("hold", "If held", points("hold", usd), "dashed"),
        ],
        "Wallets plus supply and collateral minus debt, both funds, in USD.",
    )
    blocks += chart(
        "rates_chart",
        "Rates",
        [
            ("supply", "Supply APY", points("earn:supply_apy", pct), "solid"),
            ("borrow", "Borrow APY", points("loan:borrow_apy", pct), "dashed"),
        ],
        "The base asset's supply and borrow rates, per year.",
    )
    blocks += [
        ui.Table(
            id="positions", title="Comet positions", data=context.datasets["lending_positions"]
        ),
        ui.Table(id="gas", title="Fund execution costs", data=context.datasets["fund_costs"]),
        *transactions_table(context),
        ui.Text(
            id="scope",
            text="Values are in USD at the Comet price feeds. Collateral earns nothing on "
            "Comet. Gas is shown separately.",
        ),
    ]
    return b.dashboard(
        "Compound V3 lending", *blocks, description="The lender's supply and the borrower's health."
    )


def transactions_table(context):
    """Each committed transaction, when the run has recorded any."""
    if "transactions" not in context.datasets:
        return ()
    return (
        ui.Table(id="transactions", title="Transactions", data=context.datasets["transactions"]),
    )
