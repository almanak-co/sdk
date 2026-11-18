"""WETH supplied per allocation and the health of each USDC debt, from the readings the runtime
keeps at each decision (`context.history`), with the position, cost and transaction tables."""

from decimal import Decimal

from almanak import dashboard as ui
from almanak.connectors.aave_v4.models import LendingConfig
from almanak.dashboard import build as b

WAD = Decimal(10**18)


def dashboard(ctx: ui.Context) -> ui.Dashboard:
    config = ctx.config
    if not isinstance(config, LendingConfig):
        raise TypeError("this dashboard reads an Aave V4 configuration")
    latest = ctx.history[-1].fields if ctx.history else {}
    debts = tuple(config.debts)
    return ui.Dashboard(
        title="Aave WETH positions",
        description="Safe-owned collateral, optional USDC debt and recorded execution costs.",
        blocks=(
            status(debts, latest),
            overview(ctx, debts, latest),
            *charts(ctx, config, debts),
            ui.Table(
                id="positions", title="Supply positions", data=ctx.datasets["supply_positions"]
            ),
            *tuple(
                allocation_dashboard(ctx, p) for p in ctx.positions if isinstance(p, ui.Position)
            ),
            *debt_table(ctx),
            ui.Table(id="gas", title="Fund costs", data=ctx.datasets["fund_costs"]),
            ui.Table(id="activity", title="Transaction history", data=ctx.datasets["transactions"]),
            ui.Text(
                id="policy",
                text="Each allocation supplies once. Its holding period starts at "
                "the confirmed supply block recorded in strategy memory. Teardown closes the "
                "remaining debt before releasing collateral and clears allowances. "
                "Borrowed cash is matched by a liability. Borrowing costs and operator gas "
                "are reported in their respective assets. Aave V4 readings carry no prices "
                "or rates, so no USD value is shown.",
            ),
        ),
    )


def ratio(id, amount, reason):
    return b.ratio(id, amount, reason, evidence="author_derived")


def supplied(config, position):
    """A maker of WETH amounts for one allocation's supply readings."""
    meta = config.asset_metadata.get(position.target.asset)

    def make(id, amount, reason):
        return b.token(
            id,
            amount,
            reason,
            asset=position.target.asset,
            decimals=meta.decimals if meta else 18,
            symbol=(meta.symbol if meta else None) or "WETH",
            scope=position.owned_shares.scope,
            evidence="author_derived",
        )

    return make


def points(ctx, field, make):
    return [
        (t, make(f"{field}-{i}", v, "Reading.")) for i, (t, v) in enumerate(b.history(ctx, field))
    ]


def health_of(latest, name):
    field = f"{name}:health"
    return Decimal(latest[field]) if field in latest else None


def unread(debts, latest):
    """Debts still recorded whose health was not read (health is read only with debt)."""
    return [
        d.name
        for d in debts
        if health_of(latest, d.name) is None and Decimal(latest.get(f"{d.name}:debt", "0"))
    ]


def health_reason(debt, latest):
    if health_of(latest, debt.name) is not None:
        return "Latest reading."
    if Decimal(latest.get(f"{debt.name}:debt", "0")):
        return "Debt is open; health was not read."
    return "No debt open."


def without_health(debts, latest):
    """The status when no debt reports its health."""
    owed = unread(debts, latest)
    if owed:
        return b.callout(
            "status", "Health unavailable", f"{', '.join(owed)} owes debt; health not read."
        )
    if not latest:
        return b.callout("status", "Waiting for a reading", "The next decision reads the loans.")
    return b.callout("status", "No debt", "Nothing is borrowed right now.")


def status(debts, latest):
    floors = {d.name: Decimal(d.minimum_health_factor) / WAD for d in debts}
    open_debts = {d.name: h for d in debts if (h := health_of(latest, d.name)) is not None}
    low = [name for name, h in open_debts.items() if h < floors[name]]
    if not open_debts:
        return without_health(debts, latest)
    if low:
        return b.callout(
            "status", "Below the health floor", f"{', '.join(low)} under its floor.", "danger"
        )
    if owed := unread(debts, latest):
        return b.callout(
            "status", "Health unavailable", f"{', '.join(owed)} owes debt; health not read."
        )
    lowest = min(open_debts.values())
    return b.callout(
        "status", "Healthy", f"Lowest health {lowest:.2f}, above every floor.", "success"
    )


def overview(ctx, debts, latest):
    health_metrics = [
        b.metric(f"health-{d.name}", f"{d.name.replace('_', ' ')} health", value)
        for d in debts
        for value in (
            ratio(f"health-{d.name}", health_of(latest, d.name), health_reason(d, latest)),
        )
    ]
    return ui.Group(
        id="overview",
        title="Position overview",
        blocks=(
            ui.Metric(
                id="open-positions",
                label="Positions with owned shares",
                value=ctx.values["position-count"],
            ),
            *tuple(
                ui.Metric(
                    id=f"{p.allocation}-balance",
                    label=p.allocation.replace("_", " "),
                    value=ctx.values[f"{p.allocation}-observed"],
                )
                for p in ctx.positions
            ),
            *health_metrics,
        ),
    )


def charts(ctx, config, debts):
    lines = [
        (
            p.allocation,
            p.allocation.replace("_", " "),
            points(ctx, f"{p.allocation}:supplied", supplied(config, p)),
        )
        for p in ctx.positions
        if isinstance(p, ui.Position)
    ]
    supplied_series = [b.series(key, label, found) for key, label, found in lines[:6] if found]
    health_series = [
        b.series(d.name, d.name.replace("_", " "), found)
        for d in debts[:6]
        if (found := points(ctx, f"{d.name}:health", ratio))
    ]
    found = []
    if supplied_series:
        found.append(
            b.line_chart(
                "supplied_chart",
                "WETH supplied",
                supplied_series,
                caption="Each allocation's supply at every decision, interest included.",
            )
        )
    if health_series:
        found.append(
            b.line_chart(
                "health_chart",
                "Debt health",
                health_series,
                caption="Health of each USDC debt. Below 1 it can be liquidated.",
                references=[
                    b.reference("Liquidation", ratio("liquidation", 1, "Below 1."), "danger")
                ],
            )
        )
    return found


def debt_table(ctx):
    if "debt_positions" not in ctx.datasets:
        return ()
    return (ui.Table(id="debt", title="Debt and collateral", data=ctx.datasets["debt_positions"]),)


def allocation_dashboard(ctx: ui.Context, position: ui.Position) -> ui.Group:
    name = position.allocation
    return ui.Group(
        id=f"allocation-{name}",
        title=name.replace("_", " "),
        blocks=(
            ui.BarChart(
                id=f"flows-{name}",
                title="Recorded capital movements",
                points=(
                    ui.Point(label="Supplied", value=ctx.values[f"{name}-supplied"]),
                    ui.Point(label="Withdrawn", value=ctx.values[f"{name}-withdrawn"]),
                ),
                caption="Confirmed asset effects in this allocation. "
                "Withdrawals include any realized accrual.",
            ),
            ui.Metric(id=f"owned-{name}", label="Owned supply shares", value=position.owned_shares),
            ui.Metric(
                id=f"delta-{name}", label="Closed token delta", value=ctx.values[f"{name}-delta"]
            ),
        ),
    )
