"""The fund's GM shares over time and the tokens beside them, from the readings the runtime keeps
at each decision (`context.history`). GM shares are not marked to a price: their value is
not observed, so the dashboard shows share counts and token inventory only."""

from decimal import Decimal

from almanak import dashboard as ui
from almanak.connectors.gmx_v2.models import Parameters
from almanak.dashboard import build as b
from almanak.dashboard.models import ExchangePosition

TABLES = (
    ("gm_shares", "GM shares owned"),
    ("requests", "GMX requests"),
    ("submitters", "Gas paid"),
    ("transactions", "Transactions"),
)


def symbol(metadata, asset):
    found = metadata.get(asset)
    return (found.symbol if found else None) or "token"


def token(metadata, id, held, amount, reason):
    """An amount of a held token, in its own units."""
    found = metadata.get(held.asset)
    return b.value(
        id,
        None if amount is None else Decimal(amount),
        "base_units",
        reason=reason,
        places=found.decimals if found else held.decimals or 0,
        asset=held.asset,
        scope=held.scope,
        symbol=symbol(metadata, held.asset),
        evidence="author_derived",
    )


def status(owned):
    if owned is None:
        return b.callout("state", "Not read yet", "No decision has read the fund's GM shares.")
    if Decimal(owned):
        return b.callout(
            "state", "Providing liquidity", "The fund holds GM shares of the market.", "success"
        )
    return b.callout("state", "No liquidity", "The fund holds no GM shares now.")


def now_group(metadata, latest, wallets, shares):
    return b.group(
        "now",
        "Now",
        b.metric(
            "gm_shares",
            "GM shares",
            token(
                metadata, "gm_shares", shares, latest.get("gm_shares"), "Latest decision reading."
            ),
        ),
        *(
            b.metric(
                f"wallet{i}",
                f"{symbol(metadata, held.asset)} in the fund",
                token(
                    metadata, f"wallet{i}", held, latest.get(f"wallet{i}"), "Latest wallet reading."
                ),
            )
            for i, held in enumerate(wallets)
        ),
    )


def shares_chart(context, metadata, position, market, shares):
    points = [
        (t, token(metadata, f"g{i}", shares, v, "Reading."))
        for i, (t, v) in enumerate(b.history(context, "gm_shares"))
    ]
    if not points:
        return []
    markers = [
        b.marker(t, action.replace("_", " ")[:24], series="gm_shares")
        for t, action, _ in b.trades(context, position.allocation)
        if points[0][0] <= t
    ]
    return [
        b.line_chart(
            "shares_chart",
            "GM shares",
            [b.series("gm_shares", f"GM shares ({symbol(metadata, market.share_asset)})", points)],
            markers=markers,
            caption="GM shares the fund held at each decision, with its transactions.",
        )
    ]


def wallet_charts(context, metadata, wallets):
    lines = [
        b.series(
            f"wallet{i}",
            symbol(metadata, held.asset),
            [
                (t, token(metadata, f"w{i}_{j}", held, v, "Reading."))
                for j, (t, v) in enumerate(b.history(context, f"wallet{i}"))
            ],
        )
        for i, held in enumerate(wallets[:2])
        if b.history(context, f"wallet{i}")
    ]
    return [
        b.line_chart(
            f"{line.id}_chart",
            f"{line.label} in the fund",
            [line],
            caption="The fund's wallet balance at each decision, outside the GM pool.",
        )
        for line in lines
    ]


def tables(context):
    return [
        ui.Table(id=f"{key}_table", title=title, data=context.datasets[key])
        for key, title in TABLES
        if key in context.datasets
    ]


def dashboard(context: ui.Context) -> ui.Dashboard:
    config = context.config
    if not isinstance(config, Parameters):
        raise TypeError("this dashboard reads a GMX V2 configuration")
    position = next(p for p in context.positions if isinstance(p, ExchangePosition))
    market = config.allocation(position.allocation).market
    metadata = config.asset_metadata
    latest = context.history[-1].fields if context.history else {}
    *wallets, shares = position.inventory
    blocks = [
        status(latest.get("gm_shares")),
        now_group(metadata, latest, wallets, shares),
        *shares_chart(context, metadata, position, market, shares),
        *wallet_charts(context, metadata, wallets),
        *tables(context),
        ui.Text(
            id="scope",
            text="GM shares are counted, not valued: the market's pool value per share is not "
            "read, so no USD value or PnL is shown. Gas is shown separately.",
        ),
    ]
    return b.dashboard(
        f"GMX {symbol(metadata, market.long_asset)}/{symbol(metadata, market.short_asset)} "
        "liquidity",
        *blocks,
        description="GM shares held and the tokens beside them.",
    )
