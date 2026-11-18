"""Display retained LP shares and separately calculated claims without a peg or valuation."""

from decimal import Decimal, localcontext

from almanak.connectors.aerodrome.math import removal
from almanak.connectors.aerodrome.models import Parameters
from almanak.connectors.aerodrome.ownership import at_indices
from almanak.connectors.dex_dashboard import project as exchange_project
from almanak.connectors.dex_dashboard import symbol_of
from almanak.connectors.dex_reporting import observations
from almanak.dashboard.history import decimal_text
from almanak.dashboard.models import Column, Dataset, Ref, Scope, Value
from almanak.dashboard.projection import snapshot


def position_values(state, owned, values):
    history = observations(state, owned.target)
    latest = history[-1] if history else None
    position = at_indices(owned.position, latest.pool) if latest else None
    assets = owned.target.pool.shares_asset, *owned.target.assets
    amounts = (
        (position.shares.amount, position.fees0.amount, position.fees1.amount)
        if position
        else (0, 0, 0)
    )
    row: dict[str, str | Ref] = {"allocation": owned.target.allocation}
    for label, asset, amount in zip(("shares", "fees0", "fees1"), assets, amounts, strict=True):
        key = f"{owned.target.allocation}-{label}"
        metadata = state.config.asset_metadata.get(asset)
        values[key] = Value(
            id=key,
            asset=asset,
            amount=str(amount) if latest else None,
            unit="base_units",
            decimals=18 if label == "shares" else (metadata.decimals if metadata else None),
            symbol="LP shares" if label == "shares" else symbol_of(state, asset),
            scope=Scope(account=owned.target.account, fund=owned.target.fund, target=owned.target),
            block=latest.block if latest else None,
            evidence="recorded",
            reason="Retained fund LP shares or claims calculated at the last pool fee indices."
            if latest
            else "No retained fund/pool observation.",
        )
        row[label] = Ref(ref=key)
    return row


def project(state, *, captured_at=None):
    context, _ = exchange_project(state, parameter_type=Parameters, captured_at=captured_at)
    values = dict(context.values)
    rows = tuple(position_values(state, owned, values) for owned in state.owned)
    dataset = Dataset(
        columns=(
            Column(key="allocation", label="Position"),
            Column(key="shares", label="Fungible LP shares", kind="value"),
            Column(key="fees0", label="Unclaimed currency 0 fees", kind="value"),
            Column(key="fees1", label="Unclaimed currency 1 fees", kind="value"),
        ),
        rows=rows,
        coverage="Retained shares and fee claims. LP valuation and fiat PnL are unavailable.",
    )
    context = type(context)(
        **{
            **context.model_dump(),
            "values": values,
            "datasets": {**context.datasets, "pool_shares_and_fees": dataset},
        }
    )
    return context, snapshot(state, values, captured_at)


def readings(observation, start=None) -> dict[str, str] | None:
    """One Aerodrome observation as decimal readings, in currency1 per currency0: `price` is the
    marginal price at the stored reserves (reserve1 / reserve0 for a volatile pool; for a stable
    pool the slope of x³y + y³x = k, i.e. (3x²y + y³) / (x³ + 3xy²) in token units),
    `wallet0`/`wallet1`, `shares` (the fund's LP shares), `position0`/`position1` (the shares'
    proportional claim on the pool balances), `fees0`/`fees1` (fees claimable at the pool's
    current indices), `value` (wallet, principal and fees in currency1) and with the `start`
    observation `hold` (its wallet at this price)."""
    pool = observation.pool
    if not (pool.reserve0 and pool.reserve1):
        return None
    unit0 = Decimal(10) ** pool.currency0.decimals
    unit1 = Decimal(10) ** pool.currency1.decimals
    with localcontext() as context:
        context.prec = 60
        x, y = Decimal(pool.reserve0) / unit0, Decimal(pool.reserve1) / unit1
        price = (3 * x * x * y + y**3) / (x**3 + 3 * x * y * y) if pool.source.stable else y / x
        wallet0 = Decimal(observation.wallet0.amount) / unit0
        wallet1 = Decimal(observation.wallet1.amount) / unit1
        fields = {"price": price, "wallet0": wallet0, "wallet1": wallet1}
        held0 = held1 = Decimal(0)
        position = at_indices(observation.position, pool)
        if position is not None:
            shares = position.shares.amount
            principal = (0, 0)
            if shares:
                try:
                    principal = removal(pool.balance0, pool.balance1, pool.total_supply, shares)
                except ValueError:
                    principal = (0, 0)
            fees0, fees1 = position.fees0.amount / unit0, position.fees1.amount / unit1
            fields |= {
                "shares": Decimal(shares) / Decimal(10) ** pool.shares.decimals,
                "position0": principal[0] / unit0,
                "position1": principal[1] / unit1,
                "fees0": fees0,
                "fees1": fees1,
            }
            held0 = principal[0] / unit0 + fees0
            held1 = principal[1] / unit1 + fees1
        fields["value"] = (wallet0 + held0) * price + wallet1 + held1
        if start is not None:
            fields["hold"] = (
                Decimal(start.wallet0.amount) / unit0 * price
                + Decimal(start.wallet1.amount) / unit1
            )
        return {key: decimal_text(value) for key, value in fields.items()}


def history_row(state, observed):
    from almanak.connectors.dex_dashboard import history_row as family

    return family(state, observed, readings)
