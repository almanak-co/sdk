"""Ordered coin inventory and LP shares retain identity without invented fee income."""

from decimal import Decimal, localcontext

from almanak.connectors.curve.models import Parameters
from almanak.connectors.dex_dashboard import (
    payer_values,
    several_accounts,
    symbol_of,
    token_dataset,
    transactions_dataset,
    validate_projection,
)
from almanak.connectors.dex_reporting import observations
from almanak.dashboard.history import decimal_text
from almanak.dashboard.models import (
    Context,
    ExchangePosition,
    Ref,
    Scope,
    Value,
)
from almanak.dashboard.projection import snapshot


def inventory_values(state, owned, latest, values):
    target = owned.target
    inventory = []
    for index, amount in enumerate(owned.inventory.root):
        key = f"{target.allocation}-inventory{index}"
        metadata = state.config.asset_metadata.get(amount.asset)
        value = Value(
            id=key,
            amount=str(amount.amount),
            unit="base_units",
            asset=amount.asset,
            decimals=metadata.decimals if metadata else None,
            symbol=symbol_of(state, amount.asset),
            scope=Scope(account=target.account, fund=target.fund, target=target),
            evidence="recorded" if latest else "configured",
            block=latest.block if latest else None,
            reason="Runtime-attributed liquid coin inventory, excluding LP shares."
            if latest
            else "Configured initial inventory; fund balance has not been observed.",
        )
        inventory.append(value)
        values[key] = value
    return ExchangePosition(allocation=target.allocation, target=target, inventory=tuple(inventory))


def share_value(state, owned, latest, values):
    target = owned.target
    key = f"{target.allocation}-shares"
    values[key] = Value(
        id=key,
        amount=str(owned.position.shares.amount if owned.position else 0) if latest else None,
        unit="base_units",
        asset=target.pool.shares_asset,
        decimals=18,
        symbol="LP shares",
        scope=Scope(account=target.account, fund=target.fund, target=target),
        evidence="recorded",
        block=latest.block if latest else None,
        reason="Retained fund LP shares; fees remain embedded in pool share value."
        if latest
        else "No retained fund/pool observation.",
    )
    return {
        "allocation": target.allocation,
        "account": target.account,
        "token": values[key].symbol or str(target.pool.shares_asset),
        "amount": Ref(ref=key),
    }


def project(state, *, captured_at=None):
    validate_projection(state)
    values: dict[str, Value] = {}
    positions, shares = [], []
    for owned in state.owned:
        history = observations(state, owned.target)
        latest = history[-1] if history else None
        positions.append(inventory_values(state, owned, latest, values))
        shares.append(share_value(state, owned, latest, values))
    rows = [
        {
            "allocation": position.allocation,
            "account": position.target.account,
            "token": value.symbol or str(value.asset),
            "amount": Ref(ref=value.id),
        }
        for position in positions
        for value in position.inventory
    ]
    context = Context(
        config=Parameters.model_validate(
            {name: getattr(state.config, name) for name in Parameters.model_fields}
        ),
        memory=state.memory,
        positions=tuple(positions),
        values=values,
        datasets={
            "exchange_inventory": token_dataset(
                rows, "Attributed liquid coin inventory only.", keep=several_accounts(state)
            ),
            "pool_shares": token_dataset(
                shares,
                "LP share quantities. Separate fee income and fiat valuation are unavailable.",
                keep=several_accounts(state),
            ),
            "payer_costs": payer_values(state, values),
            "transactions": transactions_dataset(state, values),
        },
    )
    return context, snapshot(state, values, captured_at)


def readings(observation, start=None) -> dict[str, str] | None:
    """One Curve observation as decimal readings in coin units: `wallet0`..`walletN-1`,
    `shares` (the fund's LP shares) and `position0`.. (their proportional claim on the pool
    balances), and `value` (wallet plus position) and with the `start` observation `hold` (its
    wallet), both in coin0. A StableSwap pool converts coins at its stored rates, i.e. at the
    peg, not at a market price; a Crypto pool converts at its price oracle and also reads
    `price1`.. (coin0 per coin i)."""
    pool = observation.pool
    units = [Decimal(10) ** coin.decimals for coin in pool.currencies]
    with localcontext() as context:
        context.prec = 60
        if hasattr(pool, "rates"):
            peg = [rate * unit for rate, unit in zip(pool.rates, units, strict=True)]
            weights = [value / peg[0] for value in peg]
            prices = {}
        else:
            oracle = (
                pool.price_oracle if isinstance(pool.price_oracle, tuple) else (pool.price_oracle,)
            )
            weights = [Decimal(1), *(Decimal(p) / 10**18 for p in oracle)]
            prices = {f"price{i}": weight for i, weight in enumerate(weights) if i}

        def coins(amounts):
            return [Decimal(amount) / unit for amount, unit in zip(amounts, units, strict=True)]

        def worth(amounts):
            return sum(a * w for a, w in zip(amounts, weights, strict=True))

        wallet = coins(value.amount for value in observation.wallets.root)
        fields = {f"wallet{i}": amount for i, amount in enumerate(wallet)} | prices
        held = [Decimal(0)] * len(units)
        position = observation.position
        if position is not None and pool.total_supply:
            shares = position.shares.amount
            held = coins(balance * shares // pool.total_supply for balance in pool.balances)
            fields["shares"] = Decimal(shares) / Decimal(10) ** pool.shares.decimals
            fields |= {f"position{i}": amount for i, amount in enumerate(held)}
        fields["value"] = worth(wallet) + worth(held)
        if start is not None:
            fields["hold"] = worth(coins(value.amount for value in start.wallets.root))
        return {key: decimal_text(value) for key, value in fields.items()}


def history_row(state, observed):
    from almanak.connectors.dex_dashboard import history_row as family

    return family(state, observed, readings)
