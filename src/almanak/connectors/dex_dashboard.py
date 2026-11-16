"""Read-only exchange inventory and payer scope from retained execution evidence."""

from decimal import Decimal, localcontext

from almanak.connectors.concentrated_math import amounts, sqrt_at_tick
from almanak.connectors.dex_reporting import observations
from almanak.dashboard.history import decimal_text
from almanak.dashboard.models import (
    Column,
    Context,
    Dataset,
    ExchangePosition,
    HistoryRow,
    Ref,
    Scope,
    Value,
)
from almanak.dashboard.projection import snapshot
from almanak.dashboard.tables import NATIVE_SYMBOLS as NATIVE_SYMBOLS
from almanak.dashboard.tables import several_accounts as several_accounts
from almanak.dashboard.tables import symbol_of as symbol_of
from almanak.dashboard.tables import token_dataset as token_dataset
from almanak.dashboard.tables import transactions_dataset as shared_transactions
from almanak.gateway.evm import native_asset


def position_values(state, owned, values):
    target = owned.target
    history = observations(state, target)
    latest = history[-1] if history else None
    inventory = []
    for index, amount in enumerate((owned.inventory0, owned.inventory1)):
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
            evidence="recorded" if history else "configured",
            block=latest.block if latest else None,
            reason="Runtime-attributed inventory; excludes LP principal and fees."
            if history
            else "Configured initial inventory; fund balance has not been observed.",
        )
        inventory.append(value)
        values[key] = value
    return ExchangePosition(allocation=target.allocation, target=target, inventory=tuple(inventory))


def readings(observation, start=None) -> dict[str, str] | None:
    """One concentrated-liquidity observation as decimal readings, in currency1 per currency0:
    `price`, `wallet0`/`wallet1`, and with a position `range_lower`/`range_upper`,
    `position0`/`position1` (principal at this price); `value` is wallet plus position in
    currency1. With the strategy's `start` observation, `hold` is its wallet at this price:
    what the starting tokens would be worth had the strategy done nothing. A fixed-range
    position reads its range from the target, since the retained position leaves it absent."""
    position = getattr(observation, "position", None)
    if position is None:
        return concentrated(observation, start=start)
    shape = position.range or getattr(getattr(observation, "target", None), "range", None)
    return concentrated(observation, position.liquidity, shape, start)


def concentrated(observation, liquidity=0, shape=None, start=None, fees=None):
    """Readings from a pool's sqrt price, the wallets and an optional `liquidity` over the tick
    `shape`; uncollected `fees` (base units of currency0, currency1) add `fees0`/`fees1` and
    count in `value`."""
    pool = getattr(observation, "pool", None)
    sqrt_price = getattr(pool, "sqrt_price_x96", None)
    if pool is None or not sqrt_price:
        return None
    unit0 = Decimal(10) ** pool.currency0.decimals
    unit1 = Decimal(10) ** pool.currency1.decimals
    with localcontext() as context:
        context.prec = 60
        scale = unit0 / unit1

        def price_at(sqrt: int) -> Decimal:
            return (Decimal(sqrt) / Decimal(2**96)) ** 2 * scale

        price = price_at(sqrt_price)
        wallet0 = Decimal(observation.wallet0.amount) / unit0
        wallet1 = Decimal(observation.wallet1.amount) / unit1
        fields = {"price": price, "wallet0": wallet0, "wallet1": wallet1}
        held0 = held1 = Decimal(0)
        if shape is not None:
            lower, upper = sqrt_at_tick(shape.lower), sqrt_at_tick(shape.upper)
            amount0, amount1 = amounts(liquidity, sqrt_price, lower, upper)
            held0, held1 = Decimal(amount0) / unit0, Decimal(amount1) / unit1
            fields |= {
                "range_lower": price_at(lower),
                "range_upper": price_at(upper),
                "position0": held0,
                "position1": held1,
            }
        if fees is not None:
            fees0, fees1 = Decimal(fees[0]) / unit0, Decimal(fees[1]) / unit1
            fields |= {"fees0": fees0, "fees1": fees1}
            held0, held1 = held0 + fees0, held1 + fees1
        fields["value"] = (wallet0 + held0) * price + wallet1 + held1
        if start is not None:
            fields["hold"] = (
                Decimal(start.wallet0.amount) / unit0 * price
                + Decimal(start.wallet1.amount) / unit1
            )
        return {key: decimal_text(value) for key, value in fields.items()}


def history_row(state, observed, read=readings) -> HistoryRow | None:
    """The decision's reading of the first owned position's pool, for the dashboard history:
    `read(observation, start)` is the family's reader."""
    targets = [owned.target for owned in state.owned]
    for observation in observed or ():
        if getattr(observation, "target", None) not in targets:
            continue
        start = next(
            (o for o in state.baseline if getattr(o, "target", None) == observation.target), None
        )
        fields = read(observation, start)
        if fields is None:
            return None
        block = observation.pool.block
        return HistoryRow(time=block.timestamp, block=block.number, fields=fields)
    return None


def payer_values(state, values):
    rows = []
    for index, payer in enumerate(
        dict.fromkeys(b.profile.submitter for b in state.config.accounts.bindings)
    ):
        bindings = [b for b in state.config.accounts.bindings if b.profile.submitter == payer]
        outcomes = [o for o in state.outcomes if o.execution.submitter == payer]
        # One payer can serve several funds. Keep the aggregate at payer scope,
        # without labeling the gas as a protocol-position balance.
        key = f"payer{index}-gas"
        asset = native_asset(payer.chain.chain_id)
        metadata = state.config.asset_metadata.get(asset)
        values[key] = Value(
            id=key,
            amount=str(sum(o.gas_paid.amount for o in outcomes)) if state.initialized else None,
            unit="base_units",
            asset=asset,
            decimals=metadata.decimals if metadata else None,
            symbol=symbol_of(state, asset),
            scope=Scope(account=f"payer{index}", fund=payer, payer=payer),
            evidence="recorded",
            block=outcomes[-1].after.block if outcomes else None,
            reason="Native gas from retained receipts across the listed fund accounts."
            if state.initialized
            else "No initialized receipt ledger.",
        )
        rows.append(
            {
                "accounts": ", ".join(b.name for b in bindings),
                "payer": payer.address,
                "gas": Ref(ref=key),
            }
        )
    # A single fund account needs no column naming it: it reads as a choice between wallets.
    shared = len(state.config.accounts.bindings) > 1
    columns = (
        *((Column(key="accounts", label="Fund accounts"),) if shared else ()),
        Column(key="payer", label="Submitter"),
        Column(key="gas", label="Receipt gas", kind="value"),
    )
    return Dataset(
        columns=columns,
        rows=tuple(r if shared else {k: v for k, v in r.items() if k != "accounts"} for r in rows),
        coverage="Retained native receipt costs. Fiat conversion and external transfers excluded.",
    )


# What each committed operation did, in the owner's words; an unknown kind is shown as named.
OPERATION_LABELS = {
    "approve": "Approve",
    "approve_erc20": "Approve",
    "approve_permit2": "Approve via Permit2",
    "revoke_erc20": "Revoke approval",
    "revoke_permit2": "Revoke Permit2 approval",
    "swap": "Swap",
    "mint": "Open LP position",
    "increase": "Add liquidity",
    "collect": "Collect fees",
    "decrease": "Remove liquidity",
    "close": "Close LP position",
    "burn": "Close LP position",
}


def operation_label(state, operation):
    kind = operation.kind
    label = OPERATION_LABELS.get(kind, kind.replace("_", " ").capitalize())
    # An approval of zero revokes: teardown's cleanup, or the reset before a new approval.
    if kind == "approve" and getattr(operation, "amount", None) == 0:
        label = "Revoke approval"
    if kind == "swap":
        spent, received = (symbol_of(state, a.asset) for a in (operation.amount, operation.minimum))
        if spent and received:
            return f"{label} {spent} → {received}"
    elif getattr(operation, "asset", None) is not None:
        token = symbol_of(state, operation.asset)
        if token:
            return f"{label} {token}"
    return label


def transactions_dataset(state, values):
    """One row per committed transaction, newest MAX_TABLE_ROWS, named in exchange terms
    ("Swap USDC → XDP"). `payer_costs` keeps the total over all."""
    return shared_transactions(state, values, lambda s, o: operation_label(s, o.operation))


def project(state, *, parameter_type, captured_at=None):
    validate_projection(state)
    values: dict[str, Value] = {}
    positions = tuple(position_values(state, owned, values) for owned in state.owned)
    context = Context(
        config=parameter_type.model_validate(
            {name: getattr(state.config, name) for name in parameter_type.model_fields}
        ),
        memory=state.memory,
        positions=positions,
        values=values,
        datasets={
            "exchange_inventory": inventory_dataset(positions, keep=several_accounts(state)),
            "payer_costs": payer_values(state, values),
            "transactions": transactions_dataset(state, values),
        },
    )
    return context, snapshot(state, values, captured_at)


def validate_projection(state):
    if state.integrity_error:
        raise ValueError("invalidated evidence cannot produce a financial dashboard")
    if len(state.owned) > 16 or len(state.config.accounts.bindings) > 16:
        raise ValueError("dashboard preview supports at most 16 exchange positions and funds")


def inventory_dataset(positions, *, keep=False):
    """One row per token an allocation holds, labelled by its symbol. The fund account is named
    only when the allocations use more than one."""
    rows = tuple(
        {
            "allocation": p.allocation,
            "account": p.target.account,
            "token": value.symbol or str(value.asset),
            "amount": Ref(ref=value.id),
        }
        for p in positions
        for value in p.inventory
    )
    return token_dataset(
        rows,
        "Attributed liquid inventory only. LP principal, uncollected fees and "
        "common-currency PnL are not included.",
        keep=keep,
    )
