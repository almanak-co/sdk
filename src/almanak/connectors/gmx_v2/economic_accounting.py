"""Hypothetical fund ownership, cumulative inputs and payer costs stay separately scoped."""

from almanak.connectors.gmx_v2.economic_parameters import payer_wallet, wallet
from almanak.connectors.gmx_v2.economic_perpetual_state import PerpetualOwnership
from almanak.connectors.gmx_v2.economic_state import Payload
from almanak.effects import AssetDelta, Effects, Position, PositionChange, WalletAsset
from almanak.models import AssetAmount


def owned_assets(owned):
    if isinstance(owned, PerpetualOwnership):
        return owned.inventory
    return (*owned.inventory, owned.market_tokens)


def inventory_after(owned, changes):
    amounts = {value.asset: value.amount for value in owned_assets(owned)}
    for value in changes:
        if value.key.asset not in amounts or value.key != wallet(owned.target, value.key.asset):
            raise ValueError("GMX modeled transfer has an unowned inventory asset")
        amounts[value.key.asset] += value.amount
    if min(amounts.values()) < 0:
        raise ValueError("GMX modeled transfer exceeds its retained owned inventory")
    inventory = tuple(
        value.model_copy(update={"amount": amounts[value.asset]}) for value in owned.inventory
    )
    if isinstance(owned, PerpetualOwnership):
        return owned.model_copy(update={"inventory": inventory})
    return owned.model_copy(
        update={
            "inventory": inventory,
            "market_tokens": owned.market_tokens.model_copy(
                update={"amount": amounts[owned.market_tokens.asset]}
            ),
        }
    )


def cumulative_spending(payload, inputs):
    """Add one request's escrowed inputs to the reported cumulative inputs; never a limit."""
    spent = {value.asset: value.amount for value in payload.spent}
    for value in inputs:
        if value.asset not in spent:
            raise ValueError("GMX modeled input is outside its configured input assets")
        spent[value.asset] += value.amount
    return tuple(AssetAmount(asset=asset, amount=amount) for asset, amount in spent.items())


def allowances_after_acceptance(payload, inputs):
    """Live planning approves each input for exactly its request, which escrow consumes."""
    used = {value.asset for value in inputs}
    return tuple(
        value.model_copy(update={"amount": 0}) if value.asset in used else value
        for value in payload.allowances
    )


def apply_fund_gas(before, after, gas):
    if not gas.amount or before.payer != before.target.fund:
        return after
    native = gas.asset
    balance = next(value.amount for value in before.wallets if value.asset == native)
    owned = next(value.amount for value in before.owned.inventory if value.asset == native)
    if owned > balance or gas.amount > balance:
        raise ValueError("GMX fund gas charge exceeds measured native cash or ownership")
    charge = max(0, gas.amount - (balance - owned))
    # Asset effects charge the fund exactly once. This mirrors the owned part after any
    # unallocated native reserve pays first, including a composed full-cash projection.
    changes = (AssetDelta(key=wallet(after.target, native), amount=-charge),)
    return after.model_copy(update={"owned": inventory_after(after.owned, changes)})


def submitter_cost(settings, target, phase):
    from almanak.gateway.evm import native_asset

    asset = native_asset(target.fund.chain.chain_id)
    if settings.costs.kind == "excluded":
        return AssetAmount(asset=asset, amount=0)
    selected = next(value for value in settings.costs.chains if value.chain == asset.chain)
    return getattr(selected, phase)


def effects(settings, held, after, fund_changes, gas):
    target = held.payload.target
    movements = tuple(value for value in fund_changes if value.amount)
    if gas.amount:
        movements += (
            AssetDelta(key=payer_wallet(settings, target, gas.asset), amount=-gas.amount),
        )
    return Effects[Payload](
        assets=movements,
        positions=(
            PositionChange[Payload](
                before=held, after=Position[Payload](key=held.key, payload=after)
            ),
        ),
    )


def validate_inventory(portfolio):
    allocated: dict[WalletAsset, int] = {}
    for position in portfolio.positions:
        payload = position.payload
        for value in owned_assets(payload.owned):
            key = wallet(payload.target, value.asset)
            allocated[key] = allocated.get(key, 0) + value.amount
    if any(amount > portfolio.balance(key) for key, amount in allocated.items()):
        raise ValueError("GMX aggregate allocation ownership exceeds explicit fund inventory")
