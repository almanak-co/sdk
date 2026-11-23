"""One fund inventory, projected into connector views without allocating cash twice."""

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.aave_v3.observations import Ownership as LendingOwnership
from almanak.connectors.aave_v3.observations import Snapshot as LendingSnapshot
from almanak.connectors.aave_v4.models import LendingSnapshot as AaveV4Snapshot
from almanak.connectors.aave_v4.models import Ownership as AaveV4Ownership
from almanak.connectors.aave_v4.models import SupplyTarget as AaveV4Target
from almanak.connectors.across.domain import Target as AcrossTarget
from almanak.connectors.across.observations import Ownership as AcrossOwnership
from almanak.connectors.across.observations import Snapshot as AcrossSnapshot
from almanak.connectors.aerodrome.models import Target as AerodromeTarget
from almanak.connectors.aerodrome.observations import Snapshot as AerodromeSnapshot
from almanak.connectors.aerodrome.positions import Ownership as AerodromeOwnership
from almanak.connectors.aerodrome_slipstream.models import Target as SlipstreamTarget
from almanak.connectors.aerodrome_slipstream.observations import Snapshot as SlipstreamSnapshot
from almanak.connectors.aerodrome_slipstream.positions import Ownership as SlipstreamOwnership
from almanak.connectors.composed.models import validate_inventory, wallet_key
from almanak.connectors.compound_v3.domain import Target as CompoundTarget
from almanak.connectors.compound_v3.observations import Ownership as CompoundOwnership
from almanak.connectors.compound_v3.observations import Snapshot as CompoundSnapshot
from almanak.connectors.curve.models import Target as CurveTarget
from almanak.connectors.curve.observations import Snapshot as CurveSnapshot
from almanak.connectors.curve.positions import Ownership as CurveOwnership
from almanak.connectors.gmx_v2.domain import Target as GmxPositionTarget
from almanak.connectors.gmx_v2.economic_perpetual_state import PerpetualOwnership
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget as GmxExchangeTarget
from almanak.connectors.gmx_v2.exchange_observations import ExchangeOwnership as GmxOwnership
from almanak.connectors.gmx_v2.exchange_observations import ExchangeSnapshot as GmxSnapshot
from almanak.connectors.gmx_v2.exchange_observations import inventory_assets as gmx_assets
from almanak.connectors.gmx_v2.observations import Snapshot as GmxPositionSnapshot
from almanak.connectors.gmx_v2.positions import Ownership as GmxPositionOwnership
from almanak.connectors.morpho_blue.domain import Target as MorphoTarget
from almanak.connectors.morpho_blue.observations import Ownership as MorphoOwnership
from almanak.connectors.morpho_blue.observations import Snapshot as MorphoSnapshot
from almanak.connectors.pancakeswap_v3.models import Target as PancakeTarget
from almanak.connectors.pancakeswap_v3.observations import Snapshot as PancakeSnapshot
from almanak.connectors.pancakeswap_v3.positions import Ownership as PancakeOwnership
from almanak.connectors.sparklend.domain import Target as SparkTarget
from almanak.connectors.sparklend.observations import Ownership as SparkOwnership
from almanak.connectors.sparklend.observations import Snapshot as SparkSnapshot
from almanak.connectors.target_routing import connector_for_target
from almanak.connectors.uniswap_v3.models import Target as ExchangeTarget
from almanak.connectors.uniswap_v3.observations import Snapshot as ExchangeSnapshot
from almanak.connectors.uniswap_v3.positions import Ownership as ExchangeOwnership
from almanak.connectors.uniswap_v4.models import Target as V4Target
from almanak.connectors.uniswap_v4.state import Ownership as V4Ownership
from almanak.connectors.uniswap_v4.state import Snapshot as V4Snapshot
from almanak.effects import AssetDelta, Balance, WalletAsset, apply_assets
from almanak.models import AssetAmount, Block

LENDING = (LendingOwnership, AaveV4Ownership, SparkOwnership, MorphoOwnership, CompoundOwnership)


def currency_views(target, balances):
    amounts = {balance.key: balance.amount for balance in balances}
    return tuple(
        AssetAmount(asset=asset, amount=amounts[wallet_key(target, asset)])
        for asset in target.assets
    )


def refresh(owned, balances):
    result = []
    for position in owned:
        if isinstance(
            position,
            (
                ExchangeOwnership,
                V4Ownership,
                PancakeOwnership,
                AerodromeOwnership,
                SlipstreamOwnership,
            ),
        ):
            first, second = currency_views(position.target, balances)
            position = position.model_copy(update={"inventory0": first, "inventory1": second})
        elif isinstance(position, CurveOwnership):
            position = position.model_copy(
                update={"inventory": AssetAmounts(currency_views(position.target, balances))}
            )
        elif isinstance(position, (GmxOwnership, GmxPositionOwnership, PerpetualOwnership)):
            position = refresh_gmx(position, balances)
        result.append(position)
    return tuple(result)


def project_cash(owned, balances, transfers=()):
    """Lending and bridge records with the fund's owned cash of their asset, and a
    bridge's transfers in flight, for one decision only; `validate` refuses a retained
    record that carries either."""
    amounts = {balance.key: balance.amount for balance in balances}
    return tuple(decision_view(position, amounts, transfers) for position in owned)


def decision_view(position, amounts, transfers):
    if not isinstance(position, (*LENDING, AcrossOwnership)):
        return position
    target = position.target
    # A chain's view holds only its own wallets: a bridge seen from its destination
    # chain states no source cash.
    cash = amounts.get(wallet_key(target, target.asset))
    update: dict = {
        "inventory": None if cash is None else AssetAmount(asset=target.asset, amount=cash)
    }
    if isinstance(position, AcrossOwnership):
        update["in_flight"] = tuple(
            t for t in transfers if t.allocation == target.allocation and t.resolution is None
        )
    return position.model_copy(update=update)


def refresh_gmx(position, balances):
    amounts = {balance.key: balance.amount for balance in balances}
    target = position.target
    assets = gmx_assets(target) if isinstance(position, GmxOwnership) else target.assets
    update: dict[str, AssetAmount | tuple[AssetAmount, ...]] = {
        "inventory": tuple(
            AssetAmount(asset=asset, amount=amounts[wallet_key(target, asset)]) for asset in assets
        )
    }
    if isinstance(position, GmxOwnership):
        update["market_tokens"] = AssetAmount(
            asset=target.market.share_asset,
            amount=amounts[wallet_key(target, target.market.share_asset)],
        )
    return position.model_copy(update=update)


def exchange_owned(target, balances):
    first, second = currency_views(target, balances)
    if isinstance(target, V4Target):
        return V4Ownership(target=target, inventory0=first, inventory1=second)
    if isinstance(target, PancakeTarget):
        return PancakeOwnership(target=target, inventory0=first, inventory1=second)
    if isinstance(target, AerodromeTarget):
        return AerodromeOwnership(target=target, inventory0=first, inventory1=second)
    if isinstance(target, SlipstreamTarget):
        return SlipstreamOwnership(target=target, inventory0=first, inventory1=second)
    return ExchangeOwnership(target=target, inventory0=first, inventory1=second)


def initial_owned(config):
    result: list[
        LendingOwnership
        | AaveV4Ownership
        | SparkOwnership
        | V4Ownership
        | ExchangeOwnership
        | PancakeOwnership
        | AerodromeOwnership
        | SlipstreamOwnership
        | MorphoOwnership
        | CompoundOwnership
        | CurveOwnership
        | GmxOwnership
        | GmxPositionOwnership
        | AcrossOwnership
    ] = []
    for allocation in config.positions:
        target = config.target(allocation.name)
        if isinstance(
            target, (ExchangeTarget, V4Target, PancakeTarget, AerodromeTarget, SlipstreamTarget)
        ):
            result.append(exchange_owned(target, config.inventory))
        elif isinstance(target, CurveTarget):
            result.append(
                CurveOwnership(
                    target=target, inventory=AssetAmounts(currency_views(target, config.inventory))
                )
            )
        elif isinstance(target, (GmxExchangeTarget, GmxPositionTarget)):
            from almanak.connectors.composed.models import component_for_allocation
            from almanak.connectors.gmx_v2.observations import initial_owned as gmx_initial

            component = component_for_allocation(config, allocation.name)
            result.append(refresh_gmx(gmx_initial(component, allocation.name), config.inventory))
        elif isinstance(target, AaveV4Target):
            result.append(AaveV4Ownership(target=target, shares=0))
        elif isinstance(target, MorphoTarget):
            result.append(
                MorphoOwnership(target=target, supply_shares=0, borrow_shares=0, collateral=0)
            )
        elif isinstance(target, CompoundTarget):
            result.append(CompoundOwnership(target=target, principal=0, collateral=0))
        elif isinstance(target, AcrossTarget):
            result.append(AcrossOwnership(target=target))
        else:
            result.append(
                (SparkOwnership if isinstance(target, SparkTarget) else LendingOwnership)(
                    target=target, supply_scaled=0, debt_scaled=0, collateral_enabled=False
                )
            )
    return tuple(result)


def validate(state):
    validate_inventory(state.config, state.inventory)
    keys = [balance.key for balance in state.top_ups]
    if len(set(keys)) != len(keys) or not set(keys) <= {b.key for b in state.inventory}:
        raise ValueError("absorbed top-ups must name unique shared fund inventory keys")
    if refresh(state.owned, state.inventory) != state.owned:
        raise ValueError("connector currency views differ from shared fund inventory")
    if any(isinstance(p, LENDING) and p.inventory is not None for p in state.owned):
        raise ValueError("retained lending ownership cannot carry projected fund cash")
    if any(
        isinstance(p, AcrossOwnership) and (p.inventory is not None or p.in_flight)
        for p in state.owned
    ):
        raise ValueError("retained bridge ownership cannot carry projected cash or transfers")


def merged_transfers(retained, identity, component):
    """Put one component's transfers back among the rest, each in its retained place."""
    changed = {t.identity: t for t in component}
    kept = tuple(changed.pop(t.identity) if t.connector == identity else t for t in retained)
    return (*kept, *changed.values())


def unchanged_ownership(state):
    """The owned fields with no outcome. A page recorded without one can still prove a
    bridge transfer unfilled, which only its own connector decides."""
    from almanak.connectors.composed.lifecycle import component_state

    transfers = state.transfers
    for identity in dict.fromkeys(t.connector for t in transfers):
        native = connector_from_identity(identity).lifecycle.ownership(
            component_state(state, identity)
        )
        transfers = merged_transfers(transfers, identity, native.get("transfers", ()))
    return {
        "owned": state.owned,
        "inventory": state.inventory,
        "requests": state.requests,
        "top_ups": state.top_ups,
        "transfers": transfers,
    }


def connector_from_identity(identity):
    from almanak.connectors.composition import connector_from_wire

    return connector_from_wire({"connector": str(identity)})


def ownership(state, outcome=None):
    if outcome is None:
        return unchanged_ownership(state)
    from almanak.connectors.composed.lifecycle import component_state, outcome_target
    from almanak.connectors.target_routing import target_connector

    operation = getattr(outcome, "operation", None)
    target = operation.target if operation is not None else outcome_target(outcome)
    identity = target_connector(target)
    binding = connector_for_target(target)
    if binding.execution_effects is None:
        raise ValueError("connector has no verified execution cash effects")
    effects = binding.execution_effects(outcome)
    top_up = repayment_top_up(state.inventory, outcome, effects)
    inventory = apply_assets(
        state.inventory,
        (*top_up, *effects, *owned_gas_effects(state.inventory, target, outcome)),
    )
    native = binding.lifecycle.ownership(component_state(state, identity), outcome)
    changed = {position.target: position for position in native["owned"]}
    owned = tuple(changed.get(position.target, position) for position in state.owned)
    passed = {f: value for f, value in native.items() if f not in ("owned", "transfers")}
    transfers = (
        merged_transfers(state.transfers, identity, native["transfers"])
        if "transfers" in native
        else state.transfers
    )
    return {
        "requests": state.requests,
        **passed,
        "owned": refresh(owned, inventory),
        "inventory": inventory,
        "top_ups": apply_top_ups(state.top_ups, top_up),
        "transfers": transfers,
    }


def apply_top_ups(retained, absorbed):
    """Cumulative outside loan asset each wallet key absorbed, so the inventory stays
    the configured inventory plus recorded effects plus these amounts."""
    amounts = {balance.key: balance.amount for balance in retained}
    for delta in absorbed:
        amounts[delta.key] = amounts.get(delta.key, 0) + delta.amount
    return tuple(Balance(key=key, amount=amount) for key, amount in amounts.items())


def repayment_top_up(inventory, outcome, effects):
    """Absorb into the inventory the loan asset a full repay spent beyond it.

    Only a full repay may spend the fund's unallocated loan asset (an operator
    top-up), so it alone credits that surplus before its receipt debit, and only
    as much as the receipt proves left the wallet.
    """
    from almanak.connectors.composed.execution import full_repayment

    operation = getattr(outcome, "operation", None)
    if operation is None or not full_repayment(operation) or not outcome.success:
        return ()
    target = operation.target
    key = wallet_key(target, target.asset)
    owned = next((balance.amount for balance in inventory if balance.key == key), 0)
    spent = -sum(delta.amount for delta in effects if delta.key == key)
    return (AssetDelta(key=key, amount=spent - owned),) if spent > owned else ()


def owned_gas_effects(inventory, target, outcome):
    gas = getattr(outcome, "gas_paid", None)
    before = getattr(outcome, "before", None)
    # A settlement the deployment did not submit has no payer of its own to charge.
    if gas is None or before is None or not gas.amount or before.payer != target.fund:
        return ()
    key = wallet_key(target, gas.asset)
    owned = next((balance.amount for balance in inventory if balance.key == key), 0)
    measured = before.native_balance
    if measured.asset != gas.asset or owned > measured.amount or gas.amount > measured.amount:
        raise ValueError("fund gas charge requires sufficient measured native cash")
    # Unallocated gas reserves pay first. Keeper outcomes have no submitter charge.
    charge = max(0, gas.amount - (measured.amount - owned))
    return (AssetDelta(key=key, amount=-charge),) if charge else ()


def observed_cash(observation):
    if isinstance(
        observation,
        (
            LendingSnapshot,
            AaveV4Snapshot,
            SparkSnapshot,
            MorphoSnapshot,
            CompoundSnapshot,
            AcrossSnapshot,
        ),
    ):
        return ((observation.target.asset, observation.wallet_balance),)
    return tuple((value.asset, value.amount) for value in observed_wallets(observation))


def observed_wallets(observation):
    if isinstance(
        observation,
        (ExchangeSnapshot, V4Snapshot, PancakeSnapshot, AerodromeSnapshot, SlipstreamSnapshot),
    ):
        return observation.wallet0, observation.wallet1
    if isinstance(observation, CurveSnapshot):
        return observation.wallets.root
    if isinstance(observation, GmxSnapshot):
        return (*observation.wallets, observation.market_tokens)
    if isinstance(observation, GmxPositionSnapshot):
        return observation.wallets
    raise ValueError("shared inventory requires a trusted connector observation")


def cash_evidence(observations):
    evidence: dict[WalletAsset, tuple[Block, int]] = {}
    for observation in observations:
        for asset, amount in observed_cash(observation):
            key = wallet_key(observation.target, asset)
            measured = observation.block, amount
            if evidence.setdefault(key, measured) != measured:
                raise ValueError("shared fund observations disagree on block or wallet balance")
    return evidence


def validate_observed(state, observations):
    evidence = cash_evidence(observations)
    for balance in state.inventory:
        if balance.key not in evidence:
            raise ValueError("shared fund inventory lacks a measured wallet balance")
        if evidence[balance.key][1] < balance.amount:
            raise ValueError("shared fund inventory exceeds its measured wallet balance")
