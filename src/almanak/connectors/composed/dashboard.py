"""Shared fund cash has one value even when several protocol positions use it."""

from almanak.configuration.composed import Parameters
from almanak.connectors.aave_v3.dashboard import position_values as lending_values
from almanak.connectors.aave_v3.observations import Ownership as LendingOwnership
from almanak.connectors.aave_v4.models import Ownership as AaveV4Ownership
from almanak.connectors.composed.aave_v4_reporting import position_values as aave_v4_values
from almanak.connectors.composed.inventory import observed_cash
from almanak.connectors.composed.models import wallet_key
from almanak.connectors.composed.reporting import submitted_state
from almanak.connectors.compound_v3.dashboard import position_values as compound_values
from almanak.connectors.compound_v3.observations import Ownership as CompoundOwnership
from almanak.connectors.curve.dashboard import share_value
from almanak.connectors.curve.positions import Ownership as CurveOwnership
from almanak.connectors.dex_reporting import observations
from almanak.connectors.morpho_blue.dashboard import position_values as morpho_values
from almanak.connectors.morpho_blue.observations import Ownership as MorphoOwnership
from almanak.connectors.sparklend.dashboard import position_values as spark_values
from almanak.connectors.sparklend.observations import Ownership as SparkOwnership
from almanak.connectors.uniswap_v3.dashboard import payer_values, validate_projection
from almanak.dashboard.models import (
    Context,
    ExchangePosition,
    Position,
    Ref,
    Scope,
    Value,
)
from almanak.dashboard.projection import snapshot
from almanak.dashboard.repayment import repayment_datasets
from almanak.dashboard.tables import (
    several_accounts,
    symbol_of,
    token_dataset,
    transactions_dataset,
)
from almanak.effects import WalletAsset
from almanak.models import Block


def latest_cash(state):
    observations = (*state.baseline, *(o.after for o in state.outcomes), *state.closure)
    result: dict[WalletAsset, Block] = {}
    for observed in observations:
        for asset, _ in observed_cash(observed):
            key = wallet_key(observed.target, asset)
            if key not in result or observed.block.number >= result[key].number:
                result[key] = observed.block
    return result


def cash_values(state):
    evidence = latest_cash(state)
    result = {}
    for index, balance in enumerate(state.inventory):
        key = balance.key
        binding = next(b for b in state.config.accounts.bindings if b.name == key.account)
        metadata = state.config.asset_metadata.get(key.asset)
        result[key] = Value(
            id=f"fund-cash:{index}",
            amount=str(balance.amount),
            unit="base_units",
            asset=key.asset,
            decimals=metadata.decimals if metadata else None,
            symbol=symbol_of(state, key.asset),
            scope=Scope(account=key.account, fund=binding.profile.fund),
            evidence="recorded" if key in evidence else "configured",
            block=evidence.get(key),
            reason="Shared runtime-owned fund cash. Protocol positions reference this balance; "
            "they do not own additional copies."
            if key in evidence
            else "Explicit initial inventory; fund balance has not been observed.",
        )
    return result


def add_values(values, additions):
    if values.keys() & additions.keys():
        raise ValueError("composed dashboard value identifiers collide")
    values.update(additions)


def positions(state, cash, values):
    result: list[Position | ExchangePosition] = []
    for owned in state.owned:
        target = owned.target
        if isinstance(
            owned,
            (LendingOwnership, AaveV4Ownership, SparkOwnership, MorphoOwnership, CompoundOwnership),
        ):
            additions: dict[str, Value] = {}
            project = {
                LendingOwnership: lending_values,
                AaveV4Ownership: aave_v4_values,
                SparkOwnership: spark_values,
                MorphoOwnership: morpho_values,
                CompoundOwnership: compound_values,
            }[type(owned)]
            from almanak.connectors.composed.lifecycle import component_state
            from almanak.connectors.target_routing import target_connector

            result.append(
                project(component_state(state, target_connector(target)), owned, additions)
            )
            add_values(values, additions)
        else:
            result.append(
                ExchangePosition(
                    allocation=target.allocation,
                    target=target,
                    inventory=tuple(cash[wallet_key(target, asset)] for asset in target.assets),
                )
            )
    return tuple(result)


def cash_dataset(state, cash):
    return token_dataset(
        tuple(
            {
                "account": key.account,
                "token": symbol_of(state, key.asset) or str(key.asset),
                "amount": Ref(ref=value.id),
            }
            for key, value in cash.items()
        ),
        "One balance per named fund, chain and asset. Lending collateral, debt and "
        "LP principal are separate from this liquid inventory.",
        position=False,
        keep=several_accounts(state),
    )


LENDING_MODULES = ("aave_v3", "aave_v4", "sparklend", "compound_v3", "morpho_blue", "pool_lending")


def describe(state, outcome):
    """Each component's transactions in its own terms."""
    from almanak.connectors.dex_dashboard import operation_label
    from almanak.connectors.gmx_v2.dashboard import gmx_label
    from almanak.connectors.lending_tables import lending_label

    module = type(outcome.operation).__module__
    if "gmx_v2" in module:
        return gmx_label(state, outcome)
    if any(name in module for name in LENDING_MODULES):
        return lending_label(state, outcome.operation)
    return operation_label(state, outcome.operation)


def share_dataset(state, values):
    rows = []
    for owned in state.owned:
        if not isinstance(owned, CurveOwnership):
            continue
        history = observations(state, owned.target)
        additions: dict[str, Value] = {}
        rows.append(share_value(state, owned, history[-1] if history else None, additions))
        add_values(values, additions)
    return token_dataset(
        rows,
        "Fund-owned Curve LP shares, separate from shared liquid cash.",
        keep=several_accounts(state),
    )


def aave_v4_debts(state, values):
    from almanak.connectors.composed.lifecycle import component_state
    from almanak.connectors.identifiers import ConnectorId
    from almanak.dashboard.debt_projection import debt_dataset

    identity = ConnectorId("aave_v4")
    if identity not in state.config.parameter_components:
        return {}
    view = component_state(state, identity)
    return {"aave_v4_debts": debt_dataset(view, values)}


def gmx_datasets(state):
    from almanak.connectors.composed.lifecycle import component_state
    from almanak.connectors.gmx_v2.dashboard import (
        position_dataset,
        request_dataset,
        shares_dataset,
    )
    from almanak.connectors.identifiers import ConnectorId

    identity = ConnectorId("gmx_v2")
    if identity not in state.config.parameter_components:
        return {}
    view = component_state(state, identity)
    shares = shares_dataset(view)
    return {
        "gmx_positions": position_dataset(view),
        "gmx_requests": request_dataset(view),
        "gmx_shares": shares.model_copy(
            update={
                "coverage": shares.coverage
                + " These shares also appear in shared fund cash; they are the same holding."
            }
        ),
    }


def project(state, *, captured_at=None):
    validate_projection(state)
    cash = cash_values(state)
    values = {value.id: value for value in cash.values()}
    projected_positions = positions(state, cash, values)
    costs: dict[str, Value] = {}
    payer_dataset = payer_values(submitted_state(state), costs)
    add_values(values, costs)
    receipts: dict[str, Value] = {}
    transactions = transactions_dataset(submitted_state(state), receipts, describe)
    add_values(values, receipts)
    shares = share_dataset(state, values)
    context = Context(
        config=Parameters.model_validate(
            {name: getattr(state.config, name) for name in Parameters.model_fields}
        ),
        memory=state.memory,
        positions=projected_positions,
        values=values,
        datasets={
            "fund_inventory": cash_dataset(state, cash),
            "payer_costs": payer_dataset,
            "transactions": transactions,
            "curve_pool_shares": shares,
            **aave_v4_debts(state, values),
            **gmx_datasets(state),
            **repayment_datasets(state, values),
        },
    )
    return context, snapshot(state, values, captured_at)
