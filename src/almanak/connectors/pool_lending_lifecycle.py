"""Shared ownership checks for concrete compatible Pool checkpoints."""

from almanak.caip import AssetType
from almanak.connectors.closure_obligations import obligation, require_closed
from almanak.gateway.ethereum import DataError
from almanak.models import Account, Block


def validate_ownership(owned, before):
    actual = before.position
    if (owned.target, owned.supply_scaled, owned.debt_scaled, owned.collateral_enabled) != (
        before.target,
        actual.supplied_scaled,
        actual.debt_scaled,
        actual.collateral_enabled,
    ):
        raise DataError("ownership_mismatch", "observed Pool position differs from owned exposure")


def validate_owned(state, observations):
    if tuple(o.target for o in observations) != tuple(o.target for o in state.owned):
        raise DataError(
            "ownership_mismatch", "observations do not cover the configured Pool positions"
        )
    for owned, observed in zip(state.owned, observations, strict=True):
        validate_ownership(owned, observed)


def validate_initial(state, observations):
    validate_owned(state, observations)
    if any(o.allowance or o.position.user_configuration for o in observations):
        raise DataError(
            "existing_exposure", "new deployment cannot adopt Pool exposure or allowances"
        )
    if any(o.pool_risk is None or o.pool_risk.emode_category != 0 for o in observations):
        raise DataError("risk_unavailable", "new deployment requires observed ordinary Pool mode")
    balances: dict[tuple[Account, AssetType], tuple[Block, int]] = {}
    for observed in observations:
        key = observed.target.fund, observed.target.asset
        evidence = observed.block, observed.wallet_balance
        if balances.setdefault(key, evidence) != evidence:
            raise DataError("inconsistent", "shared fund inventory observations disagree")


def commit_owned(owned, outcome):
    if not outcome.success:
        return owned
    after = outcome.after.position
    return tuple(
        type(item)(
            target=item.target,
            supply_scaled=after.supplied_scaled,
            debt_scaled=after.debt_scaled,
            collateral_enabled=after.collateral_enabled,
        )
        if item.target == outcome.operation.target
        else item
        for item in owned
    )


def validate_closure(state, observations):
    validate_owned(state, observations)
    require_closed(
        "Pool teardown retains supply, debt, collateral or authority",
        (
            obligation(
                observed,
                {
                    "supply_scaled": owned.supply_scaled,
                    "debt_scaled": owned.debt_scaled,
                    "collateral_enabled": owned.collateral_enabled,
                    "allowance": observed.allowance,
                    "user_configuration": observed.position.user_configuration,
                },
                {
                    "supply": observed.position.supplied_assets,
                    "debt": observed.position.debt_assets,
                },
            )
            for owned, observed in zip(state.owned, observations, strict=True)
        ),
    )
