"""Aave entry inventory and pristine position requirements."""

from almanak.connectors.aave_v4.models import DebtAllocation
from almanak.connectors.funding import funding_rows, lifecycle_entries, summed
from almanak.connectors.gas_lifecycle import BORROW_EXIT, MINIMUM_EXIT, GasLifecycle, KindCounts
from almanak.models import AssetAmount


def requirements(config, account):
    """Each supply allocation's capital. Interest is not prefunded; repayment refuses
    with its shortfall."""
    capital = (
        AssetAmount(asset=allocation.reserve.asset, amount=allocation.capital)
        for allocation in config.allocations
        if allocation.account == account
    )
    return summed(capital, "configured allocation capital")


# A supply approves the Spoke and supplies; a debt enables each named collateral
# and borrows. The debt's approval is for repayment and adds no exposure. A debt
# exits by approving, repaying the whole debt, revoking the residual allowance and
# disabling each collateral it enabled (reader.plan_repayment). Aave V4 plans no
# partial withdrawal or repayment, so it unwinds in no cycles.
GAS_LIFECYCLE = GasLifecycle(
    kinds={
        "lending": KindCounts(entry=2, exit=MINIMUM_EXIT),
        "debt": KindCounts(
            entry=1,
            exit=MINIMUM_EXIT + BORROW_EXIT,
            per_item={"collateral": 1},
            exit_per_item={"collateral": 1},
        ),
    }
)
entry_transactions = lifecycle_entries(
    GAS_LIFECYCLE,
    lambda p: (
        ("debt", {"items": {"collateral": len(p.collateral)}})
        if isinstance(p, DebtAllocation)
        else ("lending", {})
    ),
)


def token_requirements(config, account, observations):
    measured = {o.target.asset: o.wallet_balance for o in observations}
    return list(funding_rows(requirements(config, account), measured))


def pristine_position(item):
    return not any(
        (
            item.allowance,
            item.position.supplied_shares,
            item.position.total_debt,
            item.drawn_shares,
            item.premium_shares,
            item.premium_offset_ray,
            item.collateral_enabled,
        )
    )


def preflight(config, account, observations):
    # Composed callers scope observations by connector only, so bind them to the account here.
    observed = tuple(o for o in observations if o.target.account == account)
    return token_requirements(config, account, observed), bool(observed) and all(
        pristine_position(o) for o in observed
    )
