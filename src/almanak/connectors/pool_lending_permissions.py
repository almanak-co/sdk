"""Built-in Roles conditions for compatible Pool actions: scope only, never an amount bound."""

from almanak.gateway.roles_conditions import (
    FunctionGrant,
    any_amount,
    combine,
    either,
    equal,
    matches,
    positive,
)


def entry_grant(capability, operation):
    target = capability.target
    args = (
        (
            equal("address", target.asset.reference),
            positive(),
            equal("address", target.fund.address),
            equal("uint16", 0),
        )
        if operation == "supply"
        else (
            equal("address", target.asset.reference),
            positive(),
            equal("uint256", 2),
            equal("uint16", 0),
            equal("address", target.fund.address),
        )
    )
    signature = (
        "supply(address,uint256,address,uint16)"
        if operation == "supply"
        else "borrow(address,uint256,uint256,uint16,address)"
    )
    return FunctionGrant(
        target=target.reserve.pool,
        signature=signature,
        condition=matches(*args),
        explanation=(f"{target.allocation}: {operation} only {target.asset} for this Safe.",),
    )


def supply_exits(capability):
    target = capability.target
    enabled = (
        either(equal("bool", True), equal("bool", False))
        if capability.collateral
        else equal("bool", False)
    )
    return [
        FunctionGrant(
            target=target.reserve.pool,
            signature="withdraw(address,uint256,address)",
            condition=matches(
                equal("address", target.asset.reference),
                positive(),
                equal("address", target.fund.address),
            ),
            explanation=(
                f"{target.allocation}: partial or full withdrawal of the Safe's own "
                "reserve balance to itself.",
            ),
        ),
        FunctionGrant(
            target=target.reserve.pool,
            signature="setUserUseReserveAsCollateral(address,bool)",
            condition=matches(equal("address", target.asset.reference), enabled),
            explanation=(
                f"{target.allocation}: configured collateral control on the Safe's own supply.",
            ),
        ),
    ]


def closure_grants(capability):
    target = capability.target
    # The Pool pulls only from its caller, so an approval is spendable only through
    # this Safe's own supply and repay (bounded by its own debt) calls.
    grants = [
        FunctionGrant(
            target=target.asset.reference,
            signature="approve(address,uint256)",
            condition=matches(
                equal("address", target.reserve.pool),
                any_amount(),
            ),
            explanation=(f"{target.allocation}: approve only its Pool, any amount, or revoke.",),
        )
    ]
    grants.extend(supply_exits(capability))
    if capability.borrow:
        grants.append(
            FunctionGrant(
                target=target.reserve.pool,
                signature="repay(address,uint256,uint256,address)",
                condition=matches(
                    equal("address", target.asset.reference),
                    positive(),
                    equal("uint256", 2),
                    equal("address", target.fund.address),
                ),
                explanation=(
                    f"{target.allocation}: repay only this Safe's variable debt, any amount; "
                    "the Pool caps a repayment at the debt.",
                ),
            )
        )
    return grants


def derive(profile, capabilities):
    """Supply and withdraw always; collateral control when configured; borrow and repay
    only for an allocation that borrows. No grant carries an amount bound."""
    grants = []
    for capability in capabilities.requirements:
        if capability.target.fund != profile.fund:
            continue
        grants.append(entry_grant(capability, "supply"))
        if capability.borrow:
            grants.append(entry_grant(capability, "borrow"))
        grants.extend(closure_grants(capability))
    return combine(grants)
