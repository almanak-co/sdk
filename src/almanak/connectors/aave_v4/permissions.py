"""Derive Aave permissions from economic capabilities, including safe closure."""

from almanak.connectors.aave_v4.models import AaveCapability, BorrowCapability, SupplyCapability
from almanak.gateway.evm import erc20_address
from almanak.gateway.roles_conditions import (
    FunctionGrant,
    any_amount,
    combine,
    either,
    equal,
    matches,
    positive,
)
from almanak.models import SafeRoles


def operation_grant(capability: AaveCapability, operation: str, amount, explanation):
    target = capability.target
    return FunctionGrant(
        target=target.reserve.spoke,
        signature=operation + "(uint256,uint256,address)",
        condition=matches(
            equal("uint256", target.reserve.reserve_id),
            amount,
            equal("address", target.fund.address),
        ),
        explanation=(explanation,),
    )


def approval_grant(capability: AaveCapability) -> FunctionGrant:
    # The Spoke pulls only from its caller, so an approval is spendable only through
    # this Safe's own supply and repay (clamped to its own debt) calls.
    reserve = capability.target.reserve
    return FunctionGrant(
        target=erc20_address(reserve.asset, reserve.chain.chain_id),
        signature="approve(address,uint256)",
        condition=matches(
            equal("address", reserve.spoke),
            any_amount(),
        ),
        explanation=(
            f"{capability.target.allocation}: approve only {reserve.asset} to "
            f"Spoke {reserve.spoke}, any amount or zero cleanup.",
        ),
    )


def collateral_grants(capability: BorrowCapability) -> list[FunctionGrant]:
    return [
        FunctionGrant(
            target=collateral.reserve.spoke,
            signature="setUsingAsCollateral(uint256,bool,address)",
            condition=matches(
                equal("uint256", collateral.reserve.reserve_id),
                either(equal("bool", True), equal("bool", False)),
                equal("address", collateral.fund.address),
            ),
            explanation=(f"{collateral.allocation}: enable or disable collateral for its Safe.",),
        )
        for collateral in capability.collateral
    ]


def derive_aave(profile: SafeRoles, requirements: tuple[AaveCapability, ...]):
    """Scope-only grants: exact Spoke, reserve and Safe, with any nonzero amount."""
    grants: list[FunctionGrant] = []
    for capability in requirements:
        if capability.target.fund != profile.fund:
            raise ValueError("permission partition contains another fund/chain/environment")
        operation = "supply" if isinstance(capability, SupplyCapability) else "borrow"
        grants.append(
            operation_grant(
                capability,
                operation,
                positive(),
                f"{capability.target.allocation}: {operation} only its exact reserve "
                "for this Safe.",
            )
        )
        grants.extend(closure_grants(capability))
    return combine(grants)


def closure_grants(capability: AaveCapability) -> list[FunctionGrant]:
    if isinstance(capability, SupplyCapability):
        return [
            approval_grant(capability),
            operation_grant(
                capability,
                "withdraw",
                equal("uint256", 2**256 - 1),
                f"{capability.target.allocation}: full withdrawal to its Safe.",
            ),
        ]
    return [
        approval_grant(capability),
        operation_grant(
            capability,
            "repay",
            positive(),
            f"{capability.target.allocation}: repay only its Safe's debt, any amount; "
            "the Spoke clamps a repayment to the debt.",
        ),
        *collateral_grants(capability),
    ]
