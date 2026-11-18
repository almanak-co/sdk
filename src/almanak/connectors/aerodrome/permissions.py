"""Scope-only Roles conditions bind swaps and fungible LP actions to the exact configured pool."""

from eth_abi import encode

from almanak.connectors.aerodrome.authority import capability_codec
from almanak.connectors.aerodrome.models import Capabilities
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import (
    Condition,
    FunctionGrant,
    bounded,
    combine,
    comparison,
    either,
    equal,
    matches,
    positive,
)

MAXIMUM = 2**128 - 1


def finite_shares():
    return Condition(
        parameter=0,
        operator=1,
        children=(
            comparison(17, encode(("uint256",), (0,))),
            comparison(18, encode(("uint256",), (2**256 - 1,))),
        ),
    )


def nonnegative():
    return either(equal("uint256", 0), finite_shares())


def approval_grant(target, asset, limit):
    return FunctionGrant(
        target=asset.reference,
        signature="approve(address,uint256)",
        condition=matches(
            equal("address", target.contracts.router), either(equal("uint256", 0), limit)
        ),
        explanation=("Allowance to the configured Aerodrome router only, including zero cleanup.",),
    )


def swap_grant(capability, direction):
    target = capability.target
    route = matches(
        equal("address", target.assets[direction].reference),
        equal("address", target.assets[1 - direction].reference),
        equal("bool", target.pool.stable),
        equal("address", target.pool.factory),
        parameter=3,
    )
    return FunctionGrant(
        target=target.contracts.router,
        signature="swapExactTokensForTokens(uint256,uint256,(address,address,bool,address)[],address,uint256)",
        condition=matches(
            bounded(MAXIMUM),
            finite_shares(),
            matches(route, parameter=4),
            equal("address", target.fund.address),
            bounded(MAXIMUM),
        ),
        explanation=(
            "One stable/volatile route through this pool paying the Safe, any nonzero "
            "uint128 input.",
        ),
    )


def pool_arguments(target):
    return (
        equal("address", target.pool.currency0.reference),
        equal("address", target.pool.currency1.reference),
        equal("bool", target.pool.stable),
    )


def addition_grant(target):
    return FunctionGrant(
        target=target.contracts.router,
        signature="addLiquidity(address,address,bool,uint256,uint256,uint256,uint256,address,uint256)",
        condition=matches(
            *pool_arguments(target),
            bounded(MAXIMUM),
            bounded(MAXIMUM),
            nonnegative(),
            nonnegative(),
            equal("address", target.fund.address),
            bounded(MAXIMUM),
        ),
        explanation=(
            "Deposit only the configured pair and invariant, any nonzero uint128 desired "
            "inputs; "
            "LP shares pay the Safe.",
        ),
    )


def removal_grant(target):
    return FunctionGrant(
        target=target.contracts.router,
        signature="removeLiquidity(address,address,bool,uint256,uint256,uint256,address,uint256)",
        condition=matches(
            *pool_arguments(target),
            finite_shares(),
            nonnegative(),
            nonnegative(),
            equal("address", target.fund.address),
            bounded(MAXIMUM),
        ),
        explanation=(
            "Remove this pool's fungible LP shares and return both currencies to the Safe.",
        ),
    )


def liquidity_grants(capability):
    target = capability.target
    return (
        approval_grant(target, target.pool.shares_asset, finite_shares()),
        addition_grant(target),
        removal_grant(target),
        FunctionGrant(
            target=target.pool.pool,
            signature="claimFees()",
            condition=None,
            explanation=("Claim only this pool's account-wide fees to the calling Safe.",),
        ),
    )


def derive(profile, capabilities):
    grants: list[FunctionGrant] = []
    for capability in capabilities.requirements:
        if capability.target.fund != profile.fund:
            continue
        for direction, asset in enumerate(capability.target.assets):
            grants.extend(
                (
                    approval_grant(capability.target, asset, positive()),
                    swap_grant(capability, direction),
                )
            )
        if capability.target.liquidity_enabled:
            grants.extend(liquidity_grants(capability))
    return combine(grants)


class AerodromePermissions:
    codec = capability_codec()
    manifest_type = PermissionManifest[Capabilities]
    derive = staticmethod(derive)
    runtime_checks = (
        "Quote minima, slippage, deadline freshness, inventory and retained LP share amounts.",
        "Share minimum simulation, fee accounting, spending-authority cleanup and recovery.",
        "Initial fund has no unrelated shares or fees; pool holds no unrelated LP deposit.",
    )

    @staticmethod
    def identities(config, account, reader):
        from almanak.connectors.aerodrome.deployments import identities

        return identities(config, account, reader)
