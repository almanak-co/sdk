"""Scope-only PancakeSwap V3 trading and liquidity grants using built-in Roles predicates."""

from almanak.connectors.pancakeswap_v3 import liquidity_permissions
from almanak.connectors.pancakeswap_v3.abi import SWAP_PARAMS
from almanak.connectors.pancakeswap_v3.authority import capability_codec
from almanak.connectors.pancakeswap_v3.models import Capabilities
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import (
    FunctionGrant,
    any_amount,
    bounded,
    combine,
    equal,
    matches,
)

MAXIMUM = 2**128 - 1


def approval_grant(capability, asset):
    return FunctionGrant(
        target=asset.reference,
        signature="approve(address,uint256)",
        condition=matches(
            equal("address", capability.target.contracts.router),
            any_amount(),
        ),
        explanation=(
            "Allowance of any amount, including zero cleanup, to the configured router only.",
        ),
    )


def swap_grant(capability, direction):
    target = capability.target
    assets = (target.pool.currency0, target.pool.currency1)
    return FunctionGrant(
        target=target.contracts.router,
        signature=f"exactInputSingle({SWAP_PARAMS})",
        condition=matches(
            matches(
                equal("address", assets[direction].reference),
                equal("address", assets[1 - direction].reference),
                equal("uint24", target.pool.fee),
                equal("address", target.fund.address),
                bounded(MAXIMUM),
                bounded(MAXIMUM),
                bounded(MAXIMUM),
                equal("uint160", 0),
                parameter=3,
            )
        ),
        explanation=(
            "One Pancake exact-input swap with fixed assets, fee and Safe recipient; "
            "any nonzero uint128 input. Quote minima and deadline freshness are gateway checks.",
        ),
    )


def derive(profile, capabilities):
    grants: list[FunctionGrant] = []
    for capability in capabilities.requirements:
        if capability.target.fund != profile.fund:
            continue
        if capability.target.liquidity_enabled:
            grants.extend(liquidity_permissions.derive(profile, capability))
        for direction, asset in enumerate(capability.target.assets):
            grants.extend((approval_grant(capability, asset), swap_grant(capability, direction)))
    return combine(grants)


class PancakePermissions:
    codec = capability_codec()
    manifest_type = PermissionManifest[Capabilities]
    derive = staticmethod(derive)
    runtime_checks = (
        "Quote minima, slippage, deadline freshness and attributed inventory.",
        "Operation sequencing, spending-authority cleanup and outcome recovery.",
        "Exact retained NFT, pool and range ownership; LP grants allow broader NFT scope.",
        "LP multicall NFT correlation and exact deposit-asset attribution.",
    )

    @staticmethod
    def identities(config, account, reader):
        from almanak.connectors.pancakeswap_v3.deployments import identities

        return identities(config, account, reader)
