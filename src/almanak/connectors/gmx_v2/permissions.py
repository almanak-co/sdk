"""Accepted dedicated-Safe cancellation scope with exact retained-key gateway admission."""

from almanak.connectors.gmx_v2.authority import capability_codec
from almanak.connectors.gmx_v2.candidate_permissions import (
    OrderScope,
    candidate_cancellation_grants,
    candidate_order_grants,
)
from almanak.connectors.gmx_v2.deployments import identities
from almanak.connectors.gmx_v2.exchange_models import ExchangeCapability
from almanak.connectors.gmx_v2.liquidity_models import LiquidityCapability
from almanak.connectors.gmx_v2.models import Capabilities
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import FunctionGrant, combine, equal, matches


def scope(capability):
    target = capability.target
    return OrderScope(
        fund=target.fund,
        exchange_router=target.market.contracts.exchange_router,
        token_router=target.market.contracts.token_router,
        order_vault=target.market.contracts.order_vault,
        market=target.market.share_asset,
        wrapped_native=target.market.wrapped_native,
        side=target.side,
        maximum_execution_fee=capability.maximum_execution_fee,
    )


def derive(profile, capabilities):
    grants: list[FunctionGrant] = []
    for capability in capabilities.requirements:
        if capability.target.fund != profile.fund:
            continue
        if isinstance(capability, (ExchangeCapability, LiquidityCapability)):
            created, allowed = exchange_grants(capability)
            cancelled = candidate_cancellation_grants(
                capability.target.market.contracts.exchange_router
            )
            grants.extend(
                (
                    *created,
                    *(accepted_grant(grant) for grant in cancelled if grant.signature in allowed),
                )
            )
            continue
        order = scope(capability)
        cancelled = candidate_cancellation_grants(order.exchange_router)
        grants.extend(
            (
                *candidate_order_grants(order),
                claim_grant(capability.target),
                *(accepted_grant(g) for g in cancelled),
            )
        )
    return combine(grants)


def accepted_grant(grant):
    return grant.model_copy(
        update={
            "explanation": (
                "Accepted dedicated-Safe scope: any nonzero compatible Safe-owned request key.",
                *grant.explanation[1:],
            )
        }
    )


class Permissions:
    codec = capability_codec()
    manifest_type = PermissionManifest[Capabilities]
    derive = staticmethod(derive)
    identities = staticmethod(identities)
    runtime_checks = (
        "Exact retained request key, aggregate position ownership and request eligibility.",
        "Configured price arguments, the configured fee, collateral availability and cleanup.",
        "Cancellation grants can affect unrelated compatible Safe requests "
        "and their stored recipients and callbacks.",
    )


def claim_grant(target):
    return FunctionGrant(
        target=target.market.contracts.exchange_router,
        signature="claimFundingFees(address[],address[],address)",
        condition=matches(
            matches(
                *(equal("address", target.market.share_asset.reference) for _ in range(2)),
                parameter=4,
            ),
            matches(
                equal("address", target.market.long_asset.reference),
                equal("address", target.market.short_asset.reference),
                parameter=4,
            ),
            equal("address", target.fund.address),
        ),
        explanation=(
            "Claim funding for exactly the configured market and both market assets to the fund.",
            "The protocol claims all fund entitlements for those market/token keys, across sides.",
            "Gateway admission separately requires the exact retained strategy-owned amounts.",
        ),
    )


def exchange_grants(capability):
    if isinstance(capability, LiquidityCapability):
        from almanak.connectors.gmx_v2.liquidity_permissions import derive as derive_liquidity

        return derive_liquidity(capability), ("cancelDeposit(bytes32)", "cancelWithdrawal(bytes32)")
    from almanak.connectors.gmx_v2.exchange_permissions import derive as derive_exchange

    return derive_exchange(capability), ("cancelOrder(bytes32)",)
