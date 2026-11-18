"""Scope-only Comet grants; the gateway owns the economic distinctions Comet cannot make."""

from almanak.connectors.compound_v3.authority import Capabilities, capability_codec
from almanak.connectors.compound_v3.reader import read_market
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import (
    FunctionGrant,
    any_amount,
    combine,
    equal,
    matches,
    positive,
)


def call_grant(capability, name):
    target = capability.target
    meaning = (
        "supply or repay, including the full-repay sentinel"
        if name == "supply"
        else "withdraw or borrow, including the full-withdraw sentinel"
    )
    return FunctionGrant(
        target=target.reserve.market.comet,
        signature=name + "(address,uint256)",
        condition=matches(equal("address", target.asset.reference), positive()),
        explanation=(
            f"{target.allocation}: {meaning} of only {target.asset} for this Safe, any "
            "nonzero amount. Comet credits and pays only the caller.",
        ),
    )


def approval_grant(capability):
    target = capability.target
    # Comet pulls only from its caller (or with permission only the Safe can grant),
    # so an approval is spendable only through this Safe's own supply and repayment.
    return FunctionGrant(
        target=target.asset.reference,
        signature="approve(address,uint256)",
        condition=matches(equal("address", target.reserve.market.comet), any_amount()),
        explanation=(f"{target.allocation}: approve only its Comet, any amount, or revoke.",),
    )


def derive(profile, capabilities):
    """Scope-only Comet grants for every allocation.

    Comet shares one selector between supply and partial repayment and one between
    withdrawal and borrowing, so the finite grants cover both meanings and the
    full-repay and full-withdraw sentinels. The gateway distinguishes them.
    """
    grants = []
    for capability in capabilities.requirements:
        if capability.target.fund != profile.fund:
            continue
        grants.append(call_grant(capability, "supply"))
        grants.append(call_grant(capability, "withdraw"))
        grants.append(approval_grant(capability))
    return combine(grants)


def identities(config, account, reader):
    contracts = {}
    for allocation in config.positions:
        if allocation.account != account:
            continue
        market = read_market(reader.reader, allocation.reserve.market, reader.block)
        for identity in market.contracts:
            contracts[identity.address] = identity
        addresses = {
            allocation.reserve.asset.reference,
            allocation.reserve.price_feed,
            market.source.base_asset.reference,
            market.source.base_price_feed,
        }
        if not allocation.reserve.is_base:
            collateral = next(
                c for c in market.collaterals if c.asset_type == allocation.reserve.asset
            )
            if collateral.price_feed != allocation.reserve.price_feed:
                raise ValueError(
                    "Compound permission target has another configured collateral feed"
                )
        for address in addresses:
            contracts[address] = reader.identity(address)
    return tuple(contracts[address] for address in sorted(contracts))


class CompoundPermissions:
    manifest_type = PermissionManifest[Capabilities]
    codec = capability_codec()
    runtime_checks = (
        "Economic supply versus repayment and borrowing versus withdrawal are gateway checks; "
        "Comet shares each ABI selector.",
        "Exact retained Comet principal, collateral ownership, configured oracle sources "
        "and fund-wide risk.",
        "Repayment funding, partial repayment within the observed debt, cleanup and "
        "action sequencing.",
    )
    derive = staticmethod(derive)
    identities = staticmethod(identities)
