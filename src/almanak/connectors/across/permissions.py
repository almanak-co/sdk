"""Scope-only Roles grants of a bridge: one pinned deposit and its approval per route.

A Safe deposits through two grants. The approval names the sent asset and the route's
SpokePool as the only spender, for any amount, zero included, so the same grant serves
the revocation. The deposit names the SpokePool and pins where value goes: the Safe as
depositor, which a refund pays, the destination fund as recipient, both tokens, the
destination chain and an empty message.

Roles cannot relate the output amount to the input amount. A grant therefore lets its
holder deposit at any nonzero output, and only the gateway holds a deposit to the
venue's quote and the author's fee ceiling.
"""

from eth_abi import encode

from almanak.connectors.across.authority import Capabilities, capability_codec
from almanak.connectors.across.operations import DEPOSIT, word
from almanak.gateway.evm import chain_number
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import (
    Condition,
    FunctionGrant,
    any_amount,
    combine,
    equal,
    matches,
    positive,
)


def pinned_word(address: str) -> Condition:
    """An address as the left-padded bytes32 the SpokePool takes, held exactly."""
    return equal("bytes32", word(address))


def empty_message() -> Condition:
    """The `bytes` argument held to zero length, so a deposit carries no instruction
    for the recipient or for a contract the relayer would call on arrival."""
    # Roles 2.1 parameter type Dynamic (2) under operator EqualTo (16).
    return Condition(parameter=2, operator=16, value="0x" + encode(("bytes",), (b"",)).hex())


def deposit_condition(target) -> Condition:
    """One condition per `deposit` argument, in the order the planner encodes them."""
    route = target.route
    return matches(
        pinned_word(target.fund.address),
        pinned_word(target.destination_fund.address),
        pinned_word(route.asset.reference),
        pinned_word(route.destination_asset.reference),
        # Input and output amounts: any nonzero value. No amount is bound, and Roles
        # cannot compare the two.
        positive(),
        positive(),
        equal("uint256", chain_number(route.destination_chain)),
        # The exclusive relayer is the venue's choice per quote, and absent in some.
        any_amount(),
        # Quote timestamp and fill deadline: per quote and never zero. The SpokePool
        # holds both to its own buffers; their freshness is a gateway check.
        positive(),
        positive(),
        # Exclusivity is an offset or a time per quote, or zero for none.
        any_amount(),
        empty_message(),
    )


def deposit_grant(capability) -> FunctionGrant:
    target = capability.target
    route = target.route
    return FunctionGrant(
        target=route.spoke_pool,
        signature=DEPOSIT,
        condition=deposit_condition(target),
        explanation=(
            f"{target.allocation}: deposit only {route.asset} into its SpokePool for "
            f"{route.destination_asset} on {route.destination_chain}, with this Safe as "
            f"depositor, {target.destination_fund.address} as recipient and an empty "
            "message, without native value. Any nonzero input and output amount, any "
            "relayer, quote time, fill deadline and exclusivity: Roles does not relate "
            "the output to the input.",
        ),
    )


def approval_grant(capability) -> FunctionGrant:
    target = capability.target
    return FunctionGrant(
        target=target.asset.reference,
        signature="approve(address,uint256)",
        condition=matches(equal("address", target.route.spoke_pool), any_amount()),
        explanation=(f"{target.allocation}: approve only its SpokePool, any amount, or revoke.",),
    )


def derive(profile, capabilities):
    """The grants of every route this Safe sends from.

    A Safe that only receives a bridge takes none: the SpokePool pays it and it calls
    nothing. Revocation needs no grant of its own, because the approval grant admits
    zero.
    """
    grants = []
    for capability in capabilities.requirements:
        if capability.target.fund != profile.fund:
            continue
        grants.append(approval_grant(capability))
        grants.append(deposit_grant(capability))
    return combine(grants)


def identities(config, account, reader):
    """The code identity of each sent asset the account approves.

    The SpokePool is left out on purpose. Verification refuses every dispatch of an
    account once a retained identity changes, the revocation included, and a SpokePool
    that leaves its pin must refuse new deposits only. The connector holds the pool to
    its pin wherever a deposit or its approval is planned, signed or sent.
    """
    assets = {
        allocation.route.asset.reference
        for allocation in config.positions
        if allocation.account == account
    }
    return tuple(reader.identity(address) for address in sorted(assets))


class AcrossPermissions:
    manifest_type = PermissionManifest[Capabilities]
    codec = capability_codec()
    runtime_checks = (
        "The bridge fee ceiling, the quoted output amount and the deposit deadlines are "
        "gateway checks; the SpokePool checks no price, and Roles cannot relate a "
        "deposit's output amount to its input.",
        "The pinned SpokePool implementation, the bridged amount against owned "
        "inventory, the venue's exclusive relayer and approval cleanup.",
    )
    derive = staticmethod(derive)
    identities = staticmethod(identities)
