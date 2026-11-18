"""Exact Morpho market templates use built-in Roles conditions and scope only."""

from eth_abi import encode

from almanak.connectors.morpho_blue.authority import Capabilities, capability_codec
from almanak.connectors.morpho_blue.deployment import oracle_dependencies, read_deployment
from almanak.connectors.morpho_blue.domain import PARAMS_ABI
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import (
    Condition,
    FunctionGrant,
    any_amount,
    bounded,
    combine,
    equal,
    matches,
    positive,
)

MAXIMUM = 2**128 - 1


def parameters_condition(market):
    return matches(
        *(
            equal(kind, value)
            for kind, value in zip(
                ("address", "address", "address", "address", "uint256"),
                market.parameters,
                strict=True,
            )
        ),
        parameter=3,
    )


def empty_callback():
    return Condition(parameter=2, operator=16, value="0x" + encode(("bytes",), (b"",)).hex())


def economic_grant(capability, name, quantity, *, shares=False):
    target = capability.target
    collateral = not target.reserve.is_loan
    callback = name in ("supply", "repay")
    method = name + "Collateral" if collateral else name
    types = (
        [PARAMS_ABI, "uint256"]
        + ([] if collateral else ["uint256"])
        + ["address", "bytes" if callback else "address"]
    )
    arguments = [
        parameters_condition(target.reserve.market),
        equal("uint256", 0) if shares else quantity,
    ]
    if not collateral:
        arguments.append(quantity if shares else equal("uint256", 0))
    arguments.extend(
        (
            equal("address", target.fund.address),
            empty_callback() if callback else equal("address", target.fund.address),
        )
    )
    return FunctionGrant(
        target=target.reserve.market.core,
        signature=f"{method}({','.join(types)})",
        condition=matches(*arguments),
        explanation=(
            f"{target.allocation}: exact Morpho market tuple; {name} for this Safe with "
            + ("positive shares" if shares else "positive assets")
            + "; empty callback or exact Safe receiver.",
        ),
    )


def approval_grant(capability):
    # The core pulls only from its caller, so an approval is spendable only through
    # this Safe's own supply and repay (bounded by its own debt) calls.
    return FunctionGrant(
        target=capability.target.asset.reference,
        signature="approve(address,uint256)",
        condition=matches(
            equal("address", capability.target.reserve.market.core),
            any_amount(),
        ),
        explanation=("Only this Morpho core, any amount, including zero cleanup.",),
    )


def derive(profile, capabilities):
    """Supply and withdraw always; borrow and repay only for a borrowing loan allocation."""
    grants = []
    for capability in capabilities.requirements:
        if capability.target.fund != profile.fund:
            continue
        grants.append(economic_grant(capability, "supply", positive()))
        grants.append(economic_grant(capability, "withdraw", bounded(MAXIMUM)))
        if capability.target.reserve.is_loan:
            grants.append(economic_grant(capability, "withdraw", bounded(MAXIMUM), shares=True))
        if capability.borrow:
            grants.append(economic_grant(capability, "borrow", positive()))
            # Morpho reverts on a repayment above the debt, so neither form is bounded.
            grants.append(economic_grant(capability, "repay", positive()))
            grants.append(economic_grant(capability, "repay", bounded(MAXIMUM), shares=True))
        grants.append(approval_grant(capability))
    return combine(grants)


def market_identities(market, reader):
    deployment = read_deployment(reader.reader, market, reader.block)
    addresses = {market.loan_asset.reference, market.collateral_asset.reference}
    addresses.update(oracle_dependencies(deployment))
    return (*deployment.contracts, *(reader.identity(address) for address in sorted(addresses)))


def identities(config, account, reader):
    contracts = {}
    for allocation in config.positions:
        if allocation.account == account:
            for identity in market_identities(allocation.reserve.market, reader):
                contracts[identity.address] = identity
    return tuple(contracts[address] for address in sorted(contracts))


class MorphoPermissions:
    manifest_type = PermissionManifest[Capabilities]
    codec = capability_codec()
    runtime_checks = (
        "Exact retained supply shares, borrow shares and collateral belong to the "
        "strategy fund and market.",
        "Full repayment uses retained debt shares and approves the held loan balance; "
        "repayment funding and partial repayment within the observed debt are gateway checks.",
        "Fund/market health, projected interest, inventory, action ordering and cleanup "
        "are gateway checks.",
    )
    derive = staticmethod(derive)
    identities = staticmethod(identities)
