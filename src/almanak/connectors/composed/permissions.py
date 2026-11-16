"""Compile correlated native grants into one reviewed fund authority manifest."""

from almanak.connectors.composed.authority import Capabilities, capability_codec, selected
from almanak.connectors.composed.models import components_for_account
from almanak.connectors.composition import connector_for, connector_from_wire
from almanak.connectors.target_routing import target_connector
from almanak.gateway.permission_models import ContractIdentity, PermissionManifest
from almanak.gateway.roles_conditions import combine
from almanak.models import Address


def derive(profile, capabilities):
    grants = []
    identities = dict.fromkeys(target_connector(c.target) for c in capabilities.requirements)
    for identity in identities:
        binding = connector_from_wire({"connector": str(identity)})
        grants.extend(binding.permissions.derive(profile, selected(capabilities, identity)))
    return combine(grants)


def identities(config, account, reader):
    retained: dict[Address, ContractIdentity] = {}
    for _, view in components_for_account(config, account):
        for contract in connector_for(view).permissions.identities(view, account, reader):
            if retained.setdefault(contract.address, contract) != contract:
                raise ValueError("shared contract identity differs between connector evidence")
    return tuple(retained[address] for address in sorted(retained))


class ComposedPermissions:
    codec = capability_codec()
    manifest_type = PermissionManifest[Capabilities]
    derive = staticmethod(derive)
    identities = staticmethod(identities)
    runtime_checks = (
        "Shared fund inventory and connector-owned position accounting.",
        "Configured lending oracle, risk, liquidity and repayment funding checks.",
        "Exchange quote minima, slippage and deadline freshness.",
        "Strategy sequencing, cleanup, outcome recovery and retained NFT scope.",
    )
