"""Explicit Aave wiring for the reusable Zodiac permission compiler and verifier."""

from almanak.connectors.aave_v4.authority import capability_codec
from almanak.connectors.aave_v4.deployments import verify_deployments
from almanak.connectors.aave_v4.models import AaveCapabilities, DebtAllocation
from almanak.connectors.aave_v4.permissions import derive_aave
from almanak.connectors.aave_v4.reader import read_reserve
from almanak.gateway.evm import erc20_address
from almanak.gateway.permission_models import PermissionManifest as GeneratedPermissions

PermissionManifest = GeneratedPermissions[AaveCapabilities]


def protocol_identities(config, account, reader):
    addresses: set[str] = set()
    for allocation in config.positions:
        if allocation.account != account:
            continue
        reserve = allocation.reserve
        verify_deployments(reader.reader, reserve, reader.block)
        read_reserve(reader.reader, reserve, reader.block)
        addresses.update(
            (reserve.spoke, reserve.hub, erc20_address(reserve.asset, reserve.chain.chain_id))
        )
        if isinstance(allocation, DebtAllocation):
            addresses.update(
                (
                    allocation.oracle,
                    allocation.debt_price_source,
                    *allocation.collateral_price_sources,
                )
            )
    return tuple(reader.identity(address) for address in sorted(addresses))


class AavePermissions:
    manifest_type = PermissionManifest
    codec = capability_codec()
    runtime_checks = (
        "Health factor, explicit oracle sources and projected borrowing health.",
        "Intent admission, position ownership, dependencies and execution ordering.",
        "Exit timers, fresh debt closure and repayment funding.",
    )
    identities = staticmethod(protocol_identities)

    @staticmethod
    def derive(profile, capabilities):
        requirements = tuple(c for c in capabilities.requirements if c.target.fund == profile.fund)
        return derive_aave(profile, requirements)


def generate(config, strategy, backend):
    from almanak.gateway.permissions import generate as compile_permissions

    return compile_permissions(config, strategy, backend, connector=AavePermissions())


def verify(config, manifest, backend, *, account=None):
    from almanak.gateway.permissions import verify as verify_permissions

    return verify_permissions(
        config, manifest, backend, connector=AavePermissions(), account=account
    )


def validate_manifest(config, manifest):
    from almanak.gateway.permissions import validate_manifest as validate_permissions

    return validate_permissions(config, manifest, connector=AavePermissions())
