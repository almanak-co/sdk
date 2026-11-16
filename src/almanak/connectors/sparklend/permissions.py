"""SparkLend identity verification with compatible Pool Roles conditions."""

from almanak.connectors.pool_lending_permissions import closure_grants as closure_grants
from almanak.connectors.pool_lending_permissions import derive as derive
from almanak.connectors.pool_lending_permissions import entry_grant as entry_grant
from almanak.connectors.pool_lending_permissions import supply_exits as supply_exits
from almanak.connectors.sparklend.authority import Capabilities, capability_codec
from almanak.connectors.sparklend.reader import read_reserve
from almanak.gateway.permission_models import PermissionManifest


def identities(config, account, reader):
    addresses: set[str] = set()
    for allocation in config.positions:
        if allocation.account != account:
            continue
        reserve = read_reserve(reader.reader, allocation.reserve, reader.block)
        if reserve.oracle is None or (reserve.oracle.oracle, reserve.oracle.feed) != (
            allocation.oracle,
            allocation.price_source,
        ):
            raise ValueError("configured SparkLend oracle source differs from observed evidence")
        addresses.update(
            (
                allocation.reserve.pool,
                allocation.reserve.asset.reference,
                reserve.a_token.reference,
                reserve.variable_debt_token.reference,
                reserve.stable_debt_token.reference,
                allocation.oracle,
                allocation.price_source,
            )
        )
    return tuple(reader.identity(address) for address in sorted(addresses))


class SparkPermissions:
    manifest_type = PermissionManifest[Capabilities]
    codec = capability_codec()
    runtime_checks = (
        "Fund/Pool position ownership and complete collateral/debt scope.",
        "Configured oracle sources, supported collateral mode and projected health factor.",
        "Intent ordering, current liquidity, repayment funding and observed cleanup.",
    )
    derive = staticmethod(derive)
    identities = staticmethod(identities)
