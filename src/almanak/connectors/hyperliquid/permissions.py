"""Generated Roles grants of the Safe's custody path; trading needs none (blueprint 30, 4.3).

The operator approves CoreDepositWallet, deposits the Safe's own USDC and sends
CoreWriter actions 7, 13, 16 and 9 as the Safe (`custody_grants.py`). The agent signs
trades through Hyperliquid's API, which no Roles grant can scope: the gateway's
admission alone holds it to the strategy's market, size, price and leverage (D2).
"""

from almanak.connectors.hyperliquid.agents import AgentNames
from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.connectors.hyperliquid.authority import Capabilities, capability_codec
from almanak.connectors.hyperliquid.corewriter import CORE_WRITER, USDC_CONTRACT
from almanak.connectors.hyperliquid.custody_grants import custody_grants
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import combine

CONTRACTS = (USDC_CONTRACT, CORE_DEPOSIT_WALLET, CORE_WRITER)


def derive(profile, capabilities):
    """The custody grants of the profile's Safe, once, under the deployment's agent names."""
    if not any(c.target.fund == profile.fund for c in capabilities.requirements):
        return ()
    return combine(list(custody_grants(profile.fund.address, AgentNames())))


def identities(config, account, reader):
    if not any(a.account == account for a in config.allocations):
        return ()
    return tuple(reader.identity(address) for address in sorted(CONTRACTS))


class Permissions:
    codec = capability_codec()
    manifest_type = PermissionManifest[Capabilities]
    derive = staticmethod(derive)
    identities = staticmethod(identities)
    runtime_checks = (
        "The agent can trade every market of the Safe's HyperCore account at any size, "
        "price and leverage (D2): gateway admission alone holds it to the configured "
        "market, the author's leverage and slippage, and reduce-only exits no larger than "
        "the strategy's own position.",
        "Each custody request is resolved by its own HyperCore effect, never by a balance "
        "change; one without an identified effect halts.",
        "Every agent HyperCore lists under the deployment's names must be a key the signer "
        "service holds for it (D9); teardown does not revoke the agent (D2).",
    )
