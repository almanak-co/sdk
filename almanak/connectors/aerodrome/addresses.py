"""Aerodrome (and Velodrome V2 on Optimism) contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on ``AerodromeGatewayConnector``;
strategy-side connector code reads the dicts directly.
"""

from __future__ import annotations

from dataclasses import dataclass

AERODROME: dict[str, dict[str, str]] = {
    "base": {
        "router": "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
        "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
        "voter": "0x16613524e02ad97eDfeF371bC883F2F5d6C480A5",
    },
    # Velodrome V2 on Optimism — same Solidly fork interface as Aerodrome on Base.
    # Addresses verified on Optimism block explorer (optimistic.etherscan.io).
    "optimism": {
        "router": "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858",
        "factory": "0xF1046053aa5682b4F9a81b5481394DA16BE5FF5a",
        "voter": "0x41C914ee0c7E1A5edCD0295623e6dC557B5aBf3C",
    },
}


@dataclass(frozen=True, slots=True)
class SlipstreamDeployment:
    """One Slipstream factory generation and the contracts bound to it.

    Slipstream has multiple Base factory generations.  A pool's factory, the
    position manager that owns its NFTs, and the swap router / quoter that
    derive pools from that factory are operational facts, not interchangeable
    aliases: an NFT minted by one NPM cannot be closed through another, and a
    router bound to one factory cannot reach another generation's pool.

    ``swap_router`` / ``quoter`` are ``None`` for a generation Aerodrome
    deployed without a periphery; such pools are LP-only and the swap lane
    refuses them with that reason.
    """

    factory: str
    position_manager: str
    generation: str
    swap_router: str | None = None
    quoter: str | None = None


# Keyed by chain; each entry is one complete generation.  Tuple order carries
# NO meaning: every lane resolves the generation from the pool (``factory()``
# for an address, ``getPool`` on every entry for a symbolic key) and the
# chain-truth CI gate proves each entry's periphery reports ``factory()`` equal
# to the entry's factory.  Adding a generation is one entry here plus a fixture
# row in that gate.  Aerodrome's third Base deployment ("Gauge Caps",
# 0xaDe65c38CD4849aDBA595a4323a8C7DdfE89716a) is intentionally not admitted.
SLIPSTREAM_LP_DEPLOYMENTS: dict[str, tuple[SlipstreamDeployment, ...]] = {
    "base": (
        SlipstreamDeployment(
            factory="0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef",
            position_manager="0xe1f8cd9AC4e4A65F54f38a5CdAfCA44f6dD68b53",
            generation="current",
            swap_router="0x698Cb2b6dd822994581fEa6eA4Fc755d1363A92F",
            quoter="0x514c8B5f54112481E28028F1166Bd78501089259",
        ),
        SlipstreamDeployment(
            factory="0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A",
            position_manager="0x827922686190790b37229fd06084350E74485b72",
            generation="legacy",
            swap_router="0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5",
            quoter="0x254cF9E1E6e233aa1AC962CB9B05b2cfeAaE15b0",
        ),
    ),
}


def slipstream_lp_deployments(chain: str) -> tuple[SlipstreamDeployment, ...]:
    """Return the reviewed Slipstream deployments for ``chain``."""

    return SLIPSTREAM_LP_DEPLOYMENTS.get(chain.strip().lower(), ())


def _deployment_where(chain: str, attribute: str, address: str) -> SlipstreamDeployment | None:
    key = address.strip().lower()
    if not key:
        return None
    return next(
        (
            deployment
            for deployment in slipstream_lp_deployments(chain)
            if str(getattr(deployment, attribute) or "").lower() == key
        ),
        None,
    )


def slipstream_deployment_for_factory(chain: str, factory: str) -> SlipstreamDeployment | None:
    """Resolve the factory a pool reports to its reviewed generation."""

    return _deployment_where(chain, "factory", factory)


def slipstream_deployment_for_position_manager(chain: str, position_manager: str) -> SlipstreamDeployment | None:
    """Resolve an exact NPM to the factory generation that created its NFTs."""

    return _deployment_where(chain, "position_manager", position_manager)


def slipstream_deployment_for_router(chain: str, swap_router: str) -> SlipstreamDeployment | None:
    """Resolve an exact swap router to the generation whose factory it derives pools from."""

    return _deployment_where(chain, "swap_router", swap_router)


def slipstream_position_manager_kind(deployment: SlipstreamDeployment) -> str:
    """Contract-kind name under which ``deployment``'s NPM is published in :data:`AERODROME`."""

    return f"slipstream_position_manager_{deployment.generation}"


def _publish_reviewed_position_managers() -> None:
    """Mirror every reviewed NPM into the flat address table under a generation-named kind.

    Framework registries (contract roles, valuation, teardown) enumerate
    connector addresses by kind name; publishing one kind per generation lets
    them see the whole reviewed set without a singleton "the" manager.
    """

    for chain, deployments in SLIPSTREAM_LP_DEPLOYMENTS.items():
        table = AERODROME.setdefault(chain, {})
        for deployment in deployments:
            table[slipstream_position_manager_kind(deployment)] = deployment.position_manager


_publish_reviewed_position_managers()


AERODROME_TOKENS: dict[str, dict[str, str]] = {
    "base": {
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
        "AERO": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
        "cbETH": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
        "rETH": "0xB6fe221Fe9EeF5aBa221c348bA20A1Bf5e73624c",
    },
}

# Symbols (UPPER-CASED) treated as USD stablecoins when choosing the auto
# Classic-pool probe order (VIB-5548 / ALM-2889, design O4). When BOTH legs of
# a swap are in this set the resolver probes the Classic *stable* pool first
# (Solidly stable pools are the canonical venue for stable/stable pairs such as
# DAI/USDbC); otherwise it probes the *volatile* pool first. Deliberately small
# and symbol-based — it only orders two read-only probes, never gates execution
# (the on-chain pool-existence check + price-impact guard remain authoritative).
AERODROME_STABLE_SYMBOLS: frozenset[str] = frozenset(
    {"USDC", "USDBC", "USDC.E", "DAI", "USDT", "USD+", "DOLA", "EURC", "GHO"}
)
__all__ = [
    "AERODROME",
    "AERODROME_STABLE_SYMBOLS",
    "AERODROME_TOKENS",
    "SLIPSTREAM_LP_DEPLOYMENTS",
    "SlipstreamDeployment",
    "slipstream_deployment_for_factory",
    "slipstream_deployment_for_position_manager",
    "slipstream_deployment_for_router",
    "slipstream_lp_deployments",
    "slipstream_position_manager_kind",
]
