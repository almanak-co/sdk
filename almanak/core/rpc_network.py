"""Canonical network environments for RPC-backed runtime configuration.

``Network`` is deliberately narrow: it describes how Almanak resolves an RPC
endpoint (production, a supported testnet, or a local Anvil fork). It is not a
chain identifier and must not be used for open-ended provider/venue slugs.

This is not to be confused with :class:`almanak.core.enums.Network`, whose
uppercase wire values remain part of the public ISDK contract. The two enums
serve different boundaries; RPC runtime code should import this module.

Extension point
---------------
Add a member to :class:`Network` and its routing semantics to
``NETWORK_PROFILES`` in the same change. The exhaustive coverage test rejects
an enum member without a profile, so RPC lookup and settings cannot silently
drift apart.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final


class Network(StrEnum):
    """Closed set of network environments managed by the RPC runtime."""

    MAINNET = "mainnet"
    TESTNET = "testnet"
    SEPOLIA = "sepolia"
    ANVIL = "anvil"

    @classmethod
    def parse(cls, value: object) -> Network:
        """Parse a boundary value into the canonical network type.

        String inputs remain accepted at configuration, CLI, and protobuf
        boundaries for backward compatibility. Internal APIs carry the enum.
        """
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ValueError(
                f"Network must be a string, got {type(value).__name__}. "
                f"Valid values: {', '.join(member.value for member in cls)}"
            )

        normalized = value.strip().casefold()
        try:
            return cls(normalized)
        except ValueError:
            valid = ", ".join(member.value for member in cls)
            raise ValueError(f"Unknown network {value!r}. Valid values: {valid}") from None


@dataclass(frozen=True, slots=True)
class NetworkProfile:
    """Routing semantics for one RPC-managed network environment."""

    rpc_target: Network
    is_local: bool = False


NETWORK_PROFILES: Final[Mapping[Network, NetworkProfile]] = MappingProxyType(
    {
        Network.MAINNET: NetworkProfile(rpc_target=Network.MAINNET),
        Network.TESTNET: NetworkProfile(rpc_target=Network.SEPOLIA),
        Network.SEPOLIA: NetworkProfile(rpc_target=Network.SEPOLIA),
        Network.ANVIL: NetworkProfile(rpc_target=Network.ANVIL, is_local=True),
    }
)


def network_profile(network: Network) -> NetworkProfile:
    """Return the explicit routing profile for a canonical network."""
    return NETWORK_PROFILES[network]
