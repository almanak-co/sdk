"""Connector-owned strategy-side address-table declarations."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from enum import StrEnum


class AbiFamily(StrEnum):
    """Shared on-chain ABI family a group of connectors exposes."""

    #: Uniswap-V3-style factory exposing ``getPool(address,address,uint24)``.
    V3_FACTORY = "v3_factory"
    #: Canonical Uniswap V3 NonfungiblePositionManager
    #: (``balanceOf`` / ``tokenOfOwnerByIndex`` / ``positions(tokenId)``).
    V3_NPM = "v3_npm"


@dataclass(frozen=True)
class AddressTableSpec:
    """One connector-owned protocol address table plus optional ABI-family tags."""

    protocol: str
    module: str
    attribute: str
    abi_families: tuple[AbiFamily, ...] = field(default_factory=tuple)
    abi_family_order: int | None = None

    def __post_init__(self) -> None:
        """Validate the address-table selector without importing the table."""
        for field_name in ("protocol", "module", "attribute"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"AddressTableSpec.{field_name} must be a non-empty string, got {value!r}")

        normalized_protocol = self.protocol.strip().lower()
        if normalized_protocol != self.protocol:
            object.__setattr__(self, "protocol", normalized_protocol)

        if not isinstance(self.abi_families, tuple):
            raise ValueError(
                f"AddressTableSpec.abi_families must be a tuple[AbiFamily, ...], got {self.abi_families!r}"
            )
        bad_families = [family for family in self.abi_families if not isinstance(family, AbiFamily)]
        if bad_families:
            raise ValueError(f"AddressTableSpec.abi_families must contain only AbiFamily values, got {bad_families!r}")
        if len(set(self.abi_families)) != len(self.abi_families):
            raise ValueError(f"AddressTableSpec.abi_families contains duplicates: {self.abi_families!r}")
        if self.abi_family_order is not None and (
            not isinstance(self.abi_family_order, int) or self.abi_family_order < 0
        ):
            raise ValueError(
                f"AddressTableSpec.abi_family_order must be None or a non-negative int, got {self.abi_family_order!r}"
            )

    def load_table(self) -> dict[str, dict[str, str]]:
        """Import and return the connector-owned ``{chain: {kind: address}}`` table."""
        module = importlib.import_module(self.module)
        table = getattr(module, self.attribute, None)
        if not isinstance(table, dict):
            raise TypeError(
                f"AddressTableSpec for protocol {self.protocol!r} maps to {self.module}.{self.attribute}, "
                f"but that attribute is {type(table).__name__}, not a dict."
            )
        return table


__all__ = ["AbiFamily", "AddressTableSpec"]
