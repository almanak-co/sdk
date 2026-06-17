"""Connector-owned representative vault metadata registry."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import ClassVar


@dataclass(frozen=True)
class VaultRepresentativeSpec:
    """One connector-owned representative-vault table declaration."""

    protocol: str
    module: str
    attribute: str

    def __post_init__(self) -> None:
        for field_name in ("protocol", "module", "attribute"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"VaultRepresentativeSpec.{field_name} must be a non-empty string, got {value!r}")

        protocol = self.protocol.strip().lower()
        if protocol != self.protocol:
            object.__setattr__(self, "protocol", protocol)
        if self.module.startswith("."):
            raise ValueError(f"VaultRepresentativeSpec.module must be absolute, got {self.module!r}")

    def load_table(self) -> Mapping[str, Mapping[str, str]]:
        """Import and return ``{chain: {vault, underlying}}`` metadata."""
        module = importlib.import_module(self.module)
        table = getattr(module, self.attribute, None)
        if not isinstance(table, dict):
            raise TypeError(
                f"VaultRepresentativeSpec for protocol {self.protocol!r} maps to "
                f"{self.module}.{self.attribute}, but that attribute is "
                f"{type(table).__name__}, not a dict."
            )
        bad_rows = [
            chain
            for chain, row in table.items()
            if not isinstance(chain, str)
            or not chain.strip()
            or not isinstance(row, dict)
            or not isinstance(row.get("vault"), str)
            or not row.get("vault", "").strip()
            or not isinstance(row.get("underlying"), str)
            or not row.get("underlying", "").strip()
        ]
        if bad_rows:
            raise TypeError(f"VaultRepresentativeSpec for protocol {self.protocol!r} has invalid row(s): {bad_rows!r}")
        seen_chains: set[str] = set()
        duplicate_chains: list[str] = []
        for chain in table:
            normalized_chain = chain.strip().lower()
            if normalized_chain in seen_chains:
                duplicate_chains.append(chain)
            seen_chains.add(normalized_chain)
        if duplicate_chains:
            raise ValueError(
                f"VaultRepresentativeSpec for protocol {self.protocol!r} has duplicate chain keys after "
                f"normalization: {duplicate_chains!r}"
            )
        return table


class VaultRepresentativeRegistry:
    """Protocol -> representative vault table registry."""

    _cache: ClassVar[Mapping[str, Mapping[str, Mapping[str, str]]] | None] = None

    @classmethod
    def _load(cls) -> Mapping[str, Mapping[str, Mapping[str, str]]]:
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        tables: dict[str, Mapping[str, Mapping[str, str]]] = {}
        for connector_manifest in CONNECTOR_REGISTRY.with_vault_representatives():
            specs = connector_manifest.vault_representatives
            if specs is None:
                continue
            for spec in specs:
                if spec.protocol in tables:
                    raise ValueError(f"Vault representative protocol {spec.protocol!r} is declared twice")
                table = spec.load_table()
                tables[spec.protocol] = MappingProxyType(
                    {chain.strip().lower(): MappingProxyType(dict(row)) for chain, row in table.items()}
                )
        return MappingProxyType(tables)

    @classmethod
    def all(cls) -> Mapping[str, Mapping[str, Mapping[str, str]]]:
        """Return every connector-owned representative vault table."""
        if cls._cache is None:
            cls._cache = cls._load()
        return cls._cache

    @classmethod
    def table(cls, protocol: str) -> Mapping[str, Mapping[str, str]]:
        """Return representative vaults for ``protocol`` or an empty table."""
        return cls.all().get(protocol.lower(), MappingProxyType({}))

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: clear resolved representative vault metadata."""
        cls._cache = None


__all__ = ["VaultRepresentativeRegistry", "VaultRepresentativeSpec"]
