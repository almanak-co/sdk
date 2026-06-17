"""Shared constants for the permission system."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Final

from almanak.connectors._strategy_base.vault_representatives import VaultRepresentativeRegistry


class _RepresentativeVaultMapping(Mapping[str, Mapping[str, Mapping[str, str]]]):
    """Lazy view over connector-owned representative vault metadata."""

    def _data(self) -> Mapping[str, Mapping[str, Mapping[str, str]]]:
        return VaultRepresentativeRegistry.all()

    def __getitem__(self, key: str) -> Mapping[str, Mapping[str, str]]:
        return self._data()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data())

    def __len__(self) -> int:
        return len(self._data())


class _ProtocolVaultMapping(Mapping[str, Mapping[str, str]]):
    """Lazy view over one protocol's representative vault metadata."""

    def __init__(self, protocol: str) -> None:
        self._protocol = protocol

    def _data(self) -> Mapping[str, Mapping[str, str]]:
        return VaultRepresentativeRegistry.table(self._protocol)

    def __getitem__(self, key: str) -> Mapping[str, str]:
        return self._data()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data())

    def __len__(self) -> int:
        return len(self._data())


# Per-protocol vault registry for synthetic intent generation, derived lazily
# from connector-owned representative vault declarations.
# Each entry maps chain -> {vault, underlying}.
VAULT_PROTOCOL_REPRESENTATIVE: Final[Mapping[str, Mapping[str, Mapping[str, str]]]] = _RepresentativeVaultMapping()

# Backwards-compatible public view for callers/tests that need MetaMorpho's
# representative vaults directly.
METAMORPHO_VAULTS: Final[Mapping[str, Mapping[str, str]]] = _ProtocolVaultMapping("metamorpho")

__all__ = ["METAMORPHO_VAULTS", "VAULT_PROTOCOL_REPRESENTATIVE"]
