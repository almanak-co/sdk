"""Connector-owned Solana program clone declarations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SolanaProgramSpec:
    """A Solana program required by local solana-test-validator forks.

    Connectors publish these specs from their manifests so the framework's
    local-fork bootstrap does not import connector constants directly.
    """

    protocol: str
    program_id: str
    upgradeable: bool = True
    notes: str = ""

    def __post_init__(self) -> None:
        """Validate the clone spec without importing connector runtime code."""
        if not isinstance(self.protocol, str) or not self.protocol.strip():
            raise ValueError(f"SolanaProgramSpec.protocol must be a non-empty string, got {self.protocol!r}")
        if not isinstance(self.program_id, str) or not self.program_id.strip():
            raise ValueError(f"SolanaProgramSpec.program_id must be a non-empty string, got {self.program_id!r}")
        if any(char.isspace() for char in self.program_id):
            # Base58 program IDs never contain whitespace; a stray space or newline
            # is a copy-paste error that should fail at construction, not at clone time.
            raise ValueError(f"SolanaProgramSpec.program_id must not contain whitespace, got {self.program_id!r}")
        if not isinstance(self.upgradeable, bool):
            raise ValueError(f"SolanaProgramSpec.upgradeable must be a bool, got {self.upgradeable!r}")
        if not isinstance(self.notes, str):
            raise ValueError(f"SolanaProgramSpec.notes must be a string, got {self.notes!r}")

        normalized_protocol = self.protocol.strip().lower()
        if normalized_protocol != self.protocol:
            object.__setattr__(self, "protocol", normalized_protocol)


__all__ = ["SolanaProgramSpec"]
