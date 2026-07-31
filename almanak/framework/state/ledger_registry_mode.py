"""Typed transaction semantics for atomic ledger/registry persistence.

This module intentionally depends only on the standard library so the framework,
gateway service, and storage adapters can share one persistence vocabulary without
creating an accounting/state/gateway import cycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import assert_never


@dataclass(frozen=True, slots=True)
class LedgerRegistrySaveBehavior:
    """The write set and reconciliation semantics for one atomic-save mode."""

    writes_ledger: bool
    is_reconciliation: bool


class LedgerRegistrySaveMode(StrEnum):
    """Internal modes supported by the atomic ledger/registry transaction."""

    COMMIT = "commit"
    REGISTRY_RECONCILIATION = "registry_reconciliation"

    @classmethod
    def parse_wire(cls, value: str) -> LedgerRegistrySaveMode:
        """Parse the protobuf representation at the StateService boundary.

        Proto3's empty-string default preserves the original commit behavior.
        No trimming or case folding is performed: the two existing wire values
        are an exact compatibility contract, and every other value is invalid.
        """
        if not isinstance(value, str):
            raise TypeError(f"ledger/registry save mode must be a string, got {type(value).__name__}")
        if value == "":
            return cls.COMMIT
        try:
            return cls(value)
        except ValueError as exc:
            valid = ", ".join(repr(mode.value) for mode in cls)
            raise ValueError(f"invalid ledger/registry save mode {value!r}; expected one of: {valid}") from exc

    @property
    def behavior(self) -> LedgerRegistrySaveBehavior:
        """Return the exhaustive atomic write policy for this mode.

        The explicit match is deliberate: adding an enum member forces the
        author to decide whether the ledger participates in the transaction and
        whether the operation has reconciliation semantics.
        """
        match self:
            case LedgerRegistrySaveMode.COMMIT:
                return LedgerRegistrySaveBehavior(writes_ledger=True, is_reconciliation=False)
            case LedgerRegistrySaveMode.REGISTRY_RECONCILIATION:
                return LedgerRegistrySaveBehavior(writes_ledger=False, is_reconciliation=True)
        assert_never(self)

    def to_wire(self) -> str:
        """Serialize without changing the established proto3 commit default."""
        return "" if self is LedgerRegistrySaveMode.COMMIT else self.value


def ledger_registry_save_behavior(mode: LedgerRegistrySaveMode) -> LedgerRegistrySaveBehavior:
    """Return the policy for a typed internal mode.

    This small runtime guard complements static checking for plugin/test callers
    that can otherwise pass a plain string to Python despite the annotation.
    Parsing remains exclusively a wire-boundary responsibility.
    """
    if not isinstance(mode, LedgerRegistrySaveMode):
        raise TypeError(
            f"internal ledger/registry persistence APIs require LedgerRegistrySaveMode, got {type(mode).__name__}"
        )
    return mode.behavior
