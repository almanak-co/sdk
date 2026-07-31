"""Canonical strategy runtime mode vocabulary.

Runtime mode controls runner and accounting failure semantics.  It is
deliberately separate from :class:`almanak.config.runtime.SigningMode`, which
selects the transaction-signing topology.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal


class RunMode(StrEnum):
    """Strategy runtime/accounting mode persisted on financial records."""

    DRY_RUN = "dry_run"
    PAPER = "paper"
    LIVE = "live"

    @classmethod
    def parse(cls, value: RunMode | str) -> RunMode:
        """Parse a run mode at a configuration, wire, or persistence boundary."""
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise TypeError(f"run mode must be a string or RunMode, got {type(value).__name__}")
        try:
            return cls(value.strip().lower())
        except ValueError as exc:
            valid = ", ".join(mode.value for mode in cls)
            raise ValueError(f"invalid run mode {value!r}; expected one of: {valid}") from exc

    @classmethod
    def parse_optional(cls, value: RunMode | str | None) -> RunModeStamp:
        """Parse a legacy boundary where an empty string means unstamped."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return ""
        return cls.parse(value)


type RunModeStamp = RunMode | Literal[""]


def serialize_run_mode(mode: RunModeStamp) -> str:
    """Return the stable database/protobuf representation for ``mode``."""
    return RunMode.parse(mode).value if mode else ""
