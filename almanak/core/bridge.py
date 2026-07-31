"""Canonical bridge-transfer lifecycle projection for SDK consumers.

Bridge connectors and execution state providers expose richer, protocol-specific
state machines.  Dashboard consumers need the smaller, wire-stable lifecycle
projection defined here.  Keeping it in :mod:`almanak.core` lets framework and
gateway adapters share the type without either layer importing connector code.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import assert_never

__all__ = [
    "BridgeTransferStatus",
    "BridgeTransferStatusBehavior",
    "parse_bridge_transfer_status",
]


@dataclass(frozen=True, slots=True)
class BridgeTransferStatusBehavior:
    """Lifecycle semantics for a dashboard bridge-transfer status."""

    is_in_flight: bool
    is_terminal: bool
    is_success: bool
    is_unknown: bool


_IN_FLIGHT_BEHAVIOR = BridgeTransferStatusBehavior(
    is_in_flight=True,
    is_terminal=False,
    is_success=False,
    is_unknown=False,
)
_COMPLETED_BEHAVIOR = BridgeTransferStatusBehavior(
    is_in_flight=False,
    is_terminal=True,
    is_success=True,
    is_unknown=False,
)
_FAILED_BEHAVIOR = BridgeTransferStatusBehavior(
    is_in_flight=False,
    is_terminal=True,
    is_success=False,
    is_unknown=False,
)
_UNKNOWN_BEHAVIOR = BridgeTransferStatusBehavior(
    is_in_flight=False,
    is_terminal=False,
    is_success=False,
    is_unknown=True,
)


class BridgeTransferStatus(StrEnum):
    """Wire-stable lifecycle reported for a dashboard bridge transfer.

    ``UNKNOWN`` is the fail-safe projection for a future or malformed external
    value.  It is deliberately neither in flight nor terminal and can never be
    interpreted as successful completion.
    """

    IN_FLIGHT = "IN_FLIGHT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"

    @property
    def behavior(self) -> BridgeTransferStatusBehavior:
        """Return exhaustive lifecycle semantics for this status."""
        match self:
            case BridgeTransferStatus.IN_FLIGHT:
                return _IN_FLIGHT_BEHAVIOR
            case BridgeTransferStatus.COMPLETED:
                return _COMPLETED_BEHAVIOR
            case BridgeTransferStatus.FAILED:
                return _FAILED_BEHAVIOR
            case BridgeTransferStatus.UNKNOWN:
                return _UNKNOWN_BEHAVIOR
        assert_never(self)


def parse_bridge_transfer_status(value: object) -> BridgeTransferStatus:
    """Parse an external, persisted, or API value without fabricating success.

    Known values are exact and case-sensitive because their uppercase spellings
    are an existing serialization contract.  Any future, malformed, or absent
    value maps to ``UNKNOWN`` so callers can surface it separately from both
    active and terminal transfers.
    """
    if isinstance(value, BridgeTransferStatus):
        return value
    if isinstance(value, str):
        try:
            return BridgeTransferStatus(value)
        except ValueError:
            pass
    return BridgeTransferStatus.UNKNOWN
