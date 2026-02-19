"""Generic event wrapper for receipt parsers.

This module provides a generic event dataclass that can be parameterized
with protocol-specific event types using TypeVar. This allows receipt parsers
to share a common event structure while maintaining type safety for
protocol-specific event types.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, TypeVar

# TypeVar for protocol-specific event types
TEventType = TypeVar("TEventType", bound=Enum)


@dataclass
class BaseEvent[TEventType: Enum]:
    """Generic event wrapper for protocol-specific events.

    This class provides a common structure for all parsed events, regardless
    of protocol. The event_type field is parameterized with a TypeVar, allowing
    each protocol to use its own event type enum while sharing the base structure.

    Attributes:
        event_type: Protocol-specific event type (e.g., UniswapV3EventType.SWAP)
        event_name: Human-readable event name (e.g., "Swap")
        log_index: Index of this log in the transaction
        transaction_hash: Transaction hash that emitted this event
        block_number: Block number where event was emitted
        contract_address: Contract that emitted the event
        data: Decoded event data (protocol-specific structure)
        raw_topics: Raw topic values from log entry
        raw_data: Raw data hex string from log entry
        timestamp: Event timestamp (defaults to current time)

    Example:
        >>> from enum import Enum
        >>> from almanak.framework.connectors.base import BaseEvent
        >>>
        >>> class MyEventType(Enum):
        ...     SWAP = "SWAP"
        ...     MINT = "MINT"
        >>>
        >>> event = BaseEvent[MyEventType](
        ...     event_type=MyEventType.SWAP,
        ...     event_name="Swap",
        ...     log_index=5,
        ...     transaction_hash="0xabc...",
        ...     block_number=12345678,
        ...     contract_address="0xdef...",
        ...     data={"amount": 1000},
        ... )
    """

    event_type: TEventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] = field(default_factory=list)
    raw_data: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary.

        Returns:
            Dictionary representation with all fields. Event type is
            converted to its string value for JSON serialization.
        """
        return {
            "event_type": self.event_type.value if isinstance(self.event_type, Enum) else str(self.event_type),
            "event_name": self.event_name,
            "log_index": self.log_index,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "contract_address": self.contract_address,
            "data": self.data,
            "raw_topics": self.raw_topics,
            "raw_data": self.raw_data,
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self) -> str:
        """Get string representation of event."""
        return (
            f"{self.__class__.__name__}("
            f"type={self.event_type.value if isinstance(self.event_type, Enum) else self.event_type}, "
            f"name={self.event_name}, "
            f"log_index={self.log_index}, "
            f"tx={self.transaction_hash[:10]}..., "
            f"block={self.block_number})"
        )


__all__ = ["BaseEvent", "TEventType"]
