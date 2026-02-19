"""Base infrastructure for receipt parsers.

This package provides reusable components for parsing transaction receipts
across different DeFi protocols, reducing code duplication and providing
consistent patterns for event decoding and receipt parsing.

Core Components:
- BaseEvent: Generic event wrapper using TypeVar for protocol-specific event types
- EventRegistry: Manages event topic mappings and lookups
- HexDecoder: Utilities for decoding hex-encoded event data
- BaseReceiptParser: Abstract base class with template method pattern

Example:
    from almanak.framework.connectors.base import (
        BaseEvent,
        EventRegistry,
        HexDecoder,
        BaseReceiptParser,
    )

    # Create custom parser
    class MyProtocolParser(BaseReceiptParser[MyEventType, MyResult]):
        def _decode_log_data(self, event_name, topics, data, contract_address):
            # Protocol-specific decoding logic
            ...

        def _create_event(self, event_name, log_index, ...):
            # Create protocol-specific event
            ...

        def _build_result(self, events, receipt, **kwargs):
            # Build protocol-specific result
            ...
"""

from almanak.framework.connectors.base.event import BaseEvent
from almanak.framework.connectors.base.hex_utils import HexDecoder
from almanak.framework.connectors.base.receipt_parser import BaseReceiptParser, ParseResult
from almanak.framework.connectors.base.registry import EventRegistry

__all__ = [
    "BaseEvent",
    "EventRegistry",
    "HexDecoder",
    "BaseReceiptParser",
    "ParseResult",
]
