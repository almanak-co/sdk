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
    from almanak.connectors._strategy_base.base import (
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

from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    BaseConcentratedLiquidityCompiler,
    BaseProtocolCompiler,
    CLAdapterFactoryContext,
    CLCompilerContext,
    CompilerServices,
)
from almanak.connectors._strategy_base.base.event import BaseEvent
from almanak.connectors._strategy_base.base.hex_utils import HexDecoder
from almanak.connectors._strategy_base.base.receipt_parser import (
    BaseReceiptParser,
    ParseResult,
    resolve_swap_token_symbol,
    resolve_swap_token_symbol_with_fallback,
)
from almanak.connectors._strategy_base.base.registry import EventRegistry
from almanak.connectors._strategy_base.base.swap_adapter import DefaultSwapAdapter

__all__ = [
    "BaseCompilerContext",
    "BaseConcentratedLiquidityCompiler",
    "BaseEvent",
    "BaseProtocolCompiler",
    "CLAdapterFactoryContext",
    "CLCompilerContext",
    "CompilerServices",
    "BaseReceiptParser",
    "DefaultSwapAdapter",
    "EventRegistry",
    "HexDecoder",
    "ParseResult",
    "resolve_swap_token_symbol",
    "resolve_swap_token_symbol_with_fallback",
]
