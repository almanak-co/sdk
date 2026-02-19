"""Abstract base class for receipt parsers.

This module provides the BaseReceiptParser abstract base class that implements
the template method pattern for parsing transaction receipts. Protocol-specific
parsers inherit from this class and implement hook methods for custom logic.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, TypeVar

from almanak.framework.connectors.base.registry import EventRegistry

logger = logging.getLogger(__name__)

# TypeVars for protocol-specific types
TEvent = TypeVar("TEvent")  # Protocol-specific event type (e.g., UniswapV3Event)
TResult = TypeVar("TResult")  # Protocol-specific result type (e.g., ParseResult)


@dataclass
class ParseResult[TResult]:
    """Generic parse result wrapper.

    This is a minimal wrapper that can be subclassed by protocol-specific
    result types. It provides common fields that all parsers need.

    Attributes:
        success: Whether parsing succeeded
        error: Error message if parsing failed
        transaction_hash: Transaction hash
        block_number: Block number
        transaction_success: Whether transaction succeeded (status=1)
    """

    success: bool
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0
    transaction_success: bool = True


class BaseReceiptParser[TEvent, TResult](ABC):
    """Abstract base class for receipt parsers using template method pattern.

    This class implements the common receipt parsing flow and provides hook
    methods for protocol-specific customization. Subclasses must implement:
    - _decode_log_data(): Protocol-specific log decoding
    - _create_event(): Create protocol-specific event object
    - _build_result(): Build protocol-specific result object

    The template method parse_receipt() handles:
    - Transaction status validation
    - Log iteration and filtering
    - Event creation and collection
    - Error handling

    Attributes:
        registry: EventRegistry for topic lookups (optional)
        known_topics: Set of known topic signatures (optional)
        SUPPORTED_EXTRACTIONS: Class-level frozenset declaring which extraction
            fields this parser supports (e.g., {"swap_amounts", "position_id"}).
            Used by ResultEnricher to warn when expected fields are unsupported.

    Example:
        >>> from almanak.framework.connectors.base import BaseReceiptParser
        >>>
        >>> class MyProtocolParser(BaseReceiptParser[MyEvent, MyResult]):
        ...     def __init__(self):
        ...         super().__init__(registry=my_registry)
        ...
        ...     def _decode_log_data(self, event_name, topics, data, contract_address):
        ...         # Decode protocol-specific log data
        ...         if event_name == "Swap":
        ...             return {
        ...                 "amount_in": HexDecoder.decode_uint256(data, 0),
        ...                 "amount_out": HexDecoder.decode_uint256(data, 32),
        ...             }
        ...         return {}
        ...
        ...     def _create_event(self, event_name, log_index, tx_hash, ...):
        ...         # Create protocol-specific event object
        ...         return MyEvent(...)
        ...
        ...     def _build_result(self, events, receipt, **kwargs):
        ...         # Build protocol-specific result
        ...         return MyResult(success=True, events=events)
    """

    # Subclasses should override this to declare supported extraction fields.
    # When set, ResultEnricher will warn about unsupported fields.
    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset()

    def __init__(
        self,
        registry: EventRegistry | None = None,
        known_topics: set[str] | None = None,
    ) -> None:
        """Initialize the base parser.

        Args:
            registry: EventRegistry for topic lookups (optional)
            known_topics: Set of known topic signatures (optional, used if no registry)
        """
        self.registry = registry
        self.known_topics = known_topics or (set(registry.known_topics) if registry else set())

    def parse_receipt(self, receipt: dict[str, Any], **kwargs) -> TResult:
        """Parse a transaction receipt (template method).

        This is the main entry point that implements the template method pattern.
        It handles common parsing logic and calls hook methods for protocol-specific
        customization.

        Flow:
        1. Validate transaction status
        2. Extract transaction metadata
        3. Iterate through logs
        4. For each log:
           a. Check if event is known
           b. Decode log data (via _decode_log_data hook)
           c. Create event object (via _create_event hook)
        5. Build final result (via _build_result hook)

        Args:
            receipt: Transaction receipt dict from web3.py containing:
                - transactionHash: Transaction hash (bytes or hex string)
                - blockNumber: Block number (int)
                - status: Transaction status (1=success, 0=reverted)
                - logs: List of log dicts
            **kwargs: Additional protocol-specific parameters

        Returns:
            Protocol-specific result object

        Raises:
            Exception: If parsing fails critically (caught and returned in result)
        """
        try:
            # Extract transaction metadata
            tx_hash = self._normalize_tx_hash(receipt.get("transactionHash", ""))
            block_number = receipt.get("blockNumber", 0)
            status = receipt.get("status", 1)
            tx_success = status == 1

            logs = receipt.get("logs", [])

            # Check transaction status
            if not tx_success:
                return self._build_failed_result(
                    tx_hash=tx_hash,
                    block_number=block_number,
                    error="Transaction reverted",
                )

            # Handle empty logs
            if not logs:
                return self._build_empty_result(
                    tx_hash=tx_hash,
                    block_number=block_number,
                )

            # Parse logs
            events: list[TEvent] = []

            for log in logs:
                # Parse single log
                event = self._parse_log(log, tx_hash, block_number)
                if event is not None:
                    events.append(event)

            # Build final result
            return self._build_result(
                events=events,
                receipt=receipt,
                tx_hash=tx_hash,
                block_number=block_number,
                tx_success=tx_success,
                **kwargs,
            )

        except Exception as e:
            logger.exception(f"Failed to parse receipt: {e}")
            return self._build_failed_result(
                tx_hash=receipt.get("transactionHash", ""),
                block_number=receipt.get("blockNumber", 0),
                error=str(e),
            )

    def _parse_log(
        self,
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
    ) -> TEvent | None:
        """Parse a single log entry.

        This method handles the common log parsing logic:
        - Extract topics and data
        - Check if event is known
        - Get event name and type
        - Decode log data
        - Create event object

        Args:
            log: Log dict containing topics, data, address, logIndex
            tx_hash: Transaction hash
            block_number: Block number

        Returns:
            Protocol-specific event object or None if event is unknown
        """
        try:
            topics = log.get("topics", [])
            if not topics:
                return None

            # Get first topic (event signature)
            first_topic = self._normalize_topic(topics[0])

            # Check if known event
            if not self._is_known_event(first_topic):
                return None

            # Get event name
            event_name = self._get_event_name(first_topic)
            if event_name is None:
                return None

            # Get contract address
            contract_address = self._normalize_address(log.get("address", ""))

            # Get raw data
            data = self._normalize_data(log.get("data", ""))

            # Decode log data (protocol-specific)
            decoded_data = self._decode_log_data(
                event_name=event_name,
                topics=topics,
                data=data,
                contract_address=contract_address,
            )

            # Convert topics to strings
            topics_str = [self._normalize_topic(t) for t in topics]

            # Create event object (protocol-specific)
            return self._create_event(
                event_name=event_name,
                log_index=log.get("logIndex", 0),
                tx_hash=tx_hash,
                block_number=block_number,
                contract_address=contract_address,
                decoded_data=decoded_data,
                raw_topics=topics_str,
                raw_data=data,
            )

        except Exception as e:
            logger.warning(f"Failed to parse log: {e}")
            return None

    # =========================================================================
    # Abstract Methods - Must be implemented by subclasses
    # =========================================================================

    @abstractmethod
    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        contract_address: str,
    ) -> dict[str, Any]:
        """Decode protocol-specific log data.

        This hook method is called to decode the raw log data into a
        structured dictionary. Implement protocol-specific decoding logic here.

        Args:
            event_name: Name of the event (e.g., "Swap", "Mint")
            topics: List of indexed topics (raw, not normalized)
            data: Hex-encoded event data (without 0x prefix)
            contract_address: Contract that emitted the event

        Returns:
            Dictionary with decoded event data

        Example:
            >>> def _decode_log_data(self, event_name, topics, data, contract_address):
            ...     if event_name == "Swap":
            ...         sender = HexDecoder.topic_to_address(topics[1])
            ...         recipient = HexDecoder.topic_to_address(topics[2])
            ...         amount0 = HexDecoder.decode_int256(data, 0)
            ...         amount1 = HexDecoder.decode_int256(data, 32)
            ...         return {
            ...             "sender": sender,
            ...             "recipient": recipient,
            ...             "amount0": amount0,
            ...             "amount1": amount1,
            ...         }
            ...     return {}
        """
        pass

    @abstractmethod
    def _create_event(
        self,
        event_name: str,
        log_index: int,
        tx_hash: str,
        block_number: int,
        contract_address: str,
        decoded_data: dict[str, Any],
        raw_topics: list[str],
        raw_data: str,
    ) -> TEvent | None:
        """Create protocol-specific event object.

        This hook method is called to create a protocol-specific event object
        from the decoded data.

        Args:
            event_name: Name of the event
            log_index: Index of log in transaction
            tx_hash: Transaction hash
            block_number: Block number
            contract_address: Contract address
            decoded_data: Decoded event data from _decode_log_data()
            raw_topics: Raw topic hex strings
            raw_data: Raw data hex string

        Returns:
            Protocol-specific event object

        Example:
            >>> def _create_event(self, event_name, log_index, tx_hash, ...):
            ...     event_type = self.registry.get_event_type(event_name)
            ...     return MyProtocolEvent(
            ...         event_type=event_type,
            ...         event_name=event_name,
            ...         log_index=log_index,
            ...         transaction_hash=tx_hash,
            ...         block_number=block_number,
            ...         contract_address=contract_address,
            ...         data=decoded_data,
            ...         raw_topics=raw_topics,
            ...         raw_data=raw_data,
            ...     )
        """
        pass

    @abstractmethod
    def _build_result(
        self,
        events: list[TEvent],
        receipt: dict[str, Any],
        tx_hash: str,
        block_number: int,
        tx_success: bool,
        **kwargs,
    ) -> TResult:
        """Build protocol-specific result object.

        This hook method is called to build the final result object from
        the parsed events and additional context.

        Args:
            events: List of parsed events
            receipt: Original receipt dict
            tx_hash: Transaction hash
            block_number: Block number
            tx_success: Whether transaction succeeded
            **kwargs: Additional protocol-specific parameters from parse_receipt()

        Returns:
            Protocol-specific result object

        Example:
            >>> def _build_result(self, events, receipt, tx_hash, block_number, tx_success, **kwargs):
            ...     # Extract swap events
            ...     swap_events = [e for e in events if e.event_type == MyEventType.SWAP]
            ...
            ...     # Build swap result if we have swap events
            ...     swap_result = None
            ...     if swap_events:
            ...         swap_result = self._build_swap_result(swap_events[0], **kwargs)
            ...
            ...     return MyParseResult(
            ...         success=True,
            ...         events=events,
            ...         swap_events=swap_events,
            ...         swap_result=swap_result,
            ...         transaction_hash=tx_hash,
            ...         block_number=block_number,
            ...         transaction_success=tx_success,
            ...     )
        """
        pass

    # =========================================================================
    # Hook Methods - Can be overridden by subclasses
    # =========================================================================

    def _build_failed_result(
        self,
        tx_hash: str,
        block_number: int,
        error: str,
    ) -> TResult:
        """Build result for failed transaction or parsing.

        Override this to customize error handling.

        Args:
            tx_hash: Transaction hash
            block_number: Block number
            error: Error message

        Returns:
            Protocol-specific result indicating failure
        """
        return self._build_result(
            events=[],
            receipt={},
            tx_hash=tx_hash,
            block_number=block_number,
            tx_success=False,
            error=error,
        )

    def _build_empty_result(
        self,
        tx_hash: str,
        block_number: int,
    ) -> TResult:
        """Build result for empty logs.

        Override this to customize empty log handling.

        Args:
            tx_hash: Transaction hash
            block_number: Block number

        Returns:
            Protocol-specific result for empty logs
        """
        return self._build_result(
            events=[],
            receipt={},
            tx_hash=tx_hash,
            block_number=block_number,
            tx_success=True,
        )

    # =========================================================================
    # Utility Methods - Used internally
    # =========================================================================

    def _is_known_event(self, topic: str) -> bool:
        """Check if topic is a known event."""
        if self.registry:
            return self.registry.is_known_event(topic)
        return topic in self.known_topics

    def _get_event_name(self, topic: str) -> str | None:
        """Get event name from topic."""
        if self.registry:
            return self.registry.get_event_name(topic)
        return None  # Subclass should override if not using registry

    @staticmethod
    def _normalize_tx_hash(tx_hash: Any) -> str:
        """Normalize transaction hash to hex string with 0x prefix."""
        if isinstance(tx_hash, bytes):
            result = tx_hash.hex()
            return result if result.startswith("0x") else "0x" + result
        return str(tx_hash) if tx_hash else ""

    @staticmethod
    def _normalize_topic(topic: Any) -> str:
        """Normalize topic to hex string with 0x prefix and lowercase.

        Lowercase normalization ensures compatibility with all RPC providers
        regardless of hex case format (some return uppercase, some lowercase).

        Note: HexBytes.hex() returns without 0x prefix, so we must ensure
        the 0x prefix is added for all string inputs too.
        """
        if isinstance(topic, bytes):
            return "0x" + topic.hex()
        result = str(topic).lower() if topic else ""
        # Ensure 0x prefix (HexBytes.hex() and some serializations omit it)
        if result and not result.startswith("0x"):
            result = "0x" + result
        return result

    @staticmethod
    def _normalize_address(address: Any) -> str:
        """Normalize address to hex string with 0x prefix."""
        if isinstance(address, bytes):
            return "0x" + address.hex()
        return str(address).lower() if address else ""

    @staticmethod
    def _normalize_data(data: Any) -> str:
        """Normalize data to hex string without 0x prefix."""
        if isinstance(data, bytes):
            return data.hex()
        elif isinstance(data, str):
            return data[2:] if data.startswith("0x") else data
        return ""


__all__ = ["BaseReceiptParser", "ParseResult"]
