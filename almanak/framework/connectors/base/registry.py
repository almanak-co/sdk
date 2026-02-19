"""Event registry for managing event topic mappings.

This module provides the EventRegistry class for managing event topic signatures
and their corresponding event names and types. Eliminates duplicated topic
mapping dictionaries across receipt parsers.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class EventRegistry:
    """Registry for managing event topic mappings.

    This class manages the mapping between event topic signatures (keccak256 hashes)
    and event names/types. Provides fast lookup and validation methods.

    Attributes:
        event_topics: Mapping from event name to topic signature
        topic_to_event: Reverse mapping from topic signature to event name
        event_type_map: Mapping from event name to enum type
        known_topics: Set of all known topic signatures (for fast lookup)

    Example:
        >>> from enum import Enum
        >>> from almanak.framework.connectors.base import EventRegistry
        >>>
        >>> class MyEventType(Enum):
        ...     SWAP = "SWAP"
        ...     MINT = "MINT"
        >>>
        >>> EVENT_TOPICS = {
        ...     "Swap": "0xc42079...",
        ...     "Mint": "0x7a5308...",
        ... }
        >>>
        >>> EVENT_NAME_TO_TYPE = {
        ...     "Swap": MyEventType.SWAP,
        ...     "Mint": MyEventType.MINT,
        ... }
        >>>
        >>> registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)
        >>> registry.get_event_name("0xc42079...")
        'Swap'
        >>> registry.get_event_type("Swap")
        <MyEventType.SWAP: 'SWAP'>
    """

    def __init__(
        self,
        event_topics: dict[str, str],
        event_type_map: dict[str, Any],
    ) -> None:
        """Initialize the event registry.

        Args:
            event_topics: Mapping from event name to topic signature
                Example: {"Swap": "0xc42079...", "Mint": "0x7a5308..."}
            event_type_map: Mapping from event name to enum type
                Example: {"Swap": MyEventType.SWAP, "Mint": MyEventType.MINT}
        """
        self.event_topics = event_topics
        self.topic_to_event: dict[str, str] = {v: k for k, v in event_topics.items()}
        self.event_type_map = event_type_map
        self.known_topics = set(event_topics.values())

        logger.debug(f"Initialized EventRegistry with {len(event_topics)} event types")

    def get_event_name(self, topic: str) -> str | None:
        """Get event name from topic signature.

        Args:
            topic: Event topic signature (keccak256 hash)

        Returns:
            Event name or None if topic is unknown

        Example:
            >>> registry.get_event_name("0xc42079...")
            'Swap'
        """
        return self.topic_to_event.get(topic)

    def get_event_type(self, event_name: str) -> Any | None:
        """Get event type enum from event name.

        Args:
            event_name: Event name (e.g., "Swap")

        Returns:
            Event type enum or None if name is unknown

        Example:
            >>> registry.get_event_type("Swap")
            <MyEventType.SWAP: 'SWAP'>
        """
        return self.event_type_map.get(event_name)

    def get_event_type_from_topic(self, topic: str) -> Any | None:
        """Get event type enum directly from topic signature.

        Convenience method that combines get_event_name() and get_event_type().

        Args:
            topic: Event topic signature (keccak256 hash)

        Returns:
            Event type enum or None if topic is unknown

        Example:
            >>> registry.get_event_type_from_topic("0xc42079...")
            <MyEventType.SWAP: 'SWAP'>
        """
        event_name = self.get_event_name(topic)
        if event_name:
            return self.get_event_type(event_name)
        return None

    def is_known_event(self, topic: str) -> bool:
        """Check if a topic is a known event.

        Args:
            topic: Event topic signature to check

        Returns:
            True if topic is in the registry

        Example:
            >>> registry.is_known_event("0xc42079...")
            True
            >>> registry.is_known_event("0xunknown...")
            False
        """
        return topic in self.known_topics

    def get_topic_signature(self, event_name: str) -> str | None:
        """Get topic signature from event name.

        Args:
            event_name: Event name (e.g., "Swap")

        Returns:
            Topic signature or None if name is unknown

        Example:
            >>> registry.get_topic_signature("Swap")
            '0xc42079...'
        """
        return self.event_topics.get(event_name)

    def __len__(self) -> int:
        """Get number of registered events."""
        return len(self.event_topics)

    def __contains__(self, topic: str) -> bool:
        """Check if topic is in registry (allows 'in' operator)."""
        return topic in self.known_topics

    def __repr__(self) -> str:
        """Get string representation of registry."""
        return f"EventRegistry(events={len(self.event_topics)})"


__all__ = ["EventRegistry"]
