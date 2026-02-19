"""Tests for EventRegistry."""

from enum import Enum

from almanak.framework.connectors.base.registry import EventRegistry


class MockEventType(Enum):
    """Mock event type enum for testing."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    COLLECT = "COLLECT"
    UNKNOWN = "UNKNOWN"


# Sample event topics (shortened for readability)
EVENT_TOPICS = {
    "Swap": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    "Mint": "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
    "Burn": "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
    "Collect": "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0",
}

EVENT_NAME_TO_TYPE = {
    "Swap": MockEventType.SWAP,
    "Mint": MockEventType.MINT,
    "Burn": MockEventType.BURN,
    "Collect": MockEventType.COLLECT,
}


class TestEventRegistryCreation:
    """Tests for creating EventRegistry instances."""

    def test_create_basic_registry(self):
        """Test creating a basic registry."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        assert len(registry) == 4
        assert registry.event_topics == EVENT_TOPICS
        assert len(registry.topic_to_event) == 4
        assert len(registry.known_topics) == 4

    def test_create_empty_registry(self):
        """Test creating empty registry."""
        registry = EventRegistry({}, {})

        assert len(registry) == 0
        assert len(registry.known_topics) == 0

    def test_registry_creates_reverse_lookup(self):
        """Test that registry creates topic_to_event reverse mapping."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        # Check reverse lookup was created
        swap_topic = EVENT_TOPICS["Swap"]
        assert registry.topic_to_event[swap_topic] == "Swap"

        mint_topic = EVENT_TOPICS["Mint"]
        assert registry.topic_to_event[mint_topic] == "Mint"


class TestGetEventName:
    """Tests for getting event name from topic."""

    def test_get_event_name_valid(self):
        """Test getting event name for valid topic."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        swap_topic = EVENT_TOPICS["Swap"]
        result = registry.get_event_name(swap_topic)

        assert result == "Swap"

    def test_get_event_name_all_events(self):
        """Test getting event names for all registered events."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        for event_name, topic in EVENT_TOPICS.items():
            result = registry.get_event_name(topic)
            assert result == event_name

    def test_get_event_name_unknown(self):
        """Test getting event name for unknown topic."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        result = registry.get_event_name("0xunknown_topic_hash")

        assert result is None

    def test_get_event_name_empty_string(self):
        """Test getting event name for empty string."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        result = registry.get_event_name("")

        assert result is None


class TestGetEventType:
    """Tests for getting event type from event name."""

    def test_get_event_type_valid(self):
        """Test getting event type for valid name."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        result = registry.get_event_type("Swap")

        assert result == MockEventType.SWAP

    def test_get_event_type_all_events(self):
        """Test getting event types for all registered events."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        for event_name, event_type in EVENT_NAME_TO_TYPE.items():
            result = registry.get_event_type(event_name)
            assert result == event_type

    def test_get_event_type_unknown(self):
        """Test getting event type for unknown name."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        result = registry.get_event_type("UnknownEvent")

        assert result is None

    def test_get_event_type_case_sensitive(self):
        """Test that event name lookup is case sensitive."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        result = registry.get_event_type("swap")  # lowercase

        assert result is None  # Should not match "Swap"


class TestGetEventTypeFromTopic:
    """Tests for getting event type directly from topic."""

    def test_get_event_type_from_topic_valid(self):
        """Test getting event type from valid topic."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        swap_topic = EVENT_TOPICS["Swap"]
        result = registry.get_event_type_from_topic(swap_topic)

        assert result == MockEventType.SWAP

    def test_get_event_type_from_topic_all_events(self):
        """Test getting event types from all topics."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        for event_name, topic in EVENT_TOPICS.items():
            result = registry.get_event_type_from_topic(topic)
            expected_type = EVENT_NAME_TO_TYPE[event_name]
            assert result == expected_type

    def test_get_event_type_from_topic_unknown(self):
        """Test getting event type from unknown topic."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        result = registry.get_event_type_from_topic("0xunknown")

        assert result is None


class TestIsKnownEvent:
    """Tests for checking if topic is known."""

    def test_is_known_event_true(self):
        """Test that known events return True."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        swap_topic = EVENT_TOPICS["Swap"]
        assert registry.is_known_event(swap_topic) is True

    def test_is_known_event_all_topics(self):
        """Test that all registered topics are known."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        for topic in EVENT_TOPICS.values():
            assert registry.is_known_event(topic) is True

    def test_is_known_event_false(self):
        """Test that unknown events return False."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        assert registry.is_known_event("0xunknown") is False

    def test_is_known_event_empty(self):
        """Test empty string is not known."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        assert registry.is_known_event("") is False


class TestGetTopicSignature:
    """Tests for getting topic signature from event name."""

    def test_get_topic_signature_valid(self):
        """Test getting topic signature for valid event name."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        result = registry.get_topic_signature("Swap")

        assert result == EVENT_TOPICS["Swap"]

    def test_get_topic_signature_all_events(self):
        """Test getting topic signatures for all events."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        for event_name, topic in EVENT_TOPICS.items():
            result = registry.get_topic_signature(event_name)
            assert result == topic

    def test_get_topic_signature_unknown(self):
        """Test getting topic signature for unknown event."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        result = registry.get_topic_signature("UnknownEvent")

        assert result is None


class TestRegistryOperators:
    """Tests for special methods and operators."""

    def test_len(self):
        """Test __len__ operator."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        assert len(registry) == 4

    def test_len_empty(self):
        """Test __len__ on empty registry."""
        registry = EventRegistry({}, {})

        assert len(registry) == 0

    def test_contains_operator(self):
        """Test 'in' operator."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        swap_topic = EVENT_TOPICS["Swap"]
        assert swap_topic in registry
        assert "0xunknown" not in registry

    def test_contains_all_topics(self):
        """Test that all registered topics are in registry."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        for topic in EVENT_TOPICS.values():
            assert topic in registry

    def test_repr(self):
        """Test __repr__ string representation."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        repr_str = repr(registry)

        assert "EventRegistry" in repr_str
        assert "events=4" in repr_str


class TestRegistryEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_single_event_registry(self):
        """Test registry with single event."""
        single_topics = {"Swap": EVENT_TOPICS["Swap"]}
        single_types = {"Swap": MockEventType.SWAP}

        registry = EventRegistry(single_topics, single_types)

        assert len(registry) == 1
        assert registry.get_event_name(EVENT_TOPICS["Swap"]) == "Swap"

    def test_registry_with_missing_type_mapping(self):
        """Test registry when event name has no type mapping."""
        topics = {"Swap": EVENT_TOPICS["Swap"], "UnmappedEvent": "0xabc123"}
        types = {"Swap": MockEventType.SWAP}  # UnmappedEvent not in types

        registry = EventRegistry(topics, types)

        # Should still work for topics
        assert registry.get_event_name("0xabc123") == "UnmappedEvent"

        # But type lookup returns None
        assert registry.get_event_type("UnmappedEvent") is None

    def test_duplicate_topic_values(self):
        """Test behavior with duplicate topic hashes (shouldn't happen in practice)."""
        # If two event names map to same topic, reverse lookup will only keep one
        duplicate_topics = {
            "Event1": "0xsame_hash",
            "Event2": "0xsame_hash",  # Same hash as Event1
        }
        types = {
            "Event1": MockEventType.SWAP,
            "Event2": MockEventType.MINT,
        }

        registry = EventRegistry(duplicate_topics, types)

        # Only one will be in reverse lookup (last one wins in dict comprehension)
        result = registry.get_event_name("0xsame_hash")
        assert result in ["Event1", "Event2"]


class TestRegistryWithDifferentEnums:
    """Tests for using registry with different enum types."""

    def test_multiple_enum_types(self):
        """Test that registry works with different enum types."""

        class ProtocolAEventType(Enum):
            SWAP = "SWAP"
            ADD_LIQUIDITY = "ADD_LIQUIDITY"

        class ProtocolBEventType(Enum):
            DEPOSIT = "DEPOSIT"
            WITHDRAW = "WITHDRAW"

        topics_a = {
            "Swap": "0xaaa",
            "AddLiquidity": "0xbbb",
        }
        types_a = {
            "Swap": ProtocolAEventType.SWAP,
            "AddLiquidity": ProtocolAEventType.ADD_LIQUIDITY,
        }

        topics_b = {
            "Deposit": "0xccc",
            "Withdraw": "0xddd",
        }
        types_b = {
            "Deposit": ProtocolBEventType.DEPOSIT,
            "Withdraw": ProtocolBEventType.WITHDRAW,
        }

        registry_a = EventRegistry(topics_a, types_a)
        registry_b = EventRegistry(topics_b, types_b)

        assert registry_a.get_event_type("Swap") == ProtocolAEventType.SWAP
        assert registry_b.get_event_type("Deposit") == ProtocolBEventType.DEPOSIT


class TestRegistryIntegration:
    """Integration tests for common usage patterns."""

    def test_parse_log_workflow(self):
        """Test typical workflow for parsing logs."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        # Simulate parsing a log with Swap event
        log_topic = EVENT_TOPICS["Swap"]

        # 1. Check if known event
        assert registry.is_known_event(log_topic)

        # 2. Get event name
        event_name = registry.get_event_name(log_topic)
        assert event_name == "Swap"

        # 3. Get event type
        event_type = registry.get_event_type(event_name)
        assert event_type == MockEventType.SWAP

    def test_parse_unknown_log(self):
        """Test workflow for unknown log."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        unknown_topic = "0xunknown_hash"

        # Should gracefully handle unknown events
        assert not registry.is_known_event(unknown_topic)
        assert registry.get_event_name(unknown_topic) is None
        assert registry.get_event_type_from_topic(unknown_topic) is None

    def test_filter_logs_by_registry(self):
        """Test filtering logs using registry."""
        registry = EventRegistry(EVENT_TOPICS, EVENT_NAME_TO_TYPE)

        # Simulate list of log topics
        log_topics = [
            EVENT_TOPICS["Swap"],
            "0xunknown1",
            EVENT_TOPICS["Mint"],
            "0xunknown2",
            EVENT_TOPICS["Burn"],
        ]

        # Filter to known events
        known_topics = [t for t in log_topics if registry.is_known_event(t)]

        assert len(known_topics) == 3
        assert known_topics[0] == EVENT_TOPICS["Swap"]
        assert known_topics[1] == EVENT_TOPICS["Mint"]
        assert known_topics[2] == EVENT_TOPICS["Burn"]
