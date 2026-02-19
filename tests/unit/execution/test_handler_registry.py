"""Tests for the ExecutionHandlerRegistry."""

from unittest.mock import Mock

import pytest

from almanak.framework.execution.handler_registry import ExecutionHandler, ExecutionHandlerRegistry


class MockHandler:
    """Mock handler for testing."""

    def __init__(self, protocols: list[str], can_handle_result: bool = True):
        self._protocols = protocols
        self._can_handle_result = can_handle_result
        self._execute_called = False

    @property
    def supported_protocols(self) -> list[str]:
        return self._protocols

    def can_handle(self, bundle) -> bool:
        return self._can_handle_result

    async def execute(self, bundle):
        self._execute_called = True
        return {"success": True}


def test_registry_initialization():
    """Test that registry initializes empty."""
    registry = ExecutionHandlerRegistry()
    assert len(registry.get_all_handlers()) == 0
    assert len(registry.get_registered_protocols()) == 0


def test_register_handler():
    """Test registering a handler."""
    registry = ExecutionHandlerRegistry()
    handler = MockHandler(["test_protocol"])

    registry.register(handler)

    assert len(registry.get_all_handlers()) == 1
    assert "test_protocol" in registry.get_registered_protocols()


def test_register_duplicate_protocol_fails():
    """Test that registering the same protocol twice raises ValueError."""
    registry = ExecutionHandlerRegistry()
    handler1 = MockHandler(["test_protocol"])
    handler2 = MockHandler(["test_protocol"])

    registry.register(handler1)

    with pytest.raises(ValueError, match="already registered"):
        registry.register(handler2)


def test_get_handler_by_protocol_fast_path():
    """Test getting handler via fast path (protocol lookup)."""
    registry = ExecutionHandlerRegistry()
    handler = MockHandler(["polymarket"])

    registry.register(handler)

    # Create mock bundle with protocol metadata
    bundle = Mock()
    bundle.metadata = {"protocol": "polymarket"}
    bundle.transactions = []

    result = registry.get_handler(bundle)
    assert result == handler


def test_get_handler_slow_path():
    """Test getting handler via slow path (can_handle check)."""
    registry = ExecutionHandlerRegistry()

    # Handler with no specific protocols (fallback)
    handler = MockHandler([])
    registry.register(handler)

    # Create mock bundle without protocol metadata
    bundle = Mock()
    bundle.metadata = {}
    bundle.transactions = []

    result = registry.get_handler(bundle)
    assert result == handler


def test_get_handler_returns_none_when_no_match():
    """Test that get_handler returns None when no handler matches."""
    registry = ExecutionHandlerRegistry()
    handler = MockHandler(["polymarket"], can_handle_result=False)

    registry.register(handler)

    # Create bundle that handler won't handle
    bundle = Mock()
    bundle.metadata = {"protocol": "other_protocol"}
    bundle.transactions = []

    result = registry.get_handler(bundle)
    assert result is None


def test_multiple_handlers_registration():
    """Test registering multiple handlers."""
    registry = ExecutionHandlerRegistry()

    handler1 = MockHandler(["polymarket"])
    handler2 = MockHandler(["hyperliquid"])
    handler3 = MockHandler([])  # Fallback handler

    registry.register(handler1)
    registry.register(handler2)
    registry.register(handler3)

    assert len(registry.get_all_handlers()) == 3
    assert set(registry.get_registered_protocols()) == {"polymarket", "hyperliquid"}


def test_clear_registry():
    """Test clearing the registry."""
    registry = ExecutionHandlerRegistry()
    handler = MockHandler(["test_protocol"])

    registry.register(handler)
    assert len(registry.get_all_handlers()) == 1

    registry.clear()
    assert len(registry.get_all_handlers()) == 0
    assert len(registry.get_registered_protocols()) == 0


def test_handler_priority_order():
    """Test that handlers are checked in registration order."""
    registry = ExecutionHandlerRegistry()

    handler1 = MockHandler([], can_handle_result=False)
    handler2 = MockHandler([], can_handle_result=True)  # This should match

    registry.register(handler1)
    registry.register(handler2)

    bundle = Mock()
    bundle.metadata = {}
    bundle.transactions = []

    result = registry.get_handler(bundle)
    # handler2 should be returned as it's the first one that can_handle returns True
    assert result == handler2


def test_isinstance_check_for_protocol():
    """Test that isinstance check works for ExecutionHandler protocol."""
    handler = MockHandler(["test"])
    assert isinstance(handler, ExecutionHandler)


def test_non_protocol_handler_raises_typeerror():
    """Test that registering non-protocol handler raises TypeError."""
    registry = ExecutionHandlerRegistry()

    # Object without required methods
    invalid_handler = Mock(spec=[])  # No methods

    with pytest.raises(TypeError, match="must implement ExecutionHandler protocol"):
        registry.register(invalid_handler)
