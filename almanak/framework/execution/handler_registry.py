"""Execution Handler Registry for Protocol-Agnostic Bundle Routing.

This module provides the handler registry pattern to replace string-based protocol
detection in the execution layer. It defines a standard interface for execution
handlers and a registry for mapping bundles to appropriate handlers.

ARCHITECTURE
============

The handler registry pattern decouples the execution layer from protocol-specific
logic, making it easy to add new protocols without modifying PlanExecutor:

1. HANDLER INTERFACE (ExecutionHandler Protocol)
   - can_handle(bundle) - Detection logic
   - execute(bundle) - Execution logic
   - supported_protocols - Protocol names this handler supports

2. HANDLER REGISTRY
   - Maintains mapping of protocols to handlers
   - Routes bundles to appropriate handlers
   - Supports both fast-path (protocol lookup) and slow-path (can_handle check)

3. CONCRETE HANDLERS
   - Implement ExecutionHandler protocol
   - Register themselves with the registry
   - Handle protocol-specific execution

USAGE
=====

Basic setup:

    # Create registry
    registry = ExecutionHandlerRegistry()

    # Register handlers
    polymarket_handler = PolymarketClobHandler(clob_client)
    registry.register(polymarket_handler)

    onchain_handler = OnChainHandler(orchestrator)
    registry.register(onchain_handler)

    # Use in executor
    executor = PlanExecutor(config, handler_registry=registry)

Adding new protocols:

    # Just register a new handler - no PlanExecutor changes needed
    hyperliquid_handler = HyperliquidClobHandler(client)
    registry.register(hyperliquid_handler)

MIGRATION FROM STRING-BASED DETECTION
======================================

Before (string-based):
    if bundle.metadata.get("protocol") == "polymarket":
        result = await clob_handler.execute(bundle)
    else:
        result = await orchestrator.execute(bundle)

After (registry-based):
    handler = registry.get_handler(bundle)
    result = await handler.execute(bundle)

See Also:
    - notes/tech-debt/string-based-protocol-detection.md
    - almanak/framework/execution/clob_handler.py
    - almanak/framework/execution/plan_executor.py
"""

import logging
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


# =============================================================================
# Handler Protocol
# =============================================================================


@runtime_checkable
class ExecutionHandler(Protocol):
    """Protocol for bundle execution handlers.

    All execution handlers must implement this interface to be registered
    with ExecutionHandlerRegistry.

    The protocol defines three key methods:
    1. can_handle() - Determines if handler can execute the bundle
    2. execute() - Executes the bundle
    3. supported_protocols - Lists protocol names this handler supports

    Example:
        class MyHandler:
            @property
            def supported_protocols(self) -> list[str]:
                return ["my_protocol"]

            def can_handle(self, bundle: ActionBundle) -> bool:
                return bundle.metadata.get("protocol") == "my_protocol"

            async def execute(self, bundle: ActionBundle) -> ExecutionResult:
                # Custom execution logic
                ...
    """

    @property
    @abstractmethod
    def supported_protocols(self) -> list[str]:
        """List of protocol names this handler supports.

        This is used for fast-path routing in the registry.
        Return empty list for fallback handlers that don't claim
        specific protocols.

        Returns:
            List of protocol names (e.g., ["polymarket", "hyperliquid"])
        """
        ...

    @abstractmethod
    def can_handle(self, bundle: "ActionBundle") -> bool:
        """Check if this handler can execute the bundle.

        This method implements the detection logic for determining
        whether this handler is appropriate for the given bundle.

        Args:
            bundle: ActionBundle to check

        Returns:
            True if this handler can execute the bundle, False otherwise
        """
        ...

    @abstractmethod
    async def execute(self, bundle: "ActionBundle") -> Any:
        """Execute the bundle.

        Args:
            bundle: ActionBundle to execute

        Returns:
            Execution result (type depends on handler)
            - ClobExecutionResult for CLOB handlers
            - StepExecutionResult or similar for on-chain handlers

        Raises:
            Exception: If execution fails
        """
        ...


# =============================================================================
# Handler Registry
# =============================================================================


class ExecutionHandlerRegistry:
    """Registry for mapping protocols to execution handlers.

    The registry maintains two routing mechanisms:
    1. Fast path: Direct protocol name lookup (O(1))
    2. Slow path: Check all handlers via can_handle() (O(n))

    The fast path is used when bundle.metadata["protocol"] is set.
    The slow path is used for complex detection logic or fallback handlers.

    Example:
        registry = ExecutionHandlerRegistry()

        # Register CLOB handler
        clob_handler = PolymarketClobHandler(client)
        registry.register(clob_handler)  # Claims "polymarket" protocol

        # Register on-chain fallback
        onchain_handler = OnChainHandler(orchestrator)
        registry.register(onchain_handler)  # Claims no specific protocols

        # Route bundles
        handler = registry.get_handler(polymarket_bundle)  # Returns clob_handler
        handler = registry.get_handler(swap_bundle)  # Returns onchain_handler
    """

    def __init__(self) -> None:
        """Initialize empty registry."""
        self._handlers: list[ExecutionHandler] = []
        self._protocol_map: dict[str, ExecutionHandler] = {}

    def register(self, handler: ExecutionHandler) -> None:
        """Register a handler for its supported protocols.

        The handler will be added to both the handler list and the
        protocol mapping for fast lookup.

        Args:
            handler: Handler to register

        Raises:
            TypeError: If handler doesn't implement ExecutionHandler protocol
            ValueError: If a protocol is already registered to another handler
        """
        if not isinstance(handler, ExecutionHandler):
            raise TypeError(f"Handler must implement ExecutionHandler protocol, got {type(handler).__name__}")

        # Register in protocol map for fast lookup
        for protocol in handler.supported_protocols:
            if protocol in self._protocol_map:
                existing = self._protocol_map[protocol].__class__.__name__
                raise ValueError(
                    f"Protocol '{protocol}' already registered to {existing}. "
                    f"Cannot register to {handler.__class__.__name__}."
                )
            self._protocol_map[protocol] = handler
            logger.debug(
                "Registered handler for protocol",
                extra={
                    "protocol": protocol,
                    "handler": handler.__class__.__name__,
                },
            )

        # Add to handler list for slow-path lookup
        self._handlers.append(handler)
        logger.info(
            "Registered execution handler",
            extra={
                "handler": handler.__class__.__name__,
                "protocols": handler.supported_protocols or ["<fallback>"],
            },
        )

    def get_handler(self, bundle: "ActionBundle") -> ExecutionHandler | None:
        """Get handler for a bundle.

        Routing strategy:
        1. Fast path: Look up by bundle.metadata["protocol"] (O(1))
        2. Validate: Ensure handler.can_handle() returns True
        3. Slow path: Check all handlers via can_handle() (O(n))
        4. Return None if no handler found

        Args:
            bundle: Bundle to route

        Returns:
            Handler if found, None otherwise

        Example:
            handler = registry.get_handler(bundle)
            if handler is None:
                raise ValueError("No handler found for bundle")
            result = await handler.execute(bundle)
        """
        # Fast path: protocol in metadata
        protocol = bundle.metadata.get("protocol")
        if protocol and protocol in self._protocol_map:
            handler = self._protocol_map[protocol]

            # Validate handler can actually handle this bundle
            if handler.can_handle(bundle):
                logger.debug(
                    "Routed bundle via fast path",
                    extra={
                        "protocol": protocol,
                        "handler": handler.__class__.__name__,
                    },
                )
                return handler

            # Protocol mismatch - log warning
            logger.warning(
                f"Protocol '{protocol}' registered to {handler.__class__.__name__} "
                f"but handler.can_handle() returned False",
                extra={
                    "bundle_metadata": bundle.metadata,
                    "bundle_has_transactions": bool(bundle.transactions),
                },
            )

        # Slow path: check all handlers
        for handler in self._handlers:
            if handler.can_handle(bundle):
                logger.debug(
                    "Routed bundle via slow path",
                    extra={
                        "handler": handler.__class__.__name__,
                        "protocol": bundle.metadata.get("protocol", "<none>"),
                    },
                )
                return handler

        # No handler found
        logger.warning(
            "No handler found for bundle",
            extra={
                "protocol": bundle.metadata.get("protocol", "<none>"),
                "has_transactions": bool(bundle.transactions),
                "bundle_metadata_keys": list(bundle.metadata.keys()),
            },
        )
        return None

    def get_registered_protocols(self) -> list[str]:
        """Get list of all registered protocols.

        Returns:
            Sorted list of protocol names
        """
        return sorted(self._protocol_map.keys())

    def get_all_handlers(self) -> list[ExecutionHandler]:
        """Get list of all registered handlers.

        Returns:
            List of handlers in registration order
        """
        return self._handlers.copy()

    def clear(self) -> None:
        """Clear all registered handlers.

        Useful for testing or resetting the registry.
        """
        self._handlers.clear()
        self._protocol_map.clear()
        logger.info("Cleared execution handler registry")
