"""Receipt Parser Registry — thin façade over the strategy-side connector registry.

Historically this module hardcoded a per-protocol dispatch table
(``_BUILTIN_LOADERS``) mapping every protocol key + alias to a
``(module_path, class_name)`` tuple. The framework loaded each parser
class lazily via ``importlib.import_module``. That table was the largest
single source of per-protocol coupling outside the connector layer —
~45 findings in the 2026-05-27 chain/protocol coupling audit.

VIB-4854 (W2) lifts the dispatch onto each connector:

* Every connector with a ``receipt_parser.py`` ships a sibling
  ``receipt_parser_provider.py`` that defines a
  ``<Protocol>ReceiptParserConnector`` class implementing
  ``ReceiptParserCapability`` (see
  ``almanak/connectors/_strategy_base/receipt_parser_registry.py``).
* ``almanak/connectors/_strategy_receipt_registry.py`` registers every
  provider into ``STRATEGY_RECEIPT_PARSER_REGISTRY`` at import time —
  the single registration site that mirrors
  ``_gateway_registry.py``'s shape.
* This module is now a thin façade: it consumes
  ``STRATEGY_RECEIPT_PARSER_REGISTRY.classes_by_key()`` for the
  protocol → class map, then layers the same caching / alias
  normalisation / custom-registration semantics consumers (notably
  ``ResultEnricher``) have always relied on.

Public API is **byte-equivalent** with the pre-W2 module:

* :class:`ReceiptParser` — structural Protocol for a parser instance.
* :class:`ReceiptParserRegistry` — registry façade with the same
  ``get`` / ``register`` / ``unregister`` / ``list_protocols`` /
  ``is_registered`` / ``clear_cache`` surface.
* :func:`get_parser`, :func:`register_parser`, :func:`list_parsers`,
  :func:`is_parser_available` — module-level convenience functions
  backed by a shared default registry instance.
* :func:`extract_position_id` — high-level NFT / LP-position-id
  extractor that picks the right parser by ``protocol`` + ``chain``.
* :class:`ReceiptParserError`, :class:`ParserNotFoundError` — exception
  types.

The legacy ``_BUILTIN_LOADERS`` class attribute is removed; the
completeness guard at
``tests/unit/core/test_receipt_parser_registry_completeness.py`` now
walks the new strategy-side registry directly.

Aliases (``"morpho"`` → ``MorphoBlueReceiptParser``, ``"agni"`` →
``UniswapV3ReceiptParser``, …) come from two sources:

* Connector-level aliases (``receipt_parser_keys`` may declare
  multiple keys) — e.g. ``morpho_blue`` publishes both
  ``"morpho_blue"`` and ``"morpho"``.
* ``protocol_aliases.normalize_protocol`` — chain-scoped renames
  (e.g. ``("mantle", "uniswap_v3")`` → ``"agni_finance"``) applied
  before lookup.

Both mechanisms predate W2 and are preserved verbatim.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol
from almanak.connectors._strategy_base.receipt_parser_registry import (
    STRATEGY_RECEIPT_PARSER_REGISTRY,
)

# Importing the registration site is what populates
# ``STRATEGY_RECEIPT_PARSER_REGISTRY``. The import is performed for its
# side-effects (``_register_all`` runs at module import); the imported
# symbol is re-exposed for callers that want a direct reference.
from almanak.connectors._strategy_receipt_registry import (  # noqa: F401
    STRATEGY_RECEIPT_PARSER_REGISTRY as _BOOTED_REGISTRY,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Public Protocol for receipt parsers
# =============================================================================


class ReceiptParser(Protocol):
    """Structural Protocol every receipt parser implements.

    Preserved from the pre-W2 module so consumer type hints
    (``ResultEnricher``, ``copy_signal_engine``, …) stay unchanged.
    """

    def parse_receipt(self, receipt: dict[str, Any]) -> Any:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict containing ``logs``,
                ``transactionHash``, ``blockNumber``, etc.

        Returns:
            ParseResult with extracted events and data (protocol-specific
            type).
        """
        ...


# =============================================================================
# Receipt Parser Registry façade
# =============================================================================


class ReceiptParserRegistry:
    """Façade over ``STRATEGY_RECEIPT_PARSER_REGISTRY``.

    Adds the caching + custom-registration semantics consumers rely on
    on top of the strategy-side connector registry's
    ``classes_by_key()`` resolution:

    * **Per-protocol cache** — ``get(protocol)`` (no kwargs) caches the
      instantiated parser. ``register(custom)`` evicts that entry.
    * **Custom registration** — ``register(protocol, cls)`` injects a
      user-supplied parser class that takes precedence over the
      connector-provided one. Used by tests and by strategy-side
      consumers that need to substitute a parser at runtime.
    * **Alias normalisation** — ``protocol`` is run through
      :func:`almanak.connectors._strategy_base.protocol_aliases.normalize_protocol`
      before lookup, so chain-scoped renames
      (e.g. ``("mantle", "uniswap_v3")`` → ``"agni_finance"``) resolve
      identically to the pre-W2 behaviour.

    The class is intentionally instantiable so each consumer can hold
    its own cache scope (``ResultEnricher`` historically constructs one
    per execution context). The module-level convenience functions
    (``get_parser`` / ``list_parsers`` / …) share a single default
    instance.
    """

    def __init__(self) -> None:
        self._parsers: dict[str, ReceiptParser] = {}
        self._custom_classes: dict[str, type[ReceiptParser]] = {}

    # ---- lookup -----------------------------------------------------------

    def get(self, protocol: str, **kwargs: Any) -> ReceiptParser:
        """Resolve ``protocol`` to a parser instance.

        See class docstring for cache / alias semantics. Raises
        :class:`ValueError` if the protocol isn't known.
        """
        protocol_lower = normalize_protocol(kwargs.get("chain", ""), protocol)

        # Cache hit: only valid when caller passed no constructor kwargs.
        if not kwargs and protocol_lower in self._parsers:
            return self._parsers[protocol_lower]

        # Custom registration takes precedence over connector-provided
        # classes (matches the pre-W2 ordering).
        if protocol_lower in self._custom_classes:
            parser = self._custom_classes[protocol_lower](**kwargs)
            if not kwargs:
                self._parsers[protocol_lower] = parser
            return parser

        connector_classes = STRATEGY_RECEIPT_PARSER_REGISTRY.classes_by_key()
        if protocol_lower in connector_classes:
            try:
                parser_class = connector_classes[protocol_lower]
                parser = parser_class(**kwargs)
            except ImportError as exc:
                raise ValueError(f"Failed to import parser for protocol {protocol}: {exc}") from exc
            if not kwargs:
                self._parsers[protocol_lower] = parser
            return parser

        raise ValueError(f"Unknown protocol: {protocol}. Available protocols: {', '.join(self.list_protocols())}")

    # ---- mutation ---------------------------------------------------------

    def register(
        self,
        protocol: str,
        parser_class: type[ReceiptParser],
    ) -> None:
        """Register a custom parser class. Evicts the cached instance."""
        if not isinstance(parser_class, type):
            raise TypeError(
                f"Expected a class, got {type(parser_class).__name__}. "
                "Use parser_class=MyParser, not parser_class=MyParser()"
            )

        protocol_lower = protocol.lower()
        self._custom_classes[protocol_lower] = parser_class

        # Drop the cached instance so the next ``get`` reflects the new class.
        if protocol_lower in self._parsers:
            del self._parsers[protocol_lower]

        logger.debug(f"Registered custom receipt parser for protocol: {protocol}")

    def unregister(self, protocol: str) -> bool:
        """Remove a custom parser registration. Returns True if removed."""
        protocol_lower = protocol.lower()
        removed = protocol_lower in self._custom_classes

        if protocol_lower in self._custom_classes:
            del self._custom_classes[protocol_lower]
        if protocol_lower in self._parsers:
            del self._parsers[protocol_lower]

        return removed

    def clear_cache(self) -> None:
        """Drop every cached parser instance (custom classes survive)."""
        self._parsers.clear()

    # ---- introspection ----------------------------------------------------

    def list_protocols(self) -> list[str]:
        """Return every protocol key resolvable by this registry."""
        protocols: set[str] = set(STRATEGY_RECEIPT_PARSER_REGISTRY.classes_by_key().keys())
        protocols.update(self._custom_classes.keys())
        return sorted(protocols)

    def is_registered(self, protocol: str) -> bool:
        """Whether ``protocol`` resolves to a parser (built-in or custom)."""
        protocol_lower = protocol.lower()
        if protocol_lower in self._custom_classes:
            return True
        return protocol_lower in STRATEGY_RECEIPT_PARSER_REGISTRY.classes_by_key()


# =============================================================================
# Module-Level Convenience Functions
# =============================================================================

# Shared default instance — the module-level convenience functions
# delegate here so callers that don't need their own cache scope
# (``get_parser("spark")``) interact with one consistent cache.
_default_registry = ReceiptParserRegistry()


def get_parser(protocol: str, **kwargs: Any) -> ReceiptParser:
    """Convenience wrapper around the default registry's ``get``."""
    return _default_registry.get(protocol, **kwargs)


def register_parser(protocol: str, parser_class: type[ReceiptParser]) -> None:
    """Register a custom parser in the default registry."""
    _default_registry.register(protocol, parser_class)


def list_parsers() -> list[str]:
    """List every protocol key resolvable by the default registry."""
    return _default_registry.list_protocols()


def is_parser_available(protocol: str) -> bool:
    """Whether ``protocol`` resolves to a parser in the default registry."""
    return _default_registry.is_registered(protocol)


# =============================================================================
# High-level helpers
# =============================================================================


def _normalise_tx_receipt(receipt: Any) -> dict[str, Any]:
    """Coerce a tx-receipt-shaped object into the dict shape parsers expect.

    ``ExecutionResult.transaction_results[i].receipt`` can be one of three
    shapes depending on how the orchestrator constructed the result:

    * A dict already in parser shape (returned as-is).
    * An object with ``.to_dict()`` (Pydantic / dataclass receipt).
    * An object with a ``.logs`` attribute (bare receipt-like).

    Falling back to ``{"logs": receipt.logs}`` for the third shape mirrors
    the long-standing pre-W2 behaviour the parsers rely on.
    """
    if hasattr(receipt, "to_dict"):
        return receipt.to_dict()
    if hasattr(receipt, "logs"):
        return {"logs": receipt.logs}
    return receipt


def _collect_receipts_from_execution_result(result: Any) -> list[dict[str, Any]]:
    """Pull tx-receipt dicts out of an ``ExecutionResult`` (or shaped dict).

    Both the object-style ``ExecutionResult`` (has
    ``transaction_results`` attribute) and the dict-style
    ``{"transaction_results": [...]}`` shape are accepted. Only
    ``success`` results with a non-empty ``receipt`` survive — parsers
    fail noisily on missing logs, so filtering at this layer avoids
    burying the real signal.
    """
    receipts: list[dict[str, Any]] = []
    if hasattr(result, "transaction_results"):
        for tx_result in result.transaction_results:
            if tx_result.success and tx_result.receipt:
                receipts.append(_normalise_tx_receipt(tx_result.receipt))
    elif isinstance(result, dict) and "transaction_results" in result:
        for tx_result in result["transaction_results"]:
            if tx_result.get("success") and tx_result.get("receipt"):
                receipts.append(tx_result["receipt"])
    return receipts


def _coerce_to_receipts(result: Any) -> list[dict[str, Any]]:
    """Map ``result`` to a list of receipt-shaped dicts.

    Four input shapes are accepted (matching pre-W2 behaviour):

    1. ``ExecutionResult`` / dict with ``transaction_results``.
    2. Raw receipt dict carrying ``logs``.
    3. Bare list of log dicts.
    4. Anything else → empty list (the caller's loop just no-ops).
    """
    if hasattr(result, "transaction_results") or (isinstance(result, dict) and "transaction_results" in result):
        return _collect_receipts_from_execution_result(result)
    if isinstance(result, dict) and "logs" in result:
        return [result]
    if isinstance(result, list):
        return [{"logs": result}]
    return []


def extract_position_id(
    result: Any,
    protocol: str,
    chain: str | None = None,
) -> int | str | None:
    """Extract an LP-position id from an execution result.

    Picks the right parser via :func:`get_parser` and calls its
    ``extract_position_id`` method on each receipt embedded in
    ``result`` until one returns a non-``None`` id.

    Supported result shapes are documented on :func:`_coerce_to_receipts`.

    ``chain`` is required for correctness — the parser's per-chain
    address tables key on it. Omitting it defaults to ``"arbitrum"``
    with a warning, matching the pre-W2 behaviour.
    """
    if chain is None:
        logger.warning(
            "extract_position_id() called without explicit chain parameter, "
            "defaulting to 'arbitrum'. Pass chain= explicitly to silence this warning."
        )
        chain = "arbitrum"

    try:
        parser = get_parser(protocol, chain=chain)
    except ValueError as exc:
        logger.warning(f"Cannot extract position ID: {exc}")
        return None

    if not hasattr(parser, "extract_position_id"):
        logger.warning(f"Parser for {protocol} does not support position ID extraction")
        return None

    try:
        for receipt in _coerce_to_receipts(result):
            position_id = parser.extract_position_id(receipt)
            if position_id is not None:
                return position_id
    except Exception as exc:
        logger.warning(f"Failed to extract position ID: {exc}")
        return None

    return None


# =============================================================================
# Error Classes
# =============================================================================


class ReceiptParserError(Exception):
    """Base exception for receipt parser errors."""


class ParserNotFoundError(ReceiptParserError):
    """Raised when a parser is not found for a protocol."""

    def __init__(self, protocol: str, available: list[str]) -> None:
        self.protocol = protocol
        self.available = available
        super().__init__(f"No receipt parser found for protocol: {protocol}. Available: {', '.join(available)}")


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "ReceiptParserRegistry",
    "ReceiptParser",
    "get_parser",
    "register_parser",
    "list_parsers",
    "is_parser_available",
    "extract_position_id",
    "ReceiptParserError",
    "ParserNotFoundError",
]
