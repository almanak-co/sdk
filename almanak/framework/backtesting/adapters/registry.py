"""Strategy type detection and adapter resolution.

This module provides functions to detect strategy types from strategy metadata
and resolve the appropriate backtest adapter for a given strategy.

The detection system examines:
1. Strategy metadata (tags, protocols, intent_types)
2. Strategy class attributes
3. Configuration hints

The adapter resolution follows a priority order:
1. Explicit strategy_type in config
2. Detected from strategy metadata
3. Fallback to generic (None)

Example:
    from almanak.framework.backtesting.adapters.registry import (
        detect_strategy_type,
        get_adapter_for_strategy,
        StrategyTypeHint,
    )

    # Detect from strategy instance
    strategy_type = detect_strategy_type(my_strategy)
    print(f"Detected type: {strategy_type}")

    # Get adapter for strategy
    adapter = get_adapter_for_strategy(my_strategy)
    if adapter:
        fill = adapter.execute_intent(intent, portfolio, market_state)
    else:
        # Use generic backtesting logic
        ...
"""

import functools
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .base import AdapterRegistry, StrategyBacktestAdapter, get_adapter, get_adapter_with_config

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
    from almanak.framework.strategies.intent_strategy import (
        StrategyMetadata,
    )

    # PEP 562 + mypy: PROTOCOL_TO_STRATEGY_TYPE is resolved at runtime through
    # module ``__getattr__`` (lazy — see the note above it), so it is absent
    # from the module namespace at static-analysis time. Type-only declaration;
    # no runtime value is bound (that would shadow ``__getattr__``).
    PROTOCOL_TO_STRATEGY_TYPE: dict[str, str]

logger = logging.getLogger(__name__)


# =============================================================================
# Strategy Type Constants
# =============================================================================

# Canonical strategy types
STRATEGY_TYPE_LP = "lp"
STRATEGY_TYPE_PERP = "perp"
STRATEGY_TYPE_LENDING = "lending"
STRATEGY_TYPE_ARBITRAGE = "arbitrage"
STRATEGY_TYPE_SWAP = "swap"
STRATEGY_TYPE_YIELD = "yield"
STRATEGY_TYPE_MULTI_PROTOCOL = "multi_protocol"

# All known strategy types
KNOWN_STRATEGY_TYPES = frozenset(
    {
        STRATEGY_TYPE_LP,
        STRATEGY_TYPE_PERP,
        STRATEGY_TYPE_LENDING,
        STRATEGY_TYPE_ARBITRAGE,
        STRATEGY_TYPE_SWAP,
        STRATEGY_TYPE_YIELD,
        STRATEGY_TYPE_MULTI_PROTOCOL,
    }
)

# Tags that indicate specific strategy types
TAG_TO_STRATEGY_TYPE: dict[str, str] = {
    # LP-related tags
    "lp": STRATEGY_TYPE_LP,
    "liquidity": STRATEGY_TYPE_LP,
    "liquidity-provider": STRATEGY_TYPE_LP,
    "liquidity_provider": STRATEGY_TYPE_LP,
    "concentrated-liquidity": STRATEGY_TYPE_LP,
    "concentrated_liquidity": STRATEGY_TYPE_LP,
    "pool": STRATEGY_TYPE_LP,
    "amm": STRATEGY_TYPE_LP,
    # Perp-related tags
    "perp": STRATEGY_TYPE_PERP,
    "perpetual": STRATEGY_TYPE_PERP,
    "perpetuals": STRATEGY_TYPE_PERP,
    "futures": STRATEGY_TYPE_PERP,
    "leverage": STRATEGY_TYPE_PERP,
    "margin": STRATEGY_TYPE_PERP,
    # Lending-related tags
    "lending": STRATEGY_TYPE_LENDING,
    "borrowing": STRATEGY_TYPE_LENDING,
    "borrow": STRATEGY_TYPE_LENDING,
    "supply": STRATEGY_TYPE_LENDING,
    "collateral": STRATEGY_TYPE_LENDING,
    "money-market": STRATEGY_TYPE_LENDING,
    "money_market": STRATEGY_TYPE_LENDING,
    # Arbitrage-related tags
    "arbitrage": STRATEGY_TYPE_ARBITRAGE,
    "arb": STRATEGY_TYPE_ARBITRAGE,
    "mev": STRATEGY_TYPE_ARBITRAGE,
    "sandwich": STRATEGY_TYPE_ARBITRAGE,
    "flash-loan": STRATEGY_TYPE_ARBITRAGE,
    "flash_loan": STRATEGY_TYPE_ARBITRAGE,
    # Swap-related tags
    "swap": STRATEGY_TYPE_SWAP,
    "trade": STRATEGY_TYPE_SWAP,
    "trading": STRATEGY_TYPE_SWAP,
    "dca": STRATEGY_TYPE_SWAP,
    "mean-reversion": STRATEGY_TYPE_SWAP,
    "mean_reversion": STRATEGY_TYPE_SWAP,
    "momentum": STRATEGY_TYPE_SWAP,
    # Yield-related tags
    "yield": STRATEGY_TYPE_YIELD,
    "yield-farming": STRATEGY_TYPE_YIELD,
    "yield_farming": STRATEGY_TYPE_YIELD,
    "staking": STRATEGY_TYPE_YIELD,
    "vault": STRATEGY_TYPE_YIELD,
    # Multi-protocol tags
    "multi-protocol": STRATEGY_TYPE_MULTI_PROTOCOL,
    "multi_protocol": STRATEGY_TYPE_MULTI_PROTOCOL,
    "multiprotocol": STRATEGY_TYPE_MULTI_PROTOCOL,
    "cross-protocol": STRATEGY_TYPE_MULTI_PROTOCOL,
    "cross_protocol": STRATEGY_TYPE_MULTI_PROTOCOL,
    "delta-neutral": STRATEGY_TYPE_MULTI_PROTOCOL,
    "delta_neutral": STRATEGY_TYPE_MULTI_PROTOCOL,
}

# Residual / legacy protocol detection keys with genuinely NO connector
# package under ``almanak/connectors/<name>/`` (VIB-4851). Every other
# protocol -> strategy-type fact is owned by the connector manifest
# (``Connector.backtest_strategy_type``) and merged in lazily below. A guard
# test (tests/unit/connectors/test_manifest_metadata_equivalence.py) asserts
# no residual key collides with any discovered connector name/alias, so this
# map shrinks automatically as connectors appear — when one of these venues
# gets a connector, move the entry onto its manifest.
_NON_CONNECTOR_STRATEGY_TYPES: dict[str, str] = {
    # No dydx connector package (app-chain perp venue).
    "dydx": STRATEGY_TYPE_PERP,
    # No perpetual_protocol connector package.
    "perpetual_protocol": STRATEGY_TYPE_PERP,
    # No convex connector package (Curve-boosting yield venue).
    "convex": STRATEGY_TYPE_YIELD,
}


@functools.cache
def _protocol_to_strategy_type() -> dict[str, str]:
    """Cached ``PROTOCOL_TO_STRATEGY_TYPE`` (lazy — see the ``__getattr__`` note).

    Merges the residual non-connector entries with every connector-declared
    ``BacktestStrategyTypeDecl`` (the builder names no connector). Duplicate
    keys across two manifests are rejected at connector discovery; a manifest
    key colliding with a residual entry fails loud here because the residual
    entry must be deleted in the same change that adds the manifest decl.
    """
    # Deferred import: connector discovery must not run at module import time
    # (see the ``__getattr__`` note below).
    from almanak.connectors._connector import CONNECTOR_REGISTRY

    mapping = dict(_NON_CONNECTOR_STRATEGY_TYPES)
    for connector in CONNECTOR_REGISTRY.with_backtest_strategy_type():
        decl = connector.backtest_strategy_type
        assert decl is not None  # narrowed by with_backtest_strategy_type()
        for key in (decl.name or connector.name, *decl.aliases):
            if key in mapping:
                raise ValueError(
                    f"Backtest strategy-type key {key!r} declared by connector {connector.name!r} "
                    "collides with a _NON_CONNECTOR_STRATEGY_TYPES residual entry; "
                    "remove the residual entry."
                )
            mapping[key] = decl.strategy_type
    return mapping


# Intent types that indicate specific strategy types
INTENT_TO_STRATEGY_TYPE: dict[str, str] = {
    # LP intents
    "LP_OPEN": STRATEGY_TYPE_LP,
    "LP_CLOSE": STRATEGY_TYPE_LP,
    "LP_REBALANCE": STRATEGY_TYPE_LP,
    "LP_COLLECT_FEES": STRATEGY_TYPE_LP,
    "ADD_LIQUIDITY": STRATEGY_TYPE_LP,
    "REMOVE_LIQUIDITY": STRATEGY_TYPE_LP,
    # Perp intents
    "PERP_OPEN": STRATEGY_TYPE_PERP,
    "PERP_CLOSE": STRATEGY_TYPE_PERP,
    "PERP_INCREASE": STRATEGY_TYPE_PERP,
    "PERP_DECREASE": STRATEGY_TYPE_PERP,
    "MARGIN_DEPOSIT": STRATEGY_TYPE_PERP,
    "MARGIN_WITHDRAW": STRATEGY_TYPE_PERP,
    # Lending intents
    "BORROW": STRATEGY_TYPE_LENDING,
    "REPAY": STRATEGY_TYPE_LENDING,
    "SUPPLY": STRATEGY_TYPE_LENDING,
    "WITHDRAW": STRATEGY_TYPE_LENDING,
    "COLLATERAL_DEPOSIT": STRATEGY_TYPE_LENDING,
    "COLLATERAL_WITHDRAW": STRATEGY_TYPE_LENDING,
    # Swap intents (lower priority)
    "SWAP": STRATEGY_TYPE_SWAP,
}


@dataclass
class StrategyTypeHint:
    """Result of strategy type detection.

    Attributes:
        strategy_type: The detected strategy type (e.g., "lp", "perp")
        confidence: Confidence level (high, medium, low)
        source: What triggered the detection (tag, protocol, intent, explicit)
        details: Additional detection details
    """

    strategy_type: str | None
    confidence: str = "low"
    source: str = "none"
    details: str = ""


def _get_strategy_metadata(strategy: Any) -> "StrategyMetadata | None":
    """Extract metadata from a strategy object.

    Args:
        strategy: Strategy instance or class

    Returns:
        StrategyMetadata if available, None otherwise
    """
    # Try instance method first
    if hasattr(strategy, "get_metadata"):
        metadata = strategy.get_metadata()
        if metadata:
            return metadata

    # Try class attribute
    if hasattr(strategy, "STRATEGY_METADATA"):
        return getattr(strategy, "STRATEGY_METADATA", None)

    # Try for class itself
    if isinstance(strategy, type):
        return getattr(strategy, "STRATEGY_METADATA", None)

    return None


def _detect_from_tags(tags: list[str]) -> StrategyTypeHint:
    """Detect strategy type from metadata tags.

    Args:
        tags: List of strategy tags

    Returns:
        StrategyTypeHint with detection result
    """
    if not tags:
        return StrategyTypeHint(strategy_type=None)

    # Normalize tags to lowercase
    normalized_tags = [tag.lower() for tag in tags]

    # Count matches for each strategy type
    type_counts: dict[str, int] = {}
    for tag in normalized_tags:
        if tag in TAG_TO_STRATEGY_TYPE:
            strategy_type = TAG_TO_STRATEGY_TYPE[tag]
            type_counts[strategy_type] = type_counts.get(strategy_type, 0) + 1

    if not type_counts:
        return StrategyTypeHint(strategy_type=None, source="tags")

    # Return the most common type
    best_type = max(type_counts, key=lambda t: type_counts[t])
    count = type_counts[best_type]

    # Determine confidence based on match count
    confidence = "high" if count >= 2 else "medium"

    return StrategyTypeHint(
        strategy_type=best_type,
        confidence=confidence,
        source="tags",
        details=f"Matched tags: {', '.join(t for t in normalized_tags if TAG_TO_STRATEGY_TYPE.get(t) == best_type)}",
    )


def _detect_from_protocols(protocols: list[str]) -> StrategyTypeHint:
    """Detect strategy type from supported protocols.

    Args:
        protocols: List of supported protocol names

    Returns:
        StrategyTypeHint with detection result
    """
    if not protocols:
        return StrategyTypeHint(strategy_type=None)

    protocol_to_strategy_type = _protocol_to_strategy_type()

    # Normalize protocol names to lowercase
    normalized_protocols = [p.lower() for p in protocols]

    # Count matches for each strategy type
    type_counts: dict[str, int] = {}
    for protocol in normalized_protocols:
        if protocol in protocol_to_strategy_type:
            strategy_type = protocol_to_strategy_type[protocol]
            type_counts[strategy_type] = type_counts.get(strategy_type, 0) + 1

    if not type_counts:
        return StrategyTypeHint(strategy_type=None, source="protocols")

    # Return the most common type
    best_type = max(type_counts, key=lambda t: type_counts[t])
    count = type_counts[best_type]

    # Determine confidence
    confidence = "high" if count >= 2 else "medium"

    return StrategyTypeHint(
        strategy_type=best_type,
        confidence=confidence,
        source="protocols",
        details=f"Matched protocols: {', '.join(p for p in normalized_protocols if protocol_to_strategy_type.get(p) == best_type)}",
    )


def _detect_from_intents(intent_types: list[str]) -> StrategyTypeHint:
    """Detect strategy type from intent types.

    Args:
        intent_types: List of intent type names

    Returns:
        StrategyTypeHint with detection result
    """
    if not intent_types:
        return StrategyTypeHint(strategy_type=None)

    # Normalize intent types to uppercase
    normalized_intents = [i.upper() for i in intent_types]

    # Priority order: LP > Perp > Lending > Arbitrage > Swap
    # This handles cases where a strategy uses both SWAP and LP_OPEN
    priority_order = [
        STRATEGY_TYPE_LP,
        STRATEGY_TYPE_PERP,
        STRATEGY_TYPE_LENDING,
        STRATEGY_TYPE_ARBITRAGE,
        STRATEGY_TYPE_SWAP,
    ]

    for strategy_type in priority_order:
        matching_intents = [i for i in normalized_intents if INTENT_TO_STRATEGY_TYPE.get(i) == strategy_type]
        if matching_intents:
            return StrategyTypeHint(
                strategy_type=strategy_type,
                confidence="high" if len(matching_intents) >= 2 else "medium",
                source="intents",
                details=f"Matched intents: {', '.join(matching_intents)}",
            )

    return StrategyTypeHint(strategy_type=None, source="intents")


def detect_strategy_type(
    strategy: Any,
    config: dict[str, Any] | None = None,
) -> StrategyTypeHint:
    """Detect the strategy type from a strategy object.

    This function examines strategy metadata (tags, protocols, intent_types)
    to determine the most appropriate adapter type for backtesting.

    Detection priority:
    1. Explicit strategy_type in config
    2. Strategy metadata tags
    3. Supported protocols
    4. Intent types used
    5. Fallback to None (generic backtesting)

    Args:
        strategy: Strategy instance, class, or dict with metadata
        config: Optional config dict that may contain explicit strategy_type

    Returns:
        StrategyTypeHint with detected type and confidence

    Example:
        hint = detect_strategy_type(my_strategy)
        if hint.strategy_type:
            print(f"Detected {hint.strategy_type} ({hint.confidence})")
            print(f"Source: {hint.source} - {hint.details}")
    """
    # 1. Check for explicit strategy_type in config
    if config and "strategy_type" in config:
        explicit_type = str(config["strategy_type"]).lower()
        if explicit_type in KNOWN_STRATEGY_TYPES:
            return StrategyTypeHint(
                strategy_type=explicit_type,
                confidence="high",
                source="explicit",
                details="Explicitly set in config",
            )
        logger.warning(f"Unknown explicit strategy_type: {explicit_type}")

    # 2. Get strategy metadata
    metadata = None
    if isinstance(strategy, dict):
        # Handle dict-based strategy spec
        metadata_dict = strategy.get("metadata", {})
        tags = metadata_dict.get("tags", [])
        protocols = metadata_dict.get("supported_protocols", [])
        intent_types = metadata_dict.get("intent_types", [])
    else:
        metadata = _get_strategy_metadata(strategy)
        if metadata:
            tags = metadata.tags
            protocols = metadata.supported_protocols
            intent_types = metadata.intent_types
        else:
            tags = []
            protocols = []
            intent_types = []

    # 3. Try detection in priority order
    # Tags have highest priority (most explicit)
    tag_hint = _detect_from_tags(tags)
    if tag_hint.strategy_type:
        logger.debug(f"Detected strategy type from tags: {tag_hint}")
        return tag_hint

    # Protocols are next
    protocol_hint = _detect_from_protocols(protocols)
    if protocol_hint.strategy_type:
        logger.debug(f"Detected strategy type from protocols: {protocol_hint}")
        return protocol_hint

    # Intent types are last (can be misleading if strategy uses multiple)
    intent_hint = _detect_from_intents(intent_types)
    if intent_hint.strategy_type:
        logger.debug(f"Detected strategy type from intents: {intent_hint}")
        return intent_hint

    # No match found
    logger.debug("Could not detect strategy type, will use generic backtesting")
    return StrategyTypeHint(
        strategy_type=None,
        confidence="none",
        source="none",
        details="No matching tags, protocols, or intents found",
    )


def get_adapter_for_strategy(
    strategy: Any,
    config: dict[str, Any] | None = None,
) -> StrategyBacktestAdapter | None:
    """Get the appropriate backtest adapter for a strategy.

    This function detects the strategy type and returns an instantiated
    adapter if one is registered for that type. If no adapter matches,
    returns None (fallback to generic backtesting).

    Args:
        strategy: Strategy instance, class, or dict with metadata
        config: Optional config dict that may contain explicit strategy_type

    Returns:
        Instantiated adapter or None if no adapter matches

    Example:
        adapter = get_adapter_for_strategy(my_lp_strategy)
        if adapter:
            # Use adapter-specific backtesting
            fill = adapter.execute_intent(intent, portfolio, market)
        else:
            # Use generic backtesting
            ...
    """
    hint = detect_strategy_type(strategy, config)

    if hint.strategy_type is None:
        logger.debug("No strategy type detected, using generic backtesting")
        return None

    adapter = get_adapter(hint.strategy_type)
    if adapter:
        logger.debug(f"Found adapter for strategy type '{hint.strategy_type}': {adapter.__class__.__name__}")
    else:
        logger.debug(f"No adapter registered for strategy type '{hint.strategy_type}', using generic backtesting")

    return adapter


def get_adapter_for_strategy_with_config(
    strategy: Any,
    data_config: "BacktestDataConfig | None" = None,
    config: dict[str, Any] | None = None,
) -> StrategyBacktestAdapter | None:
    """Get the appropriate backtest adapter for a strategy with data config.

    This function detects the strategy type and returns an instantiated
    adapter configured with the provided BacktestDataConfig. If no adapter
    matches, returns None (fallback to generic backtesting).

    Args:
        strategy: Strategy instance, class, or dict with metadata
        data_config: BacktestDataConfig for historical data provider settings.
            If provided, will be passed to the adapter constructor.
        config: Optional config dict that may contain explicit strategy_type

    Returns:
        Instantiated adapter or None if no adapter matches

    Example:
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(
            use_historical_volume=True,
            use_historical_funding=True,
        )
        adapter = get_adapter_for_strategy_with_config(
            my_lp_strategy, data_config=data_config
        )
        if adapter:
            # Use adapter-specific backtesting with historical data
            fill = adapter.execute_intent(intent, portfolio, market)
    """
    hint = detect_strategy_type(strategy, config)

    if hint.strategy_type is None:
        logger.debug("No strategy type detected, using generic backtesting")
        return None

    adapter = get_adapter_with_config(hint.strategy_type, data_config=data_config)
    if adapter:
        logger.debug(
            f"Found adapter for strategy type '{hint.strategy_type}' with data_config: {adapter.__class__.__name__}"
        )
    else:
        logger.debug(f"No adapter registered for strategy type '{hint.strategy_type}', using generic backtesting")

    return adapter


def list_available_adapters() -> list[str]:
    """List all registered adapter strategy types.

    Returns:
        List of strategy type identifiers
    """
    return AdapterRegistry.list_strategy_types()


def get_adapter_info(strategy_type: str) -> dict[str, Any] | None:
    """Get information about a registered adapter.

    Args:
        strategy_type: Strategy type identifier

    Returns:
        Dictionary with adapter info or None if not found
    """
    metadata = AdapterRegistry.get_metadata(strategy_type)
    if metadata:
        return {
            "name": metadata.name,
            "description": metadata.description,
            "aliases": metadata.aliases,
            "adapter_class": metadata.adapter_class.__name__,
        }
    return None


# ``PROTOCOL_TO_STRATEGY_TYPE`` is manifest-derived (connector
# ``BacktestStrategyTypeDecl`` rows + ``_NON_CONNECTOR_STRATEGY_TYPES``).
# Resolving it through PEP 562 ``__getattr__`` defers connector discovery to
# first attribute access, so importing this module (and the
# ``backtesting.adapters`` package that re-exports from it) never hydrates the
# connector registry at import time — the same pytest-xdist
# import-interleave hazard ``compiler_constants.py`` documents for its lazy
# address tables. ``from X import PROTOCOL_TO_STRATEGY_TYPE`` still resolves:
# the import statement falls back to ``X.__getattr__(...)`` when the name is
# missing from the module dict.
def __getattr__(name: str) -> dict[str, str]:
    if name == "PROTOCOL_TO_STRATEGY_TYPE":
        return _protocol_to_strategy_type()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Type constants
    "KNOWN_STRATEGY_TYPES",
    "STRATEGY_TYPE_ARBITRAGE",
    "STRATEGY_TYPE_LENDING",
    "STRATEGY_TYPE_LP",
    "STRATEGY_TYPE_MULTI_PROTOCOL",
    "STRATEGY_TYPE_PERP",
    "STRATEGY_TYPE_SWAP",
    "STRATEGY_TYPE_YIELD",
    # Detection result
    "StrategyTypeHint",
    # Main functions
    "detect_strategy_type",
    "get_adapter_for_strategy",
    "get_adapter_for_strategy_with_config",
    "get_adapter_info",
    "list_available_adapters",
]
