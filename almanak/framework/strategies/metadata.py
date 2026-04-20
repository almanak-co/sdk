"""Strategy metadata and decorator for strategy registration.

This module contains the StrategyMetadata dataclass and the @almanak_strategy
decorator used to annotate and register strategy classes.

These were extracted from intent_strategy.py for maintainability. All symbols
remain importable from almanak.framework.strategies.intent_strategy.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

logger = logging.getLogger(__name__)


@dataclass
class StrategyMetadata:
    """Metadata for a strategy.

    Attributes:
        name: Strategy name (e.g., "simple_dca")
        description: Human-readable description
        version: Strategy version (e.g., "1.0.0")
        author: Author name or organization
        tags: List of tags for categorization
        supported_chains: List of supported chains
        supported_protocols: List of supported protocols
        intent_types: List of intent types this strategy may use
        default_chain: Default chain for single-chain execution (falls back to supported_chains[0])
    """

    name: str
    description: str = ""
    version: str = "1.0.0"
    author: str = ""
    tags: list[str] = field(default_factory=list)
    supported_chains: list[str] = field(default_factory=list)
    supported_protocols: list[str] = field(default_factory=list)
    intent_types: list[str] = field(default_factory=list)
    default_chain: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "author": self.author,
            "tags": self.tags,
            "supported_chains": self.supported_chains,
            "supported_protocols": self.supported_protocols,
            "intent_types": self.intent_types,
            "default_chain": self.default_chain,
        }


# Type variable for strategy class
StrategyClassT = TypeVar("StrategyClassT", bound=type)


def almanak_strategy(
    name: str,
    description: str = "",
    version: str = "1.0.0",
    author: str = "",
    tags: list[str] | None = None,
    supported_chains: list[str] | None = None,
    supported_protocols: list[str] | None = None,
    intent_types: list[str] | None = None,
    default_chain: str = "",
) -> Callable[[StrategyClassT], StrategyClassT]:
    """Decorator to add metadata to an IntentStrategy class.

    This decorator attaches metadata to a strategy class, making it
    discoverable and self-documenting. It also registers the strategy
    in STRATEGY_REGISTRY for lookup by name.

    Args:
        name: Strategy name
        description: Human-readable description
        version: Strategy version
        author: Author name
        tags: Categorization tags
        supported_chains: List of supported chains
        supported_protocols: List of supported protocols
        intent_types: List of intent types used
        default_chain: Default chain for single-chain execution (falls back to supported_chains[0])

    Returns:
        Decorated class with STRATEGY_METADATA attribute

    Example:
        @almanak_strategy(
            name="mean_reversion_simple",
            description="RSI-based mean reversion strategy",
            version="1.0.0",
            author="Almanak",
            tags=["trading", "rsi", "mean-reversion"],
            supported_chains=["arbitrum", "ethereum"],
            intent_types=["SWAP"],
            default_chain="arbitrum",
        )
        class MeanReversionStrategy(IntentStrategy):
            pass
    """
    # Import here to avoid circular import
    from . import STRATEGY_REGISTRY

    def decorator(cls: StrategyClassT) -> StrategyClassT:
        resolved_supported_chains = supported_chains or []
        resolved_default_chain = default_chain or (resolved_supported_chains[0] if resolved_supported_chains else "")
        if default_chain and resolved_supported_chains and default_chain not in resolved_supported_chains:
            raise ValueError(
                f"default_chain '{default_chain}' must be one of supported_chains: {resolved_supported_chains}"
            )

        # Auto-expand teardown complement intent types.
        # One-way: only open->close, matching the permission generator's
        # _TEARDOWN_COMPLEMENTS. A strategy declaring only close types
        # should not auto-gain open type permissions.
        expanded_intent_types = list(intent_types) if intent_types else []
        if expanded_intent_types:
            _COMPLEMENT_PAIRS = {
                "SUPPLY": "WITHDRAW",
                "BORROW": "REPAY",
                "LP_OPEN": "LP_CLOSE",
                "VAULT_DEPOSIT": "VAULT_REDEEM",
                "PERP_OPEN": "PERP_CLOSE",
            }
            declared = set(expanded_intent_types)
            missing = sorted(
                {
                    complement
                    for it in expanded_intent_types
                    if (complement := _COMPLEMENT_PAIRS.get(it)) and complement not in declared
                }
            )
            if missing:
                expanded_intent_types.extend(missing)
                logger.debug(
                    "Strategy '%s': auto-expanded intent_types with teardown complements %s",
                    name,
                    missing,
                )

        metadata = StrategyMetadata(
            name=name,
            description=description,
            version=version,
            author=author,
            tags=tags or [],
            supported_chains=resolved_supported_chains,
            supported_protocols=supported_protocols or [],
            intent_types=expanded_intent_types,
            default_chain=resolved_default_chain,
        )

        # Attach metadata to class
        cls.STRATEGY_METADATA = metadata  # type: ignore[attr-defined]
        cls.STRATEGY_NAME = name  # type: ignore[attr-defined]

        # NOTE: Do NOT set cls.SUPPORTED_CHAINS here. The CLI's is_multi_chain_strategy()
        # in run.py checks SUPPORTED_CHAINS as a runtime multi-chain signal (MultiChainOrchestrator).
        # Decorator's supported_chains is portability metadata, not a runtime signal.
        # The instance methods is_multi_chain() and get_supported_chains() fall back to
        # STRATEGY_METADATA.supported_chains when SUPPORTED_CHAINS is not manually set.

        # Register in the global registry
        if name not in STRATEGY_REGISTRY:
            STRATEGY_REGISTRY[name] = cls
            logger.info(f"Registered strategy: {name} v{version}")
        else:
            logger.debug(f"Strategy {name} already registered, skipping")

        return cls

    return decorator


__all__ = [
    "StrategyMetadata",
    "StrategyClassT",
    "almanak_strategy",
]
