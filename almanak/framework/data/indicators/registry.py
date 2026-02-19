"""Indicator Registry for Discovery and Factory Patterns.

This module provides a registry for technical indicators, enabling:
- Dynamic indicator discovery
- Factory-style instantiation
- Runtime indicator lookup by name

Example:
    from almanak.framework.data.indicators.registry import IndicatorRegistry
    from almanak.framework.data.indicators.rsi import RSICalculator

    # Register indicators
    IndicatorRegistry.register("rsi", RSICalculator)

    # Discover available indicators
    print(IndicatorRegistry.list_all())  # ['rsi']

    # Get indicator class
    RSIClass = IndicatorRegistry.get("rsi")
    calculator = RSIClass(ohlcv_provider=provider)
"""

import logging
from typing import Any

from .base import BaseIndicator

logger = logging.getLogger(__name__)


class IndicatorRegistry:
    """Registry for technical indicator discovery and instantiation.

    This class provides a centralized registry for indicator classes,
    enabling dynamic lookup and factory-style creation.

    The registry uses class methods so it can be used without instantiation,
    acting as a singleton-like pattern for global indicator registration.

    Example:
        # Register an indicator
        IndicatorRegistry.register("rsi", RSICalculator)
        IndicatorRegistry.register("bollinger", BollingerBandsCalculator)

        # List all registered indicators
        indicators = IndicatorRegistry.list_all()
        # ['rsi', 'bollinger']

        # Get an indicator class by name
        RSIClass = IndicatorRegistry.get("rsi")
        if RSIClass:
            calculator = RSIClass(ohlcv_provider=provider)

        # Check if an indicator is registered
        if IndicatorRegistry.has("macd"):
            MACDClass = IndicatorRegistry.get("macd")
    """

    _indicators: dict[str, type] = {}
    _metadata: dict[str, dict[str, Any]] = {}

    @classmethod
    def register(
        cls,
        name: str,
        indicator_class: type,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register an indicator class.

        Args:
            name: Unique name for the indicator (case-insensitive)
            indicator_class: The indicator class to register
            metadata: Optional metadata about the indicator (description, version, etc.)

        Raises:
            ValueError: If name is already registered

        Example:
            IndicatorRegistry.register("rsi", RSICalculator, metadata={
                "description": "Relative Strength Index",
                "version": "1.0.0",
                "category": "momentum",
            })
        """
        name_lower = name.lower()

        if name_lower in cls._indicators:
            logger.warning(
                "Overwriting existing indicator registration: %s",
                name_lower,
            )

        cls._indicators[name_lower] = indicator_class
        cls._metadata[name_lower] = metadata or {}

        logger.info(
            "Registered indicator: %s -> %s",
            name_lower,
            indicator_class.__name__,
        )

    @classmethod
    def get(cls, name: str) -> type | None:
        """Get an indicator class by name.

        Args:
            name: Indicator name (case-insensitive)

        Returns:
            The indicator class, or None if not found

        Example:
            RSIClass = IndicatorRegistry.get("rsi")
            if RSIClass:
                calculator = RSIClass(ohlcv_provider=provider)
        """
        return cls._indicators.get(name.lower())

    @classmethod
    def has(cls, name: str) -> bool:
        """Check if an indicator is registered.

        Args:
            name: Indicator name (case-insensitive)

        Returns:
            True if registered, False otherwise
        """
        return name.lower() in cls._indicators

    @classmethod
    def list_all(cls) -> list[str]:
        """List all registered indicator names.

        Returns:
            Sorted list of registered indicator names

        Example:
            indicators = IndicatorRegistry.list_all()
            # ['atr', 'bollinger', 'macd', 'rsi', 'stochastic']
        """
        return sorted(cls._indicators.keys())

    @classmethod
    def get_metadata(cls, name: str) -> dict[str, Any]:
        """Get metadata for an indicator.

        Args:
            name: Indicator name (case-insensitive)

        Returns:
            Metadata dictionary, or empty dict if not found
        """
        return cls._metadata.get(name.lower(), {})

    @classmethod
    def unregister(cls, name: str) -> bool:
        """Unregister an indicator.

        Args:
            name: Indicator name (case-insensitive)

        Returns:
            True if unregistered, False if not found
        """
        name_lower = name.lower()
        if name_lower in cls._indicators:
            del cls._indicators[name_lower]
            cls._metadata.pop(name_lower, None)
            logger.info("Unregistered indicator: %s", name_lower)
            return True
        return False

    @classmethod
    def clear(cls) -> None:
        """Clear all registered indicators.

        Primarily used for testing.
        """
        cls._indicators.clear()
        cls._metadata.clear()
        logger.info("Cleared all indicator registrations")

    @classmethod
    def create(
        cls,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> BaseIndicator | None:
        """Factory method to create an indicator instance.

        Args:
            name: Indicator name (case-insensitive)
            *args: Positional arguments for indicator constructor
            **kwargs: Keyword arguments for indicator constructor

        Returns:
            Indicator instance, or None if not found

        Example:
            calculator = IndicatorRegistry.create("rsi", ohlcv_provider=provider)
            if calculator:
                rsi = await calculator.calculate("WETH", timeframe="1h", period=14)
        """
        indicator_class = cls.get(name)
        if indicator_class is None:
            logger.warning("Indicator not found: %s", name)
            return None

        return indicator_class(*args, **kwargs)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "IndicatorRegistry",
]
