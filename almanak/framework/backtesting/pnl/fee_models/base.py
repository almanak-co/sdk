"""Base interface for protocol-specific fee models.

This module defines the abstract base class for all fee models used in
PnL backtesting. Fee models calculate protocol-specific transaction fees
based on trade amounts and optional parameters.

Key Components:
    - FeeModel: Abstract base class for all fee models
    - FeeModelRegistry: Registry for fee model discovery and lookup
    - get_fee_model: Convenience function for registry lookup

Example:
    from almanak.framework.backtesting.pnl.fee_models.base import (
        FeeModel,
        get_fee_model,
        register_fee_model,
    )

    # Look up a fee model by protocol
    model = get_fee_model("uniswap_v3")
    fee = model.calculate_fee(Decimal("1000"))

    # Register a custom fee model
    @register_fee_model("custom_protocol")
    class CustomFeeModel(FeeModel):
        def calculate_fee(self, trade_amount: Decimal, **kwargs: Any) -> Decimal:
            return trade_amount * Decimal("0.001")
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Any


class FeeModel(ABC):
    """Abstract base class for protocol-specific fee models.

    Fee models calculate the fees associated with executing trades on
    specific DeFi protocols. Each protocol has unique fee structures:

    - DEXs (Uniswap, PancakeSwap, etc.): Swap fees based on fee tiers
    - Lending (Aave, Compound, Morpho): Origination fees for borrows
    - Perps (GMX, Hyperliquid): Position fees, funding rates

    Subclasses must implement the `calculate_fee` method to return the
    fee amount in USD based on the trade amount and optional parameters.

    Attributes:
        model_name: Unique identifier for this fee model (property)

    Example:
        class MyFeeModel(FeeModel):
            @property
            def model_name(self) -> str:
                return "my_protocol"

            def calculate_fee(
                self,
                trade_amount: Decimal,
                **kwargs: Any,
            ) -> Decimal:
                fee_rate = Decimal("0.003")  # 0.3%
                return trade_amount * fee_rate
    """

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Return the unique name of this fee model.

        This should match the protocol identifier used in the registry.

        Returns:
            Protocol identifier string (e.g., "uniswap_v3", "aave_v3")
        """
        ...

    @abstractmethod
    def calculate_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate the fee for a trade.

        This is the simplified interface used by the registry lookup.
        Protocol-specific implementations may accept additional positional
        arguments (intent_type, market_state, etc.) for backwards compatibility.

        Args:
            trade_amount: The notional trade amount in USD
            **kwargs: Protocol-specific parameters, which may include:
                - intent_type: Type of intent being executed (IntentType)
                - market_state: Current market state (MarketState)
                - protocol: Protocol identifier string
                - fee_tier: Fee tier for DEX protocols
                - asset: Asset symbol for asset-specific fees
                - leverage: Leverage multiplier for perp protocols
                - is_maker: Whether the order is a maker order

        Returns:
            Fee amount in USD as a Decimal

        Note:
            Existing implementations may have signatures like:
            calculate_fee(intent_type, amount_usd, market_state, protocol, **kwargs)

            When using get_fee_model(), call with keyword arguments for
            protocol-specific parameters:
            model.calculate_fee(Decimal("1000"), intent_type=IntentType.SWAP)
        """
        ...

    def to_dict(self) -> dict[str, Any]:
        """Serialize the fee model configuration to a dictionary.

        Subclasses should override this to include their specific
        configuration parameters.

        Returns:
            Dictionary with model configuration
        """
        return {
            "model_name": self.model_name,
        }


@dataclass
class FeeModelMetadata:
    """Metadata for a registered fee model.

    Attributes:
        name: Protocol identifier (e.g., "uniswap_v3")
        model_class: The fee model class
        description: Human-readable description
        protocols: List of protocol variants supported (e.g., ["uniswap_v3", "uniswap_v2"])
    """

    name: str
    model_class: type[FeeModel]
    description: str = ""
    protocols: list[str] | None = None


class FeeModelRegistry:
    """Registry for fee model discovery and lookup.

    The registry maintains a mapping from protocol names to fee model
    classes, allowing dynamic lookup and instantiation of fee models.

    Fee models are registered using the `register` method or the
    `register_fee_model` decorator. They can then be looked up by
    protocol name using `get` or `get_fee_model`.

    Example:
        # Register via method
        FeeModelRegistry.register("my_protocol", MyFeeModel)

        # Look up and instantiate
        model_class = FeeModelRegistry.get("my_protocol")
        model = model_class()

        # Get all registered protocols
        protocols = FeeModelRegistry.list_protocols()
    """

    # Class-level registry storage
    _registry: dict[str, FeeModelMetadata] = {}

    @classmethod
    def register(
        cls,
        name: str,
        model_class: type[FeeModel],
        description: str = "",
        aliases: list[str] | None = None,
    ) -> None:
        """Register a fee model class for a protocol.

        Args:
            name: Primary protocol identifier (e.g., "uniswap_v3")
            model_class: The fee model class to register
            description: Human-readable description of the fee model
            aliases: Additional protocol names that map to this model
        """
        protocols = [name]
        if aliases:
            protocols.extend(aliases)

        metadata = FeeModelMetadata(
            name=name,
            model_class=model_class,
            description=description,
            protocols=protocols,
        )

        # Register under primary name
        cls._registry[name.lower()] = metadata

        # Register aliases
        if aliases:
            for alias in aliases:
                cls._registry[alias.lower()] = metadata

    @classmethod
    def get(cls, protocol: str) -> type[FeeModel] | None:
        """Get the fee model class for a protocol.

        Args:
            protocol: Protocol identifier (case-insensitive)

        Returns:
            Fee model class or None if not found
        """
        metadata = cls._registry.get(protocol.lower())
        if metadata:
            return metadata.model_class
        return None

    @classmethod
    def get_metadata(cls, protocol: str) -> FeeModelMetadata | None:
        """Get metadata for a registered fee model.

        Args:
            protocol: Protocol identifier (case-insensitive)

        Returns:
            FeeModelMetadata or None if not found
        """
        return cls._registry.get(protocol.lower())

    @classmethod
    def list_protocols(cls) -> list[str]:
        """List all registered protocol names.

        Returns:
            List of registered protocol identifiers
        """
        # Return unique primary names (not aliases)
        seen = set()
        protocols = []
        for metadata in cls._registry.values():
            if metadata.name not in seen:
                seen.add(metadata.name)
                protocols.append(metadata.name)
        return sorted(protocols)

    @classmethod
    def list_all(cls) -> dict[str, FeeModelMetadata]:
        """Get all registered fee models with their metadata.

        Returns:
            Dictionary mapping protocol names to metadata
        """
        # Return only primary names
        result = {}
        for metadata in cls._registry.values():
            if metadata.name not in result:
                result[metadata.name] = metadata
        return result

    @classmethod
    def clear(cls) -> None:
        """Clear all registered fee models.

        This is primarily useful for testing.
        """
        cls._registry.clear()


def register_fee_model(
    name: str,
    description: str = "",
    aliases: list[str] | None = None,
) -> Any:
    """Decorator to register a fee model class.

    Args:
        name: Protocol identifier (e.g., "uniswap_v3")
        description: Human-readable description
        aliases: Additional protocol names

    Returns:
        Class decorator

    Example:
        @register_fee_model("my_protocol", description="My custom fee model")
        class MyFeeModel(FeeModel):
            ...
    """

    def decorator(cls: type[FeeModel]) -> type[FeeModel]:
        FeeModelRegistry.register(name, cls, description, aliases)
        return cls

    return decorator


def get_fee_model(protocol: str) -> FeeModel | None:
    """Get an instantiated fee model for a protocol.

    This is a convenience function that looks up the fee model class
    in the registry and instantiates it with default parameters.

    Args:
        protocol: Protocol identifier (case-insensitive)

    Returns:
        Instantiated fee model or None if not found

    Example:
        model = get_fee_model("uniswap_v3")
        if model:
            fee = model.calculate_fee(Decimal("1000"))
    """
    model_class = FeeModelRegistry.get(protocol)
    if model_class:
        return model_class()
    return None


# Type alias for the fee model registry mapping
FeeModelRegistryDict = dict[str, type[FeeModel]]


def get_fee_model_registry() -> FeeModelRegistryDict:
    """Get a dictionary mapping protocol names to fee model classes.

    This provides a simple dict interface for the registry, useful for
    cases where the full registry API is not needed.

    Returns:
        Dictionary mapping protocol names to fee model classes
    """
    return {name: metadata.model_class for name, metadata in FeeModelRegistry.list_all().items()}


__all__ = [
    "FeeModel",
    "FeeModelMetadata",
    "FeeModelRegistry",
    "FeeModelRegistryDict",
    "get_fee_model",
    "get_fee_model_registry",
    "register_fee_model",
]
