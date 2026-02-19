"""Hot-reloadable configuration schema for strategy parameters.

This module defines the configuration schema for parameters that can be changed
at runtime without restarting the strategy. It distinguishes between hot-reloadable
parameters (trading/risk limits) and cold parameters (identity, connections).

Usage:
    config = HotReloadableConfig(
        max_slippage=Decimal("0.005"),
        trade_size_usd=Decimal("1000"),
        rebalance_threshold=Decimal("0.05"),
        min_health_factor=Decimal("1.5"),
        max_leverage=Decimal("3"),
        daily_loss_limit_usd=Decimal("500"),
    )

    # Update at runtime
    result = config.update(max_slippage=Decimal("0.01"))
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

# =============================================================================
# FIELDS THAT CANNOT BE HOT-RELOADED
# =============================================================================
# The following fields are considered "cold" and require a strategy restart:
#
# - strategy_id: Immutable identity of the strategy
# - chain: Network the strategy operates on (e.g., "arbitrum", "ethereum")
# - pool_address: The LP pool or protocol contract address
# - wallet_address: The wallet executing transactions
# - rpc_endpoints: RPC provider URLs
# - private_key_reference: Reference to signing key (never stored directly)
# - protocol: The protocol being used (e.g., "uniswap_v3", "aave_v3")
# - connector_config: Protocol connector configuration
#
# These fields define the fundamental identity and connections of a strategy.
# Changing them would effectively create a different strategy instance.
# =============================================================================


@dataclass
class ConfigUpdateResult:
    """Result of a configuration update attempt.

    Attributes:
        success: Whether the update was successful
        error: Error message if the update failed
        updated_fields: List of fields that were updated (if successful)
        previous_values: Previous values of updated fields (for rollback)
        timestamp: When the update was attempted
    """

    success: bool
    error: str | None = None
    updated_fields: list[str] = field(default_factory=list)
    previous_values: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert the result to a dictionary for serialization."""
        return {
            "success": self.success,
            "error": self.error,
            "updated_fields": self.updated_fields,
            "previous_values": {k: str(v) if isinstance(v, Decimal) else v for k, v in self.previous_values.items()},
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class HotReloadableConfig:
    """Base class for hot-reloadable strategy configuration.

    This class defines parameters that can be safely changed at runtime without
    restarting the strategy. Changes take effect on the next iteration.

    Trading Parameters:
        max_slippage: Maximum allowed slippage for swaps (as decimal, e.g., 0.005 = 0.5%)
        trade_size_usd: Default trade size in USD
        rebalance_threshold: Position deviation threshold before rebalancing (as decimal)

    Risk Parameters:
        min_health_factor: Minimum health factor before reducing leverage (for lending)
        max_leverage: Maximum allowed leverage multiplier
        daily_loss_limit_usd: Maximum allowed loss per day in USD (triggers pause)

    The distinction between hot-reloadable and cold parameters:
    - Hot-reloadable: Can change at runtime, affect trading behavior
    - Cold parameters: Require restart, define strategy identity (see comments above)
    """

    # Trading parameters (hot-reloadable)
    max_slippage: Decimal = Decimal("0.005")  # 0.5% default
    trade_size_usd: Decimal = Decimal("1000")
    rebalance_threshold: Decimal = Decimal("0.05")  # 5% deviation

    # Risk parameters (hot-reloadable)
    min_health_factor: Decimal = Decimal("1.5")
    max_leverage: Decimal = Decimal("3")
    daily_loss_limit_usd: Decimal = Decimal("500")

    # Metadata
    last_updated: datetime | None = None
    update_count: int = 0

    # Valid ranges for validation
    _VALID_RANGES: dict[str, tuple[Decimal, Decimal]] = field(
        default_factory=lambda: {
            "max_slippage": (Decimal("0.001"), Decimal("0.1")),  # 0.1% to 10%
            "trade_size_usd": (Decimal("10"), Decimal("1000000")),  # $10 to $1M
            "rebalance_threshold": (Decimal("0.01"), Decimal("0.5")),  # 1% to 50%
            "min_health_factor": (Decimal("1.1"), Decimal("5")),  # 1.1 to 5
            "max_leverage": (Decimal("1"), Decimal("10")),  # 1x to 10x
            "daily_loss_limit_usd": (Decimal("0"), Decimal("1000000")),  # $0 to $1M
        },
        repr=False,
    )

    # Fields that are hot-reloadable
    HOT_RELOADABLE_FIELDS: set[str] = field(
        default_factory=lambda: {
            "max_slippage",
            "trade_size_usd",
            "rebalance_threshold",
            "min_health_factor",
            "max_leverage",
            "daily_loss_limit_usd",
        },
        repr=False,
    )

    def validate_field(self, field_name: str, value: Decimal) -> str | None:
        """Validate a field value against its valid range.

        Args:
            field_name: Name of the field to validate
            value: The value to validate

        Returns:
            Error message if validation fails, None if valid
        """
        if field_name not in self._VALID_RANGES:
            return f"Unknown field: {field_name}"

        min_val, max_val = self._VALID_RANGES[field_name]
        if value < min_val or value > max_val:
            return f"{field_name} must be between {min_val} and {max_val}, got {value}"

        return None

    def update(self, **kwargs: Any) -> ConfigUpdateResult:
        """Update configuration fields with validation.

        Only hot-reloadable fields can be updated. Values are validated
        against allowed ranges before being applied.

        Args:
            **kwargs: Field names and new values to update

        Returns:
            ConfigUpdateResult indicating success or failure
        """
        # Check for non-hot-reloadable fields
        non_reloadable = set(kwargs.keys()) - self.HOT_RELOADABLE_FIELDS
        if non_reloadable:
            return ConfigUpdateResult(
                success=False,
                error=f"Cannot hot-reload fields: {', '.join(sorted(non_reloadable))}. "
                f"These require a strategy restart.",
            )

        # Validate all values before applying any
        errors: list[str] = []
        for field_name, value in kwargs.items():
            if not isinstance(value, Decimal):
                try:
                    value = Decimal(str(value))
                    kwargs[field_name] = value
                except Exception:
                    errors.append(f"{field_name}: value must be a valid decimal")
                    continue

            error = self.validate_field(field_name, value)
            if error:
                errors.append(error)

        if errors:
            return ConfigUpdateResult(
                success=False,
                error="; ".join(errors),
            )

        # Store previous values for potential rollback
        previous_values: dict[str, Any] = {}
        for field_name in kwargs:
            previous_values[field_name] = getattr(self, field_name)

        # Apply updates atomically
        updated_fields: list[str] = []
        for field_name, value in kwargs.items():
            setattr(self, field_name, value)
            updated_fields.append(field_name)

        # Update metadata
        self.last_updated = datetime.now(UTC)
        self.update_count += 1

        return ConfigUpdateResult(
            success=True,
            updated_fields=updated_fields,
            previous_values=previous_values,
        )

    def get_valid_range(self, field_name: str) -> tuple[Decimal, Decimal] | None:
        """Get the valid range for a field.

        Args:
            field_name: Name of the field

        Returns:
            Tuple of (min, max) values, or None if field not found
        """
        return self._VALID_RANGES.get(field_name)

    def to_dict(self) -> dict[str, Any]:
        """Convert the configuration to a dictionary for serialization."""
        return {
            "trading_parameters": {
                "max_slippage": str(self.max_slippage),
                "trade_size_usd": str(self.trade_size_usd),
                "rebalance_threshold": str(self.rebalance_threshold),
            },
            "risk_parameters": {
                "min_health_factor": str(self.min_health_factor),
                "max_leverage": str(self.max_leverage),
                "daily_loss_limit_usd": str(self.daily_loss_limit_usd),
            },
            "metadata": {
                "last_updated": self.last_updated.isoformat() if self.last_updated else None,
                "update_count": self.update_count,
            },
            "valid_ranges": {
                field: {"min": str(min_val), "max": str(max_val)}
                for field, (min_val, max_val) in self._VALID_RANGES.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HotReloadableConfig":
        """Create a configuration from a dictionary.

        Args:
            data: Dictionary with configuration values

        Returns:
            HotReloadableConfig instance
        """
        trading = data.get("trading_parameters", {})
        risk = data.get("risk_parameters", {})
        metadata = data.get("metadata", {})

        config = cls(
            max_slippage=Decimal(trading.get("max_slippage", "0.005")),
            trade_size_usd=Decimal(trading.get("trade_size_usd", "1000")),
            rebalance_threshold=Decimal(trading.get("rebalance_threshold", "0.05")),
            min_health_factor=Decimal(risk.get("min_health_factor", "1.5")),
            max_leverage=Decimal(risk.get("max_leverage", "3")),
            daily_loss_limit_usd=Decimal(risk.get("daily_loss_limit_usd", "500")),
            update_count=metadata.get("update_count", 0),
        )

        if metadata.get("last_updated"):
            config.last_updated = datetime.fromisoformat(metadata["last_updated"])

        return config
