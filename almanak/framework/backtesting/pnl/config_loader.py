"""Configuration loading utilities for PnL backtesting.

This module provides functions for loading backtest configurations from
previous backtest result files, enabling reproducible backtests.

Key Functions:
    - load_config_from_result: Load PnLBacktestConfig from a backtest result JSON file
    - validate_loaded_config: Validate a loaded configuration dictionary

Example:
    from almanak.framework.backtesting.pnl.config_loader import (
        load_config_from_result,
        ConfigLoadError,
    )

    # Load config from previous backtest result
    try:
        config = load_config_from_result("results/backtest_20240601.json")
        # Re-run with same config
        result = await backtester.backtest(strategy, config)
    except ConfigLoadError as e:
        print(f"Failed to load config: {e}")
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import PnLBacktestConfig

logger = logging.getLogger(__name__)


class ConfigLoadError(Exception):
    """Exception raised when config loading fails.

    This exception is raised when:
    - The result file cannot be read or parsed
    - The result file does not contain valid config data
    - The config data fails validation
    """

    pass


@dataclass
class ConfigLoadResult:
    """Result of loading a config from a backtest result file.

    Attributes:
        config: The loaded PnLBacktestConfig instance
        metadata: Metadata from the original config (SDK version, timestamps, etc.)
        source_path: Path to the source file
        warnings: Any warnings generated during loading
    """

    config: PnLBacktestConfig
    metadata: dict[str, Any]
    source_path: Path
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "config": self.config.to_dict(),
            "metadata": self.metadata,
            "source_path": str(self.source_path),
            "warnings": self.warnings,
        }


def load_config_from_result(
    result_path: str | Path,
    *,
    strict: bool = False,
) -> ConfigLoadResult:
    """Load a PnLBacktestConfig from a backtest result JSON file.

    This function reads a backtest result file and extracts the configuration
    that was used to run the original backtest. This enables reproducible
    backtests by re-using the exact same configuration.

    The function handles both:
    - New format: config saved via `to_dict_with_metadata()` with `_metadata` section
    - Legacy format: config saved via `to_dict()` without metadata

    Args:
        result_path: Path to the backtest result JSON file
        strict: If True, raise errors for any warnings (default: False)

    Returns:
        ConfigLoadResult containing the loaded config, metadata, and any warnings

    Raises:
        ConfigLoadError: If the file cannot be read, parsed, or contains invalid config
        FileNotFoundError: If the result file does not exist

    Example:
        # Basic usage
        result = load_config_from_result("results/backtest_20240601.json")
        config = result.config

        # Check for warnings
        if result.warnings:
            print(f"Warnings: {result.warnings}")

        # Access metadata
        if result.metadata:
            print(f"Original SDK version: {result.metadata.get('sdk_version')}")
    """
    path = Path(result_path)

    # Check file exists
    if not path.exists():
        raise FileNotFoundError(f"Backtest result file not found: {path}")

    if not path.is_file():
        raise ConfigLoadError(f"Path is not a file: {path}")

    # Read and parse JSON
    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigLoadError(f"Invalid JSON in result file: {e}") from e
    except OSError as e:
        raise ConfigLoadError(f"Failed to read result file: {e}") from e

    # Extract config from result
    config_data = data.get("config")
    if config_data is None:
        raise ConfigLoadError(
            f"Result file does not contain 'config' field: {path}\n"
            "This may be an old result format or a different type of result file."
        )

    if not isinstance(config_data, dict):
        raise ConfigLoadError(f"'config' field is not a dictionary: type={type(config_data).__name__}")

    # Validate and load config
    warnings: list[str] = []
    metadata: dict[str, Any] = {}

    # Extract metadata if present (from to_dict_with_metadata())
    if "_metadata" in config_data:
        metadata = config_data.get("_metadata", {})
        logger.debug(f"Loaded config metadata: {metadata}")

        # Check SDK version compatibility
        original_sdk = metadata.get("sdk_version", "unknown")
        current_sdk = PnLBacktestConfig._get_sdk_version()
        if original_sdk != current_sdk and original_sdk != "unknown":
            warning = (
                f"SDK version mismatch: original={original_sdk}, current={current_sdk}. "
                "Results may differ slightly due to SDK changes."
            )
            warnings.append(warning)
            logger.warning(warning)

    # Validate required fields
    validation_result = validate_loaded_config(config_data)
    warnings.extend(validation_result.warnings)

    if validation_result.errors:
        error_msg = "Config validation failed:\n" + "\n".join(f"  - {e}" for e in validation_result.errors)
        raise ConfigLoadError(error_msg)

    # Handle strict mode
    if strict and warnings:
        raise ConfigLoadError("Strict mode enabled and warnings found:\n" + "\n".join(f"  - {w}" for w in warnings))

    # Create config from dict
    try:
        config = PnLBacktestConfig.from_dict(config_data)
    except (KeyError, ValueError, TypeError) as e:
        raise ConfigLoadError(f"Failed to create config from data: {e}") from e

    logger.info(f"Loaded config from {path}: {config}")

    return ConfigLoadResult(
        config=config,
        metadata=metadata,
        source_path=path,
        warnings=warnings,
    )


@dataclass
class ValidationResult:
    """Result of config validation.

    Attributes:
        errors: List of validation errors (fatal)
        warnings: List of validation warnings (non-fatal)
        is_valid: Whether the config is valid (no errors)
    """

    errors: list[str]
    warnings: list[str]

    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no errors)."""
        return len(self.errors) == 0


def validate_loaded_config(config_data: dict[str, Any]) -> ValidationResult:
    """Validate a loaded configuration dictionary.

    This function checks that the config data contains all required fields
    and that the values are valid. It performs both structural validation
    (required fields present) and semantic validation (values are valid).

    Args:
        config_data: Configuration dictionary to validate

    Returns:
        ValidationResult with any errors and warnings

    Example:
        result = validate_loaded_config(config_dict)
        if not result.is_valid:
            print(f"Validation errors: {result.errors}")
        if result.warnings:
            print(f"Warnings: {result.warnings}")
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Required fields for PnLBacktestConfig
    required_fields = ["start_time", "end_time"]

    for field in required_fields:
        if field not in config_data:
            errors.append(f"Missing required field: {field}")

    # Validate datetime fields
    for field in ["start_time", "end_time"]:
        if field in config_data:
            value = config_data[field]
            if isinstance(value, str):
                try:
                    datetime.fromisoformat(value)
                except ValueError:
                    errors.append(f"Invalid datetime format for {field}: {value}")

    # Validate start_time < end_time
    if "start_time" in config_data and "end_time" in config_data:
        try:
            start = (
                datetime.fromisoformat(config_data["start_time"])
                if isinstance(config_data["start_time"], str)
                else config_data["start_time"]
            )
            end = (
                datetime.fromisoformat(config_data["end_time"])
                if isinstance(config_data["end_time"], str)
                else config_data["end_time"]
            )
            if end <= start:
                errors.append(f"end_time ({end}) must be after start_time ({start})")
        except (ValueError, TypeError):
            pass  # Already reported above

    # Validate numeric fields
    numeric_fields = {
        "interval_seconds": (1, None, "must be positive"),
        "initial_capital_usd": (0, None, "must be positive"),
        "gas_price_gwei": (0, None, "cannot be negative"),
        "inclusion_delay_blocks": (0, None, "cannot be negative"),
        "trading_days_per_year": (1, 366, "must be between 1 and 366"),
    }

    for field, (min_val, max_val, msg) in numeric_fields.items():
        if field in config_data:
            try:
                value = float(config_data[field])
                if min_val is not None and value < min_val:
                    errors.append(f"{field} {msg}: {value}")
                if max_val is not None and value > max_val:
                    errors.append(f"{field} {msg}: {value}")
            except (ValueError, TypeError):
                errors.append(f"Invalid numeric value for {field}: {config_data[field]}")

    # Validate margin ratios
    margin_fields = ["initial_margin_ratio", "maintenance_margin_ratio"]
    for field in margin_fields:
        if field in config_data:
            try:
                value = float(config_data[field])
                if not (0 < value <= 1):
                    errors.append(f"{field} must be between 0 (exclusive) and 1 (inclusive): {value}")
            except (ValueError, TypeError):
                errors.append(f"Invalid numeric value for {field}: {config_data[field]}")

    # Validate tokens list
    if "tokens" in config_data:
        tokens = config_data["tokens"]
        if not isinstance(tokens, list):
            errors.append(f"tokens must be a list, got {type(tokens).__name__}")
        elif not tokens:
            errors.append("tokens list cannot be empty")

    # Derive known fields from PnLBacktestConfig dataclass to prevent schema drift.
    # Previously this was a manually curated set that fell out of sync with to_dict().
    known_fields = set(PnLBacktestConfig.__dataclass_fields__.keys()) | {
        # Computed properties emitted by to_dict() (read-only, not used for loading)
        "duration_seconds",
        "duration_days",
        "estimated_ticks",
        # Properties emitted by to_dict_with_metadata()
        "trading_days_per_year",
        # Metadata section
        "_metadata",
        "_meta",
    }

    for field in config_data:
        if field not in known_fields:
            logger.debug(f"Ignoring unknown field in config: {field}")
            warnings.append(f"Unknown field in config (will be ignored): {field}")

    return ValidationResult(errors=errors, warnings=warnings)


__all__ = [
    "ConfigLoadError",
    "ConfigLoadResult",
    "ValidationResult",
    "load_config_from_result",
    "validate_loaded_config",
]
