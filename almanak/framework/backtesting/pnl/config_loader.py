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
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
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
    path = _resolve_result_file(result_path)
    data = _read_result_json(path)
    config_data = _extract_config_data(data, path)
    warnings: list[str] = []
    metadata = _extract_metadata(config_data, warnings)

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
    except (ArithmeticError, KeyError, ValueError, TypeError) as e:
        raise ConfigLoadError(f"Failed to create config from data: {e}") from e

    logger.info(f"Loaded config from {path}: {config}")

    return ConfigLoadResult(
        config=config,
        metadata=metadata,
        source_path=path,
        warnings=warnings,
    )


def _resolve_result_file(result_path: str | Path) -> Path:
    path = Path(result_path)
    if not path.exists():
        raise FileNotFoundError(f"Backtest result file not found: {path}")
    if not path.is_file():
        raise ConfigLoadError(f"Path is not a file: {path}")
    return path


def _read_result_json(path: Path) -> dict[str, Any]:
    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigLoadError(f"Invalid JSON in result file: {e}") from e
    except OSError as e:
        raise ConfigLoadError(f"Failed to read result file: {e}") from e

    if not isinstance(data, dict):
        raise ConfigLoadError(f"Result file root is not a dictionary: type={type(data).__name__}")
    return data


def _extract_config_data(data: Mapping[str, Any], path: Path) -> dict[str, Any]:
    config_data = data.get("config")
    if config_data is None:
        raise ConfigLoadError(
            f"Result file does not contain 'config' field: {path}\n"
            "This may be an old result format or a different type of result file."
        )
    if not isinstance(config_data, dict):
        raise ConfigLoadError(f"'config' field is not a dictionary: type={type(config_data).__name__}")
    return config_data


def _extract_metadata(config_data: Mapping[str, Any], warnings: list[str]) -> dict[str, Any]:
    if "_metadata" not in config_data:
        return {}

    metadata_raw = config_data.get("_metadata", {})
    if not isinstance(metadata_raw, Mapping):
        raise ConfigLoadError(f"'_metadata' field is not a dictionary: type={type(metadata_raw).__name__}")

    metadata = dict(metadata_raw)
    logger.debug(f"Loaded config metadata: {metadata}")
    _warn_on_sdk_version_mismatch(metadata, warnings)
    return metadata


def _warn_on_sdk_version_mismatch(metadata: Mapping[str, Any], warnings: list[str]) -> None:
    original_sdk = metadata.get("sdk_version", "unknown")
    current_sdk = PnLBacktestConfig._get_sdk_version()
    if original_sdk == current_sdk or original_sdk == "unknown":
        return

    warning = (
        f"SDK version mismatch: original={original_sdk}, current={current_sdk}. "
        "Results may differ slightly due to SDK changes."
    )
    warnings.append(warning)
    logger.warning(warning)


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


def validate_loaded_config(config_data: dict[str, Any]) -> ValidationResult:  # noqa: C901
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
    errors = _validate_config_errors(config_data)
    warnings = _unknown_config_field_warnings(config_data)

    return ValidationResult(errors=errors, warnings=warnings)


def _validate_config_errors(config_data: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    _validate_required_fields(config_data, errors)
    _validate_datetime_fields(config_data, errors)
    _validate_numeric_fields(config_data, errors)
    _validate_margin_ratios(config_data, errors)
    _validate_tokens(config_data, errors)
    return errors


def _validate_required_fields(config_data: Mapping[str, Any], errors: list[str]) -> None:
    for field in ("start_time", "end_time", "initial_capital_usd"):
        if field not in config_data:
            errors.append(f"Missing required field: {field}")


def _validate_datetime_fields(config_data: Mapping[str, Any], errors: list[str]) -> None:
    start = _parse_datetime_field("start_time", config_data, errors)
    end = _parse_datetime_field("end_time", config_data, errors)
    if start is None or end is None:
        return

    try:
        if end <= start:
            errors.append(f"end_time ({end}) must be after start_time ({start})")
    except TypeError:
        errors.append("start_time and end_time must use comparable timezone awareness")


def _parse_datetime_field(field: str, config_data: Mapping[str, Any], errors: list[str]) -> datetime | None:
    if field not in config_data:
        return None

    value = config_data[field]
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            errors.append(f"Invalid datetime format for {field}: {value}")
            return None

    errors.append(f"Invalid datetime value for {field}: expected ISO string or datetime, got {type(value).__name__}")
    return None


_NUMERIC_FIELD_RULES: dict[str, tuple[Decimal | None, Decimal | None, bool, str]] = {
    "interval_seconds": (Decimal("1"), None, True, "must be positive"),
    "initial_capital_usd": (Decimal("0"), None, False, "must be positive"),
    "gas_price_gwei": (Decimal("0"), None, True, "cannot be negative"),
    "inclusion_delay_blocks": (Decimal("0"), None, True, "cannot be negative"),
    "trading_days_per_year": (Decimal("1"), Decimal("366"), True, "must be between 1 and 366"),
}


def _validate_numeric_fields(config_data: Mapping[str, Any], errors: list[str]) -> None:
    for field, rule in _NUMERIC_FIELD_RULES.items():
        if field in config_data:
            _validate_numeric_field(field, config_data[field], rule, errors)


def _validate_numeric_field(
    field: str,
    raw_value: Any,
    rule: tuple[Decimal | None, Decimal | None, bool, str],
    errors: list[str],
) -> None:
    value = _parse_decimal_value(field, raw_value, errors)
    if value is None:
        return

    min_val, max_val, min_inclusive, message = rule
    if min_val is not None and _violates_minimum(value, min_val, min_inclusive):
        errors.append(f"{field} {message}: {value}")
    if max_val is not None and value > max_val:
        errors.append(f"{field} {message}: {value}")


def _parse_decimal_value(field: str, raw_value: Any, errors: list[str]) -> Decimal | None:
    if isinstance(raw_value, bool):
        errors.append(f"Invalid numeric value for {field}: {raw_value}")
        return None

    try:
        value = Decimal(str(raw_value))
    except (InvalidOperation, ValueError):
        errors.append(f"Invalid numeric value for {field}: {raw_value}")
        return None

    if not value.is_finite():
        errors.append(f"Invalid numeric value for {field}: {raw_value}")
        return None
    return value


def _violates_minimum(value: Decimal, min_val: Decimal, min_inclusive: bool) -> bool:
    return value < min_val if min_inclusive else value <= min_val


def _validate_margin_ratios(config_data: Mapping[str, Any], errors: list[str]) -> None:
    parsed_ratios: dict[str, Decimal] = {}
    for field in ("initial_margin_ratio", "maintenance_margin_ratio"):
        if field not in config_data:
            continue
        value = _parse_decimal_value(field, config_data[field], errors)
        if value is None:
            continue
        parsed_ratios[field] = value
        if not (Decimal("0") < value <= Decimal("1")):
            errors.append(f"{field} must be between 0 (exclusive) and 1 (inclusive): {value}")

    initial_margin = parsed_ratios.get("initial_margin_ratio")
    maintenance_margin = parsed_ratios.get("maintenance_margin_ratio")
    if initial_margin is not None and maintenance_margin is not None and maintenance_margin > initial_margin:
        errors.append("maintenance_margin_ratio must be <= initial_margin_ratio")


def _validate_tokens(config_data: Mapping[str, Any], errors: list[str]) -> None:
    if "tokens" not in config_data:
        return

    tokens = config_data["tokens"]
    if not isinstance(tokens, list):
        errors.append(f"tokens must be a list, got {type(tokens).__name__}")
    elif not tokens:
        errors.append("tokens list cannot be empty")


def _unknown_config_field_warnings(config_data: Mapping[str, Any]) -> list[str]:
    warnings: list[str] = []
    known_fields = _known_config_fields()
    for field in config_data:
        if field not in known_fields:
            logger.debug(f"Ignoring unknown field in config: {field}")
            warnings.append(f"Unknown field in config (will be ignored): {field}")
    return warnings


def _known_config_fields() -> set[str]:
    return set(PnLBacktestConfig.__dataclass_fields__.keys()) | {
        "duration_seconds",
        "duration_days",
        "estimated_ticks",
        "trading_days_per_year",
        "_metadata",
        "_meta",
    }


__all__ = [
    "ConfigLoadError",
    "ConfigLoadResult",
    "ValidationResult",
    "load_config_from_result",
    "validate_loaded_config",
]
