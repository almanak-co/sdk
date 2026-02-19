"""Unit tests for PnL backtest config loading functionality.

Tests cover:
- Loading config from valid result files
- Error handling for missing/invalid files
- Config validation logic
- SDK version mismatch warnings
"""

import json
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.config_loader import (
    ConfigLoadError,
    ConfigLoadResult,
    ValidationResult,
    load_config_from_result,
    validate_loaded_config,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def valid_config_dict() -> dict:
    """Create a valid config dictionary for testing."""
    return {
        "start_time": "2024-01-01T00:00:00+00:00",
        "end_time": "2024-06-01T00:00:00+00:00",
        "interval_seconds": 3600,
        "initial_capital_usd": "10000",
        "fee_model": "realistic",
        "slippage_model": "realistic",
        "include_gas_costs": True,
        "gas_price_gwei": "30",
        "inclusion_delay_blocks": 1,
        "chain": "arbitrum",
        "tokens": ["WETH", "USDC"],
        "benchmark_token": "WETH",
        "risk_free_rate": "0.05",
        "trading_days_per_year": 365,
        "initial_margin_ratio": "0.1",
        "maintenance_margin_ratio": "0.05",
        "mev_simulation_enabled": False,
        "auto_correct_positions": False,
        "reconciliation_alert_threshold_pct": "0.05",
        "random_seed": None,
        "duration_seconds": 13046400,
        "duration_days": 151.0,
        "estimated_ticks": 3624,
    }


@pytest.fixture
def valid_result_dict(valid_config_dict: dict) -> dict:
    """Create a valid backtest result dictionary for testing."""
    return {
        "engine": "pnl",
        "strategy_id": "test_strategy",
        "start_time": "2024-01-01T00:00:00+00:00",
        "end_time": "2024-06-01T00:00:00+00:00",
        "metrics": {
            "total_pnl_usd": "1000",
            "net_pnl_usd": "950",
            "sharpe_ratio": "1.5",
        },
        "trades": [],
        "equity_curve": [],
        "initial_capital_usd": "10000",
        "final_capital_usd": "11000",
        "chain": "arbitrum",
        "config": valid_config_dict,
    }


@pytest.fixture
def result_with_metadata(valid_result_dict: dict) -> dict:
    """Create a result dict with _metadata section."""
    valid_result_dict["config"]["_metadata"] = {
        "config_created_at": "2024-06-01T12:00:00+00:00",
        "python_version": "3.12.0",
        "sdk_version": "1.0.0",
    }
    return valid_result_dict


@pytest.fixture
def temp_result_file(valid_result_dict: dict) -> Path:
    """Create a temporary file with valid result JSON."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(valid_result_dict, f)
        return Path(f.name)


# =============================================================================
# Test load_config_from_result
# =============================================================================


class TestLoadConfigFromResult:
    """Tests for load_config_from_result function."""

    def test_loads_valid_config(self, temp_result_file: Path) -> None:
        """Test that valid config is loaded correctly."""
        result = load_config_from_result(temp_result_file)

        assert isinstance(result, ConfigLoadResult)
        assert isinstance(result.config, PnLBacktestConfig)
        assert result.config.chain == "arbitrum"
        assert result.config.initial_capital_usd == Decimal("10000")
        assert result.source_path == temp_result_file

    def test_loads_start_end_time(self, temp_result_file: Path) -> None:
        """Test that datetime fields are parsed correctly."""
        result = load_config_from_result(temp_result_file)

        assert result.config.start_time.year == 2024
        assert result.config.start_time.month == 1
        assert result.config.end_time.month == 6

    def test_loads_tokens_list(self, temp_result_file: Path) -> None:
        """Test that tokens list is loaded correctly."""
        result = load_config_from_result(temp_result_file)

        assert result.config.tokens == ["WETH", "USDC"]

    def test_extracts_metadata(self, result_with_metadata: dict) -> None:
        """Test that metadata is extracted when present."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(result_with_metadata, f)
            path = Path(f.name)

        result = load_config_from_result(path)

        assert result.metadata.get("sdk_version") == "1.0.0"
        assert result.metadata.get("python_version") == "3.12.0"

    def test_file_not_found(self) -> None:
        """Test error when file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            load_config_from_result("nonexistent_file.json")

    def test_invalid_json(self, tmp_path: Path) -> None:
        """Test error when file contains invalid JSON."""
        invalid_file = tmp_path / "invalid.json"
        invalid_file.write_text("{ invalid json }")

        with pytest.raises(ConfigLoadError) as exc_info:
            load_config_from_result(invalid_file)
        assert "Invalid JSON" in str(exc_info.value)

    def test_missing_config_field(self, tmp_path: Path) -> None:
        """Test error when config field is missing."""
        no_config_file = tmp_path / "no_config.json"
        no_config_file.write_text(json.dumps({"engine": "pnl", "strategy_id": "test"}))

        with pytest.raises(ConfigLoadError) as exc_info:
            load_config_from_result(no_config_file)
        assert "does not contain 'config' field" in str(exc_info.value)

    def test_config_not_dict(self, tmp_path: Path) -> None:
        """Test error when config is not a dictionary."""
        bad_config_file = tmp_path / "bad_config.json"
        bad_config_file.write_text(json.dumps({"config": "not a dict"}))

        with pytest.raises(ConfigLoadError) as exc_info:
            load_config_from_result(bad_config_file)
        assert "not a dictionary" in str(exc_info.value)

    def test_path_is_directory(self, tmp_path: Path) -> None:
        """Test error when path is a directory."""
        with pytest.raises(ConfigLoadError) as exc_info:
            load_config_from_result(tmp_path)
        assert "not a file" in str(exc_info.value)

    def test_sdk_version_mismatch_warning(self, result_with_metadata: dict) -> None:
        """Test that SDK version mismatch generates warning."""
        # Set a different SDK version
        result_with_metadata["config"]["_metadata"]["sdk_version"] = "0.9.0"

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(result_with_metadata, f)
            path = Path(f.name)

        result = load_config_from_result(path)

        # Should have a warning about version mismatch
        assert len(result.warnings) > 0
        assert any("SDK version mismatch" in w for w in result.warnings)

    def test_strict_mode_raises_on_warnings(self, result_with_metadata: dict) -> None:
        """Test that strict mode raises error on warnings."""
        result_with_metadata["config"]["_metadata"]["sdk_version"] = "0.9.0"

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(result_with_metadata, f)
            path = Path(f.name)

        with pytest.raises(ConfigLoadError) as exc_info:
            load_config_from_result(path, strict=True)
        assert "Strict mode" in str(exc_info.value)

    def test_accepts_string_path(self, temp_result_file: Path) -> None:
        """Test that string path works."""
        result = load_config_from_result(str(temp_result_file))
        assert result.config.chain == "arbitrum"

    def test_result_to_dict(self, temp_result_file: Path) -> None:
        """Test ConfigLoadResult serialization."""
        result = load_config_from_result(temp_result_file)
        result_dict = result.to_dict()

        assert "config" in result_dict
        assert "metadata" in result_dict
        assert "source_path" in result_dict
        assert "warnings" in result_dict


# =============================================================================
# Test validate_loaded_config
# =============================================================================


class TestValidateLoadedConfig:
    """Tests for validate_loaded_config function."""

    def test_valid_config_passes(self, valid_config_dict: dict) -> None:
        """Test that valid config passes validation."""
        result = validate_loaded_config(valid_config_dict)

        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_start_time(self) -> None:
        """Test error for missing start_time."""
        config = {"end_time": "2024-06-01T00:00:00+00:00"}
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("start_time" in e for e in result.errors)

    def test_missing_end_time(self) -> None:
        """Test error for missing end_time."""
        config = {"start_time": "2024-01-01T00:00:00+00:00"}
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("end_time" in e for e in result.errors)

    def test_invalid_datetime_format(self) -> None:
        """Test error for invalid datetime format."""
        config = {
            "start_time": "not-a-date",
            "end_time": "2024-06-01T00:00:00+00:00",
        }
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("Invalid datetime" in e for e in result.errors)

    def test_end_before_start(self) -> None:
        """Test error when end_time is before start_time."""
        config = {
            "start_time": "2024-06-01T00:00:00+00:00",
            "end_time": "2024-01-01T00:00:00+00:00",
        }
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("must be after" in e for e in result.errors)

    def test_negative_interval(self) -> None:
        """Test error for negative interval."""
        config = {
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-06-01T00:00:00+00:00",
            "interval_seconds": -100,
        }
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("interval_seconds" in e for e in result.errors)

    def test_negative_capital(self) -> None:
        """Test error for negative initial capital."""
        config = {
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-06-01T00:00:00+00:00",
            "initial_capital_usd": "-1000",
        }
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("initial_capital" in e for e in result.errors)

    def test_invalid_margin_ratio_low(self) -> None:
        """Test error for margin ratio <= 0."""
        config = {
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-06-01T00:00:00+00:00",
            "initial_margin_ratio": "0",
        }
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("initial_margin_ratio" in e for e in result.errors)

    def test_invalid_margin_ratio_high(self) -> None:
        """Test error for margin ratio > 1."""
        config = {
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-06-01T00:00:00+00:00",
            "initial_margin_ratio": "1.5",
        }
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("initial_margin_ratio" in e for e in result.errors)

    def test_empty_tokens_list(self) -> None:
        """Test error for empty tokens list."""
        config = {
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-06-01T00:00:00+00:00",
            "tokens": [],
        }
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("tokens" in e for e in result.errors)

    def test_tokens_not_list(self) -> None:
        """Test error when tokens is not a list."""
        config = {
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-06-01T00:00:00+00:00",
            "tokens": "WETH,USDC",
        }
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("tokens must be a list" in e for e in result.errors)

    def test_unknown_field_warning(self, valid_config_dict: dict) -> None:
        """Test warning for unknown fields."""
        valid_config_dict["unknown_field"] = "value"
        result = validate_loaded_config(valid_config_dict)

        assert result.is_valid  # Still valid, just a warning
        assert len(result.warnings) > 0
        assert any("unknown_field" in w for w in result.warnings)

    def test_validation_result_is_valid_property(self) -> None:
        """Test ValidationResult.is_valid property."""
        valid_result = ValidationResult(errors=[], warnings=["some warning"])
        assert valid_result.is_valid

        invalid_result = ValidationResult(errors=["some error"], warnings=[])
        assert not invalid_result.is_valid

    def test_numeric_parsing_error(self) -> None:
        """Test error for non-numeric values in numeric fields."""
        config = {
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-06-01T00:00:00+00:00",
            "interval_seconds": "not a number",
        }
        result = validate_loaded_config(config)

        assert not result.is_valid
        assert any("Invalid numeric" in e for e in result.errors)


# =============================================================================
# Test Integration - Round-trip serialization
# =============================================================================


class TestRoundTrip:
    """Tests for config serialization round-trip."""

    def test_config_roundtrip_basic(self) -> None:
        """Test that config can be serialized and deserialized."""
        original = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            initial_capital_usd=Decimal("50000"),
            chain="base",
            tokens=["WETH", "USDC", "AAVE"],
        )

        # Serialize
        config_dict = original.to_dict()

        # Validate
        validation = validate_loaded_config(config_dict)
        assert validation.is_valid, f"Validation errors: {validation.errors}"

        # Deserialize
        loaded = PnLBacktestConfig.from_dict(config_dict)

        # Verify
        assert loaded.start_time.year == original.start_time.year
        assert loaded.end_time.year == original.end_time.year
        assert loaded.initial_capital_usd == original.initial_capital_usd
        assert loaded.chain == original.chain
        assert loaded.tokens == original.tokens

    def test_config_roundtrip_with_metadata(self) -> None:
        """Test that config with metadata can be loaded."""
        original = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            initial_capital_usd=Decimal("10000"),
            random_seed=42,
        )

        # Serialize with metadata
        config_dict = original.to_dict_with_metadata()

        # Should have metadata
        assert "_metadata" in config_dict

        # Validate
        validation = validate_loaded_config(config_dict)
        assert validation.is_valid, f"Validation errors: {validation.errors}"

        # Deserialize
        loaded = PnLBacktestConfig.from_dict(config_dict)

        # Verify
        assert loaded.random_seed == 42

    def test_full_roundtrip_via_file(self) -> None:
        """Test full round-trip through file system."""
        original = PnLBacktestConfig(
            start_time=datetime(2024, 3, 15),
            end_time=datetime(2024, 9, 15),
            interval_seconds=7200,
            initial_capital_usd=Decimal("25000"),
            chain="optimism",
            tokens=["WETH", "USDC", "OP"],
            gas_price_gwei=Decimal("0.001"),
            mev_simulation_enabled=True,
        )

        # Create a mock result file
        result_dict = {
            "engine": "pnl",
            "strategy_id": "roundtrip_test",
            "config": original.to_dict_with_metadata(),
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(result_dict, f)
            path = Path(f.name)

        # Load from file
        loaded_result = load_config_from_result(path)
        loaded = loaded_result.config

        # Verify all fields match
        assert loaded.start_time.date() == original.start_time.date()
        assert loaded.end_time.date() == original.end_time.date()
        assert loaded.interval_seconds == original.interval_seconds
        assert loaded.initial_capital_usd == original.initial_capital_usd
        assert loaded.chain == original.chain
        assert loaded.tokens == original.tokens
        assert loaded.gas_price_gwei == original.gas_price_gwei
        assert loaded.mev_simulation_enabled == original.mev_simulation_enabled
