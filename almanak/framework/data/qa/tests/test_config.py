"""Tests for QA Configuration Module.

This test suite covers:
- QAConfig and QAThresholds dataclass creation
- Config loading from YAML files
- Config validation and defaults
- Error handling for missing/invalid config files
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from almanak.framework.data.qa.config import (
    QAConfig,
    QAThresholds,
    load_config,
)

# =============================================================================
# QAThresholds Tests
# =============================================================================


class TestQAThresholds:
    """Tests for QAThresholds dataclass."""

    def test_default_thresholds(self) -> None:
        """Test default threshold values."""
        thresholds = QAThresholds()

        assert thresholds.min_confidence == 0.8
        assert thresholds.max_price_impact_bps == 100
        assert thresholds.max_gap_hours == 8.0
        assert thresholds.max_stale_seconds == 120

    def test_custom_thresholds(self) -> None:
        """Test custom threshold values."""
        thresholds = QAThresholds(
            min_confidence=0.9,
            max_price_impact_bps=50,
            max_gap_hours=4.0,
            max_stale_seconds=60,
        )

        assert thresholds.min_confidence == 0.9
        assert thresholds.max_price_impact_bps == 50
        assert thresholds.max_gap_hours == 4.0
        assert thresholds.max_stale_seconds == 60


# =============================================================================
# QAConfig Tests
# =============================================================================


class TestQAConfig:
    """Tests for QAConfig dataclass."""

    def test_default_config(self) -> None:
        """Test default config values."""
        config = QAConfig()

        assert config.chain == "arbitrum"
        assert config.historical_days == 30
        assert config.timeframe == "4h"
        assert config.rsi_period == 14
        assert isinstance(config.thresholds, QAThresholds)
        assert config.popular_tokens == []
        assert config.additional_tokens == []
        assert config.dex_tokens == []

    def test_custom_config(self) -> None:
        """Test custom config values."""
        config = QAConfig(
            chain="base",
            historical_days=7,
            timeframe="1h",
            rsi_period=7,
            popular_tokens=["ETH", "USDC"],
            additional_tokens=["UNI"],
            dex_tokens=["ETH"],
        )

        assert config.chain == "base"
        assert config.historical_days == 7
        assert config.timeframe == "1h"
        assert config.rsi_period == 7
        assert config.popular_tokens == ["ETH", "USDC"]
        assert config.additional_tokens == ["UNI"]
        assert config.dex_tokens == ["ETH"]

    def test_all_tokens_property(self) -> None:
        """Test all_tokens property combines popular and additional."""
        config = QAConfig(
            popular_tokens=["ETH", "USDC"],
            additional_tokens=["UNI", "LINK"],
        )

        all_tokens = config.all_tokens
        assert len(all_tokens) == 4
        assert all_tokens == ["ETH", "USDC", "UNI", "LINK"]

    def test_to_dict(self) -> None:
        """Test config serialization to dict."""
        config = QAConfig(
            chain="arbitrum",
            historical_days=30,
            timeframe="4h",
            rsi_period=14,
            popular_tokens=["ETH"],
            additional_tokens=["UNI"],
            dex_tokens=["ETH"],
        )

        result = config.to_dict()

        assert result["chain"] == "arbitrum"
        assert result["historical_days"] == 30
        assert result["timeframe"] == "4h"
        assert result["rsi_period"] == 14
        assert result["popular_tokens"] == ["ETH"]
        assert result["additional_tokens"] == ["UNI"]
        assert result["dex_tokens"] == ["ETH"]
        assert "thresholds" in result
        assert result["thresholds"]["min_confidence"] == 0.8


# =============================================================================
# load_config Tests
# =============================================================================


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_default_config(self) -> None:
        """Test loading the default config.yaml file."""
        config = load_config()

        # Verify basic structure
        assert config.chain == "arbitrum"
        assert config.historical_days == 30
        assert config.timeframe == "4h"
        assert config.rsi_period == 14

        # Verify token lists
        assert len(config.popular_tokens) == 5
        assert "ETH" in config.popular_tokens
        assert "WBTC" in config.popular_tokens
        assert "USDC" in config.popular_tokens
        assert "LINK" in config.popular_tokens
        assert "ARB" in config.popular_tokens

        assert len(config.additional_tokens) == 5
        assert "GMX" in config.additional_tokens
        assert "PENDLE" in config.additional_tokens
        assert "UNI" in config.additional_tokens
        assert "DAI" in config.additional_tokens
        assert "RDNT" in config.additional_tokens

        assert len(config.dex_tokens) >= 5
        assert "USDC" in config.dex_tokens

        # Verify thresholds
        assert config.thresholds.min_confidence == 0.8
        assert config.thresholds.max_price_impact_bps == 100
        assert config.thresholds.max_gap_hours == 8.0
        assert config.thresholds.max_stale_seconds == 120

    def test_load_custom_config(self) -> None:
        """Test loading a custom config file."""
        custom_config = {
            "chain": "base",
            "historical_days": 7,
            "timeframe": "1h",
            "rsi_period": 7,
            "thresholds": {
                "min_confidence": 0.9,
                "max_price_impact_bps": 50,
                "max_gap_hours": 4.0,
                "max_stale_seconds": 60,
            },
            "popular_tokens": ["ETH", "USDC"],
            "additional_tokens": ["UNI"],
            "dex_tokens": ["ETH"],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(custom_config, f)
            temp_path = f.name

        try:
            config = load_config(temp_path)

            assert config.chain == "base"
            assert config.historical_days == 7
            assert config.timeframe == "1h"
            assert config.rsi_period == 7
            assert config.thresholds.min_confidence == 0.9
            assert config.thresholds.max_price_impact_bps == 50
            assert config.popular_tokens == ["ETH", "USDC"]
            assert config.additional_tokens == ["UNI"]
            assert config.dex_tokens == ["ETH"]
        finally:
            Path(temp_path).unlink()

    def test_load_config_with_path_object(self) -> None:
        """Test loading config with Path object."""
        custom_config = {
            "chain": "ethereum",
            "popular_tokens": ["ETH"],
            "additional_tokens": [],
            "dex_tokens": [],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(custom_config, f)
            temp_path = Path(f.name)

        try:
            config = load_config(temp_path)
            assert config.chain == "ethereum"
        finally:
            temp_path.unlink()

    def test_load_config_file_not_found(self) -> None:
        """Test error when config file doesn't exist."""
        with pytest.raises(FileNotFoundError) as exc_info:
            load_config("/nonexistent/path/config.yaml")

        assert "Config file not found" in str(exc_info.value)

    def test_load_config_invalid_format(self) -> None:
        """Test error when config file has invalid format."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("just a string, not a dict")
            temp_path = f.name

        try:
            with pytest.raises(ValueError) as exc_info:
                load_config(temp_path)

            assert "Invalid config file format" in str(exc_info.value)
        finally:
            Path(temp_path).unlink()

    def test_load_config_with_defaults(self) -> None:
        """Test that missing fields use default values."""
        minimal_config = {
            "chain": "base",
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(minimal_config, f)
            temp_path = f.name

        try:
            config = load_config(temp_path)

            # Explicit value
            assert config.chain == "base"

            # Defaults
            assert config.historical_days == 30
            assert config.timeframe == "4h"
            assert config.rsi_period == 14
            assert config.thresholds.min_confidence == 0.8
            assert config.popular_tokens == []
            assert config.additional_tokens == []
            assert config.dex_tokens == []
        finally:
            Path(temp_path).unlink()

    def test_load_config_partial_thresholds(self) -> None:
        """Test that partial threshold config uses defaults for missing."""
        partial_config = {
            "chain": "arbitrum",
            "thresholds": {
                "min_confidence": 0.95,
                # Other thresholds use defaults
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(partial_config, f)
            temp_path = f.name

        try:
            config = load_config(temp_path)

            assert config.thresholds.min_confidence == 0.95
            assert config.thresholds.max_price_impact_bps == 100  # default
            assert config.thresholds.max_gap_hours == 8.0  # default
            assert config.thresholds.max_stale_seconds == 120  # default
        finally:
            Path(temp_path).unlink()
