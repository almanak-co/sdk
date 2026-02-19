"""QA Configuration Module.

This module provides configuration management for the Data QA Framework,
including token lists, validation thresholds, and test parameters.

Example:
    from almanak.framework.data.qa.config import load_config, QAConfig

    # Load default configuration
    config = load_config()
    print(f"Chain: {config.chain}")
    print(f"Popular tokens: {config.popular_tokens}")

    # Load custom configuration
    config = load_config("path/to/custom_config.yaml")
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


@dataclass
class QAThresholds:
    """Validation thresholds for QA tests.

    Attributes:
        min_confidence: Minimum acceptable confidence score (0.0-1.0)
        max_price_impact_bps: Maximum acceptable price impact in basis points
        max_gap_hours: Maximum acceptable gap between OHLCV candles in hours
        max_stale_seconds: Maximum seconds before data is considered stale
    """

    min_confidence: float = 0.8
    max_price_impact_bps: int = 100
    max_gap_hours: float = 8.0
    max_stale_seconds: int = 120


@dataclass
class QAConfig:
    """Configuration for QA tests.

    Attributes:
        chain: Target blockchain network (arbitrum, base, ethereum)
        historical_days: Number of days of historical data to fetch
        timeframe: OHLCV candle timeframe (e.g., "4h", "1d")
        rsi_period: RSI calculation period
        thresholds: Validation thresholds
        popular_tokens: List of popular/high-liquidity tokens to test
        additional_tokens: List of additional tokens to test
        dex_tokens: List of tokens to test with DEX price sources
    """

    chain: str = "arbitrum"
    historical_days: int = 30
    timeframe: str = "4h"
    rsi_period: int = 14
    thresholds: QAThresholds = field(default_factory=QAThresholds)
    popular_tokens: list[str] = field(default_factory=list)
    additional_tokens: list[str] = field(default_factory=list)
    dex_tokens: list[str] = field(default_factory=list)

    @property
    def all_tokens(self) -> list[str]:
        """Return combined list of popular and additional tokens."""
        return self.popular_tokens + self.additional_tokens

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "chain": self.chain,
            "historical_days": self.historical_days,
            "timeframe": self.timeframe,
            "rsi_period": self.rsi_period,
            "thresholds": {
                "min_confidence": self.thresholds.min_confidence,
                "max_price_impact_bps": self.thresholds.max_price_impact_bps,
                "max_gap_hours": self.thresholds.max_gap_hours,
                "max_stale_seconds": self.thresholds.max_stale_seconds,
            },
            "popular_tokens": self.popular_tokens,
            "additional_tokens": self.additional_tokens,
            "dex_tokens": self.dex_tokens,
        }


def _get_default_config_path() -> Path:
    """Get the path to the default config.yaml file."""
    return Path(__file__).parent / "config.yaml"


def load_config(config_path: str | Path | None = None) -> QAConfig:
    """Load QA configuration from a YAML file.

    Args:
        config_path: Path to config YAML file. If None, uses default config.yaml.

    Returns:
        QAConfig instance with loaded configuration.

    Raises:
        FileNotFoundError: If config file does not exist.
        ValueError: If config file is invalid.
    """
    if config_path is None:
        config_path = _get_default_config_path()
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    logger.info("Loading QA config from %s", config_path)

    with open(config_path) as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Invalid config file format: expected dict, got {type(data)}")

    # Parse thresholds
    thresholds_data = data.get("thresholds", {})
    thresholds = QAThresholds(
        min_confidence=thresholds_data.get("min_confidence", 0.8),
        max_price_impact_bps=thresholds_data.get("max_price_impact_bps", 100),
        max_gap_hours=thresholds_data.get("max_gap_hours", 8.0),
        max_stale_seconds=thresholds_data.get("max_stale_seconds", 120),
    )

    # Build config
    config = QAConfig(
        chain=data.get("chain", "arbitrum"),
        historical_days=data.get("historical_days", 30),
        timeframe=data.get("timeframe", "4h"),
        rsi_period=data.get("rsi_period", 14),
        thresholds=thresholds,
        popular_tokens=data.get("popular_tokens", []),
        additional_tokens=data.get("additional_tokens", []),
        dex_tokens=data.get("dex_tokens", []),
    )

    logger.info(
        "Loaded QA config: chain=%s, tokens=%d popular + %d additional, dex_tokens=%d",
        config.chain,
        len(config.popular_tokens),
        len(config.additional_tokens),
        len(config.dex_tokens),
    )

    return config


__all__ = [
    "QAConfig",
    "QAThresholds",
    "load_config",
]
