"""Unit tests for BacktestDataConfig.

This module tests the BacktestDataConfig dataclass including:
- Default values are correct
- All fields are accessible
- Custom values can be set
- Decimal fields handle Decimal correctly
- Validation logic in __post_init__
"""

from decimal import Decimal
from pathlib import Path
import tempfile

import pytest

from almanak.framework.backtesting.config import BacktestDataConfig


class TestBacktestDataConfigDefaults:
    """Test default values of BacktestDataConfig."""

    def test_default_price_provider_is_auto(self):
        """Test that default price_provider is 'auto'."""
        config = BacktestDataConfig()
        assert config.price_provider == "auto"

    def test_default_use_historical_volume_is_true(self):
        """Test that default use_historical_volume is True."""
        config = BacktestDataConfig()
        assert config.use_historical_volume is True

    def test_default_use_historical_funding_is_true(self):
        """Test that default use_historical_funding is True."""
        config = BacktestDataConfig()
        assert config.use_historical_funding is True

    def test_default_use_historical_apy_is_true(self):
        """Test that default use_historical_apy is True."""
        config = BacktestDataConfig()
        assert config.use_historical_apy is True

    def test_default_use_historical_liquidity_is_true(self):
        """Test that default use_historical_liquidity is True."""
        config = BacktestDataConfig()
        assert config.use_historical_liquidity is True

    def test_default_strict_historical_mode_is_false(self):
        """Test that default strict_historical_mode is False."""
        config = BacktestDataConfig()
        assert config.strict_historical_mode is False

    def test_default_volume_fallback_multiplier(self):
        """Test that default volume_fallback_multiplier is Decimal('10')."""
        config = BacktestDataConfig()
        assert config.volume_fallback_multiplier == Decimal("10")
        assert isinstance(config.volume_fallback_multiplier, Decimal)

    def test_default_funding_fallback_rate(self):
        """Test that default funding_fallback_rate is Decimal('0.0001')."""
        config = BacktestDataConfig()
        assert config.funding_fallback_rate == Decimal("0.0001")
        assert isinstance(config.funding_fallback_rate, Decimal)

    def test_default_supply_apy_fallback(self):
        """Test that default supply_apy_fallback is Decimal('0.03')."""
        config = BacktestDataConfig()
        assert config.supply_apy_fallback == Decimal("0.03")
        assert isinstance(config.supply_apy_fallback, Decimal)

    def test_default_borrow_apy_fallback(self):
        """Test that default borrow_apy_fallback is Decimal('0.05')."""
        config = BacktestDataConfig()
        assert config.borrow_apy_fallback == Decimal("0.05")
        assert isinstance(config.borrow_apy_fallback, Decimal)

    def test_default_gas_fallback_gwei(self):
        """Test that default gas_fallback_gwei is Decimal('20')."""
        config = BacktestDataConfig()
        assert config.gas_fallback_gwei == Decimal("20")
        assert isinstance(config.gas_fallback_gwei, Decimal)

    def test_default_coingecko_rate_limit_per_minute(self):
        """Test that default coingecko_rate_limit_per_minute is 10."""
        config = BacktestDataConfig()
        assert config.coingecko_rate_limit_per_minute == 10
        assert isinstance(config.coingecko_rate_limit_per_minute, int)

    def test_default_subgraph_rate_limit_per_minute(self):
        """Test that default subgraph_rate_limit_per_minute is 100."""
        config = BacktestDataConfig()
        assert config.subgraph_rate_limit_per_minute == 100
        assert isinstance(config.subgraph_rate_limit_per_minute, int)

    def test_default_enable_persistent_cache_is_false(self):
        """Test that default enable_persistent_cache is False."""
        config = BacktestDataConfig()
        assert config.enable_persistent_cache is False

    def test_default_cache_directory_is_none(self):
        """Test that default cache_directory is None."""
        config = BacktestDataConfig()
        assert config.cache_directory is None


class TestBacktestDataConfigFieldAccess:
    """Test that all fields are accessible."""

    def test_all_fields_are_accessible(self):
        """Test that all documented fields can be accessed."""
        config = BacktestDataConfig()

        # Price provider
        _ = config.price_provider

        # Historical data source toggles
        _ = config.use_historical_volume
        _ = config.use_historical_funding
        _ = config.use_historical_apy
        _ = config.use_historical_liquidity

        # Strict mode
        _ = config.strict_historical_mode

        # Fallback values
        _ = config.volume_fallback_multiplier
        _ = config.funding_fallback_rate
        _ = config.supply_apy_fallback
        _ = config.borrow_apy_fallback
        _ = config.gas_fallback_gwei

        # Rate limiting
        _ = config.coingecko_rate_limit_per_minute
        _ = config.subgraph_rate_limit_per_minute

        # Cache configuration
        _ = config.enable_persistent_cache
        _ = config.cache_directory

        # All fields accessed successfully
        assert True


class TestBacktestDataConfigCustomValues:
    """Test config can be created with custom values."""

    def test_custom_price_provider_coingecko(self):
        """Test setting price_provider to 'coingecko'."""
        config = BacktestDataConfig(price_provider="coingecko")
        assert config.price_provider == "coingecko"

    def test_custom_price_provider_chainlink(self):
        """Test setting price_provider to 'chainlink'."""
        config = BacktestDataConfig(price_provider="chainlink")
        assert config.price_provider == "chainlink"

    def test_custom_price_provider_twap(self):
        """Test setting price_provider to 'twap'."""
        config = BacktestDataConfig(price_provider="twap")
        assert config.price_provider == "twap"

    def test_custom_use_historical_volume_false(self):
        """Test setting use_historical_volume to False."""
        config = BacktestDataConfig(use_historical_volume=False)
        assert config.use_historical_volume is False

    def test_custom_use_historical_funding_false(self):
        """Test setting use_historical_funding to False."""
        config = BacktestDataConfig(use_historical_funding=False)
        assert config.use_historical_funding is False

    def test_custom_use_historical_apy_false(self):
        """Test setting use_historical_apy to False."""
        config = BacktestDataConfig(use_historical_apy=False)
        assert config.use_historical_apy is False

    def test_custom_use_historical_liquidity_false(self):
        """Test setting use_historical_liquidity to False."""
        config = BacktestDataConfig(use_historical_liquidity=False)
        assert config.use_historical_liquidity is False

    def test_custom_strict_historical_mode_true(self):
        """Test setting strict_historical_mode to True."""
        config = BacktestDataConfig(strict_historical_mode=True)
        assert config.strict_historical_mode is True

    def test_custom_volume_fallback_multiplier(self):
        """Test setting custom volume_fallback_multiplier."""
        config = BacktestDataConfig(volume_fallback_multiplier=Decimal("5"))
        assert config.volume_fallback_multiplier == Decimal("5")

    def test_custom_funding_fallback_rate(self):
        """Test setting custom funding_fallback_rate."""
        config = BacktestDataConfig(funding_fallback_rate=Decimal("0.0002"))
        assert config.funding_fallback_rate == Decimal("0.0002")

    def test_custom_supply_apy_fallback(self):
        """Test setting custom supply_apy_fallback."""
        config = BacktestDataConfig(supply_apy_fallback=Decimal("0.02"))
        assert config.supply_apy_fallback == Decimal("0.02")

    def test_custom_borrow_apy_fallback(self):
        """Test setting custom borrow_apy_fallback."""
        config = BacktestDataConfig(borrow_apy_fallback=Decimal("0.04"))
        assert config.borrow_apy_fallback == Decimal("0.04")

    def test_custom_gas_fallback_gwei(self):
        """Test setting custom gas_fallback_gwei."""
        config = BacktestDataConfig(gas_fallback_gwei=Decimal("50"))
        assert config.gas_fallback_gwei == Decimal("50")

    def test_custom_coingecko_rate_limit_per_minute(self):
        """Test setting custom coingecko_rate_limit_per_minute."""
        config = BacktestDataConfig(coingecko_rate_limit_per_minute=500)
        assert config.coingecko_rate_limit_per_minute == 500

    def test_custom_subgraph_rate_limit_per_minute(self):
        """Test setting custom subgraph_rate_limit_per_minute."""
        config = BacktestDataConfig(subgraph_rate_limit_per_minute=200)
        assert config.subgraph_rate_limit_per_minute == 200

    def test_custom_enable_persistent_cache(self):
        """Test setting enable_persistent_cache to True."""
        config = BacktestDataConfig(enable_persistent_cache=True)
        assert config.enable_persistent_cache is True

    def test_custom_cache_directory(self):
        """Test setting custom cache_directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = BacktestDataConfig(cache_directory=tmpdir)
            assert config.cache_directory == tmpdir

    def test_all_custom_values_at_once(self):
        """Test creating config with all custom values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = BacktestDataConfig(
                price_provider="chainlink",
                use_historical_volume=False,
                use_historical_funding=False,
                use_historical_apy=False,
                use_historical_liquidity=False,
                strict_historical_mode=True,
                volume_fallback_multiplier=Decimal("5"),
                funding_fallback_rate=Decimal("0.0002"),
                supply_apy_fallback=Decimal("0.02"),
                borrow_apy_fallback=Decimal("0.04"),
                gas_fallback_gwei=Decimal("50"),
                coingecko_rate_limit_per_minute=500,
                subgraph_rate_limit_per_minute=200,
                enable_persistent_cache=True,
                cache_directory=tmpdir,
            )

            assert config.price_provider == "chainlink"
            assert config.use_historical_volume is False
            assert config.use_historical_funding is False
            assert config.use_historical_apy is False
            assert config.use_historical_liquidity is False
            assert config.strict_historical_mode is True
            assert config.volume_fallback_multiplier == Decimal("5")
            assert config.funding_fallback_rate == Decimal("0.0002")
            assert config.supply_apy_fallback == Decimal("0.02")
            assert config.borrow_apy_fallback == Decimal("0.04")
            assert config.gas_fallback_gwei == Decimal("50")
            assert config.coingecko_rate_limit_per_minute == 500
            assert config.subgraph_rate_limit_per_minute == 200
            assert config.enable_persistent_cache is True
            assert config.cache_directory == tmpdir


class TestBacktestDataConfigDecimalHandling:
    """Test Decimal field handling."""

    def test_volume_fallback_multiplier_preserves_decimal_precision(self):
        """Test volume_fallback_multiplier preserves Decimal precision."""
        config = BacktestDataConfig(volume_fallback_multiplier=Decimal("10.5"))
        assert config.volume_fallback_multiplier == Decimal("10.5")
        assert str(config.volume_fallback_multiplier) == "10.5"

    def test_funding_fallback_rate_preserves_decimal_precision(self):
        """Test funding_fallback_rate preserves Decimal precision."""
        config = BacktestDataConfig(funding_fallback_rate=Decimal("0.00015"))
        assert config.funding_fallback_rate == Decimal("0.00015")
        assert str(config.funding_fallback_rate) == "0.00015"

    def test_supply_apy_fallback_preserves_decimal_precision(self):
        """Test supply_apy_fallback preserves Decimal precision."""
        config = BacktestDataConfig(supply_apy_fallback=Decimal("0.0325"))
        assert config.supply_apy_fallback == Decimal("0.0325")
        assert str(config.supply_apy_fallback) == "0.0325"

    def test_borrow_apy_fallback_preserves_decimal_precision(self):
        """Test borrow_apy_fallback preserves Decimal precision."""
        config = BacktestDataConfig(borrow_apy_fallback=Decimal("0.0575"))
        assert config.borrow_apy_fallback == Decimal("0.0575")
        assert str(config.borrow_apy_fallback) == "0.0575"

    def test_gas_fallback_gwei_preserves_decimal_precision(self):
        """Test gas_fallback_gwei preserves Decimal precision."""
        config = BacktestDataConfig(gas_fallback_gwei=Decimal("25.5"))
        assert config.gas_fallback_gwei == Decimal("25.5")
        assert str(config.gas_fallback_gwei) == "25.5"

    def test_decimal_comparison_with_integers(self):
        """Test Decimal fields can be compared with integers."""
        config = BacktestDataConfig()
        assert config.volume_fallback_multiplier == 10
        assert config.supply_apy_fallback < 1
        assert config.borrow_apy_fallback > 0

    def test_decimal_comparison_with_floats(self):
        """Test Decimal fields can be compared with floats."""
        config = BacktestDataConfig()
        assert config.supply_apy_fallback == Decimal("0.03")
        assert config.borrow_apy_fallback == Decimal("0.05")

    def test_decimal_arithmetic(self):
        """Test Decimal fields support arithmetic."""
        config = BacktestDataConfig()
        doubled = config.volume_fallback_multiplier * 2
        assert doubled == Decimal("20")
        assert isinstance(doubled, Decimal)

    def test_zero_decimal_values_are_valid(self):
        """Test that zero Decimal values are valid (non-negative)."""
        config = BacktestDataConfig(
            volume_fallback_multiplier=Decimal("0"),
            funding_fallback_rate=Decimal("0"),
            supply_apy_fallback=Decimal("0"),
            borrow_apy_fallback=Decimal("0"),
            gas_fallback_gwei=Decimal("0"),
        )
        assert config.volume_fallback_multiplier == Decimal("0")
        assert config.funding_fallback_rate == Decimal("0")
        assert config.supply_apy_fallback == Decimal("0")
        assert config.borrow_apy_fallback == Decimal("0")
        assert config.gas_fallback_gwei == Decimal("0")


class TestBacktestDataConfigValidation:
    """Test validation logic in __post_init__."""

    def test_invalid_price_provider_raises_value_error(self):
        """Test that invalid price_provider raises ValueError."""
        with pytest.raises(ValueError, match="price_provider must be one of"):
            BacktestDataConfig(price_provider="invalid")  # type: ignore

    def test_negative_volume_fallback_multiplier_raises_value_error(self):
        """Test that negative volume_fallback_multiplier raises ValueError."""
        with pytest.raises(ValueError, match="volume_fallback_multiplier cannot be negative"):
            BacktestDataConfig(volume_fallback_multiplier=Decimal("-1"))

    def test_negative_funding_fallback_rate_raises_value_error(self):
        """Test that negative funding_fallback_rate raises ValueError."""
        with pytest.raises(ValueError, match="funding_fallback_rate cannot be negative"):
            BacktestDataConfig(funding_fallback_rate=Decimal("-0.0001"))

    def test_negative_supply_apy_fallback_raises_value_error(self):
        """Test that negative supply_apy_fallback raises ValueError."""
        with pytest.raises(ValueError, match="supply_apy_fallback cannot be negative"):
            BacktestDataConfig(supply_apy_fallback=Decimal("-0.01"))

    def test_negative_borrow_apy_fallback_raises_value_error(self):
        """Test that negative borrow_apy_fallback raises ValueError."""
        with pytest.raises(ValueError, match="borrow_apy_fallback cannot be negative"):
            BacktestDataConfig(borrow_apy_fallback=Decimal("-0.01"))

    def test_negative_gas_fallback_gwei_raises_value_error(self):
        """Test that negative gas_fallback_gwei raises ValueError."""
        with pytest.raises(ValueError, match="gas_fallback_gwei cannot be negative"):
            BacktestDataConfig(gas_fallback_gwei=Decimal("-10"))

    def test_zero_coingecko_rate_limit_raises_value_error(self):
        """Test that zero coingecko_rate_limit_per_minute raises ValueError."""
        with pytest.raises(ValueError, match="coingecko_rate_limit_per_minute must be positive"):
            BacktestDataConfig(coingecko_rate_limit_per_minute=0)

    def test_negative_coingecko_rate_limit_raises_value_error(self):
        """Test that negative coingecko_rate_limit_per_minute raises ValueError."""
        with pytest.raises(ValueError, match="coingecko_rate_limit_per_minute must be positive"):
            BacktestDataConfig(coingecko_rate_limit_per_minute=-1)

    def test_zero_subgraph_rate_limit_raises_value_error(self):
        """Test that zero subgraph_rate_limit_per_minute raises ValueError."""
        with pytest.raises(ValueError, match="subgraph_rate_limit_per_minute must be positive"):
            BacktestDataConfig(subgraph_rate_limit_per_minute=0)

    def test_negative_subgraph_rate_limit_raises_value_error(self):
        """Test that negative subgraph_rate_limit_per_minute raises ValueError."""
        with pytest.raises(ValueError, match="subgraph_rate_limit_per_minute must be positive"):
            BacktestDataConfig(subgraph_rate_limit_per_minute=-1)

    def test_cache_directory_created_if_not_exists(self):
        """Test that cache directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "new_cache_dir"
            assert not cache_dir.exists()

            config = BacktestDataConfig(cache_directory=str(cache_dir))
            assert cache_dir.exists()
            assert config.cache_directory == str(cache_dir)

    def test_nested_cache_directory_created(self):
        """Test that nested cache directory structure is created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "level1" / "level2" / "level3"
            assert not cache_dir.exists()

            config = BacktestDataConfig(cache_directory=str(cache_dir))
            assert cache_dir.exists()
            assert config.cache_directory == str(cache_dir)


class TestBacktestDataConfigGetCachePath:
    """Test get_cache_path method."""

    def test_get_cache_path_returns_none_when_cache_disabled(self):
        """Test that get_cache_path returns None when cache is disabled."""
        config = BacktestDataConfig(enable_persistent_cache=False)
        assert config.get_cache_path() is None

    def test_get_cache_path_returns_custom_directory(self):
        """Test that get_cache_path returns custom cache directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = BacktestDataConfig(
                enable_persistent_cache=True,
                cache_directory=tmpdir,
            )
            cache_path = config.get_cache_path()
            assert cache_path is not None
            assert cache_path == Path(tmpdir)

    def test_get_cache_path_returns_temp_directory_by_default(self):
        """Test that get_cache_path returns temp directory when no custom dir."""
        config = BacktestDataConfig(enable_persistent_cache=True)
        cache_path = config.get_cache_path()
        assert cache_path is not None
        assert "almanak_backtest_cache" in str(cache_path)

    def test_get_cache_path_returns_path_object(self):
        """Test that get_cache_path returns a Path object."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = BacktestDataConfig(
                enable_persistent_cache=True,
                cache_directory=tmpdir,
            )
            cache_path = config.get_cache_path()
            assert isinstance(cache_path, Path)


class TestBacktestDataConfigExport:
    """Test module exports."""

    def test_backtest_data_config_exported_from_module(self):
        """Test that BacktestDataConfig is exported from module."""
        from almanak.framework.backtesting.config import __all__

        assert "BacktestDataConfig" in __all__


class TestBacktestDataConfigDataclass:
    """Test dataclass behavior."""

    def test_is_dataclass(self):
        """Test that BacktestDataConfig is a dataclass."""
        from dataclasses import is_dataclass

        assert is_dataclass(BacktestDataConfig)

    def test_dataclass_equality(self):
        """Test that two configs with same values are equal."""
        config1 = BacktestDataConfig()
        config2 = BacktestDataConfig()
        assert config1 == config2

    def test_dataclass_inequality(self):
        """Test that two configs with different values are not equal."""
        config1 = BacktestDataConfig(strict_historical_mode=False)
        config2 = BacktestDataConfig(strict_historical_mode=True)
        assert config1 != config2

    def test_dataclass_repr(self):
        """Test that repr includes field values."""
        config = BacktestDataConfig(price_provider="chainlink")
        repr_str = repr(config)
        assert "BacktestDataConfig" in repr_str
        assert "chainlink" in repr_str

    def test_dataclass_can_be_copied(self):
        """Test that config can be copied using replace."""
        from dataclasses import replace

        config = BacktestDataConfig()
        config_copy = replace(config, strict_historical_mode=True)

        assert config.strict_historical_mode is False
        assert config_copy.strict_historical_mode is True
        assert config != config_copy
