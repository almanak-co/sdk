"""Tests for StrategyBase plain dict config handling (VIB-120, VIB-324)."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from unittest.mock import patch

from almanak.framework.models.hot_reload_config import HotReloadableConfig
from almanak.framework.strategies.base import StrategyBase


# --- Concrete subclass for testing (StrategyBase is abstract) ---


class _ConcreteStrategy(StrategyBase):
    """Minimal concrete strategy for testing StrategyBase."""

    STRATEGY_NAME = "TEST"

    def run(self) -> Any:
        return None


# --- VIB-120: plain dict config support ---


class TestStrategyBaseDictConfig:
    """StrategyBase must accept plain dict configs without AttributeError."""

    @patch("almanak.framework.strategies.base.add_event")
    def test_init_with_plain_dict(self, _mock_event):
        """Plain dict config should not raise on instantiation."""
        config = {"strategy_id": "test-1", "chain": "arbitrum", "max_slippage": 0.005}
        strategy = _ConcreteStrategy(config)

        assert strategy.config is config
        # getattr on a dict doesn't resolve keys, so strategy_id/chain default to "unknown"
        assert strategy.strategy_id == "unknown"
        assert strategy.chain == "unknown"

    @patch("almanak.framework.strategies.base.add_event")
    def test_init_with_empty_dict(self, _mock_event):
        """Empty dict config should work (strategy_id/chain default to 'unknown')."""
        strategy = _ConcreteStrategy({})

        assert strategy.strategy_id == "unknown"
        assert strategy.chain == "unknown"

    @patch("almanak.framework.strategies.base.add_event")
    def test_dict_config_snapshot_saved(self, _mock_event):
        """Config snapshot should contain the dict contents."""
        config = {"strategy_id": "snap-test", "trade_size_usd": 1000}
        strategy = _ConcreteStrategy(config)

        snapshots = strategy.get_config_history()
        assert len(snapshots) == 1
        assert snapshots[0]["config_dict"] == config
        assert snapshots[0]["version"] == 1
        assert snapshots[0]["updated_by"] == "initialization"

    @patch("almanak.framework.strategies.base.add_event")
    def test_dict_config_is_shallow_copy(self, _mock_event):
        """Snapshot should be a copy, not a reference to the original dict."""
        config = {"strategy_id": "copy-test", "value": 42}
        strategy = _ConcreteStrategy(config)

        snapshots = strategy.get_config_history()
        snapshot_dict = snapshots[0]["config_dict"]
        # Mutating original should not affect snapshot
        config["value"] = 999
        assert snapshot_dict["value"] == 42

    @patch("almanak.framework.strategies.base.add_event")
    def test_hot_reloadable_config_still_works(self, _mock_event):
        """HotReloadableConfig (the normal path) should still work."""
        config = HotReloadableConfig(
            max_slippage=Decimal("0.005"),
            trade_size_usd=Decimal("1000"),
        )
        strategy = _ConcreteStrategy(config)

        snapshots = strategy.get_config_history()
        assert len(snapshots) == 1
        assert "trading_parameters" in snapshots[0]["config_dict"]

    @patch("almanak.framework.strategies.base.add_event")
    def test_non_dict_non_config_object(self, _mock_event):
        """An object with neither to_dict() nor dict behavior should get empty snapshot."""

        class BareConfig:
            strategy_id = "bare"
            chain = "ethereum"

        strategy = _ConcreteStrategy(BareConfig())

        snapshots = strategy.get_config_history()
        assert snapshots[0]["config_dict"] == {}


# --- VIB-324: @dataclass config snapshot support ---


class TestDataclassConfigSnapshot:
    """StrategyBase should auto-detect @dataclass configs via dataclasses.asdict()."""

    @patch("almanak.framework.strategies.base.add_event")
    def test_dataclass_config_snapshot_populated(self, _mock_event):
        """@dataclass config should produce a non-empty snapshot."""

        @dataclass
        class MyStratConfig:
            strategy_id: str = "dc-test"
            chain: str = "base"
            trade_size_usd: int = 500
            pool_address: str = "0xabc"

        strategy = _ConcreteStrategy(MyStratConfig())

        snapshots = strategy.get_config_history()
        assert len(snapshots) == 1
        config_dict = snapshots[0]["config_dict"]
        assert config_dict["trade_size_usd"] == 500
        assert config_dict["pool_address"] == "0xabc"

    @patch("almanak.framework.strategies.base.add_event")
    def test_dataclass_config_no_warning(self, _mock_event, caplog):
        """@dataclass config should NOT emit the 'has no to_dict()' warning."""
        import logging

        @dataclass
        class QuietConfig:
            strategy_id: str = "quiet"
            chain: str = "arbitrum"

        with caplog.at_level(logging.WARNING, logger="almanak.framework.strategies.base"):
            _ConcreteStrategy(QuietConfig())

        assert "has no to_dict()" not in caplog.text

    @patch("almanak.framework.strategies.base.add_event")
    def test_dataclass_with_decimal_fields(self, _mock_event):
        """@dataclass config with Decimal fields should serialize correctly."""

        @dataclass
        class DecimalConfig:
            strategy_id: str = "dec-test"
            chain: str = "base"
            max_slippage: Decimal = Decimal("0.005")
            trade_size: Decimal = Decimal("1000.50")

        strategy = _ConcreteStrategy(DecimalConfig())

        snapshots = strategy.get_config_history()
        config_dict = snapshots[0]["config_dict"]
        assert config_dict["max_slippage"] == Decimal("0.005")
        assert config_dict["trade_size"] == Decimal("1000.50")


# --- VIB-149: get_config() utility method ---


class TestGetConfig:
    """StrategyBase.get_config(key, default) works for all config types."""

    @patch("almanak.framework.strategies.base.add_event")
    def test_dict_config(self, _mock_event):
        """get_config reads from plain dict."""
        config = {"trade_size_usd": "500", "rsi_period": 20}
        strategy = _ConcreteStrategy(config)

        assert strategy.get_config("trade_size_usd", "100") == "500"
        assert strategy.get_config("rsi_period", 14) == 20
        assert strategy.get_config("missing_key", "default_val") == "default_val"

    @patch("almanak.framework.strategies.base.add_event")
    def test_object_with_get_method(self, _mock_event):
        """get_config uses .get() for DictConfigWrapper-style objects."""

        class DictLike:
            strategy_id = "test"
            chain = "arbitrum"

            def get(self, key: str, default: Any = None) -> Any:
                data = {"trade_size_usd": "250", "rsi_period": 7}
                return data.get(key, default)

        strategy = _ConcreteStrategy(DictLike())

        assert strategy.get_config("trade_size_usd", "100") == "250"
        assert strategy.get_config("rsi_period", 14) == 7
        assert strategy.get_config("missing_key", "fallback") == "fallback"

    @patch("almanak.framework.strategies.base.add_event")
    def test_attribute_based_config(self, _mock_event):
        """get_config falls back to getattr for plain objects."""

        class AttrConfig:
            strategy_id = "test"
            chain = "arbitrum"
            trade_size_usd = "750"
            rsi_period = 30

        strategy = _ConcreteStrategy(AttrConfig())

        assert strategy.get_config("trade_size_usd", "100") == "750"
        assert strategy.get_config("rsi_period", 14) == 30
        assert strategy.get_config("missing_key", "sentinel") == "sentinel"

    @patch("almanak.framework.strategies.base.add_event")
    def test_none_config_returns_default(self, _mock_event):
        """get_config returns default when config is None."""
        strategy = _ConcreteStrategy({})
        strategy.config = None  # type: ignore[assignment]

        assert strategy.get_config("any_key", "my_default") == "my_default"
        assert strategy.get_config("other_key", 42) == 42

    @patch("almanak.framework.strategies.base.add_event")
    def test_hot_reloadable_config_via_attribute(self, _mock_event):
        """get_config reads HotReloadableConfig fields via getattr."""
        config = HotReloadableConfig(
            max_slippage=Decimal("0.005"),
            trade_size_usd=Decimal("1000"),
        )
        strategy = _ConcreteStrategy(config)

        assert strategy.get_config("max_slippage", Decimal("0.01")) == Decimal("0.005")
        assert strategy.get_config("trade_size_usd", Decimal("100")) == Decimal("1000")
        assert strategy.get_config("nonexistent_field", "default") == "default"
