"""Tests for StrategyBase plain dict config handling (VIB-120)."""

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
