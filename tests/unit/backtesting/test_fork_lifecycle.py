"""Unit tests for ForkLifecycle and persistent fork configuration (VIB-2631)."""

from decimal import Decimal

import pytest

from almanak.framework.backtesting.paper.config import ForkLifecycle, PaperTraderConfig


class TestForkLifecycleEnum:
    """Test ForkLifecycle enum values."""

    def test_rolling_reset_value(self):
        assert ForkLifecycle.ROLLING_RESET == "rolling_reset"

    def test_persistent_value(self):
        assert ForkLifecycle.PERSISTENT == "persistent"

    def test_from_string(self):
        assert ForkLifecycle("rolling_reset") == ForkLifecycle.ROLLING_RESET
        assert ForkLifecycle("persistent") == ForkLifecycle.PERSISTENT

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError):
            ForkLifecycle("invalid")


class TestPaperTraderConfigForkLifecycle:
    """Test PaperTraderConfig with ForkLifecycle settings."""

    def _make_config(self, **kwargs) -> PaperTraderConfig:
        defaults = {
            "chain": "arbitrum",
            "rpc_url": "https://example.com/rpc",
            "deployment_id": "test_strategy",
        }
        defaults.update(kwargs)
        return PaperTraderConfig(**defaults)

    def test_default_is_rolling_reset(self):
        config = self._make_config()
        assert config.fork_lifecycle == ForkLifecycle.ROLLING_RESET
        assert config.reset_fork_every_tick is True
        assert config.yield_poker_enabled is False
        assert config.use_rich_valuation is False
        assert config.position_reconciler_enabled is False

    def test_persistent_mode(self):
        config = self._make_config(fork_lifecycle=ForkLifecycle.PERSISTENT)
        assert config.fork_lifecycle == ForkLifecycle.PERSISTENT
        # reset_fork_every_tick should be auto-synced to False
        assert config.reset_fork_every_tick is False

    def test_persistent_mode_syncs_reset_flag(self):
        """Even if reset_fork_every_tick=True is explicitly passed, PERSISTENT overrides."""
        config = self._make_config(
            fork_lifecycle=ForkLifecycle.PERSISTENT,
            reset_fork_every_tick=True,
        )
        assert config.reset_fork_every_tick is False

    def test_persistent_with_yield_options(self):
        config = self._make_config(
            fork_lifecycle=ForkLifecycle.PERSISTENT,
            yield_poker_enabled=True,
            use_rich_valuation=True,
            position_reconciler_enabled=True,
            oracle_divergence_threshold=Decimal("0.10"),
        )
        assert config.yield_poker_enabled is True
        assert config.use_rich_valuation is True
        assert config.position_reconciler_enabled is True
        assert config.oracle_divergence_threshold == Decimal("0.10")

    def test_fork_lifecycle_from_string(self):
        """Config should accept string values for fork_lifecycle."""
        config = self._make_config(fork_lifecycle="persistent")
        assert config.fork_lifecycle == ForkLifecycle.PERSISTENT

    def test_to_dict_includes_fork_lifecycle(self):
        config = self._make_config(
            fork_lifecycle=ForkLifecycle.PERSISTENT,
            yield_poker_enabled=True,
        )
        d = config.to_dict()
        assert d["fork_lifecycle"] == "persistent"
        assert d["yield_poker_enabled"] is True
        assert d["use_rich_valuation"] is False
        assert "oracle_divergence_threshold" in d

    def test_from_dict_roundtrip(self):
        config = self._make_config(
            fork_lifecycle=ForkLifecycle.PERSISTENT,
            yield_poker_enabled=True,
            use_rich_valuation=True,
            oracle_divergence_threshold=Decimal("0.03"),
        )
        d = config.to_dict()
        # from_dict needs the raw rpc_url, not masked
        d["rpc_url"] = "https://example.com/rpc"
        config2 = PaperTraderConfig.from_dict(d)
        assert config2.fork_lifecycle == ForkLifecycle.PERSISTENT
        assert config2.yield_poker_enabled is True
        assert config2.use_rich_valuation is True
        assert config2.oracle_divergence_threshold == Decimal("0.03")

    def test_from_dict_defaults(self):
        """from_dict with no fork_lifecycle key should default to ROLLING_RESET."""
        d = {
            "chain": "arbitrum",
            "rpc_url": "https://example.com/rpc",
            "deployment_id": "test",
        }
        config = PaperTraderConfig.from_dict(d)
        assert config.fork_lifecycle == ForkLifecycle.ROLLING_RESET
        assert config.yield_poker_enabled is False

    def test_backward_compat_reset_fork_every_tick(self):
        """Existing code using reset_fork_every_tick=False should still work."""
        config = self._make_config(reset_fork_every_tick=False)
        assert config.reset_fork_every_tick is False
        # Fork lifecycle should still be ROLLING_RESET (backward compat)
        assert config.fork_lifecycle == ForkLifecycle.ROLLING_RESET
