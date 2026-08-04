"""Unit tests for ForkLifecycle and persistent fork configuration (VIB-2631)."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.paper.config import (
    ForkLifecycle,
    PaperTraderConfig,
    PersistentForkOracleUnavailableError,
)
from almanak.framework.backtesting.paper.engine import PaperTrader


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
        # VIB-2634: reconciler defaults ON (it only runs on persistent forks;
        # rolling-reset mode skips it with a DEBUG log).
        assert config.position_reconciler_enabled is True
        assert config.position_reconciler_tolerance_pct == Decimal("0.01")

    @pytest.mark.parametrize("lifecycle", [ForkLifecycle.PERSISTENT, "persistent"])
    def test_persistent_mode_is_refused_during_config_validation(self, lifecycle):
        with pytest.raises(
            PersistentForkOracleUnavailableError,
            match="explicitly fork-bound gateway oracle reader",
        ):
            self._make_config(fork_lifecycle=lifecycle)

    def test_to_dict_includes_fork_lifecycle(self):
        config = self._make_config()
        d = config.to_dict()
        assert d["fork_lifecycle"] == "rolling_reset"
        assert d["yield_poker_enabled"] is False
        assert d["use_rich_valuation"] is False
        assert "oracle_divergence_threshold" not in d

    def test_from_dict_roundtrip(self):
        config = self._make_config()
        d = config.to_dict()
        # from_dict needs the raw rpc_url, not masked
        d["rpc_url"] = "https://example.com/rpc"
        config2 = PaperTraderConfig.from_dict(d)
        assert config2.fork_lifecycle == ForkLifecycle.ROLLING_RESET
        assert config2.yield_poker_enabled is False
        assert config2.use_rich_valuation is False

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

    def test_legacy_non_resetting_flag_cannot_bypass_persistent_refusal(self):
        with pytest.raises(PersistentForkOracleUnavailableError, match="reset_fork_every_tick=True"):
            self._make_config(reset_fork_every_tick=False)

    @pytest.mark.parametrize(
        ("field_name", "invalid_value"),
        [
            ("fork_lifecycle", ForkLifecycle.PERSISTENT),
            ("reset_fork_every_tick", False),
        ],
    )
    def test_engine_revalidates_mutated_lifecycle_before_boot(self, field_name, invalid_value):
        config = self._make_config()
        setattr(config, field_name, invalid_value)

        with pytest.raises(PersistentForkOracleUnavailableError, match="explicitly fork-bound gateway oracle reader"):
            PaperTrader(
                fork_manager=object(),  # type: ignore[arg-type]
                portfolio_tracker=object(),  # type: ignore[arg-type]
                config=config,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("field_name", "invalid_value"),
        [
            ("fork_lifecycle", ForkLifecycle.PERSISTENT),
            ("reset_fork_every_tick", False),
        ],
    )
    async def test_run_revalidates_mutated_lifecycle_before_fork_start(self, field_name, invalid_value):
        config = self._make_config()
        fork_manager = MagicMock()
        fork_manager.start = AsyncMock()

        with (
            patch("almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"),
            patch("almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"),
            patch("almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"),
        ):
            trader = PaperTrader(
                fork_manager=fork_manager,
                portfolio_tracker=MagicMock(),
                config=config,
            )

        setattr(config, field_name, invalid_value)

        with pytest.raises(PersistentForkOracleUnavailableError, match="explicitly fork-bound gateway oracle reader"):
            await trader.run(MagicMock())

        fork_manager.start.assert_not_awaited()
