"""Branch coverage for StrategyBase.restore_config_from_snapshot.

Covers: version not found (no snapshots, and snapshots without a match),
snapshots whose config_dict yields no hot-reloadable updates (empty dict
config and unknown parameter keys), and the success path that rebuilds
updates from trading_parameters + risk_parameters and applies them via
update_config with an audit trail of ``restore_v{version}``.

Uses the real HotReloadableConfig so snapshots round-trip through
``to_dict()`` exactly as production writes them. Timeline emission is
stubbed out (add_event) so no shared timeline state is touched.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.models.hot_reload_config import HotReloadableConfig
from almanak.framework.strategies.base import StrategyBase


class _Strategy(StrategyBase):
    """Minimal concrete strategy (StrategyBase is abstract)."""

    STRATEGY_NAME = "restore-test"

    def run(self) -> Any:
        return None


@pytest.fixture(autouse=True)
def _no_timeline(monkeypatch):
    """Keep CONFIG_UPDATED events out of the shared timeline."""
    monkeypatch.setattr("almanak.framework.strategies.base.add_event", lambda event: None)


class TestVersionNotFound:
    def test_no_snapshots_at_all(self):
        strategy = _Strategy(HotReloadableConfig())
        strategy.persistent_state["config_snapshots"] = []

        result = strategy.restore_config_from_snapshot(1)

        assert not result.success
        assert result.error == "Config version 1 not found in history"

    def test_snapshots_exist_but_version_missing(self):
        strategy = _Strategy(HotReloadableConfig())  # snapshot v1 from __init__

        result = strategy.restore_config_from_snapshot(99)

        assert not result.success
        assert result.error == "Config version 99 not found in history"


class TestNoRestorableFields:
    def test_dict_config_snapshot_has_no_parameter_sections(self):
        # A plain-dict config snapshots as-is; without trading_parameters /
        # risk_parameters sections there is nothing to restore.
        strategy = _Strategy({"deployment_id": "d-1"})

        result = strategy.restore_config_from_snapshot(1)

        assert not result.success
        assert result.error == "No restorable fields found in config version 1"

    def test_unknown_parameter_keys_are_skipped(self):
        strategy = _Strategy(HotReloadableConfig())
        strategy.persistent_state["config_snapshots"].append(
            {
                "version": 5,
                "timestamp": "2026-01-01T00:00:00+00:00",
                "updated_by": "test",
                "config_dict": {
                    "trading_parameters": {"exotic_knob": "1"},
                    "risk_parameters": {"other_knob": "2"},
                },
            }
        )

        result = strategy.restore_config_from_snapshot(5)

        assert not result.success
        assert result.error == "No restorable fields found in config version 5"


class TestRestoreSuccess:
    def test_restores_trading_and_risk_parameters(self):
        config = HotReloadableConfig()
        strategy = _Strategy(config)  # snapshot v1: defaults
        assert strategy.update_config({"max_slippage": "0.02", "max_leverage": "5"}).success  # v2

        result = strategy.restore_config_from_snapshot(1)

        assert result.success
        # Both parameter sections contribute updates (all six fields restorable).
        assert set(result.updated_fields) == {
            "max_slippage",
            "trade_size_usd",
            "rebalance_threshold",
            "min_health_factor",
            "max_leverage",
            "daily_loss_limit_usd",
        }
        assert config.max_slippage == Decimal("0.005")
        assert config.max_leverage == Decimal("3")
        # The restore itself is snapshotted with an audit trail.
        snapshots = strategy.get_config_history()
        assert snapshots[-1]["updated_by"] == "restore_v1"
        assert strategy.get_current_config_version() == 3

    def test_picks_matching_version_among_many(self):
        config = HotReloadableConfig()
        strategy = _Strategy(config)  # v1: 0.005
        assert strategy.update_config({"max_slippage": "0.02"}).success  # v2
        assert strategy.update_config({"max_slippage": "0.03"}).success  # v3

        result = strategy.restore_config_from_snapshot(2)

        assert result.success
        assert config.max_slippage == Decimal("0.02")
        assert strategy.get_config_history()[-1]["updated_by"] == "restore_v2"
