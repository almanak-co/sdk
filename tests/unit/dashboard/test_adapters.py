"""Tests for PM integration adapters (VIB-2406)."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.dashboard.adapters import strategy_from_pm_dict
from almanak.framework.dashboard.models import StrategyStatus


class TestStrategyFromPmDict:
    def test_basic_conversion(self):
        entry = {
            "strategy_id": "s1",
            "name": "My Strategy",
            "status": "RUNNING",
            "chain": "arbitrum",
            "protocol": "uniswap_v3",
            "total_value_usd": "1000",
            "pnl_24h_usd": "50",
        }
        strategy = strategy_from_pm_dict(entry)
        assert strategy.id == "s1"
        assert strategy.name == "My Strategy"
        assert strategy.status == StrategyStatus.RUNNING
        assert strategy.chain == "arbitrum"
        assert strategy.total_value_usd == Decimal("1000")
        assert strategy.pnl_24h_usd == Decimal("50")

    def test_unknown_status_defaults_to_inactive(self):
        entry = {"strategy_id": "s1", "status": "WEIRD"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.status == StrategyStatus.INACTIVE

    def test_missing_status_defaults_to_inactive(self):
        entry = {"strategy_id": "s1"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.status == StrategyStatus.INACTIVE

    def test_timestamp_from_iso_string(self):
        entry = {
            "strategy_id": "s1",
            "last_action_at": "2026-04-05T12:00:00+00:00",
        }
        strategy = strategy_from_pm_dict(entry)
        assert strategy.last_action_at is not None
        assert strategy.last_action_at.year == 2026

    def test_timestamp_from_unix(self):
        entry = {
            "strategy_id": "s1",
            "last_action_at": 1775304000,  # ~2026-04-05
        }
        strategy = strategy_from_pm_dict(entry)
        assert strategy.last_action_at is not None

    def test_missing_values_use_defaults(self):
        entry = {}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.id == ""
        assert strategy.name == ""
        assert strategy.total_value_usd == Decimal("0")
        assert strategy.pnl_24h_usd == Decimal("0")

    def test_multi_chain_flag(self):
        entry = {
            "strategy_id": "s1",
            "is_multi_chain": True,
            "chains": ["arbitrum", "base"],
        }
        strategy = strategy_from_pm_dict(entry)
        assert strategy.is_multi_chain is True
        assert strategy.chains == ["arbitrum", "base"]

    def test_id_field_fallback(self):
        entry = {"id": "fallback-id"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.id == "fallback-id"

    def test_name_field_fallback(self):
        entry = {"strategy_id": "s1", "strategy_name": "Fallback Name"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.name == "Fallback Name"

    def test_value_confidence_passed_through(self):
        entry = {"strategy_id": "s1", "value_confidence": "STALE"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.value_confidence == "STALE"
