"""Tests for AlertConfig defaults and production configuration (VIB-1257)."""

from datetime import time

from almanak.framework.alerting.alert_config import (
    AlertCondition,
    AlertConfig,
)


class TestAlertConfigDefaults:
    def test_default_quiet_hours_disabled(self):
        config = AlertConfig()
        assert config.quiet_hours is None
        assert not config.is_in_quiet_hours(time(3, 0))

    def test_default_escalation_timeout(self):
        config = AlertConfig()
        assert config.escalation_timeout_seconds == 900

    def test_default_enabled(self):
        config = AlertConfig()
        assert config.enabled is True


class TestAlertConfigProduction:
    def test_production_has_quiet_hours(self):
        config = AlertConfig.default_production()
        assert config.quiet_hours is not None
        assert config.quiet_hours.start == time(22, 0)
        assert config.quiet_hours.end == time(6, 0)

    def test_production_quiet_hours_active_at_midnight(self):
        config = AlertConfig.default_production()
        assert config.is_in_quiet_hours(time(0, 0))
        assert config.is_in_quiet_hours(time(3, 0))
        assert config.is_in_quiet_hours(time(23, 0))

    def test_production_quiet_hours_inactive_during_day(self):
        config = AlertConfig.default_production()
        assert not config.is_in_quiet_hours(time(10, 0))
        assert not config.is_in_quiet_hours(time(15, 0))
        assert not config.is_in_quiet_hours(time(21, 0))

    def test_production_overrides(self):
        config = AlertConfig.default_production(
            telegram_chat_id="123456",
            enabled=False,
        )
        assert config.telegram_chat_id == "123456"
        assert config.enabled is False
        # Quiet hours still present from defaults
        assert config.quiet_hours is not None


class TestBalanceMismatchCondition:
    def test_balance_mismatch_condition_exists(self):
        assert AlertCondition.BALANCE_MISMATCH == "BALANCE_MISMATCH"
