"""Unit tests for position health monitoring."""

from decimal import Decimal

import pytest

from almanak.framework.data.position_health import (
    DeleverageTrigger,
    PTPositionHealth,
    PositionHealth,
)


# =========================================================================
# PositionHealth Tests
# =========================================================================


class TestPositionHealth:
    """Test PositionHealth dataclass and properties."""

    def test_healthy_position(self):
        health = PositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is False
        assert health.is_critical is False

    def test_warning_position(self):
        health = PositionHealth(
            health_factor=Decimal("1.3"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("7038"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is True
        assert health.is_critical is False

    def test_critical_position(self):
        health = PositionHealth(
            health_factor=Decimal("1.05"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("8714"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is True
        assert health.is_critical is True

    def test_liquidatable_position(self):
        health = PositionHealth(
            health_factor=Decimal("0.95"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("9631"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is False
        assert health.is_critical is True

    def test_zero_debt_infinite_hf(self):
        health = PositionHealth(
            health_factor=Decimal("Infinity"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("0"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is False
        assert health.is_critical is False

    def test_zero_collateral_with_debt(self):
        health = PositionHealth(
            health_factor=Decimal("0"),
            collateral_value_usd=Decimal("0"),
            debt_value_usd=Decimal("1000"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is False
        assert health.is_critical is True

    def test_boundary_hf_exactly_one(self):
        health = PositionHealth(
            health_factor=Decimal("1.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("9150"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is True

    def test_boundary_hf_exactly_1_5(self):
        health = PositionHealth(
            health_factor=Decimal("1.5"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("6100"),
            lltv=Decimal("0.915"),
        )
        assert health.is_healthy is True
        assert health.is_warning is False

    def test_to_dict(self):
        health = PositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            protocol="morpho_blue",
            market_id="0xmarket",
        )
        d = health.to_dict()
        assert d["health_factor"] == "2.0"
        assert d["protocol"] == "morpho_blue"
        assert d["is_healthy"] is True
        assert d["is_warning"] is False


# =========================================================================
# PTPositionHealth Tests
# =========================================================================


class TestPTPositionHealth:
    """Test PTPositionHealth dataclass and maturity risk."""

    def test_extends_position_health(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            implied_apy=Decimal("0.05"),
            pt_discount_pct=Decimal("3.0"),
            days_to_maturity=90,
            pendle_market="0xpendle_market",
        )
        assert pt_health.is_healthy is True
        assert pt_health.implied_apy == Decimal("0.05")
        assert pt_health.days_to_maturity == 90

    def test_maturity_risk_safe(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=60,
        )
        assert pt_health.maturity_risk == "safe"

    def test_maturity_risk_near(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=15,
        )
        assert pt_health.maturity_risk == "near"

    def test_maturity_risk_imminent(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=5,
        )
        assert pt_health.maturity_risk == "imminent"

    def test_maturity_risk_expired(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=0,
        )
        assert pt_health.maturity_risk == "expired"

    def test_maturity_risk_boundary_7_days(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=7,
        )
        assert pt_health.maturity_risk == "imminent"

    def test_maturity_risk_boundary_30_days(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            days_to_maturity=30,
        )
        assert pt_health.maturity_risk == "near"

    def test_to_dict_includes_pendle_fields(self):
        pt_health = PTPositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
            implied_apy=Decimal("0.05"),
            pt_discount_pct=Decimal("3.0"),
            days_to_maturity=90,
            pendle_market="0xpendle_market",
        )
        d = pt_health.to_dict()
        assert d["implied_apy"] == "0.05"
        assert d["pt_discount_pct"] == "3.0"
        assert d["days_to_maturity"] == 90
        assert d["pendle_market"] == "0xpendle_market"
        assert d["maturity_risk"] == "safe"
        # Verify base fields are also present
        assert d["health_factor"] == "2.0"
        assert d["is_healthy"] is True


# =========================================================================
# DeleverageTrigger Tests
# =========================================================================


class TestDeleverageTrigger:
    """Test DeleverageTrigger thresholds."""

    def test_default_thresholds(self):
        trigger = DeleverageTrigger()
        assert trigger.warning_hf == Decimal("1.5")
        assert trigger.critical_hf == Decimal("1.2")
        assert trigger.safe_target_hf == Decimal("2.0")

    def test_should_deleverage_true(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("1.1"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("8318"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_deleverage(health) is True

    def test_should_deleverage_false(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("1.8"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("5083"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_deleverage(health) is False

    def test_should_warn_true(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("1.3"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("7038"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_warn(health) is True

    def test_should_warn_false(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("2.0"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("4575"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_warn(health) is False

    def test_custom_thresholds(self):
        trigger = DeleverageTrigger(
            warning_hf=Decimal("2.0"),
            critical_hf=Decimal("1.5"),
            safe_target_hf=Decimal("3.0"),
        )
        health = PositionHealth(
            health_factor=Decimal("1.6"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("5718"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_warn(health) is True
        assert trigger.should_deleverage(health) is False

    def test_infinite_hf_no_deleverage(self):
        trigger = DeleverageTrigger()
        health = PositionHealth(
            health_factor=Decimal("Infinity"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("0"),
            lltv=Decimal("0.915"),
        )
        assert trigger.should_deleverage(health) is False
        assert trigger.should_warn(health) is False
