"""Tests for slippage guard module.

Tests cover:
- SlippageGuardConfig validation and serialization
- SlippageWarning creation and serialization
- SlippageCheckResult properties and serialization
- SlippageGuard trade checking with various scenarios
- Slippage capping behavior
- Liquidity ratio warnings
- Logging behavior
- Exception emission
- Utility functions
"""

import logging
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.fee_models.slippage_guard import (
    DEFAULT_CRITICAL_IMPACT_THRESHOLD,
    DEFAULT_HIGH_IMPACT_THRESHOLD,
    DEFAULT_MAX_SLIPPAGE_PCT,
    DEFAULT_SAFE_LIQUIDITY_PCT,
    SlippageCapExceededError,
    SlippageCheckResult,
    SlippageGuard,
    SlippageGuardConfig,
    SlippageWarning,
    cap_slippage,
    check_trade_slippage,
)

# =============================================================================
# SlippageGuardConfig Tests
# =============================================================================


class TestSlippageGuardConfigDefaults:
    """Tests for default configuration values."""

    def test_default_max_slippage(self) -> None:
        """Default max slippage should be 10%."""
        config = SlippageGuardConfig()
        assert config.max_slippage_pct == Decimal("0.10")

    def test_default_safe_liquidity(self) -> None:
        """Default safe liquidity percentage should be 5%."""
        config = SlippageGuardConfig()
        assert config.safe_liquidity_pct == Decimal("0.05")

    def test_default_high_impact_threshold(self) -> None:
        """Default high impact threshold should be 1%."""
        config = SlippageGuardConfig()
        assert config.high_impact_threshold == Decimal("0.01")

    def test_default_critical_threshold(self) -> None:
        """Default critical threshold should be 5%."""
        config = SlippageGuardConfig()
        assert config.critical_impact_threshold == Decimal("0.05")

    def test_default_log_warnings_enabled(self) -> None:
        """Warnings should be logged by default."""
        config = SlippageGuardConfig()
        assert config.log_warnings is True

    def test_default_exceptions_disabled(self) -> None:
        """Exceptions should not be raised by default."""
        config = SlippageGuardConfig()
        assert config.emit_exceptions is False


class TestSlippageGuardConfigValidation:
    """Tests for configuration validation."""

    def test_invalid_max_slippage_zero(self) -> None:
        """Max slippage of 0 should raise ValueError."""
        with pytest.raises(ValueError, match="max_slippage_pct must be between"):
            SlippageGuardConfig(max_slippage_pct=Decimal("0"))

    def test_invalid_max_slippage_negative(self) -> None:
        """Negative max slippage should raise ValueError."""
        with pytest.raises(ValueError, match="max_slippage_pct must be between"):
            SlippageGuardConfig(max_slippage_pct=Decimal("-0.05"))

    def test_invalid_max_slippage_above_one(self) -> None:
        """Max slippage above 1 should raise ValueError."""
        with pytest.raises(ValueError, match="max_slippage_pct must be between"):
            SlippageGuardConfig(max_slippage_pct=Decimal("1.5"))

    def test_invalid_safe_liquidity_zero(self) -> None:
        """Safe liquidity of 0 should raise ValueError."""
        with pytest.raises(ValueError, match="safe_liquidity_pct must be between"):
            SlippageGuardConfig(safe_liquidity_pct=Decimal("0"))

    def test_invalid_high_threshold_negative(self) -> None:
        """Negative high impact threshold should raise ValueError."""
        with pytest.raises(ValueError, match="high_impact_threshold must be non-negative"):
            SlippageGuardConfig(high_impact_threshold=Decimal("-0.01"))

    def test_high_threshold_greater_than_critical(self) -> None:
        """High threshold greater than critical should raise ValueError."""
        with pytest.raises(ValueError, match="high_impact_threshold must be <="):
            SlippageGuardConfig(
                high_impact_threshold=Decimal("0.10"),
                critical_impact_threshold=Decimal("0.05"),
            )

    def test_valid_custom_config(self) -> None:
        """Valid custom configuration should work."""
        config = SlippageGuardConfig(
            max_slippage_pct=Decimal("0.15"),
            safe_liquidity_pct=Decimal("0.03"),
            high_impact_threshold=Decimal("0.02"),
            critical_impact_threshold=Decimal("0.08"),
            log_warnings=False,
            emit_exceptions=True,
        )
        assert config.max_slippage_pct == Decimal("0.15")
        assert config.safe_liquidity_pct == Decimal("0.03")


class TestSlippageGuardConfigSerialization:
    """Tests for config serialization."""

    def test_to_dict(self) -> None:
        """Config should serialize to dict correctly."""
        config = SlippageGuardConfig(
            max_slippage_pct=Decimal("0.12"),
            log_warnings=False,
        )
        data = config.to_dict()
        assert data["max_slippage_pct"] == "0.12"
        assert data["log_warnings"] is False

    def test_from_dict(self) -> None:
        """Config should deserialize from dict correctly."""
        data = {
            "max_slippage_pct": "0.15",
            "safe_liquidity_pct": "0.08",
            "emit_exceptions": True,
        }
        config = SlippageGuardConfig.from_dict(data)
        assert config.max_slippage_pct == Decimal("0.15")
        assert config.safe_liquidity_pct == Decimal("0.08")
        assert config.emit_exceptions is True

    def test_roundtrip_serialization(self) -> None:
        """Config should survive serialization roundtrip."""
        original = SlippageGuardConfig(
            max_slippage_pct=Decimal("0.20"),
            safe_liquidity_pct=Decimal("0.10"),
            high_impact_threshold=Decimal("0.03"),
            critical_impact_threshold=Decimal("0.15"),
            log_warnings=False,
            emit_exceptions=True,
        )
        restored = SlippageGuardConfig.from_dict(original.to_dict())
        assert restored.max_slippage_pct == original.max_slippage_pct
        assert restored.safe_liquidity_pct == original.safe_liquidity_pct
        assert restored.emit_exceptions == original.emit_exceptions


# =============================================================================
# SlippageWarning Tests
# =============================================================================


class TestSlippageWarning:
    """Tests for SlippageWarning dataclass."""

    def test_warning_creation(self) -> None:
        """Warning should be created with all fields."""
        warning = SlippageWarning(
            level="high",
            message="Test warning message",
            trade_amount_usd=Decimal("50000"),
            pool_liquidity_usd=Decimal("1000000"),
            liquidity_ratio=Decimal("0.05"),
            estimated_slippage=Decimal("0.08"),
            capped_slippage=Decimal("0.08"),
            was_capped=False,
        )
        assert warning.level == "high"
        assert warning.trade_amount_usd == Decimal("50000")

    def test_warning_to_dict(self) -> None:
        """Warning should serialize correctly."""
        warning = SlippageWarning(
            level="critical",
            message="Critical warning",
            trade_amount_usd=Decimal("100000"),
            pool_liquidity_usd=Decimal("500000"),
            liquidity_ratio=Decimal("0.20"),
            estimated_slippage=Decimal("0.15"),
            capped_slippage=Decimal("0.10"),
            was_capped=True,
            details={"protocol": "uniswap_v3"},
        )
        data = warning.to_dict()
        assert data["level"] == "critical"
        assert data["was_capped"] is True
        assert data["estimated_slippage_pct"] == "15.00%"
        assert data["capped_slippage_pct"] == "10.00%"
        assert data["details"]["protocol"] == "uniswap_v3"


# =============================================================================
# SlippageCheckResult Tests
# =============================================================================


class TestSlippageCheckResult:
    """Tests for SlippageCheckResult dataclass."""

    def test_result_without_warning(self) -> None:
        """Result without warning should have correct properties."""
        result = SlippageCheckResult(
            original_slippage=Decimal("0.005"),
            capped_slippage=Decimal("0.005"),
            was_capped=False,
            warning=None,
            trade_amount_usd=Decimal("1000"),
        )
        assert result.has_warning is False
        assert result.is_critical is False
        assert result.was_capped is False

    def test_result_with_high_warning(self) -> None:
        """Result with high warning should have correct properties."""
        warning = SlippageWarning(
            level="high",
            message="High warning",
            trade_amount_usd=Decimal("50000"),
            pool_liquidity_usd=None,
            liquidity_ratio=None,
            estimated_slippage=Decimal("0.03"),
            capped_slippage=Decimal("0.03"),
            was_capped=False,
        )
        result = SlippageCheckResult(
            original_slippage=Decimal("0.03"),
            capped_slippage=Decimal("0.03"),
            was_capped=False,
            warning=warning,
            trade_amount_usd=Decimal("50000"),
        )
        assert result.has_warning is True
        assert result.is_critical is False

    def test_result_with_critical_warning(self) -> None:
        """Result with critical warning should have correct properties."""
        warning = SlippageWarning(
            level="critical",
            message="Critical warning",
            trade_amount_usd=Decimal("100000"),
            pool_liquidity_usd=None,
            liquidity_ratio=None,
            estimated_slippage=Decimal("0.12"),
            capped_slippage=Decimal("0.10"),
            was_capped=True,
        )
        result = SlippageCheckResult(
            original_slippage=Decimal("0.12"),
            capped_slippage=Decimal("0.10"),
            was_capped=True,
            warning=warning,
            trade_amount_usd=Decimal("100000"),
        )
        assert result.has_warning is True
        assert result.is_critical is True
        assert result.was_capped is True

    def test_result_to_dict(self) -> None:
        """Result should serialize correctly."""
        result = SlippageCheckResult(
            original_slippage=Decimal("0.08"),
            capped_slippage=Decimal("0.08"),
            was_capped=False,
            warning=None,
            trade_amount_usd=Decimal("25000"),
            pool_liquidity_usd=Decimal("500000"),
            liquidity_ratio=Decimal("0.05"),
        )
        data = result.to_dict()
        assert data["original_slippage_pct"] == "8.00%"
        assert data["has_warning"] is False
        assert data["liquidity_ratio"] == "0.05"


# =============================================================================
# SlippageGuard Basic Tests
# =============================================================================


class TestSlippageGuardBasic:
    """Basic tests for SlippageGuard."""

    def test_guard_default_config(self) -> None:
        """Guard should use default config when not specified."""
        guard = SlippageGuard()
        assert guard.config.max_slippage_pct == DEFAULT_MAX_SLIPPAGE_PCT

    def test_guard_custom_config(self) -> None:
        """Guard should use custom config when specified."""
        config = SlippageGuardConfig(max_slippage_pct=Decimal("0.15"))
        guard = SlippageGuard(config=config)
        assert guard.config.max_slippage_pct == Decimal("0.15")


class TestSlippageGuardCapping:
    """Tests for slippage capping behavior."""

    def test_slippage_below_cap_not_modified(self) -> None:
        """Slippage below cap should not be modified."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            estimated_slippage=Decimal("0.05"),  # 5% < 10% cap
        )
        assert result.capped_slippage == Decimal("0.05")
        assert result.was_capped is False

    def test_slippage_at_cap_not_modified(self) -> None:
        """Slippage exactly at cap should not be modified."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            estimated_slippage=Decimal("0.10"),  # 10% = cap
        )
        assert result.capped_slippage == Decimal("0.10")
        assert result.was_capped is False

    def test_slippage_above_cap_is_capped(self) -> None:
        """Slippage above cap should be capped."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            estimated_slippage=Decimal("0.15"),  # 15% > 10% cap
        )
        assert result.capped_slippage == Decimal("0.10")
        assert result.was_capped is True
        assert result.original_slippage == Decimal("0.15")

    def test_custom_cap(self) -> None:
        """Custom cap should be respected."""
        config = SlippageGuardConfig(max_slippage_pct=Decimal("0.05"))
        guard = SlippageGuard(config=config)
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            estimated_slippage=Decimal("0.08"),
        )
        assert result.capped_slippage == Decimal("0.05")
        assert result.was_capped is True

    def test_cap_slippage_method(self) -> None:
        """Cap slippage convenience method should work."""
        guard = SlippageGuard()
        assert guard.cap_slippage(Decimal("0.05")) == Decimal("0.05")
        assert guard.cap_slippage(Decimal("0.15")) == Decimal("0.10")


# =============================================================================
# SlippageGuard Warning Tests
# =============================================================================


class TestSlippageGuardHighImpactWarning:
    """Tests for high impact threshold warnings."""

    def test_no_warning_below_threshold(self) -> None:
        """No warning for slippage below high threshold."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            estimated_slippage=Decimal("0.005"),  # 0.5% < 1% threshold
        )
        assert result.warning is None

    def test_warning_at_high_threshold(self) -> None:
        """Warning for slippage at high threshold."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            estimated_slippage=Decimal("0.01"),  # 1% = threshold
        )
        assert result.warning is not None
        assert result.warning.level == "high"

    def test_warning_above_high_threshold(self) -> None:
        """Warning for slippage above high threshold."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            estimated_slippage=Decimal("0.02"),  # 2% > 1% threshold
        )
        assert result.warning is not None
        assert result.warning.level == "high"


class TestSlippageGuardCriticalWarning:
    """Tests for critical threshold warnings."""

    def test_critical_warning_at_threshold(self) -> None:
        """Critical warning for slippage at critical threshold."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            estimated_slippage=Decimal("0.05"),  # 5% = critical threshold
        )
        assert result.warning is not None
        assert result.warning.level == "critical"

    def test_critical_warning_above_threshold(self) -> None:
        """Critical warning for slippage above critical threshold."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            estimated_slippage=Decimal("0.08"),  # 8% > 5% critical threshold
        )
        assert result.warning is not None
        assert result.warning.level == "critical"


class TestSlippageGuardLiquidityWarning:
    """Tests for liquidity ratio warnings."""

    def test_no_warning_small_trade(self) -> None:
        """No warning for trade within safe liquidity limit."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("40000"),
            pool_liquidity_usd=Decimal("1000000"),  # 4% < 5% threshold
            estimated_slippage=Decimal("0.005"),
        )
        assert result.warning is None
        assert result.liquidity_ratio == Decimal("0.04")

    def test_warning_at_safe_limit(self) -> None:
        """Warning for trade at safe liquidity limit."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("50000"),
            pool_liquidity_usd=Decimal("1000000"),  # 5% = threshold
            estimated_slippage=Decimal("0.005"),
        )
        assert result.warning is not None
        assert result.warning.level == "high"

    def test_warning_above_safe_limit(self) -> None:
        """Warning for trade above safe liquidity limit."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("80000"),
            pool_liquidity_usd=Decimal("1000000"),  # 8% > 5% threshold
            estimated_slippage=Decimal("0.005"),
        )
        assert result.warning is not None
        assert result.warning.level == "high"
        assert "8.0% of pool liquidity" in result.warning.message

    def test_critical_warning_very_large_trade(self) -> None:
        """Critical warning for very large trade (>2x safe limit)."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("120000"),
            pool_liquidity_usd=Decimal("1000000"),  # 12% > 10% (2x safe limit)
            estimated_slippage=Decimal("0.005"),
        )
        assert result.warning is not None
        assert result.warning.level == "critical"


# =============================================================================
# SlippageGuard Logging Tests
# =============================================================================


class TestSlippageGuardLogging:
    """Tests for warning logging behavior."""

    def test_warning_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        """High warning should be logged."""
        guard = SlippageGuard()
        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.pnl.fee_models.slippage_guard"):
            guard.check_trade(
                trade_amount_usd=Decimal("50000"),
                estimated_slippage=Decimal("0.03"),
                token_in="ETH",
                token_out="USDC",
                protocol="uniswap_v3",
            )
        assert "slippage warning" in caplog.text.lower()

    def test_critical_logged_as_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """Critical warning should be logged as error."""
        guard = SlippageGuard()
        with caplog.at_level(logging.ERROR, logger="almanak.framework.backtesting.pnl.fee_models.slippage_guard"):
            guard.check_trade(
                trade_amount_usd=Decimal("50000"),
                estimated_slippage=Decimal("0.08"),
            )
        assert len([r for r in caplog.records if r.levelno == logging.ERROR]) > 0

    def test_logging_disabled(self, caplog: pytest.LogCaptureFixture) -> None:
        """No logging when disabled."""
        config = SlippageGuardConfig(log_warnings=False)
        guard = SlippageGuard(config=config)
        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.pnl.fee_models.slippage_guard"):
            guard.check_trade(
                trade_amount_usd=Decimal("50000"),
                estimated_slippage=Decimal("0.08"),
            )
        assert "slippage" not in caplog.text.lower()


# =============================================================================
# SlippageGuard Exception Tests
# =============================================================================


class TestSlippageGuardExceptions:
    """Tests for exception emission."""

    def test_no_exception_by_default(self) -> None:
        """No exception should be raised by default."""
        guard = SlippageGuard()
        # This should not raise despite critical slippage
        result = guard.check_trade(
            trade_amount_usd=Decimal("50000"),
            estimated_slippage=Decimal("0.15"),
        )
        assert result.is_critical

    def test_exception_when_enabled(self) -> None:
        """Exception should be raised when enabled for critical slippage."""
        config = SlippageGuardConfig(emit_exceptions=True)
        guard = SlippageGuard(config=config)
        with pytest.raises(SlippageCapExceededError) as exc_info:
            guard.check_trade(
                trade_amount_usd=Decimal("50000"),
                estimated_slippage=Decimal("0.15"),
            )
        assert exc_info.value.estimated_slippage == Decimal("0.15")
        assert exc_info.value.max_slippage == Decimal("0.10")

    def test_no_exception_for_high_warning(self) -> None:
        """No exception for high (non-critical) warning even when enabled."""
        config = SlippageGuardConfig(emit_exceptions=True)
        guard = SlippageGuard(config=config)
        # 2% is above high (1%) but below critical (5%)
        result = guard.check_trade(
            trade_amount_usd=Decimal("50000"),
            estimated_slippage=Decimal("0.02"),
        )
        assert result.warning is not None
        assert result.warning.level == "high"


# =============================================================================
# SlippageGuard Helper Methods Tests
# =============================================================================


class TestSlippageGuardHelpers:
    """Tests for helper methods."""

    def test_is_safe_trade_size_true(self) -> None:
        """Trade within safe limit should return True."""
        guard = SlippageGuard()
        assert guard.is_safe_trade_size(
            trade_amount_usd=Decimal("40000"),
            pool_liquidity_usd=Decimal("1000000"),
        )

    def test_is_safe_trade_size_false(self) -> None:
        """Trade above safe limit should return False."""
        guard = SlippageGuard()
        assert not guard.is_safe_trade_size(
            trade_amount_usd=Decimal("60000"),
            pool_liquidity_usd=Decimal("1000000"),
        )

    def test_is_safe_trade_size_zero_liquidity(self) -> None:
        """Zero liquidity should return False."""
        guard = SlippageGuard()
        assert not guard.is_safe_trade_size(
            trade_amount_usd=Decimal("1000"),
            pool_liquidity_usd=Decimal("0"),
        )

    def test_get_max_safe_trade_size(self) -> None:
        """Max safe trade size calculation."""
        guard = SlippageGuard()
        max_safe = guard.get_max_safe_trade_size(Decimal("1000000"))
        assert max_safe == Decimal("50000")  # 5% of $1M

    def test_get_max_safe_trade_size_custom(self) -> None:
        """Max safe trade size with custom config."""
        config = SlippageGuardConfig(safe_liquidity_pct=Decimal("0.10"))
        guard = SlippageGuard(config=config)
        max_safe = guard.get_max_safe_trade_size(Decimal("1000000"))
        assert max_safe == Decimal("100000")  # 10% of $1M


# =============================================================================
# SlippageGuard Serialization Tests
# =============================================================================


class TestSlippageGuardSerialization:
    """Tests for SlippageGuard serialization."""

    def test_to_dict(self) -> None:
        """Guard should serialize to dict."""
        config = SlippageGuardConfig(max_slippage_pct=Decimal("0.15"))
        guard = SlippageGuard(config=config)
        data = guard.to_dict()
        assert data["config"]["max_slippage_pct"] == "0.15"

    def test_from_dict(self) -> None:
        """Guard should deserialize from dict."""
        data = {
            "config": {
                "max_slippage_pct": "0.20",
                "emit_exceptions": True,
            }
        }
        guard = SlippageGuard.from_dict(data)
        assert guard.config.max_slippage_pct == Decimal("0.20")
        assert guard.config.emit_exceptions is True

    def test_roundtrip_serialization(self) -> None:
        """Guard should survive serialization roundtrip."""
        original_config = SlippageGuardConfig(
            max_slippage_pct=Decimal("0.12"),
            safe_liquidity_pct=Decimal("0.08"),
        )
        original = SlippageGuard(config=original_config)
        restored = SlippageGuard.from_dict(original.to_dict())
        assert restored.config.max_slippage_pct == original.config.max_slippage_pct
        assert restored.config.safe_liquidity_pct == original.config.safe_liquidity_pct


# =============================================================================
# Utility Function Tests
# =============================================================================


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_check_trade_slippage_basic(self) -> None:
        """check_trade_slippage should work with defaults."""
        result = check_trade_slippage(
            trade_amount_usd=Decimal("50000"),
            estimated_slippage=Decimal("0.05"),
        )
        assert result.capped_slippage == Decimal("0.05")

    def test_check_trade_slippage_custom_cap(self) -> None:
        """check_trade_slippage should respect custom max."""
        result = check_trade_slippage(
            trade_amount_usd=Decimal("50000"),
            estimated_slippage=Decimal("0.08"),
            max_slippage_pct=Decimal("0.05"),
        )
        assert result.capped_slippage == Decimal("0.05")
        assert result.was_capped is True

    def test_check_trade_slippage_with_liquidity(self) -> None:
        """check_trade_slippage should track liquidity ratio."""
        result = check_trade_slippage(
            trade_amount_usd=Decimal("100000"),
            estimated_slippage=Decimal("0.03"),
            pool_liquidity_usd=Decimal("500000"),
        )
        assert result.liquidity_ratio == Decimal("0.2")  # 100k / 500k = 20%
        assert result.warning is not None

    def test_cap_slippage_below_max(self) -> None:
        """cap_slippage should not modify slippage below max."""
        result = cap_slippage(Decimal("0.05"))
        assert result == Decimal("0.05")

    def test_cap_slippage_above_max(self) -> None:
        """cap_slippage should cap slippage above max."""
        result = cap_slippage(Decimal("0.15"))
        assert result == Decimal("0.10")

    def test_cap_slippage_custom_max(self) -> None:
        """cap_slippage should respect custom max."""
        result = cap_slippage(Decimal("0.08"), max_slippage_pct=Decimal("0.05"))
        assert result == Decimal("0.05")

    def test_cap_slippage_negative(self) -> None:
        """cap_slippage should handle negative values."""
        result = cap_slippage(Decimal("-0.15"))
        assert result == Decimal("0.10")  # abs(-0.15) = 0.15, capped to 0.10


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_zero_trade_amount(self) -> None:
        """Zero trade amount should work without error."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("0"),
            estimated_slippage=Decimal("0.005"),  # Low slippage to avoid warning
        )
        assert result.warning is None

    def test_zero_slippage(self) -> None:
        """Zero slippage should work without error."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("10000"),
            estimated_slippage=Decimal("0"),
        )
        assert result.capped_slippage == Decimal("0")
        assert result.warning is None

    def test_negative_slippage(self) -> None:
        """Negative slippage should be treated as absolute value."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("10000"),
            estimated_slippage=Decimal("-0.05"),
        )
        assert result.original_slippage == Decimal("0.05")

    def test_very_small_liquidity(self) -> None:
        """Very small liquidity should give high ratio warning."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("1000"),
            pool_liquidity_usd=Decimal("100"),  # 1000% of liquidity!
            estimated_slippage=Decimal("0.005"),
        )
        assert result.warning is not None
        assert result.warning.level == "critical"

    def test_warning_includes_trade_context(self) -> None:
        """Warning should include trade context when provided."""
        guard = SlippageGuard()
        result = guard.check_trade(
            trade_amount_usd=Decimal("50000"),
            estimated_slippage=Decimal("0.08"),
            token_in="ETH",
            token_out="USDC",
            protocol="uniswap_v3",
            pool_address="0x1234",
        )
        assert result.warning is not None
        assert "ETH" in result.warning.message
        assert "USDC" in result.warning.message
        assert "uniswap_v3" in result.warning.message
        assert result.warning.details["pool_address"] == "0x1234"


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_default_max_slippage_constant(self) -> None:
        """Default max slippage constant should be 10%."""
        assert DEFAULT_MAX_SLIPPAGE_PCT == Decimal("0.10")

    def test_default_safe_liquidity_constant(self) -> None:
        """Default safe liquidity constant should be 5%."""
        assert DEFAULT_SAFE_LIQUIDITY_PCT == Decimal("0.05")

    def test_default_high_impact_constant(self) -> None:
        """Default high impact constant should be 1%."""
        assert DEFAULT_HIGH_IMPACT_THRESHOLD == Decimal("0.01")

    def test_default_critical_constant(self) -> None:
        """Default critical constant should be 5%."""
        assert DEFAULT_CRITICAL_IMPACT_THRESHOLD == Decimal("0.05")
