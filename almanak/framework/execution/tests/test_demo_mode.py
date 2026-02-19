"""Tests for Demo Mode Guard.

This module contains tests for the DemoModeGuard class
and related functionality.
"""

import os

import pytest

from ..demo_mode import (
    DemoModeConfig,
    DemoModeError,
    DemoModeGuard,
    get_demo_mode_guard,
    is_demo_mode,
    validate_not_demo,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def guard() -> DemoModeGuard:
    """Create a fresh demo mode guard with cleared cache."""
    g = DemoModeGuard()
    g.clear_cache()
    DemoModeGuard._logged_demo_warning = False
    return g


@pytest.fixture
def clean_env():
    """Ensure clean environment for tests."""
    env_vars = ["ALMANAK_DEMO_MODE", "ALMANAK_FORCE_PRODUCTION"]
    old_values = {k: os.environ.get(k) for k in env_vars}

    # Clear the environment
    for k in env_vars:
        if k in os.environ:
            del os.environ[k]

    yield

    # Restore old values
    for k, v in old_values.items():
        if v is not None:
            os.environ[k] = v
        elif k in os.environ:
            del os.environ[k]


# =============================================================================
# Basic State Tests
# =============================================================================


class TestDemoModeGuard:
    """Tests for DemoModeGuard."""

    def test_demo_mode_off_by_default(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that demo mode is off by default."""
        assert guard.is_demo_mode() is False

    def test_demo_mode_enabled_with_true(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that demo mode is enabled with ALMANAK_DEMO_MODE=true."""
        os.environ["ALMANAK_DEMO_MODE"] = "true"
        guard.clear_cache()
        assert guard.is_demo_mode() is True

    def test_demo_mode_enabled_with_1(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that demo mode is enabled with ALMANAK_DEMO_MODE=1."""
        os.environ["ALMANAK_DEMO_MODE"] = "1"
        guard.clear_cache()
        assert guard.is_demo_mode() is True

    def test_demo_mode_enabled_with_yes(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that demo mode is enabled with ALMANAK_DEMO_MODE=yes."""
        os.environ["ALMANAK_DEMO_MODE"] = "yes"
        guard.clear_cache()
        assert guard.is_demo_mode() is True

    def test_demo_mode_disabled_with_false(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that demo mode is disabled with ALMANAK_DEMO_MODE=false."""
        os.environ["ALMANAK_DEMO_MODE"] = "false"
        guard.clear_cache()
        assert guard.is_demo_mode() is False

    def test_demo_mode_case_insensitive(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that demo mode check is case insensitive."""
        os.environ["ALMANAK_DEMO_MODE"] = "TRUE"
        guard.clear_cache()
        assert guard.is_demo_mode() is True


# =============================================================================
# Production Mode Tests
# =============================================================================


class TestProductionMode:
    """Tests for production mode detection."""

    def test_production_mode_requires_force_flag(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that production mode requires ALMANAK_FORCE_PRODUCTION."""
        assert guard.is_production_mode() is False

        os.environ["ALMANAK_FORCE_PRODUCTION"] = "true"
        assert guard.is_production_mode() is True

    def test_production_mode_blocked_by_demo_mode(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that demo mode takes precedence over production flag."""
        os.environ["ALMANAK_DEMO_MODE"] = "true"
        os.environ["ALMANAK_FORCE_PRODUCTION"] = "true"
        guard.clear_cache()
        assert guard.is_production_mode() is False


# =============================================================================
# Validation Tests
# =============================================================================


class TestValidation:
    """Tests for validation methods."""

    def test_validate_not_demo_passes_when_disabled(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that validation passes when demo mode is disabled."""
        guard.validate_not_demo()  # Should not raise

    def test_validate_not_demo_raises_when_enabled(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that validation raises when demo mode is enabled."""
        os.environ["ALMANAK_DEMO_MODE"] = "true"
        guard.clear_cache()

        with pytest.raises(DemoModeError) as exc_info:
            guard.validate_not_demo()

        assert "demo mode" in str(exc_info.value).lower()

    def test_validate_not_demo_includes_operation(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that validation error includes operation description."""
        os.environ["ALMANAK_DEMO_MODE"] = "true"
        guard.clear_cache()

        with pytest.raises(DemoModeError) as exc_info:
            guard.validate_not_demo("submit transaction to mainnet")

        assert "submit transaction to mainnet" in str(exc_info.value)

    def test_validate_submission_allowed(self, guard: DemoModeGuard, clean_env) -> None:
        """Test submission validation."""
        guard.validate_submission_allowed()  # Should not raise

        os.environ["ALMANAK_DEMO_MODE"] = "true"
        guard.clear_cache()

        with pytest.raises(DemoModeError):
            guard.validate_submission_allowed()

    def test_signing_allowed_by_default_in_demo(self, guard: DemoModeGuard, clean_env) -> None:
        """Test that signing is allowed by default in demo mode."""
        os.environ["ALMANAK_DEMO_MODE"] = "true"
        guard.clear_cache()

        # Default config allows signing
        guard.validate_signing_allowed()  # Should not raise

    def test_signing_blocked_with_config(self, clean_env) -> None:
        """Test that signing can be blocked with config."""
        os.environ["ALMANAK_DEMO_MODE"] = "true"

        config = DemoModeConfig(block_signing=True)
        guard = DemoModeGuard(config=config)

        with pytest.raises(DemoModeError):
            guard.validate_signing_allowed()


# =============================================================================
# Status Tests
# =============================================================================


class TestStatus:
    """Tests for status reporting."""

    def test_get_status_normal(self, guard: DemoModeGuard, clean_env) -> None:
        """Test status in normal mode."""
        status = guard.get_status()
        assert status["demo_mode"] is False
        assert status["submissions_blocked"] is False

    def test_get_status_demo(self, guard: DemoModeGuard, clean_env) -> None:
        """Test status in demo mode."""
        os.environ["ALMANAK_DEMO_MODE"] = "true"
        guard.clear_cache()

        status = guard.get_status()
        assert status["demo_mode"] is True
        assert status["submissions_blocked"] is True
        assert status["simulations_allowed"] is True


# =============================================================================
# Convenience Function Tests
# =============================================================================


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_get_demo_mode_guard_singleton(self) -> None:
        """Test that get_demo_mode_guard returns same instance."""
        guard1 = get_demo_mode_guard()
        guard2 = get_demo_mode_guard()
        assert guard1 is guard2

    def test_is_demo_mode_function(self, clean_env) -> None:
        """Test the is_demo_mode convenience function."""
        # Clear the global guard cache
        guard = get_demo_mode_guard()
        guard.clear_cache()

        assert is_demo_mode() is False

        os.environ["ALMANAK_DEMO_MODE"] = "true"
        guard.clear_cache()
        assert is_demo_mode() is True

    def test_validate_not_demo_function(self, clean_env) -> None:
        """Test the validate_not_demo convenience function."""
        guard = get_demo_mode_guard()
        guard.clear_cache()

        validate_not_demo()  # Should not raise

        os.environ["ALMANAK_DEMO_MODE"] = "true"
        guard.clear_cache()

        with pytest.raises(DemoModeError):
            validate_not_demo()


# =============================================================================
# Error Tests
# =============================================================================


class TestDemoModeError:
    """Tests for DemoModeError exception."""

    def test_error_message(self) -> None:
        """Test error message formatting."""
        error = DemoModeError("submit")
        assert "submit" in str(error)
        assert "ALMANAK_DEMO_MODE" in str(error)

    def test_custom_message(self) -> None:
        """Test custom error message."""
        error = DemoModeError("test", "Custom message here")
        assert str(error) == "Custom message here"
        assert error.operation == "test"
