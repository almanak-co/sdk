"""Tests for Safe mode preflight validation."""

from __future__ import annotations

import pytest

from almanak.framework.cli.run import _validate_safe_mode_preflight

SAFE_ADDR = "0xSafe1234567890abcdef1234567890abcdef1234"


class TestValidateSafeModePreflight:
    """Tests for _validate_safe_mode_preflight()."""

    def test_success_direct_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "direct")
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_ADDRESS", SAFE_ADDR)
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "safe_direct")

        assert _validate_safe_mode_preflight(SAFE_ADDR) is None

    def test_success_zodiac_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "zodiac")
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_ADDRESS", SAFE_ADDR)
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "safe_zodiac")

        assert _validate_safe_mode_preflight(SAFE_ADDR) is None

    def test_success_case_insensitive_address(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "direct")
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_ADDRESS", SAFE_ADDR.upper())
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "safe_direct")

        assert _validate_safe_mode_preflight(SAFE_ADDR.lower()) is None

    def test_fail_missing_gateway_safe_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ALMANAK_GATEWAY_SAFE_MODE", raising=False)
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_ADDRESS", SAFE_ADDR)
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "safe_direct")

        error = _validate_safe_mode_preflight(SAFE_ADDR)
        assert error is not None
        assert "gateway Safe mode" in error
        assert "ALMANAK_GATEWAY_SAFE_MODE=direct|zodiac" in error

    def test_fail_invalid_gateway_safe_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "invalid_value")
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_ADDRESS", SAFE_ADDR)
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "safe_direct")

        error = _validate_safe_mode_preflight(SAFE_ADDR)
        assert error is not None
        assert "ALMANAK_GATEWAY_SAFE_MODE=direct|zodiac" in error

    def test_fail_missing_gateway_safe_address(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "direct")
        monkeypatch.delenv("ALMANAK_GATEWAY_SAFE_ADDRESS", raising=False)
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "safe_direct")

        error = _validate_safe_mode_preflight(SAFE_ADDR)
        assert error is not None
        assert "ALMANAK_GATEWAY_SAFE_ADDRESS is missing" in error

    def test_fail_mode_mismatch_framework_zodiac_gateway_direct(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "direct")
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_ADDRESS", SAFE_ADDR)
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "safe_zodiac")

        error = _validate_safe_mode_preflight(SAFE_ADDR)
        assert error is not None
        assert "expects gateway 'zodiac'" in error
        assert "gateway is 'direct'" in error

    def test_fail_mode_mismatch_framework_direct_gateway_zodiac(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "zodiac")
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_ADDRESS", SAFE_ADDR)
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "safe_direct")

        error = _validate_safe_mode_preflight(SAFE_ADDR)
        assert error is not None
        assert "expects gateway 'direct'" in error
        assert "gateway is 'zodiac'" in error

    def test_fail_address_mismatch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "direct")
        monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_ADDRESS", "0xDifferentAddress")
        monkeypatch.setenv("ALMANAK_EXECUTION_MODE", "safe_direct")

        error = _validate_safe_mode_preflight(SAFE_ADDR)
        assert error is not None
        assert "address mismatch" in error.lower()
        assert SAFE_ADDR in error
        assert "0xDifferentAddress" in error
