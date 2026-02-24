"""Tests for _get_tick_spacing in IntentCompiler (VIB-179, VIB-184)."""

import logging

from almanak.framework.intents.compiler import IntentCompiler


class TestGetTickSpacing:
    """Tests for fee-tier -> tick-spacing mapping."""

    def test_fee_100_returns_1(self):
        assert IntentCompiler._get_tick_spacing(100) == 1

    def test_fee_500_returns_10(self):
        assert IntentCompiler._get_tick_spacing(500) == 10

    def test_fee_2500_returns_50(self):
        """VIB-179: PancakeSwap V3 fee tier 2500 requires tick_spacing=50."""
        assert IntentCompiler._get_tick_spacing(2500) == 50

    def test_fee_3000_returns_60(self):
        assert IntentCompiler._get_tick_spacing(3000) == 60

    def test_fee_10000_returns_200(self):
        assert IntentCompiler._get_tick_spacing(10000) == 200

    def test_unknown_fee_defaults_to_60(self):
        assert IntentCompiler._get_tick_spacing(9999) == 60

    def test_unknown_fee_emits_warning(self, caplog):
        """VIB-184: Unknown fee tier should emit a warning."""
        with caplog.at_level(logging.WARNING, logger="almanak.framework.intents.compiler"):
            IntentCompiler._get_tick_spacing(7777)
        assert "Unknown fee tier 7777" in caplog.text
        assert "tick_spacing=60" in caplog.text

    def test_known_fee_no_warning(self, caplog):
        """Known fee tiers should NOT emit a warning."""
        with caplog.at_level(logging.WARNING, logger="almanak.framework.intents.compiler"):
            IntentCompiler._get_tick_spacing(3000)
        assert caplog.text == ""
