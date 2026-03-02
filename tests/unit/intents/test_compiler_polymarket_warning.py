"""Unit tests for Polymarket warning deferral (VIB-307).

VIB-307: The Polymarket warning ("IntentCompiler on Polygon without PolymarketConfig")
was firing at init time, polluting logs for ALL Polygon strategies even if they
have nothing to do with prediction markets.

Fix: warning is now deferred to compile time, so only strategies that actually
attempt prediction intents see the warning.
"""

from decimal import Decimal
from unittest.mock import patch

from almanak import IntentCompiler, IntentCompilerConfig


_BASE_PRICES = {"MATIC": Decimal("0.8"), "USDC": Decimal("1"), "ETH": Decimal("3400"), "WETH": Decimal("3400")}


class TestPolymarketWarningDeferral:
    """VIB-307: Polymarket warning should not fire at compiler init time."""

    def test_no_warning_at_init_on_polygon_without_config(self):
        """Creating an IntentCompiler on Polygon WITHOUT PolymarketConfig should NOT warn."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)

        with patch("almanak.framework.intents.compiler.logger") as mock_logger:
            IntentCompiler(
                chain="polygon",
                price_oracle=_BASE_PRICES,
                config=config,
            )

        # Check that no Polymarket-related warning was emitted at init
        warning_calls = mock_logger.warning.call_args_list
        warning_messages = [str(call) for call in warning_calls]
        assert not any("PolymarketConfig" in msg or "polymarket_config" in msg for msg in warning_messages), (
            f"Unexpected Polymarket warning at init: {warning_messages}"
        )

    def test_no_warning_at_init_on_non_polygon_chain(self):
        """Creating an IntentCompiler on non-Polygon chain should not warn about Polymarket."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)

        with patch("almanak.framework.intents.compiler.logger") as mock_logger:
            IntentCompiler(
                chain="arbitrum",
                price_oracle=_BASE_PRICES,
                config=config,
            )

        warning_calls = mock_logger.warning.call_args_list
        warning_messages = [str(call) for call in warning_calls]
        assert not any("PolymarketConfig" in msg or "polymarket_config" in msg for msg in warning_messages)

    def test_warning_fires_when_prediction_buy_attempted_without_config(self):
        """Attempting PredictionBuyIntent on Polygon without config SHOULD warn."""
        from almanak.framework.intents.vocabulary import PredictionBuyIntent

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle=_BASE_PRICES,
            config=config,
        )

        intent = PredictionBuyIntent(
            market_id="0x1234567890abcdef1234567890abcdef12345678901234567890abcdef12345678",
            outcome="YES",
            amount_usd=Decimal("10"),
        )

        with patch("almanak.framework.intents.compiler.logger") as mock_logger:
            result = compiler._compile_prediction_buy(intent)

        # Should fail compilation (no adapter)
        assert result.status.name == "FAILED"
        # Should warn about missing config
        warning_calls = mock_logger.warning.call_args_list
        warning_messages = [str(call) for call in warning_calls]
        assert any("polymarket_config" in msg for msg in warning_messages), (
            f"Expected warning about polymarket_config, got: {warning_messages}"
        )
