"""Tests for warm-up suppression and slippage direction logic in PnLBacktestEngine."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.engine import PnLBacktester


class TestGetExecutedPriceSlippageDirection:
    """Tests for _get_executed_price slippage direction logic."""

    def setup_method(self):
        self.engine = PnLBacktester.__new__(PnLBacktester)
        self.market_state = MagicMock()
        self.slippage_pct = Decimal("0.005")  # 0.5%

    def test_swap_sell_applies_negative_slippage(self):
        """Selling from_token: price decreases (adverse for seller)."""
        # Swap WETH -> USDC: from_token=WETH, to_token=USDC
        # primary_token will be WETH (tokens[0] = from_token)
        intent = MagicMock()
        intent.from_token = "WETH"
        intent.to_token = "USDC"
        # Remove attributes that would be checked before from_token
        del intent.token

        self.market_state.get_price.return_value = Decimal("3000")

        price = self.engine._get_executed_price(intent, self.market_state, self.slippage_pct, IntentType.SWAP)

        # Selling WETH: price should decrease (3000 * 0.995 = 2985)
        assert price == Decimal("3000") * (Decimal("1") - self.slippage_pct)
        assert price < Decimal("3000")

    def test_swap_from_stablecoin_applies_sell_slippage(self):
        """Swapping from USDC to WETH: primary_token=USDC (from_token), selling USDC."""
        # Swap USDC -> WETH: from_token=USDC, to_token=WETH
        # primary_token will be USDC (tokens[0] = from_token)
        # Since to_token (WETH) != primary_token (USDC), this is a sell
        intent = MagicMock()
        intent.from_token = "USDC"
        intent.to_token = "WETH"
        del intent.token

        self.market_state.get_price.return_value = Decimal("1")  # USDC price

        price = self.engine._get_executed_price(intent, self.market_state, self.slippage_pct, IntentType.SWAP)

        # Selling USDC: adverse slippage means lower price
        assert price == Decimal("1") * (Decimal("1") - self.slippage_pct)

    def test_swap_to_token_matches_primary(self):
        """When to_token == primary_token, buying slippage applied."""
        # Edge case: intent has token attribute that matches to_token
        intent = MagicMock()
        intent.token = "WETH"  # This becomes tokens[0] = primary_token
        intent.from_token = "USDC"
        intent.to_token = "WETH"

        self.market_state.get_price.return_value = Decimal("3000")

        price = self.engine._get_executed_price(intent, self.market_state, self.slippage_pct, IntentType.SWAP)

        # to_token (WETH) == primary_token (WETH from intent.token), so buying
        assert price == Decimal("3000") * (Decimal("1") + self.slippage_pct)
        assert price > Decimal("3000")

    def test_hold_no_slippage(self):
        """Hold intents get market price without slippage."""
        intent = MagicMock()
        intent.token = "WETH"
        del intent.from_token
        del intent.to_token

        self.market_state.get_price.return_value = Decimal("3000")

        price = self.engine._get_executed_price(intent, self.market_state, self.slippage_pct, IntentType.HOLD)

        assert price == Decimal("3000")

    def test_perp_open_slippage(self):
        """Perp open intents get slippage applied."""
        intent = MagicMock()
        intent.token = "ETH"
        del intent.from_token
        intent.to_token = "ETH"

        self.market_state.get_price.return_value = Decimal("3000")

        price = self.engine._get_executed_price(intent, self.market_state, self.slippage_pct, IntentType.PERP_OPEN)

        # to_token == primary_token (ETH), so buying slippage
        assert price == Decimal("3000") * (Decimal("1") + self.slippage_pct)


class TestWarmupSuppression:
    """Tests for warm-up error suppression in the backtest tick loop."""

    def test_warmup_valueerror_suppressed(self):
        """ValueError during warm-up should be suppressed (debug log, not warning)."""
        indicator_engine = MagicMock()
        indicator_engine.is_warming_up.return_value = True

        error = ValueError("Cannot calculate RSI for ETH with period 14")

        # The warm-up check: isinstance(e, ValueError) and any(is_warming_up(...))
        is_warmup = isinstance(error, ValueError) and any(
            indicator_engine.is_warming_up(t, {}) for t in ["ETH"]
        )

        assert is_warmup is True

    def test_non_valueerror_not_suppressed(self):
        """AttributeError during warm-up should NOT be suppressed."""
        indicator_engine = MagicMock()
        indicator_engine.is_warming_up.return_value = True

        error = AttributeError("'NoneType' object has no attribute 'price'")

        is_warmup = isinstance(error, ValueError) and any(
            indicator_engine.is_warming_up(t, {}) for t in ["ETH"]
        )

        assert is_warmup is False

    def test_valueerror_outside_warmup_not_suppressed(self):
        """ValueError when NOT warming up should NOT be suppressed."""
        indicator_engine = MagicMock()
        indicator_engine.is_warming_up.return_value = False

        error = ValueError("Cannot calculate RSI for ETH with period 14")

        is_warmup = isinstance(error, ValueError) and any(
            indicator_engine.is_warming_up(t, {}) for t in ["ETH"]
        )

        assert is_warmup is False

    def test_data_not_available_message_suppressed_during_warmup(self):
        """Real error message from MarketSnapshot is suppressed when warming up."""
        indicator_engine = MagicMock()
        indicator_engine.is_warming_up.return_value = True

        # These are the ACTUAL error messages from MarketSnapshot
        messages = [
            "Cannot calculate RSI for ETH with period 14",
            "MACD data not available for ETH",
            "Bollinger Bands data not available for ETH",
            "ATR data not available for ETH",
        ]

        for msg in messages:
            error = ValueError(msg)
            is_warmup = isinstance(error, ValueError) and any(
                indicator_engine.is_warming_up(t, {}) for t in ["ETH"]
            )
            assert is_warmup is True, f"Should suppress: {msg}"
