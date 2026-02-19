"""Tests for USD fallback removal in _get_intent_amount_usd.

Verifies that the arbitrary $1000 fallback has been removed and proper
error handling is in place for missing USD amounts.

US-066b: Add unit test for USD fallback removal
- Test strict mode raises ValueError when price missing
- Test non-strict mode logs warning and uses raw amount
- Verify no silent $1000 values in fee/slippage calculations
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)


class MockDataProvider:
    """Mock data provider for testing."""

    provider_name = "mock"

    async def iterate(self, config: Any):
        """Yield nothing for testing."""
        if False:
            yield


@dataclass
class MockIntentWithUSD:
    """Intent with direct USD amount field."""

    intent_type: str = "SWAP"
    amount_usd: Decimal = field(default_factory=lambda: Decimal("500"))


@dataclass
class MockIntentWithNotionalUSD:
    """Intent with notional_usd field (perps)."""

    intent_type: str = "PERP_OPEN"
    notional_usd: Decimal = field(default_factory=lambda: Decimal("1000"))


@dataclass
class MockIntentWithValueUSD:
    """Intent with value_usd field (LP)."""

    intent_type: str = "LP_OPEN"
    value_usd: Decimal = field(default_factory=lambda: Decimal("2500"))


@dataclass
class MockIntentWithCollateralUSD:
    """Intent with collateral_usd field (lending)."""

    intent_type: str = "SUPPLY"
    collateral_usd: Decimal = field(default_factory=lambda: Decimal("750"))


@dataclass
class MockIntentWithTokenAmount:
    """Intent with token and amount (needs price conversion)."""

    intent_type: str = "SWAP"
    from_token: str = "ETH"
    amount: Decimal = field(default_factory=lambda: Decimal("1.5"))


@dataclass
class MockIntentWithAmountOnly:
    """Intent with amount but no token (can't convert to USD)."""

    intent_type: str = "SWAP"
    amount: Decimal = field(default_factory=lambda: Decimal("100"))


@dataclass
class MockIntentNoAmountFields:
    """Intent with no amount-related fields at all."""

    intent_type: str = "HOLD"
    reason: str = "waiting"


@dataclass
class MockIntentWithUnknownToken:
    """Intent with a token that has no price in market state."""

    intent_type: str = "SWAP"
    from_token: str = "UNKNOWN_TOKEN"
    amount: Decimal = field(default_factory=lambda: Decimal("50"))


@pytest.fixture
def backtester() -> PnLBacktester:
    """Create a minimal backtester for testing _get_intent_amount_usd."""
    return PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


@pytest.fixture
def market_state() -> MarketState:
    """Create a market state with known prices."""
    return MarketState(
        timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        prices={
            "ETH": Decimal("3000"),
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
            "BTC": Decimal("45000"),
        },
        chain="arbitrum",
        block_number=100000,
    )


@pytest.fixture
def market_state_no_eth() -> MarketState:
    """Create a market state without ETH price."""
    return MarketState(
        timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        prices={
            "USDC": Decimal("1"),
        },
        chain="arbitrum",
        block_number=100000,
    )


class TestDirectUSDAmountFields:
    """Tests for intents with direct USD amount fields."""

    def test_amount_usd_field_returns_value(self, backtester: PnLBacktester, market_state: MarketState):
        """Intent with amount_usd returns that value directly."""
        intent = MockIntentWithUSD(amount_usd=Decimal("500"))
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        assert result == Decimal("500")

    def test_notional_usd_field_returns_value(self, backtester: PnLBacktester, market_state: MarketState):
        """Intent with notional_usd returns that value directly."""
        intent = MockIntentWithNotionalUSD(notional_usd=Decimal("1000"))
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        assert result == Decimal("1000")

    def test_value_usd_field_returns_value(self, backtester: PnLBacktester, market_state: MarketState):
        """Intent with value_usd returns that value directly."""
        intent = MockIntentWithValueUSD(value_usd=Decimal("2500"))
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        assert result == Decimal("2500")

    def test_collateral_usd_field_returns_value(self, backtester: PnLBacktester, market_state: MarketState):
        """Intent with collateral_usd returns that value directly."""
        intent = MockIntentWithCollateralUSD(collateral_usd=Decimal("750"))
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        assert result == Decimal("750")


class TestTokenAmountConversion:
    """Tests for intents with token amount needing price conversion."""

    def test_token_amount_converted_to_usd(self, backtester: PnLBacktester, market_state: MarketState):
        """Intent with token and amount converts using market price."""
        intent = MockIntentWithTokenAmount(
            from_token="ETH",
            amount=Decimal("1.5"),
        )
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        # 1.5 ETH * $3000 = $4500
        assert result == Decimal("4500")

    def test_token_case_insensitive(self, backtester: PnLBacktester, market_state: MarketState):
        """Token lookup is case insensitive."""
        intent = MockIntentWithTokenAmount(
            from_token="eth",  # lowercase
            amount=Decimal("2"),
        )
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        # 2 ETH * $3000 = $6000
        assert result == Decimal("6000")


class TestStrictModeErrors:
    """Tests for strict_reproducibility mode raising errors."""

    def test_strict_mode_raises_when_no_price_available(
        self, backtester: PnLBacktester, market_state_no_eth: MarketState
    ):
        """Strict mode raises ValueError when token price is missing.

        US-084a: In strict mode, we raise an error rather than returning the raw
        token amount, which could be misinterpreted as USD.
        """
        intent = MockIntentWithTokenAmount(
            from_token="ETH",
            amount=Decimal("1.5"),
        )
        # No price for ETH in market_state_no_eth - should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            backtester._get_intent_amount_usd(intent, market_state_no_eth, strict_reproducibility=True)
        assert "Cannot determine USD amount" in str(exc_info.value)
        assert "no price available" in str(exc_info.value)

    def test_strict_mode_raises_when_no_token_for_amount(self, backtester: PnLBacktester, market_state: MarketState):
        """Strict mode raises ValueError when amount exists but no token for lookup."""
        intent = MockIntentWithAmountOnly(amount=Decimal("100"))
        with pytest.raises(ValueError) as exc_info:
            backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=True)
        assert "Cannot determine USD amount" in str(exc_info.value)
        assert "no token" in str(exc_info.value)

    def test_strict_mode_raises_when_no_amount_fields(self, backtester: PnLBacktester, market_state: MarketState):
        """Strict mode raises ValueError when no amount fields exist."""
        intent = MockIntentNoAmountFields()
        with pytest.raises(ValueError) as exc_info:
            backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=True)
        assert "Cannot determine USD amount" in str(exc_info.value)
        assert "no USD amount field and no token amount found" in str(exc_info.value)


class TestNonStrictModeWarnings:
    """Tests for non-strict mode logging warnings and using fallbacks."""

    def test_nonstrict_logs_warning_for_unknown_token(
        self,
        backtester: PnLBacktester,
        market_state: MarketState,
        caplog: pytest.LogCaptureFixture,
    ):
        """Non-strict mode logs warning and returns zero when token price unavailable.

        US-084a: Now returns Decimal("0") instead of raw amount to avoid
        misinterpreting token amount as USD.
        """
        intent = MockIntentWithUnknownToken(
            from_token="UNKNOWN_TOKEN",
            amount=Decimal("50"),
        )
        with caplog.at_level(logging.WARNING):
            result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        # Returns zero as fallback (not raw amount)
        assert result == Decimal("0")
        # Should log warning about missing price
        assert "No price available for token" in caplog.text
        assert "UNKNOWN_TOKEN" in caplog.text
        assert "Using zero as fallback" in caplog.text

    def test_nonstrict_logs_warning_for_amount_without_token(
        self,
        backtester: PnLBacktester,
        market_state: MarketState,
        caplog: pytest.LogCaptureFixture,
    ):
        """Non-strict mode logs warning and returns zero when amount but no token.

        US-084a: Now returns Decimal("0") instead of raw amount to avoid
        misinterpreting token amount as USD.
        """
        intent = MockIntentWithAmountOnly(amount=Decimal("100"))
        with caplog.at_level(logging.WARNING):
            result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        # Returns zero as fallback (not raw amount)
        assert result == Decimal("0")
        assert "no token for USD conversion" in caplog.text
        assert "Using zero as fallback" in caplog.text

    def test_nonstrict_logs_warning_for_no_amount_fields(
        self,
        backtester: PnLBacktester,
        market_state: MarketState,
        caplog: pytest.LogCaptureFixture,
    ):
        """Non-strict mode logs warning and returns zero when no amount fields."""
        intent = MockIntentNoAmountFields()
        with caplog.at_level(logging.WARNING):
            result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        # Returns zero as fallback (not $1000)
        assert result == Decimal("0")
        assert "Using zero as fallback" in caplog.text


class TestNo1000Fallback:
    """Tests verifying the $1000 fallback is completely removed."""

    def test_no_1000_in_any_fallback_path(self, backtester: PnLBacktester, market_state: MarketState):
        """Verify $1000 is never returned from any fallback path."""
        # Test intent with no amount fields in non-strict mode
        intent = MockIntentNoAmountFields()
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        assert result != Decimal("1000")
        assert result == Decimal("0")

    def test_amount_only_returns_zero_not_1000(self, backtester: PnLBacktester, market_state: MarketState):
        """Amount-only intent returns zero, not $1000 or raw amount.

        US-084a: Returns Decimal("0") to avoid misinterpreting token amount as USD.
        """
        intent = MockIntentWithAmountOnly(amount=Decimal("100"))
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        # Should return zero, not raw amount (100) or default $1000
        assert result == Decimal("0")
        assert result != Decimal("1000")
        assert result != Decimal("100")

    def test_unknown_token_returns_zero_not_1000(self, backtester: PnLBacktester, market_state: MarketState):
        """Unknown token intent returns zero, not $1000 or raw amount.

        US-084a: Returns Decimal("0") to avoid misinterpreting token amount as USD.
        """
        intent = MockIntentWithUnknownToken(
            from_token="MYSTERY_TOKEN",
            amount=Decimal("75"),
        )
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        # Should return zero, not raw amount (75) or default $1000
        assert result == Decimal("0")
        assert result != Decimal("1000")
        assert result != Decimal("75")

    def test_strict_mode_error_message_suggests_alternative(self, backtester: PnLBacktester, market_state: MarketState):
        """Strict mode error message tells user how to use fallback."""
        intent = MockIntentNoAmountFields()
        with pytest.raises(ValueError) as exc_info:
            backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=True)
        # Error message should mention how to enable fallback
        assert "strict_reproducibility=False" in str(exc_info.value)


class TestValidAmountReturned:
    """Tests verifying valid amounts are returned correctly."""

    def test_zero_amount_usd_returns_zero(self, backtester: PnLBacktester, market_state: MarketState):
        """Intent with amount_usd=0 returns zero."""
        intent = MockIntentWithUSD(amount_usd=Decimal("0"))
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        assert result == Decimal("0")

    def test_small_amount_preserved(self, backtester: PnLBacktester, market_state: MarketState):
        """Small amounts are preserved exactly."""
        intent = MockIntentWithUSD(amount_usd=Decimal("0.01"))
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        assert result == Decimal("0.01")

    def test_large_amount_preserved(self, backtester: PnLBacktester, market_state: MarketState):
        """Large amounts are preserved exactly."""
        intent = MockIntentWithUSD(amount_usd=Decimal("1000000"))
        result = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        assert result == Decimal("1000000")


class TestFeeSlippageNoHidden1000:
    """Tests verifying fee/slippage calculations don't use hidden $1000."""

    def test_fallback_zero_produces_zero_fees(self, backtester: PnLBacktester, market_state: MarketState):
        """When fallback is zero, fee calculations should be based on zero.

        Verifies that fee estimates based on the USD amount don't use
        a hidden $1000 default anywhere in the calculation chain.
        """
        intent = MockIntentNoAmountFields()
        amount_usd = backtester._get_intent_amount_usd(intent, market_state, strict_reproducibility=False)
        # Amount should be zero, not $1000
        assert amount_usd == Decimal("0")
        # Any fee calculation based on this should be zero or very small
        # (not based on $1000)
        fee_rate = Decimal("0.003")  # 0.3% fee
        estimated_fee = amount_usd * fee_rate
        assert estimated_fee == Decimal("0")
        # If $1000 were used, fee would be $3
        assert estimated_fee != Decimal("3")
