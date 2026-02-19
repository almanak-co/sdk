"""Tests for Prediction Market Intent types.

Tests verify that PredictionBuyIntent, PredictionSellIntent, and PredictionRedeemIntent
correctly validate parameters and support serialization/deserialization.

To run:
    uv run pytest tests/intents/test_prediction_intents.py -v
"""

from decimal import Decimal

import pytest

from almanak.framework.intents import (
    Intent,
    IntentType,
    PredictionBuyIntent,
    PredictionRedeemIntent,
    PredictionSellIntent,
)

# =============================================================================
# PredictionBuyIntent Tests
# =============================================================================


class TestPredictionBuyIntent:
    """Test PredictionBuyIntent creation and validation."""

    def test_create_with_amount_usd(self):
        """Test creating intent with USD amount."""
        intent = PredictionBuyIntent(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        assert intent.market_id == "will-bitcoin-exceed-100000"
        assert intent.outcome == "YES"
        assert intent.amount_usd == Decimal("100")
        assert intent.shares is None
        assert intent.order_type == "market"
        assert intent.time_in_force == "GTC"
        assert intent.protocol == "polymarket"
        assert intent.intent_type == IntentType.PREDICTION_BUY

    def test_create_with_shares(self):
        """Test creating intent with number of shares."""
        intent = PredictionBuyIntent(
            market_id="will-eth-hit-5000",
            outcome="NO",
            shares=Decimal("50"),
        )

        assert intent.outcome == "NO"
        assert intent.shares == Decimal("50")
        assert intent.amount_usd is None

    def test_limit_order_with_max_price(self):
        """Test creating limit order with max price."""
        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.65"),
            order_type="limit",
        )

        assert intent.order_type == "limit"
        assert intent.max_price == Decimal("0.65")

    def test_with_expiration(self):
        """Test creating intent with expiration hours."""
        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
            expiration_hours=24,
        )

        assert intent.expiration_hours == 24

    def test_all_time_in_force_options(self):
        """Test all valid time_in_force options."""
        for tif in ["GTC", "IOC", "FOK"]:
            intent = PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("100"),
                time_in_force=tif,
            )
            assert intent.time_in_force == tif

    def test_factory_method(self):
        """Test Intent.prediction_buy factory method."""
        intent = Intent.prediction_buy(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        assert isinstance(intent, PredictionBuyIntent)
        assert intent.market_id == "will-bitcoin-exceed-100000"

    def test_serialization_roundtrip(self):
        """Test serialization and deserialization."""
        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
            max_price=Decimal("0.70"),
            order_type="limit",
            time_in_force="IOC",
            expiration_hours=12,
        )

        serialized = intent.serialize()
        assert serialized["type"] == "PREDICTION_BUY"
        assert serialized["market_id"] == "test-market"
        assert serialized["outcome"] == "YES"

        deserialized = PredictionBuyIntent.deserialize(serialized)
        assert deserialized.market_id == intent.market_id
        assert deserialized.outcome == intent.outcome
        assert deserialized.amount_usd == intent.amount_usd
        assert deserialized.max_price == intent.max_price

    def test_deserialize_via_intent_class(self):
        """Test deserialization via Intent.deserialize."""
        intent = Intent.prediction_buy(
            market_id="test",
            outcome="NO",
            shares=Decimal("25"),
        )

        serialized = intent.serialize()
        deserialized = Intent.deserialize(serialized)

        assert isinstance(deserialized, PredictionBuyIntent)
        assert deserialized.outcome == "NO"


class TestPredictionBuyIntentValidation:
    """Test PredictionBuyIntent validation rules."""

    def test_must_specify_amount_usd_or_shares(self):
        """Test that either amount_usd or shares must be provided."""
        with pytest.raises(ValueError, match="Must specify either amount_usd or shares"):
            PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
            )

    def test_cannot_specify_both_amounts(self):
        """Test that both amount_usd and shares cannot be provided."""
        with pytest.raises(ValueError, match="Cannot specify both amount_usd and shares"):
            PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("100"),
                shares=Decimal("50"),
            )

    def test_amount_usd_must_be_positive(self):
        """Test that amount_usd must be positive."""
        with pytest.raises(ValueError, match="amount_usd must be positive"):
            PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("-100"),
            )

    def test_shares_must_be_positive(self):
        """Test that shares must be positive."""
        with pytest.raises(ValueError, match="shares must be positive"):
            PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                shares=Decimal("0"),
            )

    def test_max_price_lower_bound(self):
        """Test that max_price must be >= 0.01."""
        with pytest.raises(ValueError, match="max_price must be between 0.01 and 0.99"):
            PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("100"),
                max_price=Decimal("0.001"),
            )

    def test_max_price_upper_bound(self):
        """Test that max_price must be <= 0.99."""
        with pytest.raises(ValueError, match="max_price must be between 0.01 and 0.99"):
            PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("100"),
                max_price=Decimal("1.00"),
            )

    def test_limit_order_requires_max_price(self):
        """Test that limit orders require max_price."""
        with pytest.raises(ValueError, match="Limit orders require max_price"):
            PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("100"),
                order_type="limit",
            )

    def test_expiration_hours_must_be_positive(self):
        """Test that expiration_hours must be positive."""
        with pytest.raises(ValueError, match="expiration_hours must be positive"):
            PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("100"),
                expiration_hours=0,
            )


# =============================================================================
# PredictionSellIntent Tests
# =============================================================================


class TestPredictionSellIntent:
    """Test PredictionSellIntent creation and validation."""

    def test_create_with_shares(self):
        """Test creating sell intent with specific shares."""
        intent = PredictionSellIntent(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            shares=Decimal("50"),
        )

        assert intent.market_id == "will-bitcoin-exceed-100000"
        assert intent.outcome == "YES"
        assert intent.shares == Decimal("50")
        assert intent.order_type == "market"
        assert intent.intent_type == IntentType.PREDICTION_SELL

    def test_create_with_all_shares(self):
        """Test creating sell intent to sell all shares."""
        intent = PredictionSellIntent(
            market_id="test-market",
            outcome="NO",
            shares="all",
        )

        assert intent.shares == "all"
        assert intent.is_chained_amount is True

    def test_limit_order_with_min_price(self):
        """Test creating limit sell order with min price."""
        intent = PredictionSellIntent(
            market_id="test-market",
            outcome="YES",
            shares=Decimal("25"),
            min_price=Decimal("0.40"),
            order_type="limit",
        )

        assert intent.order_type == "limit"
        assert intent.min_price == Decimal("0.40")

    def test_factory_method(self):
        """Test Intent.prediction_sell factory method."""
        intent = Intent.prediction_sell(
            market_id="test-market",
            outcome="YES",
            shares="all",
        )

        assert isinstance(intent, PredictionSellIntent)
        assert intent.shares == "all"

    def test_serialization_roundtrip(self):
        """Test serialization and deserialization."""
        intent = PredictionSellIntent(
            market_id="test-market",
            outcome="NO",
            shares=Decimal("100"),
            min_price=Decimal("0.35"),
            order_type="limit",
        )

        serialized = intent.serialize()
        assert serialized["type"] == "PREDICTION_SELL"

        deserialized = PredictionSellIntent.deserialize(serialized)
        assert deserialized.market_id == intent.market_id
        assert deserialized.shares == intent.shares
        assert deserialized.min_price == intent.min_price

    def test_serialization_preserves_all(self):
        """Test that shares='all' is preserved in serialization."""
        intent = PredictionSellIntent(
            market_id="test-market",
            outcome="YES",
            shares="all",
        )

        serialized = intent.serialize()
        assert serialized["shares"] == "all"

        deserialized = PredictionSellIntent.deserialize(serialized)
        assert deserialized.shares == "all"


class TestPredictionSellIntentValidation:
    """Test PredictionSellIntent validation rules."""

    def test_shares_must_be_positive(self):
        """Test that shares must be positive."""
        with pytest.raises(ValueError, match="shares must be positive"):
            PredictionSellIntent(
                market_id="test-market",
                outcome="YES",
                shares=Decimal("0"),
            )

    def test_shares_negative_fails(self):
        """Test that negative shares fail."""
        with pytest.raises(ValueError, match="shares must be positive"):
            PredictionSellIntent(
                market_id="test-market",
                outcome="YES",
                shares=Decimal("-10"),
            )

    def test_min_price_lower_bound(self):
        """Test that min_price must be >= 0.01."""
        with pytest.raises(ValueError, match="min_price must be between 0.01 and 0.99"):
            PredictionSellIntent(
                market_id="test-market",
                outcome="YES",
                shares=Decimal("50"),
                min_price=Decimal("0.005"),
            )

    def test_min_price_upper_bound(self):
        """Test that min_price must be <= 0.99."""
        with pytest.raises(ValueError, match="min_price must be between 0.01 and 0.99"):
            PredictionSellIntent(
                market_id="test-market",
                outcome="YES",
                shares=Decimal("50"),
                min_price=Decimal("1.00"),
            )

    def test_limit_order_requires_min_price(self):
        """Test that limit orders require min_price."""
        with pytest.raises(ValueError, match="Limit orders require min_price"):
            PredictionSellIntent(
                market_id="test-market",
                outcome="YES",
                shares=Decimal("50"),
                order_type="limit",
            )


# =============================================================================
# PredictionRedeemIntent Tests
# =============================================================================


class TestPredictionRedeemIntent:
    """Test PredictionRedeemIntent creation and validation."""

    def test_create_redeem_all(self):
        """Test creating redeem intent to redeem all shares."""
        intent = PredictionRedeemIntent(
            market_id="will-bitcoin-exceed-100000",
        )

        assert intent.market_id == "will-bitcoin-exceed-100000"
        assert intent.outcome is None
        assert intent.shares == "all"
        assert intent.intent_type == IntentType.PREDICTION_REDEEM

    def test_create_redeem_specific_outcome(self):
        """Test creating redeem intent for specific outcome."""
        intent = PredictionRedeemIntent(
            market_id="test-market",
            outcome="YES",
        )

        assert intent.outcome == "YES"
        assert intent.shares == "all"

    def test_create_redeem_specific_shares(self):
        """Test creating redeem intent for specific number of shares."""
        intent = PredictionRedeemIntent(
            market_id="test-market",
            outcome="NO",
            shares=Decimal("100"),
        )

        assert intent.outcome == "NO"
        assert intent.shares == Decimal("100")
        assert intent.is_chained_amount is False

    def test_is_chained_amount(self):
        """Test is_chained_amount property."""
        intent_all = PredictionRedeemIntent(
            market_id="test-market",
            shares="all",
        )
        assert intent_all.is_chained_amount is True

        intent_specific = PredictionRedeemIntent(
            market_id="test-market",
            shares=Decimal("50"),
        )
        assert intent_specific.is_chained_amount is False

    def test_factory_method(self):
        """Test Intent.prediction_redeem factory method."""
        intent = Intent.prediction_redeem(
            market_id="test-market",
            outcome="YES",
        )

        assert isinstance(intent, PredictionRedeemIntent)
        assert intent.outcome == "YES"

    def test_serialization_roundtrip(self):
        """Test serialization and deserialization."""
        intent = PredictionRedeemIntent(
            market_id="test-market",
            outcome="YES",
            shares=Decimal("75"),
        )

        serialized = intent.serialize()
        assert serialized["type"] == "PREDICTION_REDEEM"

        deserialized = PredictionRedeemIntent.deserialize(serialized)
        assert deserialized.market_id == intent.market_id
        assert deserialized.outcome == intent.outcome
        assert deserialized.shares == intent.shares

    def test_serialization_preserves_all(self):
        """Test that shares='all' is preserved in serialization."""
        intent = PredictionRedeemIntent(
            market_id="test-market",
            shares="all",
        )

        serialized = intent.serialize()
        assert serialized["shares"] == "all"

        deserialized = PredictionRedeemIntent.deserialize(serialized)
        assert deserialized.shares == "all"


class TestPredictionRedeemIntentValidation:
    """Test PredictionRedeemIntent validation rules."""

    def test_shares_must_be_positive(self):
        """Test that shares must be positive."""
        with pytest.raises(ValueError, match="shares must be positive"):
            PredictionRedeemIntent(
                market_id="test-market",
                shares=Decimal("0"),
            )

    def test_shares_negative_fails(self):
        """Test that negative shares fail."""
        with pytest.raises(ValueError, match="shares must be positive"):
            PredictionRedeemIntent(
                market_id="test-market",
                shares=Decimal("-10"),
            )


# =============================================================================
# Intent Deserialization Tests
# =============================================================================


class TestIntentDeserialization:
    """Test Intent.deserialize correctly handles prediction intents."""

    def test_deserialize_prediction_buy(self):
        """Test deserializing PREDICTION_BUY type."""
        data = {
            "type": "PREDICTION_BUY",
            "market_id": "test-market",
            "outcome": "YES",
            "amount_usd": "100",
            "protocol": "polymarket",
        }

        intent = Intent.deserialize(data)
        assert isinstance(intent, PredictionBuyIntent)
        assert intent.outcome == "YES"

    def test_deserialize_prediction_sell(self):
        """Test deserializing PREDICTION_SELL type."""
        data = {
            "type": "PREDICTION_SELL",
            "market_id": "test-market",
            "outcome": "NO",
            "shares": "all",
            "protocol": "polymarket",
        }

        intent = Intent.deserialize(data)
        assert isinstance(intent, PredictionSellIntent)
        assert intent.shares == "all"

    def test_deserialize_prediction_redeem(self):
        """Test deserializing PREDICTION_REDEEM type."""
        data = {
            "type": "PREDICTION_REDEEM",
            "market_id": "test-market",
            "outcome": "YES",
            "shares": "50",
            "protocol": "polymarket",
        }

        intent = Intent.deserialize(data)
        assert isinstance(intent, PredictionRedeemIntent)
        assert intent.shares == Decimal("50")


# =============================================================================
# Protocol Capabilities Tests
# =============================================================================


class TestPolymarketCapabilities:
    """Test Polymarket protocol capabilities are defined correctly."""

    def test_polymarket_in_capabilities(self):
        """Test that polymarket is in PROTOCOL_CAPABILITIES."""
        from almanak.framework.intents import PROTOCOL_CAPABILITIES

        assert "polymarket" in PROTOCOL_CAPABILITIES

    def test_polymarket_operations(self):
        """Test that polymarket has correct operations."""
        from almanak.framework.intents import PROTOCOL_CAPABILITIES

        caps = PROTOCOL_CAPABILITIES["polymarket"]
        assert "prediction_buy" in caps["operations"]
        assert "prediction_sell" in caps["operations"]
        assert "prediction_redeem" in caps["operations"]

    def test_polymarket_price_bounds(self):
        """Test that polymarket has correct price bounds."""
        from almanak.framework.intents import PROTOCOL_CAPABILITIES

        caps = PROTOCOL_CAPABILITIES["polymarket"]
        assert caps["min_price"] == Decimal("0.01")
        assert caps["max_price"] == Decimal("0.99")


# =============================================================================
# PredictionExitConditions Tests (US-015)
# =============================================================================


class TestPredictionBuyIntentWithExitConditions:
    """Test PredictionBuyIntent with exit_conditions field."""

    def test_create_with_exit_conditions(self):
        """Test creating intent with exit conditions."""
        from almanak.framework.intents import PredictionExitConditions

        exit_conditions = PredictionExitConditions(
            stop_loss_price=Decimal("0.40"),
            take_profit_price=Decimal("0.85"),
            exit_before_resolution_hours=24,
        )

        intent = PredictionBuyIntent(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            amount_usd=Decimal("100"),
            exit_conditions=exit_conditions,
        )

        assert intent.exit_conditions is not None
        assert intent.exit_conditions.stop_loss_price == Decimal("0.40")
        assert intent.exit_conditions.take_profit_price == Decimal("0.85")
        assert intent.exit_conditions.exit_before_resolution_hours == 24

    def test_create_without_exit_conditions(self):
        """Test creating intent without exit conditions (default None)."""
        intent = PredictionBuyIntent(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        assert intent.exit_conditions is None

    def test_exit_conditions_with_trailing_stop(self):
        """Test exit conditions with trailing stop percentage."""
        from almanak.framework.intents import PredictionExitConditions

        exit_conditions = PredictionExitConditions(
            trailing_stop_pct=Decimal("0.10"),  # 10% trailing stop
        )

        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
            exit_conditions=exit_conditions,
        )

        assert intent.exit_conditions.trailing_stop_pct == Decimal("0.10")

    def test_exit_conditions_with_liquidity_spread_limits(self):
        """Test exit conditions with liquidity and spread thresholds."""
        from almanak.framework.intents import PredictionExitConditions

        exit_conditions = PredictionExitConditions(
            max_spread_pct=Decimal("0.05"),  # 5% max spread
            min_liquidity_usd=Decimal("5000"),  # $5000 min liquidity
        )

        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="NO",
            shares=Decimal("50"),
            exit_conditions=exit_conditions,
        )

        assert intent.exit_conditions.max_spread_pct == Decimal("0.05")
        assert intent.exit_conditions.min_liquidity_usd == Decimal("5000")

    def test_serialization_with_exit_conditions(self):
        """Test serialization of intent with exit conditions."""
        from almanak.framework.intents import PredictionExitConditions

        exit_conditions = PredictionExitConditions(
            stop_loss_price=Decimal("0.30"),
            take_profit_price=Decimal("0.80"),
            exit_before_resolution_hours=12,
            trailing_stop_pct=Decimal("0.15"),
        )

        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
            exit_conditions=exit_conditions,
        )

        serialized = intent.serialize()

        # Check exit_conditions is serialized
        assert "exit_conditions" in serialized
        assert serialized["exit_conditions"]["stop_loss_price"] == "0.30"
        assert serialized["exit_conditions"]["take_profit_price"] == "0.80"
        assert serialized["exit_conditions"]["exit_before_resolution_hours"] == 12
        assert serialized["exit_conditions"]["trailing_stop_pct"] == "0.15"

    def test_deserialization_with_exit_conditions(self):
        """Test deserialization of intent with exit conditions."""
        from almanak.framework.intents import PredictionExitConditions

        exit_conditions = PredictionExitConditions(
            stop_loss_price=Decimal("0.35"),
            take_profit_price=Decimal("0.75"),
        )

        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
            exit_conditions=exit_conditions,
        )

        serialized = intent.serialize()
        deserialized = PredictionBuyIntent.deserialize(serialized)

        assert deserialized.exit_conditions is not None
        assert deserialized.exit_conditions.stop_loss_price == Decimal("0.35")
        assert deserialized.exit_conditions.take_profit_price == Decimal("0.75")

    def test_deserialization_without_exit_conditions(self):
        """Test deserialization of intent without exit conditions."""
        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        serialized = intent.serialize()
        deserialized = PredictionBuyIntent.deserialize(serialized)

        assert deserialized.exit_conditions is None

    def test_factory_method_with_exit_conditions(self):
        """Test Intent.prediction_buy factory method with exit conditions."""
        from almanak.framework.intents import PredictionExitConditions

        exit_conditions = PredictionExitConditions(
            stop_loss_price=Decimal("0.25"),
        )

        intent = Intent.prediction_buy(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
            exit_conditions=exit_conditions,
        )

        assert isinstance(intent, PredictionBuyIntent)
        assert intent.exit_conditions is not None
        assert intent.exit_conditions.stop_loss_price == Decimal("0.25")

    def test_exit_conditions_all_fields(self):
        """Test exit conditions with all fields populated."""
        from almanak.framework.intents import PredictionExitConditions

        exit_conditions = PredictionExitConditions(
            stop_loss_price=Decimal("0.30"),
            take_profit_price=Decimal("0.85"),
            exit_before_resolution_hours=48,
            trailing_stop_pct=Decimal("0.12"),
            max_spread_pct=Decimal("0.08"),
            min_liquidity_usd=Decimal("10000"),
        )

        intent = PredictionBuyIntent(
            market_id="multi-condition-test",
            outcome="YES",
            amount_usd=Decimal("500"),
            exit_conditions=exit_conditions,
        )

        # Verify all fields
        ec = intent.exit_conditions
        assert ec.stop_loss_price == Decimal("0.30")
        assert ec.take_profit_price == Decimal("0.85")
        assert ec.exit_before_resolution_hours == 48
        assert ec.trailing_stop_pct == Decimal("0.12")
        assert ec.max_spread_pct == Decimal("0.08")
        assert ec.min_liquidity_usd == Decimal("10000")

        # Verify serialization roundtrip
        serialized = intent.serialize()
        deserialized = PredictionBuyIntent.deserialize(serialized)

        ec2 = deserialized.exit_conditions
        assert ec2.stop_loss_price == ec.stop_loss_price
        assert ec2.take_profit_price == ec.take_profit_price
        assert ec2.exit_before_resolution_hours == ec.exit_before_resolution_hours
        assert ec2.trailing_stop_pct == ec.trailing_stop_pct
        assert ec2.max_spread_pct == ec.max_spread_pct
        assert ec2.min_liquidity_usd == ec.min_liquidity_usd
