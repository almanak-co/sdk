"""Branch coverage for PredictionBuyIntent.validate_prediction_buy_intent.

Exercises every validation rule of the model validator: amount
exclusivity, positivity, max_price bounds, the limit-order max_price
requirement, and expiration_hours positivity. Pure pydantic — no chain.
"""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.intents.prediction_intents import PredictionBuyIntent

MARKET = "will-bitcoin-exceed-100000"


def _buy(**kwargs) -> PredictionBuyIntent:
    return PredictionBuyIntent(market_id=MARKET, outcome="YES", **kwargs)


class TestAmountSpecification:
    def test_amount_usd_market_order_is_valid(self):
        intent = _buy(amount_usd=Decimal("100"))
        assert intent.amount_usd == Decimal("100")
        assert intent.shares is None
        assert intent.order_type == "market"

    def test_shares_only_is_valid(self):
        intent = _buy(shares=Decimal("50"))
        assert intent.shares == Decimal("50")
        assert intent.amount_usd is None

    def test_neither_amount_nor_shares_raises(self):
        with pytest.raises(ValidationError, match="Must specify either amount_usd or shares"):
            _buy()

    def test_both_amount_and_shares_raises(self):
        with pytest.raises(ValidationError, match="Cannot specify both amount_usd and shares"):
            _buy(amount_usd=Decimal("100"), shares=Decimal("50"))

    def test_zero_amount_usd_raises(self):
        with pytest.raises(ValidationError, match="amount_usd must be positive"):
            _buy(amount_usd=Decimal("0"))

    def test_negative_amount_usd_raises(self):
        with pytest.raises(ValidationError, match="amount_usd must be positive"):
            _buy(amount_usd=Decimal("-5"))

    def test_zero_shares_raises(self):
        with pytest.raises(ValidationError, match="shares must be positive"):
            _buy(shares=Decimal("0"))


class TestMaxPrice:
    def test_max_price_below_floor_raises(self):
        with pytest.raises(ValidationError, match="max_price must be between 0.01 and 0.99"):
            _buy(amount_usd=Decimal("100"), max_price=Decimal("0.005"))

    def test_max_price_above_ceiling_raises(self):
        with pytest.raises(ValidationError, match="max_price must be between 0.01 and 0.99"):
            _buy(amount_usd=Decimal("100"), max_price=Decimal("1.5"))

    def test_max_price_boundaries_are_valid(self):
        assert _buy(amount_usd=Decimal("100"), max_price=Decimal("0.01")).max_price == Decimal("0.01")
        assert _buy(amount_usd=Decimal("100"), max_price=Decimal("0.99")).max_price == Decimal("0.99")


class TestOrderType:
    def test_limit_order_without_max_price_raises(self):
        with pytest.raises(ValidationError, match="Limit orders require max_price"):
            _buy(amount_usd=Decimal("100"), order_type="limit")

    def test_limit_order_with_max_price_is_valid(self):
        intent = _buy(shares=Decimal("50"), max_price=Decimal("0.65"), order_type="limit")
        assert intent.order_type == "limit"
        assert intent.max_price == Decimal("0.65")


class TestExpirationHours:
    def test_zero_expiration_hours_raises(self):
        with pytest.raises(ValidationError, match="expiration_hours must be positive"):
            _buy(amount_usd=Decimal("100"), expiration_hours=0)

    def test_negative_expiration_hours_raises(self):
        with pytest.raises(ValidationError, match="expiration_hours must be positive"):
            _buy(amount_usd=Decimal("100"), expiration_hours=-3)

    def test_positive_expiration_hours_is_valid(self):
        assert _buy(amount_usd=Decimal("100"), expiration_hours=24).expiration_hours == 24
