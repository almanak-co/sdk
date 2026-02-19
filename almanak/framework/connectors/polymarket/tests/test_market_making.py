"""Tests for market making utilities."""

from decimal import Decimal

import pytest

from almanak.framework.connectors.polymarket.market_making import (
    MAX_PRICE,
    MIN_PRICE,
    Quote,
    RiskParameters,
    calculate_inventory_skew,
    calculate_optimal_spread,
    generate_quote_ladder,
    should_requote,
)
from almanak.framework.connectors.polymarket.models import OrderBook, PriceLevel


class TestQuote:
    """Tests for the Quote dataclass."""

    def test_create_valid_quote(self) -> None:
        """Test creating a valid quote."""
        quote = Quote(price=Decimal("0.50"), size=Decimal("100"), side="BUY")
        assert quote.price == Decimal("0.50")
        assert quote.size == Decimal("100")
        assert quote.side == "BUY"

    def test_create_buy_quote(self) -> None:
        """Test creating a buy quote."""
        quote = Quote(price=Decimal("0.45"), size=Decimal("50"), side="BUY")
        assert quote.side == "BUY"

    def test_create_sell_quote(self) -> None:
        """Test creating a sell quote."""
        quote = Quote(price=Decimal("0.55"), size=Decimal("50"), side="SELL")
        assert quote.side == "SELL"

    def test_price_at_min_bound(self) -> None:
        """Test quote at minimum price."""
        quote = Quote(price=Decimal("0.01"), size=Decimal("10"), side="BUY")
        assert quote.price == Decimal("0.01")

    def test_price_at_max_bound(self) -> None:
        """Test quote at maximum price."""
        quote = Quote(price=Decimal("0.99"), size=Decimal("10"), side="SELL")
        assert quote.price == Decimal("0.99")

    def test_invalid_price_below_min(self) -> None:
        """Test that price below 0.01 raises ValueError."""
        with pytest.raises(ValueError, match="must be between 0.01 and 0.99"):
            Quote(price=Decimal("0.005"), size=Decimal("10"), side="BUY")

    def test_invalid_price_above_max(self) -> None:
        """Test that price above 0.99 raises ValueError."""
        with pytest.raises(ValueError, match="must be between 0.01 and 0.99"):
            Quote(price=Decimal("0.995"), size=Decimal("10"), side="SELL")

    def test_invalid_size_zero(self) -> None:
        """Test that zero size raises ValueError."""
        with pytest.raises(ValueError, match="must be positive"):
            Quote(price=Decimal("0.50"), size=Decimal("0"), side="BUY")

    def test_invalid_size_negative(self) -> None:
        """Test that negative size raises ValueError."""
        with pytest.raises(ValueError, match="must be positive"):
            Quote(price=Decimal("0.50"), size=Decimal("-10"), side="BUY")

    def test_to_dict(self) -> None:
        """Test quote serialization to dict."""
        quote = Quote(price=Decimal("0.50"), size=Decimal("100"), side="BUY")
        result = quote.to_dict()
        assert result == {"price": "0.50", "size": "100", "side": "BUY"}

    def test_from_dict(self) -> None:
        """Test quote deserialization from dict."""
        data = {"price": "0.65", "size": "200", "side": "SELL"}
        quote = Quote.from_dict(data)
        assert quote.price == Decimal("0.65")
        assert quote.size == Decimal("200")
        assert quote.side == "SELL"

    def test_round_trip_serialization(self) -> None:
        """Test that to_dict/from_dict round-trips correctly."""
        original = Quote(price=Decimal("0.42"), size=Decimal("55"), side="BUY")
        restored = Quote.from_dict(original.to_dict())
        assert restored.price == original.price
        assert restored.size == original.size
        assert restored.side == original.side


class TestRiskParameters:
    """Tests for RiskParameters dataclass."""

    def test_default_values(self) -> None:
        """Test default risk parameter values."""
        params = RiskParameters()
        assert params.base_spread == Decimal("0.02")
        assert params.skew_factor == Decimal("0.5")
        assert params.max_position == Decimal("1000")
        assert params.min_edge == Decimal("0.001")
        assert params.volatility_multiplier == Decimal("1.0")
        assert params.tick_size == Decimal("0.01")

    def test_custom_values(self) -> None:
        """Test creating with custom values."""
        params = RiskParameters(
            base_spread=Decimal("0.03"),
            skew_factor=Decimal("0.7"),
            max_position=Decimal("500"),
            min_edge=Decimal("0.002"),
            volatility_multiplier=Decimal("1.5"),
            tick_size=Decimal("0.001"),
        )
        assert params.base_spread == Decimal("0.03")
        assert params.skew_factor == Decimal("0.7")
        assert params.max_position == Decimal("500")

    def test_invalid_negative_base_spread(self) -> None:
        """Test that negative base_spread raises ValueError."""
        with pytest.raises(ValueError, match="base_spread must be non-negative"):
            RiskParameters(base_spread=Decimal("-0.01"))

    def test_invalid_skew_factor_below_zero(self) -> None:
        """Test that skew_factor below 0 raises ValueError."""
        with pytest.raises(ValueError, match="skew_factor must be between 0 and 1"):
            RiskParameters(skew_factor=Decimal("-0.1"))

    def test_invalid_skew_factor_above_one(self) -> None:
        """Test that skew_factor above 1 raises ValueError."""
        with pytest.raises(ValueError, match="skew_factor must be between 0 and 1"):
            RiskParameters(skew_factor=Decimal("1.1"))

    def test_invalid_max_position_zero(self) -> None:
        """Test that zero max_position raises ValueError."""
        with pytest.raises(ValueError, match="max_position must be positive"):
            RiskParameters(max_position=Decimal("0"))

    def test_invalid_negative_min_edge(self) -> None:
        """Test that negative min_edge raises ValueError."""
        with pytest.raises(ValueError, match="min_edge must be non-negative"):
            RiskParameters(min_edge=Decimal("-0.001"))

    def test_invalid_volatility_multiplier(self) -> None:
        """Test that non-positive volatility_multiplier raises ValueError."""
        with pytest.raises(ValueError, match="volatility_multiplier must be positive"):
            RiskParameters(volatility_multiplier=Decimal("0"))

    def test_invalid_tick_size(self) -> None:
        """Test that non-positive tick_size raises ValueError."""
        with pytest.raises(ValueError, match="tick_size must be positive"):
            RiskParameters(tick_size=Decimal("0"))


class TestCalculateInventorySkew:
    """Tests for calculate_inventory_skew function."""

    def test_neutral_position(self) -> None:
        """Test skew for neutral (zero) position."""
        skew = calculate_inventory_skew(Decimal("0"), Decimal("1000"))
        assert skew == Decimal("0")

    def test_max_long_position(self) -> None:
        """Test skew for maximum long position."""
        skew = calculate_inventory_skew(Decimal("1000"), Decimal("1000"))
        assert skew == Decimal("1")

    def test_max_short_position(self) -> None:
        """Test skew for maximum short position."""
        skew = calculate_inventory_skew(Decimal("-1000"), Decimal("1000"))
        assert skew == Decimal("-1")

    def test_half_long_position(self) -> None:
        """Test skew for half maximum long position."""
        skew = calculate_inventory_skew(Decimal("500"), Decimal("1000"))
        assert skew == Decimal("0.5")

    def test_quarter_short_position(self) -> None:
        """Test skew for quarter maximum short position."""
        skew = calculate_inventory_skew(Decimal("-250"), Decimal("1000"))
        assert skew == Decimal("-0.25")

    def test_position_exceeds_max_clamped(self) -> None:
        """Test that positions exceeding max are clamped to [-1, 1]."""
        skew = calculate_inventory_skew(Decimal("2000"), Decimal("1000"))
        assert skew == Decimal("1")

        skew = calculate_inventory_skew(Decimal("-1500"), Decimal("1000"))
        assert skew == Decimal("-1")

    def test_invalid_max_position_zero(self) -> None:
        """Test that zero max_position raises ValueError."""
        with pytest.raises(ValueError, match="max_position must be positive"):
            calculate_inventory_skew(Decimal("100"), Decimal("0"))

    def test_invalid_max_position_negative(self) -> None:
        """Test that negative max_position raises ValueError."""
        with pytest.raises(ValueError, match="max_position must be positive"):
            calculate_inventory_skew(Decimal("100"), Decimal("-1000"))


class TestCalculateOptimalSpread:
    """Tests for calculate_optimal_spread function."""

    def _create_orderbook(
        self,
        best_bid: Decimal | None = None,
        best_ask: Decimal | None = None,
    ) -> OrderBook:
        """Helper to create an orderbook for testing."""
        bids = [PriceLevel(price=best_bid, size=Decimal("100"))] if best_bid else []
        asks = [PriceLevel(price=best_ask, size=Decimal("100"))] if best_ask else []
        return OrderBook(
            market="test_token",
            asset_id="test_token",
            bids=bids,
            asks=asks,
        )

    def test_neutral_inventory_symmetric_spread(self) -> None:
        """Test that neutral inventory produces symmetric spread."""
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )
        risk_params = RiskParameters(base_spread=Decimal("0.04"))

        bid, ask = calculate_optimal_spread(orderbook, Decimal("0"), risk_params)

        # With 4% spread and neutral inventory, should be symmetric around mid (0.50)
        assert bid < Decimal("0.50")
        assert ask > Decimal("0.50")
        # Bid and ask should be equidistant from mid
        mid = Decimal("0.50")
        assert abs(mid - bid - (ask - mid)) < Decimal("0.001")

    def test_long_inventory_skews_lower(self) -> None:
        """Test that long inventory skews quotes lower (encourage selling)."""
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )
        # Use larger spread and skew to ensure shift is visible after tick rounding
        risk_params = RiskParameters(
            base_spread=Decimal("0.10"),
            skew_factor=Decimal("1.0"),  # Full skew
            max_position=Decimal("1000"),
            tick_size=Decimal("0.01"),
        )

        # Neutral position
        bid_neutral, ask_neutral = calculate_optimal_spread(orderbook, Decimal("0"), risk_params)

        # Long position (1000 of 1000 max = 100% long)
        bid_long, ask_long = calculate_optimal_spread(orderbook, Decimal("1000"), risk_params)

        # Long position should have lower or equal bid and lower or equal ask (shifted down)
        # Due to tick rounding, we check <= instead of strict <
        assert bid_long <= bid_neutral
        assert ask_long <= ask_neutral
        # But at least one should be strictly lower with 100% skew and large spread
        assert bid_long < bid_neutral or ask_long < ask_neutral

    def test_short_inventory_skews_higher(self) -> None:
        """Test that short inventory skews quotes higher (encourage buying)."""
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )
        # Use larger spread and skew to ensure shift is visible after tick rounding
        risk_params = RiskParameters(
            base_spread=Decimal("0.10"),
            skew_factor=Decimal("1.0"),  # Full skew
            max_position=Decimal("1000"),
            tick_size=Decimal("0.01"),
        )

        # Neutral position
        bid_neutral, ask_neutral = calculate_optimal_spread(orderbook, Decimal("0"), risk_params)

        # Short position (-1000 of 1000 max = 100% short)
        bid_short, ask_short = calculate_optimal_spread(orderbook, Decimal("-1000"), risk_params)

        # Short position should have higher or equal bid and higher or equal ask (shifted up)
        # Due to tick rounding, we check >= instead of strict >
        assert bid_short >= bid_neutral
        assert ask_short >= ask_neutral
        # But at least one should be strictly higher with 100% skew and large spread
        assert bid_short > bid_neutral or ask_short > ask_neutral

    def test_empty_orderbook_returns_bounds(self) -> None:
        """Test that empty orderbook returns price bounds."""
        orderbook = self._create_orderbook()  # No bids or asks
        risk_params = RiskParameters()

        bid, ask = calculate_optimal_spread(orderbook, Decimal("0"), risk_params)

        assert bid == MIN_PRICE
        assert ask == MAX_PRICE

    def test_prices_clamped_to_valid_range(self) -> None:
        """Test that prices are clamped to [0.01, 0.99]."""
        # Extreme case: very wide spread near boundary
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.02"),
            best_ask=Decimal("0.04"),
        )
        risk_params = RiskParameters(
            base_spread=Decimal("0.10"),  # Very wide spread
            max_position=Decimal("100"),
        )

        bid, ask = calculate_optimal_spread(orderbook, Decimal("0"), risk_params)

        assert bid >= MIN_PRICE
        assert ask <= MAX_PRICE

    def test_volatility_multiplier_widens_spread(self) -> None:
        """Test that higher volatility multiplier widens spread."""
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )

        # Low volatility
        low_vol_params = RiskParameters(
            base_spread=Decimal("0.02"),
            volatility_multiplier=Decimal("1.0"),
        )
        bid_low, ask_low = calculate_optimal_spread(orderbook, Decimal("0"), low_vol_params)

        # High volatility
        high_vol_params = RiskParameters(
            base_spread=Decimal("0.02"),
            volatility_multiplier=Decimal("2.0"),
        )
        bid_high, ask_high = calculate_optimal_spread(orderbook, Decimal("0"), high_vol_params)

        # High volatility should have wider spread
        spread_low = ask_low - bid_low
        spread_high = ask_high - bid_high
        assert spread_high > spread_low

    def test_bid_always_less_than_ask(self) -> None:
        """Test that bid is always less than ask."""
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.50"),  # No spread in orderbook
        )
        risk_params = RiskParameters(base_spread=Decimal("0.001"))

        bid, ask = calculate_optimal_spread(orderbook, Decimal("0"), risk_params)

        assert bid < ask


class TestGenerateQuoteLadder:
    """Tests for generate_quote_ladder function."""

    def test_single_level_ladder(self) -> None:
        """Test generating a single-level ladder."""
        quotes = generate_quote_ladder(
            mid_price=Decimal("0.50"),
            spread=Decimal("0.02"),
            num_levels=1,
            size_per_level=Decimal("10"),
        )

        assert len(quotes) == 2  # One bid, one ask
        bids = [q for q in quotes if q.side == "BUY"]
        asks = [q for q in quotes if q.side == "SELL"]
        assert len(bids) == 1
        assert len(asks) == 1

    def test_multi_level_ladder(self) -> None:
        """Test generating a multi-level ladder."""
        quotes = generate_quote_ladder(
            mid_price=Decimal("0.50"),
            spread=Decimal("0.02"),
            num_levels=3,
            size_per_level=Decimal("10"),
        )

        # Should have 3 bids and 3 asks
        bids = [q for q in quotes if q.side == "BUY"]
        asks = [q for q in quotes if q.side == "SELL"]
        assert len(bids) == 3
        assert len(asks) == 3

    def test_ladder_sorted_by_price(self) -> None:
        """Test that ladder is sorted correctly (asks high to low, then bids)."""
        quotes = generate_quote_ladder(
            mid_price=Decimal("0.50"),
            spread=Decimal("0.02"),
            num_levels=3,
            size_per_level=Decimal("10"),
        )

        # Asks should come first (SELL), sorted by descending price
        # Then bids (BUY), sorted by descending price
        for i in range(len(quotes) - 1):
            if quotes[i].side == quotes[i + 1].side:
                # Same side: prices should be descending
                assert quotes[i].price >= quotes[i + 1].price

    def test_size_per_level(self) -> None:
        """Test that each level has correct size."""
        quotes = generate_quote_ladder(
            mid_price=Decimal("0.50"),
            spread=Decimal("0.02"),
            num_levels=2,
            size_per_level=Decimal("25"),
        )

        for quote in quotes:
            assert quote.size == Decimal("25")

    def test_skew_shifts_ladder(self) -> None:
        """Test that positive skew shifts the ladder down."""
        # No skew
        no_skew = generate_quote_ladder(
            mid_price=Decimal("0.50"),
            spread=Decimal("0.10"),  # Larger spread to see effects better
            num_levels=1,
            size_per_level=Decimal("10"),
            skew=Decimal("0"),
        )

        # Positive skew (long position, want to sell) - shifts ladder down
        positive_skew = generate_quote_ladder(
            mid_price=Decimal("0.50"),
            spread=Decimal("0.10"),
            num_levels=1,
            size_per_level=Decimal("10"),
            skew=Decimal("1.0"),  # Full skew
        )

        # Get best bid and ask prices
        no_skew_bid = max(q.price for q in no_skew if q.side == "BUY")
        pos_skew_bid = max(q.price for q in positive_skew if q.side == "BUY")

        no_skew_ask = min(q.price for q in no_skew if q.side == "SELL")
        pos_skew_ask = min(q.price for q in positive_skew if q.side == "SELL")

        # Positive skew should shift ladder down (bid and ask both lower or equal)
        # This encourages selling by lowering the ask closer to mid
        assert pos_skew_bid <= no_skew_bid
        assert pos_skew_ask <= no_skew_ask
        # At least one should be strictly lower with full skew
        assert pos_skew_bid < no_skew_bid or pos_skew_ask < no_skew_ask

    def test_invalid_num_levels_zero(self) -> None:
        """Test that zero num_levels raises ValueError."""
        with pytest.raises(ValueError, match="num_levels must be between 1 and 10"):
            generate_quote_ladder(
                mid_price=Decimal("0.50"),
                spread=Decimal("0.02"),
                num_levels=0,
                size_per_level=Decimal("10"),
            )

    def test_invalid_num_levels_too_high(self) -> None:
        """Test that num_levels > 10 raises ValueError."""
        with pytest.raises(ValueError, match="num_levels must be between 1 and 10"):
            generate_quote_ladder(
                mid_price=Decimal("0.50"),
                spread=Decimal("0.02"),
                num_levels=11,
                size_per_level=Decimal("10"),
            )

    def test_invalid_spread_zero(self) -> None:
        """Test that zero spread raises ValueError."""
        with pytest.raises(ValueError, match="spread must be positive"):
            generate_quote_ladder(
                mid_price=Decimal("0.50"),
                spread=Decimal("0"),
                num_levels=1,
                size_per_level=Decimal("10"),
            )

    def test_invalid_size_negative(self) -> None:
        """Test that negative size raises ValueError."""
        with pytest.raises(ValueError, match="size_per_level must be positive"):
            generate_quote_ladder(
                mid_price=Decimal("0.50"),
                spread=Decimal("0.02"),
                num_levels=1,
                size_per_level=Decimal("-10"),
            )

    def test_invalid_skew_out_of_range(self) -> None:
        """Test that skew outside [-1, 1] raises ValueError."""
        with pytest.raises(ValueError, match="skew must be between -1 and 1"):
            generate_quote_ladder(
                mid_price=Decimal("0.50"),
                spread=Decimal("0.02"),
                num_levels=1,
                size_per_level=Decimal("10"),
                skew=Decimal("1.5"),
            )

    def test_prices_within_valid_range(self) -> None:
        """Test that all generated prices are within valid range."""
        quotes = generate_quote_ladder(
            mid_price=Decimal("0.05"),  # Near lower bound
            spread=Decimal("0.10"),  # Wide spread
            num_levels=3,
            size_per_level=Decimal("10"),
        )

        for quote in quotes:
            assert MIN_PRICE <= quote.price <= MAX_PRICE


class TestShouldRequote:
    """Tests for should_requote function."""

    def _create_orderbook(
        self,
        best_bid: Decimal | None = None,
        best_ask: Decimal | None = None,
    ) -> OrderBook:
        """Helper to create an orderbook for testing."""
        bids = [PriceLevel(price=best_bid, size=Decimal("100"))] if best_bid else []
        asks = [PriceLevel(price=best_ask, size=Decimal("100"))] if best_ask else []
        return OrderBook(
            market="test_token",
            asset_id="test_token",
            bids=bids,
            asks=asks,
        )

    def test_no_quotes_should_requote(self) -> None:
        """Test that empty quotes list triggers requote."""
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )
        assert should_requote([], orderbook) is True

    def test_empty_orderbook_should_requote(self) -> None:
        """Test that empty orderbook triggers requote."""
        quotes = [
            Quote(price=Decimal("0.49"), size=Decimal("10"), side="BUY"),
            Quote(price=Decimal("0.51"), size=Decimal("10"), side="SELL"),
        ]
        orderbook = self._create_orderbook()  # Empty

        assert should_requote(quotes, orderbook) is True

    def test_stable_market_no_requote(self) -> None:
        """Test that stable market doesn't trigger requote."""
        quotes = [
            Quote(price=Decimal("0.49"), size=Decimal("10"), side="BUY"),
            Quote(price=Decimal("0.51"), size=Decimal("10"), side="SELL"),
        ]
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )

        assert should_requote(quotes, orderbook, threshold=Decimal("0.01")) is False

    def test_mid_price_move_triggers_requote(self) -> None:
        """Test that significant mid price movement triggers requote."""
        quotes = [
            Quote(price=Decimal("0.49"), size=Decimal("10"), side="BUY"),
            Quote(price=Decimal("0.51"), size=Decimal("10"), side="SELL"),
        ]
        # Market has moved significantly
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.55"),
            best_ask=Decimal("0.57"),
        )

        assert should_requote(quotes, orderbook, threshold=Decimal("0.01")) is True

    def test_small_move_no_requote(self) -> None:
        """Test that small price movement doesn't trigger requote."""
        quotes = [
            Quote(price=Decimal("0.49"), size=Decimal("10"), side="BUY"),
            Quote(price=Decimal("0.51"), size=Decimal("10"), side="SELL"),
        ]
        # Market moved slightly
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.495"),
            best_ask=Decimal("0.505"),
        )

        assert should_requote(quotes, orderbook, threshold=Decimal("0.02")) is False

    def test_not_competitive_on_bids_triggers_requote(self) -> None:
        """Test that being outbid triggers requote."""
        quotes = [
            Quote(price=Decimal("0.49"), size=Decimal("10"), side="BUY"),
            Quote(price=Decimal("0.51"), size=Decimal("10"), side="SELL"),
        ]
        # Someone is bidding higher than us
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.52"),  # Higher than our bid
            best_ask=Decimal("0.53"),
        )

        assert should_requote(quotes, orderbook, threshold=Decimal("0.01")) is True

    def test_not_competitive_on_asks_triggers_requote(self) -> None:
        """Test that being undercut triggers requote."""
        quotes = [
            Quote(price=Decimal("0.49"), size=Decimal("10"), side="BUY"),
            Quote(price=Decimal("0.51"), size=Decimal("10"), side="SELL"),
        ]
        # Someone is asking lower than us
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.47"),
            best_ask=Decimal("0.48"),  # Lower than our ask
        )

        assert should_requote(quotes, orderbook, threshold=Decimal("0.01")) is True

    def test_missing_bid_side_triggers_requote(self) -> None:
        """Test that missing bid side triggers requote."""
        quotes = [
            Quote(price=Decimal("0.51"), size=Decimal("10"), side="SELL"),
        ]
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )

        assert should_requote(quotes, orderbook) is True

    def test_missing_ask_side_triggers_requote(self) -> None:
        """Test that missing ask side triggers requote."""
        quotes = [
            Quote(price=Decimal("0.49"), size=Decimal("10"), side="BUY"),
        ]
        orderbook = self._create_orderbook(
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )

        assert should_requote(quotes, orderbook) is True


class TestModuleExports:
    """Tests for module exports."""

    def test_all_functions_exported(self) -> None:
        """Test that all expected functions are exported."""
        from almanak.framework.connectors.polymarket import market_making

        assert hasattr(market_making, "Quote")
        assert hasattr(market_making, "RiskParameters")
        assert hasattr(market_making, "calculate_inventory_skew")
        assert hasattr(market_making, "calculate_optimal_spread")
        assert hasattr(market_making, "generate_quote_ladder")
        assert hasattr(market_making, "should_requote")

    def test_imports_from_package(self) -> None:
        """Test that functions can be imported from main package."""
        from almanak.framework.connectors.polymarket import (
            Quote,
            RiskParameters,
            calculate_inventory_skew,
            calculate_optimal_spread,
            generate_quote_ladder,
            should_requote,
        )

        # Verify they're the expected types
        assert Quote is not None
        assert RiskParameters is not None
        assert callable(calculate_inventory_skew)
        assert callable(calculate_optimal_spread)
        assert callable(generate_quote_ladder)
        assert callable(should_requote)
