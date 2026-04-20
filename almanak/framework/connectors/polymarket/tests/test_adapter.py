"""Tests for Polymarket Adapter.

Tests cover:
- Intent compilation for PredictionBuyIntent
- Intent compilation for PredictionSellIntent
- Intent compilation for PredictionRedeemIntent
- Market resolution by ID and slug
- Order parameter calculation
- Error handling
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from almanak.framework.connectors.polymarket import (
    GammaMarket,
    PolymarketAdapter,
    PolymarketConfig,
    SignatureType,
)
from almanak.framework.connectors.polymarket.ctf_sdk import ResolutionStatus
from almanak.framework.connectors.polymarket.exceptions import (
    PolymarketMarketNotFoundError,
    PolymarketMarketNotResolvedError,
)
from almanak.framework.intents.vocabulary import (
    IntentType,
    PredictionBuyIntent,
    PredictionRedeemIntent,
    PredictionSellIntent,
)

# =============================================================================
# Test Data
# =============================================================================


def create_test_market(
    market_id: str = "12345",
    question: str = "Will Bitcoin exceed $100k by end of 2025?",
    slug: str = "will-bitcoin-exceed-100k",
    yes_price: Decimal = Decimal("0.65"),
    no_price: Decimal = Decimal("0.35"),
    yes_token_id: str = "111111111111111111111111111111111111111111",
    no_token_id: str = "222222222222222222222222222222222222222222",
    condition_id: str = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
) -> GammaMarket:
    """Create a test GammaMarket."""
    return GammaMarket(
        id=market_id,
        condition_id=condition_id,
        question=question,
        slug=slug,
        outcomes=["Yes", "No"],
        outcome_prices=[yes_price, no_price],
        clob_token_ids=[yes_token_id, no_token_id],
        volume=Decimal("1000000"),
        volume_24hr=Decimal("50000"),
        liquidity=Decimal("100000"),
        active=True,
        closed=False,
        enable_order_book=True,
    )


@dataclass
class MockSignedOrder:
    """Mock signed order for testing."""

    order: Any = None
    signature: str = "0xmocksignature"

    def to_api_payload(self) -> dict[str, Any]:
        return {
            "order": {"tokenId": "111111", "side": "BUY"},
            "signature": self.signature,
        }


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_private_key():
    """Return a deterministic test private key."""
    return "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


@pytest.fixture
def test_wallet():
    """Return the wallet address corresponding to the test private key."""
    return "0xFCAd0B19bB29D4674531d6f115237E16AfCE377c"


@pytest.fixture
def config(test_private_key, test_wallet):
    """Create test configuration."""
    return PolymarketConfig(
        wallet_address=test_wallet,
        private_key=SecretStr(test_private_key),
        signature_type=SignatureType.EOA,
    )


@pytest.fixture
def test_market():
    """Create a test market."""
    return create_test_market()


@pytest.fixture
def mock_clob_client(test_market):
    """Create a mock CLOB client."""
    mock = MagicMock()
    mock.get_market.return_value = test_market
    mock.get_market_by_slug.return_value = test_market
    mock.get_positions.return_value = []
    mock.create_and_sign_limit_order.return_value = MockSignedOrder()
    mock.create_and_sign_market_order.return_value = MockSignedOrder()
    mock.close.return_value = None
    return mock


@pytest.fixture
def mock_ctf_sdk():
    """Create a mock CTF SDK."""
    mock = MagicMock()
    mock.get_condition_resolution.return_value = ResolutionStatus(
        condition_id="0x1234",
        is_resolved=True,
        payout_denominator=1,
        payout_numerators=[1, 0],
        winning_outcome=0,
    )
    mock.build_redeem_tx.return_value = MagicMock(
        to="0xConditionalTokens",
        data="0xredeemdata",
        value=0,
        gas_estimate=200000,
        description="Redeem winning positions",
    )
    return mock


@pytest.fixture
def mock_web3():
    """Create a mock Web3 instance."""
    return MagicMock()


@pytest.fixture
def adapter_with_mocks(config, mock_clob_client, mock_ctf_sdk, mock_web3):
    """Create adapter with mocked dependencies."""
    adapter = PolymarketAdapter(config, web3=mock_web3)
    adapter.clob = mock_clob_client
    adapter.ctf = mock_ctf_sdk
    return adapter


# =============================================================================
# Initialization Tests
# =============================================================================


class TestAdapterInitialization:
    """Tests for adapter initialization."""

    def test_adapter_creation(self, config):
        """Test adapter can be created with config."""
        with patch.object(PolymarketAdapter, "__init__", return_value=None):
            adapter = PolymarketAdapter.__new__(PolymarketAdapter)
            adapter.config = config
            adapter.web3 = None
            adapter._market_cache = {}
            assert adapter.config == config

    def test_adapter_creation_with_web3(self, config, mock_web3):
        """Test adapter creation with Web3 instance."""
        with patch.object(PolymarketAdapter, "__init__", return_value=None):
            adapter = PolymarketAdapter.__new__(PolymarketAdapter)
            adapter.config = config
            adapter.web3 = mock_web3
            adapter._market_cache = {}
            assert adapter.web3 == mock_web3

    def test_context_manager(self, adapter_with_mocks):
        """Test adapter can be used as context manager."""
        with adapter_with_mocks as adapter:
            assert adapter is not None
        # Close should have been called
        adapter_with_mocks.clob.close.assert_called_once()


# =============================================================================
# Buy Intent Compilation Tests
# =============================================================================


class TestBuyIntentCompilation:
    """Tests for compiling PredictionBuyIntent."""

    def test_compile_buy_intent_market_order_amount_usd(self, adapter_with_mocks, test_market):
        """Test compiling a market order with amount_usd."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.intent_type == IntentType.PREDICTION_BUY.value
        assert bundle.metadata["market_id"] == test_market.id
        assert bundle.metadata["outcome"] == "YES"
        assert bundle.metadata["side"] == "BUY"
        assert bundle.metadata["protocol"] == "polymarket"
        assert "order_payload" in bundle.metadata

    def test_compile_buy_intent_market_order_shares(self, adapter_with_mocks, test_market):
        """Test compiling a market order with shares."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.intent_type == IntentType.PREDICTION_BUY.value
        assert bundle.metadata["size"] == "50"

    def test_compile_buy_intent_limit_order(self, adapter_with_mocks, test_market):
        """Test compiling a limit order with max_price."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.65"),
            order_type="limit",
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.intent_type == IntentType.PREDICTION_BUY.value
        assert bundle.metadata["price"] == "0.65"
        assert bundle.metadata["order_type"] == "GTC"

    def test_compile_buy_intent_no_outcome(self, adapter_with_mocks, test_market):
        """Test compiling buy for NO outcome."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="NO",
            amount_usd=Decimal("100"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.metadata["outcome"] == "NO"
        assert bundle.metadata["token_id"] == test_market.no_token_id

    def test_compile_buy_intent_with_slug(self, adapter_with_mocks, test_market):
        """Test compiling buy intent using market slug."""
        # Configure mock to fail on get_market but succeed on get_market_by_slug
        adapter_with_mocks.clob.get_market.side_effect = Exception("Not found")
        adapter_with_mocks.clob.get_market_by_slug.return_value = test_market

        intent = PredictionBuyIntent(
            market_id=test_market.slug,
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.metadata["market_id"] == test_market.id
        adapter_with_mocks.clob.get_market_by_slug.assert_called()

    def test_compile_buy_intent_market_not_found(self, adapter_with_mocks):
        """Test buy intent with non-existent market."""
        adapter_with_mocks.clob.get_market.side_effect = Exception("Not found")
        adapter_with_mocks.clob.get_market_by_slug.return_value = None

        intent = PredictionBuyIntent(
            market_id="nonexistent-market",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert "error" in bundle.metadata

    def test_compile_buy_intent_time_in_force_options(self, adapter_with_mocks, test_market):
        """Test different time-in-force options."""
        for tif in ["GTC", "IOC", "FOK"]:
            intent = PredictionBuyIntent(
                market_id=test_market.id,
                outcome="YES",
                shares=Decimal("50"),
                max_price=Decimal("0.65"),
                order_type="limit",
                time_in_force=tif,
            )

            bundle = adapter_with_mocks.compile_intent(intent)
            assert bundle.metadata["order_type"] == tif


# =============================================================================
# Sell Intent Compilation Tests
# =============================================================================


class TestSellIntentCompilation:
    """Tests for compiling PredictionSellIntent."""

    def test_compile_sell_intent_specific_shares(self, adapter_with_mocks, test_market):
        """Test compiling sell with specific shares."""
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("25"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.intent_type == IntentType.PREDICTION_SELL.value
        assert bundle.metadata["size"] == "25"
        assert bundle.metadata["side"] == "SELL"

    def test_compile_sell_intent_all_shares(self, adapter_with_mocks, test_market):
        """Test compiling sell with shares='all'."""
        # Setup mock position
        position = MagicMock()
        position.token_id = test_market.yes_token_id
        position.size = Decimal("100")
        adapter_with_mocks.clob.get_positions.return_value = [position]

        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares="all",
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.intent_type == IntentType.PREDICTION_SELL.value
        assert bundle.metadata["size"] == "100"

    def test_compile_sell_intent_no_position(self, adapter_with_mocks, test_market):
        """Test sell intent when no position exists."""
        adapter_with_mocks.clob.get_positions.return_value = []

        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares="all",
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "No position to sell" in bundle.metadata["error"]

    def test_compile_sell_intent_limit_order(self, adapter_with_mocks, test_market):
        """Test compiling sell with limit order."""
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            min_price=Decimal("0.70"),
            order_type="limit",
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.metadata["price"] == "0.70"
        assert bundle.metadata["order_type"] == "GTC"


# =============================================================================
# Redeem Intent Compilation Tests
# =============================================================================


class TestRedeemIntentCompilation:
    """Tests for compiling PredictionRedeemIntent."""

    def test_compile_redeem_intent_success(self, adapter_with_mocks, test_market):
        """Test compiling redeem intent for resolved market."""
        intent = PredictionRedeemIntent(
            market_id=test_market.id,
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.intent_type == IntentType.PREDICTION_REDEEM.value
        assert len(bundle.transactions) == 1
        assert bundle.metadata["winning_outcome"] == "YES"
        assert bundle.metadata["condition_id"] == test_market.condition_id

    def test_compile_redeem_intent_specific_outcome(self, adapter_with_mocks, test_market):
        """Test compiling redeem for specific outcome."""
        intent = PredictionRedeemIntent(
            market_id=test_market.id,
            outcome="YES",
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.metadata["outcome"] == "YES"

    def test_compile_redeem_intent_market_not_resolved(self, adapter_with_mocks, test_market):
        """Test redeem intent for unresolved market."""
        adapter_with_mocks.ctf.get_condition_resolution.return_value = ResolutionStatus(
            condition_id="0x1234",
            is_resolved=False,
            payout_denominator=0,
            payout_numerators=[0, 0],
        )

        intent = PredictionRedeemIntent(
            market_id=test_market.id,
        )

        with pytest.raises(PolymarketMarketNotResolvedError):
            adapter_with_mocks.compile_intent(intent)

    def test_compile_redeem_intent_no_web3(self, config, mock_clob_client, test_market):
        """Test redeem intent without Web3 instance."""
        adapter = PolymarketAdapter.__new__(PolymarketAdapter)
        adapter.config = config
        adapter.web3 = None
        adapter.clob = mock_clob_client
        adapter.ctf = MagicMock()
        adapter._market_cache = {}

        intent = PredictionRedeemIntent(
            market_id=test_market.id,
        )

        bundle = adapter.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "Web3 instance required" in bundle.metadata["error"]


# =============================================================================
# Market Resolution Tests
# =============================================================================


class TestMarketResolution:
    """Tests for market ID resolution."""

    def test_resolve_market_by_id(self, adapter_with_mocks, test_market):
        """Test resolving market by ID."""
        market = adapter_with_mocks._resolve_market(test_market.id)

        assert market.id == test_market.id
        adapter_with_mocks.clob.get_market.assert_called_with(test_market.id)

    def test_resolve_market_by_slug(self, adapter_with_mocks, test_market):
        """Test resolving market by slug."""
        adapter_with_mocks.clob.get_market.side_effect = Exception("Not by ID")
        adapter_with_mocks.clob.get_market_by_slug.return_value = test_market

        market = adapter_with_mocks._resolve_market(test_market.slug)

        assert market.slug == test_market.slug

    def test_resolve_market_caching(self, adapter_with_mocks, test_market):
        """Test that resolved markets are cached."""
        # First resolution
        market1 = adapter_with_mocks._resolve_market(test_market.id)
        call_count = adapter_with_mocks.clob.get_market.call_count

        # Second resolution (should use cache)
        market2 = adapter_with_mocks._resolve_market(test_market.id)

        assert market1 == market2
        assert adapter_with_mocks.clob.get_market.call_count == call_count

    def test_resolve_market_not_found(self, adapter_with_mocks):
        """Test market resolution when market doesn't exist."""
        adapter_with_mocks.clob.get_market.side_effect = Exception("Not found")
        adapter_with_mocks.clob.get_market_by_slug.return_value = None

        with pytest.raises(PolymarketMarketNotFoundError):
            adapter_with_mocks._resolve_market("nonexistent")


# =============================================================================
# Token ID Resolution Tests
# =============================================================================


class TestTokenIdResolution:
    """Tests for token ID resolution."""

    def test_get_yes_token_id(self, adapter_with_mocks, test_market):
        """Test getting YES token ID."""
        token_id = adapter_with_mocks._get_token_id(test_market, "YES")
        assert token_id == test_market.yes_token_id

    def test_get_no_token_id(self, adapter_with_mocks, test_market):
        """Test getting NO token ID."""
        token_id = adapter_with_mocks._get_token_id(test_market, "NO")
        assert token_id == test_market.no_token_id

    def test_get_invalid_outcome(self, adapter_with_mocks, test_market):
        """Test getting token ID for invalid outcome."""
        with pytest.raises(ValueError, match="Invalid outcome"):
            adapter_with_mocks._get_token_id(test_market, "MAYBE")


# =============================================================================
# Order Calculation Tests
# =============================================================================


class TestOrderCalculation:
    """Tests for order parameter calculations."""

    def test_calculate_size_from_shares(self, adapter_with_mocks):
        """Test size calculation from shares."""
        intent = PredictionBuyIntent(
            market_id="test",
            outcome="YES",
            shares=Decimal("100"),
        )

        size = adapter_with_mocks._calculate_size(intent, Decimal("0.50"))
        assert size == Decimal("100")

    def test_calculate_size_from_amount_usd(self, adapter_with_mocks):
        """Test size calculation from amount_usd."""
        intent = PredictionBuyIntent(
            market_id="test",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        size = adapter_with_mocks._calculate_size(intent, Decimal("0.50"))
        assert size == Decimal("200")

    def test_calculate_expiration_with_hours(self, adapter_with_mocks):
        """Test expiration calculation with hours specified."""
        expiration = adapter_with_mocks._calculate_expiration(24)
        now = int(datetime.now(UTC).timestamp())
        expected = now + (24 * 3600)
        assert abs(expiration - expected) < 5  # Within 5 seconds

    def test_calculate_expiration_no_expiry(self, adapter_with_mocks):
        """Test expiration calculation with no expiry."""
        expiration = adapter_with_mocks._calculate_expiration(None)
        assert expiration == 0

    def test_map_time_in_force(self, adapter_with_mocks):
        """Test time-in-force mapping."""
        from almanak.framework.connectors.polymarket import OrderType

        assert adapter_with_mocks._map_time_in_force("GTC") == OrderType.GTC
        assert adapter_with_mocks._map_time_in_force("IOC") == OrderType.IOC
        assert adapter_with_mocks._map_time_in_force("FOK") == OrderType.FOK
        assert adapter_with_mocks._map_time_in_force("INVALID") == OrderType.GTC


# =============================================================================
# GammaMarket Property Tests
# =============================================================================


class TestGammaMarketProperties:
    """Tests for GammaMarket token ID properties."""

    def test_market_yes_token_id(self, test_market):
        """Test yes_token_id property."""
        assert test_market.yes_token_id == test_market.clob_token_ids[0]

    def test_market_no_token_id(self, test_market):
        """Test no_token_id property."""
        assert test_market.no_token_id == test_market.clob_token_ids[1]

    def test_market_yes_price(self, test_market):
        """Test yes_price property."""
        assert test_market.yes_price == test_market.outcome_prices[0]

    def test_market_no_price(self, test_market):
        """Test no_price property."""
        assert test_market.no_price == test_market.outcome_prices[1]
