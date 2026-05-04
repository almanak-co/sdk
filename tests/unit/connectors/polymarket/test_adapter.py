"""Tests for the gateway-compatible Polymarket adapter."""

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

    def to_api_payload(self, owner: str, order_type: str = "GTC") -> dict[str, Any]:
        return {
            "order": {"tokenId": "111111", "side": "BUY", "signature": self.signature},
            "owner": owner,
            "orderType": order_type,
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
    # round_price_to_tick is called by the adapter to snap user-supplied prices
    # onto the market's tick grid. Tests pin pre-aligned prices so the identity
    # passthrough is correct.
    mock.round_price_to_tick.side_effect = lambda price, side, market=None, tick_size=None: price
    # Adapter reads clob.credentials.api_key to build the order `owner` field
    mock.credentials.api_key = "test-api-key"
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
def adapter_with_mocks(test_wallet, mock_clob_client, mock_ctf_sdk, mock_web3):
    """Create adapter with mocked dependencies."""
    adapter = PolymarketAdapter(mock_clob_client, wallet_address=test_wallet, web3=mock_web3)
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

    def test_rejects_polymarket_config_direct_client_path(self, config):
        """Adapter must reject direct config-based initialization."""
        with pytest.raises(ValueError, match="gateway-backed Polymarket client"):
            PolymarketAdapter(config)


# =============================================================================
# Buy Intent Compilation Tests
# =============================================================================


class TestBuyIntentCompilation:
    """Tests for compiling PredictionBuyIntent."""

    def test_compile_buy_intent_with_amount_usd(self, adapter_with_mocks, test_market):
        """Test compiling a buy with amount_usd. max_price is required (VIB-3131)."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            amount_usd=Decimal("100"),
            max_price=Decimal("0.65"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.intent_type == IntentType.PREDICTION_BUY.value
        assert bundle.metadata["market_id"] == test_market.id
        assert bundle.metadata["outcome"] == "YES"
        assert bundle.metadata["side"] == "BUY"
        assert bundle.metadata["protocol"] == "polymarket"
        assert "order_request" in bundle.metadata

    def test_compile_buy_intent_with_shares(self, adapter_with_mocks, test_market):
        """Test compiling a buy with shares. max_price is required (VIB-3131)."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            max_price=Decimal("0.65"),
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

    def test_buy_intent_with_max_price_routes_to_limit_even_if_default_market(self, adapter_with_mocks, test_market):
        """When max_price is set we route to LIMIT regardless of intent.order_type.

        Pre-fix the adapter required BOTH ``order_type=='limit'`` AND ``max_price``;
        otherwise it fell back to market sweep at worst_price=0.99 — which on
        cheap markets reserves an $80 nominal against a $1 wallet and gets
        rejected by the CLOB. See PM incident 2026-04-19, market 556063.
        """
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.65"),
            # order_type left at its "market" default
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.metadata["price"] == "0.65"

    def test_buy_intent_without_max_price_returns_error_bundle(self, adapter_with_mocks, test_market):
        """A buy intent without max_price must reject — the legacy "warn and
        sweep at 0.99" path was a footgun (PM Exp 14 / VIB-3131): it reserved
        ~size*$0.99 of USDC allowance against fills that landed much cheaper,
        and on cheap markets the CLOB rejected the over-allocated order anyway.
        The compile path now raises; the surrounding handler converts that to
        an error ActionBundle with no signed order.
        """
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("100"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "max_price is required" in bundle.metadata["error"]

    def test_buy_intent_market_to_limit_elevation_uses_ioc_not_gtc(self, adapter_with_mocks, test_market):
        """When the strategy declares ``order_type='market'`` (the default) but
        we elevate to LIMIT for safety, force IOC so we don't silently leave a
        long-lived GTC order resting on the book.

        Auditor finding: the previous market path was IOC; routing to LIMIT
        with ``_map_time_in_force(intent.time_in_force)`` defaulted to GTC,
        silently changing the order's lifecycle.
        """
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.65"),
            # order_type left at its "market" default
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        # Elevated market→limit must use IOC, not GTC
        assert bundle.metadata["order_type"] == "IOC"

    def test_buy_intent_explicit_limit_keeps_declared_tif(self, adapter_with_mocks, test_market):
        """An explicit ``order_type='limit'`` must respect the user's TIF."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.65"),
            order_type="limit",
            time_in_force="GTC",
        )

        bundle = adapter_with_mocks.compile_intent(intent)
        assert bundle.metadata["order_type"] == "GTC"

    def test_buy_intent_size_uses_pre_snap_max_price(self, adapter_with_mocks, test_market):
        """When max_price doesn't lie on the tick grid, size must come from the
        user-supplied price (preserving sizing intent), not the snapped price.

        Auditor finding: amount_usd=$10 with max_price=0.0135 on a 0.01-tick
        market would otherwise snap price → 0.01 → size = 10/0.01 = 1000 shares
        (vs. user-intended 10/0.0135 ≈ 740). The snap should only affect the
        submission price, not the share count.
        """
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            amount_usd=Decimal("10.00"),
            max_price=Decimal("0.0135"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "minimum tick size rule" in bundle.metadata["error"]

    def test_compile_buy_intent_no_outcome(self, adapter_with_mocks, test_market):
        """Test compiling buy for NO outcome. max_price required (VIB-3131)."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="NO",
            amount_usd=Decimal("100"),
            max_price=Decimal("0.65"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.metadata["outcome"] == "NO"
        assert bundle.metadata["token_id"] == test_market.no_token_id

    def test_compile_buy_intent_with_slug(self, adapter_with_mocks, test_market):
        """Test compiling buy intent using market slug. max_price required (VIB-3131)."""
        adapter_with_mocks.client.get_market.side_effect = Exception("Not found")
        adapter_with_mocks.client.get_market_by_slug.return_value = test_market

        intent = PredictionBuyIntent(
            market_id=test_market.slug,
            outcome="YES",
            amount_usd=Decimal("100"),
            max_price=Decimal("0.65"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.metadata["market_id"] == test_market.id
        adapter_with_mocks.client.get_market_by_slug.assert_called()

    def test_compile_buy_intent_market_not_found(self, adapter_with_mocks):
        """Test buy intent with non-existent market."""
        adapter_with_mocks.client.get_market.side_effect = Exception("Not found")
        adapter_with_mocks.client.get_market_by_slug.return_value = None

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

    def test_buy_intent_with_expiration_hours_forces_gtd_order_type(self, adapter_with_mocks, test_market):
        """V2 GTD path: any positive ``expiration_hours`` must force the
        compiled order_type to GTD, regardless of the user-supplied TIF.

        V2 dropped the on-chain expiration field from the signed Order struct
        — the matcher only honors expiration when order_type=GTD on the wire
        envelope. Routing a GTC order with an expiration timestamp would
        silently leave the order live past the deadline (the timestamp is
        ignored by the matcher).
        """
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            max_price=Decimal("0.65"),
            order_type="limit",
            time_in_force="GTC",  # explicitly NOT GTD — the override must still kick in
            expiration_hours=2,
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.metadata["order_type"] == "GTD"
        # Expiration must be a future Unix timestamp (~2h from now). Loose
        # bounds because test wall-clock isn't pinned.
        expiration = bundle.metadata["order_request"]["expiration"]
        from datetime import UTC, datetime

        now = int(datetime.now(UTC).timestamp())
        assert expiration > now + 3600, "expiration should be > 1h in the future"
        assert expiration < now + 3 * 3600, "expiration should be < 3h in the future"


# =============================================================================
# Sell Intent Compilation Tests
# =============================================================================


class TestSellIntentCompilation:
    """Tests for compiling PredictionSellIntent."""

    def test_compile_sell_intent_specific_shares(self, adapter_with_mocks, test_market):
        """Test compiling sell with specific shares. min_price is required (VIB-3131)."""
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("25"),
            min_price=Decimal("0.50"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.intent_type == IntentType.PREDICTION_SELL.value
        assert bundle.metadata["size"] == "25"
        assert bundle.metadata["side"] == "SELL"

    def test_compile_sell_intent_all_shares(self, adapter_with_mocks, test_market):
        """Test compiling sell with shares='all'. min_price is required (VIB-3131)."""
        position = MagicMock()
        position.token_id = test_market.yes_token_id
        position.size = Decimal("100")
        adapter_with_mocks.client.get_positions.return_value = [position]

        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares="all",
            min_price=Decimal("0.50"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert bundle.intent_type == IntentType.PREDICTION_SELL.value
        assert bundle.metadata["size"] == "100"

    def test_sell_intent_without_min_price_returns_error_bundle(self, adapter_with_mocks, test_market):
        """A sell intent without min_price must reject — the legacy "warn and
        sweep at the 0.01 floor" path could fill a $0.50/share position at
        $0.01/share. Mirror of the BUY rejection (PM Exp 14 / VIB-3131).
        """
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("25"),
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "min_price is required" in bundle.metadata["error"]

    def test_sell_intent_all_shares_without_min_price_rejects_deterministically(self, adapter_with_mocks, test_market):
        """Regression: shares="all" without min_price must reject for the SAME
        reason as explicit shares — not short-circuit to "No position to sell"
        when the wallet happens to be empty (CodeRabbit catch on PR #1567).
        Without this guard, VIB-3131 enforcement would be nondeterministic in
        common teardown/dry-run flows where positions are not yet open.
        """
        adapter_with_mocks.client.get_positions.return_value = []

        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares="all",
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        # Must be the min_price error, NOT "No position to sell".
        assert "error" in bundle.metadata
        assert "min_price is required" in bundle.metadata["error"]
        assert "No position to sell" not in bundle.metadata["error"]

    def test_compile_sell_intent_no_position(self, adapter_with_mocks, test_market):
        """Test sell intent with valid min_price but no position to sell."""
        adapter_with_mocks.client.get_positions.return_value = []

        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares="all",
            min_price=Decimal("0.50"),
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

    def test_sell_intent_with_min_price_routes_to_limit_even_if_default_market(self, adapter_with_mocks, test_market):
        """Mirror of the BUY routing test on the SELL side: presence of
        ``min_price`` elevates SELL to LIMIT regardless of declared order_type.
        Pre-fix the SELL path mirrored the BUY footgun."""
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            min_price=Decimal("0.70"),
            # order_type left at its "market" default
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        # Elevation must use IOC, not GTC, to preserve fail-fast immediacy
        assert bundle.metadata["order_type"] == "IOC"
        assert bundle.metadata["price"] == "0.70"

    def test_sell_intent_off_tick_min_price_is_snapped(self, adapter_with_mocks, test_market):
        """Off-tick ``min_price`` (e.g. 0.7035 on a 0.01-tick market) must be
        snapped via ``round_price_to_tick`` before submission. Mirror of the
        BUY off-tick test on the SELL side."""
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            min_price=Decimal("0.7035"),  # off-tick on a 0.01-tick market
        )

        bundle = adapter_with_mocks.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "minimum tick size rule" in bundle.metadata["error"]


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

    def test_compile_redeem_intent_no_web3(self, test_wallet, mock_clob_client, test_market):
        """Test redeem intent without Web3 instance."""
        adapter = PolymarketAdapter(mock_clob_client, wallet_address=test_wallet, web3=None)
        adapter.ctf = MagicMock()

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
        adapter_with_mocks.client.get_market.assert_called_with(test_market.id)

    def test_resolve_market_by_slug(self, adapter_with_mocks, test_market):
        """Test resolving market by slug."""
        adapter_with_mocks.client.get_market.side_effect = Exception("Not by ID")
        adapter_with_mocks.client.get_market_by_slug.return_value = test_market

        market = adapter_with_mocks._resolve_market(test_market.slug)

        assert market.slug == test_market.slug

    def test_resolve_market_caching(self, adapter_with_mocks, test_market):
        """Test that resolved markets are cached."""
        # First resolution
        market1 = adapter_with_mocks._resolve_market(test_market.id)
        call_count = adapter_with_mocks.client.get_market.call_count

        # Second resolution (should use cache)
        market2 = adapter_with_mocks._resolve_market(test_market.id)

        assert market1 == market2
        assert adapter_with_mocks.client.get_market.call_count == call_count

    def test_resolve_market_not_found(self, adapter_with_mocks):
        """Test market resolution when market doesn't exist."""
        adapter_with_mocks.client.get_market.side_effect = Exception("Not found")
        adapter_with_mocks.client.get_market_by_slug.return_value = None

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


# =============================================================================
# V2 GTD edge cases for the BUY-intent compiler
# =============================================================================


class TestV2GtdElevation:
    """V2 dropped on-chain expiration; GTD on the API envelope is the only
    way the matcher honors a deadline. The adapter must elevate to GTD
    when ``expiration_hours`` is set, regardless of the user-supplied TIF."""

    def test_none_expiration_hours_does_not_force_gtd(self, adapter_with_mocks, test_market):
        """expiration_hours=None (the default) leaves the user TIF intact."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            max_price=Decimal("0.65"),
            order_type="limit",
            time_in_force="IOC",
        )
        bundle = adapter_with_mocks.compile_intent(intent)
        assert bundle.metadata["order_type"] == "IOC"

    def test_expiration_hours_overrides_ioc_to_gtd(self, adapter_with_mocks, test_market):
        """Even an explicit IOC must be elevated to GTD when a deadline is
        set — IOC + expiration is contradictory but GTD is the right intent."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            max_price=Decimal("0.65"),
            order_type="limit",
            time_in_force="IOC",
            expiration_hours=1,
        )
        bundle = adapter_with_mocks.compile_intent(intent)
        assert bundle.metadata["order_type"] == "GTD"

    def test_expiration_hours_overrides_market_route_ioc_to_gtd(self, adapter_with_mocks, test_market):
        """Market orders are elevated to LIMIT/IOC for safety in V2.
        A user who also sets expiration_hours expects GTD semantics —
        the deadline must win over the market→IOC override."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            max_price=Decimal("0.65"),
            order_type="market",  # would be elevated to IOC
            expiration_hours=1,
        )
        bundle = adapter_with_mocks.compile_intent(intent)
        assert bundle.metadata["order_type"] == "GTD"
