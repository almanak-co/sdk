"""Tests for Polymarket adapter CLOB pre-flight validations (VIB-3140).

The CLOB rejects orders at submission time for:
- Off-tick prices (``breaks minimum tick size rule: <tick>``)
- Sub-$1 BUY notionals (``invalid amount for a marketable BUY order ($X), min size: $1``)
- Excessive price precision (``INVALID_ORDER`` on > 4-decimal prices)

Before this change, ``--dry-run`` silently accepted those intents because the
adapter auto-snapped the price and only revalidated post-sign. These tests
lock the compile path to the live-CLOB error text so strategy authors can
grep for the same strings in both dry-run and production logs, and so
VIB-3141's ``non_retryable`` classifier can pattern-match a single set of
error strings across both environments.

The tests exercise BUY and SELL paths — tick and precision apply to both;
the $1 USD floor is BUY-only.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from almanak.framework.connectors.polymarket import (
    GammaMarket,
    PolymarketAdapter,
    PolymarketConfig,
    SignatureType,
)
from almanak.framework.connectors.polymarket.exceptions import (
    PolymarketInvalidPrecisionError,
    PolymarketInvalidTickSizeError,
    PolymarketMinimumOrderError,
)
from almanak.framework.intents.vocabulary import (
    IntentType,
    PredictionBuyIntent,
    PredictionSellIntent,
)

# =============================================================================
# Fixtures
# =============================================================================


@dataclass
class _StubSignedOrder:
    """Minimal stand-in for a signed order returned by the mocked CLOB client.

    The preflight tests never need real signing; they only assert that the
    adapter either raises (failure modes) or reaches the payload-building
    call (happy paths). ``to_api_payload`` returns a dict so the metadata
    assembly in ``_compile_buy_intent`` / ``_compile_sell_intent`` doesn't
    blow up.
    """

    def to_api_payload(self, owner: str, order_type: str = "GTC") -> dict[str, Any]:
        return {
            "order": {"tokenId": "0xstub", "side": "BUY", "signature": "0x"},
            "owner": owner,
            "orderType": order_type,
        }


@pytest.fixture
def test_private_key() -> str:
    return "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


@pytest.fixture
def test_wallet() -> str:
    return "0xFCAd0B19bB29D4674531d6f115237E16AfCE377c"


@pytest.fixture
def config(test_private_key: str, test_wallet: str) -> PolymarketConfig:
    return PolymarketConfig(
        wallet_address=test_wallet,
        private_key=SecretStr(test_private_key),
        signature_type=SignatureType.EOA,
    )


@pytest.fixture
def test_market() -> GammaMarket:
    """A test market with a 0.01 tick size (the Polymarket default)."""
    return GammaMarket(
        id="12345",
        condition_id="0x" + "ab" * 32,
        question="Will Bitcoin exceed $100k by end of 2025?",
        slug="btc-100k",
        outcomes=["Yes", "No"],
        outcome_prices=[Decimal("0.65"), Decimal("0.35")],
        clob_token_ids=["111", "222"],
        volume=Decimal("1000000"),
        volume_24hr=Decimal("50000"),
        liquidity=Decimal("100000"),
        active=True,
        closed=False,
        enable_order_book=True,
        order_price_min_tick_size=Decimal("0.01"),
        order_min_size=Decimal("5"),
    )


@pytest.fixture
def fine_tick_market() -> GammaMarket:
    """A test market with a 0.001 tick size (for tighter-spread pairs)."""
    return GammaMarket(
        id="67890",
        condition_id="0x" + "cd" * 32,
        question="Will ETH exceed $5k by end of 2025?",
        slug="eth-5k",
        outcomes=["Yes", "No"],
        outcome_prices=[Decimal("0.12"), Decimal("0.88")],
        clob_token_ids=["333", "444"],
        volume=Decimal("500000"),
        volume_24hr=Decimal("25000"),
        liquidity=Decimal("50000"),
        active=True,
        closed=False,
        enable_order_book=True,
        order_price_min_tick_size=Decimal("0.001"),
        order_min_size=Decimal("5"),
    )


@pytest.fixture
def adapter(
    config: PolymarketConfig,
    test_market: GammaMarket,
) -> PolymarketAdapter:
    """Adapter with a mocked client for validation-only checks.

    Pre-flight validation now lives on the adapter itself (not the CLOB client),
    so we only need to mock the market-resolution and position-query methods.
    ``create_and_sign_limit_order`` is stubbed for backwards compatibility but
    is no longer called by the adapter's compile path.
    """
    adapter = PolymarketAdapter.__new__(PolymarketAdapter)
    adapter.config = config
    adapter.wallet_address = config.wallet_address
    adapter.web3 = None
    adapter._market_cache = {}

    mock_clob = MagicMock()
    mock_clob.get_market.return_value = test_market
    mock_clob.get_market_by_slug.return_value = test_market
    mock_clob.get_positions.return_value = []
    mock_clob.create_and_sign_limit_order.return_value = _StubSignedOrder()
    mock_clob.get_or_create_credentials.return_value = MagicMock(api_key="test-api-key")
    mock_clob.credentials.api_key = "test-api-key"

    adapter.client = mock_clob
    adapter.clob = mock_clob
    adapter.ctf = MagicMock()
    return adapter


# =============================================================================
# BUY — Off-Tick Price
# =============================================================================


class TestBuyOffTickPrice:
    """Off-tick ``max_price`` must fail with the live-CLOB error text."""

    def test_buy_off_tick_price_001_market_rejects(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """BUY at 0.123 on a 0.01-tick market must fail at compile.

        Live CLOB emits ``breaks minimum tick size rule: 0.01`` — strategy
        authors grep that exact string. Before VIB-3140 this silently
        snapped to 0.12.
        """
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.123"),
        )

        bundle = adapter.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "breaks minimum tick size rule" in bundle.metadata["error"]
        assert "0.01" in bundle.metadata["error"]
        # The signer must not have been reached — dry-run integrity.
        adapter.clob.create_and_sign_limit_order.assert_not_called()

    def test_buy_tiny_off_tick_price_is_not_tolerance_accepted(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """A price slightly off-grid must still fail on a 0.01-tick market."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.650009"),
        )

        bundle = adapter.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "breaks minimum tick size rule" in bundle.metadata["error"]
        assert "0.01" in bundle.metadata["error"]
        adapter.clob.create_and_sign_limit_order.assert_not_called()

    def test_buy_off_tick_price_0001_market_rejects(
        self,
        adapter: PolymarketAdapter,
        fine_tick_market: GammaMarket,
    ) -> None:
        """BUY at 0.1234 on a 0.001-tick market must fail.

        Off-tick on a fine-tick market — e.g. 0.1234 is not a multiple of
        0.001. Same error string, different tick.
        """
        adapter.clob.get_market.return_value = fine_tick_market
        adapter.clob.get_market_by_slug.return_value = fine_tick_market

        intent = PredictionBuyIntent(
            market_id=fine_tick_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.1234"),
        )

        bundle = adapter.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "breaks minimum tick size rule" in bundle.metadata["error"]
        assert "0.001" in bundle.metadata["error"]
        adapter.clob.create_and_sign_limit_order.assert_not_called()


# =============================================================================
# SELL — Off-Tick Price
# =============================================================================


class TestSellOffTickPrice:
    """Mirror of the BUY off-tick tests on the SELL ``min_price`` side."""

    def test_sell_off_tick_price_rejects(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """SELL at 0.7035 on a 0.01-tick market must fail at compile."""
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("25"),
            min_price=Decimal("0.7035"),
        )

        bundle = adapter.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "breaks minimum tick size rule" in bundle.metadata["error"]
        assert "0.01" in bundle.metadata["error"]
        adapter.clob.create_and_sign_limit_order.assert_not_called()


# =============================================================================
# BUY — Sub-$1 Minimum Order Value
# =============================================================================


class TestBuyBelowMinimumOrderValue:
    """BUY orders below the $1 USD floor must be rejected at compile.

    Live CLOB: ``invalid amount for a marketable BUY order ($X), min size: $1``.
    """

    def test_buy_below_one_dollar_rejects_with_clob_text(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """5 shares × $0.06 = $0.30 fails the $1 floor with exact CLOB text."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("5"),
            max_price=Decimal("0.06"),
        )

        bundle = adapter.compile_intent(intent)

        assert "error" in bundle.metadata
        error = bundle.metadata["error"]
        # Exact live-CLOB phrase (VIB-3141 greps for this).
        assert "invalid amount for a marketable BUY order" in error
        assert "min size: $1" in error

    def test_buy_at_one_dollar_notional_passes(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """Exactly $1 notional must compile cleanly (boundary test)."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("20"),  # 20 × 0.05 = $1.00
            max_price=Decimal("0.05"),
        )

        bundle = adapter.compile_intent(intent)

        assert "error" not in bundle.metadata
        assert bundle.intent_type == IntentType.PREDICTION_BUY.value
        assert "order_request" in bundle.metadata

    def test_sell_not_subject_to_dollar_floor(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """SELL pays in shares — the $1 USD floor does NOT apply.

        A $0.30 SELL notional (5 shares @ $0.06) must compile cleanly on
        the SELL path; the CLOB only enforces the USD floor against BUY
        ``makerAmount``.
        """
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("5"),
            min_price=Decimal("0.06"),
        )

        bundle = adapter.compile_intent(intent)

        assert "error" not in bundle.metadata
        assert bundle.intent_type == IntentType.PREDICTION_SELL.value


# =============================================================================
# Excess-Precision Prices
# =============================================================================


class TestExcessPrecision:
    """Prices with more than 4 decimals must fail at compile.

    Guards against Python-float-to-Decimal bugs (e.g. ``Decimal(0.7)`` yields
    a 17-decimal representation) that otherwise cause
    ``_build_amounts_at_price`` to snap to ``(0, 0)`` and silently submit a
    zero-share order.
    """

    def test_buy_price_with_five_decimals_rejects(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """BUY max_price=0.12345 (5 decimals) must fail the precision check.

        0.12345 also happens to be off-tick on a 0.01-tick market, so the
        tick check fires first; the precision check is exercised via a
        fine-tick market below.
        """
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.12345"),
        )

        bundle = adapter.compile_intent(intent)

        assert "error" in bundle.metadata
        # Tick fires first on a 0.01-tick market; the key property is that
        # the bundle is rejected (strategy author sees a CLOB-style error).
        assert any(
            key in bundle.metadata["error"]
            for key in ("breaks minimum tick size", "INVALID_ORDER")
        )

    def test_buy_price_exceeds_4_decimals_on_fine_tick_market_rejects(
        self,
        adapter: PolymarketAdapter,
        fine_tick_market: GammaMarket,
    ) -> None:
        """On a 0.001-tick market, a 5-decimal price fails precision first.

        0.12345 is off-tick on 0.001 (would require 0.0001 tick), but more
        importantly has 5 fractional digits — exceeds the 4-decimal CLOB
        cap. Either error is acceptable; both signal INVALID_ORDER to the
        author.
        """
        # Use a fine-tick market where 5-decimal prices could in principle
        # be tick-valid on 0.0001, and set the tick to 0.0001 to isolate
        # the precision check.
        fine_tick_market.order_price_min_tick_size = Decimal("0.00001")
        adapter.clob.get_market.return_value = fine_tick_market
        adapter.clob.get_market_by_slug.return_value = fine_tick_market

        intent = PredictionBuyIntent(
            market_id=fine_tick_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.12345"),  # 5 decimals, tick-valid on 0.00001
        )

        bundle = adapter.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "INVALID_ORDER" in bundle.metadata["error"]
        assert "too many decimals" in bundle.metadata["error"]
        adapter.clob.create_and_sign_limit_order.assert_not_called()

    def test_sell_price_exceeds_4_decimals_rejects(
        self,
        adapter: PolymarketAdapter,
        fine_tick_market: GammaMarket,
    ) -> None:
        """Mirror on the SELL side."""
        fine_tick_market.order_price_min_tick_size = Decimal("0.00001")
        adapter.clob.get_market.return_value = fine_tick_market
        adapter.clob.get_market_by_slug.return_value = fine_tick_market

        intent = PredictionSellIntent(
            market_id=fine_tick_market.id,
            outcome="YES",
            shares=Decimal("100"),
            min_price=Decimal("0.12345"),
        )

        bundle = adapter.compile_intent(intent)

        assert "error" in bundle.metadata
        assert "INVALID_ORDER" in bundle.metadata["error"]
        assert "too many decimals" in bundle.metadata["error"]
        adapter.clob.create_and_sign_limit_order.assert_not_called()


# =============================================================================
# Happy Paths — Must Still Compile
# =============================================================================


class TestHappyPaths:
    """Valid BUY and SELL intents (default-market routing) must compile."""

    def test_valid_buy_limit_compiles(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """BUY with tick-aligned max_price and $100 notional compiles."""
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            amount_usd=Decimal("100"),
            max_price=Decimal("0.65"),
            order_type="limit",
            time_in_force="GTC",
        )

        bundle = adapter.compile_intent(intent)

        assert "error" not in bundle.metadata
        assert bundle.intent_type == IntentType.PREDICTION_BUY.value
        assert bundle.metadata["side"] == "BUY"
        assert bundle.metadata["price"] == "0.65"
        assert "order_request" in bundle.metadata
        assert bundle.metadata["order_request"]["side"] == "BUY"

    def test_valid_sell_limit_compiles(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """SELL with tick-aligned min_price compiles."""
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            min_price=Decimal("0.70"),
            order_type="limit",
            time_in_force="GTC",
        )

        bundle = adapter.compile_intent(intent)

        assert "error" not in bundle.metadata
        assert bundle.intent_type == IntentType.PREDICTION_SELL.value
        assert bundle.metadata["side"] == "SELL"
        assert bundle.metadata["price"] == "0.70"
        assert "order_request" in bundle.metadata
        assert bundle.metadata["order_request"]["side"] == "SELL"

    def test_valid_buy_market_subtype_compiles(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """BUY with ``order_type="market"`` (default) still compiles when
        ``max_price`` is tick-aligned.

        The adapter routes market BUYs to LIMIT-IOC for safety (PM Exp 14 /
        VIB-3131). The preflight runs before that routing and must not
        reject valid intents.
        """
        intent = PredictionBuyIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("100"),
            max_price=Decimal("0.65"),
            # order_type defaults to "market"
        )

        bundle = adapter.compile_intent(intent)

        assert "error" not in bundle.metadata
        assert bundle.metadata["order_type"] == "IOC"  # elevated market->limit
        assert "order_request" in bundle.metadata

    def test_valid_sell_market_subtype_compiles(
        self,
        adapter: PolymarketAdapter,
        test_market: GammaMarket,
    ) -> None:
        """Mirror of the BUY market-subtype test on the SELL side."""
        intent = PredictionSellIntent(
            market_id=test_market.id,
            outcome="YES",
            shares=Decimal("50"),
            min_price=Decimal("0.70"),
            # order_type defaults to "market" -> elevated to LIMIT-IOC
        )

        bundle = adapter.compile_intent(intent)

        assert "error" not in bundle.metadata
        assert bundle.metadata["order_type"] == "IOC"
        assert "order_request" in bundle.metadata


# =============================================================================
# Exception Message Surface (smoke tests — VIB-3141 coordination)
# =============================================================================


class TestExceptionMessageSurface:
    """The exception string surface is part of the public contract with
    VIB-3141's ``non_retryable`` classifier. These smoke tests pin the
    exact phrases so a well-meaning refactor of the exception classes
    cannot silently break downstream keyword-matching.
    """

    def test_tick_error_contains_clob_phrase(self) -> None:
        err = PolymarketInvalidTickSizeError(
            price="0.123", tick_size="0.01", nearest_valid="0.12"
        )
        assert "breaks minimum tick size rule" in str(err)
        assert "0.01" in str(err)

    def test_min_order_usd_error_contains_clob_phrase(self) -> None:
        err = PolymarketMinimumOrderError(size="$0.30", minimum="$1")
        assert "invalid amount for a marketable BUY order" in str(err)
        assert "min size: $1" in str(err)

    def test_min_order_shares_error_distinct_from_usd(self) -> None:
        """Share-count floor does NOT use the USD-floor phrase.

        Without the ``$`` prefix the exception emits the legacy
        ``Order size X below minimum Y`` text so log grep filters can
        distinguish the two failure modes.
        """
        err = PolymarketMinimumOrderError(size="3", minimum="5")
        assert "invalid amount for a marketable BUY order" not in str(err)
        assert "below minimum" in str(err)

    def test_precision_error_contains_invalid_order(self) -> None:
        err = PolymarketInvalidPrecisionError(
            field="price", value="0.12345", max_decimals=4
        )
        assert "INVALID_ORDER" in str(err)
        assert "too many decimals" in str(err)
