"""Tests for CLOB Action Handler.

This module tests the ClobActionHandler class which handles off-chain
CLOB order execution for Polymarket.

Tests cover:
- can_handle() detection logic
- execute() order submission
- get_status() order status retrieval
- cancel() order cancellation
- Error handling and edge cases
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.polymarket.clob_handler import ClobActionHandler  # VIB-4989: connector copy (CRAP coverage)
from almanak.framework.execution.clob_handler import (
    ClobExecutionResult,
    ClobFill,
    ClobOrderState,
    ClobOrderStatus,
)
from almanak.framework.models.reproduction_bundle import ActionBundle

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_clob_client():
    """Create a mock ClobClient.

    Pre-configured with a non-empty ``get_markets`` result so the V2 path
    in ``ClobActionHandler.execute`` (which looks up the GammaMarket from
    the token_id before calling ``create_and_post_order``) finds a market.
    """
    client = MagicMock()
    # Provide a stand-in GammaMarket so the token_id -> market lookup
    # succeeds in ClobActionHandler.execute. Tests that need to assert
    # behaviour on a missing market should override this on the fixture.
    market = MagicMock()
    market.id = "market-456"
    market.condition_id = "0xcondition"
    client.get_markets.return_value = [market]
    return client


@pytest.fixture
def handler(mock_clob_client):
    """Create a ClobActionHandler with mocked client."""
    return ClobActionHandler(clob_client=mock_clob_client)


@pytest.fixture
def handler_no_client():
    """Create a ClobActionHandler without a client."""
    return ClobActionHandler(clob_client=None)


@pytest.fixture
def valid_clob_bundle():
    """Create a valid V2 CLOB order bundle (order_request, gateway-signed)."""
    return ActionBundle(
        intent_type="PREDICTION_BUY",
        transactions=[],  # CLOB orders have no on-chain transactions
        metadata={
            "protocol": "polymarket",
            "order_request": {
                "token_id": "12345",
                "side": "BUY",
                "price": "0.50",
                "size": "100",
                "time_in_force": "GTC",
                "expiration": 0,
            },
            "side": "BUY",
            "size": "100",
            "price": "0.50",
            # VIB-3218: production adapter always sets order_type on the
            # bundle. Omitting it from the fixture would let GTC tests
            # pass via the ``order_type_hint == ""`` branch and mask
            # regressions in the IOC/FOK zero-fill demotion.
            "order_type": "GTC",
            "intent_id": "test-intent-123",
        },
    )


@pytest.fixture
def valid_order_request_bundle():
    """Create a valid gateway-backed CLOB order_request bundle (IOC variant)."""
    return ActionBundle(
        intent_type="PREDICTION_BUY",
        transactions=[],
        metadata={
            "protocol": "polymarket",
            "intent_id": "test-intent-123",
            "order_request": {
                "token_id": "12345",
                "side": "BUY",
                "price": "0.50",
                "size": "100",
                "time_in_force": "IOC",
                "expiration": 0,
            },
        },
    )


@pytest.fixture
def on_chain_bundle():
    """Create an on-chain transaction bundle (not CLOB)."""
    return ActionBundle(
        intent_type="SWAP",
        transactions=[{"to": "0x1234...", "data": "0xabcd..."}],
        metadata={"protocol": "uniswap"},
    )


@pytest.fixture
def mock_order_response():
    """Create a mock OrderResponse (unfilled, on-book)."""
    response = MagicMock()
    response.order_id = "order-123"
    response.status = MagicMock()
    response.status.value = "LIVE"
    response.market = "market-456"
    response.side = "BUY"
    response.price = Decimal("0.50")
    response.size = Decimal("100")
    response.filled_size = Decimal("0")
    response.avg_fill_price = None
    return response


@pytest.fixture
def mock_open_order():
    """Create a mock OpenOrder."""
    order = MagicMock()
    order.order_id = "order-123"
    order.market = "token-789"
    order.side = "BUY"
    order.price = Decimal("0.50")
    order.size = Decimal("100")
    order.filled_size = Decimal("25")
    order.created_at = datetime(2026, 1, 25, 12, 0, 0, tzinfo=UTC)
    return order


# =============================================================================
# can_handle() Tests
# =============================================================================


class TestCanHandle:
    """Tests for can_handle() method."""

    def test_handles_valid_clob_bundle(self, handler, valid_clob_bundle):
        """Test that handler accepts valid CLOB bundles."""
        assert handler.can_handle(valid_clob_bundle) is True

    def test_rejects_non_polymarket_bundle(self, handler, on_chain_bundle):
        """Test that handler rejects non-Polymarket bundles."""
        assert handler.can_handle(on_chain_bundle) is False

    def test_rejects_bundle_with_transactions(self, handler, valid_clob_bundle):
        """Test that handler rejects bundles with on-chain transactions."""
        valid_clob_bundle.transactions = [{"to": "0x123", "data": "0xabc"}]
        assert handler.can_handle(valid_clob_bundle) is False

    def test_rejects_bundle_without_order_request(self, handler, valid_clob_bundle):
        """Test that handler rejects bundles without order_request."""
        del valid_clob_bundle.metadata["order_request"]
        assert handler.can_handle(valid_clob_bundle) is False

    def test_rejects_bundle_with_only_legacy_order_payload(self, handler, valid_clob_bundle):
        """V1 ``order_payload`` alone must NOT be accepted (regression guard).

        VIB-3696: the handler used to accept ``order_payload`` and call
        ``submit_order_payload`` on the gateway-routed client, which has
        no such method -> AttributeError at runtime. The dead branch is
        removed; bundles that only carry the legacy key must be rejected.
        """
        del valid_clob_bundle.metadata["order_request"]
        valid_clob_bundle.metadata["order_payload"] = {"order": {}, "signature": "0x"}
        assert handler.can_handle(valid_clob_bundle) is False

    def test_rejects_wrong_protocol(self, handler, valid_clob_bundle):
        """Test that handler rejects bundles with wrong protocol."""
        valid_clob_bundle.metadata["protocol"] = "uniswap"
        assert handler.can_handle(valid_clob_bundle) is False

    def test_rejects_missing_protocol(self, handler, valid_clob_bundle):
        """Test that handler rejects bundles without protocol."""
        del valid_clob_bundle.metadata["protocol"]
        assert handler.can_handle(valid_clob_bundle) is False


# =============================================================================
# execute() Tests
# =============================================================================


class TestExecute:
    """Tests for execute() method."""

    def test_execute_success(self, handler, mock_clob_client, valid_clob_bundle, mock_order_response):
        """Test successful order submission."""
        mock_clob_client.create_and_post_order.return_value = mock_order_response

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True
        assert result.order_id == "order-123"
        assert result.status == ClobOrderStatus.LIVE
        assert result.error is None
        # V2: handler always routes through create_and_post_order; the
        # legacy submit_order_payload branch was removed in VIB-3696.
        mock_clob_client.create_and_post_order.assert_called_once()
        # Legacy V1 method must never be reached even if the mock has it.
        mock_clob_client.submit_order_payload.assert_not_called()

    def test_execute_order_request_calls_create_and_post_order(
        self, handler, mock_clob_client, valid_order_request_bundle, mock_order_response
    ):
        """Gateway-backed bundles must use create_and_post_order()."""
        valid_order_request_bundle.metadata["order_request"]["time_in_force"] = "GTC"
        mock_clob_client.create_and_post_order.return_value = mock_order_response

        result = asyncio.run(handler.execute(valid_order_request_bundle))

        assert result.success is True
        mock_clob_client.create_and_post_order.assert_called_once_with(
            token_id="12345",
            price=Decimal("0.50"),
            size=Decimal("100"),
            side="BUY",
            market=mock_clob_client.get_markets.return_value[0],
            time_in_force="GTC",
            expiration=0,
        )
        mock_clob_client.submit_order_payload.assert_not_called()

    def test_execute_resolves_market_from_token_id(
        self, handler, mock_clob_client, valid_clob_bundle, mock_order_response
    ):
        """The handler must look up the market that owns the order's
        ``token_id``, not just pick "the first market". Regression: V2
        signs against neg-risk vs binary CTF V2 based on
        ``market.neg_risk``, so picking the wrong market silently corrupts
        the signature path."""
        # Build the MarketFilters that the handler is expected to construct
        # from the bundle and use to look up the market via the SDK.
        from almanak.connectors.polymarket import MarketFilters

        # Two distinct markets — the handler must pass the bundle's token_id
        # in the filter so the SDK returns the matching one.
        owning_market = MagicMock()
        owning_market.id = "market-OWNING-12345"
        owning_market.condition_id = "0xowning"

        def get_markets_by_filter(filters: MarketFilters):
            assert filters.clob_token_ids == ["12345"], (
                f"expected clob_token_ids=['12345'] from bundle's order_request, got {filters.clob_token_ids}"
            )
            assert filters.limit == 1
            return [owning_market]

        mock_clob_client.get_markets.side_effect = get_markets_by_filter
        mock_clob_client.create_and_post_order.return_value = mock_order_response

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True
        # The submission must use the SPECIFIC market we returned for the
        # bundle's token_id, not some sibling. This passes if and only if
        # ``execute`` resolves the market via token_id-keyed lookup before
        # signing.
        mock_clob_client.create_and_post_order.assert_called_once()
        call_kwargs = mock_clob_client.create_and_post_order.call_args.kwargs
        assert call_kwargs["market"] is owning_market

    def test_execute_without_client(self, handler_no_client, valid_clob_bundle):
        """Test that execute fails gracefully without client."""
        result = asyncio.run(handler_no_client.execute(valid_clob_bundle))

        assert result.success is False
        assert result.error == "CLOB client not configured"
        assert result.status == ClobOrderStatus.PENDING

    def test_execute_invalid_bundle(self, handler, on_chain_bundle):
        """Test that execute rejects invalid bundles."""
        result = asyncio.run(handler.execute(on_chain_bundle))

        assert result.success is False
        assert result.error == "Bundle is not a CLOB order"

    def test_execute_api_error(self, handler, mock_clob_client, valid_clob_bundle):
        """Test handling of API errors."""
        mock_clob_client.create_and_post_order.side_effect = Exception("API rate limit exceeded")

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is False
        assert result.status == ClobOrderStatus.FAILED
        assert "API rate limit exceeded" in result.error

    def test_execute_maps_matched_status(self, handler, mock_clob_client, valid_clob_bundle):
        """Test that MATCHED status is properly mapped."""
        mock_response = MagicMock()
        mock_response.order_id = "order-456"
        mock_response.status = MagicMock()
        mock_response.status.value = "MATCHED"
        # VIB-3218: _classify_status reads filled_size / avg_fill_price; give
        # the mock realistic values so Decimal comparisons work.
        mock_response.filled_size = Decimal("100")
        mock_response.avg_fill_price = Decimal("0.65")
        mock_clob_client.create_and_post_order.return_value = mock_response

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True
        assert result.status == ClobOrderStatus.MATCHED

    def test_execute_includes_submitted_at(self, handler, mock_clob_client, valid_clob_bundle, mock_order_response):
        """Test that result includes submission timestamp."""
        mock_clob_client.create_and_post_order.return_value = mock_order_response

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.submitted_at is not None
        assert isinstance(result.submitted_at, datetime)

    # ========================================================================
    # VIB-3218: accept-vs-fill distinction
    # ========================================================================

    def test_gtc_on_book_reports_zero_fill_not_matched(
        self, handler, mock_clob_client, valid_clob_bundle, mock_order_response
    ):
        """GTC that rests on the book returns success=True but filled_size=0."""
        mock_clob_client.create_and_post_order.return_value = mock_order_response
        # valid_clob_bundle.metadata["order_type"] is GTC by payload

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True  # API accepted the submission
        assert result.filled_size == Decimal("0")  # But nothing filled
        assert result.status == ClobOrderStatus.LIVE
        assert result.requested_size == Decimal("100")

    def test_ioc_unmatched_reports_failed_not_live(self, handler, mock_clob_client, valid_clob_bundle):
        """IOC order with zero fills on a LIVE response is FAILED + success=False.

        Post-audit (Codex P1): the classifier demotes LIVE+filled=0 on IOC to
        FAILED, and ``execute`` then sets ``success=False`` so the runner does
        NOT call ``on_intent_executed(success=True, ...)`` on an unmatched IOC.
        """
        mock_response = MagicMock()
        mock_response.order_id = "order-ioc-unmatched"
        mock_response.status = MagicMock()
        mock_response.status.value = "LIVE"
        mock_response.filled_size = Decimal("0")
        mock_response.avg_fill_price = None
        mock_clob_client.create_and_post_order.return_value = mock_response
        valid_clob_bundle.metadata["order_type"] = "IOC"
        valid_clob_bundle.metadata["order_request"]["time_in_force"] = "IOC"

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is False  # FAILED classification flows into success
        assert result.status == ClobOrderStatus.FAILED
        assert result.filled_size == Decimal("0")
        assert result.error is not None

    def test_order_request_ioc_zero_fill_uses_nested_order_type_and_size(
        self, handler, mock_clob_client, valid_order_request_bundle
    ):
        """IOC order_request bundles must classify zero-fill as FAILED."""
        mock_response = MagicMock()
        mock_response.order_id = "order-ioc-unmatched"
        mock_response.status = MagicMock()
        mock_response.status.value = "LIVE"
        mock_response.filled_size = Decimal("0")
        mock_response.avg_fill_price = None
        mock_clob_client.create_and_post_order.return_value = mock_response

        result = asyncio.run(handler.execute(valid_order_request_bundle))

        assert result.success is False
        assert result.status == ClobOrderStatus.FAILED
        assert result.requested_size == Decimal("100")
        assert result.error is not None

    def test_partial_ioc_fill_is_terminal_matched(self, handler, mock_clob_client, valid_clob_bundle):
        """Partial IOC fill classifies as MATCHED (terminal), NOT PARTIALLY_FILLED.

        CodeRabbit #1611 round 1 (Major): IOC/FOK never rest on the book, so
        a partial fill is the FINAL state. Classifying it as PARTIALLY_FILLED
        (which ``ClobOrderState.is_open`` treats as open) would keep the
        order in the live-order set even though no additional fills will
        ever arrive. For IOC/FOK, any non-zero fill -> MATCHED (terminal).
        The actual fill amount remains on ``filled_size`` for partial-fill
        detection at the strategy level.

        For a GTC partial fill, PARTIALLY_FILLED is still the correct
        classification (see the ``_determine_order_status`` path).
        """
        mock_response = MagicMock()
        mock_response.order_id = "order-partial"
        mock_response.status = MagicMock()
        mock_response.status.value = "LIVE"
        mock_response.filled_size = Decimal("10")
        mock_response.avg_fill_price = Decimal("0.51")
        mock_clob_client.create_and_post_order.return_value = mock_response
        valid_clob_bundle.metadata["order_type"] = "IOC"
        valid_clob_bundle.metadata["order_request"]["time_in_force"] = "IOC"

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True
        assert result.filled_size == Decimal("10")
        assert result.requested_size == Decimal("100")
        assert result.status == ClobOrderStatus.MATCHED
        # ClobOrderState.is_terminal must be true for IOC/FOK partial fills.
        terminal_state = ClobOrderState(
            order_id="x",
            market_id="x",
            token_id="x",
            side="BUY",
            status=result.status,
            price=Decimal("0.5"),
            size=Decimal("100"),
            filled_size=result.filled_size,
        )
        assert terminal_state.is_terminal is True
        assert terminal_state.is_open is False
        assert result.avg_fill_price == Decimal("0.51")

    def test_partial_gtc_fill_still_partially_filled(self, handler, mock_clob_client, valid_clob_bundle):
        """Partial GTC fill remains PARTIALLY_FILLED (the order is still resting).

        Counterpart to ``test_partial_ioc_fill_is_terminal_matched`` -- GTC
        partial fills are NOT terminal, so the classifier must keep using
        PARTIALLY_FILLED there.
        """
        mock_response = MagicMock()
        mock_response.order_id = "order-partial-gtc"
        mock_response.status = MagicMock()
        mock_response.status.value = "LIVE"
        mock_response.filled_size = Decimal("10")
        mock_response.avg_fill_price = Decimal("0.51")
        mock_clob_client.create_and_post_order.return_value = mock_response
        # Fixture default is GTC (we set it explicitly for clarity).
        valid_clob_bundle.metadata["order_type"] = "GTC"
        valid_clob_bundle.metadata["order_request"]["time_in_force"] = "GTC"

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True
        assert result.status == ClobOrderStatus.PARTIALLY_FILLED
        assert result.filled_size == Decimal("10")

    def test_to_prediction_fill_preserves_fill_state(self, handler, mock_clob_client, valid_clob_bundle):
        """ClobExecutionResult.to_prediction_fill() surfaces filled vs requested."""
        mock_response = MagicMock()
        mock_response.order_id = "order-partial"
        mock_response.status = MagicMock()
        mock_response.status.value = "MATCHED"
        mock_response.filled_size = Decimal("25")
        mock_response.avg_fill_price = Decimal("0.48")
        mock_clob_client.create_and_post_order.return_value = mock_response
        valid_clob_bundle.metadata["order_type"] = "IOC"
        valid_clob_bundle.metadata["order_request"]["time_in_force"] = "IOC"

        result = asyncio.run(handler.execute(valid_clob_bundle))
        fill = result.to_prediction_fill()

        assert fill is not None
        assert fill.filled_shares == Decimal("25")
        assert fill.requested_shares == Decimal("100")
        assert fill.avg_fill_price == Decimal("0.48")
        assert fill.is_partial is True
        assert fill.is_fully_filled is False
        assert fill.is_filled is True

    def test_to_prediction_fill_returns_none_without_requested_size(
        self, handler, mock_clob_client, valid_clob_bundle, mock_order_response
    ):
        """Bundles with no size hint anywhere produce no PredictionFill.

        In production V2 every compiled bundle has size in both the top-level
        metadata and the nested order_request, so requested_size is always
        populated. This test exercises the defensive path where neither is
        present (legacy / hand-built / partially-populated bundle).
        """
        mock_clob_client.create_and_post_order.return_value = mock_order_response
        del valid_clob_bundle.metadata["size"]
        # Also drop size from order_request so _parse_decimal returns None.
        # We still need a numeric size for create_and_post_order, so swap to
        # a request bundle that omits size and has the handler fail-out
        # before the API call instead.
        valid_clob_bundle.metadata["order_request"].pop("size", None)

        result = asyncio.run(handler.execute(valid_clob_bundle))

        # Without size, the handler raises ValueError ("missing required
        # price or size fields") and routes to the FAILED exception path —
        # requested_size remains None and to_prediction_fill() returns None.
        assert result.success is False
        assert result.status == ClobOrderStatus.FAILED
        assert result.requested_size is None
        assert result.to_prediction_fill() is None

    def test_string_filled_size_is_normalized_not_routed_to_failure(self, handler, mock_clob_client, valid_clob_bundle):
        """CodeRabbit #1611 round 2 (Major): non-Decimal response numerics.

        If a connector returns ``filled_size`` as a string (or any connector
        test double forgets to coerce), the raw ``Decimal > int`` comparison
        in ``_classify_status`` would raise TypeError and silently route the
        submission to the ``except`` branch -> ``success=False, FAILED``.
        The handler must coerce through ``_parse_decimal`` before comparison.
        """
        mock_response = MagicMock()
        mock_response.order_id = "order-strings"
        mock_response.status = MagicMock()
        mock_response.status.value = "MATCHED"
        mock_response.filled_size = "100"  # string, not Decimal
        mock_response.avg_fill_price = "0.47"  # string, not Decimal
        mock_clob_client.create_and_post_order.return_value = mock_response

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True
        assert result.status == ClobOrderStatus.MATCHED
        assert result.filled_size == Decimal("100")
        assert result.avg_fill_price == Decimal("0.47")

    def test_none_filled_size_is_treated_as_zero(self, handler, mock_clob_client, valid_clob_bundle):
        """``None`` filled_size coerces to 0 without raising TypeError."""
        mock_response = MagicMock()
        mock_response.order_id = "order-none-fill"
        mock_response.status = MagicMock()
        mock_response.status.value = "LIVE"
        mock_response.filled_size = None
        mock_response.avg_fill_price = None
        mock_clob_client.create_and_post_order.return_value = mock_response
        # Default GTC in fixture -> None fill is consistent with resting order
        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True
        assert result.filled_size == Decimal("0")
        assert result.avg_fill_price is None
        assert result.status == ClobOrderStatus.LIVE

    def test_fok_unmatched_reports_failed_and_success_false(self, handler, mock_clob_client, valid_clob_bundle):
        """FOK that fails to match must surface status=FAILED AND success=False.

        Codex P1 / pr-auditor Blocker #3: a classifier-demoted FAILED status
        with ``success=True`` still rode the happy path through StrategyRunner
        (ledger entry, timeline event, on_intent_executed(success=True)).
        Requiring ``success=False`` here locks the contract.
        """
        mock_response = MagicMock()
        mock_response.order_id = "order-fok"
        mock_response.status = MagicMock()
        mock_response.status.value = "LIVE"
        mock_response.filled_size = Decimal("0")
        mock_response.avg_fill_price = None
        mock_clob_client.create_and_post_order.return_value = mock_response
        valid_clob_bundle.metadata["order_type"] = "FOK"
        valid_clob_bundle.metadata["order_request"]["time_in_force"] = "FOK"

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.status == ClobOrderStatus.FAILED
        assert result.success is False
        assert result.error is not None and "failed" in result.error.lower()

    def test_matched_status_preserved_when_requested_size_unknown(self, handler, mock_clob_client, valid_clob_bundle):
        """API MATCHED + filled_size>0 must stay MATCHED even if requested_size is None.

        CodeRabbit I5: downgrading a terminal MATCHED to PARTIALLY_FILLED (via
        the requested_size-vs-filled_size comparison) would leave downstream
        treating a completed order as still open.
        """
        mock_response = MagicMock()
        mock_response.order_id = "order-match-noreq"
        mock_response.status = MagicMock()
        mock_response.status.value = "MATCHED"
        mock_response.filled_size = Decimal("50")
        mock_response.avg_fill_price = Decimal("0.60")
        mock_clob_client.create_and_post_order.return_value = mock_response
        # No size metadata -> requested_size is None
        del valid_clob_bundle.metadata["size"]

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.status == ClobOrderStatus.MATCHED
        assert result.success is True
        assert result.filled_size == Decimal("50")


# =============================================================================
# get_status() Tests
# =============================================================================


class TestGetStatus:
    """Tests for get_status() method."""

    def test_get_status_success(self, handler, mock_clob_client, mock_open_order):
        """Test successful status retrieval."""
        mock_clob_client.get_order.return_value = mock_open_order

        result = asyncio.run(handler.get_status("order-123"))

        assert result is not None
        assert result.order_id == "order-123"
        assert result.market_id == "token-789"
        assert result.side == "BUY"
        assert result.price == Decimal("0.50")
        assert result.size == Decimal("100")
        assert result.filled_size == Decimal("25")
        assert result.status == ClobOrderStatus.PARTIALLY_FILLED
        mock_clob_client.get_order.assert_called_once_with("order-123")

    def test_get_status_order_not_found(self, handler, mock_clob_client):
        """Test status when order not found."""
        mock_clob_client.get_order.return_value = None

        result = asyncio.run(handler.get_status("nonexistent-order"))

        assert result is None

    def test_get_status_without_client(self, handler_no_client):
        """Test status retrieval without client."""
        result = asyncio.run(handler_no_client.get_status("order-123"))

        assert result is None

    def test_get_status_api_error(self, handler, mock_clob_client):
        """Test status retrieval on API error."""
        mock_clob_client.get_order.side_effect = Exception("Connection timeout")

        result = asyncio.run(handler.get_status("order-123"))

        assert result is None

    def test_get_status_fully_filled(self, handler, mock_clob_client, mock_open_order):
        """Test status for fully filled order."""
        mock_open_order.filled_size = Decimal("100")  # Same as size
        mock_clob_client.get_order.return_value = mock_open_order

        result = asyncio.run(handler.get_status("order-123"))

        assert result.status == ClobOrderStatus.MATCHED

    def test_get_status_unfilled(self, handler, mock_clob_client, mock_open_order):
        """Test status for unfilled order."""
        mock_open_order.filled_size = Decimal("0")
        mock_clob_client.get_order.return_value = mock_open_order

        result = asyncio.run(handler.get_status("order-123"))

        assert result.status == ClobOrderStatus.LIVE


# =============================================================================
# cancel() Tests
# =============================================================================


class TestCancel:
    """Tests for cancel() method."""

    def test_cancel_success(self, handler, mock_clob_client):
        """Test successful order cancellation."""
        mock_clob_client.cancel_order.return_value = True

        result = asyncio.run(handler.cancel("order-123"))

        assert result is True
        mock_clob_client.cancel_order.assert_called_once_with("order-123")

    def test_cancel_failure(self, handler, mock_clob_client):
        """Test failed order cancellation."""
        mock_clob_client.cancel_order.return_value = False

        result = asyncio.run(handler.cancel("order-123"))

        assert result is False

    def test_cancel_without_client(self, handler_no_client):
        """Test cancellation without client."""
        result = asyncio.run(handler_no_client.cancel("order-123"))

        assert result is False

    def test_cancel_api_error(self, handler, mock_clob_client):
        """Test cancellation on API error."""
        mock_clob_client.cancel_order.side_effect = Exception("Order not found")

        result = asyncio.run(handler.cancel("order-123"))

        assert result is False


# =============================================================================
# Data Class Tests
# =============================================================================


class TestClobOrderState:
    """Tests for ClobOrderState dataclass."""

    def test_is_open_for_live_order(self):
        """Test is_open returns True for live orders."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.LIVE,
            price=Decimal("0.50"),
            size=Decimal("100"),
        )
        assert state.is_open is True

    def test_is_open_for_matched_order(self):
        """Test is_open returns False for matched orders."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.MATCHED,
            price=Decimal("0.50"),
            size=Decimal("100"),
        )
        assert state.is_open is False

    def test_is_terminal_for_cancelled_order(self):
        """Test is_terminal returns True for cancelled orders."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.CANCELLED,
            price=Decimal("0.50"),
            size=Decimal("100"),
        )
        assert state.is_terminal is True

    def test_fill_percentage_calculation(self):
        """Test fill percentage calculation."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.PARTIALLY_FILLED,
            price=Decimal("0.50"),
            size=Decimal("100"),
            filled_size=Decimal("25"),
        )
        assert state.fill_percentage == 25.0

    def test_to_dict_serialization(self):
        """Test to_dict produces valid dictionary."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.LIVE,
            price=Decimal("0.50"),
            size=Decimal("100"),
        )
        result = state.to_dict()

        assert result["order_id"] == "order-1"
        assert result["status"] == "live"
        assert result["price"] == "0.50"

    def test_from_dict_deserialization(self):
        """Test from_dict creates valid state."""
        data = {
            "order_id": "order-1",
            "market_id": "market-1",
            "token_id": "token-1",
            "side": "BUY",
            "status": "live",
            "price": "0.50",
            "size": "100",
            "filled_size": "0",
            "fills": [],
            "submitted_at": "2026-01-25T12:00:00+00:00",
            "updated_at": "2026-01-25T12:00:00+00:00",
        }
        state = ClobOrderState.from_dict(data)

        assert state.order_id == "order-1"
        assert state.status == ClobOrderStatus.LIVE
        assert state.price == Decimal("0.50")


class TestClobFill:
    """Tests for ClobFill dataclass."""

    def test_to_dict_serialization(self):
        """Test to_dict produces valid dictionary."""
        fill = ClobFill(
            fill_id="fill-1",
            price=Decimal("0.50"),
            size=Decimal("25"),
            fee=Decimal("0.01"),
            timestamp=datetime(2026, 1, 25, 12, 0, 0, tzinfo=UTC),
        )
        result = fill.to_dict()

        assert result["fill_id"] == "fill-1"
        assert result["price"] == "0.50"
        assert result["size"] == "25"
        assert result["fee"] == "0.01"


class TestClobExecutionResult:
    """Tests for ClobExecutionResult dataclass."""

    def test_success_result(self):
        """Test creating a success result."""
        result = ClobExecutionResult(
            success=True,
            order_id="order-123",
            status=ClobOrderStatus.LIVE,
        )
        assert result.success is True
        assert result.order_id == "order-123"
        assert result.error is None

    def test_failure_result(self):
        """Test creating a failure result."""
        result = ClobExecutionResult(
            success=False,
            status=ClobOrderStatus.FAILED,
            error="Insufficient balance",
        )
        assert result.success is False
        assert result.error == "Insufficient balance"

    def test_to_dict_serialization(self):
        """Test to_dict produces valid dictionary."""
        result = ClobExecutionResult(
            success=True,
            order_id="order-123",
            status=ClobOrderStatus.LIVE,
        )
        data = result.to_dict()

        assert data["success"] is True
        assert data["order_id"] == "order-123"
        assert data["status"] == "live"


# =============================================================================
# Status Mapping Tests
# =============================================================================


class TestStatusMapping:
    """Tests for API status mapping."""

    def test_map_api_status_live(self, handler):
        """Test mapping LIVE status."""
        assert handler._map_api_status("LIVE") == ClobOrderStatus.LIVE

    def test_map_api_status_open(self, handler):
        """Test mapping OPEN status (alias for LIVE)."""
        assert handler._map_api_status("OPEN") == ClobOrderStatus.LIVE

    def test_map_api_status_matched(self, handler):
        """Test mapping MATCHED status."""
        assert handler._map_api_status("MATCHED") == ClobOrderStatus.MATCHED

    def test_map_api_status_filled(self, handler):
        """Test mapping FILLED status (alias for MATCHED)."""
        assert handler._map_api_status("FILLED") == ClobOrderStatus.MATCHED

    def test_map_api_status_cancelled(self, handler):
        """Test mapping CANCELLED status."""
        assert handler._map_api_status("CANCELLED") == ClobOrderStatus.CANCELLED

    def test_map_api_status_canceled_us_spelling(self, handler):
        """Test mapping CANCELED status (US spelling)."""
        assert handler._map_api_status("CANCELED") == ClobOrderStatus.CANCELLED

    def test_map_api_status_unknown(self, handler):
        """Test mapping unknown status defaults to PENDING."""
        assert handler._map_api_status("UNKNOWN_STATUS") == ClobOrderStatus.PENDING

    def test_map_api_status_case_insensitive(self, handler):
        """Test mapping is case insensitive."""
        assert handler._map_api_status("live") == ClobOrderStatus.LIVE
        assert handler._map_api_status("Live") == ClobOrderStatus.LIVE
