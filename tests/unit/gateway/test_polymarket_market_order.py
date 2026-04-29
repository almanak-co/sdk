"""Unit tests for ``PolymarketServiceServicer.CreateAndPostMarketOrder`` (VIB-3700).

Pin the cross-side price-sampling convention and the worst_price guard. The
prior version sampled the SAME side as the trade (``side=BUY`` for a market
BUY), which on Polymarket's CLOB returns the BID — not the ASK that a buyer
would actually cross. Verified convention from
``almanak/framework/connectors/polymarket/clob_client.py:879-881`` and
``models.py:653,664``:

    GET /price?side=BUY  -> best BID  (highest buyer's price)
    GET /price?side=SELL -> best ASK  (lowest seller's price)

So a market BUY must sample ``side=SELL`` (to read the ASK it crosses) and a
market SELL must sample ``side=BUY`` (to read the BID it hits). These tests
lock in that mapping plus the worst_price guard and edge-case validation.

We mock ``_request`` directly with ``AsyncMock`` rather than wiring up an
aiohttp test double — the cross-side mapping is the unit under test, and a
direct mock makes the call-arg assertion unambiguous.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from eth_account import Account

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.polymarket_service import (
    CLOB_BASE_URL,
    PolymarketServiceServicer,
)

# Deterministic Anvil-style key — never funded.
TEST_PRIVATE_KEY = "0x" + "ab" * 32
TEST_ACCOUNT = Account.from_key(TEST_PRIVATE_KEY)
TEST_WALLET = TEST_ACCOUNT.address

TEST_TOKEN_ID = "111"


@pytest.fixture
def settings() -> MagicMock:
    s = MagicMock(spec=GatewaySettings)
    s.private_key = TEST_PRIVATE_KEY
    s.polymarket_private_key = None
    s.eoa_address = TEST_WALLET
    s.polymarket_wallet_address = None
    s.safe_address = None
    s.safe_mode = None
    s.polymarket_api_key = "k"
    s.polymarket_secret = "c2VjcmV0"  # base64("secret")
    s.polymarket_passphrase = "p"
    return s


@pytest.fixture
def servicer(settings: MagicMock) -> PolymarketServiceServicer:
    return PolymarketServiceServicer(settings=settings)


def _success_order_response() -> gateway_pb2.PolymarketOrderResponse:
    """A canned downstream success — the unit under test is the market-order
    wrapper, not ``CreateAndPostOrder`` itself, so we stub the latter."""
    return gateway_pb2.PolymarketOrderResponse(
        order_id="order-123",
        status="MATCHED",
        size_matched="10",
        price="0.55",
        size="10",
        success=True,
    )


# =============================================================================
# Cross-side price sampling — the core of the VIB-3700 fix
# =============================================================================


class TestCrossSidePriceSampling:
    """Polymarket convention: ``/price?side=BUY`` returns the BID,
    ``/price?side=SELL`` returns the ASK. So to price a market BUY (which
    crosses the ASK) we must call ``side=SELL`` — and vice versa for SELL."""

    @pytest.mark.asyncio
    async def test_buy_queries_sell_side_for_ask(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Market BUY must read the ASK -> ``side=SELL`` on the wire."""
        servicer._request = AsyncMock(return_value=(True, {"price": "0.55"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="5.5", side="BUY"
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is True
        assert servicer._request.await_count == 1
        call_args = servicer._request.await_args
        # Positional: method, base_url, endpoint
        assert call_args.args == ("GET", CLOB_BASE_URL, "/price")
        # Keyword: params with side=SELL (we want the ASK for a BUY)
        assert call_args.kwargs["params"] == {
            "token_id": TEST_TOKEN_ID,
            "side": "SELL",
        }

    @pytest.mark.asyncio
    async def test_sell_queries_buy_side_for_bid(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Market SELL must read the BID -> ``side=BUY`` on the wire."""
        servicer._request = AsyncMock(return_value=(True, {"price": "0.45"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="10", side="SELL"
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is True
        call_args = servicer._request.await_args
        assert call_args.kwargs["params"] == {
            "token_id": TEST_TOKEN_ID,
            "side": "BUY",
        }

    @pytest.mark.asyncio
    async def test_buy_size_is_amount_divided_by_ask_round_down(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """A BUY's wire ``size`` is ``amount / ask`` rounded down to 6dp.
        Reading the wrong side here would silently inflate ``size``: the BID
        is ALWAYS lower than the ASK, so dividing by the BID overstates how
        many tokens the strategy can afford -> overspend at match time."""
        # ASK = 0.50, amount = 1 USDC -> size should be 1 / 0.50 = 2 tokens.
        # If we accidentally read the BID (e.g. 0.40), we'd get 2.5 instead.
        servicer._request = AsyncMock(return_value=(True, {"price": "0.50"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="1", side="BUY"
        )
        await servicer.CreateAndPostMarketOrder(request, MagicMock())

        forwarded = servicer.CreateAndPostOrder.await_args.args[0]
        assert forwarded.size == "2.000000"
        assert forwarded.price == "0.50"


# =============================================================================
# worst_price guard — submission-time slippage check
# =============================================================================


class TestWorstPriceGuard:
    """The submission-time guard rejects when the sampled top-of-book is
    already worse than the caller's worst_price. FOK at match time enforces
    the same bound on the actual fills (covered separately by CLOB tests)."""

    @pytest.mark.asyncio
    async def test_buy_rejects_when_ask_above_worst_price(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Market BUY with ASK 0.60 > worst_price 0.55 -> reject. The fix is
        load-bearing here: the prior bug sampled the BID (lower), so the
        guard ALWAYS passed for BUYs unless the spread crossed worst_price."""
        servicer._request = AsyncMock(return_value=(True, {"price": "0.60"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID,
            amount="5",
            side="BUY",
            worst_price="0.55",
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "exceeds worst_price" in response.error
        assert "0.60" in response.error
        assert "0.55" in response.error
        # Critically: order is NEVER submitted on a guard rejection.
        servicer.CreateAndPostOrder.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sell_rejects_when_bid_below_worst_price(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Market SELL with BID 0.40 < worst_price 0.45 -> reject."""
        servicer._request = AsyncMock(return_value=(True, {"price": "0.40"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID,
            amount="10",
            side="SELL",
            worst_price="0.45",
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "below worst_price" in response.error
        assert "0.40" in response.error
        assert "0.45" in response.error
        servicer.CreateAndPostOrder.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_buy_passes_when_ask_at_or_better(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """ASK exactly at worst_price (or better/lower) must pass — boundary
        case: the inequality is strict, not strict-or-equal, so equality is
        accepted (a fill at the target price honors the cap)."""
        servicer._request = AsyncMock(return_value=(True, {"price": "0.55"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID,
            amount="5.5",
            side="BUY",
            worst_price="0.55",
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is True
        servicer.CreateAndPostOrder.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sell_passes_when_bid_at_or_better(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        servicer._request = AsyncMock(return_value=(True, {"price": "0.45"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID,
            amount="10",
            side="SELL",
            worst_price="0.45",
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is True

    @pytest.mark.asyncio
    async def test_invalid_worst_price_format_returns_error(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        servicer._request = AsyncMock(return_value=(True, {"price": "0.50"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID,
            amount="5",
            side="BUY",
            worst_price="not-a-number",
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "Invalid worst_price format" in response.error
        servicer.CreateAndPostOrder.assert_not_awaited()


# =============================================================================
# Happy path: forwards to CreateAndPostOrder with FOK
# =============================================================================


class TestHappyPath:
    """Confirms the wrapper forwards the right request shape to the limit-
    order handler. ``time_in_force`` MUST be "FOK" — that's the V2 mechanism
    by which a market order is realized on Polymarket's CLOB."""

    @pytest.mark.asyncio
    async def test_buy_forwards_fok_limit_to_create_and_post_order(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        servicer._request = AsyncMock(return_value=(True, {"price": "0.55"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID,
            amount="5.5",
            side="BUY",
            expiration=0,
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is True
        servicer.CreateAndPostOrder.assert_awaited_once()
        forwarded = servicer.CreateAndPostOrder.await_args.args[0]
        assert isinstance(forwarded, gateway_pb2.PolymarketCreateOrderRequest)
        assert forwarded.token_id == TEST_TOKEN_ID
        assert forwarded.side == "BUY"
        assert forwarded.price == "0.55"
        assert forwarded.size == "10.000000"  # 5.5 / 0.55 = 10
        assert forwarded.time_in_force == "FOK"

    @pytest.mark.asyncio
    async def test_sell_forwards_amount_as_size_unchanged(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """SELL: amount IS the token size, no division by price."""
        servicer._request = AsyncMock(return_value=(True, {"price": "0.45"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="10", side="SELL"
        )
        await servicer.CreateAndPostMarketOrder(request, MagicMock())

        forwarded = servicer.CreateAndPostOrder.await_args.args[0]
        assert forwarded.size == "10"
        assert forwarded.side == "SELL"
        assert forwarded.time_in_force == "FOK"


# =============================================================================
# Input validation
# =============================================================================


class TestInputValidation:
    """Side and amount must be validated BEFORE any network call — bad input
    should never burn an HTTP round-trip on the price endpoint."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_side", ["", "buyy", "Long", "0", "1"])
    async def test_invalid_side_returns_error_without_network(
        self, servicer: PolymarketServiceServicer, bad_side: str
    ) -> None:
        servicer._request = AsyncMock()  # Should never be called
        servicer.CreateAndPostOrder = AsyncMock()

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="5", side=bad_side
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "Invalid side" in response.error
        servicer._request.assert_not_awaited()
        servicer.CreateAndPostOrder.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lowercase_side_normalized_to_upper(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """``side`` is coerced to upper-case — accept ``"buy"`` as BUY."""
        servicer._request = AsyncMock(return_value=(True, {"price": "0.55"}, None))
        servicer.CreateAndPostOrder = AsyncMock(return_value=_success_order_response())

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="5.5", side="buy"
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is True
        # Confirms case normalization landed on the BUY -> SELL-side branch.
        assert servicer._request.await_args.kwargs["params"]["side"] == "SELL"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_amount", ["not-a-number", "abc", ""])
    async def test_invalid_amount_format_returns_error(
        self, servicer: PolymarketServiceServicer, bad_amount: str
    ) -> None:
        servicer._request = AsyncMock()
        servicer.CreateAndPostOrder = AsyncMock()

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount=bad_amount, side="BUY"
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "Invalid amount format" in response.error
        servicer._request.assert_not_awaited()
        servicer.CreateAndPostOrder.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_amount", ["0", "-1", "-0.001"])
    async def test_non_positive_amount_returns_error(
        self, servicer: PolymarketServiceServicer, bad_amount: str
    ) -> None:
        servicer._request = AsyncMock()
        servicer.CreateAndPostOrder = AsyncMock()

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount=bad_amount, side="BUY"
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "Amount must be positive" in response.error
        servicer._request.assert_not_awaited()
        servicer.CreateAndPostOrder.assert_not_awaited()


# =============================================================================
# Price-endpoint error paths
# =============================================================================


class TestPriceEndpointFailures:
    """Failures from the price endpoint must surface as a structured response,
    not bubble up as an exception."""

    @pytest.mark.asyncio
    async def test_price_endpoint_failure_returns_error(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        servicer._request = AsyncMock(return_value=(False, None, "HTTP 404: No orderbook"))
        servicer.CreateAndPostOrder = AsyncMock()

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="5", side="BUY"
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "No orderbook" in response.error
        servicer.CreateAndPostOrder.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_zero_or_negative_price_returns_error(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """A "0" price (one-sided book or upstream regression) is non-actionable;
        dividing by it would explode. Fail fast with a clear error."""
        servicer._request = AsyncMock(return_value=(True, {"price": "0"}, None))
        servicer.CreateAndPostOrder = AsyncMock()

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="5", side="BUY"
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "price must be positive" in response.error
        servicer.CreateAndPostOrder.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_malformed_price_returns_error(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        servicer._request = AsyncMock(return_value=(True, {"price": "not-a-number"}, None))
        servicer.CreateAndPostOrder = AsyncMock()

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="5", side="BUY"
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "Invalid price format" in response.error
        servicer.CreateAndPostOrder.assert_not_awaited()


# =============================================================================
# Service-not-configured short-circuit
# =============================================================================


class TestNotConfigured:
    @pytest.mark.asyncio
    async def test_returns_error_when_service_unavailable(self) -> None:
        """If the service didn't receive credentials at startup it must fail
        fast — no network calls, no signing attempts."""
        s = MagicMock(spec=GatewaySettings)
        s.private_key = None
        s.polymarket_private_key = None
        s.eoa_address = None
        s.polymarket_wallet_address = None
        s.safe_address = None
        s.safe_mode = None
        s.polymarket_api_key = None
        s.polymarket_secret = None
        s.polymarket_passphrase = None
        servicer = PolymarketServiceServicer(settings=s)
        assert servicer._available is False

        request = gateway_pb2.PolymarketMarketOrderRequest(
            token_id=TEST_TOKEN_ID, amount="5", side="BUY"
        )
        response = await servicer.CreateAndPostMarketOrder(request, MagicMock())

        assert response.success is False
        assert "Polymarket not configured" in response.error
