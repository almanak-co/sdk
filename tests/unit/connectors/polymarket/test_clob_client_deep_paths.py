"""Deep-branch coverage for ``ClobClient``.

Targets uncovered paths in ``almanak/connectors/polymarket/clob_client.py``:

* HTTP context-manager + ``close()``.
* ``_calculate_backoff_delay`` exponential branch (no ``Retry-After``).
* Credential management edge paths (``derive`` failure, fallback to ``create``,
  ``_ensure_credentials`` lazy bootstrap).
* ``submit_order`` / ``submit_order_payload`` happy and error paths (the V2 wire).
* DELETE wrappers (``cancel_order``/``cancel_orders``/``cancel_all_orders``).
* ``get_order`` — open-list hit, fall-through to ``/data/orders``, exception → ``None``.
* ``get_open_orders`` — paginated envelope, bare list, malformed shape, integer/string
  ``createdAt``, parse-error skip.
* ``get_trades`` filter passthrough + parse error.
* ``get_positions`` parse-error skip.
* ``get_market_by_condition_id`` empty-cid short-circuit + cache eviction on expiry.
* ``get_markets`` filter passthrough + parse-error skip.
* ``get_price_history`` happy / cache hit / interval-vs-range validation /
  param requirements.
* ``get_trade_tape`` happy / empty / parse error / limit clamp / token filter.
* ``_validate_tick_size`` non-positive tick short-circuit.
* ``_round_to_tick_size`` non-positive tick short-circuit.
* ``round_price_to_tick`` market-derived tick path.
* ``_build_amounts_at_price`` zero-shares short-circuit + p_den > 10_000 reject.
* ``_calculate_backoff_delay`` Retry-After clamp + exponential cap.
* ``create_and_post_order`` end-to-end glue.

All HTTP calls go through a ``MagicMock(spec=httpx.Client)``. No real network.
"""

from __future__ import annotations

import base64
import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import httpx
import pytest
from eth_account import Account
from pydantic import SecretStr

from almanak.connectors.polymarket import (
    ApiCredentials,
    ClobClient,
    PolymarketConfig,
    SignatureType,
)
from almanak.connectors.polymarket.clob_client import TokenBucketRateLimiter
from almanak.connectors.polymarket.exceptions import (
    PolymarketAPIError,
    PolymarketAuthenticationError,
    PolymarketInvalidTickSizeError,
)
from almanak.connectors.polymarket.models import (
    GammaMarket,
    LimitOrderParams,
    MarketFilters,
    MarketOrderParams,
    OrderFilters,
    OrderType,
    PositionFilters,
    PriceHistoryInterval,
    SignedOrder,
    TradeFilters,
    UnsignedOrder,
)
from almanak.connectors.polymarket.signer import make_local_signer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


# Deterministic test key shared across fixtures + the module-level
# ``_make_clob_client`` helper. After issue #1961 ``PolymarketConfig`` no longer
# carries credential fields, so the signer is injected at the ``ClobClient``
# call site via this helper.
_TEST_PRIVATE_KEY = "0x" + "11" * 32


@pytest.fixture
def test_account():
    return Account.from_key(_TEST_PRIVATE_KEY)


@pytest.fixture
def credentials():
    secret = base64.urlsafe_b64encode(b"deep-cov-secret-key").decode()
    return ApiCredentials(
        api_key="api-key-deep",
        secret=SecretStr(secret),
        passphrase=SecretStr("pass-deep"),
    )


@pytest.fixture
def config(test_account):
    return PolymarketConfig(
        wallet_address=test_account.address,
        signature_type=SignatureType.EOA,
    )


@pytest.fixture
def config_with_credentials(test_account, credentials):
    return PolymarketConfig(
        wallet_address=test_account.address,
        signature_type=SignatureType.EOA,
        api_credentials=credentials,
    )


def _make_clob_client(config: PolymarketConfig, *args, **kwargs) -> ClobClient:
    """Build a ``ClobClient`` with a default local Signer for tests.

    Tests that need read-only mode (``signer=None``) or a remote signer pass
    ``signer=`` explicitly and that overrides this default.
    """
    kwargs.setdefault("signer", make_local_signer(_TEST_PRIVATE_KEY))
    return ClobClient(config, *args, **kwargs)


def _make_response(payload, status_code: int = 200):
    """Return a MagicMock that quacks like an httpx.Response."""
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    response.headers = {}
    if isinstance(payload, (list, dict)):
        response.content = json.dumps(payload).encode()
    elif payload is None:
        response.content = b""
    else:
        response.content = str(payload).encode()
    response.text = response.content.decode() if response.content else ""
    return response


def _make_http(*, request_payloads=None, get_payloads=None, post_payloads=None):
    """Build a MagicMock(spec=httpx.Client). Pass payload(s) per HTTP verb.

    For ``request``-routed calls (everything that goes through ``_request``),
    pass a single payload (returned every call) or a list (consumed in order).
    """
    mock_http = MagicMock(spec=httpx.Client)

    def _make_side_effect(payloads):
        # An empty list models a single response with an empty body (e.g. a
        # paginated endpoint returning no items). Without this branch the
        # iterator path below would raise StopIteration on the first call.
        if isinstance(payloads, list) and not payloads:
            def _side(*_a, **_k):
                return _make_response([])
            return _side

        # When `payloads` is a list of dicts, treat the WHOLE list as one response
        # body (the natural shape of a paginated REST endpoint). Use a list-of-lists
        # to drive multiple sequential responses (e.g. cache-eviction tests).
        if isinstance(payloads, list) and all(isinstance(p, dict) for p in payloads):
            def _side(*_a, **_k):
                return _make_response(payloads)
            return _side

        if isinstance(payloads, list):
            iterator = iter(payloads)

            def _side(*_a, **_k):
                return _make_response(next(iterator))

            return _side

        def _side(*_a, **_k):
            return _make_response(payloads)

        return _side

    if request_payloads is not None:
        mock_http.request.side_effect = _make_side_effect(request_payloads)
    if get_payloads is not None:
        mock_http.get.side_effect = _make_side_effect(get_payloads)
    if post_payloads is not None:
        mock_http.post.side_effect = _make_side_effect(post_payloads)
    return mock_http


def _gamma_market(*, neg_risk: bool = False, tick: str = "0.01", min_size: str = "5") -> GammaMarket:
    return GammaMarket(
        id="m-1",
        condition_id="0x" + "ab" * 32,
        question="q",
        slug="slug",
        outcomes=["Yes", "No"],
        outcome_prices=[Decimal("0.5"), Decimal("0.5")],
        clob_token_ids=["111", "222"],
        volume=Decimal("0"),
        liquidity=Decimal("0"),
        active=True,
        closed=False,
        enable_order_book=True,
        order_price_min_tick_size=Decimal(tick),
        order_min_size=Decimal(min_size),
        neg_risk=neg_risk,
    )


# ---------------------------------------------------------------------------
# HTTP / context manager
# ---------------------------------------------------------------------------


class TestContextManagerAndClose:
    def test_close_delegates_to_http(self, config_with_credentials):
        mock_http = MagicMock(spec=httpx.Client)
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        client.close()
        mock_http.close.assert_called_once()

    def test_context_manager_closes_on_exit(self, config_with_credentials):
        mock_http = MagicMock(spec=httpx.Client)
        with _make_clob_client(config_with_credentials, http_client=mock_http) as client:
            assert client.config is config_with_credentials
        mock_http.close.assert_called_once()


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


class TestCredentialsFallbackPaths:
    def test_derive_credentials_http_error_raises_auth(self, config):
        bad = MagicMock()
        bad.status_code = 401
        bad.text = "no-creds"
        bad.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized", request=MagicMock(), response=bad
        )
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.get.return_value = bad

        client = _make_clob_client(config, http_client=mock_http)
        with pytest.raises(PolymarketAuthenticationError):
            client.derive_api_credentials()

    def test_get_or_create_credentials_falls_through_to_create_on_derive_failure(self, config):
        # Derive (GET) fails; create (POST) succeeds → returns POST credentials.
        bad = MagicMock()
        bad.status_code = 404
        bad.text = "no key"
        bad.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=MagicMock(), response=bad
        )

        good = _make_response(
            {
                "apiKey": "fresh-api",
                "secret": base64.b64encode(b"fresh").decode(),
                "passphrase": "fresh-pass",
            }
        )

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.get.return_value = bad
        mock_http.post.return_value = good

        client = _make_clob_client(config, http_client=mock_http)
        creds = client.get_or_create_credentials()
        assert creds.api_key == "fresh-api"
        mock_http.get.assert_called_once()
        mock_http.post.assert_called_once()

    def test_ensure_credentials_lazily_bootstraps_when_none(self, config):
        good = _make_response(
            {
                "apiKey": "lazy-key",
                "secret": base64.b64encode(b"lazy").decode(),
                "passphrase": "lazy-pass",
            }
        )
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.get.return_value = good

        client = _make_clob_client(config, http_client=mock_http)
        assert client.credentials is None
        creds = client._ensure_credentials()
        assert creds.api_key == "lazy-key"
        assert client.credentials is creds


# ---------------------------------------------------------------------------
# Backoff
# ---------------------------------------------------------------------------


class TestCalculateBackoffDelay:
    def test_exponential_backoff_grows_with_retry_count(self, config_with_credentials):
        client = _make_clob_client(config_with_credentials)
        with patch("almanak.connectors.polymarket.clob_client.random.uniform", return_value=0.0):
            d0 = client._calculate_backoff_delay(retry_count=0)
            d1 = client._calculate_backoff_delay(retry_count=1)
            d2 = client._calculate_backoff_delay(retry_count=2)
        # base * 2^n, with jitter mocked to 0.
        assert d1 > d0
        assert d2 > d1

    def test_exponential_backoff_caps_at_max(self, config_with_credentials):
        client = _make_clob_client(config_with_credentials)
        with patch("almanak.connectors.polymarket.clob_client.random.uniform", return_value=0.0):
            d = client._calculate_backoff_delay(retry_count=20)
        assert d == client.config.max_retry_delay

    def test_retry_after_path_clamps_to_max(self, config_with_credentials):
        client = _make_clob_client(config_with_credentials)
        with patch("almanak.connectors.polymarket.clob_client.random.uniform", return_value=0.0):
            d = client._calculate_backoff_delay(retry_count=0, retry_after=999_999)
        assert d == client.config.max_retry_delay


# ---------------------------------------------------------------------------
# Submit / cancel orders
# ---------------------------------------------------------------------------


class TestSubmitOrder:
    def test_submit_order_without_credentials_raises(self, config):
        client = _make_clob_client(config)
        unsigned = UnsignedOrder(
            salt=1,
            maker=config.wallet_address,
            signer=config.wallet_address,
            token_id=123,
            maker_amount=10_000,
            taker_amount=20_000,
            side=0,
            signature_type=SignatureType.EOA.value,
            timestamp=1_700_000_000_000,
            metadata="0x" + "00" * 32,
            builder="0x" + "00" * 20,
            exchange_address="0x" + "00" * 20,
            api_expiration=0,
        )
        signed = SignedOrder(order=unsigned, signature="0x" + "11" * 65)
        with pytest.raises(PolymarketAuthenticationError):
            client.submit_order(signed)

    def test_submit_order_payload_posts_directly(self, config_with_credentials):
        # /order returns a healthy LIVE response.
        payload = {
            "orderID": "ord-1",
            "status": "LIVE",
            "transactionsHashes": [],
        }
        mock_http = _make_http(request_payloads=payload)
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        wire = {
            "order": {"tokenId": "123"},
            "orderType": "GTC",
            "signature": "0x" + "ab" * 65,
        }
        resp = client.submit_order_payload(wire)
        assert resp.order_id == "ord-1"
        # Ensure it actually issued a POST through the underlying request.
        assert mock_http.request.call_args.kwargs["method"] == "POST"


class TestCancelHelpers:
    def test_cancel_order_uses_delete_with_body(self, config_with_credentials):
        mock_http = _make_http(request_payloads={})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        assert client.cancel_order("o-123") is True
        kwargs = mock_http.request.call_args.kwargs
        assert kwargs["method"] == "DELETE"
        body = kwargs["content"]
        assert "o-123" in body

    def test_cancel_orders_passes_list_body(self, config_with_credentials):
        mock_http = _make_http(request_payloads={})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        assert client.cancel_orders(["a", "b", "c"]) is True
        body = mock_http.request.call_args.kwargs["content"]
        assert "a" in body and "b" in body and "c" in body

    def test_cancel_all_orders_calls_endpoint(self, config_with_credentials):
        mock_http = _make_http(request_payloads={})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        assert client.cancel_all_orders() is True
        kwargs = mock_http.request.call_args.kwargs
        assert kwargs["method"] == "DELETE"
        assert kwargs["url"].endswith("/cancel-all")


# ---------------------------------------------------------------------------
# get_order
# ---------------------------------------------------------------------------


class TestGetOrder:
    def test_get_order_returns_match_from_open_orders(self, config_with_credentials):
        # Open-orders list contains the requested id.
        open_payload = [
            {
                "id": "ord-A",
                "market": "tok-a",
                "side": "BUY",
                "price": "0.5",
                "original_size": "10",
                "size_matched": "0",
                "createdAt": 1_700_000_000,
            }
        ]
        mock_http = _make_http(request_payloads=open_payload)
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        order = client.get_order("ord-A")
        assert order is not None
        assert order.order_id == "ord-A"

    def test_get_order_falls_back_to_data_orders_endpoint(self, config_with_credentials):
        # First call (get_open_orders) returns nothing; second call (data/orders by id) returns the row.
        responses = [
            [],  # get_open_orders
            [
                {
                    "orderID": "ord-X",
                    "market": "tok-x",
                    "side": "SELL",
                    "price": "0.4",
                    "size": "20",
                    "filledSize": "5",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "expiration": 0,
                }
            ],  # /data/orders?orderID=
        ]
        mock_http = _make_http(request_payloads=responses)
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        order = client.get_order("ord-X")
        assert order is not None
        assert order.order_id == "ord-X"
        assert order.side == "SELL"

    def test_get_order_returns_none_when_not_found(self, config_with_credentials):
        # Both calls return empty lists.
        mock_http = _make_http(request_payloads=[[], []])
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        assert client.get_order("missing") is None

    def test_get_order_swallows_exception_returns_none(self, config_with_credentials):
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.side_effect = RuntimeError("boom")
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        assert client.get_order("anything") is None


# ---------------------------------------------------------------------------
# get_open_orders
# ---------------------------------------------------------------------------


class TestGetOpenOrders:
    def test_envelope_shape_with_data_field(self, config_with_credentials):
        envelope = {
            "data": [
                {
                    "id": "ord-1",
                    "market": "tok",
                    "side": "BUY",
                    "price": "0.5",
                    "original_size": "10",
                    "size_matched": "1",
                    "createdAt": 1_700_000_000,
                    "expiration": 0,
                }
            ],
            "next_cursor": "abc",
            "count": 1,
        }
        mock_http = _make_http(request_payloads=envelope)
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        orders = client.get_open_orders(filters=OrderFilters(market="tok", limit=50))
        assert len(orders) == 1
        assert orders[0].order_id == "ord-1"
        # Filter passed through.
        params = mock_http.request.call_args.kwargs["params"]
        assert params["market"] == "tok"
        assert params["limit"] == 50

    def test_unexpected_shape_returns_empty_list(self, config_with_credentials):
        mock_http = _make_http(request_payloads={"unexpected": "shape"})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        assert client.get_open_orders() == []

    def test_iso_string_created_at_parsed(self, config_with_credentials):
        mock_http = _make_http(
            request_payloads=[
                {
                    "id": "ord-i",
                    "market": "tok",
                    "side": "BUY",
                    "price": "0.5",
                    "size": "10",
                    "filledSize": "0",
                    "createdAt": "2024-06-01T12:34:56Z",
                }
            ]
        )
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        orders = client.get_open_orders()
        assert orders[0].created_at is not None
        assert orders[0].created_at.year == 2024

    def test_missing_created_at_yields_none(self, config_with_credentials):
        mock_http = _make_http(
            request_payloads=[
                {
                    "id": "ord-n",
                    "market": "tok",
                    "side": "BUY",
                    "price": "0.5",
                    "size": "10",
                    "filledSize": "0",
                }
            ]
        )
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        orders = client.get_open_orders()
        assert orders[0].created_at is None

    def test_parse_error_on_one_order_skips_only_that_one(self, config_with_credentials):
        # The second item has an unparseable price -> Decimal raises -> skip.
        mock_http = _make_http(
            request_payloads=[
                {
                    "id": "ord-good",
                    "market": "tok",
                    "side": "BUY",
                    "price": "0.5",
                    "size": "10",
                    "filledSize": "0",
                    "createdAt": 1_700_000_000,
                },
                {
                    "id": "ord-bad",
                    "market": "tok",
                    "side": "BUY",
                    "price": "not-a-decimal",
                    "size": "5",
                    "filledSize": "0",
                },
            ]
        )
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        orders = client.get_open_orders()
        assert len(orders) == 1
        assert orders[0].order_id == "ord-good"


# ---------------------------------------------------------------------------
# get_trades
# ---------------------------------------------------------------------------


class TestGetTrades:
    def test_filters_propagate_to_query(self, config_with_credentials):
        from datetime import datetime, timezone

        mock_http = _make_http(request_payloads={})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        before = datetime(2024, 1, 1, tzinfo=timezone.utc)
        after = datetime(2024, 6, 1, tzinfo=timezone.utc)
        client.get_trades(filters=TradeFilters(market="tok", before=before, after=after, limit=50))
        params = mock_http.request.call_args.kwargs["params"]
        assert params["market"] == "tok"
        assert params["before"].startswith("2024-01-01")
        assert params["after"].startswith("2024-06-01")
        assert params["limit"] == 50

    def test_parse_error_on_one_trade_skips(self, config_with_credentials):
        mock_http = _make_http(
            request_payloads=[
                {
                    "id": "ok",
                    "market": "m",
                    "tokenId": "tok",
                    "side": "BUY",
                    "price": "0.5",
                    "size": "10",
                    "fee": "0",
                    "timestamp": "2024-01-01T00:00:00Z",
                    "status": "CONFIRMED",
                },
                # Bad price triggers Decimal() failure → skip.
                {
                    "id": "bad",
                    "price": "x",
                },
            ]
        )
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        trades = client.get_trades()
        assert len(trades) == 1
        assert trades[0].id == "ok"


# ---------------------------------------------------------------------------
# get_positions error
# ---------------------------------------------------------------------------


class TestGetPositionsParseSkip:
    def test_parse_error_on_position_skips_row(self, config_with_credentials):
        mock_http = _make_http(
            request_payloads=[
                # Bad: avgPrice is non-numeric → Decimal() raises → skipped.
                {
                    "outcome": "Yes",
                    "size": "10",
                    "avgPrice": "abc",
                    "currentPrice": "0.5",
                    "market": "m",
                    "tokenId": "tok",
                    "conditionId": "cid",
                },
                # Good row.
                {
                    "outcome": "No",
                    "size": "1",
                    "avgPrice": "0.4",
                    "currentPrice": "0.6",
                    "market": "m2",
                    "tokenId": "tok2",
                    "conditionId": "cid2",
                },
            ]
        )
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        positions = client.get_positions()
        assert len(positions) == 1
        assert positions[0].outcome == "NO"


# ---------------------------------------------------------------------------
# get_market_by_condition_id edges
# ---------------------------------------------------------------------------


class TestGetMarketByConditionId:
    def test_empty_condition_id_returns_none_without_call(self, config_with_credentials):
        mock_http = MagicMock(spec=httpx.Client)
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        assert client.get_market_by_condition_id("") is None
        mock_http.request.assert_not_called()

    def test_expired_cache_entry_evicted_then_refetched(self, config_with_credentials):
        # Pre-populate the cache with an expired entry that points at sentinel
        # (NOT_FOUND); set its TTL to the past so the eviction branch fires.
        cid = "0xdeadbeef"
        mock_http = _make_http(request_payloads={})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        client._cache[f"market_by_cid:{cid}"] = (client._MARKET_NOT_FOUND, 0.0)

        assert client.get_market_by_condition_id(cid) is None
        # The expired sentinel must be evicted and a fresh /markets call issued.
        assert mock_http.request.call_count == 1
        # New entry written (with sentinel since /markets returned []).
        assert f"market_by_cid:{cid}" in client._cache


# ---------------------------------------------------------------------------
# get_markets filter passthrough + parse skip
# ---------------------------------------------------------------------------


class TestGetMarketsFilters:
    def test_all_filters_propagate(self, config_with_credentials):
        mock_http = _make_http(request_payloads={})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        client.get_markets(
            MarketFilters(
                active=True,
                closed=False,
                slug="abc",
                condition_ids=["c1", "c2"],
                clob_token_ids=["t1"],
                event_id="ev",
                event_slug="es",
                tag="tag1",
                limit=10,
                offset=20,
            )
        )
        params = mock_http.request.call_args.kwargs["params"]
        assert params["active"] == "true"
        assert params["closed"] == "false"
        assert params["slug"] == "abc"
        assert params["condition_ids"] == "c1,c2"
        assert params["clob_token_ids"] == "t1"
        assert params["event_id"] == "ev"
        assert params["event_slug"] == "es"
        assert params["tag"] == "tag1"
        assert params["limit"] == 10
        assert params["offset"] == 20

    def test_parse_error_skips_market(self, config_with_credentials):
        # First entry parseable, second missing required ``id`` → skipped.
        mock_http = _make_http(
            request_payloads=[
                {
                    "id": "good",
                    "conditionId": "0xg",
                    "question": "?",
                    "slug": "g",
                    "outcomes": '["Yes","No"]',
                    "outcomePrices": '["0.5","0.5"]',
                    "clobTokenIds": '["1","2"]',
                    "volume": "0",
                    "liquidity": "0",
                    "active": True,
                    "closed": False,
                    "enableOrderBook": True,
                },
                {"slug": "no-id"},
            ]
        )
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        markets = client.get_markets()
        assert len(markets) == 1
        assert markets[0].id == "good"


# ---------------------------------------------------------------------------
# Validation edges (non-network)
# ---------------------------------------------------------------------------


class TestValidationEdges:
    def test_validate_tick_size_zero_tick_short_circuits(self, config_with_credentials):
        client = _make_clob_client(config_with_credentials)
        # Control: a positive tick rejects a non-tick-aligned price.
        with pytest.raises(PolymarketInvalidTickSizeError):
            client._validate_tick_size(Decimal("0.123"), tick_size=Decimal("0.01"))
        # Zero (non-positive) tick must short-circuit and return None.
        result = client._validate_tick_size(Decimal("0.123"), tick_size=Decimal("0"))
        assert result is None

    def test_round_to_tick_size_zero_tick_returns_input(self, config_with_credentials):
        client = _make_clob_client(config_with_credentials)
        out = client._round_to_tick_size(Decimal("0.55"), Decimal("0"), "BUY")
        assert out == Decimal("0.55")

    def test_round_price_to_tick_uses_market_tick_when_no_explicit(self, config_with_credentials):
        client = _make_clob_client(config_with_credentials)
        market = _gamma_market(tick="0.01")
        rounded = client.round_price_to_tick(Decimal("0.655"), "BUY", market=market)
        assert rounded == Decimal("0.65")

    def test_build_amounts_at_price_zero_shares_short_circuits(self):
        out = ClobClient._build_amounts_at_price("BUY", Decimal("0.5"), 0)
        assert out == (0, 0)

    def test_build_amounts_at_price_rejects_non_tick_decimal(self):
        # Decimal(0.7) triggers the giant-denominator guard.
        with pytest.raises(ValueError, match="too much precision"):
            ClobClient._build_amounts_at_price("BUY", Decimal(0.7), 1_000_000)

    def test_build_amounts_at_price_rejects_zero_or_negative_price(self):
        with pytest.raises(ValueError, match="positive"):
            ClobClient._build_amounts_at_price("BUY", Decimal("0"), 1_000_000)


# ---------------------------------------------------------------------------
# get_price_history
# ---------------------------------------------------------------------------


class TestGetPriceHistory:
    def test_interval_and_range_are_mutually_exclusive(self, config_with_credentials):
        client = _make_clob_client(config_with_credentials)
        with pytest.raises(ValueError, match="Cannot specify both"):
            client.get_price_history("tok", interval="1d", start_ts=1, end_ts=2)

    def test_partial_range_raises(self, config_with_credentials):
        client = _make_clob_client(config_with_credentials)
        with pytest.raises(ValueError, match="must be specified together"):
            client.get_price_history("tok", start_ts=1)

    def test_interval_string_passthrough(self, config_with_credentials):
        mock_http = _make_http(request_payloads={"history": []})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        client.get_price_history("tok", interval="1h")
        params = mock_http.request.call_args.kwargs["params"]
        assert params["market"] == "tok"
        assert params["interval"] == "1h"

    def test_interval_enum_normalized_to_value(self, config_with_credentials):
        mock_http = _make_http(request_payloads={"history": []})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        client.get_price_history("tok", interval=PriceHistoryInterval.ONE_HOUR)
        params = mock_http.request.call_args.kwargs["params"]
        assert params["interval"] == "1h"

    def test_range_and_fidelity_propagate(self, config_with_credentials):
        mock_http = _make_http(
            request_payloads={"history": [{"t": 1_700_000_000, "p": "0.55"}]}
        )
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        result = client.get_price_history("tok", start_ts=1_700_000_000, end_ts=1_700_000_300, fidelity=5)
        params = mock_http.request.call_args.kwargs["params"]
        assert params["startTs"] == 1_700_000_000
        assert params["endTs"] == 1_700_000_300
        assert params["fidelity"] == 5
        # Result populated.
        assert len(result.prices) == 1
        assert result.prices[0].price == Decimal("0.55")
        assert result.start_time is not None and result.end_time is not None

    def test_cache_hit_skips_http_on_second_call(self, config_with_credentials):
        mock_http = _make_http(request_payloads={"history": []})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        client.get_price_history("tok", interval="1d")
        client.get_price_history("tok", interval="1d")
        # Only one underlying request.
        assert mock_http.request.call_count == 1


# ---------------------------------------------------------------------------
# get_trade_tape
# ---------------------------------------------------------------------------


class TestGetTradeTape:
    def test_default_limit_capped_at_500(self, config_with_credentials):
        mock_http = _make_http(request_payloads={})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        client.get_trade_tape(token_id=None, limit=10_000)
        params = mock_http.request.call_args.kwargs["params"]
        assert params["limit"] == 500
        assert "market" not in params  # token_id None → not added

    def test_token_id_added_to_params(self, config_with_credentials):
        mock_http = _make_http(request_payloads={})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        client.get_trade_tape(token_id="tok-123", limit=50)
        params = mock_http.request.call_args.kwargs["params"]
        assert params["market"] == "tok-123"
        assert params["limit"] == 50

    def test_parses_into_historical_trades(self, config_with_credentials):
        payload = [
            {
                "id": "trade-1",
                "tokenId": "tok",
                "side": "BUY",
                "price": "0.55",
                "size": "10",
                "timestamp": 1_700_000_000,
            }
        ]
        mock_http = _make_http(request_payloads=payload)
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        trades = client.get_trade_tape(token_id="tok", limit=10)
        assert len(trades) == 1
        assert trades[0].price == Decimal("0.55")

    def test_parse_error_skips_trade(self, config_with_credentials):
        # ``HistoricalTrade.from_api_response`` requires Decimal-coercible
        # ``price``/``size`` — give one entry that raises and one that passes.
        payload = [
            {"id": "good", "tokenId": "tok", "side": "BUY", "price": "0.5", "size": "1", "timestamp": 1_700_000_000},
            {"id": "bad", "price": "x", "size": "y"},
        ]
        mock_http = _make_http(request_payloads=payload)
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        trades = client.get_trade_tape()
        assert len(trades) == 1
        assert trades[0].id == "good"

    def test_non_list_response_yields_empty(self, config_with_credentials):
        mock_http = _make_http(request_payloads={"unexpected": True})
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        assert client.get_trade_tape() == []


# ---------------------------------------------------------------------------
# create_and_post_order glue
# ---------------------------------------------------------------------------


class TestCreateAndPostOrder:
    def test_glue_builds_signs_and_submits_buy_limit(self, config_with_credentials):
        # Single submit response reused for the POST /order call.
        mock_http = _make_http(
            request_payloads={
                "orderID": "ord-glue",
                "status": "MATCHED",
                "transactionsHashes": ["0xabc"],
                "makingAmount": "10000000",
                "takingAmount": "20000000",
            }
        )
        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        market = _gamma_market(tick="0.001", min_size="5")

        resp = client.create_and_post_order(
            token_id="123",
            price=Decimal("0.5"),
            size=Decimal("10"),
            side="BUY",
            market=market,
            time_in_force="GTC",
            expiration=0,
        )
        assert resp.order_id == "ord-glue"
        # Should have made exactly one POST request through the underlying client.
        assert mock_http.request.call_count == 1
        kwargs = mock_http.request.call_args.kwargs
        assert kwargs["method"] == "POST"
        assert kwargs["url"].endswith("/order")

    def test_glue_propagates_order_type_via_oid(self, config_with_credentials):
        # Confirm OrderType validation: invalid string raises.
        client = _make_clob_client(config_with_credentials)
        with pytest.raises(ValueError):
            client.create_and_post_order(
                token_id="123",
                price=Decimal("0.5"),
                size=Decimal("10"),
                side="BUY",
                market=_gamma_market(),
                time_in_force="NOT_AN_ORDER_TYPE",
            )

    def test_create_and_sign_market_order_returns_signed(self, config_with_credentials):
        # Cover the create_and_sign_market_order one-liner.
        client = _make_clob_client(config_with_credentials)
        market = _gamma_market(tick="0.001", min_size="5")
        params = MarketOrderParams(
            token_id="123",
            side="BUY",
            amount=Decimal("10"),
            worst_price=Decimal("0.5"),
        )
        signed = client.create_and_sign_market_order(params, market=market)
        assert isinstance(signed, SignedOrder)
        assert signed.signature.startswith("0x")

    def test_create_and_sign_limit_order_returns_signed(self, config_with_credentials):
        client = _make_clob_client(config_with_credentials)
        market = _gamma_market(tick="0.001", min_size="5")
        params = LimitOrderParams(
            token_id="123",
            side="SELL",
            price=Decimal("0.5"),
            size=Decimal("10"),
        )
        signed = client.create_and_sign_limit_order(params, market=market)
        assert isinstance(signed, SignedOrder)
        assert signed.signature.startswith("0x")


# ---------------------------------------------------------------------------
# Token-bucket rate limiter additional branches
# ---------------------------------------------------------------------------


class TestRateLimiterMisc:
    def test_default_initial_refill_does_not_overshoot_capacity(self):
        # Construct, then immediately query available_tokens — it triggers
        # a refill that should be clamped at capacity.
        limiter = TokenBucketRateLimiter(rate_per_second=10.0)
        tokens = limiter.available_tokens
        assert tokens <= 10.0
        assert tokens == pytest.approx(10.0, abs=0.5)

    def test_set_enabled_property(self):
        limiter = TokenBucketRateLimiter(rate_per_second=1.0)
        limiter.enabled = False
        assert limiter.enabled is False
        # Disabled try_acquire short-circuits to True.
        assert limiter.try_acquire() is True
        # Disabled acquire short-circuits to True.
        assert limiter.acquire(timeout=0.0) is True
