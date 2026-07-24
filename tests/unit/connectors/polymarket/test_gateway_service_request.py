"""Branch-coverage tests for ``PolymarketServiceServicer._request``.

``_request`` is the single HTTP funnel for every Polymarket gateway RPC
(gateway-side code — egress is correct here, but tests must never touch the
network). It multiplexes:

  * method / URL / query-param / JSON-body construction,
  * optional L2 (HMAC) authentication — credential availability, header
    construction failures,
  * response handling — 200 + JSON parse, non-200 error text (truncated),
    transport-level timeout / client errors.

The servicer's ``__init__`` wires signers, sessions, and credential locks
that ``_request`` never reads, so tests build the instance via
``object.__new__`` and set only the attributes ``_request`` (and the real
``_build_l2_headers`` / ``_build_l2_signature`` it calls) consume.
The aiohttp session is replaced with an in-process fake; no sockets open.
"""

from __future__ import annotations

import base64
import json
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from almanak.connectors.polymarket.gateway.service import PolymarketServiceServicer

BASE_URL = "https://clob.example.test"
WALLET = "0x" + "11" * 20
API_SECRET_B64 = base64.b64encode(b"unit-test-secret").decode()


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal stand-in for aiohttp.ClientResponse as used by ``_request``."""

    def __init__(
        self,
        status: int = 200,
        json_data: object = None,
        json_exc: Exception | None = None,
        text: str = "",
    ) -> None:
        self.status = status
        self._json_data = json_data
        self._json_exc = json_exc
        self._text = text

    async def json(self) -> object:
        if self._json_exc is not None:
            raise self._json_exc
        return self._json_data

    async def text(self) -> str:
        return self._text


class _FakeRequestCM:
    """Async context manager returned by ``session.request(...)``."""

    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    async def __aenter__(self) -> _FakeResponse:
        return self._response

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


def _make_session(
    response: _FakeResponse | None = None,
    request_exc: Exception | None = None,
) -> MagicMock:
    session = MagicMock()
    if request_exc is not None:
        session.request = MagicMock(side_effect=request_exc)
    else:
        session.request = MagicMock(return_value=_FakeRequestCM(response or _FakeResponse()))
    return session


def _make_servicer(session: MagicMock, **overrides: object) -> PolymarketServiceServicer:
    """Build a servicer skeleton carrying only what ``_request`` reads.

    Instance-attribute AsyncMocks shadow the class methods ``_get_session``
    and ``_ensure_credentials``; L2 credential attributes default to a
    complete, valid set so the real header/signature code can run.
    """
    servicer = object.__new__(PolymarketServiceServicer)
    servicer._last_credentials_failure = None
    servicer._wallet_address = WALLET
    servicer._api_key = "unit-api-key"
    servicer._api_secret = API_SECRET_B64
    servicer._api_passphrase = "unit-passphrase"
    servicer._get_session = AsyncMock(return_value=session)
    servicer._ensure_credentials = AsyncMock(return_value=True)
    for name, value in overrides.items():
        setattr(servicer, name, value)
    return servicer


# ---------------------------------------------------------------------------
# Unauthenticated request construction + success parsing
# ---------------------------------------------------------------------------


class TestUnauthenticatedRequests:
    @pytest.mark.asyncio
    async def test_get_success_returns_parsed_json(self) -> None:
        session = _make_session(_FakeResponse(status=200, json_data={"markets": [1, 2]}))
        servicer = _make_servicer(session)

        success, data, error = await servicer._request("GET", BASE_URL, "/markets")

        assert success is True
        assert data == {"markets": [1, 2]}
        assert error is None
        kwargs = session.request.call_args.kwargs
        assert kwargs["method"] == "GET"
        assert kwargs["url"] == f"{BASE_URL}/markets"
        assert kwargs["params"] is None
        assert kwargs["data"] is None
        assert kwargs["headers"] == {"Content-Type": "application/json"}
        servicer._ensure_credentials.assert_not_called()

    @pytest.mark.asyncio
    async def test_params_forwarded_to_session(self) -> None:
        session = _make_session(_FakeResponse(json_data=[]))
        servicer = _make_servicer(session)
        params = {"next_cursor": "abc", "limit": 10}

        success, _, _ = await servicer._request("GET", BASE_URL, "/trades", params=params)

        assert success is True
        assert session.request.call_args.kwargs["params"] == params

    @pytest.mark.asyncio
    async def test_json_body_serialized_compact(self) -> None:
        session = _make_session(_FakeResponse(json_data={"ok": True}))
        servicer = _make_servicer(session)

        success, _, _ = await servicer._request(
            "POST", BASE_URL, "/order", json_body={"a": 1, "b": [1, 2]}
        )

        assert success is True
        # Compact separators — no spaces — per the L2 signing contract.
        assert session.request.call_args.kwargs["data"] == '{"a":1,"b":[1,2]}'

    @pytest.mark.asyncio
    async def test_empty_dict_json_body_is_treated_as_no_body(self) -> None:
        """Documents actual behavior: a falsy ``{}`` body sends data=None."""
        session = _make_session(_FakeResponse(json_data={}))
        servicer = _make_servicer(session)

        success, _, _ = await servicer._request("POST", BASE_URL, "/order", json_body={})

        assert success is True
        assert session.request.call_args.kwargs["data"] is None

    @pytest.mark.asyncio
    async def test_non_dict_json_payload_is_returned_verbatim(self) -> None:
        session = _make_session(_FakeResponse(json_data=[{"id": "t1"}]))
        servicer = _make_servicer(session)

        success, data, error = await servicer._request("GET", BASE_URL, "/tape")

        assert (success, data, error) == (True, [{"id": "t1"}], None)


# ---------------------------------------------------------------------------
# Authenticated path: credential gate + L2 header construction
# ---------------------------------------------------------------------------


class TestAuthenticatedRequests:
    @pytest.mark.asyncio
    async def test_credentials_unavailable_surfaces_captured_reason(self) -> None:
        session = _make_session()
        servicer = _make_servicer(session)
        servicer._ensure_credentials = AsyncMock(return_value=False)
        servicer._last_credentials_failure = "signer service returned 503"

        success, data, error = await servicer._request(
            "GET", BASE_URL, "/orders", authenticated=True
        )

        assert success is False
        assert data is None
        assert error == "Polymarket credentials unavailable: signer service returned 503"
        session.request.assert_not_called()

    @pytest.mark.asyncio
    async def test_credentials_unavailable_without_reason_uses_default(self) -> None:
        session = _make_session()
        servicer = _make_servicer(session)
        servicer._ensure_credentials = AsyncMock(return_value=False)
        servicer._last_credentials_failure = None

        success, data, error = await servicer._request(
            "GET", BASE_URL, "/orders", authenticated=True
        )

        assert success is False
        assert data is None
        assert error == "Polymarket credentials unavailable: Polymarket credentials not configured"

    @pytest.mark.asyncio
    async def test_l2_header_value_error_is_returned_not_raised(self) -> None:
        """Real ``_build_l2_headers`` raises when a credential attr is empty."""
        session = _make_session()
        servicer = _make_servicer(session, _api_key=None)

        success, data, error = await servicer._request(
            "DELETE", BASE_URL, "/order", authenticated=True
        )

        assert success is False
        assert data is None
        assert error == "Polymarket L2 credentials missing: api_key"
        session.request.assert_not_called()

    @pytest.mark.asyncio
    async def test_l2_headers_merged_into_request(self) -> None:
        session = _make_session(_FakeResponse(json_data={"orders": []}))
        servicer = _make_servicer(session)

        success, _, _ = await servicer._request("GET", BASE_URL, "/orders", authenticated=True)

        assert success is True
        headers = session.request.call_args.kwargs["headers"]
        assert headers["Content-Type"] == "application/json"
        assert headers["POLY_ADDRESS"] == WALLET
        assert headers["POLY_API_KEY"] == "unit-api-key"
        assert headers["POLY_PASSPHRASE"] == "unit-passphrase"
        assert headers["POLY_TIMESTAMP"].isdigit()
        # Real HMAC signature: base64, decodes to 32 bytes (SHA-256).
        assert len(base64.b64decode(headers["POLY_SIGNATURE"])) == 32

    @pytest.mark.asyncio
    async def test_signed_path_includes_query_string_and_body(self) -> None:
        """Params and body must be folded into the signed L2 path/message."""
        session = _make_session(_FakeResponse(json_data={}))
        servicer = _make_servicer(session)
        captured: dict[str, object] = {}

        def fake_build_l2_headers(method: str, path: str, body: str = "") -> dict[str, str]:
            captured.update(method=method, path=path, body=body)
            return {"POLY_ADDRESS": WALLET}

        servicer._build_l2_headers = fake_build_l2_headers

        success, _, _ = await servicer._request(
            "POST",
            BASE_URL,
            "/order",
            params={"market": "m1", "limit": 5},
            json_body={"side": "BUY"},
            authenticated=True,
        )

        assert success is True
        assert captured["method"] == "POST"
        assert captured["path"] == "/order?market=m1&limit=5"
        assert captured["body"] == '{"side":"BUY"}'


# ---------------------------------------------------------------------------
# Error handling: HTTP status, JSON parse, transport failures
# ---------------------------------------------------------------------------


class TestErrorHandling:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [400, 404, 500])
    async def test_non_200_status_returns_error_with_body(self, status: int) -> None:
        session = _make_session(_FakeResponse(status=status, text="upstream said no"))
        servicer = _make_servicer(session)

        success, data, error = await servicer._request("GET", BASE_URL, "/markets")

        assert success is False
        assert data is None
        assert error == f"HTTP {status}: upstream said no"

    @pytest.mark.asyncio
    async def test_non_200_error_text_truncated_to_500_chars(self) -> None:
        session = _make_session(_FakeResponse(status=500, text="x" * 600))
        servicer = _make_servicer(session)

        _, _, error = await servicer._request("GET", BASE_URL, "/markets")

        assert error == "HTTP 500: " + "x" * 500

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "json_exc",
        [
            pytest.param(
                aiohttp.ContentTypeError(request_info=MagicMock(), history=()),
                id="content-type-error",
            ),
            pytest.param(json.JSONDecodeError("Expecting value", "doc", 0), id="json-decode-error"),
            pytest.param(ValueError("not json"), id="value-error"),
        ],
    )
    async def test_200_with_unparseable_body_returns_json_parse_error(
        self, json_exc: Exception
    ) -> None:
        session = _make_session(_FakeResponse(status=200, json_exc=json_exc))
        servicer = _make_servicer(session)

        success, data, error = await servicer._request("GET", BASE_URL, "/markets")

        assert success is False
        assert data is None
        assert error is not None
        assert error.startswith("JSON parse error: ")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "transport_exc",
        [
            pytest.param(TimeoutError("deadline exceeded"), id="timeout"),
            pytest.param(aiohttp.ClientConnectionError("connection refused"), id="conn-refused"),
            pytest.param(aiohttp.ClientError("generic client failure"), id="client-error"),
        ],
    )
    async def test_transport_errors_returned_as_error_string(
        self, transport_exc: Exception
    ) -> None:
        session = _make_session(request_exc=transport_exc)
        servicer = _make_servicer(session)

        success, data, error = await servicer._request("GET", BASE_URL, "/markets")

        assert success is False
        assert data is None
        assert error == str(transport_exc)
