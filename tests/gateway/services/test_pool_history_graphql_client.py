"""Unit tests for ``GatewayGraphQLClient.query`` (VIB-4753 / POOL-5).

The dispatcher owns rate-limit + budget policy, so this transport client is
deliberately thin: a single ``query`` method that POSTs one GraphQL request
and maps every HTTP / GraphQL failure onto the preserved exception taxonomy
(``SubgraphRateLimitError`` / ``SubgraphQueryError`` / ``SubgraphConnectionError``).

These tests exercise every branch of ``query`` with a mocked
``aiohttp.ClientSession`` (NO network):

- 200 success -> returns the ``data`` object, stats incremented
- variables threaded into the POST payload
- non-dict / missing-``data`` JSON -> empty dict (Empty != Zero is upstream;
  here a shapeless body simply yields ``{}``)
- HTTP 429 -> ``SubgraphRateLimitError`` with the three Retry-After shapes
  (numeric / non-numeric HTTP-date / absent) all handled without leaking a
  ValueError
- non-200 -> ``SubgraphQueryError`` carrying the status + truncated body
- GraphQL ``errors`` array on a 200 -> ``SubgraphQueryError`` preserving the
  raw errors
- transport ``aiohttp.ClientError`` -> ``SubgraphConnectionError``
- Authorization header is built per-request from the api_key and never logged
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from almanak.gateway.data.pool_history._graphql import (
    DEFAULT_TIMEOUT_SECONDS,
    GatewayGraphQLClient,
    SubgraphConnectionError,
    SubgraphQueryError,
    SubgraphRateLimitError,
    _mask_api_key,
)

_URL = "https://gateway.thegraph.com/api/subgraphs/id/test-subgraph"
_QUERY = "{ pools { id } }"


def _mock_session_returning(response: MagicMock) -> MagicMock:
    """Build a mock ``ClientSession`` whose ``post(...)`` yields ``response``.

    ``session.post(...)`` is used as an async context manager
    (``async with session.post(...) as response``), so the returned object
    must implement ``__aenter__`` / ``__aexit__``.
    """
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.post = MagicMock(return_value=context)
    return session


def _patch_session(client: GatewayGraphQLClient, session: MagicMock):
    # ``_get_session`` is async; patch it with an AsyncMock so
    # ``await self._get_session()`` resolves to our mock session.
    return patch.object(client, "_get_session", AsyncMock(return_value=session))


# =============================================================================
# Successful query execution
# =============================================================================


class TestSuccessfulQuery:
    @pytest.mark.asyncio
    async def test_returns_data_object(self):
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"data": {"pools": [{"id": "0x1"}]}})

        with _patch_session(client, _mock_session_returning(response)):
            result = await client.query(url=_URL, query=_QUERY)

        assert result == {"pools": [{"id": "0x1"}]}
        assert client._stats.total_queries == 1
        assert client._stats.successful_queries == 1
        assert client._stats.failed_queries == 0
        await client.close()

    @pytest.mark.asyncio
    async def test_variables_threaded_into_payload(self):
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"data": {"pool": {"id": "0x2"}}})
        session = _mock_session_returning(response)

        with _patch_session(client, session):
            await client.query(
                url=_URL,
                query="query($id: ID!) { pool(id: $id) { id } }",
                variables={"id": "0x2"},
            )

        # The POST payload must carry both the query and the variables.
        _, kwargs = session.post.call_args
        assert kwargs["json"]["query"].startswith("query($id")
        assert kwargs["json"]["variables"] == {"id": "0x2"}
        await client.close()

    @pytest.mark.asyncio
    async def test_no_variables_omits_variables_key(self):
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"data": {}})
        session = _mock_session_returning(response)

        with _patch_session(client, session):
            await client.query(url=_URL, query=_QUERY)

        _, kwargs = session.post.call_args
        assert "variables" not in kwargs["json"]
        await client.close()

    @pytest.mark.asyncio
    async def test_missing_data_field_returns_empty_dict(self):
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"extensions": {}})  # no "data"

        with _patch_session(client, _mock_session_returning(response)):
            result = await client.query(url=_URL, query=_QUERY)

        assert result == {}
        assert client._stats.successful_queries == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_non_dict_json_returns_empty_dict(self):
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value=["unexpected", "list"])

        with _patch_session(client, _mock_session_returning(response)):
            result = await client.query(url=_URL, query=_QUERY)

        assert result == {}
        await client.close()

    @pytest.mark.asyncio
    async def test_authorization_header_present_with_api_key(self):
        client = GatewayGraphQLClient(api_key="secret-token-1234567890")
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"data": {}})
        session = _mock_session_returning(response)

        with _patch_session(client, session):
            await client.query(url=_URL, query=_QUERY)

        _, kwargs = session.post.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer secret-token-1234567890"
        assert kwargs["headers"]["Content-Type"] == "application/json"
        await client.close()

    @pytest.mark.asyncio
    async def test_no_authorization_header_without_api_key(self):
        client = GatewayGraphQLClient(api_key=None)
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"data": {}})
        session = _mock_session_returning(response)

        with _patch_session(client, session):
            await client.query(url=_URL, query=_QUERY)

        _, kwargs = session.post.call_args
        assert "Authorization" not in kwargs["headers"]
        await client.close()


# =============================================================================
# HTTP 429 — rate limit, with the three Retry-After shapes
# =============================================================================


class TestRateLimit:
    @pytest.mark.asyncio
    async def test_429_numeric_retry_after(self):
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 429
        response.headers = {"Retry-After": "60"}

        with _patch_session(client, _mock_session_returning(response)):
            with pytest.raises(SubgraphRateLimitError) as exc:
                await client.query(url=_URL, query=_QUERY)

        assert exc.value.retry_after_seconds == 60.0
        assert client._stats.rate_limited_queries == 1
        assert client._stats.failed_queries == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_429_non_numeric_retry_after_does_not_raise_valueerror(self):
        # An HTTP-date Retry-After must NOT mask the clean rate-limit error.
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 429
        response.headers = {"Retry-After": "Wed, 21 Oct 2025 07:28:00 GMT"}

        with _patch_session(client, _mock_session_returning(response)):
            with pytest.raises(SubgraphRateLimitError) as exc:
                await client.query(url=_URL, query=_QUERY)

        assert exc.value.retry_after_seconds is None
        await client.close()

    @pytest.mark.asyncio
    async def test_429_no_retry_after_header(self):
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 429
        response.headers = {}

        with _patch_session(client, _mock_session_returning(response)):
            with pytest.raises(SubgraphRateLimitError) as exc:
                await client.query(url=_URL, query=_QUERY)

        assert exc.value.retry_after_seconds is None
        await client.close()


# =============================================================================
# Non-200 and GraphQL-errors -> SubgraphQueryError
# =============================================================================


class TestQueryErrors:
    @pytest.mark.asyncio
    async def test_non_200_raises_query_error_with_status(self):
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 500
        response.text = AsyncMock(return_value="Internal Server Error")

        with _patch_session(client, _mock_session_returning(response)):
            with pytest.raises(SubgraphQueryError, match="HTTP 500") as exc:
                await client.query(url=_URL, query=_QUERY)

        assert exc.value.query == _QUERY
        assert client._stats.failed_queries == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_graphql_errors_array_raises_query_error(self):
        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(
            return_value={
                "errors": [
                    {"message": "Cannot query field 'bogus'"},
                    {"message": "Syntax error"},
                ]
            }
        )

        with _patch_session(client, _mock_session_returning(response)):
            with pytest.raises(SubgraphQueryError) as exc:
                await client.query(url=_URL, query=_QUERY)

        assert "Cannot query field 'bogus'" in str(exc.value)
        assert len(exc.value.errors) == 2
        assert exc.value.query == _QUERY
        assert client._stats.failed_queries == 1
        await client.close()


# =============================================================================
# Transport failure -> SubgraphConnectionError
# =============================================================================


class TestConnectionError:
    @pytest.mark.asyncio
    async def test_client_error_raises_connection_error(self):
        client = GatewayGraphQLClient()
        session = MagicMock()
        session.post = MagicMock(side_effect=aiohttp.ClientError("Connection refused"))

        with _patch_session(client, session):
            with pytest.raises(SubgraphConnectionError, match="Connection failed"):
                await client.query(url=_URL, query=_QUERY)

        assert client._stats.failed_queries == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_json_decode_error_maps_to_query_error(self):
        # A 200 with a malformed body raises json.JSONDecodeError (a ValueError),
        # which is NOT an aiohttp.ClientError. It must be mapped into the
        # taxonomy as SubgraphQueryError, not escape query() unhandled.
        import json

        client = GatewayGraphQLClient()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(side_effect=json.JSONDecodeError("boom", "", 0))

        with _patch_session(client, _mock_session_returning(response)):
            with pytest.raises(SubgraphQueryError, match="Response parsing failed") as exc:
                await client.query(url=_URL, query=_QUERY)

        assert exc.value.query == _QUERY
        assert client._stats.failed_queries == 1
        await client.close()


# =============================================================================
# API-key masking (never leak the bearer token)
# =============================================================================


class TestApiKeyMasking:
    def test_mask_none(self):
        assert _mask_api_key(None) == "not_set"

    def test_mask_short_key(self):
        assert _mask_api_key("short") == "***"

    def test_mask_long_key(self):
        assert _mask_api_key("abcdefghijklmnop") == "abcd...mnop"

    def test_repr_never_leaks_raw_key(self):
        client = GatewayGraphQLClient(api_key="abcdefghijklmnop")
        rendered = repr(client)
        assert "abcdefghijklmnop" not in rendered
        assert "abcd...mnop" in rendered
        assert str(DEFAULT_TIMEOUT_SECONDS) in rendered


# =============================================================================
# Session lifecycle
# =============================================================================


class TestSessionLifecycle:
    @pytest.mark.asyncio
    async def test_close_is_idempotent_without_session(self):
        client = GatewayGraphQLClient()
        await client.close()
        await client.close()  # must not raise

    @pytest.mark.asyncio
    async def test_close_closes_open_session(self):
        client = GatewayGraphQLClient()
        mock_session = AsyncMock()
        mock_session.closed = False
        client._session = mock_session

        await client.close()

        mock_session.close.assert_awaited_once()
        assert client._session is None
