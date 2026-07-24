"""Unit tests for TokenSafetyClient._check_rugcheck (token_safety/client.py).

Branch coverage for the RugCheck transport wrapper: success with and
without an API key header, empty/malformed payload defaults, the
pre-request rate-limit sleep, 429 retry-then-success with backoff, 429
with retries exhausted, non-200 responses, and aiohttp.ClientError.

The transport seam is a fake session injected directly on the client
(``_get_session`` returns any non-closed session unchanged), so no
sockets are opened. Sleeps are recorded by swapping the module-level
``asyncio`` binding for a namespace whose ``sleep`` records instead of
waiting.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest

import almanak.framework.data.token_safety.client as client_module
from almanak.framework.data.token_safety.client import TokenSafetyClient
from almanak.framework.data.token_safety.models import RiskLevel, RugCheckResult

MINT = "Mint111111111111111111111111111111111111111"

REPORT_PAYLOAD: dict[str, Any] = {
    "score": 50,
    "rugged": False,
    "risks": [
        {"name": "mutable_metadata", "description": "Metadata can change", "level": "warn"},
    ],
    "tokenMeta": {"symbol": "TKN", "name": "Token"},
    "fileMeta": {"name": "Token File"},
    "totalMarketLiquidity": 123.5,
}


class _FakeResponse:
    """Async-context-manager response with scripted status/body/json."""

    def __init__(self, status: int = 200, payload: dict[str, Any] | None = None, body: str = "") -> None:
        self.status = status
        self._payload = payload if payload is not None else {}
        self._body = body

    async def text(self) -> str:
        return self._body

    async def json(self) -> dict[str, Any]:
        return self._payload

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeSession:
    """Scripted GET transport: responses are returned, exceptions raised."""

    closed = False

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, headers: Any = None) -> Any:
        self.calls.append({"url": url, "headers": dict(headers or {})})
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _make_client(script: list[Any], *, api_key: str | None = None) -> tuple[TokenSafetyClient, _FakeSession]:
    """Build a client without running __init__ (skips load_config)."""
    client = object.__new__(TokenSafetyClient)
    client._request_timeout = 15.0
    client._cache_ttl = 300
    client._rugcheck_api_key = api_key
    client._cache = {}
    client._last_rugcheck_request = 0.0
    client._last_goplus_request = 0.0
    session = _FakeSession(script)
    client._session = session
    return client, session


def _patch_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Swap the module's asyncio binding so sleeps record instead of wait."""
    sleeps: list[float] = []

    async def _sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(client_module, "asyncio", SimpleNamespace(sleep=_sleep))
    return sleeps


class TestCheckRugcheckSuccess:
    def test_parses_full_report_with_api_key_header(self):
        client, session = _make_client([_FakeResponse(200, REPORT_PAYLOAD)], api_key="key-123")

        result = asyncio.run(client._check_rugcheck(MINT))

        assert isinstance(result, RugCheckResult)
        assert result.score == 50
        assert result.risk_level == RiskLevel.SAFE.value
        assert not result.rugged
        assert [(f.name, f.level) for f in result.risks] == [("mutable_metadata", RiskLevel.MEDIUM)]
        assert result.token_name == "Token File"
        assert result.token_symbol == "TKN"
        assert result.total_market_liquidity == 123.5
        assert result.raw_response == REPORT_PAYLOAD

        assert len(session.calls) == 1
        call = session.calls[0]
        assert call["url"] == f"{client_module.RUGCHECK_BASE_URL}/v1/tokens/{MINT}/report"
        assert call["headers"] == {"X-API-KEY": "key-123"}
        assert client._last_rugcheck_request > 0.0

    def test_no_api_key_sends_no_auth_header(self):
        client, session = _make_client([_FakeResponse(200, REPORT_PAYLOAD)])

        result = asyncio.run(client._check_rugcheck(MINT))

        assert isinstance(result, RugCheckResult)
        assert session.calls[0]["headers"] == {}

    def test_empty_payload_parses_to_defaults(self):
        client, _ = _make_client([_FakeResponse(200, {})])

        result = asyncio.run(client._check_rugcheck(MINT))

        assert isinstance(result, RugCheckResult)
        assert result.score == 0
        assert result.risk_level == RiskLevel.SAFE.value
        assert result.risks == []
        assert not result.rugged
        assert result.total_market_liquidity == 0.0

    def test_rugged_token_with_critical_score(self):
        payload = {"score": 950, "rugged": True, "risks": None}
        client, _ = _make_client([_FakeResponse(200, payload)])

        result = asyncio.run(client._check_rugcheck(MINT))

        assert result.rugged
        assert result.risk_level == RiskLevel.CRITICAL.value


class TestCheckRugcheckRateLimit:
    def test_waits_before_request_when_called_too_soon(self, monkeypatch):
        sleeps = _patch_sleep(monkeypatch)
        client, session = _make_client([_FakeResponse(200, REPORT_PAYLOAD)])
        client._last_rugcheck_request = time.time()

        result = asyncio.run(client._check_rugcheck(MINT))

        assert isinstance(result, RugCheckResult)
        assert len(session.calls) == 1
        assert len(sleeps) == 1
        assert 0.0 < sleeps[0] <= client_module._RUGCHECK_RATE_LIMIT

    def test_429_retries_with_backoff_then_succeeds(self, monkeypatch):
        sleeps = _patch_sleep(monkeypatch)
        client, session = _make_client(
            [_FakeResponse(429), _FakeResponse(200, REPORT_PAYLOAD)],
        )

        result = asyncio.run(client._check_rugcheck(MINT))

        assert isinstance(result, RugCheckResult)
        assert result.score == 50
        assert len(session.calls) == 2
        # attempt 0 backoff = _RUGCHECK_RATE_LIMIT * 2
        assert client_module._RUGCHECK_RATE_LIMIT * 2 in sleeps

    def test_429_exhausts_retries_returns_none(self, monkeypatch):
        _patch_sleep(monkeypatch)
        client, session = _make_client([_FakeResponse(429)] * 3)

        result = asyncio.run(client._check_rugcheck(MINT))

        assert result is None
        assert len(session.calls) == 3


class TestCheckRugcheckFailures:
    def test_non_200_returns_none(self):
        client, session = _make_client([_FakeResponse(500, body="server oops")])

        result = asyncio.run(client._check_rugcheck(MINT))

        assert result is None
        assert len(session.calls) == 1

    def test_client_error_returns_none(self):
        client, session = _make_client([aiohttp.ClientError("connection refused")])

        result = asyncio.run(client._check_rugcheck(MINT))

        assert result is None
        assert len(session.calls) == 1
