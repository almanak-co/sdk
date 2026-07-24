"""Unit tests for TokenSafetyClient._check_goplus (token_safety/client.py).

Branch coverage for the GoPlus transport wrapper: success parse, error-code
payload passthrough, the pre-request rate-limit sleep, 429 retry-then-success
with backoff, 429 with retries exhausted, non-200 responses, and
aiohttp.ClientError. Mirrors test_token_safety_rugcheck.py: the transport
seam is a fake session injected directly on the client (``_get_session``
returns any non-closed session unchanged), so no sockets are opened. Sleeps
are recorded by swapping the module-level ``asyncio`` binding for a
namespace whose ``sleep`` records instead of waiting.
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
from almanak.framework.data.token_safety.models import GoPlusResult

MINT = "Mint111111111111111111111111111111111111111"

SECURITY_PAYLOAD: dict[str, Any] = {
    "code": 1,
    "message": "OK",
    "result": {
        MINT: {
            "mintable": {"status": "1"},
            "freezable": {"status": "0"},
            "closable": {"status": "0"},
            "balance_mutable_authority": {"status": "0"},
            "transfer_fee": {},
            "transfer_fee_upgradable": {"status": "0"},
            "transfer_hook": [],
            "transfer_hook_upgradable": {"status": "0"},
            "metadata_mutable": {"status": "1"},
            "non_transferable": "0",
            "default_account_state": "1",
            "trusted_token": 0,
            "holder_count": "1500",
            "holders": [{"percent": "12.5"}],
        },
    },
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

    def get(self, url: str, params: Any = None) -> Any:
        self.calls.append({"url": url, "params": dict(params or {})})
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _make_client(script: list[Any]) -> tuple[TokenSafetyClient, _FakeSession]:
    """Build a client without running __init__ (skips load_config)."""
    client = object.__new__(TokenSafetyClient)
    client._request_timeout = 15.0
    client._cache_ttl = 300
    client._rugcheck_api_key = None
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


class TestCheckGoplusSuccess:
    def test_parses_token_security_payload(self):
        client, session = _make_client([_FakeResponse(200, SECURITY_PAYLOAD)])

        result = asyncio.run(client._check_goplus(MINT))

        assert isinstance(result, GoPlusResult)
        assert result.mintable is True
        assert result.freezable is False
        assert result.metadata_mutable is True
        assert result.holder_count == 1500
        assert result.top_holder_pct == 12.5
        assert result.raw_response == SECURITY_PAYLOAD["result"][MINT]

        assert len(session.calls) == 1
        call = session.calls[0]
        assert call["url"] == f"{client_module.GOPLUS_BASE_URL}/api/v1/solana/token_security"
        assert call["params"] == {"contract_addresses": MINT}
        assert client._last_goplus_request > 0.0

    def test_error_code_payload_returns_none(self):
        """HTTP 200 with a GoPlus error code parses to None (no data)."""
        client, session = _make_client([_FakeResponse(200, {"code": 0, "message": "nope"})])

        result = asyncio.run(client._check_goplus(MINT))

        assert result is None
        assert len(session.calls) == 1


class TestCheckGoplusRateLimit:
    def test_waits_before_request_when_called_too_soon(self, monkeypatch):
        sleeps = _patch_sleep(monkeypatch)
        client, session = _make_client([_FakeResponse(200, SECURITY_PAYLOAD)])
        client._last_goplus_request = time.time()

        result = asyncio.run(client._check_goplus(MINT))

        assert isinstance(result, GoPlusResult)
        assert len(session.calls) == 1
        assert len(sleeps) == 1
        assert 0.0 < sleeps[0] <= client_module._GOPLUS_RATE_LIMIT

    def test_429_retries_with_backoff_then_succeeds(self, monkeypatch):
        sleeps = _patch_sleep(monkeypatch)
        client, session = _make_client(
            [_FakeResponse(429), _FakeResponse(200, SECURITY_PAYLOAD)],
        )

        result = asyncio.run(client._check_goplus(MINT))

        assert isinstance(result, GoPlusResult)
        assert result.mintable is True
        assert len(session.calls) == 2
        # attempt 0 backoff = _GOPLUS_RATE_LIMIT * 2
        assert client_module._GOPLUS_RATE_LIMIT * 2 in sleeps

    def test_429_exhausts_retries_returns_none(self, monkeypatch):
        _patch_sleep(monkeypatch)
        client, session = _make_client([_FakeResponse(429)] * 3)

        result = asyncio.run(client._check_goplus(MINT))

        assert result is None
        assert len(session.calls) == 3


class TestCheckGoplusFailures:
    def test_non_200_returns_none(self):
        client, session = _make_client([_FakeResponse(500, body="server oops")])

        result = asyncio.run(client._check_goplus(MINT))

        assert result is None
        assert len(session.calls) == 1

    def test_client_error_returns_none(self):
        client, session = _make_client([aiohttp.ClientError("connection refused")])

        result = asyncio.run(client._check_goplus(MINT))

        assert result is None
        assert len(session.calls) == 1
