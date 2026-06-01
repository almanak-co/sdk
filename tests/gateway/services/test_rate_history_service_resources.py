"""Gateway-boundary tests for RateHistoryService resource lifecycle.

Covers the CodeRabbit PR #2474 re-review hardening on
``almanak/gateway/services/rate_history_service.py``:

1. Lazy resource builders (``_get_http_session`` / ``_get_web3``) use
   double-checked locking under ``_resource_init_lock`` so concurrent gRPC
   requests reuse a single shared ``ClientSession`` / ``AsyncWeb3`` instead
   of racing the check-and-create and orphaning sockets.
2. An unexpected exception inside an RPC handler maps to
   ``grpc.StatusCode.INTERNAL`` with a *sanitized* client-facing message —
   raw exception text (RPC URLs, credentials, stack traces) never crosses
   the gateway boundary.

These guard the security perimeter per AGENTS.md §"Gateway is the security
boundary" (changes under ``almanak/gateway/**`` require ``tests/gateway/``
coverage).
"""

from __future__ import annotations

import asyncio
from typing import Any

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import (
    _INTERNAL_ERROR_DETAIL,
    RateHistoryServiceServicer,
    RateHistoryUnavailable,
)


class _MockContext:
    """Captures ``(code, details)`` set by the servicer."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


# =============================================================================
# Locking: shared resources are built exactly once under concurrency
# =============================================================================


def test_get_http_session_reuses_single_instance_under_concurrency(monkeypatch) -> None:
    """50 concurrent ``_get_http_session`` calls build exactly one session."""
    servicer = RateHistoryServiceServicer(GatewaySettings())

    create_count = 0

    class _FakeSession:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            nonlocal create_count
            create_count += 1
            self.closed = False

    # Avoid real socket / SSL work — count constructions only.
    monkeypatch.setattr(
        "almanak.gateway.services.rate_history_service.aiohttp.ClientSession",
        _FakeSession,
    )
    monkeypatch.setattr(
        "almanak.gateway.services.rate_history_service.aiohttp.TCPConnector",
        lambda *a, **k: object(),
    )
    monkeypatch.setattr(
        "almanak.gateway.services.rate_history_service.build_ssl_context",
        lambda: None,
    )

    async def _hammer() -> list[Any]:
        return await asyncio.gather(*[servicer._get_http_session() for _ in range(50)])

    sessions = asyncio.run(_hammer())

    # All callers got the same instance, built exactly once.
    assert create_count == 1
    assert len({id(s) for s in sessions}) == 1


def test_get_web3_reuses_single_instance_per_chain_under_concurrency(monkeypatch) -> None:
    """Concurrent ``_get_web3('ethereum')`` calls build one AsyncWeb3."""
    servicer = RateHistoryServiceServicer(GatewaySettings())

    create_count = 0

    class _FakeWeb3:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            nonlocal create_count
            create_count += 1

    # ``_get_web3`` imports ``web3`` and ``get_rpc_url`` lazily inside the
    # locked section, so patch them at their source modules.
    import web3 as web3_mod

    import almanak.gateway.utils as gw_utils

    monkeypatch.setattr(web3_mod, "AsyncWeb3", _FakeWeb3)
    monkeypatch.setattr(web3_mod, "AsyncHTTPProvider", lambda *a, **k: object())
    monkeypatch.setattr(gw_utils, "get_rpc_url", lambda chain, network=None: "http://rpc.test")
    monkeypatch.setattr(
        "almanak.gateway.services.rate_history_service.build_ssl_context",
        lambda: None,
    )

    async def _hammer() -> list[Any]:
        return await asyncio.gather(*[servicer._get_web3("ethereum") for _ in range(50)])

    instances = asyncio.run(_hammer())

    assert create_count == 1
    assert len({id(w) for w in instances}) == 1


# =============================================================================
# Error mapping: unexpected exceptions sanitize to INTERNAL
# =============================================================================


class _ExplodingLendingProvider:
    """Lending provider whose fetch raises a leaky unexpected error."""

    SECRET = "https://secret-rpc.example/key=DEADBEEF stacktrace leak"

    def lending_supported_chains(self) -> frozenset[str]:
        return frozenset({"ethereum"})

    async def fetch_lending_current(self, servicer: Any, **kwargs: Any) -> Any:
        raise RuntimeError(self.SECRET)


def test_unexpected_exception_maps_to_internal_with_sanitized_message() -> None:
    """A handler-internal RuntimeError → INTERNAL + sanitized message, no leak."""
    servicer = RateHistoryServiceServicer(GatewaySettings())
    provider = _ExplodingLendingProvider()
    # Register directly so validation passes and the fetch path runs.
    servicer._lending_providers["aave_v3"] = provider  # type: ignore[assignment]

    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="aave_v3",
        chain="ethereum",
        asset_symbol="USDC",
        side="supply",
    )
    ctx = _MockContext()
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INTERNAL
    assert ctx.details == _INTERNAL_ERROR_DETAIL
    assert response.success is False
    assert response.error == _INTERNAL_ERROR_DETAIL
    # The raw exception text (secret RPC URL / stack hint) must NOT leak.
    assert "secret-rpc" not in response.error
    assert "DEADBEEF" not in response.error
    assert "secret-rpc" not in ctx.details


def test_rate_history_unavailable_is_not_internal() -> None:
    """A RateHistoryUnavailable is a clean envelope, NOT an INTERNAL error."""

    class _UnavailableProvider:
        def lending_supported_chains(self) -> frozenset[str]:
            return frozenset({"ethereum"})

        async def fetch_lending_current(self, servicer: Any, **kwargs: Any) -> Any:
            raise RateHistoryUnavailable(source="on_chain", reason="reserve not found")

    servicer = RateHistoryServiceServicer(GatewaySettings())
    servicer._lending_providers["aave_v3"] = _UnavailableProvider()  # type: ignore[assignment]

    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="aave_v3",
        chain="ethereum",
        asset_symbol="USDC",
        side="supply",
    )
    ctx = _MockContext()
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    # Unavailable is an expected outcome: no INTERNAL code, success=False
    # envelope carrying the (non-secret) reason and source.
    assert ctx.code is None
    assert response.success is False
    assert response.source == "on_chain"
    assert "reserve not found" in response.error
