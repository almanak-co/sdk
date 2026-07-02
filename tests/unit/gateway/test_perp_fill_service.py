"""Unit tests for ``PerpFillServiceServicer`` — the gateway-side per-fill
economics + funding reader (VIB-5595).

Covers the two RPCs' observable branches so the registry-driven dispatch and
the Empty≠Zero proto mapping are pinned:

1. Registry construction — venue is resolved once, case-insensitively; a
   duplicate venue across two connectors is a hard error at construction.
2. ``GetUserFills`` / ``GetUserFunding`` guard branches — unknown venue and a
   missing ``wallet_address`` both stamp ``INVALID_ARGUMENT`` and return a
   ``success=False`` envelope (via the shared ``_resolve_request`` helper).
3. Connector failure paths — an exception in the fetch, and an ``ok=False``
   result, both map to ``UNAVAILABLE`` + ``success=False`` (a read fault, NOT a
   measured-empty book).
4. Happy paths — a measured empty book returns ``success=True`` with an empty
   list; populated results map field-for-field to the proto envelope, with
   Empty≠Zero preserved (an unreported ``fee`` stays ``""``, never ``"0"``).

The servicer talks to connectors only through the structural
``GatewayPerpFillsCapability`` Protocol, so tests use a minimal fake and patch
the module-level ``GATEWAY_REGISTRY`` — the global registry stays untouched.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import grpc
import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services import perp_fill_service as pfs
from almanak.gateway.services.perp_fill_service import (
    PerpFillData,
    PerpFillResult,
    PerpFillServiceServicer,
    PerpFundingData,
    PerpFundingResult,
)


# --------------------------------------------------------------------------- fakes
class _FakeContext:
    """Records the gRPC status code / details the servicer stamps."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str | None = None

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


class _FakeConnector:
    """Minimal ``GatewayPerpFillsCapability`` implementation.

    ``fills_result`` / ``funding_result`` are the values returned by the fetch
    methods; set either to an ``Exception`` instance to make that fetch raise.
    """

    def __init__(
        self,
        venue: str = "hyperliquid",
        fills_result: Any = None,
        funding_result: Any = None,
    ) -> None:
        self._venue = venue
        self._fills_result = fills_result if fills_result is not None else PerpFillResult()
        self._funding_result = (
            funding_result if funding_result is not None else PerpFundingResult()
        )
        self.fills_calls: list[dict[str, Any]] = []
        self.funding_calls: list[dict[str, Any]] = []

    def fills_venue(self) -> str:
        return self._venue

    async def fetch_user_fills(
        self, service: Any, *, wallet_address: str, coin: str, start_ts: int
    ) -> PerpFillResult:
        self.fills_calls.append(
            {"wallet_address": wallet_address, "coin": coin, "start_ts": start_ts}
        )
        if isinstance(self._fills_result, Exception):
            raise self._fills_result
        return self._fills_result

    async def fetch_user_funding(
        self, service: Any, *, wallet_address: str, coin: str, start_ts: int
    ) -> PerpFundingResult:
        self.funding_calls.append(
            {"wallet_address": wallet_address, "coin": coin, "start_ts": start_ts}
        )
        if isinstance(self._funding_result, Exception):
            raise self._funding_result
        return self._funding_result


def _settings() -> SimpleNamespace:
    return SimpleNamespace(network="mainnet")


def _servicer(monkeypatch: pytest.MonkeyPatch, *connectors: _FakeConnector) -> PerpFillServiceServicer:
    """Build a servicer whose registry yields exactly ``connectors``."""

    fake_registry = SimpleNamespace(capability_providers=lambda _cap: list(connectors))
    monkeypatch.setattr(pfs, "GATEWAY_REGISTRY", fake_registry)
    return PerpFillServiceServicer(_settings())  # type: ignore[arg-type]


# ------------------------------------------------------------------- construction
def test_registry_resolves_venue_case_insensitively(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(monkeypatch, _FakeConnector(venue="HyperLiquid"))
    assert "hyperliquid" in servicer._fills_providers


def test_duplicate_venue_raises_at_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(RuntimeError, match="Duplicate perp-fills provider"):
        _servicer(
            monkeypatch,
            _FakeConnector(venue="hyperliquid"),
            _FakeConnector(venue="hyperliquid"),
        )


# ----------------------------------------------------------------- guard branches
@pytest.mark.asyncio
async def test_get_fills_unknown_venue_is_invalid_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(monkeypatch, _FakeConnector(venue="hyperliquid"))
    ctx = _FakeContext()
    resp = await servicer.GetUserFills(
        gateway_pb2.UserFillsRequest(venue="unknown", wallet_address="0xabc"), ctx
    )
    assert resp.success is False
    assert "Unknown venue" in resp.error
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.asyncio
async def test_get_fills_missing_wallet_is_invalid_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(monkeypatch, _FakeConnector(venue="hyperliquid"))
    ctx = _FakeContext()
    resp = await servicer.GetUserFills(
        gateway_pb2.UserFillsRequest(venue="hyperliquid", wallet_address=""), ctx
    )
    assert resp.success is False
    assert "wallet_address is required" in resp.error
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.asyncio
async def test_get_funding_unknown_venue_is_invalid_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(monkeypatch, _FakeConnector(venue="hyperliquid"))
    ctx = _FakeContext()
    resp = await servicer.GetUserFunding(
        gateway_pb2.UserFundingRequest(venue="nope", wallet_address="0xabc"), ctx
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.asyncio
async def test_get_funding_missing_wallet_is_invalid_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(monkeypatch, _FakeConnector(venue="hyperliquid"))
    ctx = _FakeContext()
    resp = await servicer.GetUserFunding(
        gateway_pb2.UserFundingRequest(venue="hyperliquid", wallet_address=""), ctx
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT


# ------------------------------------------------------------------ failure paths
@pytest.mark.asyncio
async def test_get_fills_fetch_exception_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(
        monkeypatch, _FakeConnector(venue="hyperliquid", fills_result=RuntimeError("boom"))
    )
    ctx = _FakeContext()
    resp = await servicer.GetUserFills(
        gateway_pb2.UserFillsRequest(venue="hyperliquid", wallet_address="0xabc"), ctx
    )
    assert resp.success is False
    assert "boom" in resp.error
    assert ctx.code == grpc.StatusCode.UNAVAILABLE


@pytest.mark.asyncio
async def test_get_fills_not_ok_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(
        monkeypatch,
        _FakeConnector(
            venue="hyperliquid", fills_result=PerpFillResult(ok=False, error="rpc down")
        ),
    )
    ctx = _FakeContext()
    resp = await servicer.GetUserFills(
        gateway_pb2.UserFillsRequest(venue="hyperliquid", wallet_address="0xabc"), ctx
    )
    assert resp.success is False
    assert resp.error == "rpc down"
    assert ctx.code == grpc.StatusCode.UNAVAILABLE


@pytest.mark.asyncio
async def test_get_funding_fetch_exception_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(
        monkeypatch,
        _FakeConnector(venue="hyperliquid", funding_result=RuntimeError("kaput")),
    )
    ctx = _FakeContext()
    resp = await servicer.GetUserFunding(
        gateway_pb2.UserFundingRequest(venue="hyperliquid", wallet_address="0xabc"), ctx
    )
    assert resp.success is False
    assert "kaput" in resp.error
    assert ctx.code == grpc.StatusCode.UNAVAILABLE


@pytest.mark.asyncio
async def test_get_funding_not_ok_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(
        monkeypatch,
        _FakeConnector(
            venue="hyperliquid",
            funding_result=PerpFundingResult(ok=False, error="funding read failed"),
        ),
    )
    ctx = _FakeContext()
    resp = await servicer.GetUserFunding(
        gateway_pb2.UserFundingRequest(venue="hyperliquid", wallet_address="0xabc"), ctx
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.UNAVAILABLE


# --------------------------------------------------------------------- happy paths
@pytest.mark.asyncio
async def test_get_fills_empty_book_is_measured_success(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(
        monkeypatch, _FakeConnector(venue="hyperliquid", fills_result=PerpFillResult(ok=True))
    )
    ctx = _FakeContext()
    resp = await servicer.GetUserFills(
        gateway_pb2.UserFillsRequest(venue="hyperliquid", wallet_address="0xabc"), ctx
    )
    assert resp.success is True
    assert list(resp.fills) == []
    assert ctx.code is None  # measured empty book is not an error


@pytest.mark.asyncio
async def test_get_fills_maps_fields_and_preserves_empty_not_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fill = PerpFillData(
        coin="BTC",
        px="65000.5",
        sz="0.01",
        dir="Open Long",
        fee="",  # unreported fee stays empty — must NOT become "0"
        closed_pnl="12.34",
        oid="777",
        cloid="0xdeadbeef",
        time_ms=1_700_000_000_000,
        crossed=True,
        fee_token="USDC",
    )
    conn = _FakeConnector(venue="hyperliquid", fills_result=PerpFillResult(ok=True, fills=[fill]))
    servicer = _servicer(monkeypatch, conn)
    ctx = _FakeContext()
    resp = await servicer.GetUserFills(
        gateway_pb2.UserFillsRequest(
            venue="hyperliquid", wallet_address="0xabc", coin="BTC", start_time_ms=42
        ),
        ctx,
    )
    assert resp.success is True
    assert len(resp.fills) == 1
    out = resp.fills[0]
    assert out.coin == "BTC"
    assert out.px == "65000.5"
    assert out.closed_pnl == "12.34"
    assert out.cloid == "0xdeadbeef"
    assert out.crossed is True
    assert out.fee == ""  # Empty≠Zero preserved across the boundary
    # request args are forwarded to the connector unchanged
    assert conn.fills_calls == [{"wallet_address": "0xabc", "coin": "BTC", "start_ts": 42}]


@pytest.mark.asyncio
async def test_get_funding_maps_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    delta = PerpFundingData(coin="ETH", usdc="-1.5", funding_rate="0.0001", time_ms=99)
    conn = _FakeConnector(
        venue="hyperliquid", funding_result=PerpFundingResult(ok=True, deltas=[delta])
    )
    servicer = _servicer(monkeypatch, conn)
    ctx = _FakeContext()
    resp = await servicer.GetUserFunding(
        gateway_pb2.UserFundingRequest(venue="hyperliquid", wallet_address="0xabc"), ctx
    )
    assert resp.success is True
    assert len(resp.deltas) == 1
    assert resp.deltas[0].coin == "ETH"
    assert resp.deltas[0].usdc == "-1.5"
    assert resp.deltas[0].funding_rate == "0.0001"


@pytest.mark.asyncio
async def test_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer(monkeypatch, _FakeConnector(venue="hyperliquid"))
    # No session opened yet — close must not raise.
    await servicer.close()
    await servicer.close()
