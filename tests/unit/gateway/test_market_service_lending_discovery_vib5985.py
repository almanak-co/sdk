"""MarketService verified lending-market resolution (VIB-5985).

Proves the two new RPCs against a scripted eth_call layer (no chain):

* catalog listing + address-resolved token filters + LLTV filter,
* pagination (page_size + page_token),
* unsupported protocol → INVALID_ARGUMENT listing supported protocols,
* GetLendingMarket verify-PASS (verified=True / source=onchain_verify),
* verify-FAIL on recomputed-id mismatch → INVALID_ARGUMENT, NOT a silent record,
* not-found (zero loan token) → NOT_FOUND.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

import almanak.gateway.services.pt_rpc_adapter as pt_rpc_adapter
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer

# sUSDe/USDC 91.5% market — the exact market the incident strategy needed.
_SUSDE_USDC_ID = "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab"
_USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
_SUSDE = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
_ORACLE = "0x873CD44b860DEDFe139f93e12A4AcCa0926Ffb87"
_IRM = "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC"
_LLTV = 915000000000000000


class _Ctx:
    """Minimal grpc.aio ServicerContext stand-in capturing code + details."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self._details = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self._details = details

    def details(self) -> str:
        return self._details


def _servicer() -> MarketServiceServicer:
    svc = MarketServiceServicer.__new__(MarketServiceServicer)
    svc._lending_market_discovery_providers = None
    svc.settings = MagicMock(network="mainnet")
    return svc


def _word_addr(addr: str) -> str:
    return addr.lower().replace("0x", "").zfill(64)


def _word_uint(value: int) -> str:
    return hex(value)[2:].zfill(64)


def _idtomarketparams_payload(loan: str, collateral: str, oracle: str, irm: str, lltv: int) -> str:
    return "0x" + _word_addr(loan) + _word_addr(collateral) + _word_addr(oracle) + _word_addr(irm) + _word_uint(lltv)


def _patch_eth_call(monkeypatch, payload: str) -> None:
    async def _fake(to: str, data: str) -> str:
        return payload

    monkeypatch.setattr(pt_rpc_adapter, "build_gateway_eth_call", lambda **_: _fake)


# ---------------------------------------------------------------------------
# ListLendingMarkets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_catalog_candidate_unverified():
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(
            protocol="morpho_blue", chain="ethereum", collateral_token="sUSDe", loan_token="USDC"
        ),
        ctx,
    )
    assert resp.success is True
    assert resp.total_matches == 1
    (m,) = resp.markets
    assert m.market_id == _SUSDE_USDC_ID
    assert m.lltv_bps == 9150
    assert m.kind == gateway_pb2.LENDING_MARKET_KIND_ISOLATED_PAIR
    # Candidate: NOT verified, catalog source.
    assert m.verified is False
    assert m.source == gateway_pb2.LENDING_MARKET_SOURCE_CURATED_CATALOG
    assert m.loan_symbol == "USDC"
    assert m.collateral_symbol == "sUSDe"


@pytest.mark.asyncio
async def test_list_address_filter_resolves_same_as_symbol():
    svc = _servicer()
    by_symbol = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue", chain="ethereum", collateral_token="sUSDe"),
        _Ctx(),
    )
    by_address = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue", chain="ethereum", collateral_token=_SUSDE),
        _Ctx(),
    )
    ids_sym = sorted(m.market_id for m in by_symbol.markets)
    ids_addr = sorted(m.market_id for m in by_address.markets)
    assert ids_sym == ids_addr
    assert _SUSDE_USDC_ID in ids_sym


@pytest.mark.asyncio
async def test_list_lltv_filter():
    svc = _servicer()
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(
            protocol="morpho_blue", chain="ethereum", collateral_token="sUSDe", lltv_bps=9150
        ),
        _Ctx(),
    )
    assert resp.success is True
    assert all(m.lltv_bps == 9150 for m in resp.markets)
    assert _SUSDE_USDC_ID in {m.market_id for m in resp.markets}
    # A non-matching LLTV yields zero candidates (a legal, meaningful result).
    empty = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(
            protocol="morpho_blue", chain="ethereum", collateral_token="sUSDe", lltv_bps=1
        ),
        _Ctx(),
    )
    assert empty.success is True
    assert empty.total_matches == 0
    assert list(empty.markets) == []


@pytest.mark.asyncio
async def test_list_pagination_walks_all_matches():
    svc = _servicer()
    # Page through ALL ethereum markets with page_size=2 and assert the union
    # equals the single-shot listing, with correct next_page_token chaining.
    full = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue", chain="ethereum"), _Ctx()
    )
    total = full.total_matches
    assert total > 2

    seen: list[str] = []
    token = ""
    pages = 0
    while True:
        resp = await svc.ListLendingMarkets(
            gateway_pb2.ListLendingMarketsRequest(
                protocol="morpho_blue", chain="ethereum", page_size=2, page_token=token
            ),
            _Ctx(),
        )
        assert resp.total_matches == total
        assert len(resp.markets) <= 2
        seen.extend(m.market_id for m in resp.markets)
        token = resp.next_page_token
        pages += 1
        if not token:
            break
        assert pages < 100
    assert len(seen) == total
    assert sorted(seen) == sorted(m.market_id for m in full.markets)


@pytest.mark.asyncio
async def test_list_unsupported_protocol_lists_supported():
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="aave_v3", chain="ethereum"), ctx
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "unsupported protocol" in resp.error
    assert "morpho_blue" in resp.error


@pytest.mark.asyncio
async def test_list_unsupported_chain():
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue", chain="dogechain"), ctx
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "does not support chain" in ctx.details()


@pytest.mark.asyncio
async def test_list_requires_protocol_and_chain():
    svc = _servicer()
    r1 = await svc.ListLendingMarkets(gateway_pb2.ListLendingMarketsRequest(chain="ethereum"), _Ctx())
    assert r1.success is False and "protocol is required" in r1.error
    r2 = await svc.ListLendingMarkets(gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue"), _Ctx())
    assert r2.success is False and "chain is required" in r2.error


@pytest.mark.asyncio
async def test_list_invalid_page_token():
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue", chain="ethereum", page_token="notanint"),
        ctx,
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "invalid page_token" in resp.error


@pytest.mark.asyncio
async def test_list_negative_page_token():
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue", chain="ethereum", page_token="-1"),
        ctx,
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "non-negative" in ctx.details()


@pytest.mark.asyncio
async def test_list_page_size_capped():
    svc = _servicer()
    # A page_size above the server cap is clamped; a single-shot call still
    # returns every ethereum match (there are fewer than the cap).
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue", chain="ethereum", page_size=10_000),
        _Ctx(),
    )
    assert resp.success is True
    assert resp.next_page_token == ""
    assert len(resp.markets) == resp.total_matches


@pytest.mark.asyncio
async def test_list_unresolvable_filter_is_invalid_argument():
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue", chain="ethereum", loan_token="NOTATOKEN"),
        ctx,
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "cannot resolve token filter" in resp.error


@pytest.mark.asyncio
async def test_list_provider_exception_is_internal(monkeypatch):
    svc = _servicer()
    provider = svc._lending_discovery_providers()["morpho_blue"]

    def _boom(**_):
        raise RuntimeError("catalog exploded")

    monkeypatch.setattr(provider, "list_lending_markets", _boom)
    ctx = _Ctx()
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(protocol="morpho_blue", chain="ethereum"), ctx
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INTERNAL
    # Client-visible message is sanitized — the raw exception text (which could
    # embed an RPC target / upstream body) must NOT cross the boundary.
    assert resp.error == "internal error listing lending markets; see gateway logs"
    assert "catalog exploded" not in resp.error
    assert "catalog exploded" not in ctx.details()


# ---------------------------------------------------------------------------
# GetLendingMarket
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_verify_pass(monkeypatch):
    _patch_eth_call(monkeypatch, _idtomarketparams_payload(_USDC, _SUSDE, _ORACLE, _IRM, _LLTV))
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.GetLendingMarket(
        gateway_pb2.GetLendingMarketRequest(protocol="morpho_blue", chain="ethereum", market_id=_SUSDE_USDC_ID),
        ctx,
    )
    assert resp.success is True
    assert resp.market.verified is True
    assert resp.market.source == gateway_pb2.LENDING_MARKET_SOURCE_ONCHAIN_VERIFY
    assert resp.market.market_id == _SUSDE_USDC_ID
    assert resp.market.lltv_bps == 9150
    assert resp.market.loan_token == _USDC.lower()
    assert resp.market.collateral_token == _SUSDE.lower()


@pytest.mark.asyncio
async def test_get_verify_fail_on_id_mismatch_is_loud(monkeypatch):
    # Return params for a DIFFERENT market (mutated LLTV) so the recomputed id
    # cannot match the requested id — the RPC must reject, not silently return.
    _patch_eth_call(monkeypatch, _idtomarketparams_payload(_USDC, _SUSDE, _ORACLE, _IRM, 999000000000000000))
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.GetLendingMarket(
        gateway_pb2.GetLendingMarketRequest(protocol="morpho_blue", chain="ethereum", market_id=_SUSDE_USDC_ID),
        ctx,
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "verification failed" in resp.error
    # No market payload leaked.
    assert resp.market.market_id == ""
    assert resp.market.verified is False


@pytest.mark.asyncio
async def test_get_not_found_zero_loan_token(monkeypatch):
    _patch_eth_call(monkeypatch, "0x" + "0" * 320)
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.GetLendingMarket(
        gateway_pb2.GetLendingMarketRequest(protocol="morpho_blue", chain="ethereum", market_id=_SUSDE_USDC_ID),
        ctx,
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.NOT_FOUND


@pytest.mark.asyncio
async def test_get_generic_exception_is_sanitized(monkeypatch):
    # A transport-layer failure (e.g. GatewayPtRpcError embedding the RPC target
    # + upstream HTTP body) hits the generic catch-all → UNAVAILABLE with a
    # FIXED message; the sensitive text must NOT reach the caller.
    _patch_eth_call(monkeypatch, "0x" + "0" * 320)
    svc = _servicer()
    provider = svc._lending_discovery_providers()["morpho_blue"]
    secret = "eth_call transport error (to=0xDEADBEEF...): <html>upstream body 502</html>"

    async def _boom(**_):
        raise RuntimeError(secret)

    monkeypatch.setattr(provider, "verify_lending_market", _boom)
    ctx = _Ctx()
    resp = await svc.GetLendingMarket(
        gateway_pb2.GetLendingMarketRequest(protocol="morpho_blue", chain="ethereum", market_id=_SUSDE_USDC_ID),
        ctx,
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert resp.error == "lending-market verification unavailable; see gateway logs"
    assert secret not in resp.error
    assert secret not in ctx.details()
    assert "0xDEADBEEF" not in resp.error


@pytest.mark.asyncio
async def test_get_unsupported_protocol(monkeypatch):
    _patch_eth_call(monkeypatch, "0x" + "0" * 320)
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.GetLendingMarket(
        gateway_pb2.GetLendingMarketRequest(protocol="aave_v3", chain="ethereum", market_id=_SUSDE_USDC_ID),
        ctx,
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "unsupported protocol" in resp.error


@pytest.mark.asyncio
async def test_get_requires_market_id():
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.GetLendingMarket(
        gateway_pb2.GetLendingMarketRequest(protocol="morpho_blue", chain="ethereum"), ctx
    )
    assert resp.success is False
    assert "market_id is required" in resp.error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_id",
    [
        "0xNOTHEX" + "0" * 58,  # non-hex chars
        "0x85c7f4",  # too short
        _SUSDE_USDC_ID + "00",  # too long (33 bytes)
        _SUSDE_USDC_ID[2:],  # missing 0x prefix
        "85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab",  # 64 hex, no 0x
    ],
)
async def test_get_rejects_malformed_market_id(monkeypatch, bad_id):
    # Must be rejected up front as INVALID_ARGUMENT (caller bug), NOT reach the
    # eth_call path — so a stray eth_call here would be a bug.
    def _boom(**_):
        raise AssertionError("eth_call transport must not be built for a malformed id")

    monkeypatch.setattr(pt_rpc_adapter, "build_gateway_eth_call", _boom)
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.GetLendingMarket(
        gateway_pb2.GetLendingMarketRequest(protocol="morpho_blue", chain="ethereum", market_id=bad_id),
        ctx,
    )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "invalid market_id" in resp.error
    # Message names the expected 32-byte hex shape.
    assert "32-byte hex" in resp.error
