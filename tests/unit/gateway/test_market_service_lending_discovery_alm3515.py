"""MarketService.GetLendingMarket carries raw liquidity fields (ALM-3515).

Proves the gateway wiring end-to-end: a verified Morpho market's
``total_supply_assets`` / ``total_borrow_assets`` (populated by
``verify_morpho_market``'s ``market(bytes32)`` read) reach the wire proto via
``_lending_market_to_proto``. Also proves ``ListLendingMarkets`` (the offline
curated-catalog path) never carries these fields — they are a byproduct of
on-chain verification only, never fabricated for an unverified candidate.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

import almanak.gateway.services.pt_rpc_adapter as pt_rpc_adapter
from almanak.connectors.morpho_blue.gateway.market_discovery import _MARKET_SELECTOR
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer

_SUSDE_USDC_ID = "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab"
_USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
_SUSDE = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
_ORACLE = "0x873CD44b860DEDFe139f93e12A4AcCa0926Ffb87"
_IRM = "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC"
_LLTV = 915000000000000000


class _Ctx:
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


def _market_state_payload(*, total_supply_assets: int, total_borrow_assets: int) -> str:
    return "0x" + "".join(_word_uint(v) for v in (total_supply_assets, 0, total_borrow_assets, 0, 0, 0))


def _patch_routed_eth_call(monkeypatch, *, id_to_market_params: str, market_state: str) -> None:
    """Dispatch by selector, like the real chain would for two different reads."""

    async def _fake(to: str, data: str) -> str:
        selector = data[:10]
        if selector == "0x2c3c9157":
            return id_to_market_params
        if selector == _MARKET_SELECTOR:
            return market_state
        raise AssertionError(f"unexpected selector {selector!r}")

    monkeypatch.setattr(pt_rpc_adapter, "build_gateway_eth_call", lambda **_: _fake)


@pytest.mark.asyncio
async def test_get_lending_market_carries_liquidity_fields(monkeypatch):
    _patch_routed_eth_call(
        monkeypatch,
        id_to_market_params=_idtomarketparams_payload(_USDC, _SUSDE, _ORACLE, _IRM, _LLTV),
        market_state=_market_state_payload(total_supply_assets=1_000_000_000_000, total_borrow_assets=500_000_000_000),
    )
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.GetLendingMarket(
        gateway_pb2.GetLendingMarketRequest(protocol="morpho_blue", chain="ethereum", market_id=_SUSDE_USDC_ID),
        ctx,
    )
    assert resp.success is True
    assert resp.market.verified is True
    assert resp.market.total_supply_assets == "1000000000000"
    assert resp.market.total_borrow_assets == "500000000000"


@pytest.mark.asyncio
async def test_get_lending_market_liquidity_read_failure_leaves_fields_empty(monkeypatch):
    """A market()-read failure must not fail GetLendingMarket itself — identity
    verification (idToMarketParams) is independent of the supplementary
    liquidity read."""

    async def _fake(to: str, data: str) -> str:
        selector = data[:10]
        if selector == "0x2c3c9157":
            return _idtomarketparams_payload(_USDC, _SUSDE, _ORACLE, _IRM, _LLTV)
        raise RuntimeError("RPC timeout")

    monkeypatch.setattr(pt_rpc_adapter, "build_gateway_eth_call", lambda **_: _fake)
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.GetLendingMarket(
        gateway_pb2.GetLendingMarketRequest(protocol="morpho_blue", chain="ethereum", market_id=_SUSDE_USDC_ID),
        ctx,
    )
    assert resp.success is True
    assert resp.market.verified is True
    assert resp.market.total_supply_assets == ""
    assert resp.market.total_borrow_assets == ""


@pytest.mark.asyncio
async def test_list_lending_markets_never_carries_liquidity_fields(monkeypatch):
    """The offline curated-catalog path (ListLendingMarkets) is a pure
    candidate listing — it must never carry live liquidity state, only the
    on-chain-verified GetLendingMarket path does (ALM-3515)."""
    svc = _servicer()
    ctx = _Ctx()
    resp = await svc.ListLendingMarkets(
        gateway_pb2.ListLendingMarketsRequest(
            protocol="morpho_blue", chain="ethereum", collateral_token="sUSDe", loan_token="USDC"
        ),
        ctx,
    )
    assert resp.success is True
    assert resp.markets
    for market in resp.markets:
        assert market.total_supply_assets == ""
        assert market.total_borrow_assets == ""
