"""Morpho Blue + MetaMorpho lending-rate dispatcher tests (VIB-5040 / D1).

Proves the two gaps the demo APY gate hit are closed:

1. ``_lending_providers`` now contains both ``metamorpho`` (morpho_vault,
   registered via ``lending_aliases``) and ``morpho_blue``.
2. ``GetLendingRateCurrent`` returns a real positive ``Decimal`` rate — not a
   ``RateHistoryUnavailable`` raise — given a mocked on-chain read:
   * morpho_blue: ``market(id)`` + IRM ``borrowRateView`` → supply/borrow APY.
   * metamorpho: ``convertToAssets`` delta over a block window → supply APY.

Mirrors ``test_rate_history_service_aave_v3.py``'s mocked-RPC harness.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer

_SECONDS_PER_YEAR = 365 * 24 * 60 * 60


class _MockContext:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _make_servicer(router: Callable[[dict[str, Any]], Any]) -> RateHistoryServiceServicer:
    """Build a servicer whose shared HTTP session answers ``router(payload)``."""

    def _post(url: str, *, json: dict[str, Any]) -> Any:
        result = router(json)
        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value={"jsonrpc": "2.0", "id": json.get("id"), "result": result})
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=mock_response)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx

    mock_session = MagicMock()
    mock_session.post = _post
    servicer = RateHistoryServiceServicer(GatewaySettings())
    servicer._get_http_session = AsyncMock(return_value=mock_session)  # type: ignore[method-assign]
    return servicer


# =============================================================================
# Registration
# =============================================================================


def test_metamorpho_and_morpho_blue_registered() -> None:
    servicer = RateHistoryServiceServicer(GatewaySettings())
    assert "metamorpho" in servicer._lending_providers
    assert "morpho_blue" in servicer._lending_providers
    # metamorpho routes to the morpho_vault gateway connector via lending_aliases.
    assert type(servicer._lending_providers["metamorpho"]).__name__ == "MorphoVaultGatewayConnector"
    # Its own protocol key still resolves too (alias is additive).
    assert servicer._lending_providers["metamorpho"] is servicer._lending_providers["morpho_vault"]


# =============================================================================
# Morpho Blue — on-chain IRM read
# =============================================================================


def _encode_market_struct(
    *,
    total_supply_assets: int,
    total_borrow_assets: int,
    fee: int = 0,
) -> str:
    """Six-uint128 ``market(id)`` return (only assets + fee are read)."""
    words = [
        total_supply_assets,  # totalSupplyAssets
        2 * total_supply_assets,  # totalSupplyShares (unread)
        total_borrow_assets,  # totalBorrowAssets
        2 * total_borrow_assets,  # totalBorrowShares (unread)
        1_700_000_000,  # lastUpdate (unread)
        fee,  # fee
    ]
    return "0x" + "".join(f"{w:064x}" for w in words)


def test_morpho_blue_supply_returns_real_decimal() -> None:
    """market() + borrowRateView() compose a real positive supply APY."""
    # 80% utilisation, no fee.
    market_hex = _encode_market_struct(total_supply_assets=10**12, total_borrow_assets=8 * 10**11)
    # borrow rate per second (WAD) chosen to give ~10% borrow APY:
    # ln(1.10) / SECONDS_PER_YEAR * 1e18.
    borrow_rate_wad = int((Decimal("1.10").ln() / Decimal(_SECONDS_PER_YEAR)) * Decimal(10**18))
    borrow_rate_hex = "0x" + f"{borrow_rate_wad:064x}"

    def _router(payload: dict[str, Any]) -> str:
        # id=1 => market(), id=2 => borrowRateView()
        return market_hex if payload.get("id") == 1 else borrow_rate_hex

    servicer = _make_servicer(_router)

    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="morpho_blue", chain="base", asset_symbol="USDC", side="supply"
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is True, response.error
    assert ctx.code is None
    # Supply APY measured, positive; borrow side empty (Empty != Zero).
    assert response.point.supply_apy_pct != ""
    assert response.point.borrow_apy_pct == ""
    # Utilisation = 80%.
    assert Decimal(response.point.utilization_pct) == Decimal("80")
    # Supply = borrow * util * (1-fee), continuously compounded:
    #   exp(0.8 * ln(1.10)) - 1 = 1.10**0.8 - 1 ≈ 7.923%.
    # Assert a tight band (not just "> 0 and < 10") so a non-annualized or
    # otherwise wrong APY formula fails; the band tolerates only the tiny WAD
    # integer-truncation of borrow_rate_wad.
    supply_apy = Decimal(response.point.supply_apy_pct)
    assert Decimal("7.90") < supply_apy < Decimal("7.95"), supply_apy


def test_morpho_blue_borrow_selects_borrow_side() -> None:
    market_hex = _encode_market_struct(total_supply_assets=10**12, total_borrow_assets=5 * 10**11)
    borrow_rate_wad = int((Decimal("1.08").ln() / Decimal(_SECONDS_PER_YEAR)) * Decimal(10**18))
    borrow_rate_hex = "0x" + f"{borrow_rate_wad:064x}"

    servicer = _make_servicer(lambda p: market_hex if p.get("id") == 1 else borrow_rate_hex)
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="morpho_blue", chain="base", asset_symbol="USDC", side="borrow"
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is True, response.error
    assert response.point.supply_apy_pct == ""  # not selected
    # ~8% borrow APY (continuous compounding of the per-second rate; the WAD
    # round-trip loses a few ULPs, so assert a tight band rather than equality).
    assert abs(Decimal(response.point.borrow_apy_pct) - Decimal("8")) < Decimal("0.001")


def test_morpho_blue_arbitrum_served_from_market_catalogue() -> None:
    """Arbitrum is served straight from
    MORPHO_MARKETS['arbitrum'] (wstETH/USDC + WBTC/USDC lend USDC) against
    the chain-specific Morpho singleton."""
    from almanak.connectors.morpho_blue.addresses import MORPHO_BLUE

    market_hex = _encode_market_struct(total_supply_assets=10**12, total_borrow_assets=6 * 10**11)
    borrow_rate_wad = int((Decimal("1.06").ln() / Decimal(_SECONDS_PER_YEAR)) * Decimal(10**18))
    borrow_rate_hex = "0x" + f"{borrow_rate_wad:064x}"

    captured: list[dict[str, Any]] = []

    def _router(payload: dict[str, Any]) -> str:
        captured.append(payload)
        return market_hex if payload.get("id") == 1 else borrow_rate_hex

    servicer = _make_servicer(_router)
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="morpho_blue", chain="arbitrum", asset_symbol="USDC", side="supply"
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is True, response.error
    assert ctx.code is None
    assert response.point.supply_apy_pct != ""
    assert Decimal(response.point.supply_apy_pct) > Decimal("0")
    assert Decimal(response.point.utilization_pct) == Decimal("60")
    # market() reads target the Arbitrum-specific singleton, NOT the
    # universal vanity address (which has no code on Arbitrum).
    market_calls = [p for p in captured if p.get("id") == 1]
    assert market_calls, captured
    for payload in market_calls:
        assert payload["params"][0]["to"] == MORPHO_BLUE["arbitrum"]["morpho"]


def test_morpho_blue_polygon_served_from_market_catalogue() -> None:
    """Polygon is served from the rate lane (WBTC/USDC lends USDC)."""
    market_hex = _encode_market_struct(total_supply_assets=10**12, total_borrow_assets=4 * 10**11)
    borrow_rate_wad = int((Decimal("1.05").ln() / Decimal(_SECONDS_PER_YEAR)) * Decimal(10**18))
    borrow_rate_hex = "0x" + f"{borrow_rate_wad:064x}"

    servicer = _make_servicer(lambda p: market_hex if p.get("id") == 1 else borrow_rate_hex)
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="morpho_blue", chain="polygon", asset_symbol="USDC", side="borrow"
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is True, response.error
    assert response.point.borrow_apy_pct != ""
    assert abs(Decimal(response.point.borrow_apy_pct) - Decimal("5")) < Decimal("0.001")
    assert response.point.supply_apy_pct == ""  # unselected side stays unmeasured


def test_morpho_blue_unknown_asset_yields_success_false() -> None:
    """An asset no registered market lends surfaces success=False, never 0%."""
    servicer = _make_servicer(lambda p: "0x")
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="morpho_blue", chain="base", asset_symbol="DOGE", side="supply"
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert "No registered Morpho Blue market" in response.error
    assert response.point.supply_apy_pct == ""


# =============================================================================
# MetaMorpho — ERC-4626 convertToAssets delta
# =============================================================================


def test_metamorpho_supply_returns_real_decimal() -> None:
    """convertToAssets delta over a block window annualises to a real APY."""
    latest = 30_000_000
    latest_hex = hex(latest)
    # Share price grew 1.0000 -> 1.0001 over 4 hours (14_400 s).
    assets_then = 10**18
    assets_now = 10**18 + 10**14  # +0.01%
    ts_then = 1_700_000_000
    ts_now = ts_then + 14_400

    def _router(payload: dict[str, Any]) -> Any:
        method = payload["method"]
        if method == "eth_blockNumber":
            return latest_hex
        if method == "eth_getBlockByNumber":
            block_hex = payload["params"][0]
            ts = ts_now if block_hex == latest_hex else ts_then
            return {"number": block_hex, "timestamp": hex(ts)}
        if method == "eth_call":
            block_hex = payload["params"][1]
            assets = assets_now if block_hex == latest_hex else assets_then
            return "0x" + f"{assets:064x}"
        raise AssertionError(f"unexpected method {method}")

    servicer = _make_servicer(_router)
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="metamorpho", chain="base", asset_symbol="USDC", side="supply"
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is True, response.error
    assert ctx.code is None
    assert response.point.supply_apy_pct != ""
    apy = Decimal(response.point.supply_apy_pct)
    # +0.01% over 4h annualises to a meaningful positive APY (~22% here);
    # the exact value is deterministic but we assert the sign + a sane band.
    assert apy > Decimal("0")
    assert apy < Decimal("100")
    # Borrow / utilisation are unmeasured for a supply-only vault.
    assert response.point.borrow_apy_pct == ""
    assert response.point.utilization_pct == ""


def test_metamorpho_borrow_side_unavailable() -> None:
    """Vaults are supply-only: borrow => success=False, never a fabricated rate."""
    servicer = _make_servicer(lambda p: "0x")
    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="metamorpho", chain="base", asset_symbol="USDC", side="borrow"
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert "supply-only" in response.error
