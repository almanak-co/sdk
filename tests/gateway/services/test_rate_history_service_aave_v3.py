"""Aave V3 lending-rate dispatcher integration tests (VIB-4859 / W7 step 2).

Exercises the full ``GetLendingRateCurrent`` path with a mocked
upstream JSON-RPC, verifying that:

1. The dispatcher routes ``protocol="aave_v3"`` to ``AaveV3GatewayConnector``.
2. The connector body builds the right ``getReserveData`` calldata.
3. The ABI-decoded ray-units convert to the right APY percent / utilisation.
4. ``side="supply"`` and ``side="borrow"`` select different fields from
   the same upstream response (and leave the unselected one empty).
5. Non-Aave tokens (all-zero struct) surface as ``DataSourceUnavailable``,
   never as 0% APY.

These tests stand in for the byte-equivalence replay harness called out
in the plan PR §6 — the harness itself (full backtest replay with
``vcrpy``-style cassettes) is too heavyweight for a step-2 commit but
the per-call decoder is locked here.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer


class _MockContext:
    """Captures ``(code, details)`` set by the servicer."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _encode_aave_reserve_data(
    supply_rate_ray: int,
    borrow_rate_ray: int,
    *,
    total_atoken: int = 10**12,
    total_variable_debt: int = 5 * 10**11,
) -> str:
    """Build the 12-word ``getReserveData`` return that the connector decodes.

    Order (per AaveProtocolDataProvider):
      0  unbacked
      1  accruedToTreasuryScaled
      2  totalAToken              ← used for utilisation
      3  totalStableDebt
      4  totalVariableDebt        ← used for utilisation
      5  liquidityRate            ← supply APY (ray)
      6  variableBorrowRate       ← borrow APY (ray)
      7  stableBorrowRate
      8  averageStableBorrowRate
      9  liquidityIndex
      10 variableBorrowIndex
      11 lastUpdateTimestamp
    """
    words = [
        0,
        0,
        total_atoken,
        0,
        total_variable_debt,
        supply_rate_ray,
        borrow_rate_ray,
        0,
        0,
        10**27,  # liquidityIndex (ignored by the rate parser)
        10**27,
        1_700_000_000,
    ]
    return "0x" + "".join(w.to_bytes(32, byteorder="big").hex() for w in words)


def _make_servicer_with_mock_rpc(reserve_data_hex: str) -> tuple[
    RateHistoryServiceServicer, list[dict[str, Any]]
]:
    """Build the servicer wired to a mocked aiohttp + ``get_rpc_url``.

    Returns ``(servicer, captured_payloads)`` so tests can assert what
    calldata the connector built.
    """
    captured: list[dict[str, Any]] = []

    # Mock aiohttp session: ``servicer._get_http_session`` returns this.
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = AsyncMock(
        return_value={"jsonrpc": "2.0", "id": 1, "result": reserve_data_hex}
    )

    def _post(url: str, *, json: dict[str, Any]) -> Any:
        captured.append(json)
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=mock_response)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx

    mock_session = MagicMock()
    mock_session.post = _post

    servicer = RateHistoryServiceServicer(GatewaySettings())
    servicer._get_http_session = AsyncMock(return_value=mock_session)  # type: ignore[method-assign]
    return servicer, captured


# =============================================================================
# Happy path: supply rate decode
# =============================================================================


def test_aave_v3_supply_decodes_to_correct_percent_and_utilisation() -> None:
    """5%/ray supply rate decodes to 5% APY, utilisation = debt/atoken * 100."""
    # 5% in ray = 5 / 100 * 1e27 = 5e25.
    supply_ray = 5 * 10**25
    borrow_ray = 7 * 10**25  # 7% — should NOT appear in a supply-side response.

    servicer, _captured = _make_servicer_with_mock_rpc(
        _encode_aave_reserve_data(
            supply_ray,
            borrow_ray,
            total_atoken=10**12,
            total_variable_debt=72 * 10**10,  # → 72% utilisation
        )
    )

    with patch(
        "almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"
    ):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="aave_v3",
            chain="ethereum",
            asset_symbol="USDC",
            side="supply",
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code is None  # No error code set.
    assert response.success is True
    assert response.protocol == "aave_v3"
    assert response.chain == "ethereum"
    assert response.asset_symbol == "USDC"
    assert response.side == "supply"
    assert response.source == "on_chain"
    assert response.is_live_data is True

    # Supply APY should be 5% (allow tiny rounding from Decimal division).
    assert Decimal(response.point.supply_apy_pct) == Decimal("5")
    # Borrow side was NOT selected => empty string ("unmeasured by this call").
    assert response.point.borrow_apy_pct == ""
    # Utilisation = 72 / 100 * 100 = 72%.
    assert Decimal(response.point.utilization_pct) == Decimal("72")


# =============================================================================
# Happy path: borrow rate decode (same upstream, different field)
# =============================================================================


def test_aave_v3_borrow_selects_variable_borrow_rate() -> None:
    supply_ray = 5 * 10**25
    borrow_ray = 7 * 10**25

    servicer, captured = _make_servicer_with_mock_rpc(
        _encode_aave_reserve_data(supply_ray, borrow_ray)
    )

    with patch(
        "almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"
    ):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="aave_v3",
            chain="ethereum",
            asset_symbol="USDC",
            side="borrow",
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is True
    assert response.side == "borrow"
    assert response.point.supply_apy_pct == ""  # NOT selected.
    assert Decimal(response.point.borrow_apy_pct) == Decimal("7")

    # Sanity: the connector did one call (no second round-trip needed).
    assert len(captured) == 1
    # Calldata: selector (4B) + padded address (32B) -> 0x + 8 + 64 = 74 chars.
    calldata = captured[0]["params"][0]["data"]
    assert calldata.startswith("0x35ea6a75")
    assert len(calldata) == 2 + 8 + 64


# =============================================================================
# Happy path: bsc on the rate lane
# =============================================================================


def test_aave_v3_bsc_targets_bsc_data_provider() -> None:
    """bsc is servable: the eth_call goes to the bsc PoolDataProvider with
    the curated bsc USDC address (AAVE_V3_TOKENS)."""
    from almanak.connectors.aave_v3.addresses import AAVE_V3, AAVE_V3_TOKENS

    supply_ray = 3 * 10**25  # 3%

    servicer, captured = _make_servicer_with_mock_rpc(_encode_aave_reserve_data(supply_ray, 4 * 10**25))

    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="aave_v3",
            chain="bsc",
            asset_symbol="USDC",
            side="supply",
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code is None
    assert response.success is True, response.error
    assert response.chain == "bsc"
    assert Decimal(response.point.supply_apy_pct) == Decimal("3")

    assert len(captured) == 1
    call_params = captured[0]["params"][0]
    assert call_params["to"] == AAVE_V3["bsc"]["pool_data_provider"]
    assert call_params["data"].endswith(AAVE_V3_TOKENS["bsc"]["USDC"][2:].lower())


# =============================================================================
# Failure: all-zero struct (token resolves but isn't an Aave reserve)
# =============================================================================


def test_aave_v3_non_reserve_token_yields_success_false_envelope() -> None:
    """All-zero ``getReserveData`` must NOT decode as 0% APY (silent zero)."""
    all_zero = "0x" + "00" * (12 * 32)

    servicer, _captured = _make_servicer_with_mock_rpc(all_zero)

    with patch(
        "almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"
    ):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="aave_v3",
            chain="ethereum",
            asset_symbol="USDC",
            side="supply",
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert "not a listed Aave reserve" in response.error
    assert response.source == "aave_v3"
    # Critically: NO numeric values were emitted (no silent zero-fill).
    # The point field is the default (all-empty) LendingRatePoint.
    assert response.point.supply_apy_pct == ""
    assert response.point.borrow_apy_pct == ""
    assert response.point.utilization_pct == ""


# =============================================================================
# Failure: empty RPC result
# =============================================================================


def test_aave_v3_empty_rpc_result_yields_success_false() -> None:
    """``result=0x`` means the eth_call reverted / returned nothing."""
    servicer, _captured = _make_servicer_with_mock_rpc("0x")

    with patch(
        "almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"
    ):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="aave_v3",
            chain="ethereum",
            asset_symbol="USDC",
            side="supply",
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert response.source == "aave_v3"


# =============================================================================
# Failure: no RPC URL configured for chain
# =============================================================================


def test_aave_v3_no_rpc_url_yields_data_source_unavailable() -> None:
    servicer, _captured = _make_servicer_with_mock_rpc("0x")

    def _raise(*args: Any, **kwargs: Any) -> str:
        raise ValueError("No RPC URL for 'ethereum'")

    with patch("almanak.gateway.utils.get_rpc_url", side_effect=_raise):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="aave_v3",
            chain="ethereum",
            asset_symbol="USDC",
            side="supply",
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert "No RPC URL configured" in response.error
    assert response.source == "aave_v3"


# =============================================================================
# History lane: not yet implemented in step 2 => RateHistoryUnavailable
# =============================================================================


def test_aave_v3_history_step3_pending() -> None:
    """Step 3 (lending cluster) lights up history. Step 2 raises cleanly."""
    servicer = RateHistoryServiceServicer(GatewaySettings())
    request = gateway_pb2.GetLendingRateHistoryRequest(
        protocol="aave_v3",
        chain="ethereum",
        asset_symbol="USDC",
        side="supply",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
    )
    ctx = _MockContext()
    response = asyncio.run(servicer.GetLendingRateHistory(request, ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert "step 3" in response.error
    assert response.source == "aave_v3"
    # success=False MUST come with no points (mirrors PoolHistoryService).
    assert len(response.points) == 0


# =============================================================================
# Validator: unsupported chain on a registered protocol
# =============================================================================


def test_aave_v3_unsupported_chain_yields_invalid_argument() -> None:
    servicer = RateHistoryServiceServicer(GatewaySettings())
    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="aave_v3",
        chain="solana",  # Aave V3 is EVM-only.
        asset_symbol="USDC",
        side="supply",
    )
    ctx = _MockContext()
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "does not support chain 'solana'" in ctx.details
    assert response.success is False
