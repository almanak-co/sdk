"""Spark lending-rate dispatcher tests.

Spark is an Aave V3 fork; ``SparkGatewayConnector`` serves
``GetLendingRateCurrent`` through the fork-shared ``getReserveData``
pipeline (``almanak/connectors/_base/aave_fork_gateway_rates.py``).
These tests prove:

1. The dispatcher routes ``protocol="spark"`` to ``SparkGatewayConnector``.
2. The connector body targets Spark's OWN ``pool_data_provider`` (not
   Aave's) with the identical ``getReserveData`` calldata shape, resolving
   the asset through the global ``TokenResolver`` (Spark ships no curated
   token table).
3. Ray-unit decode + side selection behave exactly like Aave's
   (fork-shared pipeline, same 12-word return).
4. Non-reserve tokens (all-zero struct) surface as ``success=False``,
   never as 0% APY.
5. Chains without a Spark deployment are rejected by the validator.

Mirrors ``test_rate_history_service_aave_v3.py``'s mocked-RPC harness.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import grpc

from almanak.connectors.spark.addresses import SPARK
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


def _encode_reserve_data(
    supply_rate_ray: int,
    borrow_rate_ray: int,
    *,
    total_atoken: int = 10**12,
    total_variable_debt: int = 5 * 10**11,
) -> str:
    """Build the 12-word Aave-fork ``getReserveData`` return blob."""
    words = [
        0,  # unbacked
        0,  # accruedToTreasuryScaled
        total_atoken,
        0,  # totalStableDebt
        total_variable_debt,
        supply_rate_ray,
        borrow_rate_ray,
        0,  # stableBorrowRate
        0,  # averageStableBorrowRate
        10**27,  # liquidityIndex
        10**27,  # variableBorrowIndex
        1_700_000_000,  # lastUpdateTimestamp
    ]
    return "0x" + "".join(w.to_bytes(32, byteorder="big").hex() for w in words)


def _make_servicer_with_mock_rpc(reserve_data_hex: str) -> tuple[RateHistoryServiceServicer, list[dict[str, Any]]]:
    """Build the servicer wired to a mocked aiohttp + ``get_rpc_url``."""
    captured: list[dict[str, Any]] = []

    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = AsyncMock(return_value={"jsonrpc": "2.0", "id": 1, "result": reserve_data_hex})

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
# Registration
# =============================================================================


def test_spark_registered_as_lending_provider() -> None:
    servicer = RateHistoryServiceServicer(GatewaySettings())
    assert "spark" in servicer._lending_providers
    assert type(servicer._lending_providers["spark"]).__name__ == "SparkGatewayConnector"
    # Spark must NOT share Aave's provider instance — it is its own connector
    # with its own address table, only the pipeline is fork-shared.
    assert servicer._lending_providers["spark"] is not servicer._lending_providers["aave_v3"]


def test_spark_lending_supported_chains_track_address_table() -> None:
    servicer = RateHistoryServiceServicer(GatewaySettings())
    provider = servicer._lending_providers["spark"]
    assert provider.lending_supported_chains() == frozenset(SPARK.keys())
    assert "ethereum" in provider.lending_supported_chains()


# =============================================================================
# Happy path: supply decode against Spark's own data provider
# =============================================================================


def test_spark_supply_decodes_and_targets_spark_data_provider() -> None:
    """5%/ray supply rate decodes to 5% APY; eth_call goes to SPARK's provider."""
    supply_ray = 5 * 10**25  # 5% in ray
    borrow_ray = 7 * 10**25

    servicer, captured = _make_servicer_with_mock_rpc(
        _encode_reserve_data(
            supply_ray,
            borrow_ray,
            total_atoken=10**12,
            total_variable_debt=72 * 10**10,  # → 72% utilisation
        )
    )

    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="spark",
            chain="ethereum",
            asset_symbol="USDC",
            side="supply",
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code is None
    assert response.success is True, response.error
    assert response.protocol == "spark"
    assert response.source == "on_chain"
    assert Decimal(response.point.supply_apy_pct) == Decimal("5")
    assert response.point.borrow_apy_pct == ""  # unselected side stays unmeasured
    assert Decimal(response.point.utilization_pct) == Decimal("72")

    # One eth_call, addressed to Spark's own PoolDataProvider (not Aave's).
    assert len(captured) == 1
    call_params = captured[0]["params"][0]
    assert call_params["to"] == SPARK["ethereum"]["pool_data_provider"]
    calldata = call_params["data"]
    assert calldata.startswith("0x35ea6a75")  # getReserveData(address)
    assert len(calldata) == 2 + 8 + 64
    # Asset resolved via TokenResolver: canonical ethereum USDC address.
    assert calldata.endswith("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")


def test_spark_borrow_selects_variable_borrow_rate() -> None:
    supply_ray = 5 * 10**25
    borrow_ray = 7 * 10**25

    servicer, _captured = _make_servicer_with_mock_rpc(_encode_reserve_data(supply_ray, borrow_ray))

    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="spark",
            chain="ethereum",
            asset_symbol="DAI",
            side="borrow",
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is True, response.error
    assert response.point.supply_apy_pct == ""
    assert Decimal(response.point.borrow_apy_pct) == Decimal("7")


# =============================================================================
# Failure: all-zero struct (token resolves but isn't a Spark reserve)
# =============================================================================


def test_spark_non_reserve_token_yields_success_false_envelope() -> None:
    """All-zero ``getReserveData`` must NOT decode as 0% APY (silent zero)."""
    all_zero = "0x" + "00" * (12 * 32)

    servicer, _captured = _make_servicer_with_mock_rpc(all_zero)

    with patch("almanak.gateway.utils.get_rpc_url", return_value="http://rpc.test"):
        request = gateway_pb2.GetLendingRateCurrentRequest(
            protocol="spark",
            chain="ethereum",
            asset_symbol="USDC",
            side="supply",
        )
        ctx = _MockContext()
        response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert "not a listed Spark reserve" in response.error
    assert response.source == "spark"
    assert response.point.supply_apy_pct == ""
    assert response.point.borrow_apy_pct == ""
    assert response.point.utilization_pct == ""


# =============================================================================
# Validator: chain without a Spark deployment
# =============================================================================


def test_spark_unsupported_chain_yields_invalid_argument() -> None:
    servicer = RateHistoryServiceServicer(GatewaySettings())
    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="spark",
        chain="arbitrum",  # Spark ships no Arbitrum deployment.
        asset_symbol="USDC",
        side="supply",
    )
    ctx = _MockContext()
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "does not support chain 'arbitrum'" in ctx.details
    assert response.success is False


# =============================================================================
# History lane: pending the W7 step-3 lending cluster => success=False
# =============================================================================


def test_spark_history_pending_yields_success_false() -> None:
    servicer = RateHistoryServiceServicer(GatewaySettings())
    request = gateway_pb2.GetLendingRateHistoryRequest(
        protocol="spark",
        chain="ethereum",
        asset_symbol="USDC",
        side="supply",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
    )
    ctx = _MockContext()
    response = asyncio.run(servicer.GetLendingRateHistory(request, ctx))  # type: ignore[arg-type]

    assert response.success is False
    assert response.source == "spark"
    assert len(response.points) == 0
