"""Foundation tests for ``RateHistoryService`` (VIB-4859 / W7 step 1).

Locks the validator + envelope behaviour of the new servicer BEFORE the
prototype connectors come online in Step 2. With no
``GatewayLendingRateHistoryCapability`` / ``GatewayDexTwapCapability``
providers wired yet, every dispatch returns INVALID_ARGUMENT with the
"unknown protocol/dex" message and a ``success=False`` envelope.

These tests must continue to pass UNCHANGED through Step 2's prototype
landing — Aave V3 and Uniswap V3 narrow the failing tests below (the
"unknown protocol/dex" case) to "supported on the registered chain set".
"""

from __future__ import annotations

import asyncio

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import (
    GasPricePoint,
    LendingRatePoint,
    RateHistoryServiceServicer,
    RateHistoryUnavailable,
)


class _MockContext:
    """Captures the ``(code, details)`` the servicer sets on the gRPC context."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


@pytest.fixture
def servicer() -> RateHistoryServiceServicer:
    return RateHistoryServiceServicer(GatewaySettings())


# =============================================================================
# Lending — GetLendingRateCurrent
# =============================================================================


def test_lending_current_rejects_empty_protocol(servicer: RateHistoryServiceServicer) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="",
        chain="ethereum",
        asset_symbol="USDC",
        side="supply",
    )
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "protocol is required" in ctx.details
    assert response.success is False
    assert "protocol is required" in response.error


def test_lending_current_rejects_empty_chain(servicer: RateHistoryServiceServicer) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="aave_v3",
        chain="",
        asset_symbol="USDC",
        side="supply",
    )
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "chain is required" in ctx.details
    assert response.success is False


def test_lending_current_rejects_empty_asset(servicer: RateHistoryServiceServicer) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="aave_v3",
        chain="ethereum",
        asset_symbol="",
        side="supply",
    )
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "asset_symbol is required" in ctx.details
    assert response.success is False


@pytest.mark.parametrize("side", ["", "long", "short", "supplyy"])
def test_lending_current_rejects_invalid_side(servicer: RateHistoryServiceServicer, side: str) -> None:
    """``side`` must be the literal ``"supply"`` / ``"borrow"`` post-normalisation.

    ``"Supply"`` / ``"BORROW"`` ARE normalised to lowercase and pass the
    side validator — they're case-insensitive on the wire. Only genuinely
    bad tokens (typos, ``"long"`` / ``"short"`` from a perp caller, the
    empty string) are rejected.
    """
    ctx = _MockContext()
    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="aave_v3",
        chain="ethereum",
        asset_symbol="USDC",
        side=side,
    )
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    # The side validator fires BEFORE the protocol-existence check, so
    # the message is about ``side``, not ``protocol``.
    assert "side must be 'supply' or 'borrow'" in ctx.details
    assert response.success is False


@pytest.mark.parametrize("side", ["supply", "borrow", "Supply", "BORROW"])
def test_lending_current_accepts_case_insensitive_side(servicer: RateHistoryServiceServicer, side: str) -> None:
    """Side normalisation: case-insensitive, then strict ``supply`` / ``borrow``.

    With a registered ``aave_v3`` provider but a chain it doesn't
    support, the dispatcher falls through to the "unsupported chain"
    check (NOT the "unsupported protocol" check). This locks the
    validation order: side first, then protocol-existence, then chain.
    """
    ctx = _MockContext()
    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="aave_v3",
        chain="solana",  # Aave V3 isn't on Solana — guaranteed unsupported.
        asset_symbol="USDC",
        side=side,
    )
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    # Side passes, protocol passes, chain fails.
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "does not support chain" in ctx.details
    assert "solana" in ctx.details
    assert response.success is False


def test_lending_current_rejects_unknown_protocol(
    servicer: RateHistoryServiceServicer,
) -> None:
    """With no connectors implementing the capability yet, every protocol
    is "unknown". After Step 2 lands aave_v3 this fails the chain check
    for unsupported chains instead — the test will be updated alongside.
    """
    ctx = _MockContext()
    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol="nonexistent_protocol",
        chain="ethereum",
        asset_symbol="USDC",
        side="supply",
    )
    response = asyncio.run(servicer.GetLendingRateCurrent(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "unsupported protocol" in ctx.details
    assert "nonexistent_protocol" in ctx.details
    assert response.success is False


# =============================================================================
# Lending — GetLendingRateHistory window validation
# =============================================================================


@pytest.mark.parametrize(
    "start_ts,end_ts,expected",
    [
        (0, 1_700_000_000, "start_ts must be > 0"),
        (1_700_000_000, 0, "end_ts must be > 0"),
        (1_700_000_000, 1_700_000_000, "start_ts must be < end_ts"),
        (1_700_000_100, 1_700_000_000, "start_ts must be < end_ts"),
    ],
)
def test_lending_history_window_validation(
    servicer: RateHistoryServiceServicer,
    start_ts: int,
    end_ts: int,
    expected: str,
) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetLendingRateHistoryRequest(
        protocol="aave_v3",
        chain="ethereum",
        asset_symbol="USDC",
        side="supply",
        start_ts=start_ts,
        end_ts=end_ts,
    )
    response = asyncio.run(servicer.GetLendingRateHistory(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert expected in ctx.details
    assert response.success is False


# =============================================================================
# Funding — validator
# =============================================================================


def test_funding_history_rejects_empty_venue(
    servicer: RateHistoryServiceServicer,
) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetFundingRateHistoryRequest(
        venue="",
        market="ETH-USD",
        chain="arbitrum",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
    )
    response = asyncio.run(servicer.GetFundingRateHistory(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "venue is required" in ctx.details
    assert response.success is False


def test_funding_history_rejects_unknown_venue(
    servicer: RateHistoryServiceServicer,
) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetFundingRateHistoryRequest(
        venue="binance_perps",
        market="ETH-USD",
        chain="arbitrum",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
    )
    response = asyncio.run(servicer.GetFundingRateHistory(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "unsupported venue" in ctx.details
    assert "binance_perps" in ctx.details
    assert response.success is False


# =============================================================================
# Gas price — validator + gateway helper dispatch
# =============================================================================


def test_gas_price_rejects_empty_chain(servicer: RateHistoryServiceServicer) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetGasPriceAtRequest(chain="", timestamp=0)
    response = asyncio.run(servicer.GetGasPriceAt(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "chain is required" in ctx.details
    assert response.success is False


def test_gas_price_rejects_negative_timestamp(servicer: RateHistoryServiceServicer) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetGasPriceAtRequest(chain="ethereum", timestamp=-1)
    response = asyncio.run(servicer.GetGasPriceAt(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "timestamp must be >= 0" in ctx.details
    assert response.success is False


def test_gas_price_rejects_unknown_chain(servicer: RateHistoryServiceServicer) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetGasPriceAtRequest(chain="not-a-chain", timestamp=0)
    response = asyncio.run(servicer.GetGasPriceAt(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "unknown chain" in ctx.details
    assert response.success is False


def test_gas_price_success_maps_gateway_helper(
    servicer: RateHistoryServiceServicer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from decimal import Decimal

    async def _fake_fetch(
        got_servicer: RateHistoryServiceServicer,
        *,
        chain: str,
        timestamp: int,
        descriptor: object | None = None,
    ) -> tuple[GasPricePoint, str]:
        assert got_servicer is servicer
        assert chain == "ethereum"
        assert timestamp == 1_700_000_000
        assert getattr(descriptor, "name", None) == "ethereum"
        return (
            GasPricePoint(
                timestamp=1_700_000_000,
                base_fee_gwei=Decimal("20"),
                priority_fee_gwei=None,
                gas_price_gwei=None,
            ),
            "archive_rpc",
        )

    monkeypatch.setattr("almanak.gateway.services.rate_history_service.fetch_gas_price_at", _fake_fetch)

    ctx = _MockContext()
    request = gateway_pb2.GetGasPriceAtRequest(chain="ETHEREUM", timestamp=1_700_000_000)
    response = asyncio.run(servicer.GetGasPriceAt(request, ctx))  # type: ignore[arg-type]

    assert ctx.code is None
    assert response.success is True
    assert response.chain == "ethereum"
    assert response.source == "archive_rpc"
    assert response.point.base_fee_gwei == "20"
    assert response.point.priority_fee_gwei == ""
    assert response.point.gas_price_gwei == ""


# =============================================================================
# DEX TWAP — validator
# =============================================================================


def test_twap_rejects_empty_dex(servicer: RateHistoryServiceServicer) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetDexTwapRequest(
        dex="",
        chain="arbitrum",
        pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
        secs_ago_start=600,
        secs_ago_end=0,
    )
    response = asyncio.run(servicer.GetDexTwap(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "dex is required" in ctx.details
    assert response.success is False


def test_twap_rejects_inverted_window(
    servicer: RateHistoryServiceServicer,
) -> None:
    ctx = _MockContext()
    # secs_ago_start must be the OLDER boundary > the NEWER boundary
    # secs_ago_end. Inverting is INVALID_ARGUMENT.
    request = gateway_pb2.GetDexTwapRequest(
        dex="uniswap_v3",
        chain="arbitrum",
        pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
        secs_ago_start=0,
        secs_ago_end=600,
    )
    response = asyncio.run(servicer.GetDexTwap(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "secs_ago_start must be > secs_ago_end" in ctx.details
    assert response.success is False


def test_twap_series_rejects_zero_interval(
    servicer: RateHistoryServiceServicer,
) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetDexTwapSeriesRequest(
        dex="uniswap_v3",
        chain="arbitrum",
        pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
        interval_secs=0,
    )
    response = asyncio.run(servicer.GetDexTwapSeries(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "interval_secs must be > 0" in ctx.details
    assert response.success is False


def test_volume_rejects_empty_pool_address(
    servicer: RateHistoryServiceServicer,
) -> None:
    ctx = _MockContext()
    request = gateway_pb2.GetDexVolumeHistoryRequest(
        dex="uniswap_v3",
        chain="arbitrum",
        pool_address="",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
        interval_secs=3600,
    )
    response = asyncio.run(servicer.GetDexVolumeHistory(request, ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "pool_address is required" in ctx.details
    assert response.success is False


# =============================================================================
# Server-side dataclass encoding (Empty != Zero contract)
# =============================================================================


def test_encode_decimal_none_is_empty_string(
    servicer: RateHistoryServiceServicer,
) -> None:
    """None Decimal => empty string on the wire (per ``Empty != Zero``)."""
    assert servicer._encode_decimal(None) == ""


def test_encode_decimal_zero_is_zero_string(
    servicer: RateHistoryServiceServicer,
) -> None:
    """``Decimal("0")`` is NOT the same as ``None`` — measured zero is a
    legitimate observation that MUST survive the round-trip."""
    from decimal import Decimal

    assert servicer._encode_decimal(Decimal("0")) == "0"


def test_lending_point_partial_unmeasured(
    servicer: RateHistoryServiceServicer,
) -> None:
    """A connector that only measures supply APY (not borrow, not utilisation)
    encodes the missing fields as empty strings, not zeros."""
    from decimal import Decimal

    point = LendingRatePoint(
        timestamp=1_700_000_000,
        supply_apy_pct=Decimal("5.25"),
        borrow_apy_pct=None,
        utilization_pct=None,
    )
    encoded = servicer._encode_lending_point(point)
    assert encoded.timestamp == 1_700_000_000
    assert encoded.supply_apy_pct == "5.25"
    assert encoded.borrow_apy_pct == ""
    assert encoded.utilization_pct == ""


def test_gas_point_partial_unmeasured(
    servicer: RateHistoryServiceServicer,
) -> None:
    """Archive blocks may measure only base fee; priority stays empty."""
    from decimal import Decimal

    point = GasPricePoint(
        timestamp=1_700_000_000,
        base_fee_gwei=Decimal("20"),
        priority_fee_gwei=None,
        gas_price_gwei=None,
    )
    encoded = servicer._encode_gas_point(point)
    assert encoded.timestamp == 1_700_000_000
    assert encoded.base_fee_gwei == "20"
    assert encoded.priority_fee_gwei == ""
    assert encoded.gas_price_gwei == ""


def test_rate_history_unavailable_carries_source_and_reason() -> None:
    """``RateHistoryUnavailable`` keeps ``source`` / ``reason`` typed so the
    dispatcher can record them on the failure envelope rather than parsing
    a flattened message."""
    exc = RateHistoryUnavailable("the_graph", "rate limited", retry_after=12.0)
    assert exc.source == "the_graph"
    assert exc.reason == "rate limited"
    assert exc.retry_after == 12.0
    assert "the_graph" in str(exc)
    assert "rate limited" in str(exc)
