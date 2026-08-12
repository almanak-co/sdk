"""ALM-3223 exact reference-feed gateway coverage."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, PriceResult
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer
from almanak.integrations.chainlink.catalog import CATALOG
from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource


def _latest_round_data(answer: int, updated_at: int) -> str:
    words = [
        (1).to_bytes(32, "big"),
        answer.to_bytes(32, "big", signed=True),
        updated_at.to_bytes(32, "big"),
        updated_at.to_bytes(32, "big"),
        (1).to_bytes(32, "big"),
    ]
    return "0x" + b"".join(words).hex()


@pytest.mark.asyncio
async def test_bsc_xau_reference_read_is_exact_and_preserves_feed_timestamp():
    spec = CATALOG.feed("bsc", "XAU/USD")
    assert spec is not None
    assert spec.address == "0x86896fEB19D8A607c3b11f2aF50A0f239Bd71CD0"
    assert spec.heartbeat_seconds == 600

    updated_at = int(time.time()) - 120
    source = ChainlinkPriceSource(chain="bsc")
    assert "XAU" not in source.supported_tokens, "reference assets must not leak into generic token pricing"
    source._chain_id_validated = True
    source._feed_decimals[spec.address.lower()] = 8
    with patch.object(
        source,
        "_eth_call",
        new_callable=AsyncMock,
        return_value=_latest_round_data(241_234_000_000, updated_at),
    ):
        result = await source.get_reference_price("XAU", "USD")

    assert result.price == Decimal("2412.34")
    assert int(result.timestamp.timestamp()) == updated_at
    assert result.stale is False
    assert spec.address.lower() in result.source
    await source.close()


@pytest.mark.asyncio
async def test_reference_read_fails_closed_when_rpc_chain_guard_disables_endpoint():
    source = ChainlinkPriceSource(chain="bsc")
    source._rpc_url = "https://wrong-chain.invalid"

    async def reject_wrong_chain() -> None:
        source._chain_id_validated = True
        source._rpc_url = None

    with (
        patch.object(source, "_validate_chain_id", new_callable=AsyncMock, side_effect=reject_wrong_chain) as guard,
        patch.object(source, "_eth_call", new_callable=AsyncMock) as eth_call,
        pytest.raises(DataSourceUnavailable, match="No positively chain-validated RPC URL available"),
    ):
        await source.get_reference_price("XAU", "USD")

    guard.assert_awaited_once()
    eth_call.assert_not_awaited()
    await source.close()


@pytest.mark.asyncio
async def test_reference_read_rejects_rpc_after_unconfirmed_chain_validation_attempt():
    source = ChainlinkPriceSource(chain="bsc")
    source._rpc_url = "https://unconfirmed-chain.invalid"
    source._chain_id_validation_attempted = True
    source._chain_id_validated = False

    with (
        patch.object(source, "_eth_call", new_callable=AsyncMock) as eth_call,
        pytest.raises(DataSourceUnavailable, match="No positively chain-validated RPC URL available"),
    ):
        await source.get_reference_price("XAU", "USD")

    eth_call.assert_not_awaited()
    await source.close()


@pytest.mark.asyncio
async def test_future_feed_timestamp_is_preserved_for_precise_strategy_rejection():
    spec = CATALOG.feed("bsc", "XAU/USD")
    assert spec is not None
    updated_at = int(time.time()) + 120
    source = ChainlinkPriceSource(chain="bsc")
    source._chain_id_validated = True
    source._feed_decimals[spec.address.lower()] = 8

    with patch.object(
        source,
        "_eth_call",
        new_callable=AsyncMock,
        return_value=_latest_round_data(241_234_000_000, updated_at),
    ):
        result = await source.get_reference_price("XAU", "USD")

    assert int(result.timestamp.timestamp()) == updated_at
    assert result.stale is False
    await source.close()


@pytest.mark.asyncio
async def test_gateway_reference_price_carries_session_and_provider_provenance():
    observed_at = datetime(2026, 8, 10, 18, tzinfo=UTC)
    source = MagicMock()
    source.get_reference_price = AsyncMock(
        return_value=PriceResult(
            price=Decimal("2412.34"),
            source="chainlink:bsc:XAU/USD:0xfeed",
            timestamp=observed_at,
            confidence=0.95,
            stale=False,
        )
    )
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.settings = SimpleNamespace(chains=["bsc"])
    service._ensure_initialized = AsyncMock()
    service._price_aggregators = {"bsc": SimpleNamespace(sources=[source])}
    context = MagicMock()

    with patch("almanak.gateway.services.market_service.reference_market_status") as status:
        from almanak.gateway.data.price.market_hours import MarketHoursObservation, ReferenceMarketStatus

        status.return_value = MarketHoursObservation(
            ReferenceMarketStatus.OPEN,
            observed_at,
            "pandas_market_calendars:CMEGlobex_Gold",
        )
        response = await service.GetReferencePrice(
            gateway_pb2.ReferencePriceRequest(instrument="xau", quote="USD", chain="bsc"),
            context,
        )

    assert response.availability == gateway_pb2.REFERENCE_PRICE_AVAILABILITY_AVAILABLE
    assert response.price == "2412.34"
    assert response.observed_at == int(observed_at.timestamp())
    assert response.market_status == gateway_pb2.REFERENCE_MARKET_STATUS_OPEN
    assert response.market_status_source.endswith("CMEGlobex_Gold")
    source.get_reference_price.assert_awaited_once_with("XAU", "USD")


@pytest.mark.asyncio
async def test_gateway_reference_price_rejects_registry_valid_unconfigured_chain():
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.settings = SimpleNamespace(chains=["arbitrum"])
    service._ensure_initialized = AsyncMock()
    service._price_aggregators = {"arbitrum": SimpleNamespace(sources=[])}
    service.reinitialize = AsyncMock()
    context = MagicMock()

    response = await service.GetReferencePrice(
        gateway_pb2.ReferencePriceRequest(instrument="XAU", quote="USD", chain="bsc"),
        context,
    )

    assert response.availability == gateway_pb2.REFERENCE_PRICE_AVAILABILITY_UNMEASURED
    assert response.reason == "chain_not_configured"
    context.set_code.assert_called_once()
    service.reinitialize.assert_not_awaited()


@pytest.mark.asyncio
async def test_gateway_reference_price_does_not_expose_provider_exception_text(caplog):
    observed_at = datetime(2026, 8, 10, 18, tzinfo=UTC)
    source = MagicMock()
    source.get_reference_price = AsyncMock(side_effect=RuntimeError("secret-provider-detail"))
    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.settings = SimpleNamespace(chains=["bsc"])
    service._ensure_initialized = AsyncMock()
    service._price_aggregators = {"bsc": SimpleNamespace(sources=[source])}
    context = MagicMock()

    with patch("almanak.gateway.services.market_service.reference_market_status") as status:
        from almanak.gateway.data.price.market_hours import MarketHoursObservation, ReferenceMarketStatus

        status.return_value = MarketHoursObservation(ReferenceMarketStatus.OPEN, observed_at, "calendar")
        response = await service.GetReferencePrice(
            gateway_pb2.ReferencePriceRequest(instrument="XAU", quote="USD", chain="bsc"),
            context,
        )

    assert response.availability == gateway_pb2.REFERENCE_PRICE_AVAILABILITY_ERRORED
    assert response.reason == "reference_price_unavailable"
    assert "secret-provider-detail" not in caplog.text
