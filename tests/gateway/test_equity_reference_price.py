"""BSC equity observations retain identity and freshness through the gateway."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from eth_abi import encode

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.market import MarketSnapshotBuilder
from almanak.gateway.data.price.market_hours import reference_market_status
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer
from almanak.integrations.chainlink.catalog import CATALOG
from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource


@pytest.mark.asyncio
@pytest.mark.parametrize("instrument", ["GOOGL", "TSLA"])
@pytest.mark.parametrize(
    ("day", "age", "expected_reason"),
    [
        (8, 60, None),
        (8, 121, "reference_price_too_old"),
        (8, 301, "reference_price_too_old"),
        (8, 86401, "reference_price_provider_stale"),
        (8, -1, "reference_price_timestamp_in_future"),
        (7, 60, "reference_market_closed"),
    ],
)
async def test_equity_reference_round_survives_gateway_and_strategy_gate(instrument, day, age, expected_reason):
    now = datetime(2026, 9, day, 15, tzinfo=UTC)
    observed_at = int(now.timestamp()) - age
    spec = CATALOG.feed("bsc", f"{instrument}/USD")
    assert spec is not None
    source = ChainlinkPriceSource(chain="bsc")
    source._rpc_url = "https://bsc.invalid"
    source._chain_id_validated = True
    assert instrument not in source.supported_tokens

    async def eth_call(address, data):
        assert address == spec.address
        if data == "0x313ce567":
            return "0x" + encode(["uint8"], [8]).hex()
        assert data == "0xfeaf968c"
        return (
            "0x"
            + encode(
                ["uint80", "int256", "uint256", "uint256", "uint80"],
                [10, 33_712_000_000, observed_at, observed_at, 10],
            ).hex()
        )

    service = MarketServiceServicer.__new__(MarketServiceServicer)
    service.settings = SimpleNamespace(chains=["bsc"])
    service._ensure_initialized = AsyncMock()
    service._price_aggregators = {"bsc": SimpleNamespace(sources=[source])}
    try:
        with (
            patch.object(source, "_eth_call", side_effect=eth_call),
            patch("almanak.integrations.chainlink.gateway.live.time.time", return_value=now.timestamp()),
            patch(
                "almanak.gateway.services.market_service.reference_market_status",
                side_effect=lambda pair: reference_market_status(pair, as_of=now),
            ),
        ):
            response = await service.GetReferencePrice(
                gateway_pb2.ReferencePriceRequest(instrument=instrument, quote="USD", chain="bsc"),
                MagicMock(),
            )
    finally:
        await source.close()

    assert response.availability == gateway_pb2.REFERENCE_PRICE_AVAILABILITY_AVAILABLE
    assert response.observed_at == observed_at
    assert response.source == f"chainlink:bsc:{instrument}/USD:{spec.address.lower()}"
    assert response.market_status_source == "pandas_market_calendars:NYSE"
    client = MagicMock(is_connected=True)
    client.market.GetReferencePrice.return_value = response
    snapshot = MarketSnapshotBuilder.for_strategy_runner(
        strategy=SimpleNamespace(chain="bsc", wallet_address="0x1"),
        gateway_client=client,
        runtime_surface="unit_test",
    )
    reference = snapshot.reference_price(instrument, chain="bsc")
    assert reference.price == Decimal("337.12")
    assert reference.trade_block_reason(max_age_seconds=120, now=now) == expected_reason


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "instrument",
    ["GOOGLB", "TSLAB", "GOOGLX", "GOOGLON", "TSLAX", "TSLAON", "GOOG", "0x3f53de71c126bdabae20f9cd64848d317f6c3238"],
)
async def test_reference_provider_never_guesses_wrapper_or_share_class(instrument):
    source = ChainlinkPriceSource(chain="bsc")
    try:
        with patch.object(source, "_eth_call", new_callable=AsyncMock) as rpc:
            with pytest.raises(DataSourceUnavailable, match="No catalogued Chainlink reference feed"):
                await source.get_reference_price(instrument)
            rpc.assert_not_awaited()
    finally:
        await source.close()
