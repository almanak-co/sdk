"""Strategy-facing exact reference-price safety contract for ALM-3223."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak import ReferencePriceData as PublicReferencePriceData
from almanak.framework.market import MarketSnapshotBuilder
from almanak.framework.market.models import ReferenceMarketStatus, ReferencePriceData
from almanak.framework.market.snapshot import MarketSnapshot
from almanak.gateway.proto import gateway_pb2


def _snapshot(response: gateway_pb2.ReferencePriceResponse) -> tuple[MarketSnapshot, MagicMock]:
    client = MagicMock()
    client.is_connected = True
    client.market.GetReferencePrice.return_value = response
    strategy = SimpleNamespace(chain="bsc", wallet_address="0x1")
    return MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy,
        gateway_client=client,
        runtime_surface="unit_test",
    ), client


def test_reference_price_maps_exact_gateway_observation_and_is_tradeable():
    assert PublicReferencePriceData is ReferencePriceData
    observed_at = datetime(2026, 8, 10, 18, tzinfo=UTC)
    snapshot, client = _snapshot(
        gateway_pb2.ReferencePriceResponse(
            instrument="XAU",
            quote="USD",
            chain="bsc",
            price="2412.34",
            availability=gateway_pb2.REFERENCE_PRICE_AVAILABILITY_AVAILABLE,
            confidence=0.95,
            source="chainlink:bsc:XAU/USD:0xfeed",
            observed_at=int(observed_at.timestamp()),
            stale=False,
            market_status=gateway_pb2.REFERENCE_MARKET_STATUS_OPEN,
            market_status_as_of=int(observed_at.timestamp()),
            market_status_source="pandas_market_calendars:CMEGlobex_Gold",
        )
    )

    result = snapshot.reference_price("XAU")

    assert result.price == Decimal("2412.34")
    assert result.observed_at == observed_at
    assert result.market_status is ReferenceMarketStatus.OPEN
    assert result.is_tradeable(max_age_seconds=300, min_confidence=0.9, now=observed_at)
    sent = client.market.GetReferencePrice.call_args.args[0]
    assert (sent.instrument, sent.quote, sent.chain) == ("XAU", "USD", "bsc")


def test_unavailable_reference_preserves_gateway_session_provenance():
    as_of = datetime(2026, 8, 9, 12, tzinfo=UTC)
    snapshot, _ = _snapshot(
        gateway_pb2.ReferencePriceResponse(
            instrument="XAU",
            quote="USD",
            chain="bsc",
            availability=gateway_pb2.REFERENCE_PRICE_AVAILABILITY_ERRORED,
            market_status=gateway_pb2.REFERENCE_MARKET_STATUS_CLOSED,
            market_status_as_of=int(as_of.timestamp()),
            market_status_source="pandas_market_calendars:CMEGlobex_Gold",
            reason="feed_rpc_unavailable",
        )
    )

    result = snapshot.reference_price("XAU")

    assert result.price is None
    assert result.market_status is ReferenceMarketStatus.CLOSED
    assert result.stale is True
    assert result.market_status_as_of == as_of
    assert result.reason == "feed_rpc_unavailable"
    assert result.trade_block_reason(max_age_seconds=300, now=as_of) == "feed_rpc_unavailable"


def test_available_malformed_reference_is_a_critical_gateway_contract_failure():
    snapshot, _ = _snapshot(
        gateway_pb2.ReferencePriceResponse(
            instrument="XAU",
            quote="USD",
            chain="bsc",
            price="not-a-decimal",
            availability=gateway_pb2.REFERENCE_PRICE_AVAILABILITY_AVAILABLE,
        )
    )

    result = snapshot.reference_price("XAU")

    assert result.price is None
    assert result.stale is True
    assert snapshot.has_critical_data_failures()
    assert "malformed_reference_price" in snapshot.summarize_critical_data_failures()


def test_reference_price_trade_gate_rejects_old_closed_unknown_and_stale():
    observed_at = datetime(2026, 8, 10, 18, tzinfo=UTC)
    base = {
        "instrument": "XAU",
        "quote": "USD",
        "chain": "bsc",
        "price": Decimal("2412.34"),
        "confidence": 0.95,
        "source": "chainlink",
        "observed_at": observed_at,
        "stale": False,
        "market_status": ReferenceMarketStatus.OPEN,
        "market_status_as_of": observed_at,
        "market_status_source": "calendar",
    }

    fresh = ReferencePriceData(**base)
    assert fresh.trade_block_reason(max_age_seconds=300, now=datetime(2026, 8, 10, 18, 5, 1, tzinfo=UTC)) == (
        "reference_price_too_old"
    )
    assert (
        ReferencePriceData(**(base | {"market_status": ReferenceMarketStatus.CLOSED})).trade_block_reason(
            max_age_seconds=300, now=observed_at
        )
        == "reference_market_closed"
    )
    assert (
        ReferencePriceData(**(base | {"market_status": ReferenceMarketStatus.UNKNOWN})).trade_block_reason(
            max_age_seconds=300, now=observed_at
        )
        == "reference_market_unknown"
    )
    assert (
        ReferencePriceData(**(base | {"stale": True})).trade_block_reason(max_age_seconds=300, now=observed_at)
        == "reference_price_provider_stale"
    )
    assert ReferencePriceData(**(base | {"observed_at": observed_at.replace(tzinfo=None)})).is_tradeable(
        max_age_seconds=300, now=observed_at
    )
    assert (
        ReferencePriceData(**(base | {"observed_at": observed_at.replace(minute=1)})).trade_block_reason(
            max_age_seconds=300, now=observed_at
        )
        == "reference_price_timestamp_in_future"
    )
