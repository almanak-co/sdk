"""VIB-4347: ``DashboardAPIClient.get_ohlcv`` routes through the shared factory.

The whole point of VIB-4347 is to **avoid** dashboards constructing
``gateway_pb2.GeckoTerminalGetOHLCV`` directly. The mock-spy test below is
the load-bearing assertion: any future code change that re-introduces a
hardcoded GeckoTerminal call from the dashboard API client will fail this
test loudly.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.models import DataEnvelope, DataMeta
from almanak.framework.dashboard.custom.api_client import DashboardAPIClient


def _candle(close: str = "1900") -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=datetime(2026, 5, 13, 12, 0, 0, tzinfo=UTC),
        open=Decimal("1890"),
        high=Decimal("1910"),
        low=Decimal("1880"),
        close=Decimal(close),
        volume=Decimal("123.45"),
    )


def _envelope(
    candles: list[OHLCVCandle],
    *,
    source: str = "geckoterminal",
    confidence: float = 1.0,
    cache_hit: bool = False,
) -> DataEnvelope[list[OHLCVCandle]]:
    return DataEnvelope(
        value=candles,
        meta=DataMeta(
            source=source,
            observed_at=datetime.now(UTC),
            confidence=confidence,
            cache_hit=cache_hit,
        ),
    )


@pytest.fixture
def gateway_client_with_spy() -> tuple[MagicMock, MagicMock]:
    """Return (DashboardClient, integration-spy) with the spy attached so we
    can assert it is NEVER invoked by ``get_ohlcv``."""
    integration_spy = MagicMock(name="integration_stub")
    raw_client = MagicMock(name="GatewayClient")
    raw_client.integration = integration_spy
    raw_client.market = MagicMock(name="market_stub")
    dashboard_client = MagicMock(name="GatewayDashboardClient")
    dashboard_client._client = raw_client
    return dashboard_client, integration_spy


# =============================================================================
# D1.4 — get_ohlcv routes through the factory and NOT GeckoTerminalGetOHLCV
# =============================================================================


def test_get_ohlcv_does_not_call_geckoterminal_directly(
    gateway_client_with_spy: tuple[MagicMock, MagicMock],
) -> None:
    """The load-bearing test for VIB-4347's whole purpose."""
    dashboard_client, integration_spy = gateway_client_with_spy
    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")

    fake_stack = MagicMock(name="OHLCVStack")
    fake_stack.router.get_ohlcv.return_value = _envelope([_candle()])
    with patch(
        "almanak.framework.data.ohlcv.create_ohlcv_stack",
        return_value=fake_stack,
    ):
        result = api.get_ohlcv(
            "WETH", chain="arbitrum", pool_address="0xabc", timeframe="1h", limit=10
        )

    # Direct GeckoTerminalGetOHLCV call is forbidden.
    integration_spy.GeckoTerminalGetOHLCV.assert_not_called()
    # We did get the candle back via the factory path.
    assert len(result) == 1
    assert result[0]["close"] == "1900"


def test_get_ohlcv_routes_through_factory(
    gateway_client_with_spy: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, _ = gateway_client_with_spy
    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")

    fake_stack = MagicMock(name="OHLCVStack")
    fake_stack.router.get_ohlcv.return_value = _envelope([_candle()])
    with patch(
        "almanak.framework.data.ohlcv.create_ohlcv_stack",
        return_value=fake_stack,
    ) as factory_spy:
        api.get_ohlcv(
            "WETH",
            chain="arbitrum",
            pool_address="0xpool",
            timeframe="4h",
            limit=42,
        )

    factory_spy.assert_called_once()
    factory_kwargs = factory_spy.call_args.kwargs
    assert factory_kwargs["chain"] == "arbitrum"
    assert factory_kwargs["pool_address"] == "0xpool"
    # Router is called with the resolved chain / timeframe / limit / pool_address.
    fake_stack.router.get_ohlcv.assert_called_once()
    router_kwargs = fake_stack.router.get_ohlcv.call_args.kwargs
    assert router_kwargs["chain"] == "arbitrum"
    assert router_kwargs["pool_address"] == "0xpool"
    assert router_kwargs["timeframe"] == "4h"
    assert router_kwargs["limit"] == 42
    assert router_kwargs["quote"] == "USD"


# =============================================================================
# D1.4 + provenance — dicts include source / confidence / cache_hit when set
# =============================================================================


def test_get_ohlcv_dict_includes_provenance(
    gateway_client_with_spy: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, _ = gateway_client_with_spy
    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")

    fake_stack = MagicMock(name="OHLCVStack")
    fake_stack.router.get_ohlcv.return_value = _envelope(
        [_candle()],
        source="binance",
        confidence=0.7,
        cache_hit=True,
    )
    with patch(
        "almanak.framework.data.ohlcv.create_ohlcv_stack",
        return_value=fake_stack,
    ):
        rows = api.get_ohlcv("WETH", chain="arbitrum")

    assert len(rows) == 1
    row = rows[0]
    # Core OHLCV
    assert row["timestamp"] == "2026-05-13T12:00:00+00:00"
    assert row["open"] == "1890"
    assert row["high"] == "1910"
    assert row["low"] == "1880"
    assert row["close"] == "1900"
    assert row["volume"] == "123.45"
    # Provenance — losing this at the dashboard boundary recreates part of the
    # hardcoded-provider problem this factory exists to prevent.
    assert row["source"] == "binance"
    assert row["confidence"] == 0.7
    assert row["cache_hit"] is True


# =============================================================================
# D1.4 — chain resolution (explicit arg first, then strategy config)
# =============================================================================


def test_get_ohlcv_chain_resolution_falls_back_to_config(
    gateway_client_with_spy: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, _ = gateway_client_with_spy
    dashboard_client.get_strategy_config.return_value = {"default_chain": "base"}
    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")

    fake_stack = MagicMock(name="OHLCVStack")
    fake_stack.router.get_ohlcv.return_value = _envelope([_candle()])
    with patch(
        "almanak.framework.data.ohlcv.create_ohlcv_stack",
        return_value=fake_stack,
    ) as factory_spy:
        api.get_ohlcv("WETH")  # no explicit chain

    factory_spy.assert_called_once()
    assert factory_spy.call_args.kwargs["chain"] == "base"


def test_get_ohlcv_no_chain_returns_empty(
    gateway_client_with_spy: tuple[MagicMock, MagicMock],
) -> None:
    """When no chain is passable AND none in config, return [] (no raise)."""
    dashboard_client, _ = gateway_client_with_spy
    dashboard_client.get_strategy_config.return_value = {}
    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")

    with patch(
        "almanak.framework.data.ohlcv.create_ohlcv_stack",
    ) as factory_spy:
        result = api.get_ohlcv("WETH")

    assert result == []
    factory_spy.assert_not_called()


# =============================================================================
# F1 — get_ohlcv handles factory error without raising
# =============================================================================


def test_get_ohlcv_handles_factory_error(
    gateway_client_with_spy: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, _ = gateway_client_with_spy
    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")

    with patch(
        "almanak.framework.data.ohlcv.create_ohlcv_stack",
        side_effect=RuntimeError("gateway is down"),
    ):
        result = api.get_ohlcv("WETH", chain="arbitrum")
    assert result == []


# =============================================================================
# F2 — get_ohlcv with pool_address=None still works (CEX-token path)
# =============================================================================


def test_get_ohlcv_pool_address_none_falls_back_to_token(
    gateway_client_with_spy: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, _ = gateway_client_with_spy
    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")

    fake_stack = MagicMock(name="OHLCVStack")
    fake_stack.router.get_ohlcv.return_value = _envelope([_candle()])
    with patch(
        "almanak.framework.data.ohlcv.create_ohlcv_stack",
        return_value=fake_stack,
    ) as factory_spy:
        rows = api.get_ohlcv("WETH", chain="arbitrum")  # no pool_address

    factory_spy.assert_called_once()
    assert factory_spy.call_args.kwargs["pool_address"] is None
    assert len(rows) == 1


# =============================================================================
# Provenance NOT stamped when meta is absent — never invent it.
# =============================================================================


def test_get_ohlcv_no_provenance_when_envelope_lacks_meta(
    gateway_client_with_spy: tuple[MagicMock, MagicMock],
) -> None:
    """If the envelope has no recognizable provenance, the dict must NOT
    contain `source` / `confidence` / `cache_hit` — never invent provenance."""
    dashboard_client, _ = gateway_client_with_spy
    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")

    # Build a duck-typed envelope that has .value but no .meta with attrs.
    fake_envelope = MagicMock()
    fake_envelope.value = [_candle()]
    fake_envelope.meta = None
    fake_stack = MagicMock(name="OHLCVStack")
    fake_stack.router.get_ohlcv.return_value = fake_envelope
    with patch(
        "almanak.framework.data.ohlcv.create_ohlcv_stack",
        return_value=fake_stack,
    ):
        rows = api.get_ohlcv("WETH", chain="arbitrum")

    assert len(rows) == 1
    row = rows[0]
    assert "source" not in row
    assert "confidence" not in row
    assert "cache_hit" not in row
