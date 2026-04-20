"""Tests for DashboardDataClient (VIB-2404)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.dashboard.data_client import (
    DashboardDataClient,
    PnLDataPoint,
    PortfolioMetricsSummary,
    TradeRecord,
)
from almanak.framework.dashboard.gateway_client import (
    GatewayDashboardClient,
    PositionInfo,
    StrategyDetails,
    StrategySummary,
    TimelineEvent,
)


@pytest.fixture
def mock_gw():
    """Create a mock GatewayDashboardClient."""
    gw = MagicMock(spec=GatewayDashboardClient)
    gw.is_connected = True
    return gw


@pytest.fixture
def client(mock_gw):
    """Create a DashboardDataClient wrapping the mock."""
    return DashboardDataClient(gateway_client=mock_gw)


class TestDashboardDataClient:
    def test_get_strategies_delegates_to_gw(self, client, mock_gw):
        mock_gw.list_strategies.return_value = [
            StrategySummary(
                strategy_id="s1",
                name="Test",
                status="RUNNING",
                chain="arbitrum",
                protocol="uniswap_v3",
                total_value_usd=Decimal("1000"),
                pnl_24h_usd=Decimal("50"),
                last_action_at=None,
                attention_required=False,
                attention_reason="",
                is_multi_chain=False,
            )
        ]

        result = client.get_strategies()
        assert len(result) == 1
        assert result[0].strategy_id == "s1"
        mock_gw.list_strategies.assert_called_once()

    def test_get_strategy_detail_delegates(self, client, mock_gw):
        mock_gw.get_strategy_details.return_value = StrategyDetails(
            summary=StrategySummary(
                strategy_id="s1",
                name="Test",
                status="RUNNING",
                chain="base",
                protocol="aerodrome",
                total_value_usd=Decimal("500"),
                pnl_24h_usd=Decimal("10"),
                last_action_at=None,
                attention_required=False,
                attention_reason="",
                is_multi_chain=False,
            ),
            position=PositionInfo(),
        )

        detail = client.get_strategy_detail("s1")
        assert detail.summary.strategy_id == "s1"
        mock_gw.get_strategy_details.assert_called_once()

    def test_get_timeline_delegates(self, client, mock_gw):
        mock_gw.get_timeline.return_value = [
            TimelineEvent(
                timestamp=datetime(2026, 4, 5, tzinfo=UTC),
                event_type="SWAP",
                description="Test swap",
            )
        ]

        events = client.get_timeline("s1", limit=10)
        assert len(events) == 1
        assert events[0].event_type == "SWAP"

    def test_get_pnl_history(self, client, mock_gw):
        mock_gw.get_strategy_details.return_value = StrategyDetails(
            summary=StrategySummary(
                strategy_id="s1",
                name="T",
                status="RUNNING",
                chain="",
                protocol="",
                total_value_usd=Decimal("0"),
                pnl_24h_usd=Decimal("0"),
                last_action_at=None,
                attention_required=False,
                attention_reason="",
                is_multi_chain=False,
            ),
            position=PositionInfo(),
            pnl_history=[
                {
                    "timestamp": datetime(2026, 4, 5, 12, 0, tzinfo=UTC),
                    "value_usd": Decimal("1000"),
                    "pnl_usd": Decimal("50"),
                },
            ],
        )

        points = client.get_pnl_history("s1")
        assert len(points) == 1
        assert isinstance(points[0], PnLDataPoint)
        assert points[0].value_usd == Decimal("1000")

    def test_get_portfolio_metrics(self, client, mock_gw):
        mock_gw.get_strategy_details.return_value = StrategyDetails(
            summary=StrategySummary(
                strategy_id="s1",
                name="T",
                status="RUNNING",
                chain="",
                protocol="",
                total_value_usd=Decimal("5000"),
                pnl_24h_usd=Decimal("100"),
                last_action_at=None,
                attention_required=False,
                attention_reason="",
                is_multi_chain=False,
            ),
            position=PositionInfo(),
        )

        metrics = client.get_portfolio_metrics("s1")
        assert isinstance(metrics, PortfolioMetricsSummary)
        assert metrics.total_value_usd == Decimal("5000")
        assert metrics.pnl_usd == Decimal("100")

    def test_execute_action_delegates(self, client, mock_gw):
        mock_gw.execute_action.return_value = True
        assert client.execute_action("s1", "PAUSE", "testing") is True


class TestTradeRecord:
    def test_to_dict(self):
        trade = TradeRecord(
            id="t1",
            strategy_id="s1",
            timestamp=datetime(2026, 4, 5, tzinfo=UTC),
            intent_type="SWAP",
            token_in="USDC",
            amount_in="1000",
            token_out="ETH",
            amount_out="0.5",
        )
        d = trade.to_dict()
        assert d["id"] == "t1"
        assert d["token_in"] == "USDC"
        assert "2026-04-05" in d["timestamp"]
