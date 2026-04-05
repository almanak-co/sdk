"""Tests for export functionality (VIB-2405)."""

import csv
import io
import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.dashboard.data_client import (
    DashboardDataClient,
    PnLDataPoint,
    TradeRecord,
)
from almanak.framework.dashboard.export import export_pnl, export_timeline, export_trades
from almanak.framework.dashboard.gateway_client import TimelineEvent


@pytest.fixture
def mock_client():
    client = MagicMock(spec=DashboardDataClient)
    return client


class TestExportTrades:
    def test_csv_format(self, mock_client):
        mock_client.get_trades.return_value = [
            TradeRecord(
                id="t1",
                strategy_id="s1",
                timestamp=datetime(2026, 4, 5, tzinfo=UTC),
                intent_type="SWAP",
                token_in="USDC",
                amount_in="1000",
                token_out="ETH",
                amount_out="0.5",
                success=True,
            ),
        ]

        result = export_trades(mock_client, "s1", fmt="csv")
        text = result.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["token_in"] == "USDC"
        assert rows[0]["token_out"] == "ETH"

    def test_json_format(self, mock_client):
        mock_client.get_trades.return_value = [
            TradeRecord(id="t1", strategy_id="s1", intent_type="SWAP"),
        ]

        result = export_trades(mock_client, "s1", fmt="json")
        data = json.loads(result)
        assert len(data) == 1
        assert data[0]["intent_type"] == "SWAP"

    def test_empty_trades_csv(self, mock_client):
        mock_client.get_trades.return_value = []
        result = export_trades(mock_client, "s1", fmt="csv")
        assert result == b""

    def test_empty_trades_json(self, mock_client):
        mock_client.get_trades.return_value = []
        result = export_trades(mock_client, "s1", fmt="json")
        assert json.loads(result) == []


class TestExportTimeline:
    def test_csv_format(self, mock_client):
        mock_client.get_timeline.return_value = [
            TimelineEvent(
                timestamp=datetime(2026, 4, 5, tzinfo=UTC),
                event_type="SWAP",
                description="Test swap",
                tx_hash="0xabc",
                chain="arbitrum",
            ),
        ]

        result = export_timeline(mock_client, "s1", fmt="csv")
        text = result.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["event_type"] == "SWAP"
        assert rows[0]["tx_hash"] == "0xabc"


class TestExportPnL:
    def test_csv_format(self, mock_client):
        mock_client.get_pnl_history.return_value = [
            PnLDataPoint(
                timestamp=datetime(2026, 4, 5, tzinfo=UTC),
                value_usd=Decimal("1000"),
                pnl_usd=Decimal("50"),
            ),
        ]

        result = export_pnl(mock_client, "s1", fmt="csv")
        text = result.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["value_usd"] == "1000"
        assert rows[0]["pnl_usd"] == "50"
