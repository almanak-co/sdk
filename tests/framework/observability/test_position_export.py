"""Tests for position event export (Phase 3, VIB-2777)."""

import json

from almanak.framework.dashboard.export import export_positions


class TestExportPositions:
    def test_csv_export(self):
        rows = [
            {
                "id": "e1",
                "deployment_id": "strat:abc",
                "position_id": "12345",
                "event_type": "OPEN",
                "position_type": "LP",
                "timestamp": "2026-01-01T00:00:00",
                "value_usd": "10000",
            },
            {
                "id": "e2",
                "deployment_id": "strat:abc",
                "position_id": "12345",
                "event_type": "CLOSE",
                "position_type": "LP",
                "timestamp": "2026-01-02T00:00:00",
                "value_usd": "10500",
            },
        ]
        result = export_positions(rows, fmt="csv")
        text = result.decode("utf-8")
        lines = text.strip().split("\n")
        assert len(lines) == 3  # header + 2 rows
        assert "id,deployment_id,position_id" in lines[0]
        assert "OPEN" in lines[1]
        assert "CLOSE" in lines[2]

    def test_json_export(self):
        rows = [{"id": "e1", "event_type": "OPEN"}]
        result = export_positions(rows, fmt="json")
        data = json.loads(result.decode("utf-8"))
        assert len(data) == 1
        assert data[0]["event_type"] == "OPEN"

    def test_empty_export(self):
        result = export_positions([], fmt="csv")
        assert result == b""

    def test_empty_json_export(self):
        result = export_positions([], fmt="json")
        assert result == b"[]"
