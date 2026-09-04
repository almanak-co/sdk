"""Tests for ``almanak strat export`` command routing and lifecycle."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import almanak.framework.cli.export as export_module


@pytest.fixture
def client():
    return MagicMock()


def _invoke(client, *args):
    with patch(
        "almanak.framework.dashboard.data_client.DashboardDataClient.for_gateway",
        return_value=client,
    ) as client_factory:
        result = CliRunner().invoke(export_module.export, ["--deployment-id", "deployment:test", *args])
    return result, client_factory


def test_exports_trades_to_stdout(client):
    with patch("almanak.framework.dashboard.export.export_trades", return_value=b"trade-data") as exporter:
        result, client_factory = _invoke(
            client,
            "--data",
            "trades",
            "--format",
            "json",
            "--limit",
            "25",
            "--host",
            "gateway.test",
            "--port",
            "1234",
        )

    assert result.exit_code == 0, result.output
    assert result.output == "trade-data"
    client_factory.assert_called_once_with(host="gateway.test", port=1234)
    client.connect.assert_called_once_with()
    exporter.assert_called_once_with(client, "deployment:test", limit=25, fmt="json")
    client.disconnect.assert_called_once_with()


def test_exports_timeline_to_file(client, tmp_path):
    output = tmp_path / "timeline.csv"
    with patch("almanak.framework.dashboard.export.export_timeline", return_value=b"timeline-data") as exporter:
        result, _ = _invoke(client, "--data", "timeline", "--output", str(output))

    assert result.exit_code == 0, result.output
    assert result.output == f"Exported timeline to {output} (13 bytes)\n"
    assert output.read_bytes() == b"timeline-data"
    exporter.assert_called_once_with(client, "deployment:test", limit=10000, fmt="csv")
    client.disconnect.assert_called_once_with()


def test_exports_pnl_to_stdout(client):
    with patch("almanak.framework.dashboard.export.export_pnl", return_value=b"pnl-data") as exporter:
        result, _ = _invoke(client, "--data", "pnl", "--format", "json")

    assert result.exit_code == 0, result.output
    assert result.output == "pnl-data"
    exporter.assert_called_once_with(client, "deployment:test", fmt="json")
    client.disconnect.assert_called_once_with()


def test_connection_failure_exits_without_exporting_or_disconnecting(client):
    client.connect.side_effect = RuntimeError("gateway unavailable")
    with patch("almanak.framework.dashboard.export.export_trades") as exporter:
        result, _ = _invoke(client, "--host", "gateway.test", "--port", "1234")

    assert result.exit_code == 1
    assert result.output == "Failed to connect to gateway at gateway.test:1234: gateway unavailable\n"
    exporter.assert_not_called()
    client.disconnect.assert_not_called()


def test_export_failure_still_disconnects(client):
    with patch(
        "almanak.framework.dashboard.export.export_trades",
        side_effect=RuntimeError("query failed"),
    ):
        result, _ = _invoke(client)

    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "query failed"
    client.disconnect.assert_called_once_with()


def test_rejects_unknown_data_type_before_connecting(client):
    result, client_factory = _invoke(client, "--data", "unknown")

    assert result.exit_code == 2
    assert "Invalid value for '--data' / '-d'" in result.output
    client_factory.assert_not_called()
    client.connect.assert_not_called()
