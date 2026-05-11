"""CLI smoke tests for ``almanak ax positions reconcile`` (T24 / VIB-4210).

These tests verify:
- The subcommand is registered as `ax positions reconcile` (proper Click group).
- `--help` renders cleanly.
- Argument parsing works for the documented options.
- A mocked gateway dispatch + response rendering doesn't crash.

Full network-level / gateway-integration tests live with the gateway
service tests; this file isolates the CLI-shape contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.framework.cli.ax import ax
from almanak.gateway.proto import gateway_pb2


def test_positions_group_exists():
    runner = CliRunner()
    result = runner.invoke(ax, ["positions", "--help"])
    assert result.exit_code == 0
    assert "reconcile" in result.output


def test_positions_reconcile_help():
    runner = CliRunner()
    result = runner.invoke(ax, ["positions", "reconcile", "--help"])
    assert result.exit_code == 0
    assert "--deployment-id" in result.output
    assert "--apply" in result.output
    assert "--max-age-blocks" in result.output
    assert "--operator-note" in result.output
    assert "--trigger" in result.output
    # The help text references the four diff buckets so operators know what
    # the command actually does.
    assert "phantom_missing" in result.output
    assert "stranded" in result.output


def test_positions_reconcile_requires_deployment_id():
    runner = CliRunner()
    result = runner.invoke(ax, ["positions", "reconcile"])
    # Click should reject with non-zero exit and surface the missing-option error.
    assert result.exit_code != 0
    assert "deployment-id" in result.output.lower() or "deployment_id" in result.output.lower()


def test_positions_reconcile_dispatches_request():
    """Mock _get_executor + client.position.Reconcile; verify request marshalling."""
    runner = CliRunner()
    fake_response = gateway_pb2.ReconcileResponse(
        reconciliation_id="abc-uuid-1234",
        source_block_number=12345678,
        matched_count=0,
        phantom_missing_count=0,
        stranded_count=0,
        rebuilt_count=0,
        duration_seconds=0.123,
    )
    mock_client = MagicMock()
    mock_client.position.Reconcile.return_value = fake_response

    captured: dict = {}

    def fake_get_executor(_ctx):
        return MagicMock(), mock_client

    with patch("almanak.framework.cli.ax._get_executor", fake_get_executor):
        result = runner.invoke(
            ax,
            ["positions", "reconcile", "--deployment-id", "TestStrat:abc"],
            catch_exceptions=False,
        )
    assert result.exit_code == 0, f"exit={result.exit_code}, output={result.output}"

    # Verify the Reconcile call was made with the right shape.
    mock_client.position.Reconcile.assert_called_once()
    call_args, call_kwargs = mock_client.position.Reconcile.call_args
    request = call_args[0]
    assert request.deployment_id == "TestStrat:abc"
    assert request.primitives == ["lp"]
    assert request.apply is False  # dry-run by default
    assert request.trigger == "operator_cli"

    # Verify the response rendering happened.
    assert "reconciliation_id: abc-uuid-1234" in result.output
    assert "source_block_number: 12345678" in result.output


def test_positions_reconcile_apply_flag_dispatches_apply_true():
    runner = CliRunner()
    fake_response = gateway_pb2.ReconcileResponse(
        reconciliation_id="apply-uuid",
        source_block_number=999,
        matched_count=0,
        phantom_missing_count=1,
        stranded_count=0,
        rebuilt_count=1,
    )
    fake_response.rebuilt.append(
        gateway_pb2.RebuiltRow(
            physical_identity_hash="hash_rebuilt",
            primitive="lp",
            accounting_category="lp",
            source="reconciliation_discovery",
            last_reconciled_at_block=999,
            reconciliation_id="apply-uuid",
        )
    )
    mock_client = MagicMock()
    mock_client.position.Reconcile.return_value = fake_response

    with patch("almanak.framework.cli.ax._get_executor", return_value=(MagicMock(), mock_client)):
        result = runner.invoke(
            ax,
            ["positions", "reconcile", "--deployment-id", "TestStrat:abc", "--apply"],
            catch_exceptions=False,
        )
    assert result.exit_code == 0
    request = mock_client.position.Reconcile.call_args[0][0]
    assert request.apply is True
    assert "registry written" in result.output  # apply-label shown
    assert "hash_rebuilt"[:16] in result.output


def test_positions_reconcile_json_output():
    runner = CliRunner()
    fake_response = gateway_pb2.ReconcileResponse(
        reconciliation_id="json-uuid",
        source_block_number=1,
        matched_count=0,
        phantom_missing_count=0,
        stranded_count=0,
        rebuilt_count=0,
        duration_seconds=0.001,
    )
    mock_client = MagicMock()
    mock_client.position.Reconcile.return_value = fake_response

    with patch("almanak.framework.cli.ax._get_executor", return_value=(MagicMock(), mock_client)):
        result = runner.invoke(
            ax,
            ["--json", "positions", "reconcile", "--deployment-id", "TestStrat:abc"],
            catch_exceptions=False,
        )
    assert result.exit_code == 0
    import json

    parsed = json.loads(result.output)
    assert parsed["reconciliation_id"] == "json-uuid"
    assert parsed["source_block_number"] == 1
    assert "matched_count" in parsed


def test_positions_reconcile_primitive_filter_csv():
    runner = CliRunner()
    fake_response = gateway_pb2.ReconcileResponse(reconciliation_id="primitive-uuid")
    mock_client = MagicMock()
    mock_client.position.Reconcile.return_value = fake_response

    with patch("almanak.framework.cli.ax._get_executor", return_value=(MagicMock(), mock_client)):
        result = runner.invoke(
            ax,
            [
                "positions",
                "reconcile",
                "--deployment-id",
                "TestStrat:abc",
                "--primitives",
                "lp,lp",  # csv with dup; should split + strip
            ],
            catch_exceptions=False,
        )
    assert result.exit_code == 0
    request = mock_client.position.Reconcile.call_args[0][0]
    assert list(request.primitives) == ["lp", "lp"]
