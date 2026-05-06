"""Unit tests for `almanak.framework.cli.status:strategy_logs` and its helpers.

Targets the helpers extracted from `strategy_logs` (VIB-4080 W3 Sub-F). Per the
W3 Sub-A audit, `strategy_logs` had **zero** direct test references in the repo
before this file landed — every test below is pure coverage lift.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from almanak.framework.cli import status as status_mod
from almanak.framework.cli.status import _parse_since_value, _validate_logs_limit, strategy_logs

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


def _make_event(
    *,
    timestamp: int = 1_700_000_000,
    event_type: str = "TRADE",
    description: str = "Buy 1 ETH",
    chain: str = "arbitrum",
    tx_hash: str = "",
    details_json: str = "",
) -> SimpleNamespace:
    """Build a TimelineEvent-like duck-typed object."""
    return SimpleNamespace(
        timestamp=timestamp,
        event_type=event_type,
        description=description,
        chain=chain,
        tx_hash=tx_hash,
        details_json=details_json,
    )


def _make_response(events: list[Any] | None = None, has_more: bool = False) -> SimpleNamespace:
    """Build a GetTimelineResponse-like duck-typed object."""
    return SimpleNamespace(events=list(events or []), has_more=has_more)


class _FakeDashboard:
    """Stand-in for `client.dashboard`."""

    def __init__(self, response: Any = None, raise_exc: Exception | None = None) -> None:
        self._response = response
        self._raise = raise_exc
        self.last_request: Any = None

    def GetTimeline(self, request: Any) -> Any:  # noqa: N802 (proto naming)
        self.last_request = request
        if self._raise is not None:
            raise self._raise
        return self._response


class _FakeClient:
    """Stand-in for GatewayClient that records disconnect calls."""

    def __init__(self, response: Any = None, raise_exc: Exception | None = None) -> None:
        self.dashboard = _FakeDashboard(response=response, raise_exc=raise_exc)
        self.disconnect_called = 0

    def disconnect(self) -> None:
        self.disconnect_called += 1


def _invoke_logs(fake_client: _FakeClient, *args: str) -> Any:
    """Invoke `strategy_logs` via CliRunner with `_make_client` patched."""
    runner = CliRunner()
    with patch.object(status_mod, "_make_client", return_value=fake_client):
        return runner.invoke(strategy_logs, list(args))


# ---------------------------------------------------------------------------
# Helper-level test (1)
# ---------------------------------------------------------------------------


def test_validate_logs_limit_rejects_below_one(capsys: pytest.CaptureFixture) -> None:
    """`_validate_logs_limit` exits 1 for any value < 1, accepts >= 1 silently."""
    # Valid values pass through without exit / output.
    _validate_logs_limit(1)
    _validate_logs_limit(100)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""

    # 0 -> sys.exit(1) with the verbatim guidance line.
    with pytest.raises(SystemExit) as excinfo:
        _validate_logs_limit(0)
    assert excinfo.value.code == 1
    assert "--limit must be >= 1." in capsys.readouterr().err

    # Negative also rejected.
    with pytest.raises(SystemExit) as excinfo:
        _validate_logs_limit(-5)
    assert excinfo.value.code == 1
    assert "--limit must be >= 1." in capsys.readouterr().err


def test_strategy_logs_rejects_invalid_limit_via_cli() -> None:
    """End-to-end: `strategy_logs --limit 0` exits non-zero with the guard message."""
    runner = CliRunner()
    # No client setup needed — validation runs before any gateway call.
    result = runner.invoke(strategy_logs, ["-s", "any", "--limit", "0"])
    assert result.exit_code != 0
    assert "--limit must be >= 1." in (result.output + (result.stderr_bytes or b"").decode())


def test_parse_since_value_handles_none_epoch_iso_and_invalid(capsys: pytest.CaptureFixture) -> None:
    """`_parse_since_value` covers all four branches of the --since parser."""
    # Empty / None → 0
    assert _parse_since_value(None) == 0
    assert _parse_since_value("") == 0
    # Epoch-seconds string → int
    assert _parse_since_value("1700000000") == 1_700_000_000
    # ISO 8601 with Z suffix → fromisoformat round-trip
    from datetime import datetime as _dt

    expected = int(_dt.fromisoformat("2026-03-01T00:00:00+00:00").timestamp())
    assert _parse_since_value("2026-03-01T00:00:00Z") == expected
    # Invalid value → sys.exit(1) with a verbatim error string
    with pytest.raises(SystemExit) as excinfo:
        _parse_since_value("not-a-timestamp")
    assert excinfo.value.code == 1
    assert "Invalid --since value: not-a-timestamp" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Click command (CliRunner) tests (6)
# ---------------------------------------------------------------------------


def test_strategy_logs_happy_path_pretty() -> None:
    """Mocked gateway returns 3 events → pretty header + bodies + footer."""
    client = _FakeClient(
        response=_make_response(
            events=[
                _make_event(event_type="TRADE", description="Buy 1 ETH", tx_hash="0xdead"),
                _make_event(timestamp=1_700_000_500, event_type="REBALANCE", description="Rebalance"),
                _make_event(timestamp=1_700_001_000, event_type="ERROR", description="oops"),
            ],
            has_more=True,
        )
    )
    result = _invoke_logs(client, "-s", "demo")
    assert result.exit_code == 0, result.output
    assert "Timeline: demo" in result.output
    assert "Buy 1 ETH" in result.output
    assert "Rebalance" in result.output
    assert "tx: 0xdead" in result.output
    # has_more=True triggers the footer suffix
    assert "Showing 3 events (more available)" in result.output
    assert "Filter:" not in result.output  # no --type passed
    assert client.disconnect_called == 1
    # Default request fields
    req = client.dashboard.last_request
    assert req.strategy_id == "demo"
    assert req.event_type_filter == ""
    assert req.since_timestamp == 0
    assert req.limit == 50


def test_strategy_logs_as_json_emits_json_array() -> None:
    """`--json` emits a parseable array including details JSON."""
    client = _FakeClient(
        response=_make_response(
            events=[_make_event(tx_hash="0xfeed", details_json='{"amount": 1}')],
        )
    )
    result = _invoke_logs(client, "-s", "demo", "--json")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output.strip())
    assert isinstance(data, list) and len(data) == 1
    assert data[0]["event_type"] == "TRADE"
    assert data[0]["tx_hash"] == "0xfeed"
    assert data[0]["details"] == {"amount": 1}


def test_strategy_logs_event_type_filter_uppercases_and_prints_filter() -> None:
    """`--type trade` is uppercased into the request and printed in pretty mode."""
    client = _FakeClient(response=_make_response(events=[_make_event()]))
    result = _invoke_logs(client, "-s", "demo", "--type", "trade")
    assert result.exit_code == 0, result.output
    assert client.dashboard.last_request.event_type_filter == "TRADE"
    assert "Filter: trade" in result.output


def test_strategy_logs_since_filter_propagates_to_request() -> None:
    """`--since <epoch>` is parsed and forwarded into the proto request."""
    client = _FakeClient(response=_make_response(events=[_make_event()]))
    result = _invoke_logs(client, "-s", "demo", "--since", "1700000000")
    assert result.exit_code == 0, result.output
    assert client.dashboard.last_request.since_timestamp == 1_700_000_000


def test_strategy_logs_empty_result_messages() -> None:
    """No events → friendly pretty message AND `[]` for --json."""
    # Pretty
    client = _FakeClient(response=_make_response(events=[]))
    result = _invoke_logs(client, "-s", "missing")
    assert result.exit_code == 0, result.output
    assert "No events found for strategy: missing" in result.output
    assert client.disconnect_called == 1
    # JSON
    client = _FakeClient(response=_make_response(events=[]))
    result = _invoke_logs(client, "-s", "missing", "--json")
    assert result.exit_code == 0, result.output
    assert result.output.strip() == "[]"


def test_strategy_logs_gateway_error_exits_1_and_disconnects() -> None:
    """RPC failure → exit 1, exact error string, finally-clause disconnect runs."""
    client = _FakeClient(raise_exc=RuntimeError("rpc dropped"))
    result = _invoke_logs(client, "-s", "demo")
    assert result.exit_code == 1
    assert "Failed to get timeline: rpc dropped" in result.output
    assert client.disconnect_called == 1
