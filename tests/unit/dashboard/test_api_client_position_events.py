"""VIB-4347: ``DashboardAPIClient.get_position_events`` / ``get_position_history``.

Both methods route through the canonical ``StateService`` RPCs (no parsing of
``timeline_events.details_json``). The mock-spy tests below assert the right
RPC is called with the right deployment_id / position_types arguments.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.dashboard.custom.api_client import DashboardAPIClient


def _proto_event(position_id: str = "pid-1") -> SimpleNamespace:
    """Duck-typed PositionEventData proto row."""
    return SimpleNamespace(
        id="evt-1",
        deployment_id="MyStrategy:abc",
        cycle_id="cyc-1",
        execution_mode="live",
        position_id=position_id,
        position_type="LP",
        event_type="OPEN",
        timestamp=1746230400,  # 2026-05-13 00:00:00 UTC
        protocol="uniswap_v3",
        chain="arbitrum",
        token0="WETH",
        token1="USDC",
        amount0="1.0",
        amount1="3000.0",
        value_usd="6000.0",
        tick_lower=200,
        tick_upper=300,
        liquidity="1234567890",
        in_range=True,
        fees_token0="0",
        fees_token1="0",
        leverage="",
        entry_price="",
        mark_price="",
        unrealized_pnl="",
        is_long=None,
        tx_hash="0xabc",
        gas_usd="2.5",
        ledger_entry_id="ledger-1",
        protocol_fees_usd="0",
        attribution_json="{}",
        attribution_version=1,
    )


@pytest.fixture
def gateway_client_with_state() -> tuple[MagicMock, MagicMock]:
    state_stub = MagicMock(name="state_stub")
    raw_client = MagicMock(name="GatewayClient")
    raw_client.state = state_stub
    dashboard_client = MagicMock(name="GatewayDashboardClient")
    dashboard_client._client = raw_client
    return dashboard_client, state_stub


# =============================================================================
# get_position_events — calls GetPositionEventsFiltered with right args
# =============================================================================


def test_get_position_events_calls_filtered_rpc(
    gateway_client_with_state: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, state_stub = gateway_client_with_state
    response = MagicMock()
    response.events = [_proto_event(), _proto_event(position_id="pid-2")]
    response.error = ""
    state_stub.GetPositionEventsFiltered.return_value = response

    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")
    result = api.get_position_events(position_types=["LP", "PERP"])

    state_stub.GetPositionEventsFiltered.assert_called_once()
    call_request = state_stub.GetPositionEventsFiltered.call_args.args[0]
    assert call_request.deployment_id == "MyStrategy:abc"
    assert list(call_request.position_types) == ["LP", "PERP"]
    assert len(result) == 2
    # Adapter ran — dicts are populated, not raw proto.
    assert result[0]["position_id"] == "pid-1"
    assert result[1]["position_id"] == "pid-2"
    assert result[0]["chain"] == "arbitrum"


def test_get_position_events_none_filter_expands_to_all_position_types(
    gateway_client_with_state: tuple[MagicMock, MagicMock],
) -> None:
    """``position_types=None`` MUST expand to every known PositionType.

    The gateway treats an empty ``position_types`` list as the empty-set fast
    path (``state_service.py`` §GetPositionEventsFiltered), so the "no filter"
    docstring contract requires the client to materialise the full universe.
    Regression guard for the original ``position_types=[]`` bug flagged on
    PR #2270.
    """
    from almanak.framework.observability.position_events import PositionType

    dashboard_client, state_stub = gateway_client_with_state
    response = MagicMock()
    response.events = []
    response.error = ""
    state_stub.GetPositionEventsFiltered.return_value = response

    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")
    api.get_position_events()

    call_request = state_stub.GetPositionEventsFiltered.call_args.args[0]
    expected = [pt.value for pt in PositionType]
    assert list(call_request.position_types) == expected
    assert len(expected) > 0


def test_get_position_events_empty_list_passes_through_verbatim(
    gateway_client_with_state: tuple[MagicMock, MagicMock],
) -> None:
    """``position_types=[]`` MUST be passed through verbatim (not expanded).

    ``None`` (the default — "no filter") expands to every PositionType so
    the gateway returns rows; ``[]`` (an explicit empty filter) passes
    through verbatim so the gateway returns no rows. Conflating the two
    would silently broaden results when the caller computed a filter that
    turned out empty (e.g. "no allowed position types for this user").
    CodeRabbit major on PR #2270.
    """
    dashboard_client, state_stub = gateway_client_with_state
    response = MagicMock()
    response.events = []
    response.error = ""
    state_stub.GetPositionEventsFiltered.return_value = response

    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")
    api.get_position_events(position_types=[])

    call_request = state_stub.GetPositionEventsFiltered.call_args.args[0]
    assert list(call_request.position_types) == []


def test_get_position_events_handles_response_error(
    gateway_client_with_state: tuple[MagicMock, MagicMock],
) -> None:
    """Server-side error string -> empty list (no raise)."""
    dashboard_client, state_stub = gateway_client_with_state
    response = MagicMock()
    response.events = []
    response.error = "deployment_id is required"
    state_stub.GetPositionEventsFiltered.return_value = response

    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")
    assert api.get_position_events() == []


def test_get_position_events_handles_rpc_exception(
    gateway_client_with_state: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, state_stub = gateway_client_with_state
    state_stub.GetPositionEventsFiltered.side_effect = RuntimeError("gateway down")

    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")
    assert api.get_position_events() == []


# =============================================================================
# get_position_history — calls GetPositionHistory with right args
# =============================================================================


def test_get_position_history_calls_history_rpc(
    gateway_client_with_state: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, state_stub = gateway_client_with_state
    response = MagicMock()
    response.events = [_proto_event(position_id="pid-X")]
    state_stub.GetPositionHistory.return_value = response

    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")
    result = api.get_position_history(position_id="pid-X")

    state_stub.GetPositionHistory.assert_called_once()
    call_request = state_stub.GetPositionHistory.call_args.args[0]
    assert call_request.position_id == "pid-X"
    assert call_request.deployment_id == "MyStrategy:abc"
    assert call_request.strategy_id == "MyStrategy:abc"
    assert len(result) == 1
    assert result[0]["position_id"] == "pid-X"


def test_get_position_history_empty_position_id_returns_empty(
    gateway_client_with_state: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, state_stub = gateway_client_with_state
    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")

    assert api.get_position_history(position_id="") == []
    state_stub.GetPositionHistory.assert_not_called()


def test_get_position_history_handles_rpc_exception(
    gateway_client_with_state: tuple[MagicMock, MagicMock],
) -> None:
    dashboard_client, state_stub = gateway_client_with_state
    state_stub.GetPositionHistory.side_effect = RuntimeError("gateway down")

    api = DashboardAPIClient(dashboard_client, "MyStrategy:abc")
    assert api.get_position_history(position_id="pid-X") == []
