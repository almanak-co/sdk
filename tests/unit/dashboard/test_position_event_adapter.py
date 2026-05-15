"""VIB-4347: ``PositionEventData`` -> dict adapter for dashboard plots."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from almanak.framework.dashboard.custom.position_event_adapter import (
    position_event_to_dict,
    position_events_to_position_data_dicts,
)
from almanak.framework.dashboard.plots import plot_positions_over_time


def _proto_event(
    position_id: str = "pid-1",
    event_type: str = "OPEN",
    timestamp: int = 1746230400,  # 2026-05-03 00:00:00 UTC
    tick_lower: int = 200,
    tick_upper: int = 300,
) -> SimpleNamespace:
    return SimpleNamespace(
        id="evt-1",
        deployment_id="MyStrategy:abc",
        cycle_id="cyc-1",
        execution_mode="live",
        position_id=position_id,
        position_type="LP",
        event_type=event_type,
        timestamp=timestamp,
        protocol="uniswap_v3",
        chain="arbitrum",
        token0="WETH",
        token1="USDC",
        amount0="1.0",
        amount1="3000.0",
        value_usd="6000.0",
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        liquidity="123",
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


# =============================================================================
# position_event_to_dict — basic shape
# =============================================================================


def test_position_event_to_dict_keys() -> None:
    d = position_event_to_dict(_proto_event())
    # Spot-check the most important keys are present and typed.
    assert d["position_id"] == "pid-1"
    assert d["event_type"] == "OPEN"
    assert d["chain"] == "arbitrum"
    # Timestamp is converted from epoch to ISO 8601. The test fixture's epoch
    # 1746230400 = 2025-05-03 UTC; assert via parseability rather than string
    # match to keep the test resilient to TZ formatting differences.
    parsed = datetime.fromisoformat(d["timestamp"])
    assert parsed.year == 2025 and parsed.month == 5
    assert d["tick_lower"] == 200
    assert d["tick_upper"] == 300


def test_position_event_to_dict_zero_timestamp_is_empty() -> None:
    """0 / falsy timestamp -> empty string (not 1970-01-01)."""
    d = position_event_to_dict(_proto_event(timestamp=0))
    assert d["timestamp"] == ""


# =============================================================================
# position_events_to_position_data_dicts — rollup OPEN+CLOSE per position
# =============================================================================


def test_rollup_one_open_only_position_is_active() -> None:
    rows = [_proto_event(position_id="p1", event_type="OPEN", timestamp=1746000000)]
    positions = position_events_to_position_data_dicts(rows)
    assert len(positions) == 1
    assert positions[0]["position_id"] == "p1"
    assert positions[0]["is_active"] is True
    assert positions[0]["date_end"] is None
    assert isinstance(positions[0]["date_start"], datetime)


def test_rollup_open_close_pair_is_closed() -> None:
    rows = [
        _proto_event(position_id="p1", event_type="OPEN", timestamp=1746000000),
        _proto_event(position_id="p1", event_type="CLOSE", timestamp=1746100000),
    ]
    positions = position_events_to_position_data_dicts(rows)
    assert len(positions) == 1
    assert positions[0]["is_active"] is False
    assert isinstance(positions[0]["date_start"], datetime)
    assert isinstance(positions[0]["date_end"], datetime)
    assert positions[0]["date_end"] > positions[0]["date_start"]


def test_rollup_multiple_positions_grouped_independently() -> None:
    rows = [
        _proto_event(position_id="p1", event_type="OPEN", timestamp=1746000000),
        _proto_event(position_id="p2", event_type="OPEN", timestamp=1746050000),
        _proto_event(position_id="p1", event_type="CLOSE", timestamp=1746100000),
    ]
    positions = position_events_to_position_data_dicts(rows)
    by_id = {p["position_id"]: p for p in positions}
    assert by_id["p1"]["is_active"] is False
    assert by_id["p2"]["is_active"] is True


def test_rollup_accepts_dicts_or_protos() -> None:
    """Adapter must accept both raw proto rows AND pre-converted dicts so the
    Dashboard API client can stream either shape through."""
    raw = _proto_event(position_id="p1", event_type="OPEN", timestamp=1746000000)
    as_dict = position_event_to_dict(raw)
    via_proto = position_events_to_position_data_dicts([raw])
    via_dict = position_events_to_position_data_dicts([as_dict])
    assert via_proto == via_dict


# =============================================================================
# Plot integration — plot_positions_over_time accepts the adapter output
# =============================================================================


def test_adapter_output_consumable_by_plot_positions_over_time() -> None:
    """End-to-end: adapter output -> plot_positions_over_time(...) must not raise.

    `plot_positions_over_time` accepts both PositionData and dict input. The
    dict shape produced by `position_events_to_position_data_dicts` must
    contain enough fields to satisfy the plot helper without error.
    """
    import pandas as pd

    rows = [
        _proto_event(position_id="p1", event_type="OPEN", timestamp=1746000000),
        _proto_event(position_id="p1", event_type="CLOSE", timestamp=1746100000),
    ]
    positions = position_events_to_position_data_dicts(rows)
    # plot_positions_over_time expects a pandas DataFrame for price_data;
    # empty DataFrame triggers the empty-data short-circuit cleanly.
    price_data = pd.DataFrame(columns=["timestamp", "close"])
    fig = plot_positions_over_time(positions=positions, price_data=price_data)
    assert fig is not None
