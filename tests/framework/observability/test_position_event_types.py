"""Closed-vocabulary contracts for position lifecycle events (ALM-3078)."""

from datetime import UTC, datetime

import pytest

from almanak.framework.observability.position_events import (
    PositionEvent,
    PositionEventType,
    PositionEventTypeDecodeError,
    PositionType,
)


def test_constructor_normalizes_legacy_strings_to_enums() -> None:
    event = PositionEvent(position_type="LP", event_type="OPEN")

    assert event.position_type is PositionType.LP
    assert event.event_type is PositionEventType.OPEN


def test_to_dict_preserves_historical_string_shape() -> None:
    event = PositionEvent(
        position_type=PositionType.PERP,
        event_type=PositionEventType.CLOSE,
        timestamp=datetime(2026, 7, 1, tzinfo=UTC),
    )

    row = event.to_dict()

    assert row["position_type"] == "PERP"
    assert row["event_type"] == "CLOSE"
    assert row["timestamp"] == "2026-07-01T00:00:00+00:00"


def test_persisted_row_round_trips_to_typed_model() -> None:
    persisted = PositionEvent(
        id="event-1",
        position_id="position-1",
        position_type=PositionType.LENDING_DEBT,
        event_type=PositionEventType.DECREASE,
        timestamp=datetime(2026, 7, 1, tzinfo=UTC),
    ).to_dict()

    decoded = PositionEvent.from_persisted_row(persisted)

    assert decoded.position_type is PositionType.LENDING_DEBT
    assert decoded.event_type is PositionEventType.DECREASE
    assert decoded.to_dict()["position_type"] == persisted["position_type"]
    assert decoded.to_dict()["event_type"] == persisted["event_type"]


def test_persisted_row_preserves_zero_and_empty_value_semantics() -> None:
    row = {
        "timestamp": "2026-07-01T00:00:00+00:00",
        "position_type": "LP",
        "event_type": "OPEN",
        "amount0": 0,
        "value_usd": 0,
        "amount1": None,
        "protocol_fees_usd": 0,
        "attribution_json": "",
    }

    decoded = PositionEvent.from_persisted_row(row)

    assert decoded.amount0 == "0"
    assert decoded.amount1 == ""
    assert decoded.value_usd == "0"
    assert decoded.protocol_fees_usd == "0"
    assert decoded.attribution_json == "{}"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("position_type", "LIQUIDITY_POOL"),
        ("event_type", "OPNE"),
    ],
)
def test_constructor_rejects_unknown_vocabulary(field: str, value: str) -> None:
    with pytest.raises(PositionEventTypeDecodeError, match=field):
        PositionEvent(**{field: value})


def test_persisted_row_rejects_unknown_vocabulary() -> None:
    row = {
        "timestamp": "2026-07-01T00:00:00+00:00",
        "position_type": "LP",
        "event_type": "OPNE",
    }

    with pytest.raises(PositionEventTypeDecodeError, match="event_type"):
        PositionEvent.from_persisted_row(row)
