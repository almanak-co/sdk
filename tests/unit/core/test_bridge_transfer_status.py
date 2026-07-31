"""Canonical bridge-transfer lifecycle contract (ALM-3090)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.core.bridge import BridgeTransferStatus, parse_bridge_transfer_status
from almanak.framework.dashboard.models import BridgeTransfer


def test_known_wire_vocabulary_is_unchanged() -> None:
    assert {status.value for status in BridgeTransferStatus if status is not BridgeTransferStatus.UNKNOWN} == {
        "IN_FLIGHT",
        "COMPLETED",
        "FAILED",
    }


@pytest.mark.parametrize(
    "status",
    [
        BridgeTransferStatus.IN_FLIGHT,
        BridgeTransferStatus.COMPLETED,
        BridgeTransferStatus.FAILED,
    ],
)
def test_known_status_round_trips_through_json(status: BridgeTransferStatus) -> None:
    payload = json.dumps({"status": status})
    assert json.loads(payload)["status"] == status.value
    assert parse_bridge_transfer_status(json.loads(payload)["status"]) is status


@pytest.mark.parametrize("value", ["SETTLING", "completed", "", None, 7, object()])
def test_unknown_external_values_fail_safe(value: object) -> None:
    status = parse_bridge_transfer_status(value)
    assert status is BridgeTransferStatus.UNKNOWN
    assert not status.behavior.is_success
    assert not status.behavior.is_terminal
    assert not status.behavior.is_in_flight
    assert status.behavior.is_unknown


def test_status_behavior_is_exhaustive() -> None:
    assert BridgeTransferStatus.IN_FLIGHT.behavior.is_in_flight
    assert BridgeTransferStatus.COMPLETED.behavior.is_terminal
    assert BridgeTransferStatus.COMPLETED.behavior.is_success
    assert BridgeTransferStatus.FAILED.behavior.is_terminal
    assert not BridgeTransferStatus.FAILED.behavior.is_success
    assert BridgeTransferStatus.UNKNOWN.behavior.is_unknown


def test_dashboard_model_parses_historical_and_future_wire_values() -> None:
    known = BridgeTransfer(
        transfer_id="known",
        token="USDC",
        amount=Decimal("1"),
        from_chain="arbitrum",
        to_chain="base",
        initiated_at=datetime.now(UTC),
        status="COMPLETED",  # type: ignore[arg-type] -- external serialized fixture
    )
    future = BridgeTransfer(
        transfer_id="future",
        token="USDC",
        amount=Decimal("1"),
        from_chain="arbitrum",
        to_chain="base",
        initiated_at=datetime.now(UTC),
        status="SETTLING",  # type: ignore[arg-type] -- future external value
    )

    assert known.status is BridgeTransferStatus.COMPLETED
    assert future.status is BridgeTransferStatus.UNKNOWN
