"""Tests for teardown API strategy registry data conversion."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from almanak.framework.api import teardown as teardown_api
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary


def _summary(*positions: PositionInfo) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:test",
        timestamp=datetime.now(UTC),
        positions=list(positions),
    )


def _position(position_id: str, health_factor: Decimal | None) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id=position_id,
        chain="arbitrum",
        protocol="aave_v3",
        value_usd=Decimal("100"),
        liquidation_risk=health_factor is not None and health_factor < Decimal("1"),
        health_factor=health_factor,
        details={"asset": "USDC"},
    )


def test_get_strategy_data_requires_configured_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(teardown_api, "_strategy_registry", None)

    with pytest.raises(HTTPException) as exc_info:
        teardown_api._get_strategy_data("deployment:missing")

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Strategy registry not configured. Cannot query strategy data."


def test_get_strategy_data_reports_missing_deployment(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = MagicMock()
    registry.get_strategy.return_value = None
    registry.list_strategies.return_value = ["deployment:available"]
    monkeypatch.setattr(teardown_api, "_strategy_registry", registry)

    with pytest.raises(HTTPException) as exc_info:
        teardown_api._get_strategy_data("deployment:missing")

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == ("Strategy deployment:missing not found. Available: ['deployment:available']")
    registry.get_strategy.assert_called_once_with("deployment:missing")


def test_get_strategy_data_preserves_position_query_error(monkeypatch: pytest.MonkeyPatch) -> None:
    error = RuntimeError("gateway unavailable")
    strategy = MagicMock()
    strategy.get_open_positions.side_effect = error
    registry = MagicMock()
    registry.get_strategy.return_value = strategy
    monkeypatch.setattr(teardown_api, "_strategy_registry", registry)

    with pytest.raises(HTTPException) as exc_info:
        teardown_api._get_strategy_data("deployment:broken")

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Failed to query positions from strategy: gateway unavailable"
    assert exc_info.value.__cause__ is error


def test_get_strategy_data_serializes_positions_and_minimum_health_factor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    positions = (
        _position("no-health", None),
        _position("healthy", Decimal("2")),
        _position("less-healthy", Decimal("1.25")),
        _position("insolvent", Decimal("0")),
    )
    strategy = SimpleNamespace(
        name="Aave Strategy",
        chain="arbitrum",
        protocol="aave_v3",
        get_open_positions=lambda: _summary(*positions),
    )
    registry = MagicMock()
    registry.get_strategy.return_value = strategy
    monkeypatch.setattr(teardown_api, "_strategy_registry", registry)

    data = teardown_api._get_strategy_data("deployment:test")

    assert data == {
        "deployment_id": "deployment:test",
        "name": "Aave Strategy",
        "chain": "arbitrum",
        "protocol": "aave_v3",
        "positions": [
            {
                "type": "SUPPLY",
                "position_id": "no-health",
                "chain": "arbitrum",
                "protocol": "aave_v3",
                "value_usd": 100.0,
                "liquidation_risk": False,
                "details": {"asset": "USDC"},
            },
            {
                "type": "SUPPLY",
                "position_id": "healthy",
                "chain": "arbitrum",
                "protocol": "aave_v3",
                "value_usd": 100.0,
                "liquidation_risk": False,
                "details": {"asset": "USDC"},
                "health_factor": 2.0,
            },
            {
                "type": "SUPPLY",
                "position_id": "less-healthy",
                "chain": "arbitrum",
                "protocol": "aave_v3",
                "value_usd": 100.0,
                "liquidation_risk": False,
                "details": {"asset": "USDC"},
                "health_factor": 1.25,
            },
            {
                "type": "SUPPLY",
                "position_id": "insolvent",
                "chain": "arbitrum",
                "protocol": "aave_v3",
                "value_usd": 100.0,
                "liquidation_risk": True,
                "details": {"asset": "USDC"},
                "health_factor": 0.0,
            },
        ],
        "total_value_usd": 400.0,
        "health_factor": 0.0,
    }

    rebuilt = teardown_api._build_position_summary(data)
    assert [position.health_factor for position in rebuilt.positions] == [
        None,
        Decimal("2.0"),
        Decimal("1.25"),
        Decimal("0.0"),
    ]


def test_get_strategy_data_defaults_optional_strategy_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    strategy = SimpleNamespace(get_open_positions=lambda: _summary())
    registry = MagicMock()
    registry.get_strategy.return_value = strategy
    monkeypatch.setattr(teardown_api, "_strategy_registry", registry)

    data = teardown_api._get_strategy_data("deployment:minimal")

    assert data["name"] == "deployment:minimal"
    assert data["chain"] == "unknown"
    assert data["protocol"] == "unknown"
    assert data["positions"] == []
    assert data["health_factor"] is None
