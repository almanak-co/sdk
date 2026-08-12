"""Pure transport and integrity tests for GMX Synthetics funding history."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors.gmx_v2.gateway.funding_history import (
    fetch_gmx_funding_history,
    fetch_latest_gmx_funding_snapshot,
)
from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

_MARKET = "0x7c54D547FAD72f8AFbf6E5b04403A0168b654C6f"
_START = 1_750_449_600


class _Response:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def json(self) -> dict[str, Any]:
        return self.payload

    async def text(self) -> str:
        return str(self.payload)


class _Session:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.market_addresses: list[str] = []

    def post(self, _url: str, *, json: dict[str, Any], headers: dict[str, str]) -> _Response:
        del headers
        variables = json["variables"]
        self.market_addresses.append(variables["marketAddress"])
        if "start" in variables:
            rows = [row for row in self.rows if variables["start"] <= row["snapshotTimestamp"] <= variables["end"]]
        elif "end" in variables:
            rows = sorted(
                (row for row in self.rows if row["snapshotTimestamp"] <= variables["end"]),
                key=lambda row: row["snapshotTimestamp"],
                reverse=True,
            )[:1]
        else:
            rows = self.rows[:1]
        return _Response({"data": {"fundingRateSnapshots": rows}})


def _row(timestamp: int, *, address: str = _MARKET) -> dict[str, Any]:
    return {
        "marketAddress": address,
        "snapshotTimestamp": timestamp,
        "fundingFactorPerSecondLong": "-25903000627149213888888",
        "fundingFactorPerSecondShort": "40512692907534675501066",
    }


@pytest.mark.asyncio
async def test_complete_hourly_grid_preserves_asymmetric_sides() -> None:
    rows = [_row(_START + offset * 3600) for offset in range(3)]
    session = _Session(rows)

    points = await fetch_gmx_funding_history(
        session,
        chain="arbitrum",
        market_address=_MARKET.lower(),
        start_ts=_START,
        end_ts=_START + 7200,
    )

    assert [point.timestamp for point in points] == [_START, _START + 3600, _START + 7200]
    assert session.market_addresses == [_MARKET]
    assert all(point.long_rate_hourly < 0 < point.short_rate_hourly for point in points)
    assert all(point.rate_hourly == -point.long_rate_hourly for point in points)


@pytest.mark.asyncio
async def test_missing_hour_fails_closed() -> None:
    rows = [_row(_START), _row(_START + 7200)]

    with pytest.raises(RateHistoryUnavailable, match=f"first_missing={_START + 3600}"):
        await fetch_gmx_funding_history(
            _Session(rows),
            chain="arbitrum",
            market_address=_MARKET,
            start_ts=_START,
            end_ts=_START + 7200,
        )


@pytest.mark.asyncio
async def test_response_market_identity_mismatch_fails_closed() -> None:
    with pytest.raises(RateHistoryUnavailable, match="did not preserve"):
        await fetch_gmx_funding_history(
            _Session([_row(_START, address="0x0000000000000000000000000000000000000001")]),
            chain="arbitrum",
            market_address=_MARKET,
            start_ts=_START,
            end_ts=_START,
        )


@pytest.mark.asyncio
async def test_prelisting_window_reports_first_available_snapshot() -> None:
    first = _START + 7200

    with pytest.raises(RateHistoryUnavailable, match=f"first available snapshot is {first}"):
        await fetch_gmx_funding_history(
            _Session([_row(first)]),
            chain="arbitrum",
            market_address=_MARKET,
            start_ts=_START,
            end_ts=_START + 3600,
        )


@pytest.mark.asyncio
async def test_latest_snapshot_tolerates_indexer_lag_without_weakening_history_grid() -> None:
    latest_available = _START - 7200
    session = _Session([_row(latest_available)])

    point = await fetch_latest_gmx_funding_snapshot(
        session,
        chain="arbitrum",
        market_address=_MARKET.lower(),
        end_ts=_START,
    )

    assert point.timestamp == latest_available
    assert session.market_addresses == [_MARKET]
