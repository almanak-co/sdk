"""GMX-native hourly funding history from the official Synthetics indexer.

The source is keyed by the exact, on-chain-verified GMX market-token address.
There is deliberately no symbol table and no cross-venue fallback: market
catalogue discovery belongs to :mod:`market_registry`, while this module only
transports and validates one immutable hourly series.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from eth_utils import to_checksum_address

if TYPE_CHECKING:
    from almanak.gateway.services.rate_history_service import FundingRatePoint

GMX_FUNDING_INDEXER_URLS: Mapping[str, str] = {
    "arbitrum": "https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql",
    "avalanche": "https://gmx.squids.live/gmx-synthetics-avalanche:prod/api/graphql",
}
GMX_FUNDING_SOURCE = "gmx_synthetics_subsquid"

_PAGE_SIZE = 1000
_SECONDS_PER_HOUR = 3600
_FUNDING_FACTOR_PRECISION = Decimal(10) ** 30

_HISTORY_QUERY = """
query FundingRateSnapshots(
  $marketAddress: String!
  $start: Int!
  $end: Int!
  $offset: Int!
  $limit: Int!
) {
  fundingRateSnapshots(
    where: {
      marketAddress_eq: $marketAddress
      snapshotTimestamp_gte: $start
      snapshotTimestamp_lte: $end
    }
    orderBy: snapshotTimestamp_ASC
    offset: $offset
    limit: $limit
  ) {
    marketAddress
    snapshotTimestamp
    fundingFactorPerSecondLong
    fundingFactorPerSecondShort
  }
}
"""

_EARLIEST_QUERY = """
query EarliestFundingRateSnapshot($marketAddress: String!) {
  fundingRateSnapshots(
    where: {marketAddress_eq: $marketAddress}
    orderBy: snapshotTimestamp_ASC
    limit: 1
  ) {
    snapshotTimestamp
  }
}
"""

_LATEST_QUERY = """
query LatestFundingRateSnapshot($marketAddress: String!, $end: Int!) {
  fundingRateSnapshots(
    where: {
      marketAddress_eq: $marketAddress
      snapshotTimestamp_lte: $end
    }
    orderBy: snapshotTimestamp_DESC
    limit: 1
  ) {
    marketAddress
    snapshotTimestamp
    fundingFactorPerSecondLong
    fundingFactorPerSecondShort
  }
}
"""


def _indexer_market_address(market_address: str, *, source: str) -> str:
    """Return the EIP-55 spelling required by GMX's case-sensitive indexer."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    try:
        return to_checksum_address(market_address)
    except ValueError as exc:
        raise RateHistoryUnavailable(source, f"Invalid GMX market address: {market_address!r}") from exc


def _hourly_rate(raw_factor: Any, *, field: str) -> Decimal:
    if isinstance(raw_factor, bool) or not isinstance(raw_factor, str | int):
        raise ValueError(f"GMX funding snapshot {field} is not an integer string")
    try:
        factor = Decimal(str(raw_factor))
    except InvalidOperation as exc:
        raise ValueError(f"GMX funding snapshot {field} is not an integer string") from exc
    if factor != factor.to_integral_value():
        raise ValueError(f"GMX funding snapshot {field} is not an integer")
    return factor * Decimal(_SECONDS_PER_HOUR) / _FUNDING_FACTOR_PRECISION


def legacy_scalar_rate(long_rate: Decimal, short_rate: Decimal) -> Decimal:
    """Project asymmetric position-payment rates onto the legacy scalar sign.

    The legacy convention is positive when longs pay and negative when shorts
    pay. The scalar carries the paying side's magnitude; side-aware accounting
    consumes the two signed rates directly.
    """
    if long_rate <= 0 <= short_rate:
        return -long_rate
    if short_rate <= 0 <= long_rate:
        return short_rate
    raise ValueError(
        "GMX funding snapshot has invalid side signs: exactly one side must pay unless both rates are zero"
    )


async def _response_json(response: Any, *, source: str) -> Any:
    from almanak.gateway.services.rate_history_service import RateHistoryRateLimited, RateHistoryUnavailable

    if response.status == 429:
        headers = getattr(response, "headers", {})
        raw_retry = headers.get("Retry-After") if hasattr(headers, "get") else None
        retry_after: float | None = None
        if raw_retry is not None:
            try:
                retry_after = max(0.0, float(str(raw_retry)))
            except ValueError:
                retry_after = None
        raise RateHistoryRateLimited(source, "GMX funding indexer returned HTTP 429", retry_after=retry_after)
    if response.status != 200:
        body = await response.text()
        raise RateHistoryUnavailable(source, f"GMX funding indexer returned HTTP {response.status}: {body[:200]}")
    try:
        return await response.json()
    except Exception as exc:
        raise RateHistoryUnavailable(source, f"GMX funding indexer returned malformed JSON: {exc}") from exc


async def _graphql(
    session: Any, *, url: str, query: str, variables: Mapping[str, Any], source: str
) -> Mapping[str, Any]:
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    try:
        async with session.post(
            url,
            json={"query": query, "variables": dict(variables)},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        ) as response:
            payload = await _response_json(response, source=source)
    except RateHistoryUnavailable:
        raise
    except Exception as exc:
        raise RateHistoryUnavailable(source, f"GMX funding indexer request failed: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise RateHistoryUnavailable(source, "GMX funding indexer response is not an object")
    errors = payload.get("errors")
    if errors:
        raise RateHistoryUnavailable(source, f"GMX funding indexer GraphQL error: {str(errors)[:300]}")
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise RateHistoryUnavailable(source, "GMX funding indexer response has no data object")
    return data


async def _earliest_timestamp(session: Any, *, url: str, market_address: str, source: str) -> int | None:
    data = await _graphql(
        session,
        url=url,
        query=_EARLIEST_QUERY,
        variables={"marketAddress": market_address},
        source=source,
    )
    rows = data.get("fundingRateSnapshots")
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], Mapping):
        return None
    timestamp = rows[0].get("snapshotTimestamp")
    return timestamp if isinstance(timestamp, int) and not isinstance(timestamp, bool) else None


async def fetch_gmx_funding_history(
    session: Any,
    *,
    chain: str,
    market_address: str,
    start_ts: int,
    end_ts: int,
) -> list[Any]:
    """Fetch and validate a complete exact-address hourly GMX funding grid."""
    from almanak.gateway.services.rate_history_service import FundingRatePoint, RateHistoryUnavailable

    url = GMX_FUNDING_INDEXER_URLS.get(chain)
    source = GMX_FUNDING_SOURCE
    if url is None:
        raise RateHistoryUnavailable(source, f"GMX funding history is not configured for chain {chain!r}")
    indexer_address = _indexer_market_address(market_address, source=source)

    rows: list[Any] = []
    offset = 0
    while True:
        data = await _graphql(
            session,
            url=url,
            query=_HISTORY_QUERY,
            variables={
                "marketAddress": indexer_address,
                "start": start_ts,
                "end": end_ts,
                "offset": offset,
                "limit": _PAGE_SIZE,
            },
            source=source,
        )
        page = data.get("fundingRateSnapshots")
        if not isinstance(page, list):
            raise RateHistoryUnavailable(source, "GMX funding indexer data has no fundingRateSnapshots list")
        rows.extend(page)
        if len(page) < _PAGE_SIZE:
            break
        offset += len(page)

    if not rows:
        earliest = await _earliest_timestamp(
            session,
            url=url,
            market_address=indexer_address,
            source=source,
        )
        suffix = f"; first available snapshot is {earliest}" if earliest is not None else ""
        raise RateHistoryUnavailable(
            source,
            f"No GMX funding history for market {market_address} in [{start_ts}, {end_ts}]{suffix}",
        )

    points: list[Any] = []
    seen: set[int] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise RateHistoryUnavailable(source, "GMX funding indexer returned a malformed snapshot row")
        row_address = row.get("marketAddress")
        if not isinstance(row_address, str) or row_address.lower() != market_address.lower():
            raise RateHistoryUnavailable(source, "GMX funding indexer did not preserve the requested market address")
        timestamp = row.get("snapshotTimestamp")
        if not isinstance(timestamp, int) or isinstance(timestamp, bool) or not start_ts <= timestamp <= end_ts:
            raise RateHistoryUnavailable(source, "GMX funding indexer returned an out-of-window timestamp")
        if timestamp in seen:
            raise RateHistoryUnavailable(source, f"GMX funding indexer returned duplicate timestamp {timestamp}")
        seen.add(timestamp)
        try:
            long_rate = _hourly_rate(row.get("fundingFactorPerSecondLong"), field="long factor")
            short_rate = _hourly_rate(row.get("fundingFactorPerSecondShort"), field="short factor")
            scalar = legacy_scalar_rate(long_rate, short_rate)
        except ValueError as exc:
            raise RateHistoryUnavailable(source, str(exc)) from exc
        points.append(
            FundingRatePoint(
                timestamp=timestamp,
                rate_hourly=scalar,
                rate_annualized=scalar * Decimal(8760),
                long_rate_hourly=long_rate,
                short_rate_hourly=short_rate,
            )
        )

    points.sort(key=lambda point: point.timestamp)
    expected_start = ((start_ts + _SECONDS_PER_HOUR - 1) // _SECONDS_PER_HOUR) * _SECONDS_PER_HOUR
    expected_end = (end_ts // _SECONDS_PER_HOUR) * _SECONDS_PER_HOUR
    expected = (
        list(range(expected_start, expected_end + 1, _SECONDS_PER_HOUR)) if expected_start <= expected_end else []
    )
    actual = [point.timestamp for point in points]
    if actual != expected:
        actual_set = set(actual)
        first_missing = next((timestamp for timestamp in expected if timestamp not in actual_set), None)
        first_available = actual[0] if actual else None
        raise RateHistoryUnavailable(
            source,
            "Incomplete GMX hourly funding coverage for "
            f"{market_address}: first_missing={first_missing}, first_available={first_available}, "
            f"requested=[{start_ts}, {end_ts}]",
        )
    return points


async def fetch_latest_gmx_funding_snapshot(
    session: Any,
    *,
    chain: str,
    market_address: str,
    end_ts: int,
) -> FundingRatePoint:
    """Fetch the latest exact-address snapshot at or before ``end_ts``.

    This current-rate lane intentionally does not assert an hourly grid: the
    official indexer can lag the wall clock. Historical range requests remain
    strict in :func:`fetch_gmx_funding_history`.
    """
    from almanak.gateway.services.rate_history_service import FundingRatePoint, RateHistoryUnavailable

    url = GMX_FUNDING_INDEXER_URLS.get(chain)
    if url is None:
        raise RateHistoryUnavailable(
            GMX_FUNDING_SOURCE,
            f"GMX funding history is not configured for chain {chain!r}",
        )
    indexer_address = _indexer_market_address(market_address, source=GMX_FUNDING_SOURCE)
    data = await _graphql(
        session,
        url=url,
        query=_LATEST_QUERY,
        variables={"marketAddress": indexer_address, "end": end_ts},
        source=GMX_FUNDING_SOURCE,
    )
    rows = data.get("fundingRateSnapshots")
    if not isinstance(rows, list):
        raise RateHistoryUnavailable(GMX_FUNDING_SOURCE, "GMX funding indexer data has no fundingRateSnapshots list")
    if not rows:
        raise RateHistoryUnavailable(
            GMX_FUNDING_SOURCE,
            f"No GMX funding snapshot for market {market_address} at or before {end_ts}",
        )
    row = rows[0]
    if not isinstance(row, Mapping):
        raise RateHistoryUnavailable(GMX_FUNDING_SOURCE, "GMX funding indexer returned a malformed snapshot row")
    row_address = row.get("marketAddress")
    if not isinstance(row_address, str) or row_address.lower() != market_address.lower():
        raise RateHistoryUnavailable(
            GMX_FUNDING_SOURCE,
            "GMX funding indexer did not preserve the requested market address",
        )
    timestamp = row.get("snapshotTimestamp")
    if not isinstance(timestamp, int) or isinstance(timestamp, bool) or timestamp > end_ts:
        raise RateHistoryUnavailable(GMX_FUNDING_SOURCE, "GMX funding indexer returned a future timestamp")
    try:
        long_rate = _hourly_rate(row.get("fundingFactorPerSecondLong"), field="long factor")
        short_rate = _hourly_rate(row.get("fundingFactorPerSecondShort"), field="short factor")
        scalar = legacy_scalar_rate(long_rate, short_rate)
    except ValueError as exc:
        raise RateHistoryUnavailable(GMX_FUNDING_SOURCE, str(exc)) from exc
    return FundingRatePoint(
        timestamp=timestamp,
        rate_hourly=scalar,
        rate_annualized=scalar * Decimal(8760),
        long_rate_hourly=long_rate,
        short_rate_hourly=short_rate,
    )


__all__ = [
    "GMX_FUNDING_INDEXER_URLS",
    "GMX_FUNDING_SOURCE",
    "fetch_gmx_funding_history",
    "fetch_latest_gmx_funding_snapshot",
    "legacy_scalar_rate",
]
