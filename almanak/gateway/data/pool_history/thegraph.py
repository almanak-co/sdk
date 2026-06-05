"""The Graph pool-history provider (POOL-5 / VIB-4753) — PRIMARY, all resolutions.

Queries Uniswap-V3-style subgraphs for ``poolHourDatas`` (1h / 4h) and
``poolDayDatas`` (1d), paginating ``first``/``skip`` at ``page_size=1000``,
and translates the rows into ``gateway_pb2.PoolSnapshot`` with Empty != Zero
decimal semantics.

Resolution handling:

* ``1h`` -> ``poolHourDatas`` ordered by ``periodStartUnix`` ascending.
* ``4h`` -> ``poolHourDatas`` (hourly) **down-sampled** to the 4h grid by
  keeping only rows where ``periodStartUnix % 14400 == 0`` (decision #9:
  TheGraph has no native 4h bucket, so 4h is derived from hourly — never
  silently relabel daily/other data as 4h).
* ``1d`` -> ``poolDayDatas`` ordered by ``date`` ascending.

Rate-limit + budget topology (owned here so all TheGraph quota accounting is
in one place — mirrors ``pool_analytics_service`` doing rate-limit inside the
provider method):

* A shared ``_TokenBucket`` (rate=2/s; spike R3: key quota is per-account, not
  per-subgraph). Empty bucket -> ``_ProviderError`` (decision #6: a throttled
  PRIMARY must fall through AND be observable — not a silent local skip).
* A ``_MonthlyBudgetTracker``. When tripped -> ``_NotAttempted`` (the chain
  falls through to DefiLlama / CoinGecko Onchain — D3.F11 trip).

No URL registered for ``(protocol, chain)`` -> ``_NotAttempted`` (this is how
Aerodrome — which registers no subgraph endpoints — falls through to
CoinGecko Onchain at sub-daily and 1d resolutions).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from decimal import Decimal

from almanak.gateway.proto import gateway_pb2

from ._base import (
    _NOT_ATTEMPTED,
    ProviderResult,
    _MonthlyBudgetTracker,
    _ProviderError,
    _safe_decimal_str,
    _TokenBucket,
    build_unmeasured_fields,
)
from ._graphql import (
    GatewayGraphQLClient,
    SubgraphClientError,
    SubgraphConnectionError,
    SubgraphQueryError,
    SubgraphRateLimitError,
)

logger = logging.getLogger(__name__)

#: TheGraph subgraph page size. A 90d-1h window (2160 rows) paginates as
#: 1000 + 1000 + 160 (3 pages) — the multi-page aggregation happy path
#: (D2.M3 ``test_thegraph_multipage_aggregation``).
_PAGE_SIZE = 1000

#: Defensive page ceiling so a pathological subgraph that keeps returning
#: full pages can't loop forever. 90d-1h = 3 pages; 730d-1d = 730 rows = 1
#: page; this ceiling comfortably covers the soft caps while bounding the
#: loop. Truncation at the soft cap is POOL-6's job, not this ceiling.
_MAX_PAGES = 100

#: 4h grid size in seconds — a row survives the 4h down-sample iff its
#: hourly ``periodStartUnix`` lands on a 4h boundary.
_FOUR_HOUR_SECONDS = 14400

_POOL_HOUR_QUERY = """
query PoolHourDatas($pool: String!, $start: Int!, $end: Int!, $first: Int!, $skip: Int!) {
  poolHourDatas(
    first: $first
    skip: $skip
    orderBy: periodStartUnix
    orderDirection: asc
    where: { pool: $pool, periodStartUnix_gte: $start, periodStartUnix_lt: $end }
  ) {
    periodStartUnix
    tvlUSD
    volumeUSD
    feesUSD
    token0Price
    token1Price
  }
}
""".strip()

_POOL_DAY_QUERY = """
query PoolDayDatas($pool: String!, $start: Int!, $end: Int!, $first: Int!, $skip: Int!) {
  poolDayDatas(
    first: $first
    skip: $skip
    orderBy: date
    orderDirection: asc
    where: { pool: $pool, date_gte: $start, date_lt: $end }
  ) {
    date
    tvlUSD
    volumeUSD
    feesUSD
  }
}
""".strip()


class TheGraphPoolHistoryProvider:
    """Primary pool-history provider backed by The Graph subgraphs."""

    name = "the_graph"

    def __init__(
        self,
        *,
        client: GatewayGraphQLClient,
        url_resolver: Callable[[str, str], str | None],
        rate_limiter: _TokenBucket,
        budget: _MonthlyBudgetTracker,
    ) -> None:
        self._client = client
        # ``url_resolver(protocol, chain) -> url | None`` reads the registry
        # ``GatewaySubgraphCapability`` (decision #1: registry-driven URLs,
        # NOT a hardcoded ``_SUBGRAPH_URLS`` dict).
        self._url_resolver = url_resolver
        self._rate_limiter = rate_limiter
        self._budget = budget

    async def fetch(
        self,
        *,
        chain: str,
        pool_address: str,
        protocol: str,
        start_ts: int,
        end_ts: int,
        resolution: int,
    ) -> ProviderResult:
        url = self._url_resolver(protocol, chain)
        if url is None:
            # No subgraph registered for this (protocol, chain) — local skip,
            # NOT an error. (Aerodrome falls through here.)
            logger.debug("TheGraph: no subgraph URL for (%s, %s); skipping", protocol, chain)
            return _NOT_ATTEMPTED

        if self._budget.is_tripped():
            logger.warning(
                "TheGraph monthly budget breaker tripped (%d/%d); falling through",
                self._budget.queries,
                self._budget.budget_max,
            )
            return _NOT_ATTEMPTED

        # Empty bucket: a throttled PRIMARY must fall through AND be
        # observable (decision #6) — raise so the dispatcher bumps errors +
        # fallback and records WHY, rather than a silent local skip.
        if not self._rate_limiter.acquire():
            raise _ProviderError("the_graph: rate-limit bucket empty")

        is_daily = resolution == gateway_pb2.Resolution.RESOLUTION_1D
        query = _POOL_DAY_QUERY if is_daily else _POOL_HOUR_QUERY
        time_field = "date" if is_daily else "periodStartUnix"

        try:
            rows = await self._paginate(
                url=url,
                query=query,
                pool_address=pool_address,
                start_ts=start_ts,
                end_ts=end_ts,
                data_path="poolDayDatas" if is_daily else "poolHourDatas",
            )
        except SubgraphRateLimitError as exc:
            raise _ProviderError(f"the_graph: rate limited ({exc})") from exc
        except (SubgraphQueryError, SubgraphConnectionError, SubgraphClientError) as exc:
            raise _ProviderError(f"the_graph: {exc}") from exc

        if not rows:
            # Reached the subgraph, queried, no rows for this window: genuine
            # not-found. NEVER return [] as success.
            return None

        snapshots = self._rows_to_snapshots(rows, time_field=time_field, resolution=resolution)
        if not snapshots:
            # Down-sampling (4h) can legitimately empty an otherwise-populated
            # hourly response if no hourly row lands on a 4h boundary — treat
            # as not-found so the chain falls through rather than returning [].
            return None
        return snapshots

    async def _paginate(
        self,
        *,
        url: str,
        query: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        data_path: str,
    ) -> list[dict]:
        """Fetch all pages for the window, concatenating in order.

        Each page increments the monthly budget counter (one query == one page
        == one billable unit). Pagination stops when a page returns fewer than
        ``_PAGE_SIZE`` rows (last page) or the ``_MAX_PAGES`` ceiling is hit.
        """
        all_rows: list[dict] = []
        for page in range(_MAX_PAGES):
            # Budget is enforced BEFORE every page (audit Important #4). The
            # top-level ``fetch`` check returns _NOT_ATTEMPTED before the first
            # page; if the monthly cap is reached MID-pagination, we cannot
            # complete the window — returning the partial pages would be silent
            # truncation, so raise and let the dispatcher fall through.
            if self._budget.is_tripped():
                raise _ProviderError(
                    f"the_graph: monthly budget exhausted mid-pagination "
                    f"({self._budget.queries}/{self._budget.budget_max})"
                )
            self._budget.record_query()
            variables = {
                "pool": pool_address,
                "start": int(start_ts),
                "end": int(end_ts),
                "first": _PAGE_SIZE,
                "skip": page * _PAGE_SIZE,
            }
            data = await self._client.query(url=url, query=query, variables=variables)
            page_rows = data.get(data_path, []) if isinstance(data, dict) else []
            if not isinstance(page_rows, list) or not page_rows:
                break
            all_rows.extend(page_rows)
            if len(page_rows) < _PAGE_SIZE:
                break
        return all_rows

    def _rows_to_snapshots(
        self,
        rows: list[dict],
        *,
        time_field: str,
        resolution: int,
    ) -> list[gateway_pb2.PoolSnapshot]:
        parsed: list[tuple[int, dict]] = []
        for row in rows:
            ts_raw = row.get(time_field)
            if ts_raw is None:
                logger.debug("TheGraph: dropping row with missing %s", time_field)
                continue
            try:
                timestamp = int(ts_raw)
            except (TypeError, ValueError):
                logger.debug("TheGraph: dropping row with unparseable %s=%r", time_field, ts_raw)
                continue
            parsed.append((timestamp, row))

        if resolution == gateway_pb2.Resolution.RESOLUTION_4H:
            # 4h has no native subgraph bucket: aggregate hourly rows into 4h
            # bars. Flows (volume/fees) are SUMMED across the constituent hours
            # (audit blocker #2 — keeping a single hour under-reports ~4x).
            return _aggregate_4h(parsed)
        return [_row_to_snapshot(row, ts) for ts, row in parsed]


def _row_to_snapshot(row: dict, timestamp: int) -> gateway_pb2.PoolSnapshot:
    """Translate one subgraph row into a ``PoolSnapshot`` (Empty != Zero).

    Reserves are unmeasured on the Uniswap-V3 hour/day schema (it carries
    prices, not raw reserves; the proto has no ``tick``/``price`` field, and
    POOL-1 forbids deriving reserves from price), so ``token*_reserve`` stay
    ``""`` and are listed in ``unmeasured_fields``. ``poolDayDatas`` carries
    no ``feesUSD`` in some schema versions -> unmeasured.
    """
    tvl = _safe_decimal_str(row.get("tvlUSD"))
    volume_24h = _safe_decimal_str(row.get("volumeUSD"))
    fee_revenue_24h = _safe_decimal_str(row.get("feesUSD"))
    token0_reserve = ""  # not on the V3 hour/day schema (price-only)
    token1_reserve = ""
    unmeasured = build_unmeasured_fields(
        tvl=tvl,
        volume_24h=volume_24h,
        fee_revenue_24h=fee_revenue_24h,
        token0_reserve=token0_reserve,
        token1_reserve=token1_reserve,
    )
    return gateway_pb2.PoolSnapshot(
        timestamp=timestamp,
        tvl=tvl,
        volume_24h=volume_24h,
        fee_revenue_24h=fee_revenue_24h,
        token0_reserve=token0_reserve,
        token1_reserve=token1_reserve,
        unmeasured_fields=unmeasured,
    )


def _sum_flow(rows: list[dict], key: str) -> str:
    """Sum a per-hour FLOW field (volumeUSD / feesUSD) across a 4h bucket.

    Empty != Zero: returns ``""`` iff NO constituent hour had a measured value;
    otherwise the decimal-string sum of the measured hours (a measured zero —
    ``"0"`` — contributes 0 and counts as measured). Unmeasured hours are
    skipped, never coerced to 0.
    """
    total: Decimal | None = None
    for row in rows:
        s = _safe_decimal_str(row.get(key))
        if s == "":
            continue
        total = (total if total is not None else Decimal(0)) + Decimal(s)
    return "" if total is None else str(total)


def _last_level(rows: list[dict], key: str) -> str:
    """Carry a LEVEL field (tvlUSD) as the bucket's last measured observation.

    TVL is a level, not a flow — the 4h bar's TVL is the close (most recent
    measured hour in the bucket). ``""`` if no hour measured it. ``rows`` are
    ascending by timestamp (subgraph orderBy asc + in-order pagination).
    """
    level = ""
    for row in rows:
        s = _safe_decimal_str(row.get(key))
        if s != "":
            level = s
    return level


def _aggregate_4h(parsed: list[tuple[int, dict]]) -> list[gateway_pb2.PoolSnapshot]:
    """Aggregate ascending hourly rows into 4h bars (audit blocker #2).

    Each 4h bar [b, b+14400) carries: ``timestamp = b`` (4h-aligned); SUMMED
    ``volume_24h`` / ``fee_revenue_24h`` over its hours; the close (last
    measured) ``tvl`` level; reserves unmeasured (V3 hour schema is price-only).
    """
    buckets: dict[int, list[dict]] = {}
    order: list[int] = []
    for ts, row in parsed:
        bucket = ts - (ts % _FOUR_HOUR_SECONDS)
        if bucket not in buckets:
            buckets[bucket] = []
            order.append(bucket)
        buckets[bucket].append(row)

    snapshots: list[gateway_pb2.PoolSnapshot] = []
    for bucket in order:
        bucket_rows = buckets[bucket]
        tvl = _last_level(bucket_rows, "tvlUSD")
        volume_24h = _sum_flow(bucket_rows, "volumeUSD")
        fee_revenue_24h = _sum_flow(bucket_rows, "feesUSD")
        token0_reserve = ""
        token1_reserve = ""
        unmeasured = build_unmeasured_fields(
            tvl=tvl,
            volume_24h=volume_24h,
            fee_revenue_24h=fee_revenue_24h,
            token0_reserve=token0_reserve,
            token1_reserve=token1_reserve,
        )
        snapshots.append(
            gateway_pb2.PoolSnapshot(
                timestamp=bucket,
                tvl=tvl,
                volume_24h=volume_24h,
                fee_revenue_24h=fee_revenue_24h,
                token0_reserve=token0_reserve,
                token1_reserve=token1_reserve,
                unmeasured_fields=unmeasured,
            )
        )
    return snapshots


__all__ = ["TheGraphPoolHistoryProvider"]
