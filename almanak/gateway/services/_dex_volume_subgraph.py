"""Shared gateway-side DEX trading-volume subgraph fetch (VIB-4870 / W7).

The pre-W7 strategy-side providers under
``almanak/framework/backtesting/pnl/providers/dex/*`` each opened their
own ``aiohttp`` session and queried their protocol's TheGraph subgraph
for daily trading volume — eight near-identical files that all violated
the gateway-boundary rule (``AGENTS.md`` §"Gateway boundary").

This module is the single gateway-side egress path that the per-DEX
``GatewayDexVolumeCapability.fetch_volume_history`` bodies delegate to.
The DEX-specific knowledge (subgraph deployment ID per chain, the
``*DayDatas`` entity name, the daily-volume field name) is captured in a
:class:`DexVolumeSubgraphSpec`; everything else — the HTTP session, the
TheGraph gateway URL construction, the API-key auth header, the GraphQL
error handling, the timestamp-windowing — is shared.

"No silent zeros" (matches ``RateHistoryService`` / ``PoolHistoryService``):
the pre-W7 providers returned ``Decimal("0")`` LOW-confidence fallback
rows when the subgraph was empty / rate-limited / errored. That is
exactly the "unmeasured masquerading as measured-zero" pattern the W7
capability contract forbids. Here, every "no data" path raises
:class:`RateHistoryUnavailable`, which the dispatcher translates to a
``success=False`` envelope (and the framework reader maps to
:class:`DataSourceUnavailable`).

Strategy-side code MUST NOT import this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.gateway.services.rate_history_service import (
        DexVolumePoint,
        RateHistoryServiceServicer,
    )

# TheGraph decentralised-network gateway. The deployment ID is appended;
# the API key is sent as a Bearer header (matches the gateway's own
# ``TheGraphIntegration`` and the pre-W7 ``SubgraphClient``).
_THEGRAPH_GATEWAY_URL = "https://gateway.thegraph.com/api/subgraphs/id"

# Page size for the ``*DayDatas`` query. The pre-W7 providers used 1000;
# daily granularity means 1000 rows covers ~2.7 years in a single page,
# which exceeds any realistic backtest window, so no pagination is
# needed (the legacy code did not paginate either).
_PAGE_FIRST = 1000


# Seconds in one UTC day — used to convert a Messari ``day`` (days since
# the Unix epoch) to/from unix seconds.
_SECONDS_PER_DAY = 86400


@dataclass(frozen=True)
class DexVolumeSubgraphSpec:
    """DEX-specific knowledge for a daily-volume subgraph query.

    * ``dex_name`` — routing identifier (matches
      ``GatewayDexVolumeCapability.dex_name`` / the request ``dex``).
    * ``subgraph_ids`` — chain (lowercase) → TheGraph deployment ID. The
      keys define the connector's ``volume_supported_chains``.
    * ``entity`` — the GraphQL collection name (``"poolDayDatas"`` for
      V3-family, ``"pairDayDatas"`` for Solidly, ``"lbPairDayDatas"`` for
      TraderJoe LB, ``"liquidityPoolDailySnapshots"`` for Curve/Messari,
      ``"poolSnapshots"`` for Balancer V2).
    * ``id_field`` — the ``where`` filter key for the pool/pair address
      (``"pool"`` for V3 / Curve / Balancer, ``"pairAddress"`` for
      Aerodrome, ``"lbPair"`` for TraderJoe).
    * ``volume_field`` — the daily-volume field (``"volumeUSD"`` for V3 /
      TraderJoe, ``"dailyVolumeUSD"`` for Solidly / Curve, ``"swapVolume"``
      for Balancer).
    * ``time_field`` — the timestamp/day field used for the ``where``
      range filter, the ``orderBy`` key, and the returned point timestamp
      (``"date"``, ``"day"``, or ``"timestamp"``).
    * ``time_unit`` — ``"seconds"`` (``date`` / ``timestamp`` fields carry
      unix seconds) or ``"days"`` (Messari ``day`` carries days since the
      Unix epoch). Controls how the request window (always unix seconds)
      is converted into the filter values and how the returned field is
      decoded back to a unix-seconds ``DexVolumePoint.timestamp``.
    * ``source`` — the ``DataSourceInfo.source`` string the pre-W7
      provider stamped (preserved for byte-equivalence of the migrated
      lane's provenance).
    """

    dex_name: str
    subgraph_ids: dict[str, str]
    entity: str
    id_field: str
    volume_field: str
    source: str
    time_field: str = "date"
    time_unit: str = "seconds"

    def __post_init__(self) -> None:
        if self.time_unit not in ("seconds", "days"):
            raise ValueError(f"time_unit must be 'seconds' or 'days', got {self.time_unit!r}")

    def supported_chains(self) -> frozenset[str]:
        return frozenset(self.subgraph_ids)

    def to_filter_value(self, unix_seconds: int) -> int:
        """Convert a unix-seconds window bound to the subgraph's time unit."""
        if self.time_unit == "days":
            return unix_seconds // _SECONDS_PER_DAY
        return unix_seconds

    def to_unix_seconds(self, field_value: int) -> int:
        """Convert a returned ``time_field`` value back to unix seconds."""
        if self.time_unit == "days":
            return field_value * _SECONDS_PER_DAY
        return field_value


def _build_query(spec: DexVolumeSubgraphSpec) -> str:
    """Build the daily-volume GraphQL query for ``spec``.

    Mirrors the pre-W7 per-provider query shape: a ``first: 1000``
    window filtered by ``<id_field>`` + ``<time_field>`` range, ascending
    by ``<time_field>``. Only ``<time_field>`` + the volume field are
    selected (the pre-W7 providers selected extra fields like ``tvlUSD``
    they never consumed for the volume lane).
    """
    return f"""
query GetDexDayVolume($poolAddress: String!, $startTime: Int!, $endTime: Int!) {{
    {spec.entity}(
        first: {_PAGE_FIRST}
        where: {{
            {spec.id_field}: $poolAddress
            {spec.time_field}_gte: $startTime
            {spec.time_field}_lte: $endTime
        }}
        orderBy: {spec.time_field}
        orderDirection: asc
    ) {{
        {spec.time_field}
        {spec.volume_field}
    }}
}}
"""


def _parse_rows(
    rows: list[dict[str, Any]],
    spec: DexVolumeSubgraphSpec,
) -> list[DexVolumePoint]:
    """Decode daily-volume rows into ascending ``DexVolumePoint`` list.

    The pre-W7 providers parsed the volume field via ``Decimal(str(value))``
    with a ``"0"`` default; we keep the same ``Decimal`` decode but treat
    a missing / unparseable field as a malformed row and raise (the row
    came back from the subgraph but without the expected field, which is
    an upstream-schema problem, not a measured zero). The ``time_field``
    is decoded to unix seconds via ``spec.to_unix_seconds`` so Curve's
    Messari ``day`` (days since epoch) lands on the same scale as the
    other DEXes.
    """
    from almanak.gateway.services.rate_history_service import DexVolumePoint

    points: list[DexVolumePoint] = []
    for row in rows:
        # A missing / null time field must NOT default to the Unix epoch:
        # that would emit a bogus ``timestamp=0`` point while the helper
        # reports success. Treat it as a malformed row and raise, same as
        # a missing volume field. (CodeRabbit / Gemini PR #2493.)
        raw_ts = row.get(spec.time_field)
        if raw_ts is None:
            raise ValueError(f"row missing {spec.time_field!r} for {spec.dex_name}")
        try:
            ts = spec.to_unix_seconds(int(raw_ts))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unparseable {spec.time_field!r}={raw_ts!r} for {spec.dex_name}") from exc
        raw = row.get(spec.volume_field)
        if raw is None:
            raise ValueError(f"row missing {spec.volume_field!r} for {spec.dex_name} (ts={ts})")
        try:
            volume = Decimal(str(raw))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"unparseable {spec.volume_field!r}={raw!r} for {spec.dex_name} (ts={ts})") from exc
        points.append(DexVolumePoint(timestamp=ts, volume_usd=volume))
    return points


def _resolve_subgraph_id(spec: DexVolumeSubgraphSpec, chain: str) -> str:
    """Return the deployment ID for ``chain`` or raise ``RateHistoryUnavailable``."""
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    subgraph_id = spec.subgraph_ids.get(chain)
    if subgraph_id is None:
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"no subgraph configured for chain {chain!r} (supports: {sorted(spec.subgraph_ids)})",
        )
    return subgraph_id


def _parse_retry_after(raw: str | None) -> float | None:
    """Parse a ``Retry-After`` header value to seconds, or ``None``.

    Per RFC 7231 the header may be a number of seconds OR an HTTP-date.
    Only the numeric form is mapped to a ``retry_after`` hint; an
    HTTP-date (or any unparseable value) returns ``None`` rather than
    crashing the request. (Gemini PR #2493.)
    """
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


async def fetch_dex_volume_history(
    servicer: RateHistoryServiceServicer,
    spec: DexVolumeSubgraphSpec,
    *,
    chain: str,
    pool_address: str,
    start_ts: int,
    end_ts: int,
    interval_secs: int,
) -> list[DexVolumePoint]:
    """Fetch daily DEX trading volume from ``spec``'s subgraph.

    Shared body for every ``GatewayDexVolumeCapability.fetch_volume_history``.
    Egress runs on the servicer's shared aiohttp session + the gateway's
    ``thegraph_api_key`` setting — no per-connector session, no
    strategy-side egress.

    **Daily granularity only.** The underlying subgraphs serve *daily*
    rows (``*DayDatas`` / Messari day snapshots), which is the resolution
    the pre-W7 providers returned and the resolution the migrated lane's
    consumers expect. ``interval_secs`` MUST therefore be ``86400`` (one
    UTC day); any other value is rejected with ``RateHistoryUnavailable``
    rather than silently returning daily rows mislabelled as a different
    cadence (``DexVolumePoint.timestamp`` is documented as aligned to the
    requested interval). Resampling to coarser/finer buckets is not in
    scope for the migrated lane. (Codex PR #2493.)

    The single ``first: 1000`` page covers ~2.7 years of daily rows; a
    requested window wider than that would return a partial series that
    still looks successful. Such windows are rejected fail-fast rather
    than silently truncated. (CodeRabbit PR #2493.)

    Raises :class:`RateHistoryUnavailable` for every "no data" path
    (chain not configured, interval mismatch, window too wide, rate-limit,
    GraphQL error, empty window, malformed row) — never a silent-zero
    fallback.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    if interval_secs != _SECONDS_PER_DAY:
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"only daily volume (interval_secs={_SECONDS_PER_DAY}) is served; got {interval_secs}",
        )

    requested_days = (end_ts // _SECONDS_PER_DAY) - (start_ts // _SECONDS_PER_DAY) + 1
    if requested_days > _PAGE_FIRST:
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"requested window spans {requested_days} daily points, exceeds single-page limit {_PAGE_FIRST}",
        )

    subgraph_id = _resolve_subgraph_id(spec, chain)
    api_key = servicer.settings.thegraph_api_key
    if not api_key:
        raise RateHistoryUnavailable(
            spec.dex_name,
            "gateway thegraph_api_key is not configured",
        )

    url = f"{_THEGRAPH_GATEWAY_URL}/{subgraph_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "query": _build_query(spec),
        "variables": {
            "poolAddress": pool_address.lower(),
            "startTime": spec.to_filter_value(start_ts),
            "endTime": spec.to_filter_value(end_ts),
        },
    }

    session = await servicer._get_http_session()
    try:
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status == 429:
                raise RateHistoryUnavailable(
                    spec.dex_name,
                    "subgraph rate limit exceeded",
                    retry_after=_parse_retry_after(response.headers.get("Retry-After")),
                )
            if response.status != 200:
                body = (await response.text())[:500]
                raise RateHistoryUnavailable(
                    spec.dex_name,
                    f"subgraph HTTP {response.status}: {body}",
                )
            data = await response.json()
    except RateHistoryUnavailable:
        raise
    except Exception as exc:  # aiohttp.ClientError, JSON decode, …
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"subgraph request failed: {exc}",
        ) from exc

    # A well-formed GraphQL response is a JSON object; an array / scalar
    # body under odd error conditions would make ``.get`` raise
    # ``AttributeError``. Guard so it surfaces as a clean unavailable.
    # (Gemini PR #2493.)
    if not isinstance(data, dict):
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"subgraph returned non-object JSON: {type(data).__name__}",
        )

    errors = data.get("errors")
    if errors:
        messages = "; ".join(e.get("message", str(e)) for e in errors)
        raise RateHistoryUnavailable(spec.dex_name, f"subgraph GraphQL errors: {messages}")

    rows = (data.get("data") or {}).get(spec.entity) or []
    if not rows:
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"subgraph returned no {spec.entity} for pool {pool_address!r} on {chain} ({start_ts}..{end_ts})",
        )

    try:
        return _parse_rows(rows, spec)
    except ValueError as exc:
        raise RateHistoryUnavailable(spec.dex_name, str(exc)) from exc


__all__ = ["DexVolumeSubgraphSpec", "fetch_dex_volume_history"]
