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
    * ``resolve_bare_address_pool_id`` — when ``True``, a bare 42-char
      ``0x`` pool *address* is first resolved to the subgraph's full
      pool ID via a ``pools(where: {address: $address}) { id }`` lookup
      before the volume query runs (VIB-5090). Needed for Balancer V2,
      whose ``poolSnapshots`` are keyed by the 32-byte pool ID (address
      + pool-type/index suffix) — a bare address matches no rows.
      Identifiers that are not a bare 42-char address (e.g. a full
      66-char pool ID) pass through unchanged.
    """

    dex_name: str
    subgraph_ids: dict[str, str]
    entity: str
    id_field: str
    volume_field: str
    source: str
    time_field: str = "date"
    time_unit: str = "seconds"
    resolve_bare_address_pool_id: bool = False
    #: When True, ``volume_field`` carries the pool's CUMULATIVE lifetime
    #: volume (Balancer ``poolSnapshots.swapVolume``), not a per-day value:
    #: daily volume is the difference of consecutive snapshots. Left as a
    #: raw daily read, a mature pool reports its lifetime total (billions)
    #: as one day's volume — inflating LP fee backtests by orders of
    #: magnitude.
    cumulative_volume: bool = False

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


# Snapshots are event-driven on some venues (idle days produce no row), so a
# one-day lookback can miss the baseline row entirely. Look back a week to
# find the latest pre-window cumulative reading.
_CUMULATIVE_BASELINE_LOOKBACK_SECONDS = 7 * 24 * 3600


def _difference_cumulative_points(points: list[Any], start_ts: int) -> list[Any]:
    """Convert ascending cumulative-volume points to per-day volumes.

    Each day's volume = its cumulative reading minus the previous
    snapshot's (clamped at 0 for corrections/restatements). A point with no
    earlier baseline is DROPPED, never emitted raw: snapshots are
    event-driven, so a missing lookback row usually means an idle pool, and
    a raw cumulative reading would report the pool's lifetime total as one
    day's volume. The genuinely-new-pool case loses its first partial day,
    which is negligible against that failure mode.
    """
    from almanak.gateway.services.rate_history_service import DexVolumePoint

    daily: list[Any] = []
    previous = None
    for point in points:
        if previous is not None:
            value = point.volume_usd - previous.volume_usd
            if value < 0:
                value = Decimal("0")
            if point.timestamp >= start_ts:
                daily.append(DexVolumePoint(timestamp=point.timestamp, volume_usd=value))
        previous = point
    return daily


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


# Length of a bare EVM address: ``0x`` + 20 bytes hex. A Balancer V2
# full pool ID is ``0x`` + 32 bytes hex (66 chars) and passes through
# the bare-address check unchanged.
_BARE_ADDRESS_LEN = 42

# Pool-ID lookup for ``resolve_bare_address_pool_id`` specs (VIB-5090).
# ``first: 2`` so an (impossible-for-Balancer-V2, but guarded) ambiguous
# address — two pools sharing one address — is detectable without
# paging. Matches the Balancer V2 subgraph schema: ``pools`` keyed by
# full pool ID, filterable by the pool contract ``address``.
_POOL_ID_LOOKUP_QUERY = """
query ResolvePoolId($address: String!) {
    pools(first: 2, where: { address: $address }) {
        id
    }
}
"""


def _is_bare_pool_address(identifier: str) -> bool:
    """True when ``identifier`` is a bare 42-char ``0x`` hex address.

    Per-character check rather than ``int(..., 16)``: ``int`` tolerates
    signs and whitespace ("+1", " 1"), which are not valid address bytes.
    """
    if len(identifier) != _BARE_ADDRESS_LEN or not identifier.lower().startswith("0x"):
        return False
    return all(c in "0123456789abcdefABCDEF" for c in identifier[2:])


async def _resolve_pool_id_from_address(
    servicer: RateHistoryServiceServicer,
    spec: DexVolumeSubgraphSpec,
    *,
    chain: str,
    url: str,
    headers: dict[str, str],
    pool_address: str,
) -> str:
    """Resolve a bare pool address to the subgraph's full pool ID (VIB-5090).

    The mapping is immutable (a Balancer V2 pool ID is the pool address
    plus a fixed pool-type/index suffix assigned at registration), so
    hits are served from the servicer's in-process
    ``_dex_pool_id_cache`` — a plain unbounded dict, same idiom as the
    servicer's ``_web3_cache`` for other immutable per-process
    resources. No lock: a concurrent double-miss merely duplicates one
    idempotent lookup and last-write-wins with an identical value.

    Raises :class:`RateHistoryUnavailable` (→ ``DataSourceUnavailable``
    framework-side) when the address matches no pool or — guarded even
    though Balancer V2 makes it impossible — more than one pool.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    address = pool_address.lower()
    cache_key = (spec.dex_name, chain, address)
    cached = servicer._dex_pool_id_cache.get(cache_key)
    if cached is not None:
        return cached

    payload = {
        "query": _POOL_ID_LOOKUP_QUERY,
        "variables": {"address": address},
    }
    data = await _post_subgraph_query(servicer, spec, url=url, headers=headers, payload=payload)

    rows = (data.get("data") or {}).get("pools") or []
    if not rows:
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"no {spec.dex_name} pool found for address {pool_address!r} on {chain}; "
            "if the pool exists, pass its full pool ID instead of the bare address",
        )
    if len(rows) > 1:
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"ambiguous pool address {pool_address!r} on {chain}: multiple pools share it; "
            "pass the full pool ID instead of the bare address",
        )
    row = rows[0]
    if not isinstance(row, dict):
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"pool-ID lookup for address {pool_address!r} on {chain} returned a malformed row: {row!r}",
        )
    pool_id = row.get("id")
    if not isinstance(pool_id, str) or not pool_id:
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"pool-ID lookup for address {pool_address!r} on {chain} returned a malformed id: {pool_id!r}",
        )

    pool_id = pool_id.lower()
    servicer._dex_pool_id_cache[cache_key] = pool_id
    return pool_id


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


async def _post_subgraph_query(
    servicer: RateHistoryServiceServicer,
    spec: DexVolumeSubgraphSpec,
    *,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
) -> dict[str, Any]:
    """POST one GraphQL ``payload`` and return the decoded JSON object.

    Shared transport + error handling for the daily-volume query and the
    VIB-5090 pool-ID lookup: rate-limit (429 → ``retry_after`` hint),
    non-200, client/JSON failures, non-object bodies, and GraphQL
    ``errors`` all raise :class:`RateHistoryUnavailable` — never a
    silent fallback. Behaviour is byte-identical to the pre-VIB-5090
    inline block in :func:`fetch_dex_volume_history`.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

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
    if errors is not None and not isinstance(errors, list):
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"subgraph returned malformed GraphQL errors payload: {type(errors).__name__}",
        )
    if errors:
        messages = "; ".join(e.get("message", str(e)) if isinstance(e, dict) else str(e) for e in errors)
        raise RateHistoryUnavailable(spec.dex_name, f"subgraph GraphQL errors: {messages}")

    payload_data = data.get("data")
    if payload_data is not None and not isinstance(payload_data, dict):
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"subgraph returned malformed data payload: {type(payload_data).__name__}",
        )

    return data


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

    When ``spec.resolve_bare_address_pool_id`` is set and
    ``pool_address`` is a bare 42-char ``0x`` address, the address is
    first resolved to the subgraph's full pool ID (VIB-5090, Balancer
    V2 — ``poolSnapshots`` are keyed by the 32-byte pool ID). Resolved
    mappings are cached on the servicer for the process lifetime.

    Raises :class:`RateHistoryUnavailable` for every "no data" path
    (chain not configured, interval mismatch, window too wide, rate-limit,
    GraphQL error, empty window, malformed row, pool-ID resolution
    no-match / ambiguity) — never a silent-zero fallback.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    if interval_secs != _SECONDS_PER_DAY:
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"only daily volume (interval_secs={_SECONDS_PER_DAY}) is served; got {interval_secs}",
        )

    requested_days = (end_ts // _SECONDS_PER_DAY) - (start_ts // _SECONDS_PER_DAY) + 1
    # Cumulative specs fetch extra pre-window baseline rows from the same
    # single page, so they count against the limit too.
    lookback_days = (_CUMULATIVE_BASELINE_LOOKBACK_SECONDS // _SECONDS_PER_DAY) if spec.cumulative_volume else 0
    if requested_days + lookback_days > _PAGE_FIRST:
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"requested window spans {requested_days} daily points"
            f"{f' (+{lookback_days} baseline lookback)' if lookback_days else ''}, "
            f"exceeds single-page limit {_PAGE_FIRST}",
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

    # VIB-5090: Balancer V2 ``poolSnapshots`` are keyed by the full
    # 32-byte pool ID, not the pool address — resolve a bare 42-char
    # address to the full ID first (cached; full IDs pass through).
    identifier = pool_address.lower()
    if spec.resolve_bare_address_pool_id and _is_bare_pool_address(identifier):
        identifier = await _resolve_pool_id_from_address(
            servicer,
            spec,
            chain=chain,
            url=url,
            headers=headers,
            pool_address=pool_address,
        )

    # Cumulative specs need one snapshot BEFORE the window as the baseline
    # for the first in-window day's difference.
    query_start_ts = start_ts - _CUMULATIVE_BASELINE_LOOKBACK_SECONDS if spec.cumulative_volume else start_ts
    payload = {
        "query": _build_query(spec),
        "variables": {
            "poolAddress": identifier,
            "startTime": spec.to_filter_value(query_start_ts),
            "endTime": spec.to_filter_value(end_ts),
        },
    }

    data = await _post_subgraph_query(servicer, spec, url=url, headers=headers, payload=payload)

    rows = (data.get("data") or {}).get(spec.entity) or []
    if not rows:
        resolved_note = f" (resolved pool ID {identifier!r})" if identifier != pool_address.lower() else ""
        raise RateHistoryUnavailable(
            spec.dex_name,
            f"subgraph returned no {spec.entity} for pool {pool_address!r}{resolved_note} "
            f"on {chain} ({start_ts}..{end_ts})",
        )

    try:
        points = _parse_rows(rows, spec)
    except ValueError as exc:
        raise RateHistoryUnavailable(spec.dex_name, str(exc)) from exc
    if spec.cumulative_volume:
        points = _difference_cumulative_points(points, start_ts)
        if not points:
            # The raw-row check above passes when only pre-window (lookback)
            # snapshots exist; differencing then drops them, leaving no
            # in-window volume. Fail closed — returning [] here would silently
            # become zero-fee backtest data (CodeRabbit #3271).
            raise RateHistoryUnavailable(
                spec.dex_name,
                f"no in-window {spec.entity} for pool {pool_address!r} on {chain} "
                f"({start_ts}..{end_ts}) after cumulative differencing",
            )
    return points


__all__ = ["DexVolumeSubgraphSpec", "fetch_dex_volume_history"]
