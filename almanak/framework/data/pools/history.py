"""Pool history - thin gRPC client over the gateway's PoolHistoryService.

This module used to do its own HTTP / GraphQL egress to The Graph,
DefiLlama, and GeckoTerminal, which violated the gateway-boundary rule
(AGENTS.md "Gateway boundary": strategy containers have no outbound
network access except the gateway gRPC channel). VIB-4728 moves all
egress server-side; this module (POOL-7 / VIB-4755) becomes a thin
client that translates gRPC responses into the typed
``DataEnvelope[list[PoolSnapshot]]`` framework shape.

Public surface:

- ``PoolSnapshot`` — dataclass returned inside ``DataEnvelope``.
  Money / reserve fields are typed ``Decimal | None`` (NOT non-Optional)
  so empty-string wire values map to ``None`` (Empty != Zero — never
  substitute ``Decimal("0")`` for unmeasured).
- ``PoolHistoryReader`` — the live reader; takes a ``GatewayClient``
  and translates gRPC responses. ``protocol`` is keyword-only required
  (no default) — closes the silent cross-protocol surface flagged by
  Round-4 of the UAT card iteration (`docs/internal/uat-cards/VIB-4755.md`
  §D-2). A caller on a Base Aerodrome pool address who forgets
  ``protocol="aerodrome"`` raises ``TypeError`` at the framework
  boundary BEFORE any gRPC round-trip.

HOLD contract: any caller that catches ``DataSourceUnavailable`` MUST
either re-raise it or return ``Intent.hold(...)``. A bare catch breaks
the runner's HOLD inference via ``classify_failure`` walking
``__cause__``.

Example::

    from almanak.framework.data.pools.history import PoolHistoryReader

    reader = PoolHistoryReader(gateway_client=client)
    envelope = reader.get_pool_history(
        pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        chain="arbitrum",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 3, 31),
        resolution="1h",
        protocol="uniswap_v3",
    )
    for snap in envelope.value:
        # snap.tvl is Decimal | None — `None` means unmeasured.
        if snap.tvl is not None:
            print(snap.tvl, snap.volume_24h, snap.fee_revenue_24h)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import grpc

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

# Money / reserve fields tracked for Empty != Zero per-row metadata.
# Each name in this tuple corresponds to a Decimal-as-string field on
# the proto's ``PoolSnapshot`` message; the framework boundary maps
# empty-string wire values to ``None`` and records the name in
# ``unmeasured_fields``. Order matches the proto's field order.
_MONEY_FIELD_NAMES: tuple[str, ...] = (
    "tvl",
    "volume_24h",
    "fee_revenue_24h",
    "token0_reserve",
    "token1_reserve",
)

# Resolution string -> proto enum value. Mirrored from the legacy
# pre-VIB-4728 reader so backtest helpers + indicator code continues
# to express resolutions as strings; the gateway proto's enum is the
# authoritative wire form. Typed ``Any`` rather than
# ``gateway_pb2.Resolution.ValueType`` to avoid a top-level proto
# import at module load (lazy-loaded inside ``_load_resolution_map``).
_RESOLUTION_TO_ENUM: dict[str, Any] = {}

# Defensive ceiling on the cursor-iteration loop in
# ``PoolHistoryReader.get_pool_history``. The largest realistic
# window — 730d at 1h, the 1d soft cap on the gateway — is ~24
# chunks at the default 90d-1h soft cap. 50 leaves headroom while
# bounding a gateway bug where ``next_start_ts`` never advances.
_MAX_CURSOR_ITERATIONS = 50


def _load_resolution_map() -> dict[str, Any]:
    """Lazy-load resolution enum mapping to avoid forcing proto import at module load.

    Returned values are ``gateway_pb2.Resolution.ValueType``; typed as
    ``Any`` so the helper can be imported without forcing the proto
    module to load. The dict is populated on first call and cached.
    """
    global _RESOLUTION_TO_ENUM
    if _RESOLUTION_TO_ENUM:
        return _RESOLUTION_TO_ENUM
    from almanak.gateway.proto import gateway_pb2

    _RESOLUTION_TO_ENUM = {
        "1h": gateway_pb2.Resolution.RESOLUTION_1H,
        "4h": gateway_pb2.Resolution.RESOLUTION_4H,
        "1d": gateway_pb2.Resolution.RESOLUTION_1D,
    }
    return _RESOLUTION_TO_ENUM


# =============================================================================
# Data Model — PoolSnapshot (evolved per VIB-4755 D-1)
# =============================================================================


@dataclass(frozen=True)
class PoolSnapshot:
    """Historical pool state at a specific point in time.

    All money / reserve fields are typed ``Decimal | None`` — ``None``
    means the upstream provider did NOT measure the field for this row
    (Empty != Zero). The unmeasured field names are also listed in
    ``unmeasured_fields`` for grep-friendly inspection
    (``if "tvl" in snap.unmeasured_fields:``).

    The pre-VIB-4728 dataclass shape (non-Optional Decimal with
    ``Decimal("0")`` substituted for unmeasured) was an Empty == Zero
    bug; VIB-4728 evolves the boundary to honour CLAUDE.md §Accounting
    "Empty != Zero".

    Attributes:
        timestamp: UTC datetime of the snapshot (timezone-aware).
        tvl: Total value locked in USD. ``None`` if unmeasured by the
            serving provider for this row.
        volume_24h: 24-hour trading volume in USD. ``None`` if unmeasured.
        fee_revenue_24h: 24-hour fee revenue in USD. ``None`` if unmeasured.
        token0_reserve: token0 reserve in human-readable units. ``None``
            if unmeasured (GeckoTerminal does not report reserves).
        token1_reserve: token1 reserve in human-readable units. ``None``
            if unmeasured.
        unmeasured_fields: Names of fields that are ``None`` on this
            row. Invariant: ``unmeasured_fields == frozenset(name for
            name in <money fields> if getattr(self, name) is None)``.
    """

    timestamp: datetime
    tvl: Decimal | None
    volume_24h: Decimal | None
    fee_revenue_24h: Decimal | None
    token0_reserve: Decimal | None
    token1_reserve: Decimal | None
    unmeasured_fields: frozenset[str] = field(default_factory=frozenset)


# =============================================================================
# Wire decoding helpers — empty-string => None (Empty != Zero)
# =============================================================================


def _decimal_or_none(value: str) -> Decimal | None:
    """Parse a Decimal-as-string proto field, returning None for empty.

    The gateway uses ``""`` to mean "not measured by this provider for
    this row" (per AGENTS.md "Empty != Zero"). The framework boundary
    maps that directly to Python ``None`` — NEVER substitutes
    ``Decimal("0")``. A measured zero ("0" / "0.0") survives as
    ``Decimal("0")``.

    Non-finite values (``Decimal("NaN")``, ``Decimal("Infinity")``,
    ``Decimal("-Infinity")``) are treated as unmeasured (``None``) —
    Round-1 PR audit (Claude pr-auditor) flagged that ``Decimal(str)``
    accepts those tokens silently; if they propagated into PoolSnapshot
    money fields, any downstream IL / volume / accounting math
    touching them would yield NaN or blow up entirely (NaN * x = NaN,
    Infinity * 0 = NaN, etc.). Treating them as ``None`` keeps the
    Empty != Zero contract clean: a non-finite wire value means the
    serving provider failed to measure this row.
    """
    if not value:
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError, TypeError):
        # Malformed wire value — surface as unmeasured rather than
        # substituting zero. Logging this is the gateway's job; the
        # framework reader stays silent to avoid log spam on a long
        # time-series with a few corrupted rows.
        return None
    if not parsed.is_finite():
        return None
    return parsed


# =============================================================================
# PoolHistoryReader — thin gRPC client (VIB-4755)
# =============================================================================


class PoolHistoryReader:
    """Thin gRPC client over the gateway's ``PoolHistoryService``.

    This class no longer owns any HTTP / GraphQL egress. All upstream
    provider fetching (The Graph, DefiLlama, GeckoTerminal) happens
    inside the gateway sidecar. The constructor REQUIRES a connected
    ``GatewayClient`` — constructing one without it deliberately raises
    ``TypeError`` so any stale ``PoolHistoryReader()`` call from before
    VIB-4728 fails loudly.

    ``protocol`` on ``get_pool_history`` is **keyword-only required**
    (no default) — closes the silent cross-protocol surface flagged by
    Phase 0b Round-4 of `docs/internal/uat-cards/VIB-4755.md` §D-2.

    Args:
        gateway_client: The connected gateway client. REQUIRED — a
            ``None`` value raises ``TypeError``.
        timeout_seconds: gRPC call timeout (default 30s — longer than
            ``PoolAnalyticsReader``'s 15s because pool history can
            paginate across multiple upstream calls server-side).
    """

    def __init__(
        self,
        gateway_client: GatewayClient,
        *,
        timeout_seconds: float = 30.0,
    ) -> None:
        if gateway_client is None:
            raise TypeError(
                "PoolHistoryReader now requires a connected GatewayClient. "
                "VIB-4728 moved HTTP / GraphQL egress to the gateway side; "
                "constructing a reader without a gateway client is a "
                "programming error. See docs/internal/uat-cards/VIB-4755.md.",
            )
        self._gateway_client = gateway_client
        self._timeout_seconds = timeout_seconds

    # -- Public API -----------------------------------------------------------

    def get_pool_history(
        self,
        pool_address: str,
        chain: str,
        start_date: datetime,
        end_date: datetime | None = None,
        resolution: str = "1h",
        *,
        protocol: str,
    ) -> DataEnvelope[list[PoolSnapshot]]:
        """Fetch historical pool snapshots via the gateway.

        Args:
            pool_address: Pool contract address.
            chain: Chain name (e.g. ``"arbitrum"``, ``"base"``).
            start_date: Start of the history window (UTC).
            end_date: End of the history window. ``None`` resolves to
                ``datetime.now(UTC)`` AT THE FRAMEWORK BOUNDARY (per
                D-2 / D1.S2): a captured ``request.end_ts`` shows the
                resolved Unix-seconds value, NOT ``0``, so the cache
                key is stable. ``MarketSnapshot.pool_history()`` callers
                resolve to the snapshot's frozen ``timestamp`` instead
                (deterministic-replay path) — that resolution happens
                in the accessor before delegating to this reader.
            resolution: One of ``"1h"``, ``"4h"``, ``"1d"``.
            protocol: REQUIRED keyword-only. Protocol slug — e.g.
                ``"uniswap_v3"``, ``"aerodrome"``. The framework reader
                passes this straight through to the gateway; the
                validator + dispatcher rejects ``""`` or unknown
                values with ``INVALID_ARGUMENT``. No default — closes
                the silent cross-protocol surface (VIB-4755 D-2).

        Returns:
            ``DataEnvelope[list[PoolSnapshot]]`` with INFORMATIONAL
            classification. Money fields are ``Decimal | None``
            (``None`` => unmeasured by the serving provider, listed
            in ``snap.unmeasured_fields``).

        Raises:
            DataSourceUnavailable: When the gateway returns a non-OK
                status, the gateway client is not connected, OR the
                gateway returns ``success=False``. Callers that catch
                this exception MUST either re-raise or return
                ``Intent.hold(...)`` so the runner's HOLD inference
                still fires via ``classify_failure`` walking
                ``__cause__``.
        """
        # Import the proto symbols lazily so the framework reader can
        # be imported without forcing the gateway stubs to load
        # (matters for CLI / test surfaces that import the dataclasses
        # only). Same lazy-import pattern as PoolAnalyticsReader.
        from almanak.gateway.proto import gateway_pb2

        # Resolution explicit-fail BEFORE the gRPC round-trip — Round-1
        # PR audit (Claude pr-auditor finding #4). The legacy reader
        # raised ``ValueError("Unsupported resolution '5m'...")`` at the
        # framework boundary. The gateway validator would also reject
        # an UNSPECIFIED resolution, but the resulting
        # ``DataSourceUnavailable("validator rejected request")``
        # message loses the explicit "you passed a bad string" signal
        # that strategy authors rely on. Fail-fast here mirrors D-2's
        # TypeError-before-RPC contract for ``protocol``.
        resolution_map = _load_resolution_map()
        if resolution not in resolution_map:
            raise ValueError(
                f"Unsupported resolution {resolution!r}. Supported: {sorted(resolution_map.keys())}.",
            )

        # Resolve end_date=None at the framework boundary — VIB-4755 D-2
        # contract: direct PoolHistoryReader callers resolve to
        # datetime.now(UTC); MarketSnapshot.pool_history() callers
        # resolve to the snapshot's frozen timestamp BEFORE calling
        # this method (that resolution happens in the accessor).
        if end_date is None:
            end_date = datetime.now(UTC)

        # Normalize tz-naive datetimes to UTC at the framework boundary
        # (gemini-code-assist /pr-audit high-priority finding). Python's
        # ``datetime.timestamp()`` on a tz-naive datetime assumes LOCAL
        # system timezone, which would shift the query window by several
        # hours depending on the host's offset — a silent
        # cross-deployment bug. Explicit UTC normalization keeps the
        # wire-shape ``start_ts`` / ``end_ts`` deterministic across hosts.
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=UTC)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=UTC)

        # The gateway applies a per-resolution SOFT CAP on the window
        # served per call (e.g. 90d at 1h). When the requested window
        # exceeds the cap, the gateway returns
        # ``success=True + truncation_reason=CAP_EXCEEDED +
        # next_start_ts > 0`` so the caller can re-chunk. The framework
        # reader is the natural cursor-iteration boundary: strategy
        # authors expect a single ``get_pool_history(...)`` call to
        # return the full requested window. We loop server-side until
        # the cursor terminates with ``next_start_ts == 0`` (which is
        # either the final slice OR a ``PROVIDER_RETENTION`` sentinel
        # meaning the upstream provider has no more data backward).
        #
        # Codex Round-1 PR audit P2 + Claude pr-auditor #2: silent
        # truncation here is a correctness gap — without this loop a
        # 200d-1h request would return only the first 90 days inside
        # a normal-looking envelope.
        #
        # ``_MAX_CURSOR_ITERATIONS`` is a defensive ceiling against a
        # cursor that fails to advance (would otherwise infinite-loop).
        # The largest realistic window — 730d at 1h, the 1d soft cap —
        # is ~24 chunks at the default 90d-1h soft cap.
        request_end_ts_original = int(end_date.timestamp())
        cursor_start_ts = int(start_date.timestamp())
        accumulated_snapshots: list[PoolSnapshot] = []
        observed_source = "gateway"
        finalized_only_aggregate = True
        cursor_iterations = 0

        # Build the request. Address / chain / protocol go to the wire
        # as-is; the gateway validator handles strip / lowercase /
        # case-sensitivity (EVM lowercases, Solana preserves) per the
        # umbrella D3.F8 inherited-normalization rules. The framework
        # reader does NOT pre-normalize because that would diverge from
        # the gateway's canonical form and create cache-key collisions
        # if the gateway updates its normalization.
        while cursor_iterations < _MAX_CURSOR_ITERATIONS:
            cursor_iterations += 1
            request = gateway_pb2.PoolHistoryRequest(
                pool_address=pool_address,
                chain=chain,
                protocol=protocol,
                start_ts=cursor_start_ts,
                end_ts=request_end_ts_original,
                resolution=resolution_map[resolution],
            )

            try:
                response = self._gateway_client.pool_history.GetPoolHistory(
                    request,
                    timeout=self._timeout_seconds,
                )
            except grpc.RpcError as exc:
                raise DataSourceUnavailable(
                    source="pool_history",
                    reason=f"gateway error: {exc}",
                ) from exc
            except RuntimeError as exc:
                # GatewayClient raises RuntimeError("Gateway client not
                # connected") from the `pool_history` property when the
                # channel is None. Map to the typed exception so the
                # runner's HOLD inference fires via the same
                # DATA_UNAVAILABLE path as a real outage, rather than
                # leaking a RuntimeError up through the iteration loop.
                # Inherited from VIB-4727 #9.
                raise DataSourceUnavailable(
                    source="pool_history",
                    reason=f"gateway client not connected: {exc}",
                ) from exc

            if not response.success:
                # No fake-success envelope (inherited #10). The gateway's
                # success=False signals a hard failure (gateway unreachable
                # upstream, all providers failed, pool not found); raise
                # the typed exception so callers get HOLD inference, not
                # an empty-list silent success.
                raise DataSourceUnavailable(
                    source="pool_history",
                    reason=response.error or "pool history returned success=False",
                )

            # Decode rows. Per-row Empty != Zero: empty-string wire
            # fields map to Python None. The unmeasured_fields set is
            # DERIVED from the decoded values (NOT seeded from
            # ``row.unmeasured_fields``) — CodeRabbit Round-1 finding:
            # seeding from the wire would let a stale or buggy gateway
            # response mark a field as unmeasured even when the decoded
            # value is present, violating the PoolSnapshot invariant
            # ``unmeasured_fields == frozenset(name for name in (5)
            # if getattr(self, name) is None)``. The decoded value is
            # authoritative; the wire's list is informational only.
            for row in response.snapshots:
                decoded: dict[str, Decimal | None] = {
                    name: _decimal_or_none(getattr(row, name)) for name in _MONEY_FIELD_NAMES
                }
                unmeasured = frozenset(name for name in _MONEY_FIELD_NAMES if decoded[name] is None)
                accumulated_snapshots.append(
                    PoolSnapshot(
                        timestamp=datetime.fromtimestamp(int(row.timestamp), tz=UTC),
                        tvl=decoded["tvl"],
                        volume_24h=decoded["volume_24h"],
                        fee_revenue_24h=decoded["fee_revenue_24h"],
                        token0_reserve=decoded["token0_reserve"],
                        token1_reserve=decoded["token1_reserve"],
                        unmeasured_fields=unmeasured,
                    ),
                )
            observed_source = response.source or observed_source
            finalized_only_aggregate = finalized_only_aggregate and bool(response.finalized_only)

            # Cursor termination:
            #   next_start_ts == 0 → terminal (final slice OR PROVIDER_RETENTION sentinel).
            #   next_start_ts > 0 AND > cursor_start_ts → advance and loop.
            #   next_start_ts > 0 AND <= cursor_start_ts → defensive break (cursor not advancing).
            next_cursor = int(response.next_start_ts)
            if next_cursor == 0:
                break
            if next_cursor <= cursor_start_ts:
                # Cursor failed to advance — gateway bug; stop rather than
                # infinite-loop. Surface as success with whatever we got
                # (subsequent slice was unreachable; the accumulated rows
                # are still authoritative for the served window).
                break
            cursor_start_ts = next_cursor
        # NB: no `else` clause on the while — if we hit
        # _MAX_CURSOR_ITERATIONS without breaking, we still return what
        # we accumulated. Hitting the ceiling means the strategy
        # requested a window so large it required >24 chunks; the
        # accumulated rows are still authoritative for the chunks we
        # served. A defensive log here would be appropriate but the
        # gateway-side observability covers cursor-chunk counts.

        snapshots = accumulated_snapshots

        # Compose envelope metadata. Confidence is fixed at 0.85 for a
        # populated response — per-row unmeasured information is
        # carried at the row level (unlike PoolAnalytics which decays
        # response-level confidence per the umbrella D1.S2 Codex
        # Round-4 fix #3 reasoning that a time-series response of N
        # rows cannot collapse to a single response-level confidence).
        meta = DataMeta(
            source=observed_source,
            observed_at=datetime.now(UTC),
            finality="off_chain",
            staleness_ms=0,
            latency_ms=0,
            confidence=0.85,
            cache_hit=finalized_only_aggregate,
        )
        return DataEnvelope(
            value=snapshots,
            meta=meta,
            classification=DataClassification.INFORMATIONAL,
        )

    # -- Compatibility surface for legacy callers ---------------------------

    def health(self) -> dict[str, dict[str, int]]:
        """Provider health is now owned by the gateway servicer.

        The legacy class exposed this for the old direct-HTTP
        providers. Strategy-container code that wants provider stats
        should call the gateway's metrics endpoint instead. Returning
        an empty dict keeps the attribute non-throwing for any callers
        that still poll it during the cut-over. Mirrors the VIB-4727
        ``PoolAnalyticsReader.health()`` compat shim.
        """
        return {}


__all__ = [
    "PoolHistoryReader",
    "PoolSnapshot",
]
