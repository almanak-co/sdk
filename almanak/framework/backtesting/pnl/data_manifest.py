"""Run-scoped data-provenance manifest for the backtest data lanes (ALM-2943).

Every serve that flows through a backtest data lane — price ticks, pool
volume/TVL days, funding history, OHLCV views, and (future) lending APY /
pool-state history — appends one manifest observation: which lane served
which key from which source, over which time range, and with what outcome.
The manifest is the run's single lane-keyed provenance record; the two
pre-existing run structures (``DataQualityTracker``,
``DataCoverageMetrics``) remain untouched consumers-to-unify (survey §4).

Outcomes:

* ``served`` — a real measurement answered the request (any ladder rung).
* ``degraded`` — the lane fell to a heuristic / fallback / miss and the run
  continued on lower-fidelity data.
* ``refused`` — the lane declined to answer (strict mode raise, contract
  refusal) and the caller saw an error instead of a value.

Entries are AGGREGATED per ``(lane, key, source, outcome, ladder)``: a
90-day hourly run records one row per distinct serve shape with a count
and a first/last time range, not one row per tick. The source-ladder order
in effect is recorded PER-SERVE (per aggregate row) so the future as-of
pinning work (ALM-2943 remainder) can consume ladder ordering from the
manifest instead of re-deriving it; the default order is the single
configurable :data:`DEFAULT_SOURCE_LADDER`.

Thread-safe: the volume (sync) and liquidity (async-executor) lanes can
record concurrently.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

__all__ = [
    "DEFAULT_SOURCE_LADDER",
    "LANE_FUNDING",
    "LANE_LENDING_APY",
    "LANE_OHLCV",
    "LANE_POOL_STATE",
    "LANE_POOL_TVL",
    "LANE_POOL_VOLUME",
    "LANE_PRICE",
    "OUTCOME_DEGRADED",
    "OUTCOME_REFUSED",
    "OUTCOME_SERVED",
    "RunDataManifest",
]

#: Canonical lane names. One lane = one kind of question a run asks of the
#: data plane; keys within a lane identify the token / pool / market asked.
LANE_PRICE = "price"
LANE_OHLCV = "ohlcv"
LANE_POOL_VOLUME = "pool_volume"
LANE_POOL_TVL = "pool_tvl"
LANE_FUNDING = "funding"
# TODO(ALM-2943): lending APY serves (InterestCalculator / LendingAPYProvider
# inside the lending adapter) are not yet instrumented — wire through the
# broker's lending seam when it lands.
LANE_LENDING_APY = "lending_apy"
# TODO(ALM-2943): pool-state history (as-of tick/liquidity snapshots) is a
# future lane; reserved so consumers can key on it from day one.
LANE_POOL_STATE = "pool_state"

OUTCOME_SERVED = "served"
OUTCOME_DEGRADED = "degraded"
OUTCOME_REFUSED = "refused"

#: The single configurable source-ladder order (highest-fidelity first).
#: As-of pinning (pending human decision on ordering) will consume the
#: per-serve ladder recorded in the manifest — change the order HERE (or
#: pass ``source_ladder`` to the broker/manifest), never at a call site.
DEFAULT_SOURCE_LADDER: tuple[str, ...] = ("subgraph", "coingecko-onchain", "multiplier")

#: Distinct-aggregate ceiling. Aggregation keeps a normal run tiny (one row
#: per lane x key x source x outcome); the cap is insurance against a
#: pathological key cardinality, with dropped rows counted honestly.
_MAX_DISTINCT_ENTRIES = 10_000


def _time_marker(value: datetime | date | None) -> str | None:
    """ISO marker for range bookkeeping (lexicographic order == chronological).

    Datetimes are normalized to UTC (naive values assumed UTC) so markers share
    one offset and string comparison stays chronologically correct.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return value.isoformat()


@dataclass
class _ManifestAggregate:
    """Mutable per-shape aggregate; one serialized manifest row."""

    count: int
    first: str | None
    last: str | None
    detail: str


class RunDataManifest:
    """Per-run collector of data-lane serve observations (ALM-2943)."""

    def __init__(self, *, source_ladder: Sequence[str] = DEFAULT_SOURCE_LADDER) -> None:
        self._lock = threading.Lock()
        self.source_ladder: tuple[str, ...] = tuple(source_ladder)
        self._entries: dict[tuple[str, str, str, str, tuple[str, ...]], _ManifestAggregate] = {}
        self._dropped = 0

    def record(
        self,
        *,
        lane: str,
        key: str,
        source: str,
        outcome: str,
        at: datetime | date | None = None,
        start: datetime | date | None = None,
        end: datetime | date | None = None,
        detail: str = "",
        ladder: Sequence[str] | None = None,
    ) -> None:
        """Append one serve observation.

        ``at`` is a point-in-time serve (price tick, funding hour); ``start``
        / ``end`` describe a range serve (a pool-day). ``ladder`` records the
        source-ladder order in effect for THIS serve; ``None`` uses the
        manifest's configured order. Never raises: provenance bookkeeping
        must not out-fail the lane it observes.
        """
        first = _time_marker(start if start is not None else at)
        last = _time_marker(end if end is not None else at)
        ladder_key = tuple(ladder) if ladder is not None else self.source_ladder
        entry_key = (str(lane), str(key), str(source), str(outcome), ladder_key)
        with self._lock:
            aggregate = self._entries.get(entry_key)
            if aggregate is None:
                if len(self._entries) >= _MAX_DISTINCT_ENTRIES:
                    self._dropped += 1
                    return
                self._entries[entry_key] = _ManifestAggregate(count=1, first=first, last=last, detail=detail)
                return
            aggregate.count += 1
            if first is not None and (aggregate.first is None or first < aggregate.first):
                aggregate.first = first
            if last is not None and (aggregate.last is None or last > aggregate.last):
                aggregate.last = last
            if not aggregate.detail and detail:
                aggregate.detail = detail

    @property
    def has_entries(self) -> bool:
        with self._lock:
            return bool(self._entries)

    def entries(self) -> list[dict[str, Any]]:
        """Serialized aggregate rows, deterministically ordered."""
        with self._lock:
            items = sorted(self._entries.items(), key=lambda item: item[0])
            return [
                {
                    "lane": lane,
                    "key": key,
                    "source": source,
                    "outcome": outcome,
                    "ladder": list(ladder),
                    "count": aggregate.count,
                    "first": aggregate.first,
                    "last": aggregate.last,
                    "detail": aggregate.detail,
                }
                for (lane, key, source, outcome, ladder), aggregate in items
            ]

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe manifest payload (str/int/list/None leaves only)."""
        payload: dict[str, Any] = {
            "schema_version": 1,
            "source_ladder": list(self.source_ladder),
            "entries": self.entries(),
        }
        with self._lock:
            if self._dropped:
                payload["dropped_entries"] = self._dropped
        return payload
