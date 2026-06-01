"""Null readers for deterministic backtest / paper-fork factories.

VIB-4728 / POOL-7 (VIB-4755) — moving pool history egress to the
gateway introduced the same backtest-determinism risk VIB-4727
identified for pool analytics: a live gateway call at backtest time
makes "works in backtest" silently diverge from production behaviour
(upstream providers can revise historical bars; multi-tenant gateway
caches can be poisoned by other strategies). The agreed contract is
that backtest factories inject a Null reader that always raises
``DataSourceUnavailable("backtest")`` so strategies that depend on
``pool_history(...)`` take a deterministic code path (a static
assumption, a fixture-backed reader, or HOLD) inside backtests.

Public surface:

- ``NullPoolHistoryReader`` — deterministic stub injected by
  ``MarketSnapshotBuilder.for_pnl_backtest_state`` and ``for_paper_fork``
  (D-4). Raises ``DataSourceUnavailable("backtest")`` on every call;
  constructs NO network / subprocess / FFI primitives at any point
  (verified by the D2.M6 38-primitive monkeypatch determinism proof
  in `docs/internal/uat-cards/VIB-4755.md` §D2.M6).

Why a new module (not co-located in ``pools/history.py`` like
``NullPoolAnalyticsReader`` is in ``pools/analytics.py``): the
VIB-4755 UAT card §D-3 documents the choice — keeping the Null
reader in a separate module makes the D3.F10 source-inspection
guard's scope explicit (``null_readers`` is its own row in the
Scan A table) AND avoids the import-graph noise of having the Null
stub live next to the gateway-talking reader (a tester reading
``pools/history.py`` does NOT immediately see a "null" stub that
might look like a fallback).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataEnvelope

if TYPE_CHECKING:
    from almanak.framework.data.pools.history import PoolSnapshot


class NullPoolHistoryReader:
    """Always-raises stub used by backtest factories (VIB-4755).

    Live gateway HTTP at backtest time = nondeterministic results
    across runs — strategies that "work in backtest" then silently
    change behaviour in production. The agreed contract is: backtest
    factories inject this null reader; strategies that depend on
    ``pool_history(...)`` must take a deterministic code path inside
    backtests.

    Any call raises ``DataSourceUnavailable("backtest")`` so the
    runner's HOLD inference path is exercised identically to a real
    gateway outage. The class is intentionally a thin shell — NO
    primitives are constructed in ``__init__`` or anywhere else.
    This is verified by the D2.M6 38-primitive monkeypatch
    determinism test (`tests/framework/market/test_backtest_pool_history_determinism.py
    ::test_null_reader_constructs_no_network_primitives`).
    """

    def get_pool_history(
        self,
        pool_address: str,  # noqa: ARG002
        chain: str,  # noqa: ARG002
        start_date: datetime,  # noqa: ARG002
        end_date: datetime | None = None,  # noqa: ARG002
        resolution: str = "1h",  # noqa: ARG002
        *,
        protocol: str,  # noqa: ARG002  # REQUIRED (VIB-4755 D-2)
    ) -> DataEnvelope[list[PoolSnapshot]]:
        raise DataSourceUnavailable(
            source="pool_history",
            reason="backtest",
        )

    def health(self) -> dict[str, dict[str, int]]:
        """Compat shim — mirrors the live reader's health() shape.

        The live reader's ``health()`` returns ``{}`` (per-provider
        stats are now server-side). The null reader returns the same
        empty dict so any caller that polls ``.health()`` during the
        cut-over gets the same non-throwing response in both backtest
        and live paths.
        """
        return {}


__all__ = [
    "NullPoolHistoryReader",
]
