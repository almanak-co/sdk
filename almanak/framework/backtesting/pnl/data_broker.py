"""Run-scoped backtest data broker — lifecycle skeleton (ALM-2943).

Today the data lanes construct their providers ad hoc: the pool-history
ladder is a process-wide singleton reached directly, funding providers are
built inside the perp adapter, APY providers live inside the lending
adapter's calculator. The broker introduces ONE seam that owns provider
ACCESS for a run:

* **Process-wide caches stay process-wide where they are correctness-
  relevant.** ``PoolHistoryFallback``'s definitive-miss memo, unsupported-
  pair table, and transport breaker deliberately outlive a run (they gate a
  gateway service, not a run's data) — the broker routes access to the
  same singleton rather than re-owning the cache.
* **Run-scoped construction is memoized on the broker.** Funding-history
  providers are coalesced per ``(protocol, chain)`` for the run, so two
  adapters in one run share one instance.
* **Every serve stamps the run manifest** (:class:`RunDataManifest`),
  keeping provenance and lifecycle on the same object.

Attachment mechanism: the engine activates the broker with
:func:`data_broker_scope` around the simulation + finalization phases and
also stores it on ``BacktestState``. Engine-owned lanes (the price loop,
``BacktestOHLCVView``) receive the manifest handle explicitly; lanes that
cannot reach run state without public-API changes (the LP adapter's volume
rescue, the liquidity provider's TVL rescue, the perp adapter's funding
path) discover the active broker through a **contextvar**. The contextvar
is set inside the engine's run coroutine, so all awaited helpers and tasks
created within it inherit the value; plain executor threads do NOT — which
is fine because every record/access site runs on the loop side of the
sync/async bridges (``run_sync_gateway_call`` workers never touch the
broker). Outside a run (live trading, direct provider use) the seam
degrades to the legacy direct paths and recording is a no-op.

TODO(ALM-2943): lending-APY provider seam — route the lending adapter's
``InterestCalculator``/``LendingAPYProvider`` construction through
:meth:`BacktestDataBroker.lending_apy_provider`.
TODO(ALM-2943): pool-state history seam — as-of pool-state reads
(``LANE_POOL_STATE``) will be constructed and served here; the as-of
source-ladder ordering is a consumer of the manifest's per-serve ladder
records (ordering itself is a pending human decision).
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from almanak.framework.backtesting.pnl.data_manifest import DEFAULT_SOURCE_LADDER, RunDataManifest

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.providers.pool_history_fallback import PoolHistoryFallback

__all__ = [
    "BacktestDataBroker",
    "active_data_broker",
    "data_broker_scope",
    "pool_history_provider",
    "record_data_serve",
]


class BacktestDataBroker:
    """Owns provider access + the run manifest for one backtest run."""

    def __init__(
        self,
        *,
        manifest: RunDataManifest | None = None,
        source_ladder: Sequence[str] = DEFAULT_SOURCE_LADDER,
    ) -> None:
        self.manifest = manifest if manifest is not None else RunDataManifest(source_ladder=source_ladder)
        self._lock = threading.Lock()
        # Run-scoped memo of constructed funding-history providers, keyed by
        # the caller's (protocol, chain)-shaped key. Failed builds are NOT
        # cached here (the perp adapter's own tried-memo handles those).
        self._funding_providers: dict[Any, Any] = {}
        # TODO(ALM-2943): self._lending_apy_providers — lending seam.
        # TODO(ALM-2943): self._pool_state_reader — pool-state history seam.

    def pool_history(self) -> PoolHistoryFallback:
        """The pool-history ladder helper (process-wide by design, see module doc)."""
        from almanak.framework.backtesting.pnl.providers.pool_history_fallback import get_pool_history_fallback

        return get_pool_history_fallback()

    def funding_provider(self, key: Any, build: Callable[[], Any]) -> Any:
        """Return the run's funding provider for ``key``, building it once.

        Coalesces construction across adapters within the run; the caller
        keeps ownership of validation (built-chain verification) and of
        memoizing rejected/failed builds.
        """
        with self._lock:
            if key not in self._funding_providers:
                self._funding_providers[key] = build()
            return self._funding_providers[key]


_ACTIVE_BROKER: ContextVar[BacktestDataBroker | None] = ContextVar("almanak_backtest_data_broker", default=None)


def active_data_broker() -> BacktestDataBroker | None:
    """The broker for the current run, or ``None`` outside a backtest."""
    return _ACTIVE_BROKER.get()


def record_data_serve(**observation: Any) -> None:
    """Stamp the active run's manifest; a silent no-op outside a run."""
    broker = _ACTIVE_BROKER.get()
    if broker is not None:
        broker.manifest.record(**observation)


def pool_history_provider() -> PoolHistoryFallback:
    """Pool-history access seam: broker-routed in a run, legacy singleton outside."""
    broker = _ACTIVE_BROKER.get()
    if broker is not None:
        return broker.pool_history()
    from almanak.framework.backtesting.pnl.providers.pool_history_fallback import get_pool_history_fallback

    return get_pool_history_fallback()


@contextmanager
def data_broker_scope(broker: BacktestDataBroker) -> Iterator[BacktestDataBroker]:
    """Activate ``broker`` for the enclosed run phases (re-entrant safe)."""
    token = _ACTIVE_BROKER.set(broker)
    try:
        yield broker
    finally:
        _ACTIVE_BROKER.reset(token)
